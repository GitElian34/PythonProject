"""
verif_baseline_db_directe.py
═══════════════════════════════════════════════════════════════════
Vérification indépendante de la baseline "Alti vs Insitu" : au lieu
de passer par results.p / .nc (toute la chaîne d'évaluation), on va
chercher les mesures BRUTES directement dans les deux BDD sources :
  - hydroweb_next.db  (altimétrie, is_valid=1)
  - insitu_data.db    (insitu, h_med_wsh)

Réutilise l'association station<->insitu déjà trouvée (via SWORD) dans
un fichier metrics_*.csv existant, pour comparer exactement les mêmes
paires station/insitu -> si le NSE médian obtenu ici colle à celui
du pipeline complet (0.209 pour 27j, 0.252 pour 10j), ça confirme
qu'il n'y a AUCUN décalage temporel caché nulle part dans la chaîne
results.p -> CSV -> matching.

Usage :
    python verif_baseline_db_directe.py --freq 10j --metrics-csv \
        ./Models_Testing/DtoD/residus/metrics_DtoD96_hwnext_10j_sword_insitu.csv
═══════════════════════════════════════════════════════════════════
"""

import argparse
import sqlite3
import numpy as np
import pandas as pd
from pathlib import Path

HW_DB = "./data/hydroweb_next.db"
INSITU_DB = "./data/insitu_data.db"
DATE_MIN, DATE_MAX = "2016-01-01", "2025-12-31"
MIN_PAIRS = 10

GAP_RANGES = {"10j": (7, 15), "27j": (22, 32)}


def compute_metrics(obs, pred):
    n = len(obs)
    if n < MIN_PAIRS:
        return {"NSE": np.nan, "KGE": np.nan, "n": n}
    obs, pred = np.asarray(obs, dtype=float), np.asarray(pred, dtype=float)
    denom = np.sum((obs - obs.mean()) ** 2)
    nse = 1 - np.sum((obs - pred) ** 2) / denom if denom > 0 else np.nan
    if obs.std() > 0 and pred.std() > 0:
        r = np.corrcoef(obs, pred)[0, 1]
        alpha = pred.std() / obs.std()
        kge = 1 - np.sqrt((r - 1) ** 2 + (alpha - 1) ** 2)
    else:
        kge = np.nan
    return {"NSE": float(nse) if not np.isnan(nse) else np.nan,
            "KGE": float(kge) if not np.isnan(kge) else np.nan, "n": n}


def zscore(arr):
    arr = np.asarray(arr, dtype=float)
    m = ~np.isnan(arr)
    if m.sum() < 2:
        return arr * np.nan
    mu, sig = arr[m].mean(), arr[m].std()
    return (arr - mu) / sig if sig > 0 else arr * 0


def get_alti_raw(conn_hw, code):
    """Mesures altimétriques BRUTES, directement en BDD, is_valid=1 uniquement."""
    df = pd.read_sql("""
        SELECT measure_date, orthometric_height FROM measurements
        WHERE (station_code = ? OR station_code = ?)
          AND is_valid = 1
          AND measure_date >= ? AND measure_date <= ?
        ORDER BY measure_date
    """, conn_hw, params=(code, code.zfill(13), DATE_MIN, DATE_MAX))
    df["measure_date"] = pd.to_datetime(df["measure_date"])
    return df


def get_insitu_raw(conn_ins, code_sta):
    df = pd.read_sql("""
        SELECT date, h_med_wsh AS wl FROM mesures_insitu
        WHERE code_sta = ? AND date >= ? AND date <= ? ORDER BY date
    """, conn_ins, params=(code_sta, DATE_MIN, DATE_MAX))
    df["date"] = pd.to_datetime(df["date"])
    return df.dropna(subset=["wl"])


def align(dates_ref, df_other, col_date, col_val, window_days):
    wl = np.full(len(dates_ref), np.nan)
    other_dates = np.array(df_other[col_date].values, dtype="datetime64[D]")
    other_vals = df_other[col_val].values
    for i, d in enumerate(np.array(dates_ref, dtype="datetime64[D]")):
        diff = np.abs((other_dates - d).astype(float))
        if len(diff) == 0:
            continue
        idx = int(np.argmin(diff))
        if diff[idx] <= window_days:
            wl[i] = other_vals[idx]
    return wl


def main(freq, metrics_csv):
    window_days = GAP_RANGES[freq][0]  # tolérance = borne basse du gap range, cohérent avec le pipeline

    df_pairs = pd.read_csv(metrics_csv, dtype={"station": str})
    print(f"{len(df_pairs)} paires station<->insitu chargées depuis {metrics_csv}")

    conn_hw = sqlite3.connect(HW_DB)
    conn_ins = sqlite3.connect(INSITU_DB)

    nse_list, kge_list = [], []
    rows = []

    for _, row in df_pairs.iterrows():
        code = row["station"]
        code_ins = row["insitu_code"]

        df_alti = get_alti_raw(conn_hw, code)
        if len(df_alti) < MIN_PAIRS:
            continue
        df_ins = get_insitu_raw(conn_ins, code_ins)
        if len(df_ins) < 5:
            continue

        ins_aligned = align(df_alti["measure_date"].values, df_ins, "date", "wl", window_days)
        mask = ~np.isnan(ins_aligned)
        if mask.sum() < MIN_PAIRS:
            continue

        alti_z = zscore(df_alti["orthometric_height"].values)
        ins_z = zscore(ins_aligned)

        mask2 = ~(np.isnan(alti_z) | np.isnan(ins_z))
        m = compute_metrics(ins_z[mask2], alti_z[mask2])

        if not np.isnan(m["NSE"]):
            nse_list.append(m["NSE"])
            kge_list.append(m["KGE"])
            rows.append({"station": code, "insitu_code": code_ins, "n": m["n"],
                         "NSE_direct_db": m["NSE"], "KGE_direct_db": m["KGE"]})

    conn_hw.close()
    conn_ins.close()

    df_out = pd.DataFrame(rows)
    out_csv = Path(f"./verif_baseline_direct_db_{freq}.csv")
    df_out.to_csv(out_csv, index=False)

    print(f"\nStations comparées : {len(nse_list)}/{len(df_pairs)}")
    print(f"NSE médian (comparaison DIRECTE BDD, sans passer par results.p) : "
          f"{np.median(nse_list):.3f}")
    print(f"KGE médian (comparaison DIRECTE BDD)                            : "
          f"{np.median(kge_list):.3f}")
    print(f"\nÀ comparer à la baseline du pipeline complet (attendu ~0.209 pour 27j, "
          f"~0.252 pour 10j selon tes résultats précédents)")
    print(f"\nDétail -> {out_csv}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--freq", choices=["10j", "27j"], required=True)
    parser.add_argument("--metrics-csv", required=True,
                        help="Un fichier metrics_*_sword_insitu.csv existant, pour réutiliser "
                             "les associations station<->insitu déjà trouvées")
    args = parser.parse_args()
    main(args.freq, args.metrics_csv)