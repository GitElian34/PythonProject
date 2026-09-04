"""
plot_classic_stations_per_year.py
═══════════════════════════════════════════════════════════════════
Graphiques annuels (obs alti / pred modèle / insitu ALIGNÉ) pour les 5
meilleures stations 10j et les 5 meilleures 27j (classées par NSE
modèle vs insitu), à partir des sorties de compare_classic_vs_insitu.py.

⚠️ CORRECTIONS vs version précédente :
  1. L'insitu est maintenant Z-SCORÉ avant d'être tracé (sur toute la
     période disponible de la station) -> comparable visuellement à
     obs/pred, qui sont déjà z-scorés depuis la création du dataset
     (create_dataset_DtoD.py). Sans ça, on superposait des mètres
     bruts (insitu) et des données centrées-réduites (obs/pred) sur le
     même axe -> écart visuel énorme qui n'a rien à voir avec le NSE
     réel (calculé, lui, en z-score des deux côtés).
  2. L'insitu n'est affiché qu'AUX DATES où une observation altimétrique
     existe (alignement dans la fenêtre de tolérance ±window_days),
     sous forme de marqueurs discrets, plus une courbe continue brute
     quotidienne -> comparaison directe, point par point.

Sorties :
  Models_Testing/plot/Classic/10j/{station}/{annee}.png
  Models_Testing/plot/Classic/27j/{station}/{annee}.png
═══════════════════════════════════════════════════════════════════
"""

import sqlite3
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

RESIDUALS_DIR = Path("./Models_Testing/Classic/residus")
PLOT_DIR = Path("./Models_Testing/plot/Classic")

INSITU_DB = "./data/insitu_data.db"
DATE_MIN, DATE_MAX = "2016-01-01", "2025-12-31"

N_STATIONS = 5
SELECT_BY = "NSE"

RUNS = [
    {"freq": "10j", "window_days": 5,
     "residuals_csv": RESIDUALS_DIR / "residuals_10j_hwnext_recale.csv",
     "metrics_csv": RESIDUALS_DIR / "metrics_10j_hwnext_sword_insitu.csv"},
    {"freq": "27j", "window_days": 14,
     "residuals_csv": RESIDUALS_DIR / "residuals_27j_hwnext_recale.csv",
     "metrics_csv": RESIDUALS_DIR / "metrics_27j_hwnext_sword_insitu.csv"},
]

C_OBS, C_PRED, C_INS = "#1565C0", "#C0392B", "#2E7D32"

_cache_ins = {}


def get_insitu_series(code_sta):
    if code_sta not in _cache_ins:
        conn = sqlite3.connect(INSITU_DB)
        df = pd.read_sql("""
            SELECT date, h_med_wsh AS wl FROM mesures_insitu
            WHERE code_sta = ? AND date >= ? AND date <= ? ORDER BY date
        """, conn, params=(code_sta, DATE_MIN, DATE_MAX))
        conn.close()
        df["date"] = pd.to_datetime(df["date"])
        df = df.dropna(subset=["wl"])
        _cache_ins[code_sta] = df
    return _cache_ins[code_sta]


def zscore_series(wl_values):
    """Z-score sur toute la période disponible (pas juste l'année affichée)."""
    arr = np.asarray(wl_values, dtype=float)
    mu, sig = np.nanmean(arr), np.nanstd(arr)
    return (arr - mu) / sig if sig > 0 else arr * 0


def align_insitu_to_dates(dates, df_ins_z, window_days):
    """
    Pour chaque date d'observation altimétrique, trouve la valeur insitu
    (déjà z-scorée) la plus proche dans le temps, dans la fenêtre de
    tolérance. Retourne un array aligné (même longueur que `dates`,
    NaN si rien dans la fenêtre).
    """
    wl = np.full(len(dates), np.nan)
    ins_dates = np.array(df_ins_z["date"].values, dtype="datetime64[D]")
    ins_wl = df_ins_z["wl_z"].values
    for i, d in enumerate(np.array(dates, dtype="datetime64[D]")):
        diff = np.abs((ins_dates - d).astype(float))
        idx = int(np.argmin(diff))
        if diff[idx] <= window_days:
            wl[i] = ins_wl[idx]
    return wl


def nse_simple(obs, sim):
    mask = ~(np.isnan(obs) | np.isnan(sim))
    if mask.sum() < 3:
        return np.nan
    o, s = obs[mask], sim[mask]
    denom = np.sum((o - o.mean()) ** 2)
    return float(1 - np.sum((o - s) ** 2) / denom) if denom > 0 else np.nan


def plot_station_year(freq, station, sub_res, ins_aligned, year, out_path):
    sub_year_mask = sub_res["date_recalee"].dt.year == year
    sub_year = sub_res[sub_year_mask]
    if sub_year.empty:
        return False

    ins_year = ins_aligned[sub_year_mask.values]
    dates_year = sub_year["date_recalee"].values
    obs_year = sub_year["obs"].values
    pred_year = sub_year["pred"].values

    nse_alti = nse_simple(ins_year, obs_year)
    nse_model = nse_simple(ins_year, pred_year)

    mask_ins = ~np.isnan(ins_year)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 4.5), sharey=True)
    fig.suptitle(f"Station {station} ({freq}) — {year}  [échelle z-scorée]",
                 fontsize=12, fontweight="bold")

    # ── Graphe 1 : Altimétrie vs Insitu ──────────────────────────
    ax1.plot(dates_year, obs_year, "o-", color=C_OBS, label="Altimétrie (obs)",
              markersize=4, linewidth=1)
    if mask_ins.sum() > 0:
        ax1.plot(dates_year[mask_ins], ins_year[mask_ins], "^-", color=C_INS,
                  label="Insitu (aligné)", markersize=6, linewidth=1, alpha=0.85)
    nse_txt1 = f"NSE = {nse_alti:.3f}" if not np.isnan(nse_alti) else "NSE = n/a"
    ax1.set_title(f"Altimétrie vs Insitu — {nse_txt1}", fontsize=10, fontweight="bold")
    ax1.set_xlabel("Date")
    ax1.set_ylabel("Niveau d'eau (z-score)")
    ax1.legend(fontsize=8)
    ax1.grid(True, alpha=0.3)

    # ── Graphe 2 : Modèle vs Insitu ───────────────────────────────
    ax2.plot(dates_year, pred_year, "s--", color=C_PRED, label="Modèle (pred)",
              markersize=4, linewidth=1)
    if mask_ins.sum() > 0:
        ax2.plot(dates_year[mask_ins], ins_year[mask_ins], "^-", color=C_INS,
                  label="Insitu (aligné)", markersize=6, linewidth=1, alpha=0.85)
    nse_txt2 = f"NSE = {nse_model:.3f}" if not np.isnan(nse_model) else "NSE = n/a"
    ax2.set_title(f"Modèle vs Insitu — {nse_txt2}", fontsize=10, fontweight="bold")
    ax2.set_xlabel("Date")
    ax2.legend(fontsize=8)
    ax2.grid(True, alpha=0.3)

    plt.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=130)
    plt.close(fig)
    return True


for run in RUNS:
    freq = run["freq"]
    window_days = run["window_days"]
    print(f"\n{'=' * 60}\n  PLOTS CLASSIC — {freq.upper()}\n{'=' * 60}")

    if not run["residuals_csv"].exists() or not run["metrics_csv"].exists():
        print(f"⚠ Fichiers manquants pour {freq} -> lance compare_classic_vs_insitu.py d'abord")
        continue

    df_res = pd.read_csv(run["residuals_csv"])
    df_res["station"] = df_res["station"].astype(str)
    df_res["date_recalee"] = pd.to_datetime(df_res["date_recalee"])

    df_metrics = pd.read_csv(run["metrics_csv"])
    df_metrics["station"] = df_metrics["station"].astype(str)

    top_stations = (df_metrics.dropna(subset=[SELECT_BY])
                     .sort_values(SELECT_BY, ascending=False)
                     .head(N_STATIONS))

    print(f"  Top {N_STATIONS} stations (par {SELECT_BY}) :")
    for _, row in top_stations.iterrows():
        print(f"    {row['station']:<15} {SELECT_BY}={row[SELECT_BY]:.3f}  "
              f"insitu={row['insitu_code']}  dist={row['dist_insitu_km']}km")

    for _, row in top_stations.iterrows():
        station, insitu_code = row["station"], row["insitu_code"]
        sub_res = df_res[df_res["station"] == station].sort_values("date_recalee").reset_index(drop=True)
        if sub_res.empty:
            continue

        df_ins_raw = get_insitu_series(insitu_code)
        if df_ins_raw is None or df_ins_raw.empty:
            print(f"  ⚠ {station} : pas de série insitu -> skip")
            continue

        # Z-score de l'insitu sur toute sa période disponible
        df_ins_z = df_ins_raw.copy()
        df_ins_z["wl_z"] = zscore_series(df_ins_z["wl"].values)

        # Alignement aux dates d'observation altimétrique (marqueurs discrets)
        ins_aligned = align_insitu_to_dates(sub_res["date_recalee"].values, df_ins_z, window_days)

        years = sorted(sub_res["date_recalee"].dt.year.unique())
        n_plotted = 0
        for year in years:
            out_path = PLOT_DIR / freq / station / f"{year}.png"
            if plot_station_year(freq, station, sub_res, ins_aligned, year, out_path):
                n_plotted += 1
        print(f"  {station} : {n_plotted} graphiques -> {PLOT_DIR / freq / station}/")

print("\n✅ Terminé.")