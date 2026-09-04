"""
build_outlier_nse_analysis.py
════════════════════════════════════════════════════════════════════════
Calcule, pour HW Next (10j/27j) et pour 4 MÉTHODES de sévérité
d'outlier :

    "≥1/3 modèles"   : au moins 1 des 3 modèles Quantile80/90/96
                       flague le point (obs hors [Q5,Q95] de CE modèle).
    "≥2/3 modèles"   : au moins 2 des 3.
    "3/3 modèles"    : les 3 flaguent le même point.
    "3/3 + 0.5σ"     : 3/3 (ci-dessus) ET l'écart de dépassement MOYEN
                       sur les 3 modèles (distance à la borne violée,
                       en unités de σ_résidu de chaque modèle) est ≥ 0.5.

... l'impact sur 4 métriques baseline "Alti vs Insitu" (NSE/KGE/R2/NRMSE,
indépendant de tout modèle) selon DEUX stratégies :

    TABLEAU A (retrait)      : les points flagués sont EXCLUS du calcul.
    TABLEAU B (remplacement) : les points flagués gardent leur date/poids,
                               mais la valeur altimétrique est REMPLACÉE
                               par la prédiction Q50 du modèle Quantile96.

σ_résidu = écart-type de (obs − pred_q50), par station et par modèle.
NRMSE = RMSE sur séries z-scorées (pas RMSE/plage sur données brutes,
non disponible depuis ces fichiers) -- Δ positif = RMSE plus BAS après
retrait/remplacement (donc amélioration, même convention que les autres
métriques où Δ positif = mieux).

Sources :
  Models_Testing/Quantille/residus/residuals_Quantile{80,90,96}_hwnext_{freq}_bands.csv
  Models_Testing/Quantille/residus/metrics_Quantile96_hwnext_{freq}_sword_insitu.csv
  data/insitu_data.db

Sorties :
  Models_Testing/Quantille/figures/table_outlier_nse_removal.png   (+ .csv)
  Models_Testing/Quantille/figures/table_outlier_nse_replacement.png (+ .csv)
════════════════════════════════════════════════════════════════════════
"""

import sqlite3
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

MODELS = ["Quantile80", "Quantile90", "Quantile96"]
PRIMARY_MODEL = "Quantile96"  # source de insitu_code, obs, et valeur de remplacement (Q50)
SOURCE = "hwnext"             # <-- restreint à HW Next uniquement
FREQS = ["10j", "27j"]
WINDOW_DAYS = {"10j": 5, "27j": 14}
MIN_PAIRS = 10
SIGMA_MARGIN = 0.5

METHODS = ["≥1/3 modèles", "≥2/3 modèles", "3/3 modèles", "3/3 + 0.5σ"]

RESIDUS_DIR = Path("./Models_Testing/Quantille/residus")
OUT_DIR = Path("./Models_Testing/Quantille/figures")
INSITU_DB = "./data/insitu_data.db"
DATE_MIN, DATE_MAX = "2016-01-01", "2025-12-31"

COLOR_TEXT = "#1A1A1A"
COLOR_SUBTEXT = "#6B6B6B"
COLOR_LINE = "#2B2B2B"
COLOR_ROW_LINE = "#E3E3E3"


# ═══════════════════════════════════════════════════════════════
# INSITU
# ═══════════════════════════════════════════════════════════════
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
        _cache_ins[code_sta] = df.dropna(subset=["wl"])
    return _cache_ins[code_sta]


def align_insitu(dates, df_ins, window_days):
    wl = np.full(len(dates), np.nan)
    if df_ins.empty:
        return wl
    id_ = np.array(df_ins["date"].values, dtype="datetime64[D]")
    iv = df_ins["wl"].values
    for i, d in enumerate(np.array(dates, dtype="datetime64[D]")):
        diff = np.abs((id_ - d).astype(float))
        idx = int(np.argmin(diff))
        if diff[idx] <= window_days:
            wl[i] = iv[idx]
    return wl


def zscore_params(values):
    v = np.asarray(values, dtype=float)
    v = v[~np.isnan(v)]
    if len(v) < 2:
        return 0.0, 1.0
    mu, sig = v.mean(), v.std()
    return mu, (sig if sig > 0 else 1.0)


def compute_metrics(obs, pred) -> dict:
    """NSE/KGE (sans beta)/RMSE/R2 -- même formule que compare_other_models_vs_insitu.py."""
    obs, pred = np.asarray(obs, dtype=float), np.asarray(pred, dtype=float)
    n = len(obs)
    if n < MIN_PAIRS:
        return {"NSE": np.nan, "KGE": np.nan, "RMSE": np.nan, "R2": np.nan}
    denom = np.sum((obs - obs.mean()) ** 2)
    nse = 1 - np.sum((obs - pred) ** 2) / denom if denom > 0 else np.nan
    rmse = float(np.sqrt(np.mean((obs - pred) ** 2)))
    if obs.std() > 0 and pred.std() > 0:
        r = np.corrcoef(obs, pred)[0, 1]
        r2 = float(r ** 2)
    else:
        r, r2 = np.nan, np.nan
    if obs.std() > 0 and not np.isnan(r):
        alpha = pred.std() / obs.std()
        kge = 1 - np.sqrt((r - 1) ** 2 + (alpha - 1) ** 2)
    else:
        kge = np.nan
    return {"NSE": float(nse) if pd.notna(nse) else np.nan,
            "KGE": float(kge) if pd.notna(kge) else np.nan,
            "RMSE": rmse, "R2": r2}


# ═══════════════════════════════════════════════════════════════
# CHARGEMENT + FLAGS PAR MODÈLE (basique + écart en sigma)
# ═══════════════════════════════════════════════════════════════
def load_bands(model_label: str, freq: str) -> pd.DataFrame:
    path = RESIDUS_DIR / f"residuals_{model_label}_{SOURCE}_{freq}_bands.csv"
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path)
    df["station"] = df["station"].astype(str)
    df["date"] = pd.to_datetime(df["date"])
    n_before = len(df)
    df = df.drop_duplicates(subset=["station", "date"])
    if len(df) != n_before:
        print(f"  ⚠ {model_label} {SOURCE} {freq} : {n_before - len(df)} lignes dupliquées "
              f"(station,date) retirées")
    return df


def load_and_flag(model_label: str, freq: str) -> pd.DataFrame:
    df = load_bands(model_label, freq)
    if df.empty:
        return df
    df = df.dropna(subset=["obs", "pred_q05", "pred_q50", "pred_q95"]).copy()
    if df.empty:
        return df
    residual = df["obs"] - df["pred_q50"]
    sigma_by_station = df.assign(_res=residual).groupby("station")["_res"].transform(
        lambda s: s.std() if len(s) >= 5 else np.nan)
    df["is_outlier"] = (df["obs"] < df["pred_q05"]) | (df["obs"] > df["pred_q95"])
    excess_raw = (df["pred_q05"] - df["obs"]).clip(lower=0) + (df["obs"] - df["pred_q95"]).clip(lower=0)
    df["excess_sigma"] = excess_raw / sigma_by_station
    return df


def build_point_table(freq: str) -> pd.DataFrame:
    """Une ligne par (station,date) commune aux 3 modèles, avec n_agree,
    avg_excess_sigma, et pred_q50 du PRIMARY_MODEL (pour remplacement)."""
    flagged = {m: load_and_flag(m, freq) for m in MODELS}
    if any(flagged[m].empty for m in MODELS):
        return pd.DataFrame()

    base = None
    for m in MODELS:
        sub = flagged[m][["station", "date", "obs", "is_outlier", "excess_sigma"]].rename(
            columns={"is_outlier": f"out_{m}", "excess_sigma": f"excess_{m}"})
        base = sub if base is None else base.merge(sub, on=["station", "date", "obs"], how="inner")
    if base is None or base.empty:
        return pd.DataFrame()

    base["n_agree"] = base[[f"out_{m}" for m in MODELS]].sum(axis=1)
    base["avg_excess_sigma"] = base[[f"excess_{m}" for m in MODELS]].mean(axis=1)

    primary_q50 = flagged[PRIMARY_MODEL][["station", "date", "pred_q50"]]
    base = base.merge(primary_q50, on=["station", "date"], how="left")
    return base


def flag_for_method(base: pd.DataFrame, method: str) -> pd.Series:
    if method == "≥1/3 modèles":
        return base["n_agree"] >= 1
    if method == "≥2/3 modèles":
        return base["n_agree"] >= 2
    if method == "3/3 modèles":
        return base["n_agree"] == 3
    if method == "3/3 + 0.5σ":
        return (base["n_agree"] == 3) & (base["avg_excess_sigma"] >= SIGMA_MARGIN)
    raise ValueError(method)


# ═══════════════════════════════════════════════════════════════
# MÉTRIQUES BASELINE : RETRAIT vs REMPLACEMENT (4 métriques)
# ═══════════════════════════════════════════════════════════════
def compute_rows(freq: str) -> list:
    # Population de référence = TOUS les points valides de PRIMARY_MODEL
    # (Quantile96) -- le join à 3 modèles sert UNIQUEMENT à déterminer
    # qui est flaggé, pas à restreindre la population du calcul.
    df_obs_all = load_bands(PRIMARY_MODEL, freq)
    if df_obs_all.empty:
        print(f"⚠ HW Next {freq} : pas de fichier {PRIMARY_MODEL} -> ignoré")
        return []
    df_obs_all = df_obs_all.dropna(subset=["obs"])

    point_table = build_point_table(freq)
    if point_table.empty:
        print(f"⚠ HW Next {freq} : consensus 3 modèles indisponible -> ignoré")
        return []

    met_path = RESIDUS_DIR / f"metrics_{PRIMARY_MODEL}_{SOURCE}_{freq}_sword_insitu.csv"
    if not met_path.exists():
        print(f"⚠ HW Next {freq} : {met_path} introuvable -> ignoré")
        return []
    df_met = pd.read_csv(met_path)
    df_met["station"] = df_met["station"].astype(str)
    insitu_code_by_station = dict(zip(df_met["station"], df_met["insitu_code"]))

    results = []

    for method in METHODS:
        flag = flag_for_method(point_table, method)
        outlier_lookup = set(zip(point_table.loc[flag, "station"], point_table.loc[flag, "date"]))

        before_list = {k: [] for k in ["NSE", "KGE", "RMSE", "R2"]}
        removed_list = {k: [] for k in ["NSE", "KGE", "RMSE", "R2"]}
        replaced_list = {k: [] for k in ["NSE", "KGE", "RMSE", "R2"]}
        n_stations, n_points_total, n_points_flagged = 0, 0, 0

        for station, sub in df_obs_all.groupby("station"):
            insitu_code = insitu_code_by_station.get(station)
            if not insitu_code:
                continue
            sub = sub.sort_values("date")
            if len(sub) < MIN_PAIRS:
                continue

            df_ins = get_insitu_series(insitu_code)
            ins_wl = align_insitu(sub["date"].values, df_ins, WINDOW_DAYS[freq])

            obs_mu, obs_sigma = zscore_params(sub["obs"].values)
            obs_z = (sub["obs"].values - obs_mu) / obs_sigma
            q50_z = (sub["pred_q50"].values - obs_mu) / obs_sigma  # même échelle que obs
            ins_mu, ins_sigma = zscore_params(ins_wl)
            ins_z = (ins_wl - ins_mu) / ins_sigma

            mask_valid = ~(np.isnan(obs_z) | np.isnan(ins_z))
            if mask_valid.sum() < MIN_PAIRS:
                continue

            is_flagged = np.array([(station, d) in outlier_lookup for d in sub["date"]])

            m_before = compute_metrics(ins_z[mask_valid], obs_z[mask_valid])

            mask_removed = mask_valid & ~is_flagged
            m_removed = compute_metrics(ins_z[mask_removed], obs_z[mask_removed])

            obs_z_replaced = np.where(is_flagged, q50_z, obs_z)
            mask_replaced_valid = mask_valid & ~(is_flagged & np.isnan(q50_z))
            m_replaced = compute_metrics(ins_z[mask_replaced_valid], obs_z_replaced[mask_replaced_valid])

            if pd.notna(m_before["NSE"]):
                for k in before_list:
                    before_list[k].append(m_before[k])
                n_stations += 1
                n_points_total += int(mask_valid.sum())
                n_points_flagged += int((mask_valid & is_flagged).sum())
            if pd.notna(m_removed["NSE"]):
                for k in removed_list:
                    removed_list[k].append(m_removed[k])
            if pd.notna(m_replaced["NSE"]):
                for k in replaced_list:
                    replaced_list[k].append(m_replaced[k])

        if not before_list["NSE"]:
            continue

        def med(lst):
            return float(np.median(lst)) if lst else np.nan

        before_med = {k: med(v) for k, v in before_list.items()}
        removed_med = {k: med(v) for k, v in removed_list.items()}
        replaced_med = {k: med(v) for k, v in replaced_list.items()}
        pct_flagged = round(100 * n_points_flagged / n_points_total, 2) if n_points_total else np.nan

        row = {
            "freq": freq, "method": method,
            "n_stations": n_stations, "n_points_total": n_points_total,
            "pct_flagged": pct_flagged,
        }
        for k in ["NSE", "KGE", "R2"]:
            row[f"{k}_before"] = round(before_med[k], 3)
            row[f"{k}_removed"] = round(removed_med[k], 3)
            row[f"delta_removed_{k}"] = round(removed_med[k] - before_med[k], 3)
            row[f"{k}_replaced"] = round(replaced_med[k], 3)
            row[f"delta_replaced_{k}"] = round(replaced_med[k] - before_med[k], 3)
        # RMSE : Δ positif = RMSE plus bas après retrait/remplacement (amélioration)
        row["NRMSE_before"] = round(before_med["RMSE"], 3)
        row["NRMSE_removed"] = round(removed_med["RMSE"], 3)
        row["delta_removed_NRMSE"] = round(before_med["RMSE"] - removed_med["RMSE"], 3)
        row["NRMSE_replaced"] = round(replaced_med["RMSE"], 3)
        row["delta_replaced_NRMSE"] = round(before_med["RMSE"] - replaced_med["RMSE"], 3)

        results.append(row)
        print(f"  HW Next {freq} [{method}] : n={n_points_total} pts, {pct_flagged}% flagués  "
              f"ΔNSE(retrait)={row['delta_removed_NSE']:+.3f}  "
              f"ΔNSE(remplacement)={row['delta_replaced_NSE']:+.3f}")

    return results


# ═══════════════════════════════════════════════════════════════
# RENDU PNG — tableau épuré (typographique, sans bandeaux colorés)
# ═══════════════════════════════════════════════════════════════
def fmt(x, digits=3):
    return f"{x:.{digits}f}" if pd.notna(x) else "—"


def fmt_delta(x):
    return f"{x:+.3f}" if pd.notna(x) else "—"


def fmt_pct(x):
    return f"{x:.2f}%" if pd.notna(x) else "—"


def render_flat_table(df: pd.DataFrame, out_path: Path, title: str, suffix: str) -> None:
    """suffix: 'removed' ou 'replaced' -- sélectionne les colonnes delta_{suffix}_*."""
    columns = [
        ("Groupe", "freq", "text", 0.9),
        ("Critère", "method", "text", 1.3),
        ("% flagué", "pct_flagged", "pct", 1.0),
        ("ΔNSE", f"delta_{suffix}_NSE", "delta", 1.0),
        ("ΔKGE", f"delta_{suffix}_KGE", "delta", 1.0),
        ("ΔR²", f"delta_{suffix}_R2", "delta", 1.0),
        ("ΔNRMSE", f"delta_{suffix}_NRMSE", "delta", 1.0),
    ]
    n_cols = len(columns)
    widths = [w for _, _, _, w in columns]
    widths = [w / sum(widths) for w in widths]

    row_h, header_h, title_h = 0.55, 0.55, 0.6
    n_rows = len(df)
    fig_h = title_h + header_h + n_rows * row_h + 0.35
    fig_w = 1.55 * n_cols

    fig = plt.figure(figsize=(fig_w, fig_h), dpi=200)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_xlim(0, fig_w)
    ax.set_ylim(0, fig_h)
    ax.invert_yaxis()
    ax.axis("off")

    margin = 0.25
    avail_w = fig_w - 2 * margin
    x_edges = [margin]
    for w in widths:
        x_edges.append(x_edges[-1] + w * avail_w)

    ax.text(margin, title_h / 2, title, ha="left", va="center",
            fontsize=13, fontweight="bold", color=COLOR_TEXT)

    y = title_h
    # En-tête : texte gras aligné à gauche, pas de fond coloré
    for j, (label, _, _, _) in enumerate(columns):
        xa = x_edges[j]
        ax.text(xa, y + header_h / 2, label, ha="left", va="center",
                fontsize=10, fontweight="bold", color=COLOR_TEXT)
    y += header_h
    ax.plot([margin, margin + avail_w], [y, y], color=COLOR_LINE, linewidth=1.1)

    for i, r in df.iterrows():
        for j, (_, key, kind, _) in enumerate(columns):
            xa = x_edges[j]
            if kind == "text":
                ax.text(xa, y + row_h / 2, str(r[key]), ha="left", va="center",
                        fontsize=9.5, color=COLOR_TEXT)
            elif kind == "pct":
                ax.text(xa, y + row_h / 2, fmt_pct(r[key]), ha="left", va="center",
                        fontsize=9.5, color=COLOR_TEXT)
            elif kind == "delta":
                ax.text(xa, y + row_h / 2, fmt_delta(r[key]), ha="left", va="center",
                        fontsize=9.5, fontweight="bold", color=COLOR_TEXT)
            else:
                ax.text(xa, y + row_h / 2, fmt(r[key]), ha="left", va="center",
                        fontsize=9.5, color=COLOR_TEXT)
        y += row_h
        ax.plot([margin, margin + avail_w], [y, y], color=COLOR_ROW_LINE, linewidth=0.7)

    fig.savefig(out_path, facecolor="white")
    plt.close(fig)
    print(f"✅ {out_path}")


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    all_rows = []
    for freq in FREQS:
        all_rows.extend(compute_rows(freq))

    if not all_rows:
        raise FileNotFoundError("Aucune donnée exploitable.")

    df = pd.DataFrame(all_rows)
    df.to_csv(OUT_DIR / "table_outlier_nse_full.csv", index=False)

    render_flat_table(
        df, OUT_DIR / "table_outlier_nse_removal.png",
        title="HW Next — Alti-Insitu, impact du RETRAIT des outliers",
        suffix="removed",
    )
    render_flat_table(
        df, OUT_DIR / "table_outlier_nse_replacement.png",
        title=f"HW Next — Alti-Insitu, impact du REMPLACEMENT des outliers par {PRIMARY_MODEL} Q50",
        suffix="replaced",
    )


if __name__ == "__main__":
    main()