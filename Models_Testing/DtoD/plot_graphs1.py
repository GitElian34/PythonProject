"""
plot_stations_per_year_sparse.py
════════════════════════════════════════════════════════════════════════
Graphiques annuels (obs alti / pred modèle / insitu) pour les 5
meilleures stations 10j et les 5 meilleures stations 27j (modèle de
référence : DtoD96, classées par NSE modèle vs insitu).

VERSION "SPARSE" : le modèle n'est tracé QU'AUX dates où une vraie
observation altimétrique existe (même cadence que obs) — pas de
prédiction journalière ici. L'insitu reste en référence continue.
Voir plot_stations_per_year_daily.py pour la version avec prédiction
journalière du modèle.

Sources :
  Models_Testing/DtoD/residus/residuals_DtoD96_hwnext_{freq}.csv
      (colonnes station, date, obs, pred — pred est journalier dans ce
       fichier mais on le sous-échantillonne ici aux dates où obs existe)
  Models_Testing/DtoD/residus/metrics_DtoD96_hwnext_{freq}_sword_insitu.csv
      (colonnes station, insitu_code, NSE, ... — sert à sélectionner le
       top 5 et à annoter les figures)
  data/insitu_data.db (série insitu brute, quotidienne)

Sorties :
  Models_Testing/DtoD/plots/sparse/{freq}/{station}/{year}.png
════════════════════════════════════════════════════════════════════════
"""

import sqlite3
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

MODEL_LABEL = "DtoD96"
FREQS = ["10j", "27j"]
N_STATIONS = 5
SELECT_BY = "NSE"
WINDOW_DAYS = {"10j": 5, "27j": 14}  # tolérance de recalage insitu, cf. compare_other_models_vs_insitu.py

RESIDUS_DIR = Path("./Models_Testing/DtoD/residus")
PLOT_DIR = Path("./Models_Testing/DtoD/plots/sparse")
INSITU_DB = "./data/insitu_data.db"
DATE_MIN, DATE_MAX = "2016-01-01", "2025-12-31"

# Palette
C_OBS = "#1B4F72"
C_PRED = "#C0392B"
C_INS = "#229954"
C_TEXT = "#2C3E50"

plt.rcParams.update({
    "font.family": "DejaVu Sans",
    "axes.edgecolor": "#BFC9CA",
    "axes.labelcolor": C_TEXT,
    "text.color": C_TEXT,
    "xtick.color": C_TEXT,
    "ytick.color": C_TEXT,
})


# ═══════════════════════════════════════════════════════════════
# INSITU : série brute quotidienne
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


def nse(obs, pred):
    obs, pred = np.asarray(obs, dtype=float), np.asarray(pred, dtype=float)
    denom = np.sum((obs - obs.mean()) ** 2)
    return 1 - np.sum((obs - pred) ** 2) / denom if denom > 0 else np.nan


def zscore_params(values):
    """Moyenne/écart-type sur les valeurs non-NaN (même logique que zscore()
    dans compare_other_models_vs_insitu.py)."""
    v = np.asarray(values, dtype=float)
    v = v[~np.isnan(v)]
    if len(v) < 2:
        return 0.0, 1.0
    mu, sigma = v.mean(), v.std()
    return mu, (sigma if sigma > 0 else 1.0)


def align_insitu_to_dates(dates, df_ins, window_days):
    """Valeur insitu la plus proche de chaque date (obs), dans la fenêtre
    de tolérance -- sert uniquement à calculer les paramètres du z-score
    insitu sur le sous-échantillon aligné à l'altimétrie."""
    out = np.full(len(dates), np.nan)
    if df_ins.empty:
        return out
    id_ = np.array(df_ins["date"].values, dtype="datetime64[D]")
    iv = df_ins["wl"].values
    for i, d in enumerate(np.array(dates, dtype="datetime64[D]")):
        diff = np.abs((id_ - d).astype(float))
        idx = int(np.argmin(diff))
        if diff[idx] <= window_days:
            out[i] = iv[idx]
    return out


# ═══════════════════════════════════════════════════════════════
# PLOT PAR STATION / ANNÉE
# ═══════════════════════════════════════════════════════════════
def plot_station_year(freq, station, sub_obs, df_ins, year, global_nse, out_path):
    sub_year = sub_obs[sub_obs["date"].dt.year == year]
    if sub_year.empty:
        return False

    ins_year = df_ins[df_ins["date"].dt.year == year] if len(df_ins) else pd.DataFrame()

    year_nse = nse(sub_year["obs"], sub_year["pred"]) if len(sub_year) >= 3 else np.nan
    nse_txt = f"NSE {year} = {year_nse:.2f}" if pd.notna(year_nse) else "NSE (n insuffisant)"

    fig, ax = plt.subplots(figsize=(11, 4.5))

    if len(ins_year):
        ax.plot(ins_year["date"], ins_year["wl_z"], "-", color=C_INS,
                linewidth=1.1, alpha=0.55, label="Insitu (référence continue, z-score)", zorder=1)

    ax.plot(sub_year["date"], sub_year["obs_z"], "o", color=C_OBS, markersize=6,
            markeredgecolor="white", markeredgewidth=0.6, label="Altimétrie (obs, z-score)", zorder=3)
    ax.plot(sub_year["date"], sub_year["pred_z"], "s", color=C_PRED, markersize=5.5,
            markeredgecolor="white", markeredgewidth=0.6, label=f"Modèle {MODEL_LABEL} (pred, z-score)", zorder=3)
    ax.plot(sub_year["date"], sub_year["pred_z"], "--", color=C_PRED, linewidth=0.8, alpha=0.5, zorder=2)

    ax.set_title(f"{station}  ·  {freq}  ·  {year}", fontsize=12, fontweight="bold", loc="left")
    ax.text(0.99, 1.03, nse_txt, transform=ax.transAxes, ha="right", va="bottom",
            fontsize=9.5, style="italic", color=C_TEXT)
    ax.text(0.01, -0.16,
            f"NSE global (toute la période, {SELECT_BY}) = {global_nse:.3f}  ·  "
            f"séries centrées-réduites (z-score, indépendamment par courbe) "
            f"— l'altimétrie et l'insitu n'ont pas le même référentiel vertical",
            transform=ax.transAxes, ha="left", va="top", fontsize=7.5, color="#7F8C8D")

    ax.set_ylabel("Niveau d'eau — z-score")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(True, alpha=0.25, linestyle="--")
    ax.legend(frameon=False, fontsize=9, loc="upper left", bbox_to_anchor=(0, -0.06),
              ncol=3, handletextpad=0.5)

    fig.tight_layout(rect=[0, 0.03, 1, 1])
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=140, facecolor="white")
    plt.close(fig)
    return True


# ═══════════════════════════════════════════════════════════════
# BOUCLE PRINCIPALE
# ═══════════════════════════════════════════════════════════════
def main():
    for freq in FREQS:
        print(f"\n{'=' * 60}\n  PLOTS SPARSE — {freq.upper()} ({MODEL_LABEL})\n{'=' * 60}")

        res_csv = RESIDUS_DIR / f"residuals_{MODEL_LABEL}_hwnext_{freq}.csv"
        met_csv = RESIDUS_DIR / f"metrics_{MODEL_LABEL}_hwnext_{freq}_sword_insitu.csv"
        if not res_csv.exists() or not met_csv.exists():
            print(f"⚠ Fichiers manquants pour {freq} -> ignoré ({res_csv}, {met_csv})")
            continue

        df_res = pd.read_csv(res_csv)
        df_res["station"] = df_res["station"].astype(str)
        df_res["date"] = pd.to_datetime(df_res["date"])

        df_met = pd.read_csv(met_csv)
        df_met["station"] = df_met["station"].astype(str)

        top_stations = (df_met.dropna(subset=[SELECT_BY])
                         .sort_values(SELECT_BY, ascending=False)
                         .head(N_STATIONS))

        print(f"  Top {N_STATIONS} stations (par {SELECT_BY}) :")
        for _, row in top_stations.iterrows():
            print(f"    {row['station']:<15} {SELECT_BY}={row[SELECT_BY]:.3f}  insitu={row['insitu_code']}")

        for _, row in top_stations.iterrows():
            station, insitu_code, global_nse = row["station"], row["insitu_code"], row[SELECT_BY]

            sub_obs = (df_res[df_res["station"] == station]
                       .dropna(subset=["obs", "pred"])
                       .sort_values("date"))
            if sub_obs.empty:
                continue

            df_ins = get_insitu_series(insitu_code).copy()

            # Z-score indépendant par courbe, calculé UNE FOIS sur toute la
            # période dispo de la station (pas par année) pour garder une
            # échelle cohérente d'une année à l'autre.
            obs_mu, obs_sigma = zscore_params(sub_obs["obs"])
            pred_mu, pred_sigma = zscore_params(sub_obs["pred"])
            sub_obs = sub_obs.assign(
                obs_z=(sub_obs["obs"] - obs_mu) / obs_sigma,
                pred_z=(sub_obs["pred"] - pred_mu) / pred_sigma,
            )

            aligned_ins = align_insitu_to_dates(sub_obs["date"].values, df_ins, WINDOW_DAYS[freq])
            ins_mu, ins_sigma = zscore_params(aligned_ins)
            if len(df_ins):
                df_ins["wl_z"] = (df_ins["wl"] - ins_mu) / ins_sigma

            years = sorted(sub_obs["date"].dt.year.unique())
            n_plotted = 0
            for year in years:
                out_path = PLOT_DIR / freq / station / f"{year}.png"
                if plot_station_year(freq, station, sub_obs, df_ins, year, global_nse, out_path):
                    n_plotted += 1
            print(f"  {station} : {n_plotted} graphiques -> {PLOT_DIR / freq / station}/")

    print("\n✅ Terminé.")


if __name__ == "__main__":
    main()