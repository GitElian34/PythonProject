"""
plot_stations_per_year.py
═══════════════════════════════════════════════════════════════════
Génère des graphiques annuels (obs alti / pred modèle / insitu) pour
les 5 meilleures stations 10j et les 5 meilleures stations 27j
(classées par NSE modèle vs insitu, déjà calculé dans les CSV metrics).

Réutilise :
  - residuals_{freq}_hwnext_recale.csv  (obs, pred, date_recalee)
  - metrics_{freq}_hwnext_sword_insitu.csv  (NSE, insitu_code déjà associé)
  - insitu_data.db  (série brute de l'insitu associé, pour overlay)

Pas de recalcul de connectivité SWORD -> rapide.

Sorties :
  Models_Testing/plot/10j/{station}/{annee}.png
  Models_Testing/plot/27j/{station}/{annee}.png

Usage :
    python plot_stations_per_year.py
═══════════════════════════════════════════════════════════════════
"""

import sqlite3
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
RESIDUALS_DIR = Path("./Models_Testing/Residus")
PLOT_DIR      = Path("./Models_Testing/plot")

INSITU_DB   = "./data/insitu_data.db"
DATE_MIN, DATE_MAX = "2016-01-01", "2025-12-31"

N_STATIONS  = 5           # nombre de stations à tracer par fréquence
SELECT_BY   = "NSE"       # critère de sélection des stations ("NSE" = meilleures en premier)

RUNS = [
    {
        "freq": "10j",
        "residuals_csv": RESIDUALS_DIR / "residuals_10j_hwnext_recale.csv",
        "metrics_csv":   RESIDUALS_DIR / "metrics_10j_hwnext_sword_insitu.csv",
    },
    {
        "freq": "27j",
        "residuals_csv": RESIDUALS_DIR / "residuals_27j_hwnext_recale.csv",
        "metrics_csv":   RESIDUALS_DIR / "metrics_27j_hwnext_sword_insitu.csv",
    },
]

C_OBS   = "#1565C0"   # altimétrie observée
C_PRED  = "#C0392B"   # prédiction modèle
C_INS   = "#2E7D32"   # insitu


# ═══════════════════════════════════════════════════════════════
# INSITU : récupération série brute
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
        df = df.dropna(subset=["wl"])
        _cache_ins[code_sta] = df
    return _cache_ins[code_sta]


# ═══════════════════════════════════════════════════════════════
# PLOT PAR STATION / ANNÉE
# ═══════════════════════════════════════════════════════════════
def plot_station_year(freq, station, sub_res, df_ins, year, out_path):
    sub_year = sub_res[sub_res["date_recalee"].dt.year == year]
    if sub_year.empty:
        return False

    ins_year = None
    if df_ins is not None and len(df_ins):
        ins_year = df_ins[df_ins["date"].dt.year == year]

    fig, ax = plt.subplots(figsize=(11, 4.5))

    ax.plot(sub_year["date_recalee"], sub_year["obs"], "o-",
            color=C_OBS, label="Altimétrie (obs)", markersize=4, linewidth=1)
    ax.plot(sub_year["date_recalee"], sub_year["pred"], "s--",
            color=C_PRED, label="Modèle (pred)", markersize=4, linewidth=1)

    if ins_year is not None and len(ins_year):
        ax.plot(ins_year["date"], ins_year["wl"], "-",
                color=C_INS, label="Insitu", linewidth=1, alpha=0.7)

    ax.set_title(f"Station {station} ({freq}) — {year}", fontsize=11, fontweight="bold")
    ax.set_xlabel("Date")
    ax.set_ylabel("Niveau d'eau")
    ax.legend(fontsize=9)
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=130)
    plt.close(fig)
    return True


# ═══════════════════════════════════════════════════════════════
# BOUCLE PRINCIPALE
# ═══════════════════════════════════════════════════════════════
for run in RUNS:
    freq = run["freq"]
    print(f"\n{'=' * 60}")
    print(f"  PLOTS — {freq.upper()}")
    print(f"{'=' * 60}")

    if not run["residuals_csv"].exists() or not run["metrics_csv"].exists():
        print(f"⚠ Fichiers manquants pour {freq} -> ignoré")
        continue

    df_res = pd.read_csv(run["residuals_csv"])
    df_res["station"] = df_res["station"].astype(str)
    df_res["date_recalee"] = pd.to_datetime(df_res["date_recalee"])

    df_metrics = pd.read_csv(run["metrics_csv"])
    df_metrics["station"] = df_metrics["station"].astype(str)

    # Sélection des N meilleures stations par NSE
    top_stations = (df_metrics.dropna(subset=[SELECT_BY])
                     .sort_values(SELECT_BY, ascending=False)
                     .head(N_STATIONS))

    print(f"  Top {N_STATIONS} stations (par {SELECT_BY}) :")
    for _, row in top_stations.iterrows():
        print(f"    {row['station']:<15} {SELECT_BY}={row[SELECT_BY]:.3f}  "
              f"insitu={row['insitu_code']}  dist={row['dist_insitu_km']}km")

    for _, row in top_stations.iterrows():
        station = row["station"]
        insitu_code = row["insitu_code"]

        sub_res = df_res[df_res["station"] == station].sort_values("date_recalee")
        if sub_res.empty:
            continue

        df_ins = get_insitu_series(insitu_code)

        years = sorted(sub_res["date_recalee"].dt.year.unique())
        n_plotted = 0
        for year in years:
            out_path = PLOT_DIR / freq / station / f"{year}.png"
            ok = plot_station_year(freq, station, sub_res, df_ins, year, out_path)
            if ok:
                n_plotted += 1

        print(f"  {station} : {n_plotted} graphiques annuels -> {PLOT_DIR / freq / station}/")

print("\n✅ Terminé.")