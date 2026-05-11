"""
Détection automatique des stations "plates" — signal water_level quasi-constant
sur la période train.

Critères combinés :
  - IQR très faible (Q75-Q25 trop petit) → signal sans variation centrale
  - range P5-P95 faible
  - Pas de spike compensateur (sinon ce serait du "spike" pas du "plat")

Une série normalisée z-score a typiquement :
  - IQR ≈ 1.3 (gaussien)
  - range P5-P95 ≈ 3.3 (gaussien)
"""

import sqlite3
import pandas as pd
import numpy as np
import xarray as xr
from pathlib import Path
import matplotlib.pyplot as plt

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
DB_PATH     = "./data/insitu_data.db"
NC_DIR      = Path("./data/IA/NeuralHydrology_feat10j/time_series")
TRAIN_START = "2016-01-01"
TRAIN_END   = "2023-12-31"

# Seuils pour qualifier une série de "plate"
SEUIL_IQR        = 0.3    # IQR < 0.3 → quasi-plat (vs 1.3 normal)
SEUIL_RANGE_P95  = 1.0    # range P5-P95 < 1.0 → peu de variation (vs 3.3 normal)

OUT_CSV = "./stations_suspectes_plat.csv"
OUT_FIG = "./distribution_plat_detection.png"

# ═══════════════════════════════════════════════════════════════
# Charger toutes les stations disponibles
# ═══════════════════════════════════════════════════════════════
conn = sqlite3.connect(DB_PATH)
stations = pd.read_sql('SELECT code_sta FROM stations_insitu', conn)
conn.close()

print(f"Stations à analyser : {len(stations)}")

# ═══════════════════════════════════════════════════════════════
# Calculer les métriques de variabilité sur train
# ═══════════════════════════════════════════════════════════════
results = []
for sid in stations["code_sta"]:
    nc_path = NC_DIR / f"{sid}_d0.nc"
    if not nc_path.exists():
        continue
    try:
        ds = xr.open_dataset(nc_path)
        wl = ds.sel(date=slice(TRAIN_START, TRAIN_END))["water_level"].values
        ds.close()

        valid = wl[~np.isnan(wl)]
        if len(valid) < 100:
            continue

        std = valid.std()
        q25, q75 = np.percentile(valid, [25, 75])
        p05, p95 = np.percentile(valid, [5, 95])
        iqr      = q75 - q25
        range_p  = p95 - p05

        results.append({
            "station": sid,
            "n_pts": len(valid),
            "std": std,
            "iqr": iqr,
            "range_p5_p95": range_p,
            "min": valid.min(),
            "max": valid.max(),
        })
    except Exception:
        continue

df = pd.DataFrame(results)

# ═══════════════════════════════════════════════════════════════
# Identifier les stations plates
# ═══════════════════════════════════════════════════════════════
plates = df[
    (df["iqr"] < SEUIL_IQR) &
    (df["range_p5_p95"] < SEUIL_RANGE_P95)
].sort_values("iqr").copy()

print(f"\n{'='*70}")
print(f"DISTRIBUTION DES MÉTRIQUES DE VARIABILITÉ")
print(f"{'='*70}")
print(df[["iqr", "range_p5_p95"]].describe().round(3))

print(f"\n{'='*70}")
print(f"STATIONS PLATES (IQR < {SEUIL_IQR} ET range P5-P95 < {SEUIL_RANGE_P95})")
print(f"{'='*70}")
print(f"  Total : {len(plates)} sur {len(df)} ({len(plates)/len(df)*100:.1f}%)")
print()
print(plates.head(30).to_string(index=False))

# Sauvegarde
plates[["station"]].to_csv(OUT_CSV, index=False)
print(f"\n✅ Liste sauvegardée : {OUT_CSV} ({len(plates)} stations)")

# ═══════════════════════════════════════════════════════════════
# Visualisation
# ═══════════════════════════════════════════════════════════════
fig, axes = plt.subplots(1, 2, figsize=(13, 5))

# Distribution IQR
axes[0].hist(df[df["iqr"] < 3]["iqr"], bins=80, color="steelblue", edgecolor="white")
axes[0].axvline(SEUIL_IQR, color="red", ls="--", lw=2, label=f"Seuil IQR={SEUIL_IQR}")
axes[0].axvline(1.3, color="green", ls=":", lw=1, label="Gaussien (~1.3)")
axes[0].set_xlabel("IQR (Q75-Q25)")
axes[0].set_ylabel("Nb stations")
axes[0].set_title("Distribution IQR train\n(gauche = série plate)")
axes[0].legend()

# Scatter IQR vs range
axes[1].scatter(df["iqr"], df["range_p5_p95"], alpha=0.4, s=15, color="purple")
axes[1].axvline(SEUIL_IQR,        color="red", ls="--", label=f"IQR < {SEUIL_IQR}")
axes[1].axhline(SEUIL_RANGE_P95,  color="red", ls="--", label=f"range < {SEUIL_RANGE_P95}")
axes[1].set_xlabel("IQR")
axes[1].set_ylabel("Range P5-P95")
axes[1].set_title("Stations plates = bas-gauche")
axes[1].set_xlim(0, 3)
axes[1].set_ylim(0, 8)
axes[1].legend()
axes[1].grid(True, alpha=0.3)

plt.tight_layout()
plt.savefig(OUT_FIG, dpi=120)
print(f"✅ Figure : {OUT_FIG}")