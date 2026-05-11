"""
Détection automatique des stations "plates avec spike".
Critère : ratio std / IQR > seuil sur la période train.

Une série normale a std/IQR ≈ 0.77 (loi gaussienne).
Une série plate + spike a std/IQR >> 1 car le spike gonfle std mais pas IQR.
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
SEUIL_RATIO = 3.0   # std/IQR > 3 → suspect

# ═══════════════════════════════════════════════════════════════
# Charger toutes les stations disponibles
# ═══════════════════════════════════════════════════════════════
conn = sqlite3.connect(DB_PATH)
stations = pd.read_sql('''
    SELECT code_sta FROM stations_insitu
''', conn)
conn.close()

print(f"Stations à analyser : {len(stations)}")

# ═══════════════════════════════════════════════════════════════
# Calculer std/IQR sur train pour chaque station (sur _d0 suffit)
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
        iqr = q75 - q25

        if iqr < 1e-6:
            ratio = np.inf
        else:
            ratio = std / iqr

        # Spike score : combien de sigmas pour la valeur la plus extrême
        spike_score = max(abs(valid.max() - valid.mean()),
                          abs(valid.min() - valid.mean())) / std if std > 0 else 0

        results.append({
            "station": sid,
            "n_pts": len(valid),
            "std": std,
            "iqr": iqr,
            "ratio_std_iqr": ratio,
            "spike_max_sigmas": spike_score,
            "min": valid.min(),
            "max": valid.max(),
        })
    except Exception:
        continue

df = pd.DataFrame(results)

# ═══════════════════════════════════════════════════════════════
# Identifier les stations suspectes
# ═══════════════════════════════════════════════════════════════
suspectes = df[df["ratio_std_iqr"] > SEUIL_RATIO].sort_values("ratio_std_iqr", ascending=False)

print(f"\n{'='*70}")
print(f"DISTRIBUTION DU RATIO std/IQR")
print(f"{'='*70}")
print(df["ratio_std_iqr"].describe().round(3))

print(f"\n{'='*70}")
print(f"STATIONS SUSPECTES (ratio std/IQR > {SEUIL_RATIO})")
print(f"{'='*70}")
print(f"  Total : {len(suspectes)} sur {len(df)} ({len(suspectes)/len(df)*100:.1f}%)")
print()
print(suspectes.head(20).to_string(index=False))

# Sauvegarder la liste pour usage ultérieur
suspectes[["station"]].to_csv("./stations_suspectes_spike.csv", index=False)
print(f"\n✅ Liste sauvegardée : ./stations_suspectes_spike.csv ({len(suspectes)} stations)")

# ═══════════════════════════════════════════════════════════════
# Histogramme du ratio
# ═══════════════════════════════════════════════════════════════
fig, axes = plt.subplots(1, 2, figsize=(13, 5))

# Distribution
axes[0].hist(df[df["ratio_std_iqr"] < 10]["ratio_std_iqr"], bins=80,
             color="steelblue", edgecolor="white")
axes[0].axvline(SEUIL_RATIO, color="red", ls="--", lw=2, label=f"Seuil={SEUIL_RATIO}")
axes[0].axvline(0.77, color="green", ls=":", lw=1, label="Gaussien (0.77)")
axes[0].set_xlabel("ratio std / IQR")
axes[0].set_ylabel("Nb stations")
axes[0].set_title("Distribution du ratio std/IQR\n(gauche=normal, droite=spike)")
axes[0].legend()
axes[0].set_xlim(0, 10)

# Scatter ratio vs spike_max_sigmas
axes[1].scatter(df["ratio_std_iqr"], df["spike_max_sigmas"],
                alpha=0.4, s=15, color="purple")
axes[1].axvline(SEUIL_RATIO, color="red", ls="--")
axes[1].set_xlabel("ratio std / IQR")
axes[1].set_ylabel("Spike max (en sigmas)")
axes[1].set_title("Plus le ratio est haut, plus le spike domine")
axes[1].set_xscale("log")
axes[1].set_yscale("log")

plt.tight_layout()
plt.savefig("./distribution_spike_detection.png", dpi=120)
print(f"✅ Figure : ./distribution_spike_detection.png")