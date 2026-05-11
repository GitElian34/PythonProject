"""
analyse_outliers_inputs.py
═══════════════════════════════════════════════════════════════════════════
Pour une station donnée, affiche les données d'entrée du modèle à chaque
date outlier. Permet de comprendre si l'outlier vient d'une donnée
d'entrée aberrante ou d'une vraie erreur du modèle.

Usage :
  python analyse_outliers_inputs.py 0000000006326
  python analyse_outliers_inputs.py 0000000005729
═══════════════════════════════════════════════════════════════════════════
"""

import sys
import pandas as pd
import numpy as np
import xarray as xr
from pathlib import Path

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
MODEL = "arlstm_feat10jLow_modele2_0605_140952"
CSV_PATH = Path("./data/outlier_detection/residuals_all_stations.csv")
NC_DIR = Path("./data/IA/NeuralHydrology_satellite_10D/time_series")
ATTRS_CSV = Path("./data/IA/NeuralHydrology_satellite_10D/attributes/attributes.csv")

# Variables dynamiques utilisées par le modèle
DYN_VARS = [
    "precipitation_J0", "temperature_J0", "pet_J0",
    "precip_mean_J3", "pet_mean_J3", "temp_mean_J3",
    "precip_mean_J10", "temp_mean_J10",
    "clim_mean_20j", "clim_std_20j",
    "water_level",
]

# ═══════════════════════════════════════════════════════════════
# Argument
# ═══════════════════════════════════════════════════════════════
if len(sys.argv) < 2:
    print("Usage: python analyse_outliers_inputs.py <station_id>")
    print("Ex:    python analyse_outliers_inputs.py 0000000006326")
    sys.exit(1)

station = sys.argv[1]

# ═══════════════════════════════════════════════════════════════
# 1. Charger les résidus
# ═══════════════════════════════════════════════════════════════
df = pd.read_csv(CSV_PATH, parse_dates=['date'])
df['station'] = df['station'].astype(str)
df_sta = df[df['station'] == station].sort_values('date')

if len(df_sta) == 0:
    print(f"❌ Station {station} non trouvée dans les résidus.")
    print(f"   Stations dispo : {sorted(df['station'].unique())[:10]}...")
    sys.exit(1)

outliers = df_sta[df_sta['is_outlier'] == True]
print(f"{'='*80}")
print(f"ANALYSE OUTLIERS — Station {station}")
print(f"{'='*80}")
print(f"  {len(df_sta)} observations, {len(outliers)} outliers (seuil 3σ)")

if len(outliers) == 0:
    print("  Aucun outlier pour cette station.")
    sys.exit(0)

# ═══════════════════════════════════════════════════════════════
# 2. Charger le .nc
# ═══════════════════════════════════════════════════════════════
nc_path = NC_DIR / f"{station}.nc"
if not nc_path.exists():
    # Essayer avec padding à 13 chars (format HydroWeb)
    nc_path = NC_DIR / f"{station.zfill(13)}.nc"
if not nc_path.exists():
    # Essayer en strippant les zéros
    nc_path = NC_DIR / f"{station.lstrip('0')}.nc"
if not nc_path.exists():
    print(f"❌ Fichier .nc introuvable pour {station}")
    sys.exit(1)

ds = xr.open_dataset(nc_path)
dates_nc = pd.to_datetime(ds.date.values)

# ═══════════════════════════════════════════════════════════════
# 3. Charger les attributs statiques
# ═══════════════════════════════════════════════════════════════
if ATTRS_CSV.exists():
    df_attrs = pd.read_csv(ATTRS_CSV)
    df_attrs['station_id'] = df_attrs['station_id'].astype(str)
    row_attr = df_attrs[df_attrs['station_id'] == station]
    if len(row_attr) > 0:
        print(f"\n--- Attributs statiques ---")
        for col in row_attr.columns:
            if col != 'station_id':
                print(f"  {col:25s} : {row_attr[col].values[0]}")

# ═══════════════════════════════════════════════════════════════
# 4. Statistiques de référence par variable (pour détecter les anomalies)
# ═══════════════════════════════════════════════════════════════
stats = {}
for var in DYN_VARS:
    if var in ds:
        vals = ds[var].values
        vals_clean = vals[~np.isnan(vals)]
        if len(vals_clean) > 0:
            stats[var] = {
                'mean': np.mean(vals_clean),
                'std': np.std(vals_clean),
                'p5': np.percentile(vals_clean, 5),
                'p95': np.percentile(vals_clean, 95),
            }

# ═══════════════════════════════════════════════════════════════
# 5. Affichage par outlier
# ═══════════════════════════════════════════════════════════════
for idx, (_, row) in enumerate(outliers.iterrows()):
    date = row['date']
    print(f"\n{'─'*80}")
    print(f"  OUTLIER {idx+1}/{len(outliers)}  —  {date.strftime('%Y-%m-%d')}")
    print(f"{'─'*80}")
    print(f"  Obs       : {row['obs']:+.4f}")
    print(f"  Prédit    : {row['pred']:+.4f}")
    print(f"  Résidu    : {row['residual']:+.4f}")
    print(f"  Résidu σ  : {row['residual_norm']:+.2f}σ")

    # Trouver la date la plus proche dans le .nc
    idx_nc = np.argmin(np.abs(dates_nc - date))
    date_nc = dates_nc[idx_nc]
    if abs((date_nc - date).days) > 5:
        print(f"  ⚠️  Date .nc la plus proche : {date_nc} (écart {abs((date_nc - date).days)}j)")

    # Valeur au pas précédent (water_level_shift1)
    if idx_nc > 0:
        wl_prev = ds['water_level'].values[idx_nc - 1]
        print(f"\n  water_level_shift1 (t-1) : {wl_prev:+.4f}" +
              (" ← NaN!" if np.isnan(wl_prev) else ""))

    print(f"\n  {'Variable':25s} | {'Valeur':>10s} | {'Moy sta':>10s} | {'Std sta':>10s} | {'Zscore':>8s} | {'Flag':>5s}")
    print(f"  {'-'*75}")

    for var in DYN_VARS:
        if var not in ds:
            continue
        val = float(ds[var].values[idx_nc])
        if var in stats and not np.isnan(val):
            s = stats[var]
            zscore = (val - s['mean']) / s['std'] if s['std'] > 0 else 0
            flag = "⚠️" if abs(zscore) > 2 else "❌" if np.isnan(val) else ""
            print(f"  {var:25s} | {val:10.4f} | {s['mean']:10.4f} | {s['std']:10.4f} | {zscore:+8.2f} | {flag}")
        elif np.isnan(val):
            print(f"  {var:25s} | {'NaN':>10s} | {'':>10s} | {'':>10s} | {'':>8s} | ❌ NaN")
        else:
            print(f"  {var:25s} | {val:10.4f} |")

ds.close()

print(f"\n{'='*80}")
print(f"Légende : ⚠️ = zscore > 2σ (valeur extrême pour cette station)")
print(f"          ❌ = NaN")
print(f"{'='*80}")