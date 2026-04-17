"""
Exploration des attributs dynamiques — vérification des données
Précipitation, température, PET et water_level
"""

import pandas as pd
import numpy as np
import xarray as xr
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import glob
import os

TIME_SERIES_DIR = "./data/IA/NeuralHydrology/time_series/"
OUTPUT_DIR      = "./data/IA/NeuralHydrology/Visualisation/"
os.makedirs(OUTPUT_DIR, exist_ok=True)

VARIABLES = ['precipitation', 'temperature', 'pet', 'water_level']

# ── Chargement de tous les fichiers NetCDF ────────────────────
nc_files = sorted(glob.glob(os.path.join(TIME_SERIES_DIR, "*.nc")))
print(f"Fichiers NetCDF trouvés : {len(nc_files)}")

all_data = []
for path in nc_files:
    station_id = os.path.basename(path).replace(".nc", "")
    try:
        ds = xr.open_dataset(path)
        df = ds.to_dataframe()
        df['station_id'] = station_id
        all_data.append(df)
    except Exception as e:
        print(f"  ⚠️  {station_id} — erreur lecture : {e}")

df_all = pd.concat(all_data)
print(f"Total lignes : {len(df_all)}")
print(f"Période      : {df_all.index.min()} → {df_all.index.max()}\n")

# ── Statistiques descriptives ─────────────────────────────────
print("=" * 60)
print("STATISTIQUES DESCRIPTIVES (toutes stations confondues)")
print("=" * 60)
print(df_all[VARIABLES].describe().round(3).to_string())

# ── Vérifications de cohérence ────────────────────────────────
print("\n" + "=" * 60)
print("VÉRIFICATIONS DE COHÉRENCE")
print("=" * 60)

# Précipitations
precip_neg = (df_all['precipitation'] < 0).sum()
precip_max = df_all['precipitation'].max()
print(f"\nPrécipitation :")
print(f"  Valeurs négatives     : {precip_neg}")
print(f"  Max journalier        : {precip_max:.1f} mm  {'⚠️  suspect si >300mm' if precip_max > 300 else '✅'}")
print(f"  % jours sans pluie    : {(df_all['precipitation'] == 0).mean():.1%}")

# Température
temp_min = df_all['temperature'].min()
temp_max = df_all['temperature'].max()
print(f"\nTempérature :")
print(f"  Min : {temp_min:.1f}°C  {'⚠️  suspect si < -20°C' if temp_min < -20 else '✅'}")
print(f"  Max : {temp_max:.1f}°C  {'⚠️  suspect si > 45°C'  if temp_max > 45  else '✅'}")

# PET
pet_neg  = (df_all['pet'] < 0).sum()
pet_max  = df_all['pet'].max()
print(f"\nPET :")
print(f"  Valeurs négatives     : {pet_neg}  {'⚠️  vérifier unités' if pet_neg > 0 else '✅'}")
print(f"  Max journalier        : {pet_max:.2f} mm  {'⚠️  suspect si >15mm' if pet_max > 15 else '✅'}")
print(f"  Moyenne annuelle est. : {df_all['pet'].mean() * 365:.0f} mm/an")

# Water level
wl_nan = df_all['water_level'].isna().sum()
wl_nan_pct = wl_nan / len(df_all)
print(f"\nWater level (normalisé) :")
print(f"  NaN : {wl_nan} ({wl_nan_pct:.1%}) {'⚠️  beaucoup de NaN' if wl_nan_pct > 0.3 else '✅'}")
print(f"  Min : {df_all['water_level'].min():.3f}")
print(f"  Max : {df_all['water_level'].max():.3f}")
print(f"  Std : {df_all['water_level'].std():.3f}  (attendu ~1.0 après normalisation)")

# NaN par variable
print(f"\nNaN par variable :")
for col in VARIABLES:
    n = df_all[col].isna().sum()
    pct = n / len(df_all)
    flag = '⚠️ ' if pct > 0.1 else '✅'
    print(f"  {flag} {col:20s} : {n:6d} NaN ({pct:.1%})")

# ── Saisonnalité ──────────────────────────────────────────────
print("\n" + "=" * 60)
print("SAISONNALITÉ MOYENNE (toutes stations)")
print("=" * 60)
df_all['month'] = pd.to_datetime(df_all.index).month
monthly = df_all.groupby('month')[['precipitation', 'temperature', 'pet']].mean().round(2)
print(monthly.to_string())

# ── Figure 1 : Distributions ─────────────────────────────────
fig, axes = plt.subplots(2, 2, figsize=(14, 10))
axes = axes.flatten()

configs = [
    ('precipitation', 'Précipitation (mm/j)', 'steelblue',  (0, None)),
    ('temperature',   'Température (°C)',      'tomato',     (None, None)),
    ('pet',           'PET (mm/j)',            'forestgreen',(0, None)),
    ('water_level',   'Water level (normalisé)','purple',    (None, None)),
]

for ax, (col, label, color, (vmin, vmax)) in zip(axes, configs):
    data = df_all[col].dropna()
    if vmin is not None:
        data = data[data >= vmin]
    if vmax is not None:
        data = data[data <= vmax]
    ax.hist(data, bins=50, color=color, edgecolor='white', alpha=0.8)
    ax.axvline(data.mean(),   color='red',    ls='--', lw=1.5,
               label=f'mean={data.mean():.2f}')
    ax.axvline(data.median(), color='orange', ls='--', lw=1.5,
               label=f'med={data.median():.2f}')
    ax.set_title(label, fontsize=12, fontweight='bold')
    ax.set_xlabel('Valeur')
    ax.set_ylabel('Nb observations')
    ax.legend(fontsize=9)
    ax.grid(alpha=0.3)

plt.suptitle('Distribution des variables dynamiques\n(toutes stations, toute la période)',
             fontsize=13, fontweight='bold')
plt.tight_layout()
out1 = os.path.join(OUTPUT_DIR, "dynamic_distributions.png")
plt.savefig(out1, dpi=150, bbox_inches='tight')
print(f"\n✅ Figure 1 sauvegardée : {out1}")

# ── Figure 2 : Saisonnalité ───────────────────────────────────
fig, axes = plt.subplots(1, 3, figsize=(16, 5))
mois_labels = ['J','F','M','A','M','J','J','A','S','O','N','D']

for ax, col, color, label in zip(
    axes,
    ['precipitation', 'temperature', 'pet'],
    ['steelblue', 'tomato', 'forestgreen'],
    ['Précipitation (mm/j)', 'Température (°C)', 'PET (mm/j)']
):
    monthly_mean = df_all.groupby('month')[col].mean()
    monthly_std  = df_all.groupby('month')[col].std()
    ax.bar(range(1, 13), monthly_mean, color=color, alpha=0.7, label='Moyenne')
    ax.errorbar(range(1, 13), monthly_mean, yerr=monthly_std,
                fmt='none', color='black', capsize=3, alpha=0.5)
    ax.set_xticks(range(1, 13))
    ax.set_xticklabels(mois_labels)
    ax.set_title(label, fontsize=11, fontweight='bold')
    ax.set_xlabel('Mois')
    ax.grid(alpha=0.3, axis='y')

plt.suptitle('Saisonnalité des forçages ERA5\n(moyenne ± std sur toutes stations)',
             fontsize=13, fontweight='bold')
plt.tight_layout()
out2 = os.path.join(OUTPUT_DIR, "dynamic_seasonality.png")
plt.savefig(out2, dpi=150, bbox_inches='tight')
print(f"✅ Figure 2 sauvegardée : {out2}")

# ── Figure 3 : Série temporelle d'une station exemple ────────
sample_station = os.path.basename(nc_files[0]).replace(".nc", "")
df_sample = df_all[df_all['station_id'] == sample_station].copy()

fig, axes = plt.subplots(4, 1, figsize=(16, 12), sharex=True)
for ax, (col, label, color) in zip(axes, [
    ('precipitation', 'Précipitation (mm/j)', 'steelblue'),
    ('temperature',   'Température (°C)',      'tomato'),
    ('pet',           'PET (mm/j)',            'forestgreen'),
    ('water_level',   'Water level (norm.)',   'purple'),
]):
    ax.plot(df_sample.index, df_sample[col], color=color, lw=0.8, alpha=0.8)
    ax.set_ylabel(label, fontsize=9)
    ax.grid(alpha=0.3)

axes[0].set_title(f'Série temporelle complète — station {sample_station}',
                  fontsize=12, fontweight='bold')
plt.tight_layout()
out3 = os.path.join(OUTPUT_DIR, "dynamic_timeseries_example.png")
plt.savefig(out3, dpi=150, bbox_inches='tight')
print(f"✅ Figure 3 sauvegardée : {out3}")