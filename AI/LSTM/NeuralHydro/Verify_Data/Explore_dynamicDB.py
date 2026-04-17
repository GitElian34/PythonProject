"""
Exploration des attributs dynamiques depuis la BDD SQLite
Précipitation, température, PET — vérification sur 100 stations
"""

import sqlite3
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import os

DB_PATH    = './data/insitu_data.db'
OUTPUT_DIR = './data/IA/NeuralHydrology/Visualisation/'
os.makedirs(OUTPUT_DIR, exist_ok=True)
N_STATIONS = 100

# ── Chargement depuis la BDD ──────────────────────────────────
print(f"Chargement de {N_STATIONS} stations depuis la BDD...")

conn = sqlite3.connect(DB_PATH)

# Sélectionne 100 stations aléatoires avec suffisamment de données
stations = pd.read_sql(f'''
    SELECT code_sta
    FROM era5_bv_jour
    GROUP BY code_sta
    HAVING COUNT(*) >= 365
    ORDER BY RANDOM()
    LIMIT {N_STATIONS}
''', conn)

print(f"Stations trouvées : {len(stations)}")

# Charge toutes leurs données ERA5
codes = stations['code_sta'].tolist()
placeholders = ','.join(['?' for _ in codes])

df_all = pd.read_sql(f'''
    SELECT
        e.code_sta,
        e.mesure_date       AS date,
        e.precip_sum_bv     AS precipitation,
        e.temp_moy_bv       AS temperature,
        e.pet_sum_bv        AS pet
    FROM era5_bv_jour e
    WHERE e.code_sta IN ({placeholders})
    ORDER BY e.code_sta, e.mesure_date
''', conn, params=codes)

conn.close()

df_all['date'] = pd.to_datetime(df_all['date'])
print(f"Total lignes  : {len(df_all)}")
print(f"Période       : {df_all['date'].min().date()} → {df_all['date'].max().date()}\n")

# ── Statistiques descriptives ─────────────────────────────────
VARIABLES = ['precipitation', 'temperature', 'pet']
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
precip_mean = df_all['precipitation'].mean()
print(f"\nPrécipitation :")
print(f"  Valeurs négatives  : {precip_neg}  {'⚠️' if precip_neg > 0 else '✅'}")
print(f"  Max journalier     : {precip_max:.2f} mm  {'⚠️  suspect si >200mm' if precip_max > 200 else '✅'}")
print(f"  Moyenne journalière: {precip_mean:.2f} mm  (attendu: 1.5-3 mm/j en France)")
print(f"  Moy annuelle est.  : {precip_mean*365:.0f} mm/an  (attendu: 600-1500 mm/an)")
print(f"  % jours sans pluie : {(df_all['precipitation'] == 0).mean():.1%}  (attendu: 40-60%)")

# Température
temp_min = df_all['temperature'].min()
temp_max = df_all['temperature'].max()
temp_mean = df_all['temperature'].mean()
print(f"\nTempérature :")
print(f"  Min  : {temp_min:.1f}°C  {'⚠️  suspect si < -20°C' if temp_min < -20 else '✅'}")
print(f"  Max  : {temp_max:.1f}°C  {'⚠️  suspect si > 40°C'  if temp_max > 40  else '✅'}")
print(f"  Mean : {temp_mean:.1f}°C  (attendu: 10-14°C pour la France)")

# PET
pet_neg = (df_all['pet'] < 0).sum()
pet_max  = df_all['pet'].max()
pet_mean = df_all['pet'].mean()
print(f"\nPET :")
print(f"  Valeurs négatives  : {pet_neg}  {'⚠️' if pet_neg > 0 else '✅'}")
print(f"  Max journalier     : {pet_max:.2f} mm  {'⚠️  suspect si >15mm' if pet_max > 15 else '✅'}")
print(f"  Moyenne journalière: {pet_mean:.2f} mm/j")
print(f"  Moy annuelle est.  : {pet_mean*365:.0f} mm/an  (attendu: 500-900 mm/an)")

# NaN
print(f"\nNaN par variable :")
for col in VARIABLES:
    n = df_all[col].isna().sum()
    pct = n / len(df_all)
    print(f"  {'⚠️' if pct > 0.05 else '✅'} {col:15s} : {n} NaN ({pct:.1%})")

# ── Saisonnalité ──────────────────────────────────────────────
print("\n" + "=" * 60)
print("SAISONNALITÉ MOYENNE (toutes stations)")
print("=" * 60)
df_all['month'] = df_all['date'].dt.month
monthly = df_all.groupby('month')[VARIABLES].mean().round(2)
print(monthly.to_string())

# ── Stats par station ─────────────────────────────────────────
print("\n" + "=" * 60)
print("TOP 5 STATIONS AVEC PRÉCIP MAX SUSPECTE")
print("=" * 60)
stats_sta = df_all.groupby('code_sta')['precipitation'].agg(['max','mean','std']).round(2)
print(stats_sta.nlargest(5, 'max').to_string())

# ── Figure 1 : Distributions ──────────────────────────────────
fig, axes = plt.subplots(1, 3, figsize=(16, 5))

configs = [
    ('precipitation', 'Précipitation (mm/j)', 'steelblue'),
    ('temperature',   'Température (°C)',      'tomato'),
    ('pet',           'PET (mm/j)',            'forestgreen'),
]

for ax, (col, label, color) in zip(axes, configs):
    data = df_all[col].dropna()
    data = data[data >= 0] if col != 'temperature' else data
    ax.hist(data, bins=60, color=color, edgecolor='white', alpha=0.8)
    ax.axvline(data.mean(),   color='red',    ls='--', lw=1.5,
               label=f'mean={data.mean():.2f}')
    ax.axvline(data.median(), color='orange', ls='--', lw=1.5,
               label=f'med={data.median():.2f}')
    ax.set_title(label, fontsize=12, fontweight='bold')
    ax.set_xlabel('Valeur')
    ax.set_ylabel('Nb observations')
    ax.legend(fontsize=9)
    ax.grid(alpha=0.3)

plt.suptitle(f'Distribution des forçages ERA5 — {N_STATIONS} stations BDD',
             fontsize=13, fontweight='bold')
plt.tight_layout()
out1 = os.path.join(OUTPUT_DIR, "db_dynamic_distributions.png")
plt.savefig(out1, dpi=150, bbox_inches='tight')
print(f"\n✅ Figure 1 : {out1}")

# ── Figure 2 : Saisonnalité ───────────────────────────────────
fig, axes = plt.subplots(1, 3, figsize=(16, 5))
mois_labels = ['J','F','M','A','M','J','J','A','S','O','N','D']

for ax, (col, label, color) in zip(axes, configs):
    m_mean = df_all.groupby('month')[col].mean()
    m_std  = df_all.groupby('month')[col].std()
    ax.bar(range(1, 13), m_mean, color=color, alpha=0.7)
    ax.errorbar(range(1, 13), m_mean, yerr=m_std,
                fmt='none', color='black', capsize=3, alpha=0.5)
    ax.set_xticks(range(1, 13))
    ax.set_xticklabels(mois_labels)
    ax.set_title(label, fontsize=11, fontweight='bold')
    ax.set_xlabel('Mois')
    ax.grid(alpha=0.3, axis='y')

plt.suptitle(f'Saisonnalité ERA5 — {N_STATIONS} stations BDD',
             fontsize=13, fontweight='bold')
plt.tight_layout()
out2 = os.path.join(OUTPUT_DIR, "db_dynamic_seasonality.png")
plt.savefig(out2, dpi=150, bbox_inches='tight')
print(f"✅ Figure 2 : {out2}")

# ── Figure 3 : Série temporelle d'une station exemple ─────────
sample = codes[0]
df_sample = df_all[df_all['code_sta'] == sample].set_index('date')

fig, axes = plt.subplots(3, 1, figsize=(16, 10), sharex=True)
for ax, (col, label, color) in zip(axes, configs):
    ax.plot(df_sample.index, df_sample[col], color=color, lw=0.8, alpha=0.8)
    ax.set_ylabel(label, fontsize=9)
    ax.grid(alpha=0.3)

axes[0].set_title(f'Série temporelle — station {sample}',
                  fontsize=12, fontweight='bold')
plt.tight_layout()
out3 = os.path.join(OUTPUT_DIR, "db_dynamic_timeseries_example.png")
plt.savefig(out3, dpi=150, bbox_inches='tight')
print(f"✅ Figure 3 : {out3}")