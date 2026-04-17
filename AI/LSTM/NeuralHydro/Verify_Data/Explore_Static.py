"""
Exploration des attributs statiques — vérification des données
Focus sur les fractions CORINE et les textures de sol SoilGrids
"""

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

ATTRS_PATH = "./data/IA/NeuralHydrology/attributes/attributes.csv"

df = pd.read_csv(ATTRS_PATH, index_col='station_id')
print(f"Stations : {len(df)}")
print(f"Colonnes : {list(df.columns)}\n")

# ── Statistiques descriptives ─────────────────────────────────
print("=" * 60)
print("STATISTIQUES DESCRIPTIVES")
print("=" * 60)
print(df.describe().round(3).to_string())

# ── Vérifications de cohérence ────────────────────────────────
print("\n" + "=" * 60)
print("VÉRIFICATIONS DE COHÉRENCE")
print("=" * 60)

# 1 — Fractions CORINE doivent sommer à ~1
fracs = ['frac_urban', 'frac_agriculture', 'frac_forest',
         'frac_semi_natural', 'frac_wetland', 'frac_water']
df['sum_fracs'] = df[fracs].sum(axis=1)
pb_somme = df[np.abs(df['sum_fracs'] - 1) > 0.05]
print(f"\nFractions CORINE sommant à ±5% de 1 : {len(pb_somme)} stations OK")
if len(pb_somme) > 0:
    print(f"  ⚠️  {len(pb_somme)} stations avec somme anormale :")
    print(pb_somme['sum_fracs'].describe().round(3))

# 2 — Textures de sol doivent sommer à ~100%
df['sum_soil'] = df[['sg_clay_0_30cm', 'sg_sand_0_30cm', 'sg_silt_0_30cm']].sum(axis=1)
pb_soil = df[np.abs(df['sum_soil'] - 100) > 10]
print(f"\nTextures sol (clay+sand+silt) ≈ 100% : {len(df) - len(pb_soil)}/{len(df)} stations OK")
if len(pb_soil) > 0:
    print(f"  ⚠️  {len(pb_soil)} stations avec somme anormale :")
    print(df['sum_soil'].describe().round(1))

# 3 — Valeurs manquantes
print(f"\nValeurs manquantes par colonne :")
nulls = df.isnull().sum()
for col, n in nulls[nulls > 0].items():
    print(f"  ⚠️  {col} : {n} NaN")
if nulls.sum() == 0:
    print("  ✅ Aucune valeur manquante")

# 4 — Valeurs aberrantes
print(f"\nValeurs hors plage attendue :")
checks = {
    'aire_km2'         : (0, 50000),
    'frac_urban'       : (0, 1),
    'frac_forest'      : (0, 1),
    'sg_clay_0_30cm'   : (0, 80),
    'sg_sand_0_30cm'   : (0, 95),
    'sg_silt_0_30cm'   : (0, 80),
}
tout_ok = True
for col, (vmin, vmax) in checks.items():
    if col not in df.columns:
        continue
    hors = df[(df[col] < vmin) | (df[col] > vmax)]
    if len(hors) > 0:
        print(f"  ⚠️  {col} : {len(hors)} valeurs hors [{vmin}, {vmax}]")
        print(f"       min={df[col].min():.2f} max={df[col].max():.2f}")
        tout_ok = False
if tout_ok:
    print("  ✅ Toutes les valeurs dans les plages attendues")

# ── Visualisation ─────────────────────────────────────────────
fig, axes = plt.subplots(3, 4, figsize=(18, 12))
axes = axes.flatten()

cols_to_plot = [c for c in df.columns if c != 'sum_fracs' and c != 'sum_soil']

for i, col in enumerate(cols_to_plot[:12]):
    ax = axes[i]
    data = df[col].dropna()
    ax.hist(data, bins=30, color='steelblue', edgecolor='white', alpha=0.8)
    ax.axvline(data.mean(),   color='red',    ls='--', lw=1.5, label=f'mean={data.mean():.2f}')
    ax.axvline(data.median(), color='orange', ls='--', lw=1.5, label=f'med={data.median():.2f}')
    ax.set_title(col, fontsize=10, fontweight='bold')
    ax.set_xlabel('Valeur')
    ax.set_ylabel('Nb stations')
    ax.legend(fontsize=7)
    ax.grid(alpha=0.3)

# Masquer les axes inutilisés
for j in range(len(cols_to_plot), 12):
    axes[j].set_visible(False)

plt.suptitle('Distribution des attributs statiques', fontsize=14, fontweight='bold', y=1.01)
plt.tight_layout()
plt.savefig("static_attrs_distribution.png", dpi=150, bbox_inches='tight')
print(f"\n✅ Figure sauvegardée : static_attrs_distribution.png")

# ── Top 5 stations aberrantes ─────────────────────────────────
print("\n" + "=" * 60)
print("TOP 5 STATIONS PAR AIRE (les plus grandes)")
print("=" * 60)
print(df.nlargest(5, 'aire_km2')[['aire_km2'] + fracs].round(3).to_string())

print("\nTOP 5 STATIONS LES PLUS URBAINES")
print("=" * 60)
print(df.nlargest(5, 'frac_urban')[['frac_urban', 'frac_agriculture', 'frac_forest', 'aire_km2']].round(3).to_string())