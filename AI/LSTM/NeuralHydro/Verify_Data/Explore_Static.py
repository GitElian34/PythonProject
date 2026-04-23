"""
Analyse des attributs statiques (aire_km2, CORINE, sol)
pour les 20 meilleures et 20 pires stations.
"""

import sqlite3
import pandas as pd
import numpy as np

DB_PATH = "./data/insitu_data.db"

TOP20 = [
    "O787401001", "O303521001", "M322301010", "P613402001", "M010401010",
    "M814401010", "J341303001", "M351401010", "K212301002", "P821501001",
    "O504251002", "O709401002", "Y210002001", "J360181001", "H703301001",
    "L056301001", "A455000201", "M038401020", "H030101001", "A133003001",
]

FLOP20 = [
    "Y047403001", "Y503201001", "Y046600501", "O546431001", "P246401001",
    "V343401001", "A243003001", "J701063001", "Y067406001", "U234502001",
    "K457221001", "H760201001", "O723403001", "J321302002", "Y551404001",
    "U221502001", "K640252001", "K437311001", "A623201001", "K035631001",
]

all_stations = TOP20 + FLOP20

# ── Chargement des attributs ───────────────────────────────────
conn = sqlite3.connect(DB_PATH)
ph   = ','.join(['?' for _ in all_stations])
df   = pd.read_sql(f'''
    SELECT b.code_sta AS station_id,
           b.aire_km2,
           s.lon, s.lat,
           c.frac_urban, c.frac_agriculture, c.frac_forest,
           c.frac_semi_natural, c.frac_wetland, c.frac_water,
           c.sg_clay_0_30cm, c.sg_sand_0_30cm, c.sg_silt_0_30cm
    FROM bv_data b
    JOIN bv_corine c       ON b.code_sta = c.code_sta
    JOIN stations_insitu s ON b.code_sta = s.code_sta
    WHERE b.code_sta IN ({ph})
''', conn, params=all_stations)
conn.close()

df['groupe'] = df['station_id'].apply(lambda x: 'TOP' if x in TOP20 else 'FLOP')

# ── Variables à analyser ───────────────────────────────────────
VARS = [
    'aire_km2',
    'lon', 'lat',
    'frac_urban', 'frac_agriculture', 'frac_forest',
    'frac_semi_natural', 'frac_wetland', 'frac_water',
    'sg_clay_0_30cm', 'sg_sand_0_30cm', 'sg_silt_0_30cm',
]

# ── Tableau comparatif ─────────────────────────────────────────
print("=" * 75)
print("ANALYSE ATTRIBUTS STATIQUES — TOP 20 vs FLOP 20")
print("=" * 75)
print(f"{'Variable':<22} {'TOP médiane':>12} {'FLOP médiane':>13} {'TOP moy':>10} {'FLOP moy':>10} {'Δ moy':>8}")
print("-" * 75)

top_df  = df[df['groupe'] == 'TOP']
flop_df = df[df['groupe'] == 'FLOP']

for var in VARS:
    if var not in df.columns:
        continue
    t_med = top_df[var].median()
    f_med = flop_df[var].median()
    t_moy = top_df[var].mean()
    f_moy = flop_df[var].mean()
    delta = t_moy - f_moy

    # Marquer les différences notables (>20% de la plage)
    flag = " ◄" if abs(delta) > 0.05 * max(abs(t_moy), abs(f_moy), 1) else ""

    print(f"  {var:<20} {t_med:>12.3f} {f_med:>13.3f} {t_moy:>10.3f} {f_moy:>10.3f} {delta:>+8.3f}{flag}")

# ── Distribution aire_km2 ──────────────────────────────────────
print(f"\n{'─'*55}")
print("DISTRIBUTION aire_km2")
print(f"{'─'*55}")
for groupe, gdf in df.groupby('groupe'):
    q = gdf['aire_km2'].quantile([0.25, 0.5, 0.75])
    print(f"  {groupe:<5} : min={gdf['aire_km2'].min():>8.0f}  "
          f"Q25={q[0.25]:>8.0f}  med={q[0.50]:>8.0f}  "
          f"Q75={q[0.75]:>8.0f}  max={gdf['aire_km2'].max():>8.0f}")

# ── Répartition géographique (lat/lon) ────────────────────────
print(f"\n{'─'*55}")
print("RÉPARTITION GÉOGRAPHIQUE")
print(f"{'─'*55}")
for groupe, gdf in df.groupby('groupe'):
    print(f"  {groupe:<5} : lon [{gdf['lon'].min():.1f} → {gdf['lon'].max():.1f}]  "
          f"lat [{gdf['lat'].min():.1f} → {gdf['lat'].max():.1f}]")

# ── Dominante occupation du sol ───────────────────────────────
print(f"\n{'─'*55}")
print("OCCUPATION DU SOL DOMINANTE (par station)")
print(f"{'─'*55}")
corine_cols = ['frac_urban', 'frac_agriculture', 'frac_forest',
               'frac_semi_natural', 'frac_wetland', 'frac_water']

for groupe in ['TOP', 'FLOP']:
    gdf = df[df['groupe'] == groupe]
    dominant = gdf[corine_cols].idxmax(axis=1).str.replace('frac_', '')
    print(f"  {groupe:<5} : {dominant.value_counts().to_dict()}")