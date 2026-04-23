"""
Génère les fichiers .nc pour TOUTES les stations disponibles dans la BDD.
Pas de split train/val — juste les séries temporelles + attributes.csv complet.
"""

import sqlite3
import pandas as pd
import numpy as np
import xarray as xr
import os

DB_PATH    = './data/insitu_data.db'
OUTPUT_DIR = './data/IA/NeuralHydrology/'
DATE_DEB   = '2016-01-01'
DATE_FIN   = '2025-12-31'

os.makedirs(os.path.join(OUTPUT_DIR, 'time_series'), exist_ok=True)
os.makedirs(os.path.join(OUTPUT_DIR, 'attributes'), exist_ok=True)

conn       = sqlite3.connect(DB_PATH)
date_range = pd.date_range(DATE_DEB, DATE_FIN, freq='D')

# ═══════════════════════════════════════════════════════════════
# TOUTES LES STATIONS AVEC DONNÉES ERA5 + CORINE
# ═══════════════════════════════════════════════════════════════
print("Sélection de toutes les stations disponibles...\n")

candidats = pd.read_sql('''
    SELECT DISTINCT e.code_sta
    FROM era5_bv_jour e
    JOIN mesures_insitu m  ON e.mesure_id = m.id
    JOIN bv_data b         ON e.code_sta = b.code_sta
    JOIN bv_corine c       ON e.code_sta = c.code_sta
    JOIN stations_insitu s ON e.code_sta = s.code_sta
    WHERE s.lon IS NOT NULL AND s.lat IS NOT NULL
    ORDER BY e.code_sta
''', conn)

print(f"{len(candidats)} stations candidates\n")

# ═══════════════════════════════════════════════════════════════
# EXPORT .NC
# ═══════════════════════════════════════════════════════════════
stations_ok = []
stations_skip = []

for i, (_, row) in enumerate(candidats.iterrows()):
    code_sta = row['code_sta']

    # Skip si .nc déjà présent
    nc_path = os.path.join(OUTPUT_DIR, 'time_series', f'{code_sta}.nc')
    if os.path.exists(nc_path):
        stations_ok.append(code_sta)
        continue

    df = pd.read_sql('''
        SELECT
            e.mesure_date   AS date,
            e.precip_sum_bv AS precipitation,
            e.temp_moy_bv   AS temperature,
            e.pet_sum_bv    AS pet,
            m.h_med_wsh     AS water_level
        FROM era5_bv_jour e
        JOIN mesures_insitu m ON e.mesure_id = m.id
        WHERE e.code_sta = ?
          AND e.mesure_date >= ? AND e.mesure_date <= ?
        ORDER BY e.mesure_date
    ''', conn, params=(code_sta, DATE_DEB, DATE_FIN))

    if df.empty:
        stations_skip.append(code_sta)
        continue

    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date').reindex(date_range)
    df.index.name = 'date'

    # Normalisation water_level
    wl_mean = df['water_level'].mean()
    wl_std  = df['water_level'].std()
    if pd.isna(wl_std) or wl_std <= 0:
        stations_skip.append(code_sta)
        continue

    df['water_level'] = (df['water_level'] - wl_mean) / wl_std

    # Export NetCDF
    data_vars = {col: xr.Variable("date", df[col].values.astype(np.float32))
                 for col in df.columns}
    ds = xr.Dataset(data_vars, coords={"date": df.index.values})
    ds.to_netcdf(nc_path, engine="scipy", format="NETCDF3_CLASSIC")

    stations_ok.append(code_sta)

    if (i + 1) % 100 == 0:
        print(f"  {i+1}/{len(candidats)} — {len(stations_ok)} exportées, {len(stations_skip)} ignorées")

print(f"\n{len(stations_ok)} stations exportées")
print(f"{len(stations_skip)} stations ignorées (données vides ou constantes)")

# ═══════════════════════════════════════════════════════════════
# ATTRIBUTES — toutes les stations ok
# ═══════════════════════════════════════════════════════════════
print("\nExport attributes.csv...")

placeholders = ','.join(['?' for _ in stations_ok])
attrs = pd.read_sql(f'''
    SELECT
        b.code_sta      AS station_id,
        b.aire_km2,
        s.lon,
        s.lat,
        s.dist_barrage_m,
        c.frac_urban,
        c.frac_agriculture,
        c.frac_forest,
        c.frac_semi_natural,
        c.frac_wetland,
        c.frac_water,
        c.sg_clay_0_30cm,
        c.sg_sand_0_30cm,
        c.sg_silt_0_30cm
    FROM bv_data b
    JOIN bv_corine c       ON b.code_sta = c.code_sta
    JOIN stations_insitu s ON b.code_sta = s.code_sta
    WHERE b.code_sta IN ({placeholders})
    ORDER BY b.code_sta
''', conn, params=stations_ok)

conn.close()

attrs.to_csv(os.path.join(OUTPUT_DIR, 'attributes', 'attributes.csv'), index=False)
print(f"✅ attributes.csv — {len(attrs)} stations")
print(f"\nPériode : {DATE_DEB} → {DATE_FIN}")