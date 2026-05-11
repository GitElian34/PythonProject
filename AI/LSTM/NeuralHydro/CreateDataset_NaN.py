"""
create_dataset_nan10j.py
═══════════════════════════════════════════════════════════════════════════
Dataset modèle 1 — NaN version
  - ERA5 : quotidien complet (precipitation, temperature, pet)
  - water_level : 1 valeur tous les 10j (décalages 0 à 9), reste NaN
  - 10 fichiers .nc par station : {code_sta}_d0.nc ... {code_sta}_d9.nc
  - Dossier : ./data/IA/NeuralHydrology_nan10j/

Logique des décalages :
  d=0 → mesures aux jours 0, 10, 20, 30, ...
  d=1 → mesures aux jours 1, 11, 21, 31, ...
  ...
  d=9 → mesures aux jours 9, 19, 29, 39, ...
═══════════════════════════════════════════════════════════════════════════
"""

import sqlite3
import pandas as pd
import numpy as np
import xarray as xr
import os
import random

# ─── Paramètres ─────────────────────────────────────────────────────────────
DB_PATH    = './data/insitu_data.db'
OUTPUT_DIR = './data/IA/NeuralHydrology_nan10j/'
BASINS_DIR = './AI/LSTM/NeuralHydro_nan10j/'
DATE_DEB   = '2016-01-01'
DATE_FIN   = '2025-12-31'

N_STATIONS    = 200
VAL_RATIO     = 0.2
SEED          = 42
MIN_GAP_JOURS = 60
MIN_DIST_M    = 500
FREQ_JOURS    = 10     # 1 mesure water_level tous les 10 jours

os.makedirs(os.path.join(OUTPUT_DIR, 'time_series'), exist_ok=True)
os.makedirs(os.path.join(OUTPUT_DIR, 'attributes'), exist_ok=True)
os.makedirs(BASINS_DIR, exist_ok=True)
random.seed(SEED)

conn       = sqlite3.connect(DB_PATH)
date_range = pd.date_range(DATE_DEB, DATE_FIN, freq='D')

# ═══════════════════════════════════════════════════════════════
# SÉLECTION DES STATIONS
# ═══════════════════════════════════════════════════════════════
print(f"Sélection des candidats (dist_barrage >= {MIN_DIST_M}m, gap <= {MIN_GAP_JOURS}j)...\n")

candidats = pd.read_sql('''
    SELECT DISTINCT e.code_sta
    FROM era5_bv_jour e
    JOIN mesures_insitu m  ON e.mesure_id = m.id
    JOIN bv_data b         ON e.code_sta = b.code_sta
    JOIN bv_corine c       ON e.code_sta = c.code_sta
    JOIN stations_insitu s ON e.code_sta = s.code_sta
    WHERE e.mesure_date >= ? AND e.mesure_date <= ?
      AND s.lon IS NOT NULL AND s.lat IS NOT NULL
      AND (s.gap_max_jours IS NULL OR s.gap_max_jours <= ?)
      AND (s.dist_barrage_m IS NULL OR s.dist_barrage_m >= ?)
    GROUP BY e.code_sta
    HAVING COUNT(DISTINCT e.mesure_date) >= 1000
       AND MIN(e.mesure_date) <= ?
       AND MAX(e.mesure_date) >= ?
''', conn, params=(DATE_DEB, DATE_FIN, MIN_GAP_JOURS, MIN_DIST_M, DATE_DEB, DATE_FIN))

print(f"{len(candidats)} candidats trouvés")

# Filtrer sur les .nc déjà générés (depuis le dataset original)
nc_existants = {
    f.replace('.nc', '')
    for f in os.listdir('./data/IA/NeuralHydrology/time_series')
    if f.endswith('.nc')
}
candidats = candidats[candidats['code_sta'].isin(nc_existants)].reset_index(drop=True)
print(f"{len(candidats)} candidats avec .nc disponible")

candidats = candidats.sample(frac=1, random_state=SEED).reset_index(drop=True)
stations_ok = candidats['code_sta'].tolist()[:N_STATIONS]
print(f"{len(stations_ok)} stations sélectionnées\n")

# ═══════════════════════════════════════════════════════════════
# EXPORT .NC — 10 fichiers par station (un par décalage)
# ═══════════════════════════════════════════════════════════════
print(f"Export .nc ({FREQ_JOURS} décalages par station)...\n")

stations_exportees = []   # station_ids finaux (avec suffixe _d0..._d9)
stations_skip      = []

for i, (_, row) in enumerate(candidats.iterrows()):
    code_sta = row['code_sta']

    # Charger les données quotidiennes depuis la BDD
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

    # Normalisation water_level (z-score sur toute la série)
    wl_mean = df['water_level'].mean()
    wl_std  = df['water_level'].std()
    if pd.isna(wl_std) or wl_std <= 0:
        stations_skip.append(code_sta)
        continue

    df['water_level'] = (df['water_level'] - wl_mean) / wl_std

    # Générer 1 .nc par décalage
    for decalage in range(FREQ_JOURS):
        station_id = f"{code_sta}_d{decalage}"
        nc_path    = os.path.join(OUTPUT_DIR, 'time_series', f'{station_id}.nc')

        if os.path.exists(nc_path):
            stations_exportees.append(station_id)
            continue

        df_out = df.copy()

        # Masquer water_level sauf aux positions du décalage
        # positions valides : decalage, decalage+10, decalage+20, ...
        mask_satellite = np.zeros(len(df_out), dtype=bool)
        mask_satellite[decalage::FREQ_JOURS] = True
        df_out.loc[~mask_satellite, 'water_level'] = np.nan

        # Export NetCDF
        # ERA5 reste quotidien complet — water_level est creux
        data_vars = {
            col: xr.Variable("date", df_out[col].values.astype(np.float32))
            for col in df_out.columns
        }
        ds = xr.Dataset(data_vars, coords={"date": df_out.index.values})
        ds.to_netcdf(nc_path, engine="scipy", format="NETCDF3_CLASSIC")
        stations_exportees.append(station_id)

    if (i + 1) % 50 == 0:
        print(f"  {i+1}/{len(candidats)} stations traitées "
              f"({len(stations_exportees)} .nc générés)")

conn_attrs = sqlite3.connect(DB_PATH)

print(f"\n{len(stations_exportees)} .nc générés")
print(f"{len(stations_skip)} stations ignorées")

# ═══════════════════════════════════════════════════════════════
# ATTRIBUTES — une ligne par station_id (code_sta_dX)
# Les valeurs statiques sont identiques pour les 10 décalages
# d'une même station — on duplique simplement la ligne
# ═══════════════════════════════════════════════════════════════
print("\nExport attributes.csv...")

# Récupérer les attributs pour les stations de base
stations_base = list({sid.rsplit('_d', 1)[0] for sid in stations_exportees})
placeholders  = ','.join(['?' for _ in stations_base])

attrs_base = pd.read_sql(f'''
    SELECT
        b.code_sta      AS station_id,
        b.aire_km2,
        s.lon,
        s.lat,
        s.dist_barrage_m,
        s.strahler,
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
''', conn_attrs, params=stations_base)
conn_attrs.close()

# Dupliquer chaque ligne pour les 10 décalages
rows = []
for _, row in attrs_base.iterrows():
    for decalage in range(FREQ_JOURS):
        new_row = row.copy()
        new_row['station_id'] = f"{row['station_id']}_d{decalage}"
        rows.append(new_row)

attrs_out = pd.DataFrame(rows).reset_index(drop=True)

# Ne garder que les station_id effectivement exportées
attrs_out = attrs_out[attrs_out['station_id'].isin(stations_exportees)]
attrs_out.to_csv(os.path.join(OUTPUT_DIR, 'attributes', 'attributes.csv'), index=False)
print(f"✅ attributes.csv — {len(attrs_out)} lignes ({len(stations_base)} stations × décalages)")

# ═══════════════════════════════════════════════════════════════
# TRAIN / VAL SPLIT — 80/20 au niveau station de base
# On évite que les décalages d'une même station se retrouvent
# à la fois en train et en val (fuite d'information)
# ═══════════════════════════════════════════════════════════════
print("\nSplit train/val...")

random.shuffle(stations_base)
n_val   = max(1, int(len(stations_base) * VAL_RATIO))
n_train = len(stations_base) - n_val

train_base = stations_base[:n_train]
val_base   = stations_base[n_train:]

# Étendre aux 10 décalages
train_ids = [f"{s}_d{d}" for s in train_base for d in range(FREQ_JOURS)
             if f"{s}_d{d}" in set(stations_exportees)]
val_ids   = [f"{s}_d{d}" for s in val_base   for d in range(FREQ_JOURS)
             if f"{s}_d{d}" in set(stations_exportees)]

with open(os.path.join(BASINS_DIR, 'train_basins.txt'), 'w') as f:
    f.write('\n'.join(train_ids))
with open(os.path.join(BASINS_DIR, 'val_basins.txt'), 'w') as f:
    f.write('\n'.join(val_ids))

print(f"✅ train_basins.txt — {len(train_ids)} ids "
      f"({len(train_base)} stations × {FREQ_JOURS} décalages)")
print(f"✅ val_basins.txt   — {len(val_ids)} ids "
      f"({len(val_base)} stations × {FREQ_JOURS} décalages)")
print(f"\nPériode : {DATE_DEB} → {DATE_FIN}")
print(f"Dossier : {OUTPUT_DIR}")