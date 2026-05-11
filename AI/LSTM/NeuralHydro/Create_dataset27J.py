"""
create_dataset_feat27j.py — VERSION 4 (27 jours)
═══════════════════════════════════════════════════════════════════════════
Identique à create_dataset_feat10j_v4.py mais avec un pas de 27 jours.
Adapté pour les stations satellite à fréquence ~27j.

Différences vs feat10j :
  - FREQ_JOURS = 27 (27 décalages _d0 à _d26)
  - Moyennes glissantes J27 au lieu de J10
  - Dossier de sortie : ./data/IA/NeuralHydrology_feat27j/
  - Variables : precip_mean_J27, temp_mean_J27, pet_mean_J27, etc.

Usage :
  python create_dataset_feat27j.py
═══════════════════════════════════════════════════════════════════════════
"""

import sqlite3
import pandas as pd
import numpy as np
import xarray as xr
import os
import random
import shutil

# ─── Paramètres ─────────────────────────────────────────────────────────────
DB_PATH    = './data/insitu_data.db'
OUTPUT_DIR = './data/IA/NeuralHydrology_feat27j/'
BASINS_DIR = './AI/LSTM/NeuralHydro_feat27j/'
DATE_DEB   = '2016-01-01'
DATE_FIN   = '2025-12-31'

N_STATIONS    = 2000
VAL_RATIO     = 0.2
SEED          = 42
MIN_GAP_JOURS = 60
MIN_DIST_M    = 500
FREQ_JOURS    = 27      # ← 27 jours au lieu de 10
RESET         = True
CLIM_WINDOW   = 20

os.makedirs(os.path.join(OUTPUT_DIR, 'time_series'), exist_ok=True)
os.makedirs(os.path.join(OUTPUT_DIR, 'attributes'), exist_ok=True)
os.makedirs(BASINS_DIR, exist_ok=True)
random.seed(SEED)

# Reset complet si demandé
if RESET:
    ts_dir = os.path.join(OUTPUT_DIR, 'time_series')
    n_old = len(os.listdir(ts_dir))
    if n_old > 0:
        print(f"🗑️  Suppression de {n_old} anciens .nc...")
        shutil.rmtree(ts_dir)
        os.makedirs(ts_dir)

conn       = sqlite3.connect(DB_PATH)
date_range = pd.date_range(DATE_DEB, DATE_FIN, freq='D')

# ═══════════════════════════════════════════════════════════════
# CHARGEMENT DE LA CLIMATOLOGIE (table climatologie_wl)
# ═══════════════════════════════════════════════════════════════
print("Chargement de la climatologie...")

clim_all = pd.read_sql_query('''
    SELECT code_sta, doy, wl_mean, wl_std
    FROM climatologie_wl
    ORDER BY code_sta, doy
''', conn)

clim_dict = {}
for code, grp in clim_all.groupby('code_sta'):
    clim_dict[code] = grp.set_index('doy')[['wl_mean', 'wl_std']]

print(f"  {len(clim_dict)} stations avec climatologie chargée\n")


# ═══════════════════════════════════════════════════════════════
# FONCTION : CLIMATOLOGIE FENÊTRÉE ±20j (excluant année courante)
# ═══════════════════════════════════════════════════════════════
def compute_clim_fenetre(wl_series, window=CLIM_WINDOW):
    """
    Pour chaque date, calcule mean/std du water_level normalisé sur ±window jours
    autour du DOY, en excluant l'année courante (leave-one-year-out).
    """
    wl_valid = wl_series.dropna()

    if len(wl_valid) < 30:
        n = len(wl_series)
        return np.zeros(n, dtype=np.float32), np.ones(n, dtype=np.float32)

    valid_doys  = np.clip(wl_valid.index.dayofyear, 1, 365)
    valid_years = wl_valid.index.year
    valid_vals  = wl_valid.values

    all_dates = wl_series.index
    all_doys  = np.clip(all_dates.dayofyear, 1, 365)
    all_years = all_dates.year

    clim_mean = np.zeros(len(all_dates), dtype=np.float32)
    clim_std  = np.ones(len(all_dates), dtype=np.float32)

    for i in range(len(all_dates)):
        cur_doy  = all_doys[i]
        cur_year = all_years[i]

        doy_diff = np.abs(valid_doys - cur_doy)
        doy_diff = np.minimum(doy_diff, 365 - doy_diff)

        mask = (doy_diff <= window) & (valid_years != cur_year)
        vals_in_window = valid_vals[mask]

        if len(vals_in_window) >= 3:
            clim_mean[i] = vals_in_window.mean()
            clim_std[i]  = vals_in_window.std()
            if clim_std[i] < 0.01:
                clim_std[i] = 1.0
        else:
            clim_mean[i] = 0.0
            clim_std[i]  = 1.0

    return clim_mean, clim_std


# ═══════════════════════════════════════════════════════════════
# SÉLECTION DES STATIONS — exclut les flaggées
# ═══════════════════════════════════════════════════════════════
print(f"Sélection des candidats (dist_barrage >= {MIN_DIST_M}m, gap <= {MIN_GAP_JOURS}j, "
      f"flag_capteur NULL)...\n")

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
      AND s.flag_capteur IS NULL
    GROUP BY e.code_sta
    HAVING COUNT(DISTINCT e.mesure_date) >= 1000
       AND MIN(e.mesure_date) <= ?
       AND MAX(e.mesure_date) >= ?
''', conn, params=(DATE_DEB, DATE_FIN, MIN_GAP_JOURS, MIN_DIST_M, DATE_DEB, DATE_FIN))

print(f"{len(candidats)} candidats trouvés (sans stations flaggées)")

nc_existants = {
    f.replace('.nc', '')
    for f in os.listdir('./data/IA/NeuralHydrology/time_series')
    if f.endswith('.nc')
}
candidats = candidats[candidats['code_sta'].isin(nc_existants)].reset_index(drop=True)
print(f"{len(candidats)} candidats avec .nc disponible")

candidats = candidats.sample(frac=1, random_state=SEED).reset_index(drop=True)
candidats = candidats.head(N_STATIONS)
print(f"{len(candidats)} stations sélectionnées\n")

# ═══════════════════════════════════════════════════════════════
# EXPORT .NC — features agrégées avec moyennes J3 et J27
# ═══════════════════════════════════════════════════════════════
print(f"Export .nc (features agrégées + neige + clim + clim_20j + doy, "
      f"{FREQ_JOURS} décalages par station)...\n")

stations_exportees = []
stations_skip      = []

for i, (_, row) in enumerate(candidats.iterrows()):
    code_sta = row['code_sta']

    df = pd.read_sql('''
        SELECT
            e.mesure_date     AS date,
            e.precip_sum_bv   AS precipitation,
            e.temp_moy_bv     AS temperature,
            e.pet_sum_bv      AS pet,
            sn.snow_depth_bv  AS snow_depth,
            sn.snowmelt_bv    AS snowmelt,
            m.h_med_wsh       AS water_level
        FROM era5_bv_jour e
        JOIN mesures_insitu m ON e.mesure_id = m.id
        LEFT JOIN era5_snow_bv_jour sn ON e.mesure_id = sn.mesure_id
        WHERE e.code_sta = ?
          AND e.mesure_date >= ? AND e.mesure_date <= ?
        ORDER BY e.mesure_date
    ''', conn, params=(code_sta, DATE_DEB, DATE_FIN))

    if df.empty:
        stations_skip.append((code_sta, "pas de données ERA5"))
        continue

    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date').reindex(date_range)
    df.index.name = 'date'

    # ── Normalisation water_level ───────────────────────────────────────────
    wl_mean = df['water_level'].mean()
    wl_std  = df['water_level'].std()
    if pd.isna(wl_std) or wl_std <= 0:
        stations_skip.append((code_sta, "std water_level nul"))
        continue
    df['water_level'] = (df['water_level'] - wl_mean) / wl_std

    # ── Remplir les NaN neige par 0 ─────────────────────────────────────────
    df['snow_depth'] = df['snow_depth'].fillna(0)
    df['snowmelt']   = df['snowmelt'].fillna(0)

    # ── Features agrégées ───────────────────────────────────────────────────
    # Moyenne glissante 3j
    df['precip_mean_J3']     = df['precipitation'].rolling(3,  min_periods=1).mean()
    df['temp_mean_J3']       = df['temperature'].rolling(3,    min_periods=1).mean()
    df['pet_mean_J3']        = df['pet'].rolling(3,            min_periods=1).mean()
    df['snow_depth_mean_J3'] = df['snow_depth'].rolling(3,     min_periods=1).mean()
    df['snowmelt_mean_J3']   = df['snowmelt'].rolling(3,       min_periods=1).mean()
    # Moyenne glissante 27j (au lieu de 10j)
    df['precip_mean_J10']     = df['precipitation'].rolling(10, min_periods=1).mean()
    df['temp_mean_J10']       = df['temperature'].rolling(10,   min_periods=1).mean()
    df['pet_mean_J10']        = df['pet'].rolling(10,           min_periods=1).mean()
    df['snow_depth_mean_J10'] = df['snow_depth'].rolling(10,    min_periods=1).mean()
    df['snowmelt_mean_J10']   = df['snowmelt'].rolling(10,      min_periods=1).mean()
    # Moyenne glissante 27j
    df['precip_mean_J27']     = df['precipitation'].rolling(27, min_periods=1).mean()
    df['temp_mean_J27']       = df['temperature'].rolling(27,   min_periods=1).mean()
    df['pet_mean_J27']        = df['pet'].rolling(27,           min_periods=1).mean()
    df['snow_depth_mean_J27'] = df['snow_depth'].rolling(27,    min_periods=1).mean()
    df['snowmelt_mean_J27']   = df['snowmelt'].rolling(27,      min_periods=1).mean()

    # ── Climatologie + encodage cyclique DOY ────────────────────────────────
    doy = np.clip(df.index.dayofyear, a_min=1, a_max=365)

    df['doy_sin'] = np.sin(2 * np.pi * doy / 365)
    df['doy_cos'] = np.cos(2 * np.pi * doy / 365)

    if code_sta in clim_dict:
        clim_sta = clim_dict[code_sta]
        df['clim_mean'] = clim_sta.loc[doy, 'wl_mean'].values
        df['clim_std']  = clim_sta.loc[doy, 'wl_std'].values
    else:
        df['clim_mean'] = 0.0
        df['clim_std']  = 1.0

    # ── Climatologie fenêtrée ±20j (leave-one-year-out) ─────────────────────
    clim_m20, clim_s20 = compute_clim_fenetre(df['water_level'], window=CLIM_WINDOW)
    df['clim_mean_20j'] = clim_m20
    df['clim_std_20j']  = clim_s20

    # Renommer les variables J0
    df = df.rename(columns={
        'precipitation': 'precipitation_J0',
        'temperature'  : 'temperature_J0',
        'pet'          : 'pet_J0',
        'snow_depth'   : 'snow_depth_J0',
        'snowmelt'     : 'snowmelt_J0',
    })

    # ── Variables à exporter ────────────────────────────────────────────────
    cols_out = [
        # Météo classique
        'precipitation_J0', 'temperature_J0', 'pet_J0',
        'precip_mean_J3',   'temp_mean_J3',   'pet_mean_J3',
        'precip_mean_J10',  'temp_mean_J10',  'pet_mean_J10',
        'precip_mean_J27',  'temp_mean_J27',  'pet_mean_J27',
        # Neige
        'snow_depth_J0',        'snowmelt_J0',
        'snow_depth_mean_J3',   'snowmelt_mean_J3',
        'snow_depth_mean_J10',  'snowmelt_mean_J10',
        'snow_depth_mean_J27',  'snowmelt_mean_J27',
        # Climatologie par DOY
        'clim_mean', 'clim_std',
        # Climatologie fenêtrée ±20j (leave-one-year-out)
        'clim_mean_20j', 'clim_std_20j',
        # Encodage cyclique
        'doy_sin',   'doy_cos',
        # Cible
        'water_level',
    ]

    # ── Générer 1 .nc par décalage (27 décalages) ──────────────────────────
    for decalage in range(FREQ_JOURS):
        station_id = f"{code_sta}_d{decalage}"
        nc_path    = os.path.join(OUTPUT_DIR, 'time_series', f'{station_id}.nc')

        if os.path.exists(nc_path):
            stations_exportees.append(station_id)
            continue

        indices_sat = list(range(decalage, len(df), FREQ_JOURS))
        df_sat      = df.iloc[indices_sat][cols_out].copy()

        if df_sat['water_level'].dropna().shape[0] < 50:
            continue

        data_vars = {
            col: xr.Variable("date", df_sat[col].values.astype(np.float32))
            for col in cols_out
        }
        ds = xr.Dataset(data_vars, coords={"date": df_sat.index.values})
        ds.to_netcdf(nc_path, engine="scipy", format="NETCDF3_CLASSIC")
        stations_exportees.append(station_id)

    if (i + 1) % 50 == 0:
        print(f"  {i+1}/{len(candidats)} stations traitées "
              f"({len(stations_exportees)} .nc générés)")

print(f"\n{len(stations_exportees)} .nc générés")
print(f"{len(stations_skip)} stations ignorées")
if stations_skip[:5]:
    print("Premières skipées :")
    for code, reason in stations_skip[:5]:
        print(f"  {code} : {reason}")

# ═══════════════════════════════════════════════════════════════
# ATTRIBUTES
# ═══════════════════════════════════════════════════════════════
print("\nExport attributes.csv...")

stations_base = list({sid.rsplit('_d', 1)[0] for sid in stations_exportees})
placeholders  = ','.join(['?' for _ in stations_base])

conn_attrs = sqlite3.connect(DB_PATH)
attrs_base = pd.read_sql(f'''
    SELECT
        b.code_sta         AS station_id,
        b.aire_km2,
        s.lon,
        s.lat,
        s.dist_barrage_m,
        s.strahler,
        s.elevation_mean,
        s.slope_mean,
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

rows = []
for _, row in attrs_base.iterrows():
    for decalage in range(FREQ_JOURS):
        new_row = row.copy()
        new_row['station_id'] = f"{row['station_id']}_d{decalage}"
        rows.append(new_row)

attrs_out = pd.DataFrame(rows).reset_index(drop=True)
attrs_out = attrs_out[attrs_out['station_id'].isin(stations_exportees)]
attrs_out.to_csv(os.path.join(OUTPUT_DIR, 'attributes', 'attributes.csv'), index=False)
print(f"✅ attributes.csv — {len(attrs_out)} lignes "
      f"({len(stations_base)} stations × décalages)")

# ═══════════════════════════════════════════════════════════════
# TRAIN / VAL SPLIT
# ═══════════════════════════════════════════════════════════════
print("\nSplit train/val...")

random.shuffle(stations_base)
n_val   = max(1, int(len(stations_base) * VAL_RATIO))
n_train = len(stations_base) - n_val

train_base = stations_base[:n_train]
val_base   = stations_base[n_train:]

set_exportees = set(stations_exportees)
train_ids = [f"{s}_d{d}" for s in train_base for d in range(FREQ_JOURS)
             if f"{s}_d{d}" in set_exportees]
val_ids   = [f"{s}_d{d}" for s in val_base   for d in range(FREQ_JOURS)
             if f"{s}_d{d}" in set_exportees]

with open(os.path.join(BASINS_DIR, 'train_basins.txt'), 'w') as f:
    f.write('\n'.join(train_ids))
with open(os.path.join(BASINS_DIR, 'val_basins.txt'), 'w') as f:
    f.write('\n'.join(val_ids))

print(f"✅ train_basins.txt — {len(train_ids)} ids ({len(train_base)} stations × {FREQ_JOURS})")
print(f"✅ val_basins.txt   — {len(val_ids)} ids ({len(val_base)} stations × {FREQ_JOURS})")

print(f"\nPériode : {DATE_DEB} → {DATE_FIN}")
print(f"Dossier : {OUTPUT_DIR}")
print(f"""
📋 Variables dynamiques pour le yaml :
   use_frequencies:
     - 27D

   dynamic_inputs:
     - precipitation_J0
     - temperature_J0
     - pet_J0
     - precip_mean_J3
     - temp_mean_J3
     - pet_mean_J3
     - precip_mean_J27
     - temp_mean_J27
     - pet_mean_J27
     - snow_depth_J0
     - snowmelt_J0
     - snow_depth_mean_J3
     - snowmelt_mean_J3
     - snow_depth_mean_J27
     - snowmelt_mean_J27
     - clim_mean
     - clim_std
     - clim_mean_20j
     - clim_std_20j
     - doy_sin
     - doy_cos

   seq_length:
     27D: 13    # 13 pas × 27j = 351j ≈ 1 an (comparable au 36 × 10j = 360j)

   lagged_features:
     water_level:
       - 1      # lag-1 = 27j réels

   target_variables:
     - water_level

   data_dir: ./data/IA/NeuralHydrology_feat27j
   train_basin_file: ./AI/LSTM/NeuralHydro_feat27j/train_basins.txt
""")

conn.close()