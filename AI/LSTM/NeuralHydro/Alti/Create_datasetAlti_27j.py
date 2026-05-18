"""
create_dataset_satellite_27D.py
═══════════════════════════════════════════════════════════════════════════
Dataset satellite sur grille 27D RÉGULIÈRE

Variables dynamiques :
  precipitation_J0, temperature_J0, pet_J0
  precip_mean_J3, pet_mean_J3, temp_mean_J3
  precip_mean_J27, temp_mean_J27
  snow_depth_J0, snowmelt_J0
  clim_mean_20j, clim_std_20j
  doy_sin, doy_cos
  water_level

Sortie :
  ./data/IA/NeuralHydrology_satellite_27D/time_series/{station}.nc
  ./data/IA/NeuralHydrology_satellite_27D/attributes/attributes.csv
═══════════════════════════════════════════════════════════════════════════
"""

import sqlite3
import pandas as pd
import numpy as np
import xarray as xr
import os
from pathlib import Path

# ─── Paramètres ─────────────────────────────────────────────────────────────
DB_PATH       = './data/hydro_data.db'
OUTPUT_DIR    = './data/IA/NeuralHydrology_satellite_27D/'
BASINS_DIR    = './AI/LSTM/NeuralHydro_satellite_27D/'
STATIONS_FILE = './Exploring_data/Stations_27j/stations_27j.txt'

DATE_DEB      = '2016-01-01'
DATE_FIN      = '2025-12-31'
FREQ_JOURS    = 27
SNAP_TOL      = 13     # ±13j (moitié de 27)
CLIM_WINDOW   = 20
MIN_MESURES   = 15
RESET         = True

# Stations à exclure (à remplir après inspection visuelle des PNG)
EXCLUDE_STATIONS = set()

os.makedirs(os.path.join(OUTPUT_DIR, 'time_series'), exist_ok=True)
os.makedirs(os.path.join(OUTPUT_DIR, 'attributes'), exist_ok=True)
os.makedirs(BASINS_DIR, exist_ok=True)

if RESET:
    ts_dir = os.path.join(OUTPUT_DIR, 'time_series')
    n_old = len([f for f in os.listdir(ts_dir) if f.endswith('.nc')])
    if n_old > 0:
        print(f"🗑️  Suppression de {n_old} anciens .nc...")
        for f in os.listdir(ts_dir):
            if f.endswith('.nc'):
                os.remove(os.path.join(ts_dir, f))

conn = sqlite3.connect(DB_PATH)

# ═══════════════════════════════════════════════════════════════
# CHARGEMENT
# ═══════════════════════════════════════════════════════════════
print("=" * 70)
print("CRÉATION DATASET SATELLITE 27D (+ SNOW + DOY)")
print("=" * 70)

if not Path(STATIONS_FILE).exists():
    print(f"\n❌ {STATIONS_FILE} introuvable !")
    print("   Lance d'abord : python analyse_stations_27j.py")
    conn.close()
    exit(1)

with open(STATIONS_FILE) as f:
    stations_27j = [line.strip() for line in f if line.strip()]
stations_27j = [s for s in stations_27j if s not in EXCLUDE_STATIONS]
print(f"\n  {len(stations_27j)} stations 27j")

grid_27D = pd.date_range(DATE_DEB, DATE_FIN, freq=f'{FREQ_JOURS}D')
print(f"  Grille 27D : {len(grid_27D)} pas")

# Vérifier que la table snow existe
has_snow = conn.execute(
    "SELECT name FROM sqlite_master WHERE type='table' AND name='era5_snow_bv_jour'"
).fetchone() is not None

if not has_snow:
    print("\n  ⚠️  Table era5_snow_bv_jour absente — snow sera mis à 0")

nb_snow = 0
if has_snow:
    nb_snow = conn.execute("SELECT COUNT(*) FROM era5_snow_bv_jour").fetchone()[0]
    print(f"  Snow data : {nb_snow} lignes dans era5_snow_bv_jour")


# ═══════════════════════════════════════════════════════════════
# CLIMATOLOGIE FENÊTRÉE
# ═══════════════════════════════════════════════════════════════
def compute_clim_fenetre(dates, wl_values, window=CLIM_WINDOW):
    n = len(dates)
    clim_mean = np.zeros(n, dtype=np.float32)
    clim_std  = np.ones(n, dtype=np.float32)

    valid_mask = ~np.isnan(wl_values)
    if valid_mask.sum() < 10:
        return clim_mean, clim_std

    valid_dates = dates[valid_mask]
    valid_vals  = wl_values[valid_mask]
    valid_doys  = np.clip(np.array([d.timetuple().tm_yday for d in valid_dates]), 1, 365)
    valid_years = np.array([d.year for d in valid_dates])

    all_doys  = np.clip(np.array([d.timetuple().tm_yday for d in dates]), 1, 365)
    all_years = np.array([d.year for d in dates])

    for i in range(n):
        doy_diff = np.abs(valid_doys - all_doys[i])
        doy_diff = np.minimum(doy_diff, 365 - doy_diff)
        mask = (doy_diff <= window) & (valid_years != all_years[i])
        vals_in = valid_vals[mask]
        if len(vals_in) >= 3:
            clim_mean[i] = vals_in.mean()
            clim_std[i]  = vals_in.std()
            if clim_std[i] < 0.01:
                clim_std[i] = 1.0

    return clim_mean, clim_std


# ═══════════════════════════════════════════════════════════════
# EXPORT .NC
# ═══════════════════════════════════════════════════════════════
print(f"\nExport .nc...\n")

stations_exportees = []
stations_skip      = []

for i, code_sta in enumerate(stations_27j):

    # ── 1. ERA5 quotidien ──────────────────────────────────────────────
    df_era5 = pd.read_sql('''
        SELECT mesure_date AS date, temp_moy_bv, precip_sum_bv, pet_sum_bv
        FROM era5_bv_jour
        WHERE station_code = ? ORDER BY mesure_date
    ''', conn, params=(code_sta,))

    if df_era5.empty:
        stations_skip.append((code_sta, "pas de ERA5"))
        continue

    df_era5['date'] = pd.to_datetime(df_era5['date'])
    df_era5 = df_era5.set_index('date').sort_index()

    # ── 2. Snow quotidien ──────────────────────────────────────────────
    has_snow_sta = False
    if has_snow:
        df_snow = pd.read_sql('''
            SELECT mesure_date AS date, snow_depth_bv, snowmelt_bv
            FROM era5_snow_bv_jour
            WHERE station_code = ? ORDER BY mesure_date
        ''', conn, params=(code_sta,))

        if not df_snow.empty:
            df_snow['date'] = pd.to_datetime(df_snow['date'])
            df_snow = df_snow.set_index('date').sort_index()
            has_snow_sta = True

    # ── 3. Mesures satellite ───────────────────────────────────────────
    df_mes = pd.read_sql('''
        SELECT measure_date, orthometric_height
        FROM measurements
        WHERE station_code = ? AND is_valid = 1
              AND orthometric_height IS NOT NULL
        ORDER BY measure_date
    ''', conn, params=(code_sta,))

    if df_mes.empty:
        stations_skip.append((code_sta, "pas de mesures"))
        continue

    df_mes['measure_date'] = pd.to_datetime(df_mes['measure_date'])
    df_mes = df_mes.set_index('measure_date').sort_index()
    df_mes = df_mes[~df_mes.index.duplicated(keep='first')]

    # ── 4. Normalisation water_level ───────────────────────────────────
    wl_raw = df_mes['orthometric_height'].dropna()
    wl_mean = wl_raw.mean()
    wl_std  = wl_raw.std()
    if pd.isna(wl_std) or wl_std <= 0:
        stations_skip.append((code_sta, "std wl nul"))
        continue

    # ── 5. Construire le DataFrame sur grille 27D ──────────────────────
    df_grid = pd.DataFrame(index=grid_27D)
    df_grid.index.name = 'date'

    # ERA5 J0
    df_grid['precipitation_J0'] = df_era5.reindex(grid_27D)['precip_sum_bv'].values
    df_grid['temperature_J0']   = df_era5.reindex(grid_27D)['temp_moy_bv'].values
    df_grid['pet_J0']           = df_era5.reindex(grid_27D)['pet_sum_bv'].values

    # Snow J0
    if has_snow_sta:
        snow_reindexed = df_snow.reindex(grid_27D)
        df_grid['snow_depth_J0'] = snow_reindexed['snow_depth_bv'].fillna(0).values
        df_grid['snowmelt_J0']   = snow_reindexed['snowmelt_bv'].fillna(0).values
    else:
        df_grid['snow_depth_J0'] = 0.0
        df_grid['snowmelt_J0']   = 0.0

    # ERA5 moyennes J3, J10 et J27 depuis le quotidien
    precip_J3  = np.full(len(grid_27D), np.nan)
    temp_J3    = np.full(len(grid_27D), np.nan)
    pet_J3     = np.full(len(grid_27D), np.nan)
    precip_J10 = np.full(len(grid_27D), np.nan)
    temp_J10   = np.full(len(grid_27D), np.nan)
    pet_J10    = np.full(len(grid_27D), np.nan)
    precip_J27 = np.full(len(grid_27D), np.nan)
    temp_J27   = np.full(len(grid_27D), np.nan)

    for j, dt in enumerate(grid_27D):
        # J3
        start_3 = dt - pd.Timedelta(days=2)
        sub_3 = df_era5.loc[start_3:dt]
        if len(sub_3) >= 1:
            precip_J3[j] = sub_3['precip_sum_bv'].mean()
            temp_J3[j]   = sub_3['temp_moy_bv'].mean()
            pet_J3[j]    = sub_3['pet_sum_bv'].mean()

        # J10
        start_10 = dt - pd.Timedelta(days=9)
        sub_10 = df_era5.loc[start_10:dt]
        if len(sub_10) >= 1:
            precip_J10[j] = sub_10['precip_sum_bv'].mean()
            temp_J10[j]   = sub_10['temp_moy_bv'].mean()
            pet_J10[j]    = sub_10['pet_sum_bv'].mean()

        # J27
        start_27 = dt - pd.Timedelta(days=26)
        sub_27 = df_era5.loc[start_27:dt]
        if len(sub_27) >= 1:
            precip_J27[j] = sub_27['precip_sum_bv'].mean()
            temp_J27[j]   = sub_27['temp_moy_bv'].mean()

    df_grid['precip_mean_J3']  = precip_J3
    df_grid['pet_mean_J3']     = pet_J3
    df_grid['temp_mean_J3']    = temp_J3
    df_grid['precip_mean_J10'] = precip_J10
    df_grid['pet_mean_J10']    = pet_J10
    df_grid['temp_mean_J10']   = temp_J10
    df_grid['precip_mean_J27'] = precip_J27
    df_grid['temp_mean_J27']   = temp_J27

    # ── Caractérisation fine des précipitations depuis le quotidien ────
    precip_max_J27     = np.full(len(grid_27D), np.nan)
    precip_last7       = np.full(len(grid_27D), np.nan)
    nb_jours_pluie_J27 = np.full(len(grid_27D), np.nan)
    precip_mean_J14    = np.full(len(grid_27D), np.nan)

    for j, dt in enumerate(grid_27D):
        start_27 = dt - pd.Timedelta(days=26)
        sub_27 = df_era5.loc[start_27:dt]['precip_sum_bv']
        if len(sub_27) >= 1:
            precip_max_J27[j]     = sub_27.max()
            nb_jours_pluie_J27[j] = (sub_27 > 1.0).sum()

        start_7 = dt - pd.Timedelta(days=6)
        sub_7 = df_era5.loc[start_7:dt]['precip_sum_bv']
        if len(sub_7) >= 1:
            precip_last7[j] = sub_7.mean()

        start_14 = dt - pd.Timedelta(days=13)
        sub_14 = df_era5.loc[start_14:dt]['precip_sum_bv']
        if len(sub_14) >= 1:
            precip_mean_J14[j] = sub_14.mean()

    df_grid['precip_max_J27']     = precip_max_J27
    df_grid['precip_last7']       = precip_last7
    df_grid['nb_jours_pluie_J27'] = nb_jours_pluie_J27
    df_grid['precip_mean_J14']    = precip_mean_J14

    # ── 6. Snap mesures satellite ──────────────────────────────────────
    water_level = np.full(len(grid_27D), np.nan)
    for j, dt_grid in enumerate(grid_27D):
        deltas = np.abs((df_mes.index - dt_grid).total_seconds()) / 86400.0
        if len(deltas) == 0:
            continue
        idx_min = deltas.argmin()
        if deltas[idx_min] <= SNAP_TOL:
            water_level[j] = (df_mes['orthometric_height'].iloc[idx_min] - wl_mean) / wl_std

    df_grid['water_level'] = water_level

    n_valid = np.sum(~np.isnan(water_level))
    if n_valid < MIN_MESURES:
        stations_skip.append((code_sta, f"seulement {n_valid} mesures"))
        continue

    # ── 7. Climatologie fenêtrée ───────────────────────────────────────
    clim_m, clim_s = compute_clim_fenetre(
        grid_27D.to_pydatetime(), water_level, window=CLIM_WINDOW
    )
    df_grid['clim_mean_20j'] = clim_m
    df_grid['clim_std_20j']  = clim_s

    # ── 8. DOY sin/cos ─────────────────────────────────────────────────
    doy = np.clip(grid_27D.dayofyear, 1, 365)
    df_grid['doy_sin'] = np.sin(2 * np.pi * doy / 365).astype(np.float32)
    df_grid['doy_cos'] = np.cos(2 * np.pi * doy / 365).astype(np.float32)

    # ── 9. Export NetCDF ───────────────────────────────────────────────
    cols_out = [
        'precipitation_J0', 'temperature_J0', 'pet_J0',
        'precip_mean_J3',   'pet_mean_J3',    'temp_mean_J3',
        'precip_mean_J10',  'pet_mean_J10',   'temp_mean_J10',
        'precip_mean_J27',  'temp_mean_J27',
        # Caractérisation fine précipitations
        'precip_max_J27', 'precip_last7', 'nb_jours_pluie_J27', 'precip_mean_J14',
        'snow_depth_J0',    'snowmelt_J0',
        'clim_mean_20j',    'clim_std_20j',
        'doy_sin',          'doy_cos',
        'water_level',
    ]

    nc_path = os.path.join(OUTPUT_DIR, 'time_series', f'{code_sta}.nc')
    data_vars = {
        col: xr.Variable("date", df_grid[col].values.astype(np.float32))
        for col in cols_out
    }
    ds = xr.Dataset(data_vars, coords={"date": grid_27D.values})
    ds.to_netcdf(nc_path, engine="scipy", format="NETCDF3_CLASSIC")
    stations_exportees.append(code_sta)

    pct_valid = n_valid / len(grid_27D) * 100
    print(f"  [{i+1:3d}/{len(stations_27j)}] {code_sta} — "
          f"{n_valid} mesures ({pct_valid:.0f}%) ✅")

print(f"\n{'='*60}")
print(f"✅ {len(stations_exportees)} .nc générés")
print(f"⚠️  {len(stations_skip)} stations ignorées")
for code, reason in stations_skip:
    print(f"  {code} : {reason}")

# ═══════════════════════════════════════════════════════════════
# ATTRIBUTES
# ═══════════════════════════════════════════════════════════════
print(f"\nExport attributes.csv...")

placeholders = ','.join(['?' for _ in stations_exportees])
attrs = pd.read_sql(f'''
    SELECT
        b.station_code      AS station_id,
        b.aire_km2,
        s.reference_longitude AS lon,
        s.reference_latitude  AS lat,
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
    JOIN bv_corine c ON b.station_code = c.station_code
    JOIN stations s  ON b.station_code = s.station_code
    WHERE b.station_code IN ({placeholders})
    ORDER BY b.station_code
''', conn, params=stations_exportees)

attrs['station_id'] = attrs['station_id'].astype(str)
attrs.to_csv(os.path.join(OUTPUT_DIR, 'attributes', 'attributes.csv'), index=False)
print(f"✅ attributes.csv — {len(attrs)} stations")

# ═══════════════════════════════════════════════════════════════
# FICHIER STATIONS
# ═══════════════════════════════════════════════════════════════
basin_file = os.path.join(BASINS_DIR, 'stations_27j.txt')
with open(basin_file, 'w') as f:
    f.write('\n'.join(stations_exportees))
print(f"✅ {basin_file} — {len(stations_exportees)} stations")

print(f"""
{'='*60}
RÉSUMÉ
{'='*60}
  Stations   : {len(stations_exportees)}
  Variables  : 14 dynamiques + water_level
  Grille     : {len(grid_27D)} pas × 27D

  📋 Config YAML :
     data_dir: ./data/IA/NeuralHydrology_satellite_27D
     test_basin_file: ./AI/LSTM/NeuralHydro_satellite_27D/stations_27j.txt

     use_frequencies:
       - 27D

     seq_length:
       27D: 13       # 13 × 27j = 351j ≈ 1 an

     dynamic_inputs:
       - precipitation_J0
       - temperature_J0
       - pet_J0
       - precip_mean_J3
       - pet_mean_J3
       - temp_mean_J3
       - precip_mean_J27
       - temp_mean_J27
       - snow_depth_J0
       - snowmelt_J0
       - clim_mean_20j
       - clim_std_20j
       - doy_sin
       - doy_cos
""")

conn.close()