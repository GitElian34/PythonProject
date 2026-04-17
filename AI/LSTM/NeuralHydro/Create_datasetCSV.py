import sqlite3
import pandas as pd
import numpy as np
import xarray as xr
import os

DB_PATH    = './data/insitu_data.db'
OUTPUT_DIR = './data/IA/NeuralHydrology/'
BASINS_DIR = './AI/LSTM/NeuralHydro/'
DATE_DEB   = '2016-01-01'
DATE_FIN   = '2025-12-31'
N_STATIONS = 250
VAL_RATIO  = 0.2

os.makedirs(os.path.join(OUTPUT_DIR, 'time_series'), exist_ok=True)
os.makedirs(os.path.join(OUTPUT_DIR, 'attributes'), exist_ok=True)
os.makedirs(BASINS_DIR, exist_ok=True)

conn = sqlite3.connect(DB_PATH)

stations = pd.read_sql('''
    SELECT DISTINCT e.code_sta
    FROM era5_bv_jour e
    JOIN mesures_insitu m ON e.mesure_id = m.id
    JOIN bv_data b        ON e.code_sta = b.code_sta
    JOIN bv_corine c      ON e.code_sta = c.code_sta
    WHERE e.mesure_date >= ? AND e.mesure_date <= ?
    GROUP BY e.code_sta
    HAVING COUNT(DISTINCT e.mesure_date) >= 1000
       AND MIN(e.mesure_date) <= ?
       AND MAX(e.mesure_date) >= ?
    ORDER BY RANDOM()
    LIMIT ?
''', conn, params=(DATE_DEB, DATE_FIN, DATE_DEB, DATE_FIN, N_STATIONS))

print(f"{len(stations)} stations sélectionnées")

stations_ok = []
date_range  = pd.date_range(DATE_DEB, DATE_FIN, freq='D')

for _, row in stations.iterrows():
    code_sta = row['code_sta']

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
        print(f"  ⚠️  {code_sta} — pas de données")
        continue

    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date').reindex(date_range)
    df.index.name = 'date'

    # ── Sécurité 1 : couverture temporelle ───────────────────────
    date_min = df.index[df.notna().any(axis=1)].min()
    date_max = df.index[df.notna().any(axis=1)].max()
    if date_min > pd.Timestamp('2016-03-01') or date_max < pd.Timestamp('2025-09-01'):
        print(f"  ⚠️  {code_sta} — couverture incomplète "
              f"({date_min.date()} → {date_max.date()}), ignorée")
        continue

    # ── Sécurité 2 : trop de NaN ─────────────────────────────────
    nan_pct = df.isnull().mean().max()
    if nan_pct > 0.2:
        print(f"  ⚠️  {code_sta} — trop de NaN ({nan_pct:.0%})")
        continue

    # ── Sécurité 3 : attributs statiques présents ────────────────
    check = conn.execute(
        "SELECT COUNT(*) FROM bv_corine WHERE code_sta = ?", (code_sta,)
    ).fetchone()[0]
    if check == 0:
        print(f"  ⚠️  {code_sta} — pas d'attributs dans bv_corine, ignorée")
        continue

    # ── Normalisation water_level par station ─────────────────────
    wl_mean = df['water_level'].mean()
    wl_std  = df['water_level'].std()
    if wl_std > 0:
        df['water_level'] = (df['water_level'] - wl_mean) / wl_std
        print(f"  📏 {code_sta} — normalisé "
              f"(mean={wl_mean:.3f}, std={wl_std:.3f}) "
              f"| couverture {date_min.date()} → {date_max.date()}")
    else:
        print(f"  ⚠️  {code_sta} — water_level constant")
        continue

    # ── Export NetCDF directement (sans passer par CSV) ──────────
    nc_path = os.path.join(OUTPUT_DIR, 'time_series', f'{code_sta}.nc')
    if os.path.exists(nc_path):
        os.remove(nc_path)

    data_vars = {}
    for col in df.columns:
        data_vars[col] = xr.Variable("date", df[col].values.astype(np.float32))

    ds = xr.Dataset(data_vars, coords={"date": df.index.values})
    ds.to_netcdf(nc_path, engine="scipy", format="NETCDF3_CLASSIC")

    stations_ok.append(code_sta)
    print(f"  ✅ {code_sta} — {len(df)} jours → {code_sta}.nc")

# ── Attributes ────────────────────────────────────────────────
if stations_ok:
    placeholders = ','.join(['?' for _ in stations_ok])
    attrs = pd.read_sql(f'''
        SELECT
            b.code_sta      AS station_id,
            b.aire_km2,
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
        JOIN bv_corine c ON b.code_sta = c.code_sta
        WHERE b.code_sta IN ({placeholders})
        ORDER BY b.code_sta
    ''', conn, params=stations_ok)

    # ── Sécurité 4 : tous les attrs présents ─────────────────────
    missing_attrs = set(stations_ok) - set(attrs['station_id'].tolist())
    if missing_attrs:
        print(f"\n⚠️  Stations sans attributs (exclues) : {missing_attrs}")
        stations_ok = [s for s in stations_ok if s not in missing_attrs]
        attrs = attrs[attrs['station_id'].isin(stations_ok)]

    attrs_path = os.path.join(OUTPUT_DIR, 'attributes', 'attributes.csv')
    attrs.to_csv(attrs_path, index=False)
    print(f"\n✅ attributes/attributes.csv — {len(attrs)} stations")

# ── Train / Val split ─────────────────────────────────────────
n_val        = max(1, int(len(stations_ok) * VAL_RATIO))
n_train      = len(stations_ok) - n_val
train_basins = stations_ok[:n_train]
val_basins   = stations_ok[n_train:]

with open(os.path.join(BASINS_DIR, 'train_basins.txt'), 'w') as f:
    f.write('\n'.join(train_basins))

with open(os.path.join(BASINS_DIR, 'val_basins.txt'), 'w') as f:
    f.write('\n'.join(val_basins))

print(f"\n✅ train_basins.txt — {len(train_basins)} stations")
print(f"✅ val_basins.txt   — {len(val_basins)} stations")

conn.close()
print(f"\nStations exportées : {len(stations_ok)}")
print(f"Période            : {DATE_DEB} → {DATE_FIN}")