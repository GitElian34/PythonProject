"""
create_dataset_DtoD.py
════════════════════════════════════════════════════════════════════════
Crée 5 datasets NeuralHydrology identiques sauf le taux de masquage
du water_level, pour tester l'impact de la robustesse aux gaps sur
la généralisation zero-shot vers les données altimétriques.

Datasets générés :
  NeuralHydroDtoD0   → 0%  NaN  (baseline, teacher forcing pur)
  NeuralHydroDtoD20  → 20% NaN
  NeuralHydroDtoD50  → 50% NaN
  NeuralHydroDtoD80  → 80% NaN
  NeuralHydroDtoD90  → 90% NaN  (simule passages satellite ~10j)

Différences vs create_dataset_feat27j.py :
  - Données quotidiennes (pas de rééchantillonnage par décalage)
  - Features : J0, J3, J10 uniquement (J27 supprimé)
  - water_level masqué aléatoirement selon NAN_RATE
  - 2000 stations, split 1600 train / 400 val
  - Un dossier par taux de masquage

Usage :
    python create_dataset_DtoD.py
════════════════════════════════════════════════════════════════════════
"""

import sqlite3
import pandas as pd
import numpy as np
import xarray as xr
import os
import random
import shutil

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES GLOBAUX
# ═══════════════════════════════════════════════════════════════
DB_PATH    = "./data/insitu_data.db"
BASE_DIR   = "./data/IA/"
BASINS_DIR = "./AI/LSTM/"
DATE_DEB   = "2016-01-01"
DATE_FIN   = "2025-12-31"

N_STATIONS    = 2000
N_TRAIN       = 1600
N_VAL         = 400
SEED          = 42
MIN_GAP_JOURS = 60
MIN_DIST_M    = 500
CLIM_WINDOW   = 20
RESET         = False

# Taux de masquage à générer
NAN_RATES = [0.0, 0.20, 0.50, 0.80, 0.90]

# Features exportées — quotidiennes, J0 + J3 + J10 uniquement
COLS_OUT = [
    "precipitation_J0", "temperature_J0", "pet_J0",
    "precip_mean_J3",   "temp_mean_J3",   "pet_mean_J3",
    "precip_mean_J10",  "temp_mean_J10",  "pet_mean_J10",
    "precip_max_J10",   "precip_last7",   "nb_jours_pluie_J10",
    "snow_depth_J0",        "snowmelt_J0",
    "snow_depth_mean_J3",   "snowmelt_mean_J3",
    "snow_depth_mean_J10",  "snowmelt_mean_J10",
    "clim_mean", "clim_std",
    "clim_mean_20j", "clim_std_20j",
    "doy_sin", "doy_cos",
    "water_level",
]

# ═══════════════════════════════════════════════════════════════
# CLIMATOLOGIE FENÊTRÉE ±20j (leave-one-year-out)
# ═══════════════════════════════════════════════════════════════
def compute_clim_fenetre(wl_series, window=CLIM_WINDOW):
    wl_valid = wl_series.dropna()
    if len(wl_valid) < 30:
        n = len(wl_series)
        return np.zeros(n, dtype=np.float32), np.ones(n, dtype=np.float32)

    valid_doys  = np.clip(wl_valid.index.dayofyear, 1, 365)
    valid_years = wl_valid.index.year
    valid_vals  = wl_valid.values
    all_dates   = wl_series.index
    all_doys    = np.clip(all_dates.dayofyear, 1, 365)
    all_years   = all_dates.year

    clim_mean = np.zeros(len(all_dates), dtype=np.float32)
    clim_std  = np.ones(len(all_dates),  dtype=np.float32)

    for i in range(len(all_dates)):
        doy_diff = np.abs(valid_doys - all_doys[i])
        doy_diff = np.minimum(doy_diff, 365 - doy_diff)
        mask     = (doy_diff <= window) & (valid_years != all_years[i])
        vals     = valid_vals[mask]
        if len(vals) >= 3:
            clim_mean[i] = vals.mean()
            clim_std[i]  = vals.std() if vals.std() > 0.01 else 1.0
        else:
            clim_mean[i] = 0.0
            clim_std[i]  = 1.0

    return clim_mean, clim_std

# ═══════════════════════════════════════════════════════════════
# CONNEXION ET SÉLECTION DES STATIONS
# ═══════════════════════════════════════════════════════════════
conn       = sqlite3.connect(DB_PATH)
date_range = pd.date_range(DATE_DEB, DATE_FIN, freq="D")
rng_global = np.random.default_rng(SEED)
random.seed(SEED)

print("Chargement climatologie...")
clim_all = pd.read_sql_query(
    "SELECT code_sta, doy, wl_mean, wl_std FROM climatologie_wl ORDER BY code_sta, doy",
    conn
)
clim_dict = {}
for code, grp in clim_all.groupby("code_sta"):
    clim_dict[code] = grp.set_index("doy")[["wl_mean", "wl_std"]]
print(f"  {len(clim_dict)} stations avec climatologie\n")

print(f"Sélection des {N_STATIONS} stations...")
candidats = pd.read_sql("""
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
""", conn, params=(DATE_DEB, DATE_FIN, MIN_GAP_JOURS, MIN_DIST_M, DATE_DEB, DATE_FIN))

nc_existants = {
    f.replace(".nc", "")
    for f in os.listdir("./data/IA/NeuralHydrology/time_series")
    if f.endswith(".nc")
}
candidats = candidats[candidats["code_sta"].isin(nc_existants)].reset_index(drop=True)
candidats = candidats.sample(frac=1, random_state=SEED).reset_index(drop=True)
candidats = candidats.head(N_STATIONS)
print(f"  {len(candidats)} stations sélectionnées\n")

# Split train/val fixe (identique pour tous les datasets)
stations_list = candidats["code_sta"].tolist()
train_stations = stations_list[:N_TRAIN]
val_stations   = stations_list[N_TRAIN:N_TRAIN + N_VAL]
print(f"  Train : {len(train_stations)} | Val : {len(val_stations)}\n")

# ═══════════════════════════════════════════════════════════════
# CONSTRUCTION DES FEATURES (commun à tous les datasets)
# ═══════════════════════════════════════════════════════════════
print("Construction des features pour toutes les stations...")
features_cache = {}  # code_sta → df features complet (sans masquage)

for i, code_sta in enumerate(stations_list):
    df = pd.read_sql("""
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
    """, conn, params=(code_sta, DATE_DEB, DATE_FIN))

    if df.empty:
        continue

    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date").reindex(date_range)
    df.index.name = "date"

    # Normalisation water_level
    wl_mean = df["water_level"].mean()
    wl_std  = df["water_level"].std()
    if pd.isna(wl_std) or wl_std <= 0:
        continue
    df["water_level"] = (df["water_level"] - wl_mean) / wl_std

    # Neige
    df["snow_depth"] = df["snow_depth"].fillna(0)
    df["snowmelt"]   = df["snowmelt"].fillna(0)

    # Features J3
    df["precip_mean_J3"]     = df["precipitation"].rolling(3,  min_periods=1).mean()
    df["temp_mean_J3"]       = df["temperature"].rolling(3,    min_periods=1).mean()
    df["pet_mean_J3"]        = df["pet"].rolling(3,            min_periods=1).mean()
    df["snow_depth_mean_J3"] = df["snow_depth"].rolling(3,     min_periods=1).mean()
    df["snowmelt_mean_J3"]   = df["snowmelt"].rolling(3,       min_periods=1).mean()

    # Features J10
    df["precip_mean_J10"]     = df["precipitation"].rolling(10, min_periods=1).mean()
    df["temp_mean_J10"]       = df["temperature"].rolling(10,   min_periods=1).mean()
    df["pet_mean_J10"]        = df["pet"].rolling(10,           min_periods=1).mean()
    df["snow_depth_mean_J10"] = df["snow_depth"].rolling(10,    min_periods=1).mean()
    df["snowmelt_mean_J10"]   = df["snowmelt"].rolling(10,      min_periods=1).mean()
    df["precip_max_J10"]      = df["precipitation"].rolling(10, min_periods=1).max()
    df["precip_last7"]        = df["precipitation"].rolling(7,  min_periods=1).mean()
    df["nb_jours_pluie_J10"]  = (df["precipitation"] > 1.0).astype(float).rolling(10, min_periods=1).sum()

    # DOY cyclique
    doy = np.clip(df.index.dayofyear, 1, 365)
    df["doy_sin"] = np.sin(2 * np.pi * doy / 365).astype(np.float32)
    df["doy_cos"] = np.cos(2 * np.pi * doy / 365).astype(np.float32)

    # Climatologie par DOY
    if code_sta in clim_dict:
        clim_sta = clim_dict[code_sta]
        df["clim_mean"] = clim_sta.loc[doy, "wl_mean"].values
        df["clim_std"]  = clim_sta.loc[doy, "wl_std"].values
    else:
        df["clim_mean"] = 0.0
        df["clim_std"]  = 1.0

    # Climatologie fenêtrée ±20j
    clim_m20, clim_s20 = compute_clim_fenetre(df["water_level"])
    df["clim_mean_20j"] = clim_m20
    df["clim_std_20j"]  = clim_s20

    # Renommage J0
    df = df.rename(columns={
        "precipitation": "precipitation_J0",
        "temperature"  : "temperature_J0",
        "pet"          : "pet_J0",
        "snow_depth"   : "snow_depth_J0",
        "snowmelt"     : "snowmelt_J0",
    })

    if df["water_level"].dropna().shape[0] < 100:
        continue

    features_cache[code_sta] = df

    if (i + 1) % 100 == 0:
        print(f"  {i+1}/{len(stations_list)} stations traitées ({len(features_cache)} OK)")

print(f"\n  {len(features_cache)} stations avec features valides\n")

# ═══════════════════════════════════════════════════════════════
# EXPORT — 1 DOSSIER PAR TAUX DE MASQUAGE
# ═══════════════════════════════════════════════════════════════
for nan_rate in NAN_RATES:
    pct      = int(nan_rate * 100)
    name     = f"NeuralHydroDtoD{pct}"
    out_dir  = os.path.join(BASE_DIR, name)
    ts_dir   = os.path.join(out_dir, "time_series")
    att_dir  = os.path.join(out_dir, "attributes")
    bas_dir  = os.path.join(BASINS_DIR, name)

    os.makedirs(ts_dir,  exist_ok=True)
    os.makedirs(att_dir, exist_ok=True)
    os.makedirs(bas_dir, exist_ok=True)

    if RESET:
        shutil.rmtree(ts_dir); os.makedirs(ts_dir)

    print(f"\n{'='*60}")
    print(f"  Dataset {name}  (masquage={pct}%)")
    print(f"{'='*60}")

    # Graine reproductible par dataset
    rng = np.random.default_rng(SEED + pct)

    exported    = []
    skipped     = []
    attrs_rows  = []

    for code_sta in features_cache:
        df = features_cache[code_sta].copy()
        nc_path = os.path.join(ts_dir, f"{code_sta}.nc")

        if os.path.exists(nc_path):
            exported.append(code_sta)
            continue

        # Masquage aléatoire du water_level
        if nan_rate > 0:
            wl = df["water_level"].values.copy()
            valid_idx = np.where(~np.isnan(wl))[0]
            n_mask    = int(len(valid_idx) * nan_rate)
            mask_idx  = rng.choice(valid_idx, size=n_mask, replace=False)
            wl[mask_idx] = np.nan
            df["water_level"] = wl

        # Export .nc
        data_vars = {
            col: xr.Variable("date", df[col].values.astype(np.float32))
            for col in COLS_OUT if col in df.columns
        }
        ds = xr.Dataset(data_vars, coords={"date": df.index.values})
        ds.attrs["nan_rate"] = nan_rate
        ds.to_netcdf(nc_path, engine="scipy", format="NETCDF3_CLASSIC")
        exported.append(code_sta)

    print(f"  .nc exportés : {len(exported)} | skippés : {len(skipped)}")

    # ── Attributes ─────────────────────────────────────────────
    stations_exportees = [s for s in exported if s in features_cache]
    placeholders = ",".join(["?" for _ in stations_exportees])
    conn_a = sqlite3.connect(DB_PATH)
    attrs = pd.read_sql(f"""
        SELECT
            b.code_sta AS station_id,
            b.aire_km2,
            s.lon, s.lat,
            s.dist_barrage_m, s.strahler,
            s.elevation_mean, s.slope_mean,
            c.frac_urban, c.frac_agriculture, c.frac_forest,
            c.frac_semi_natural, c.frac_wetland, c.frac_water,
            c.sg_clay_0_30cm, c.sg_sand_0_30cm, c.sg_silt_0_30cm
        FROM bv_data b
        JOIN bv_corine c       ON b.code_sta = c.code_sta
        JOIN stations_insitu s ON b.code_sta = s.code_sta
        WHERE b.code_sta IN ({placeholders})
        ORDER BY b.code_sta
    """, conn_a, params=stations_exportees)
    conn_a.close()
    attrs.to_csv(os.path.join(att_dir, "attributes.csv"), index=False)
    print(f"  attributes.csv : {len(attrs)} lignes")

    # ── Train / Val split ──────────────────────────────────────
    set_exp   = set(exported)
    train_ids = [s for s in train_stations if s in set_exp]
    val_ids   = [s for s in val_stations   if s in set_exp]

    with open(os.path.join(bas_dir, "train_basins.txt"), "w") as f:
        f.write("\n".join(train_ids))
    with open(os.path.join(bas_dir, "val_basins.txt"), "w") as f:
        f.write("\n".join(val_ids))

    print(f"  train_basins.txt : {len(train_ids)} stations")
    print(f"  val_basins.txt   : {len(val_ids)} stations")
    print(f"  Dossier          : {out_dir}")

conn.close()

print(f"\n{'='*60}")
print("  DONE — 5 datasets générés :")
for nan_rate in NAN_RATES:
    pct = int(nan_rate * 100)
    print(f"    NeuralHydroDtoD{pct:<3}  →  {pct}% NaN sur water_level")
print(f"{'='*60}")
print(f"""
📋 Config NeuralHydrology (même pour tous) :
   use_frequencies: [1D]

   dynamic_inputs:
     - precipitation_J0
     - temperature_J0
     - pet_J0
     - precip_mean_J3
     - temp_mean_J3
     - pet_mean_J3
     - precip_mean_J10
     - temp_mean_J10
     - pet_mean_J10
     - precip_max_J10
     - precip_last7
     - nb_jours_pluie_J10
     - snow_depth_J0
     - snowmelt_J0
     - snow_depth_mean_J3
     - snowmelt_mean_J3
     - snow_depth_mean_J10
     - snowmelt_mean_J10
     - clim_mean
     - clim_std
     - clim_mean_20j
     - clim_std_20j
     - doy_sin
     - doy_cos

   target_variables: [water_level]
   seq_length: 365
""")