"""
create_dataset_dahiti_DtoD.py
════════════════════════════════════════════════════════════════════════
Crée les .nc DAHITI au format compatible avec les modèles DtoD
(données quotidiennes, features J0/J3/J10, water_level aux vraies
dates de mesure satellite et NaN ailleurs).

Différences vs create_dataset_hwnext_DtoD.py :
  - BDD : dahiti.db
  - WSE : table measurements, colonne orthometric_height
  - Filtre doublons S3A/S3B (min_gap=20j)
  - ID station : dahiti_id (entier sans zéros)

Sorties :
  ./data/IA/NeuralHydrologyDahitiDtoD/time_series/<id>.nc
  ./data/IA/NeuralHydrologyDahitiDtoD/attributes/attributes.csv
  ./data/IA/NeuralHydrologyDahitiDtoD/stations_dahiti.txt
════════════════════════════════════════════════════════════════════════
"""

import sqlite3
import numpy as np
import pandas as pd
import xarray as xr
import os

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
DB_PATH      = "./data/dahiti.db"
OUTPUT_DIR   = "./data/IA/NeuralHydrologyDahitiDtoD"
DATE_DEB     = "2016-01-01"
DATE_FIN     = "2025-12-31"
CLIM_WINDOW  = 20
MIN_GAP_DAYS = 20   # filtre doublons S3A/S3B

STATIC_COLS = [
    "aire_km2", "lon", "lat",
    "frac_urban", "frac_forest", "frac_agriculture",
    "sg_clay_0_30cm", "sg_sand_0_30cm", "sg_silt_0_30cm",
    "strahler", "elevation_mean", "slope_mean",
]

COLS_OUT = [
    "precipitation_J0", "temperature_J0", "pet_J0",
    "precip_mean_J3",   "temp_mean_J3",   "pet_mean_J3",
    "precip_mean_J10",  "temp_mean_J10",  "pet_mean_J10",
    "precip_max_J10",   "precip_last7",   "nb_jours_pluie_J10",
    "snow_depth_J0",        "snowmelt_J0",
    "snow_depth_mean_J3",   "snowmelt_mean_J3",
    "snow_depth_mean_J10",  "snowmelt_mean_J10",
    "clim_mean_20j", "clim_std_20j",
    "doy_sin", "doy_cos",
    "water_level",
]

ts_dir  = os.path.join(OUTPUT_DIR, "time_series")
att_dir = os.path.join(OUTPUT_DIR, "attributes")
os.makedirs(ts_dir,  exist_ok=True)
os.makedirs(att_dir, exist_ok=True)

# ═══════════════════════════════════════════════════════════════
# FILTRE DOUBLONS S3A/S3B
# ═══════════════════════════════════════════════════════════════
def filter_one_per_pass(df, min_gap=MIN_GAP_DAYS):
    if df.empty:
        return df
    df = df.sort_values("date").reset_index(drop=True)
    kept = [df.iloc[0]]
    for _, row in df.iterrows():
        if (row["date"] - kept[-1]["date"]).days >= min_gap:
            kept.append(row)
    return pd.DataFrame(kept).reset_index(drop=True)

# ═══════════════════════════════════════════════════════════════
# CLIMATOLOGIE FENÊTRÉE ±20j
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
# CHARGEMENT STATIONS
# ═══════════════════════════════════════════════════════════════
conn       = sqlite3.connect(DB_PATH)
date_range = pd.date_range(DATE_DEB, DATE_FIN, freq="D")

stations = pd.read_sql("""
    SELECT
        s.station_code,
        s.hydroweb_name,
        s.reference_longitude AS lon,
        s.reference_latitude  AS lat,
        COALESCE(b.aire_km2,         0.0) AS aire_km2,
        COALESCE(s.frac_urban,       0.0) AS frac_urban,
        COALESCE(s.frac_forest,      0.0) AS frac_forest,
        COALESCE(s.frac_agriculture, 0.0) AS frac_agriculture,
        COALESCE(s.sg_clay_0_30cm,   0.0) AS sg_clay_0_30cm,
        COALESCE(s.sg_sand_0_30cm,   0.0) AS sg_sand_0_30cm,
        COALESCE(s.sg_silt_0_30cm,   0.0) AS sg_silt_0_30cm,
        COALESCE(s.strahler,         0.0) AS strahler,
        COALESCE(s.elevation_mean,   0.0) AS elevation_mean,
        COALESCE(s.slope_mean,       0.0) AS slope_mean
    FROM stations s
    LEFT JOIN bv_data b ON s.station_code = b.station_code
    WHERE s.reference_longitude IS NOT NULL
      AND s.reference_latitude  IS NOT NULL
    ORDER BY s.station_code
""", conn)

print(f"{len(stations)} stations DAHITI chargées")

# ═══════════════════════════════════════════════════════════════
# CONSTRUCTION DES .NC
# ═══════════════════════════════════════════════════════════════
attr_rows   = []
station_ids = []
exported = skipped = errors = 0

for i, row in stations.iterrows():
    station_code = row["station_code"]
    name         = row.get("hydroweb_name") or station_code
    dahiti_id    = int(station_code.lstrip("0") or 0)
    nc_path      = os.path.join(ts_dir, f"{dahiti_id}.nc")

    try:
        # ── ERA5 ──────────────────────────────────────────────
        df_era5 = pd.read_sql("""
            SELECT date,
                   precip_sum_bv AS precipitation,
                   temp_moy_bv   AS temperature,
                   pet_sum_bv    AS pet,
                   snow_depth_bv AS snow_depth,
                   snowmelt_bv   AS snowmelt
            FROM era5_bv_jour
            WHERE station_code = ?
              AND date >= ? AND date <= ?
            ORDER BY date
        """, conn, params=(station_code, DATE_DEB, DATE_FIN))

        if df_era5.empty or df_era5["precipitation"].isna().all():
            print(f"  [{i+1:3d}] {name} — ERA5 vide → skip")
            skipped += 1
            continue

        df_era5["date"] = pd.to_datetime(df_era5["date"])
        df_era5 = df_era5.set_index("date").reindex(date_range)
        df_era5.index.name = "date"

        # ── WSE DAHITI avec filtre S3A/S3B ────────────────────
        df_wse = pd.read_sql("""
            SELECT measure_date AS date, orthometric_height AS wl
            FROM measurements
            WHERE station_code = ?
              AND measure_date >= ? AND measure_date <= ?
              AND is_valid = 1
            ORDER BY measure_date
        """, conn, params=(station_code, DATE_DEB, DATE_FIN))

        if df_wse.empty or len(df_wse) < 5:
            print(f"  [{i+1:3d}] {name} — WSE insuffisant → skip")
            skipped += 1
            continue

        df_wse["date"] = pd.to_datetime(df_wse["date"])
        df_wse = df_wse.dropna(subset=["wl"])
        df_wse = filter_one_per_pass(df_wse)

        if len(df_wse) < 5:
            print(f"  [{i+1:3d}] {name} — WSE insuffisant après filtre S3A/S3B → skip")
            skipped += 1
            continue

        # Place chaque mesure sur le jour exact de la grille
        wse_series = pd.Series(index=date_range, dtype=float, name="wl")
        for _, r in df_wse.iterrows():
            if r["date"] in wse_series.index:
                wse_series[r["date"]] = r["wl"]

        # Normalisation
        wl_mean = wse_series.mean()
        wl_std  = wse_series.std()
        if pd.isna(wl_std) or wl_std <= 0:
            print(f"  [{i+1:3d}] {name} — std nul → skip")
            skipped += 1
            continue
        wse_series = (wse_series - wl_mean) / wl_std

        # ── Features ERA5 ─────────────────────────────────────
        df = df_era5.copy()
        df["snow_depth"] = df["snow_depth"].fillna(0)
        df["snowmelt"]   = df["snowmelt"].fillna(0)

        df["precip_mean_J3"]     = df["precipitation"].rolling(3,  min_periods=1).mean()
        df["temp_mean_J3"]       = df["temperature"].rolling(3,    min_periods=1).mean()
        df["pet_mean_J3"]        = df["pet"].rolling(3,            min_periods=1).mean()
        df["snow_depth_mean_J3"] = df["snow_depth"].rolling(3,     min_periods=1).mean()
        df["snowmelt_mean_J3"]   = df["snowmelt"].rolling(3,       min_periods=1).mean()

        df["precip_mean_J10"]     = df["precipitation"].rolling(10, min_periods=1).mean()
        df["temp_mean_J10"]       = df["temperature"].rolling(10,   min_periods=1).mean()
        df["pet_mean_J10"]        = df["pet"].rolling(10,           min_periods=1).mean()
        df["snow_depth_mean_J10"] = df["snow_depth"].rolling(10,    min_periods=1).mean()
        df["snowmelt_mean_J10"]   = df["snowmelt"].rolling(10,      min_periods=1).mean()
        df["precip_max_J10"]      = df["precipitation"].rolling(10, min_periods=1).max()
        df["precip_last7"]        = df["precipitation"].rolling(7,  min_periods=1).mean()
        df["nb_jours_pluie_J10"]  = (df["precipitation"] > 1.0).astype(float).rolling(10, min_periods=1).sum()

        doy = np.clip(df.index.dayofyear, 1, 365)
        df["doy_sin"] = np.sin(2 * np.pi * doy / 365).astype(np.float32)
        df["doy_cos"] = np.cos(2 * np.pi * doy / 365).astype(np.float32)

        clim_m, clim_s = compute_clim_fenetre(wse_series)
        df["clim_mean_20j"] = clim_m
        df["clim_std_20j"]  = clim_s

        df["water_level"] = wse_series.values

        df = df.rename(columns={
            "precipitation": "precipitation_J0",
            "temperature"  : "temperature_J0",
            "pet"          : "pet_J0",
            "snow_depth"   : "snow_depth_J0",
            "snowmelt"     : "snowmelt_J0",
        })

        # ── Export .nc ────────────────────────────────────────
        data_vars = {
            col: xr.Variable("date", df[col].values.astype(np.float32))
            for col in COLS_OUT if col in df.columns
        }
        ds = xr.Dataset(data_vars, coords={"date": df.index.values})
        ds.attrs["station_code"] = station_code
        ds.to_netcdf(nc_path, engine="scipy", format="NETCDF3_CLASSIC")

        attr_row = {"station_id": str(dahiti_id)}
        for col in STATIC_COLS:
            attr_row[col] = row.get(col, 0.0)
        attr_rows.append(attr_row)
        station_ids.append(str(dahiti_id))
        exported += 1

        n_wse = int(wse_series.dropna().shape[0])
        if (i + 1) % 50 == 0 or i < 5:
            print(f"  [{i+1:3d}/{len(stations)}] {name:40s} → {dahiti_id}.nc | {n_wse} WSE")

    except Exception as e:
        print(f"  [{i+1:3d}] {name} — erreur : {e}")
        errors += 1

conn.close()

# ── Attributes ─────────────────────────────────────────────────
df_attrs = pd.DataFrame(attr_rows).set_index("station_id")
df_attrs.to_csv(os.path.join(att_dir, "attributes.csv"))

# ── stations txt ───────────────────────────────────────────────
txt_path = os.path.join(OUTPUT_DIR, "stations_dahiti.txt")
with open(txt_path, "w") as f:
    f.write("\n".join(station_ids))

print(f"\n{'='*55}")
print(f"  .nc exportés : {exported}")
print(f"  Ignorés      : {skipped}")
print(f"  Erreurs      : {errors}")
print(f"  Dossier      : {OUTPUT_DIR}")
print(f"  stations.txt : {txt_path}")
print(f"{'='*55}")