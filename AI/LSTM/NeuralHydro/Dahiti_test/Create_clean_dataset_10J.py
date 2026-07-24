"""
step4_create_dataset_dahiti_10j_clean.py
════════════════════════════════════════════════════════════════════════
Recrée les .nc DAHITI 10j avec UNE SEULE mesure par passage satellite
(élimine les doublons S3A/S3B à 5j d'écart).

Différence avec step4_create_dataset_dahiti_27j_clean.py :
  - MIN_GAP_DAYS = 7 (passages ~10j, Sentinel-3 cycle court)
  - Sorties dans ./data/IA/NeuralHydrologyDahiti10jClean/

Sorties :
  - ./data/IA/NeuralHydrologyDahiti10jClean/10j/time_series/<id>.nc
  - ./data/IA/NeuralHydrologyDahiti10jClean/10j/attributes/attributes.csv
  - ./data/IA/NeuralHydrologyDahiti10jClean/stations_dahiti_10j.txt
════════════════════════════════════════════════════════════════════════
"""

import logging
import os
import sqlite3

import numpy as np
import pandas as pd
import xarray as xr

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("step4_dahiti_10j_clean")

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
DB_PATH      = "./data/dahiti.db"
OUTPUT_DIR   = "./data/IA/NeuralHydrologyDahiti10jClean"
DATE_DEB     = "2016-01-01"
DATE_FIN     = "2025-12-31"
CLIM_WINDOW  = 20
MIN_GAP_DAYS = 7    # passages ~10j — élimine doublons S3A/S3B à 5j

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
    "precip_mean_J27",  "temp_mean_J27",  "pet_mean_J27",
    "precip_max_J27", "precip_last7", "nb_jours_pluie_J27", "precip_mean_J14",
    "snow_depth_J0",       "snowmelt_J0",
    "snow_depth_mean_J3",  "snowmelt_mean_J3",
    "snow_depth_mean_J10", "snowmelt_mean_J10",
    "snow_depth_mean_J27", "snowmelt_mean_J27",
    "clim_mean", "clim_std",
    "clim_mean_20j", "clim_std_20j",
    "doy_sin", "doy_cos",
    "water_level",
]

# ═══════════════════════════════════════════════════════════════
# FILTRE 10j — UNE MESURE PAR PASSAGE
# ═══════════════════════════════════════════════════════════════
def filter_one_per_pass(df_wse: pd.DataFrame, min_gap: int = MIN_GAP_DAYS) -> pd.DataFrame:
    """
    Garde une seule mesure toutes les ~10j.
    Élimine les doublons S3A/S3B (qui sont à 1-2j d'écart sur cycle 10j).
    Prend la première mesure de chaque passage.
    """
    if df_wse.empty:
        return df_wse
    df_sorted = df_wse.sort_values("date").reset_index(drop=True)
    kept = [df_sorted.iloc[0]]
    for _, row in df_sorted.iterrows():
        if (row["date"] - kept[-1]["date"]).days >= min_gap:
            kept.append(row)
    return pd.DataFrame(kept).reset_index(drop=True)

# ═══════════════════════════════════════════════════════════════
# CLIMATOLOGIE FENÊTRÉE ±20j
# ═══════════════════════════════════════════════════════════════
def compute_clim_fenetre(wl_series: pd.Series, window: int = CLIM_WINDOW):
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
# CHARGEMENT BDD
# ═══════════════════════════════════════════════════════════════
def load_stations(conn):
    """Charge les stations 10j — missions à cycle court (Sentinel-6, Jason-3)."""
    return pd.read_sql("""
        SELECT
            s.station_code,
            s.hydroweb_name,
            s.mission_track AS inferred_mission,
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
          AND (s.mission_track LIKE '%S6%'
            OR s.mission_track LIKE '%J3%'
            OR s.mission_track LIKE '%Jason%')
        ORDER BY s.station_code
    """, conn)

def load_era5_station(conn, station_code, date_range):
    df = pd.read_sql("""
        SELECT e.date,
               e.precip_sum_bv AS precipitation,
               e.temp_moy_bv   AS temperature,
               e.pet_sum_bv    AS pet,
               e.snow_depth_bv AS snow_depth,
               e.snowmelt_bv   AS snowmelt
        FROM era5_bv_jour e
        WHERE e.station_code = ?
          AND e.date >= ? AND e.date <= ?
        ORDER BY e.date
    """, conn, params=(station_code, str(date_range[0].date()),
                       str(date_range[-1].date())))
    if df.empty:
        return pd.DataFrame(index=date_range)
    df["date"] = pd.to_datetime(df["date"])
    return df.set_index("date").reindex(date_range)

def load_wse_station_filtered(conn, station_code, date_range):
    """
    Charge les mesures WSE et applique le filtre une mesure par passage.
    Retourne une Series indexée sur date_range (NaN aux dates sans mesure).
    """
    df = pd.read_sql("""
        SELECT measure_date AS date, orthometric_height AS wl
        FROM measurements
        WHERE station_code = ?
          AND measure_date >= ? AND measure_date <= ?
          AND is_valid = 1
        ORDER BY measure_date
    """, conn, params=(station_code, str(date_range[0].date()),
                       str(date_range[-1].date())))

    out = pd.Series(index=date_range, dtype=float, name="wl")
    if df.empty:
        return out

    df["date"] = pd.to_datetime(df["date"])
    df = df.dropna(subset=["wl"])

    # Filtre une mesure par passage (~10j)
    df_filtered = filter_one_per_pass(df)

    log.info(f"    WSE : {len(df)} mesures BDD → {len(df_filtered)} après filtre 10j")

    # Place chaque mesure sur le jour le plus proche de la grille
    for _, row in df_filtered.iterrows():
        nearest = date_range[np.argmin(np.abs(date_range - row["date"]))]
        out.loc[nearest] = row["wl"]

    return out

# ═══════════════════════════════════════════════════════════════
# CONSTRUCTION DES FEATURES
# ═══════════════════════════════════════════════════════════════
def build_features(df_era5, wse_series, date_range):
    df = df_era5.copy()

    wl_mean = wse_series.mean()
    wl_std  = wse_series.std()
    if pd.isna(wl_std) or wl_std <= 0:
        return pd.DataFrame()

    df["water_level"] = (wse_series - wl_mean) / wl_std
    df["snow_depth"]  = df["snow_depth"].fillna(0)
    df["snowmelt"]    = df["snowmelt"].fillna(0)

    for win, suffix in [(3, "J3"), (10, "J10"), (27, "J27")]:
        df[f"precip_mean_{suffix}"]     = df["precipitation"].rolling(win, min_periods=1).mean()
        df[f"temp_mean_{suffix}"]       = df["temperature"].rolling(win,   min_periods=1).mean()
        df[f"pet_mean_{suffix}"]        = df["pet"].rolling(win,           min_periods=1).mean()
        df[f"snow_depth_mean_{suffix}"] = df["snow_depth"].rolling(win,    min_periods=1).mean()
        df[f"snowmelt_mean_{suffix}"]   = df["snowmelt"].rolling(win,      min_periods=1).mean()

    df["precip_max_J27"]     = df["precipitation"].rolling(27, min_periods=1).max()
    df["precip_last7"]       = df["precipitation"].rolling(7,  min_periods=1).mean()
    df["nb_jours_pluie_J27"] = (df["precipitation"] > 1.0).astype(float).rolling(27, min_periods=1).sum()
    df["precip_mean_J14"]    = df["precipitation"].rolling(14, min_periods=1).mean()

    doy = np.clip(date_range.dayofyear, 1, 365)
    df["doy_sin"] = np.sin(2 * np.pi * doy / 365).astype(np.float32)
    df["doy_cos"] = np.cos(2 * np.pi * doy / 365).astype(np.float32)

    clim_m20, clim_s20 = compute_clim_fenetre(df["water_level"], window=CLIM_WINDOW)
    df["clim_mean_20j"] = clim_m20
    df["clim_std_20j"]  = clim_s20
    df["clim_mean"]     = np.nan
    df["clim_std"]      = np.nan

    df = df.rename(columns={
        "precipitation": "precipitation_J0",
        "temperature"  : "temperature_J0",
        "pet"          : "pet_J0",
        "snow_depth"   : "snow_depth_J0",
        "snowmelt"     : "snowmelt_J0",
    })

    return df

# ═══════════════════════════════════════════════════════════════
# EXPORT .NC
# ═══════════════════════════════════════════════════════════════
def write_nc(df, nc_path, dahiti_id):
    data_vars = {
        col: xr.Variable("date", df[col].values.astype(np.float32))
        for col in COLS_OUT if col in df.columns
    }
    ds = xr.Dataset(data_vars, coords={"date": df.index.values})
    ds.attrs["dahiti_id"] = dahiti_id
    os.makedirs(os.path.dirname(nc_path), exist_ok=True)
    ds.to_netcdf(nc_path, engine="scipy", format="NETCDF3_CLASSIC")

# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════
def run():
    conn       = sqlite3.connect(DB_PATH)
    date_range = pd.date_range(DATE_DEB, DATE_FIN, freq="D")

    ts_dir  = os.path.join(OUTPUT_DIR, "10j", "time_series")
    att_dir = os.path.join(OUTPUT_DIR, "10j", "attributes")
    os.makedirs(ts_dir,  exist_ok=True)
    os.makedirs(att_dir, exist_ok=True)

    stations = load_stations(conn)
    log.info(f"{len(stations)} stations 10j chargées")

    attr_rows   = []
    station_ids = []
    exported = skipped = errors = 0

    for i, row in stations.iterrows():
        station_code = row["station_code"]
        dahiti_id    = int(station_code.lstrip("0") or 0)
        name         = row.get("hydroweb_name") or station_code
        nc_path      = os.path.join(ts_dir, f"{dahiti_id}.nc")

        try:
            df_era5 = load_era5_station(conn, station_code, date_range)
            if df_era5.empty or df_era5["precipitation"].isna().all():
                log.warning(f"  [{i+1:3d}] {name} — ERA5 vide → skip")
                skipped += 1
                continue

            wse_series = load_wse_station_filtered(conn, station_code, date_range)

            if wse_series.dropna().shape[0] < 5:
                log.warning(f"  [{i+1:3d}] {name} — WSE insuffisant → skip")
                skipped += 1
                continue

            df_feat = build_features(df_era5, wse_series, date_range)
            if df_feat.empty:
                log.warning(f"  [{i+1:3d}] {name} — features vides → skip")
                skipped += 1
                continue

            write_nc(df_feat, nc_path, dahiti_id)

            attr_row = {"station_id": str(dahiti_id)}
            for col in STATIC_COLS:
                attr_row[col] = row.get(col, 0.0)
            attr_rows.append(attr_row)
            station_ids.append(str(dahiti_id))
            exported += 1

            n_wse = int(wse_series.dropna().shape[0])
            log.info(f"  [{i+1:3d}/{len(stations)}] {name:45s} → {dahiti_id}.nc | {n_wse} WSE")

        except Exception as e:
            log.error(f"  [{i+1:3d}] {name} — erreur : {e}")
            errors += 1

    conn.close()

    # attributes.csv
    df_attrs = pd.DataFrame(attr_rows).set_index("station_id")
    df_attrs.to_csv(os.path.join(att_dir, "attributes.csv"))
    log.info(f"attributes.csv → {att_dir} ({len(df_attrs)} stations)")

    # stations txt
    txt_path = os.path.join(OUTPUT_DIR, "stations_dahiti_10j.txt")
    with open(txt_path, "w") as f:
        f.write("\n".join(station_ids))
    log.info(f"stations txt → {txt_path} ({len(station_ids)} stations)")

    print("\n" + "═" * 60)
    print("  RAPPORT")
    print("═" * 60)
    print(f"  .nc exportés : {exported}")
    print(f"  Ignorés      : {skipped}")
    print(f"  Erreurs      : {errors}")
    print(f"  Dossier      : {OUTPUT_DIR}/10j/")
    print("═" * 60)


if __name__ == "__main__":
    run()