#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
═══════════════════════════════════════════════════════════════════════════
step4_create_dataset_dahiti.py — Étape 4 : BDD DAHITI → fichiers .nc
═══════════════════════════════════════════════════════════════════════════

Génère par sous-dossier (27j / 10j / autres) :
  - Un fichier .nc par station (variables dynamiques)
  - attributes/attributes.csv (attributs statiques pour NeuralHydrology)
  - stations_dahiti_<freq>.txt (liste des IDs pour le yaml)

Colonnes attributes.csv (= static_attributes du yaml) :
  station_id, aire_km2, lon, lat,
  frac_urban, frac_forest, frac_agriculture,
  sg_clay_0_30cm, sg_sand_0_30cm, sg_silt_0_30cm,
  strahler, elevation_mean, slope_mean

Usage :
    python step4_create_dataset_dahiti.py
    python step4_create_dataset_dahiti.py --db ./data/dahiti.db
    python step4_create_dataset_dahiti.py --reset
═══════════════════════════════════════════════════════════════════════════
"""

import argparse
import logging
import os
import shutil
import sqlite3

import numpy as np
import pandas as pd
import xarray as xr

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("step4_dahiti")

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES GLOBAUX
# ═══════════════════════════════════════════════════════════════
DB_PATH     = "./data/dahiti.db"
OUTPUT_DIR  = "./data/IA/NeuralHydrologyDahitiFull"
DATE_DEB    = "2016-01-01"
DATE_FIN    = "2025-12-31"
RESET       = False
CLIM_WINDOW = 20

# SUBDIR_MAP   = {"S3A/S3B": "27j", "J3/S6A": "10j"} pour Dahiti
SUBDIR_MAP = {
    "S3A" : "27j",
    "S3B" : "27j",
    "J3"  : "10j",
    "J2"  : "10j",
    "S6A" : "10j",
    "S6B" : "10j",
    "SWOT": "21j",   # SWOT = 21j, nouveau sous-dossier Pour hdroweb hysope nova
}
SUBDIR_OTHER = "autres"
ALL_SUBDIRS = list(dict.fromkeys(list(SUBDIR_MAP.values()) + [SUBDIR_OTHER]))
# → ["27j", "10j", "21j", "autres"]

# Colonnes static_attributes attendues par le modèle
# Valeur par défaut si absente de la BDD DAHITI → 0.0 (mieux que NaN pour l'inférence)
STATIC_COLS = [
    "aire_km2", "lon", "lat",
    "frac_urban", "frac_forest", "frac_agriculture",
    "sg_clay_0_30cm", "sg_sand_0_30cm", "sg_silt_0_30cm",
    "strahler", "elevation_mean", "slope_mean",
]


# ═══════════════════════════════════════════════════════════════
# CLIMATOLOGIE FENÊTRÉE ±20j (leave-one-year-out)
# ═══════════════════════════════════════════════════════════════
def compute_clim_fenetre(wl_series: pd.Series,
                         window: int = CLIM_WINDOW) -> tuple[np.ndarray, np.ndarray]:
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
def load_stations(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Charge toutes les stations avec leurs attributs statiques.
    Les colonnes manquantes dans DAHITI sont remplies à 0.0.
    """
    # Colonnes disponibles dans dahiti.db après étapes 2-3
    df = pd.read_sql("""
        SELECT
            s.station_code,
            s.hydroweb_name,
            s.mission_track          AS inferred_mission,
            s.reference_longitude    AS lon,
            s.reference_latitude     AS lat,
            -- Attributs remplis par step2
            COALESCE(b.aire_km2,       0.0) AS aire_km2,
            COALESCE(s.frac_urban,     0.0) AS frac_urban,
            COALESCE(s.frac_forest,    0.0) AS frac_forest,
            COALESCE(s.frac_agriculture, 0.0) AS frac_agriculture,
            COALESCE(s.sg_clay_0_30cm, 0.0) AS sg_clay_0_30cm,
            COALESCE(s.sg_sand_0_30cm, 0.0) AS sg_sand_0_30cm,
            COALESCE(s.sg_silt_0_30cm, 0.0) AS sg_silt_0_30cm,
            COALESCE(s.strahler,       0.0) AS strahler,
            COALESCE(s.elevation_mean, 0.0) AS elevation_mean,
            COALESCE(s.slope_mean,     0.0) AS slope_mean
        FROM stations s
        LEFT JOIN bv_data b ON s.station_code = b.station_code
        WHERE s.reference_longitude IS NOT NULL
          AND s.reference_latitude  IS NOT NULL
        ORDER BY s.station_code
    """, conn)
    return df


def load_era5_station(conn: sqlite3.Connection,
                      station_code: str,
                      date_range: pd.DatetimeIndex) -> pd.DataFrame:
    df = pd.read_sql("""
        SELECT
            e.date,
            e.precip_sum_bv  AS precipitation,
            e.temp_moy_bv    AS temperature,
            e.pet_sum_bv     AS pet,
            e.snow_depth_bv  AS snow_depth,
            e.snowmelt_bv    AS snowmelt
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


def load_wse_station(conn: sqlite3.Connection,
                     station_code: str,
                     date_range: pd.DatetimeIndex) -> pd.DataFrame:
    df = pd.read_sql("""
        SELECT measure_date,
               orthometric_height AS wse,
               uncertainty        AS wse_u
        FROM measurements
        WHERE station_code = ?
          AND measure_date >= ? AND measure_date <= ?
          AND is_valid = 1
        ORDER BY measure_date
    """, conn, params=(station_code, str(date_range[0].date()),
                       str(date_range[-1].date())))

    out = pd.DataFrame(index=date_range, columns=["wse", "wse_u"], dtype=float)
    if df.empty:
        return out

    df["measure_date"] = pd.to_datetime(df["measure_date"])
    df = df.set_index("measure_date").groupby(level=0).first()  # dédoublonnage

    for idx in df.index:
        nearest = date_range[np.argmin(np.abs(date_range - idx))]
        out.loc[nearest, "wse"]   = df.loc[idx, "wse"]
        out.loc[nearest, "wse_u"] = df.loc[idx, "wse_u"]

    return out


# ═══════════════════════════════════════════════════════════════
# CONSTRUCTION DES FEATURES
# ═══════════════════════════════════════════════════════════════
def build_features(df_era5: pd.DataFrame,
                   wse_series: pd.Series,
                   date_range: pd.DatetimeIndex) -> pd.DataFrame:
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
# EXPORT .NC
# ═══════════════════════════════════════════════════════════════
def write_nc(df: pd.DataFrame, nc_path: str, dahiti_id: int):
    data_vars = {
        col: xr.Variable("date", df[col].values.astype(np.float32))
        for col in COLS_OUT if col in df.columns
    }
    ds = xr.Dataset(data_vars, coords={"date": df.index.values})
    ds.attrs["dahiti_id"] = dahiti_id
    os.makedirs(os.path.dirname(nc_path), exist_ok=True)
    ds.to_netcdf(nc_path, engine="scipy", format="NETCDF3_CLASSIC")


# ═══════════════════════════════════════════════════════════════
# EXPORT ATTRIBUTES.CSV + STATIONS TXT
# ═══════════════════════════════════════════════════════════════
def write_attributes(rows: list[dict], output_dir: str, subdir: str):
    """
    Écrit attributes/attributes.csv dans output_dir/subdir/.
    Format attendu par NeuralHydrology : station_id en index, une colonne par attribut.
    """
    if not rows:
        return
    df = pd.DataFrame(rows)
    df = df.set_index("station_id")
    attrs_dir = os.path.join(output_dir, subdir, "attributes")
    os.makedirs(attrs_dir, exist_ok=True)
    path = os.path.join(attrs_dir, "attributes.csv")
    df.to_csv(path)
    log.info(f"  attributes.csv → {path} ({len(df)} stations)")


def write_stations_txt(station_ids: list[str], output_dir: str, subdir: str):
    """
    Écrit stations_dahiti_<subdir>.txt dans output_dir/
    pour pointer depuis le yaml.
    """
    if not station_ids:
        return
    path = os.path.join(output_dir, f"stations_dahiti_{subdir}.txt")
    with open(path, "w") as f:
        f.write("\n".join(station_ids))
    log.info(f"  stations txt → {path} ({len(station_ids)} stations)")


# ═══════════════════════════════════════════════════════════════
# FONCTION PRINCIPALE
# ═══════════════════════════════════════════════════════════════
def run_step4(db_path: str   = DB_PATH,
              output_dir: str = OUTPUT_DIR,
              date_deb: str  = DATE_DEB,
              date_fin: str  = DATE_FIN,
              reset: bool    = RESET) -> dict:

    conn       = sqlite3.connect(db_path)
    date_range = pd.date_range(date_deb, date_fin, freq="D")

    # Créer / reset les dossiers
    for subdir in ALL_SUBDIRS:
        path = os.path.join(output_dir, subdir)
        if reset and os.path.exists(path):
            shutil.rmtree(path)
            log.info(f"Dossier supprimé : {path}")
        os.makedirs(os.path.join(path, "time_series"), exist_ok=True)
        os.makedirs(os.path.join(path, "attributes"),  exist_ok=True)

    stations = load_stations(conn)
    log.info(f"{len(stations)} stations chargées")

    # Accumulateurs par sous-dossier
    attrs_by_subdir:    dict[str, list] = {s: [] for s in ALL_SUBDIRS}
    stations_by_subdir: dict[str, list] = {s: [] for s in ALL_SUBDIRS}

    exported = skipped = errors = 0

    for i, row in stations.iterrows():
        station_code = row["station_code"]
        dahiti_id    = int(station_code.lstrip("0") or 0)
        name = row.get("hydroweb_name") or row.get("station_code", f"ID_{station_code}")
        mission      = str(row.get("inferred_mission") or "")

        # subdir = SUBDIR_OTHER
        # for key, val in SUBDIR_MAP.items():   Version Dahiti
        #     if key in mission:
        #         subdir = val
        #         break

        subdir = SUBDIR_OTHER
        for key, val in SUBDIR_MAP.items():
            if key in str(mission):
                subdir = val
                break

        nc_path = os.path.join(output_dir, subdir, "time_series", f"{dahiti_id}.nc")

        # Attributs statiques — toujours accumulés même si .nc déjà existant
        attr_row = {"station_id": str(dahiti_id)}
        for col in STATIC_COLS:
            attr_row[col] = row.get(col, 0.0)
        # lon/lat depuis la station directement
        attr_row["lon"] = row.get("lon", 0.0)
        attr_row["lat"] = row.get("lat", 0.0)

        if os.path.exists(nc_path) and not reset:
            log.info(f"  [{i+1:3d}/{len(stations)}] {name} — .nc existant → skip")
            attrs_by_subdir[subdir].append(attr_row)
            stations_by_subdir[subdir].append(str(dahiti_id))
            exported += 1
            continue

        try:
            df_era5 = load_era5_station(conn, station_code, date_range)
            if df_era5.empty or df_era5["precipitation"].isna().all():
                log.warning(f"  [{i+1:3d}/{len(stations)}] {name} — ERA5 vide → skip")
                skipped += 1
                continue

            df_wse     = load_wse_station(conn, station_code, date_range)
            wse_series = df_wse["wse"]

            if wse_series.dropna().shape[0] < 5:
                log.warning(f"  [{i+1:3d}/{len(stations)}] {name} — WSE vide → skip")
                skipped += 1
                continue

            df_feat = build_features(df_era5, wse_series, date_range)
            if df_feat.empty:
                log.warning(f"  [{i+1:3d}/{len(stations)}] {name} — features vides → skip")
                skipped += 1
                continue

            write_nc(df_feat, nc_path, dahiti_id)

            attrs_by_subdir[subdir].append(attr_row)
            stations_by_subdir[subdir].append(str(dahiti_id))
            exported += 1

            log.info(
                f"  [{i+1:3d}/{len(stations)}] {name:45s} → {subdir}/"
                f"{dahiti_id}.nc | {int(wse_series.dropna().shape[0])} WSE "
                f"| mission={mission or '?'}"
            )

        except Exception as e:
            log.error(f"  [{i+1:3d}/{len(stations)}] {name} — erreur : {e}")
            errors += 1

    conn.close()

    # ── Écriture attributes.csv + stations txt par sous-dossier ──
    for subdir in ALL_SUBDIRS:
        write_attributes(attrs_by_subdir[subdir],    output_dir, subdir)
        write_stations_txt(stations_by_subdir[subdir], output_dir, subdir)

    # ── Rapport ──────────────────────────────────────────────────
    print("\n" + "═" * 60)
    print("  RAPPORT ÉTAPE 4")
    print("═" * 60)
    print(f"  .nc exportés : {exported}")
    print(f"  Ignorés      : {skipped}")
    print(f"  Erreurs      : {errors}")
    print(f"\n  Par fréquence :")
    for subdir in ALL_SUBDIRS:
        ts_path = os.path.join(output_dir, subdir, "time_series")
        n = len([f for f in os.listdir(ts_path) if f.endswith(".nc")])
        print(f"    {subdir:8s} : {n} .nc  |  {len(attrs_by_subdir[subdir])} lignes attributes.csv")
    print(f"\n  Dossier : {output_dir}")
    print("═" * 60)

    return {"exported": exported, "skipped": skipped, "errors": errors}


# ═══════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Étape 4 DAHITI — BDD → .nc + attributes.csv")
    parser.add_argument("--db",     type=str, default=DB_PATH)
    parser.add_argument("--output", type=str, default=OUTPUT_DIR)
    parser.add_argument("--deb",    type=str, default=DATE_DEB)
    parser.add_argument("--fin",    type=str, default=DATE_FIN)
    parser.add_argument("--reset",  action="store_true")
    args = parser.parse_args()

    run_step4(
        db_path    = args.db,
        output_dir = args.output,
        date_deb   = args.deb,
        date_fin   = args.fin,
        reset      = args.reset,
    )