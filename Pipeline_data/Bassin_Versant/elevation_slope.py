#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
═══════════════════════════════════════════════════════════════════════════
step2c_elevation_slope.py — Elevation et slope (SRTM)
═══════════════════════════════════════════════════════════════════════════

Pour chaque station avec BV, calcule les stats zonales d'elevation
et de slope sur le polygone du BV.

Prérequis :
    pip install rasterstats shapely
    gdaldem (GDAL CLI)
    Fichier : srtm_france.tif
═══════════════════════════════════════════════════════════════════════════
"""

import logging
import os
import sqlite3
import subprocess
from pathlib import Path

from rasterstats import zonal_stats
from shapely import wkt
from shapely.geometry import mapping
import warnings
warnings.filterwarnings("ignore")

log = logging.getLogger("step2c")

DEFAULT_DEM_PATH = "./data/Elevation/srtm_france.tif"
DEFAULT_SLOPE_PATH = "./data/Elevation/slope_france.tif"


def ensure_slope_raster(dem_path: str, slope_path: str):
    """Calcule le raster slope si il n'existe pas."""
    if os.path.exists(slope_path):
        log.info(f"Slope raster existant : {slope_path}")
        return

    log.info(f"Calcul du raster slope depuis {dem_path}...")
    cmd = ["gdaldem", "slope", dem_path, slope_path,
           "-compute_edges", "-p", "-s", "111120"]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"gdaldem erreur : {result.stderr}")
    log.info(f"Slope raster créé : {slope_path}")


def run_step2c(conn: sqlite3.Connection,
               dem_path: str = DEFAULT_DEM_PATH,
               slope_path: str = DEFAULT_SLOPE_PATH) -> dict:
    """
    Étape 2c : calcule elevation/slope pour toutes les stations avec BV.

    Returns:
        {"updated": n, "errors": n}
    """
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from Pipeline_data.Database.db_operations import get_stations_with_bv, update_elevation_slope

    if not os.path.exists(dem_path):
        log.error(f"MNT introuvable : {dem_path}")
        return {"updated": 0, "errors": 0}

    ensure_slope_raster(dem_path, slope_path)

    # Stations avec BV mais sans elevation
    stations = conn.execute("""
        SELECT b.station_code, b.polygone_wkt
        FROM bv_data b
        JOIN stations s ON b.station_code = s.station_code
        WHERE b.polygone_wkt IS NOT NULL
          AND s.elevation_mean IS NULL
    """).fetchall()

    log.info(f"{len(stations)} stations à traiter")

    updated, errors_list = 0, []

    for i, (station_code, polygon_wkt) in enumerate(stations):
        try:
            geom = mapping(wkt.loads(polygon_wkt))

            stats_elev = zonal_stats(geom, dem_path, stats=["mean", "std"], nodata=-32768)
            stats_slope = zonal_stats(geom, slope_path, stats=["mean", "std"], nodata=-9999)

            elev_mean = stats_elev[0]["mean"]
            elev_std = stats_elev[0]["std"]
            slope_mean = stats_slope[0]["mean"]
            slope_std = stats_slope[0]["std"]

            if elev_mean is None or slope_mean is None:
                errors_list.append((station_code, "stats None"))
                continue

            update_elevation_slope(conn, station_code,
                                   elev_mean, elev_std, slope_mean, slope_std)
            updated += 1

            if (i + 1) % 50 == 0:
                conn.commit()
                log.info(f"  {i+1}/{len(stations)} traitées ({updated} OK)")

        except Exception as e:
            errors_list.append((station_code, str(e)[:100]))

    conn.commit()

    if errors_list:
        log.warning(f"{len(errors_list)} erreurs :")
        for code, msg in errors_list[:5]:
            log.warning(f"  {code} : {msg}")

    log.info(f"Étape 2c terminée : {updated} stations, {len(errors_list)} erreurs")
    return {"updated": updated, "errors": len(errors_list)}


if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    parser = argparse.ArgumentParser(description="Étape 2c — Elevation / Slope")
    parser.add_argument("--db", type=str, default="./data/test.db")
    parser.add_argument("--dem", type=str, default=DEFAULT_DEM_PATH)
    parser.add_argument("--slope", type=str, default=DEFAULT_SLOPE_PATH)
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    run_step2c(conn, args.dem, args.slope)
    conn.close()