#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
═══════════════════════════════════════════════════════════════════════════
step3a_era5_pixels.py — Pixels ERA5 par bassin versant
═══════════════════════════════════════════════════════════════════════════

Pour chaque station avec BV, identifie les points de la grille ERA5-Land
(0.1°) qui tombent dans le polygone du BV → table era5_transfert.

Prérequis : pip install shapely numpy
═══════════════════════════════════════════════════════════════════════════
"""

import logging
import sqlite3
from pathlib import Path

import numpy as np
from shapely.wkt import loads
from shapely.geometry import Point

log = logging.getLogger("step3a")

# Grille ERA5-Land 0.1° sur la France
GRID_RES = 0.1
GRID_LON = np.arange(-6.0, 10.0 + GRID_RES, GRID_RES)
GRID_LAT = np.arange(41.0, 52.0 + GRID_RES, GRID_RES)


def find_pixels_in_polygon(polygone_wkt: str) -> list[tuple]:
    """
    Identifie les points de la grille ERA5 0.1° contenus dans un polygone.

    Args:
        polygone_wkt: Polygone en WKT (EPSG:4326)

    Returns:
        Liste de (lon, lat) des pixels ERA5 dans le BV
    """
    poly = loads(polygone_wkt)
    bounds = poly.bounds  # (minx, miny, maxx, maxy)

    # Filtrer la grille sur la bounding box pour accélérer
    lon_mask = (GRID_LON >= bounds[0] - GRID_RES) & (GRID_LON <= bounds[2] + GRID_RES)
    lat_mask = (GRID_LAT >= bounds[1] - GRID_RES) & (GRID_LAT <= bounds[3] + GRID_RES)
    lons_sub = GRID_LON[lon_mask]
    lats_sub = GRID_LAT[lat_mask]

    pixels = []
    for lon in lons_sub:
        for lat in lats_sub:
            if poly.contains(Point(lon, lat)):
                pixels.append((round(lon, 1), round(lat, 1)))

    # Si aucun pixel dedans (BV trop petit), prendre le centroïde
    if not pixels:
        c = poly.centroid
        pixels.append((round(c.x, 1), round(c.y, 1)))

    return pixels


def run_step3a(conn: sqlite3.Connection) -> dict:
    """
    Étape 3a : calcule les pixels ERA5 pour chaque BV.

    Returns:
        {"computed": n, "errors": n, "total_pixels": n}
    """
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from Pipeline_data.Database.db_operations import (
        get_stations_without_era5_pixels, insert_era5_pixels,
    )

    station_codes = get_stations_without_era5_pixels(conn)
    log.info(f"Stations sans pixels ERA5 : {len(station_codes)}")

    if not station_codes:
        log.info("Toutes les stations ont déjà leurs pixels ERA5")
        return {"computed": 0, "errors": 0, "total_pixels": 0}

    computed, errors, total_pixels = 0, 0, 0

    for i, code in enumerate(station_codes):
        try:
            # Récupérer le polygone
            row = conn.execute(
                "SELECT polygone_wkt FROM bv_data WHERE station_code = ?",
                (code,)
            ).fetchone()

            if not row or not row[0]:
                log.warning(f"  {code} — pas de polygone")
                errors += 1
                continue

            pixels = find_pixels_in_polygon(row[0])
            insert_era5_pixels(conn, code, pixels)
            total_pixels += len(pixels)
            computed += 1

            if (i + 1) % 20 == 0:
                log.info(f"  {i+1}/{len(station_codes)} traitées")

        except Exception as e:
            log.error(f"  {code} — ERREUR : {e}")
            errors += 1

    log.info(f"Étape 3a terminée : {computed} stations, {total_pixels} pixels total, {errors} erreurs")
    return {"computed": computed, "errors": errors, "total_pixels": total_pixels}


if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    parser = argparse.ArgumentParser(description="Étape 3a — Pixels ERA5 par BV")
    parser.add_argument("--db", type=str, default="./data/test.db")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    run_step3a(conn)
    conn.close()