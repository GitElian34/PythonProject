#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
═══════════════════════════════════════════════════════════════════════════
step2a_delineate_bv.py — Délinéation des bassins versants (pysheds)
═══════════════════════════════════════════════════════════════════════════

Pour chaque station sans BV, calcule le bassin versant à partir des
rasters HydroSHEDS (flow direction + flow accumulation) et insère
le polygone + aire dans bv_data.

Prérequis :
    pip install pysheds geopandas rasterio shapely

Données nécessaires :
    - hyd_eu_dir_15s.tif  (HydroSHEDS flow direction Europe)
    - hyd_eu_acc_15s.tif  (HydroSHEDS flow accumulation Europe)
═══════════════════════════════════════════════════════════════════════════
"""

import logging
import os
import sqlite3
import tempfile
from pathlib import Path

import geopandas as gpd
import rasterio
from rasterio.windows import from_bounds
from pysheds.grid import Grid
from shapely.geometry import shape

log = logging.getLogger("step2a")

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES PAR DÉFAUT
# ═══════════════════════════════════════════════════════════════
DEFAULT_DIR_PATH = "/home/sar_hydro/STUDIES/EtudesEB/HydroSHED/other/hyd_eu_dir_15s.tif"
DEFAULT_ACC_PATH = "/home/sar_hydro/STUDIES/EtudesEB/HydroSHED/other/hyd_eu_acc_15s.tif"
BBOX_FRANCE = {"left": -6.0, "right": 10.0, "bottom": 41.0, "top": 52.0}
ACC_THRESHOLD = 500  # seuil snap_to_mask


# ═══════════════════════════════════════════════════════════════
# FONCTIONS
# ═══════════════════════════════════════════════════════════════

def clip_rasters_france(dir_path: str, acc_path: str,
                        bbox: dict = BBOX_FRANCE) -> tuple[str, str]:
    """
    Clippe les rasters DIR et ACC sur la France dans des fichiers temporaires.
    Retourne (tmp_dir_path, tmp_acc_path).
    """
    log.info("Clipping des rasters sur la France...")
    tmp_dir = tempfile.mkdtemp()
    tmp_dir_path = os.path.join(tmp_dir, "dir_france.tif")
    tmp_acc_path = os.path.join(tmp_dir, "acc_france.tif")

    for src_path, dst_path in [(dir_path, tmp_dir_path), (acc_path, tmp_acc_path)]:
        with rasterio.open(src_path) as src:
            win = from_bounds(
                bbox["left"], bbox["bottom"],
                bbox["right"], bbox["top"],
                src.transform,
            )
            data = src.read(1, window=win)
            transform = src.window_transform(win)
            profile = src.profile.copy()
            profile.update({
                "height": data.shape[0],
                "width": data.shape[1],
                "transform": transform,
            })
            with rasterio.open(dst_path, "w", **profile) as dst:
                dst.write(data, 1)

    log.info(f"Rasters clippés dans {tmp_dir}")
    return tmp_dir_path, tmp_acc_path


def compute_watershed(lon: float, lat: float,
                      dir_path: str, acc_path: str) -> tuple[float, str]:
    """
    Calcule le BV d'un point (lon, lat).

    Returns:
        (aire_km2, polygone_wkt)
    """
    grid = Grid.from_raster(dir_path)
    fdir = grid.read_raster(dir_path)
    acc = grid.read_raster(acc_path)

    # Snap le point sur le pixel d'accumulation le plus proche
    xs, ys = grid.snap_to_mask(acc > ACC_THRESHOLD, (lon, lat))

    # Calcul du catchment
    catch = grid.catchment(x=xs, y=ys, fdir=fdir)

    # Aire en km² (chaque pixel ~15 arcsec ≈ 0.0625 km² à cette latitude)
    aire = round(float(catch.sum()) * 0.0625, 1)

    # Polygoniser
    grid.clip_to(catch)
    shapes = grid.polygonize(grid.view(catch).astype("uint8"))
    poly = gpd.GeoDataFrame(
        geometry=[shape(s) for s, v in shapes if v == 1],
        crs="EPSG:4326",
    ).dissolve().geometry.iloc[0]

    return aire, poly.wkt


def run_step2a(conn: sqlite3.Connection,
               dir_path: str = DEFAULT_DIR_PATH,
               acc_path: str = DEFAULT_ACC_PATH) -> dict:
    """
    Étape 2a : calcule les BV pour toutes les stations sans BV.

    Args:
        conn: Connexion à la BDD pipeline
        dir_path: Chemin vers le raster flow direction
        acc_path: Chemin vers le raster flow accumulation

    Returns:
        {"computed": n, "errors": n}
    """
    # Import ici pour éviter la dépendance circulaire au niveau module
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from Pipeline_data.Database.db_operations import get_stations_without_bv, insert_bv

    log.info([dir_path, acc_path])
    stations = get_stations_without_bv(conn)
    log.info(f"Stations sans BV : {len(stations)}")

    if not stations:
        log.info("Toutes les stations ont déjà un BV")
        return {"computed": 0, "errors": 0}

    # Vérifier que les rasters existent
    log.info("0")
    log.info([dir_path, acc_path])
    for p in [dir_path, acc_path]:
        log.info(p)
        if not os.path.exists(p):
            log.error(f"Raster introuvable : {p}")
            return {"computed": 0, "errors": len(stations)}

    # Clipper une seule fois
    log.info("1")
    tmp_dir, tmp_acc = clip_rasters_france(dir_path, acc_path)

    computed, errors = 0, 0
    total = len(stations)

    for i, sta in enumerate(stations):
        code = sta["station_code"]
        log.info(f"[{i+1}/{total}] {code} ({sta.get('hydroweb_name', '?')})...")
        log.info("2")
        try:
            aire, wkt = compute_watershed(
                sta["lon"], sta["lat"], tmp_dir, tmp_acc
            )
            insert_bv(conn, code, sta.get("hydroweb_name"), aire, wkt)

            # Aussi mettre à jour upstream_watershed_km2 dans stations
            conn.execute(
                "UPDATE stations SET upstream_watershed_km2 = ? WHERE station_code = ?",
                (aire, code)
            )
            conn.commit()

            log.info(f"  → {aire} km²")
            computed += 1

        except Exception as e:
            log.error(f"  → ERREUR : {e}")
            errors += 1
    log.info("3")
    log.info(f"Étape 2a terminée : {computed} BV calculés, {errors} erreurs")
    return {"computed": computed, "errors": errors}


# ═══════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    parser = argparse.ArgumentParser(description="Étape 2a — Délinéation des BV")
    parser.add_argument("--db", type=str, default="./data/test.db")
    parser.add_argument("--dir", type=str, default=DEFAULT_DIR_PATH)
    parser.add_argument("--acc", type=str, default=DEFAULT_ACC_PATH)
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    log.info([args.dir, args.acc])
    run_step2a(conn, args.dir, args.acc)
    conn.close()