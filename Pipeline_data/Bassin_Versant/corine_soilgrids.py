#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
═══════════════════════════════════════════════════════════════════════════
step2d_corine_soilgrids.py — Corine Land Cover + SoilGrids
═══════════════════════════════════════════════════════════════════════════

Pour chaque station avec BV, extrait les fractions d'occupation du sol
(Corine) et la texture des sols (SoilGrids 0-30cm).

Prérequis :
    pip install rasterio numpy pyproj shapely
    Fichiers : U2018_CLC2018_V2020_20u1.tif + SoilGrids/*.tif
═══════════════════════════════════════════════════════════════════════════
"""

import logging
import os
import sqlite3
from pathlib import Path

import numpy as np
import rasterio
from rasterio.mask import mask
from shapely.wkt import loads
from shapely.ops import transform
import pyproj

log = logging.getLogger("step2d")

DEFAULT_CORINE_PATH = "./data/Bassin_Versants/Corine/u2018_clc2018_v2020_20u1_raster100m/DATA/U2018_CLC2018_V2020_20u1.tif"
DEFAULT_SOILGRIDS_DIR = "./data/Bassin_Versants/SoilGrids/"

VARIABLES_SOIL = ["clay", "sand", "silt"]
DEPTHS = {"0-5cm_mean": 5, "5-15cm_mean": 10, "15-30cm_mean": 15}
CONV = {"clay": 0.1, "sand": 0.1, "silt": 0.1}

CODE_TO_CAT = {
    1: "urban", 2: "urban", 3: "urban", 4: "urban", 5: "urban",
    6: "urban", 7: "urban", 8: "urban", 9: "urban", 10: "urban", 11: "urban",
    12: "agriculture", 13: "agriculture", 14: "agriculture", 15: "agriculture",
    16: "agriculture", 17: "agriculture", 18: "agriculture", 19: "agriculture",
    20: "agriculture", 21: "agriculture", 22: "agriculture",
    23: "forest", 24: "forest", 25: "forest",
    26: "semi_natural", 27: "semi_natural", 28: "semi_natural",
    29: "semi_natural", 30: "semi_natural", 31: "semi_natural",
    32: "semi_natural", 33: "semi_natural", 34: "semi_natural",
    35: "wetland", 36: "wetland", 37: "wetland", 38: "wetland", 39: "wetland",
    40: "water", 41: "water", 42: "water", 43: "water", 44: "water",
}


def _reproject_3035(polygone_wgs84):
    """Reprojette un polygone WGS84 → EPSG:3035 (Corine)."""
    projet = pyproj.Transformer.from_crs(
        "EPSG:4326", "EPSG:3035", always_xy=True
    ).transform
    return transform(projet, polygone_wgs84)


def _extract_corine(polygone_wgs84, src):
    """Extrait les fractions Corine sur un polygone."""
    polygone_3035 = _reproject_3035(polygone_wgs84)
    out_image, _ = mask(src, [polygone_3035.__geo_interface__],
                        crop=True, nodata=-128)
    pixels = out_image[0].flatten()
    valid = pixels[(pixels != -128) & (pixels > 0)]

    if len(valid) == 0:
        return None

    fractions = {cat: 0.0 for cat in
                 ["urban", "agriculture", "forest", "semi_natural", "wetland", "water"]}
    for code, count in zip(*np.unique(valid, return_counts=True)):
        cat = CODE_TO_CAT.get(int(code))
        if cat:
            fractions[cat] += count / len(valid)

    return fractions


def _load_soilgrids(soilgrids_dir: str) -> dict:
    """Charge les rasters SoilGrids."""
    rasters = {}
    for var in VARIABLES_SOIL:
        rasters[var] = {}
        for depth_id in DEPTHS:
            path = os.path.join(soilgrids_dir, f"{var}_{depth_id}.tif")
            if os.path.exists(path):
                rasters[var][depth_id] = rasterio.open(path)
    return rasters


def _close_soilgrids(rasters: dict):
    """Ferme les rasters SoilGrids."""
    for var in rasters:
        for depth_id in rasters[var]:
            rasters[var][depth_id].close()


def _extract_soilgrids(polygone_wgs84, rasters: dict) -> dict:
    """Extrait la texture des sols pondérée par profondeur."""
    results = {}
    for var in VARIABLES_SOIL:
        if var not in rasters or not rasters[var]:
            results[var] = None
            continue

        weighted_sum, total_weight = 0.0, 0
        for depth_id, weight in DEPTHS.items():
            if depth_id not in rasters[var]:
                continue
            src = rasters[var][depth_id]
            try:
                out_image, _ = mask(src, [polygone_wgs84.__geo_interface__],
                                    crop=True, nodata=src.nodata)
                data = out_image[0].astype(float)
                if src.nodata is not None:
                    data[data == src.nodata] = np.nan
                data[data <= 0] = np.nan
                valid = data[~np.isnan(data)]
                if len(valid) > 0:
                    weighted_sum += np.mean(valid) * CONV[var] * weight
                    total_weight += weight
            except Exception:
                pass

        results[var] = weighted_sum / total_weight if total_weight > 0 else None
    return results


def run_step2d(conn: sqlite3.Connection,
               corine_path: str = DEFAULT_CORINE_PATH,
               soilgrids_dir: str = DEFAULT_SOILGRIDS_DIR) -> dict:
    """
    Étape 2d : extrait Corine + SoilGrids pour toutes les stations avec BV.

    Returns:
        {"updated": n, "errors": n}
    """
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from Pipeline_data.Database.db_operations import update_corine_soilgrids

    # Stations avec BV mais sans Corine
    stations = conn.execute("""
        SELECT b.station_code, b.polygone_wkt, s.river_name
        FROM bv_data b
        JOIN stations s ON b.station_code = s.station_code
        WHERE b.polygone_wkt IS NOT NULL
          AND s.frac_urban IS NULL
    """).fetchall()

    log.info(f"{len(stations)} stations à traiter")

    if not stations:
        return {"updated": 0, "errors": 0}

    # Charger SoilGrids
    rasters_soil = _load_soilgrids(soilgrids_dir)

    updated, errors_list = 0, []

    with rasterio.open(corine_path) as src_corine:
        for i, (station_code, wkt_str, river_name) in enumerate(stations):
            try:
                polygone = loads(wkt_str)

                # Corine
                fractions = _extract_corine(polygone, src_corine)
                if fractions is None:
                    errors_list.append((station_code, "Aucun pixel Corine"))
                    continue

                # SoilGrids
                soil = _extract_soilgrids(polygone, rasters_soil)

                # Insertion
                update_corine_soilgrids(conn, station_code, fractions, soil)
                updated += 1

                if (i + 1) % 20 == 0:
                    conn.commit()
                    log.info(f"  {i+1}/{len(stations)} traitées ({updated} OK)")

            except Exception as e:
                errors_list.append((station_code, str(e)[:100]))

    conn.commit()
    _close_soilgrids(rasters_soil)

    if errors_list:
        log.warning(f"{len(errors_list)} erreurs")

    log.info(f"Étape 2d terminée : {updated} stations, {len(errors_list)} erreurs")
    return {"updated": updated, "errors": len(errors_list)}


if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    parser = argparse.ArgumentParser(description="Étape 2d — Corine + SoilGrids")
    parser.add_argument("--db", type=str, default="./data/test.db")
    parser.add_argument("--corine", type=str, default=DEFAULT_CORINE_PATH)
    parser.add_argument("--soilgrids", type=str, default=DEFAULT_SOILGRIDS_DIR)
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    run_step2d(conn, args.corine, args.soilgrids)
    conn.close()