#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
═══════════════════════════════════════════════════════════════════════════
step2b_strahler.py — Ordre de Strahler (RiverATLAS)
═══════════════════════════════════════════════════════════════════════════

Pour chaque station, trouve le tronçon RiverATLAS le plus proche et
extrait l'ordre de Strahler.

Prérequis :
    pip install geopandas pandas
    Fichier : RiverATLAS_v10_eu.shp
═══════════════════════════════════════════════════════════════════════════
"""

import logging
import sqlite3
from pathlib import Path

import geopandas as gpd
import pandas as pd

log = logging.getLogger("step2b")

DEFAULT_RIVER_ATLAS = "./data/HydroSHED/RiverATLAS_v10_eu.shp"
FRANCE_BBOX = (-5.5, 41.0, 10.0, 51.5)
DIST_SEUIL_M = 5000


def run_step2b(conn: sqlite3.Connection,
               river_atlas_path: str = DEFAULT_RIVER_ATLAS) -> dict:
    """
    Étape 2b : calcule l'ordre de Strahler pour toutes les stations.

    Returns:
        {"updated": n, "suspects": n}
    """
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from Pipeline_data.Database.db_operations import update_strahler_batch

    # 1. Charger RiverATLAS (France uniquement)
    log.info("Chargement RiverATLAS (France)...")
    rivers = gpd.read_file(river_atlas_path, bbox=FRANCE_BBOX)
    log.info(f"  {len(rivers)} tronçons chargés")

    if "ORD_STRA" not in rivers.columns:
        log.error("Colonne ORD_STRA introuvable dans RiverATLAS")
        return {"updated": 0, "suspects": 0}

    # 2. Charger les stations
    stations = pd.read_sql("""
        SELECT station_code, reference_longitude AS lon, reference_latitude AS lat
        FROM stations
        WHERE reference_longitude IS NOT NULL AND reference_latitude IS NOT NULL
          AND strahler IS NULL
    """, conn)
    log.info(f"  {len(stations)} stations sans Strahler")

    if stations.empty:
        log.info("Toutes les stations ont déjà un Strahler")
        return {"updated": 0, "suspects": 0}

    # 3. GeoDataFrame + reprojection mètres
    stations_gdf = gpd.GeoDataFrame(
        stations,
        geometry=gpd.points_from_xy(stations.lon, stations.lat),
        crs="EPSG:4326",
    ).to_crs("EPSG:3857")

    rivers = rivers.to_crs("EPSG:3857")

    # 4. sjoin_nearest
    log.info("Matching stations → tronçons RiverATLAS...")
    result = gpd.sjoin_nearest(
        stations_gdf,
        rivers[["geometry", "ORD_STRA"]],
        how="left",
        distance_col="dist_m",
    )

    # 5. Contrôle qualité
    suspects = result[result["dist_m"] > DIST_SEUIL_M]
    if not suspects.empty:
        log.warning(f"{len(suspects)} stations à >{DIST_SEUIL_M/1000:.0f}km d'un tronçon")

    # 6. Insérer
    valid = result.dropna(subset=["ORD_STRA"])
    updates = [(int(row["ORD_STRA"]), row["station_code"]) for _, row in valid.iterrows()]
    n = update_strahler_batch(conn, updates)

    log.info(f"Étape 2b terminée : {n} stations avec Strahler")
    return {"updated": n, "suspects": len(suspects)}


if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    parser = argparse.ArgumentParser(description="Étape 2b — Strahler")
    parser.add_argument("--db", type=str, default="./data/test.db")
    parser.add_argument("--atlas", type=str, default=DEFAULT_RIVER_ATLAS)
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    run_step2b(conn, args.atlas)
    conn.close()