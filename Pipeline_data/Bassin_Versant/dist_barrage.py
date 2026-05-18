#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
═══════════════════════════════════════════════════════════════════════════
step2e_dist_barrage.py — Distance au barrage le plus proche (ROE)
═══════════════════════════════════════════════════════════════════════════

Télécharge le ROE (si pas déjà en local), l'insère dans la BDD pipeline,
puis calcule la distance haversine au barrage le plus proche pour chaque
station.

Prérequis :
    pip install geopandas requests
═══════════════════════════════════════════════════════════════════════════
"""

import io
import logging
import os
import sqlite3
import zipfile
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import requests

log = logging.getLogger("step2e")

ROE_DIR = Path("./data/insitu/Barrages/dataset")
ROE_URLS = [
    "https://www.data.gouv.fr/api/1/datasets/r/b7f5faef-6f41-4e78-9c41-826c09c72d52",
    "https://www.data.gouv.fr/api/1/datasets/r/2fe5ad95-480b-4d65-884b-f08a272d73bc",
]


def _download_roe(roe_dir: Path) -> Path | None:
    """Télécharge le ROE France si pas déjà présent."""
    roe_dir.mkdir(parents=True, exist_ok=True)
    shp_files = list(roe_dir.glob("*.shp"))

    if shp_files:
        log.info(f"ROE déjà présent : {shp_files[0]}")
        return shp_files[0]

    log.info("Téléchargement du ROE France...")
    for url in ROE_URLS:
        try:
            resp = requests.get(url, timeout=120)
            resp.raise_for_status()
            if resp.content[:4] == b"PK\x03\x04":
                z = zipfile.ZipFile(io.BytesIO(resp.content))
                z.extractall(roe_dir)
                shp_files = list(roe_dir.glob("*.shp"))
                if shp_files:
                    log.info(f"ROE extrait : {shp_files[0]}")
                    return shp_files[0]
        except Exception as e:
            log.warning(f"Erreur téléchargement ROE : {e}")

    log.error("Impossible de télécharger le ROE")
    return None


def _load_and_filter_roe(shp_path: Path) -> list[tuple]:
    """Charge le shapefile ROE, filtre les barrages, retourne une liste de tuples."""
    gdf = gpd.read_file(shp_path).to_crs(epsg=4326)
    log.info(f"  {len(gdf)} obstacles au total")

    # Filtre barrages uniquement (CdTypeOuvr 1.1.x)
    gdf = gdf[gdf["CdTypeOuvr"].str.startswith("1.1", na=False)].copy()
    log.info(f"  {len(gdf)} barrages retenus")

    gdf["lon"] = gdf.geometry.x
    gdf["lat"] = gdf.geometry.y
    gdf = gdf.dropna(subset=["lon", "lat"])

    batch = [
        (
            str(row["CdObstEcou"]),
            str(row["NomPrincip"]) if row["NomPrincip"] else None,
            str(row["LbTypeOuvr"]),
            float(row["lon"]),
            float(row["lat"]),
        )
        for _, row in gdf.iterrows()
    ]
    return batch


def _haversine_min_distance(lat1, lon1, roe_lats, roe_lons):
    """Distance minimale en mètres entre un point et tous les barrages ROE."""
    R = 6_371_000.0
    lat1_r = np.radians(lat1)
    lon1_r = np.radians(lon1)
    dlat = roe_lats - lat1_r
    dlon = roe_lons - lon1_r
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1_r) * np.cos(roe_lats) * np.sin(dlon / 2) ** 2
    return int(R * 2 * np.arcsin(np.sqrt(np.clip(a, 0, 1))).min())


def run_step2e(conn: sqlite3.Connection,
               roe_dir: Path = ROE_DIR) -> dict:
    """
    Étape 2e : insère le ROE puis calcule la distance au barrage pour chaque station.

    Returns:
        {"roe_count": n, "updated": n}
    """
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from Pipeline_data.Database.db_operations import insert_roe_batch, update_dist_barrage

    # 1. Vérifier si le ROE est déjà en BDD
    roe_count = conn.execute("SELECT COUNT(*) FROM roe_obstacles").fetchone()[0]

    if roe_count == 0:
        # Télécharger et insérer
        shp_path = _download_roe(roe_dir)
        if shp_path is None:
            return {"roe_count": 0, "updated": 0}

        batch = _load_and_filter_roe(shp_path)
        roe_count = insert_roe_batch(conn, batch)

    log.info(f"ROE : {roe_count} barrages en BDD")

    # 2. Charger les coordonnées ROE
    roe_rows = conn.execute("SELECT lon, lat FROM roe_obstacles").fetchall()
    roe_lons = np.radians(np.array([r[0] for r in roe_rows]))
    roe_lats = np.radians(np.array([r[1] for r in roe_rows]))

    # 3. Stations sans distance
    stations = conn.execute("""
        SELECT station_code, reference_longitude, reference_latitude
        FROM stations
        WHERE reference_longitude IS NOT NULL
          AND dist_barrage_m IS NULL
    """).fetchall()

    log.info(f"{len(stations)} stations sans distance barrage")

    for station_code, lon, lat in stations:
        dist = _haversine_min_distance(lat, lon, roe_lats, roe_lons)
        update_dist_barrage(conn, station_code, dist)

    conn.commit()

    log.info(f"Étape 2e terminée : {len(stations)} distances calculées")
    return {"roe_count": roe_count, "updated": len(stations)}


if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    parser = argparse.ArgumentParser(description="Étape 2e — Distance barrages ROE")
    parser.add_argument("--db", type=str, default="./data/test.db")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    run_step2e(conn)
    conn.close()