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

⚠️ CHANGEMENT vs version précédente :
    Le bbox n'est plus une constante figée par pays (BBOX_FRANCE /
    BBOX_GERMANY). Il est maintenant soit fourni explicitement,
    soit déduit automatiquement des coordonnées des stations sans BV
    en base (avec une marge de sécurité) via get_bbox_from_stations().
    Ça évite de casser un pays en changeant le défaut pour un autre.
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

# Conservés pour compat / usage manuel explicite si besoin, mais ne sont
# plus utilisés comme défaut implicite de run_step2a.
BBOX_FRANCE  = {"left": -6.0, "right": 10.0, "bottom": 41.0, "top": 52.0}
BBOX_GERMANY = {"left": 5.5, "right": 15.5, "bottom": 47.0, "top": 55.5}

ACC_THRESHOLD = 500  # seuil snap_to_mask
BBOX_MARGIN_DEG = 0.5  # marge de sécurité autour des stations pour l'auto-bbox


# ═══════════════════════════════════════════════════════════════
# FONCTIONS
# ═══════════════════════════════════════════════════════════════

def get_bbox_from_stations(stations: list[dict],
                           margin_deg: float = BBOX_MARGIN_DEG) -> dict:
    """
    Calcule un bbox englobant une liste de stations (dicts avec au moins
    les clés "lon"/"lat"), avec une marge de sécurité (en degrés) pour
    ne pas couper le bassin versant d'une station proche du bord.

    Prend directement la liste déjà chargée via get_stations_without_bv()
    plutôt que de refaire une requête SQL sur la table stations, pour ne
    pas dépendre du nom exact des colonnes / de la structure des tables
    (qui peut varier selon la BDD).

    Returns:
        {"left": ..., "right": ..., "bottom": ..., "top": ...}
    """
    if not stations:
        raise ValueError("Impossible de déduire un bbox : liste de stations vide.")

    lons = [s["lon"] for s in stations if s.get("lon") is not None]
    lats = [s["lat"] for s in stations if s.get("lat") is not None]

    if not lons or not lats:
        raise ValueError(
            "Impossible de déduire un bbox : aucune coordonnée lon/lat "
            "exploitable parmi les stations fournies."
        )

    lon_min, lon_max = min(lons), max(lons)
    lat_min, lat_max = min(lats), max(lats)

    bbox = {
        "left":   lon_min - margin_deg,
        "right":  lon_max + margin_deg,
        "bottom": lat_min - margin_deg,
        "top":    lat_max + margin_deg,
    }
    log.info(f"Bbox auto-détecté depuis {lon_min:.2f},{lat_min:.2f} → "
             f"{lon_max:.2f},{lat_max:.2f} (marge {margin_deg}°) : {bbox}")
    return bbox


def clip_rasters(dir_path: str, acc_path: str, bbox: dict) -> tuple[str, str]:
    """
    Clippe les rasters DIR et ACC sur le bbox donné.
    Retourne (tmp_dir_path, tmp_acc_path).
    """
    log.info(f"Clipping des rasters sur bbox={bbox}...")
    tmp_dir = tempfile.mkdtemp()
    tmp_dir_path = os.path.join(tmp_dir, "dir_clip.tif")
    tmp_acc_path = os.path.join(tmp_dir, "acc_clip.tif")

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
    # ⚠️ Approximation : la taille réelle d'un pixel diminue avec cos(latitude).
    # À 41-52°N (France) l'erreur est faible ; à 47-55°N (Allemagne) elle
    # reste modeste mais peut légèrement sous-estimer les grandes aires.
    aire = round(float(catch.sum()) * 0.0625, 1)

    # Polygoniser
    grid.clip_to(catch)
    shapes = grid.polygonize(grid.view(catch).astype("uint8"))
    poly = gpd.GeoDataFrame(
        geometry=[shape(s) for s, v in shapes if v == 1],
        crs="EPSG:4326",
    ).dissolve().geometry.iloc[0]

    return aire, poly.wkt


def reset_bv_data(conn: sqlite3.Connection) -> dict:
    """
    Reset complet : supprime toutes les entrées bv_data et remet à NULL,
    dans `stations`, les colonnes dérivées du BV (aire, élévation/slope,
    Corine/SoilGrids), pour repartir de zéro. Ne touche jamais aux mesures
    de niveau d'eau ni aux colonnes station_code/lon/lat/strahler/etc.
    """
    keywords = ["elev", "slope", "frac_", "clc", "corine", "soil", "watershed"]
    never_touch = {"station_code", "lon", "lat", "hydroweb_name", "strahler", "dist_barrage_m", "id"}

    cols = [c[1] for c in conn.execute("PRAGMA table_info(stations)").fetchall()]
    reset_cols = [c for c in cols if c not in never_touch and any(k in c.lower() for k in keywords)]

    n_bv = conn.execute("SELECT COUNT(*) FROM bv_data").fetchone()[0]
    conn.execute("DELETE FROM bv_data")

    if reset_cols:
        set_clause = ", ".join(f"{c} = NULL" for c in reset_cols)
        conn.execute(f"UPDATE stations SET {set_clause}")

    conn.commit()
    log.info(f"Reset : {n_bv} lignes bv_data supprimées, colonnes {reset_cols} remises à NULL dans stations")
    return {"bv_deleted": n_bv, "columns_reset": reset_cols}


def run_step2a(conn: sqlite3.Connection,
               dir_path: str = DEFAULT_DIR_PATH,
               acc_path: str = DEFAULT_ACC_PATH,
               bbox: dict | None = None) -> dict:
    """
    Étape 2a : calcule les BV pour toutes les stations sans BV.

    Args:
        conn: Connexion à la BDD pipeline
        dir_path: Chemin vers le raster flow direction
        acc_path: Chemin vers le raster flow accumulation
        bbox: Zone de clipping des rasters {"left","right","bottom","top"}.
              Si None (défaut), déduit automatiquement des coordonnées des
              stations sans BV en base, avec une marge de sécurité.
              Passer BBOX_FRANCE / BBOX_GERMANY (ou un bbox custom)
              pour forcer une zone précise si besoin.

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
    log.info([dir_path, acc_path])
    for p in [dir_path, acc_path]:
        log.info(p)
        if not os.path.exists(p):
            log.error(f"Raster introuvable : {p}")
            return {"computed": 0, "errors": len(stations)}

    # Déterminer le bbox : fourni explicitement, sinon auto-détecté
    # à partir de la liste `stations` déjà chargée (pas de requête SQL
    # supplémentaire, donc pas de dépendance au nom exact des colonnes).
    if bbox is None:
        bbox = get_bbox_from_stations(stations)
    else:
        log.info(f"Bbox fourni explicitement : {bbox}")

    # Clipper une seule fois
    tmp_dir, tmp_acc = clip_rasters(dir_path, acc_path, bbox=bbox)

    computed, errors = 0, 0
    total = len(stations)

    for i, sta in enumerate(stations):
        code = sta["station_code"]
        log.info(f"[{i+1}/{total}] {code} ({sta.get('hydroweb_name', '?')})...")
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
    log.info(f"Étape 2a terminée : {computed} BV calculés, {errors} erreurs")
    return {"computed": computed, "errors": errors}


# ═══════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    parser = argparse.ArgumentParser(
        description="Étape 2a — Délinéation des BV",
        epilog="""
Exemples :
  python step2a_delineate_bv.py --db ./data/test.db
      (bbox auto-détecté depuis les stations sans BV en base)

  python step2a_delineate_bv.py --db ./data/test.db --bbox 5.5 47.0 15.5 55.5
      (bbox forcé, ici Allemagne)
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--db", type=str, default="./data/test.db")
    parser.add_argument("--dir", type=str, default=DEFAULT_DIR_PATH)
    parser.add_argument("--acc", type=str, default=DEFAULT_ACC_PATH)
    parser.add_argument("--bbox", type=float, nargs=4, default=None,
                        metavar=("LEFT", "BOTTOM", "RIGHT", "TOP"),
                        help="Bbox explicite (défaut : auto-détecté depuis les stations)")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)

    bbox = None
    if args.bbox:
        left, bottom, right, top = args.bbox
        bbox = {"left": left, "bottom": bottom, "right": right, "top": top}

    log.info([args.dir, args.acc])
    run_step2a(conn, args.dir, args.acc, bbox=bbox)
    conn.close()