#!/usr/bin/env python3
"""
extract_corine_satellite.py
Extrait les fractions CORINE + texture des sols (SoilGrids)
pour les 222 stations satellite de hydro_data.db.
Utilise les fonctions de db_hydro.py.
"""

import sqlite3
import numpy as np
import rasterio
from rasterio.mask import mask
from shapely.wkt import loads
from shapely.ops import transform
import pyproj
import os

from data_processing.db_manager import creer_table_corine, inserer_corine

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
DB_PATH       = "./data/hydro_data.db"
CORINE_PATH   = "./data/Bassin_Versants/Corine/u2018_clc2018_v2020_20u1_raster100m/DATA/U2018_CLC2018_V2020_20u1.tif"
SOILGRIDS_DIR = "./data/Bassin_Versants/SoilGrids/"

VARIABLES = ["clay", "sand", "silt"]
DEPTHS    = {"0-5cm_mean": 5, "5-15cm_mean": 10, "15-30cm_mean": 15}
CONV      = {"clay": 0.1, "sand": 0.1, "silt": 0.1}

CODE_TO_CAT = {
    1:  "urban", 2:  "urban", 3:  "urban", 4:  "urban",
    5:  "urban", 6:  "urban", 7:  "urban", 8:  "urban",
    9:  "urban", 10: "urban", 11: "urban",
    12: "agriculture", 13: "agriculture", 14: "agriculture",
    15: "agriculture", 16: "agriculture", 17: "agriculture",
    18: "agriculture", 19: "agriculture", 20: "agriculture",
    21: "agriculture", 22: "agriculture",
    23: "forest", 24: "forest", 25: "forest",
    26: "semi_natural", 27: "semi_natural", 28: "semi_natural",
    29: "semi_natural", 30: "semi_natural", 31: "semi_natural",
    32: "semi_natural", 33: "semi_natural", 34: "semi_natural",
    35: "wetland", 36: "wetland", 37: "wetland",
    38: "wetland", 39: "wetland",
    40: "water", 41: "water", 42: "water",
    43: "water", 44: "water",
}

# ═══════════════════════════════════════════════════════════════
# FONCTIONS CORINE
# ═══════════════════════════════════════════════════════════════
def reprojeter_3035(polygone_wgs84):
    projet = pyproj.Transformer.from_crs(
        "EPSG:4326", "EPSG:3035", always_xy=True
    ).transform
    return transform(projet, polygone_wgs84)


def extraire_corine(polygone_wgs84, src):
    polygone_3035 = reprojeter_3035(polygone_wgs84)
    out_image, _  = mask(src, [polygone_3035.__geo_interface__],
                         crop=True, nodata=-128)
    pixels = out_image[0].flatten()
    valid  = pixels[(pixels != -128) & (pixels > 0)]

    if len(valid) == 0:
        return None, 0

    fractions = {cat: 0.0 for cat in
                 ["urban", "agriculture", "forest", "semi_natural", "wetland", "water"]}

    for code, count in zip(*np.unique(valid, return_counts=True)):
        cat = CODE_TO_CAT.get(int(code))
        if cat:
            fractions[cat] += count / len(valid)

    return fractions, len(valid)


# ═══════════════════════════════════════════════════════════════
# FONCTIONS SOILGRIDS
# ═══════════════════════════════════════════════════════════════
def charger_soilgrids(soilgrids_dir):
    rasters = {}
    print("Chargement SoilGrids...")
    for var in VARIABLES:
        rasters[var] = {}
        for depth_id in DEPTHS:
            path = os.path.join(soilgrids_dir, f"{var}_{depth_id}.tif")
            if not os.path.exists(path):
                print(f"  ⚠️  Manquant : {path}")
                continue
            rasters[var][depth_id] = rasterio.open(path)
            print(f"  ✅ {var}_{depth_id}")
    return rasters


def fermer_soilgrids(rasters):
    for var in rasters:
        for depth_id in rasters[var]:
            rasters[var][depth_id].close()


def extraire_soilgrids(polygone_wgs84, rasters):
    results = {}
    for var in VARIABLES:
        if var not in rasters or not rasters[var]:
            results[var] = None
            continue

        weighted_sum = 0.0
        total_weight = 0

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
                    mean_val      = np.mean(valid) * CONV[var]
                    weighted_sum += mean_val * weight
                    total_weight += weight
            except Exception as e:
                print(f"    ⚠️  {var} {depth_id} : {e}")

        results[var] = weighted_sum / total_weight if total_weight > 0 else None
    return results


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════
def main():
    conn = sqlite3.connect(DB_PATH)
    creer_table_corine(conn)

    # Stations avec polygone BV pas encore traitées
    stations = conn.execute('''
        SELECT b.station_code, b.polygone_wkt, b.aire_km2, s.river_name
        FROM bv_data b
        JOIN stations s ON b.station_code = s.station_code
        WHERE b.polygone_wkt IS NOT NULL
          AND b.station_code NOT IN (SELECT station_code FROM bv_corine)
        ORDER BY b.aire_km2 DESC
    ''').fetchall()

    print(f"{len(stations)} stations à traiter\n")

    rasters_soil = charger_soilgrids(SOILGRIDS_DIR)

    with rasterio.open(CORINE_PATH) as src_corine:
        for i, (station_code, wkt, aire_km2, river_name) in enumerate(stations):
            print(f"\n[{i+1}/{len(stations)}] {station_code} — {river_name} ({aire_km2:.0f} km²)")

            try:
                polygone = loads(wkt)

                # CORINE
                fractions, nb_pixels = extraire_corine(polygone, src_corine)
                if fractions is None:
                    print(f"  ⚠️  Aucun pixel CORINE, station ignorée")
                    continue
                print(f"  ✅ CORINE : urban={fractions['urban']:.1%} "
                      f"agri={fractions['agriculture']:.1%} "
                      f"forest={fractions['forest']:.1%}")

                # SOILGRIDS
                soil = extraire_soilgrids(polygone, rasters_soil)
                if soil.get('clay'):
                    print(f"  ✅ SoilGrids : clay={soil['clay']:.1f}% "
                          f"sand={soil['sand']:.1f}% silt={soil['silt']:.1f}%")
                else:
                    print(f"  ⚠️  SoilGrids partiel")

                # INSERTION via db_hydro
                inserer_corine(conn, station_code, fractions, nb_pixels, soil)

            except Exception as e:
                print(f"  ❌ ERREUR : {e}")

    fermer_soilgrids(rasters_soil)

    nb = conn.execute("SELECT COUNT(*) FROM bv_corine").fetchone()[0]
    print(f"\n✅ Terminé — {nb} stations dans bv_corine")
    conn.close()


if __name__ == '__main__':
    main()