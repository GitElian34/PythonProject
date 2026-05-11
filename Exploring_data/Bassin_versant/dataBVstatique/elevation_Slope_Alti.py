#!/usr/bin/env python3
"""
add_elevation_slope_satellite.py
═══════════════════════════════════════════════════════════════════════════
Calcule elevation_mean et slope_mean pour les BV des 222 stations satellite
à partir d'un MNT (SRTM) et insère dans hydro_data.db.

Fonctions BDD utilisées depuis db_hydro.py :
  - ajouter_colonnes_elevation_slope_satellite(conn)
  - get_bv_satellite_a_traiter_elevation(conn)
  - mettre_a_jour_elevation_slope_satellite(conn, ...)
  - get_elevation_slope_stats_satellite(conn)
═══════════════════════════════════════════════════════════════════════════
"""

import sqlite3
import subprocess
import os
from rasterstats import zonal_stats
from shapely import wkt
from shapely.geometry import mapping
import warnings
warnings.filterwarnings("ignore")

from data_processing.db_manager import (
    ajouter_colonnes_elevation_slope_satellite,
    get_bv_satellite_a_traiter_elevation,
    mettre_a_jour_elevation_slope_satellite,
    get_elevation_slope_stats_satellite,
)

# ─── Paramètres ─────────────────────────────────────────────────────────────
DB_PATH    = './data/hydro_data.db'
DEM_PATH   = './data/Elevation/srtm_france.tif'
SLOPE_PATH = './data/Elevation/slope_france.tif'

# ═══════════════════════════════════════════════════════════════
# 1. Calculer le raster slope si besoin
# ═══════════════════════════════════════════════════════════════
print("=" * 55)
print("ELEVATION / SLOPE — STATIONS SATELLITE")
print("=" * 55)

if not os.path.exists(DEM_PATH):
    print(f"❌ MNT introuvable : {DEM_PATH}")
    exit(1)

if not os.path.exists(SLOPE_PATH):
    print(f"\nCalcul du raster slope depuis {DEM_PATH}...")
    cmd = ['gdaldem', 'slope', DEM_PATH, SLOPE_PATH,
           '-compute_edges', '-p', '-s', '111120']
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"❌ ERREUR gdaldem : {result.stderr}")
        exit(1)
    print(f"✅ Slope raster créé : {SLOPE_PATH}")
else:
    print(f"\n✅ Slope raster déjà existant : {SLOPE_PATH}")

# ═══════════════════════════════════════════════════════════════
# 2. Préparer la BDD
# ═══════════════════════════════════════════════════════════════
conn = sqlite3.connect(DB_PATH)
print()
ajouter_colonnes_elevation_slope_satellite(conn)

# ═══════════════════════════════════════════════════════════════
# 3. Récupérer les stations à traiter
# ═══════════════════════════════════════════════════════════════
rows = get_bv_satellite_a_traiter_elevation(conn)
print(f"\n📂 {len(rows)} stations satellite à traiter\n")

# ═══════════════════════════════════════════════════════════════
# 4. Calculer zonal stats et insérer
# ═══════════════════════════════════════════════════════════════
errors = []
ok = 0

for i, (station_code, polygon_wkt) in enumerate(rows):
    try:
        geom = mapping(wkt.loads(polygon_wkt))

        stats_elev  = zonal_stats(geom, DEM_PATH,   stats=['mean', 'std'], nodata=-32768)
        stats_slope = zonal_stats(geom, SLOPE_PATH, stats=['mean', 'std'], nodata=-9999)

        elev_mean  = stats_elev[0]['mean']
        elev_std   = stats_elev[0]['std']
        slope_mean = stats_slope[0]['mean']
        slope_std  = stats_slope[0]['std']

        if elev_mean is None or slope_mean is None:
            errors.append((station_code, "stats None"))
            continue

        mettre_a_jour_elevation_slope_satellite(
            conn, station_code,
            elev_mean, elev_std, slope_mean, slope_std
        )
        ok += 1

        if (i + 1) % 50 == 0:
            conn.commit()
            print(f"  {i+1}/{len(rows)} stations traitées ({ok} OK, {len(errors)} erreurs)")

    except Exception as e:
        errors.append((station_code, str(e)[:100]))

conn.commit()

# ═══════════════════════════════════════════════════════════════
# 5. Stats finales
# ═══════════════════════════════════════════════════════════════
print(f"\n" + "=" * 55)
print(f"RÉSUMÉ")
print(f"=" * 55)
print(f"   Stations traitées avec succès : {ok}")
print(f"   Erreurs                       : {len(errors)}")

if errors:
    print(f"\n   Premières erreurs :")
    for code, msg in errors[:10]:
        print(f"     {code} : {msg}")

get_elevation_slope_stats_satellite(conn)
conn.close()
print("\n✅ Terminé !")