import sqlite3
import numpy as np
from shapely.wkt import loads
from shapely.ops import transform
import pyproj

DB_PATH   = "./data/insitu_data.db"
TOLERANCE = 10000  # 2km

def gravelius(polygone_wgs84, aire_km2, tolerance=TOLERANCE):
    projet = pyproj.Transformer.from_crs(
        "EPSG:4326", "EPSG:3857", always_xy=True
    ).transform
    polygone_m = transform(projet, polygone_wgs84)
    polygone_s = polygone_m.simplify(tolerance, preserve_topology=True)

    perimetre_km = polygone_s.length / 1000
    k            = 0.28 * perimetre_km / np.sqrt(aire_km2)

    largeur  = (polygone_m.bounds[2] - polygone_m.bounds[0]) / 1000
    hauteur  = (polygone_m.bounds[3] - polygone_m.bounds[1]) / 1000
    perim_rect = 2 * (largeur + hauteur)
    k_rect   = 0.28 * perim_rect / np.sqrt(aire_km2)

    return round(k, 3), round(perimetre_km, 2), round(k_rect, 3)


conn = sqlite3.connect(DB_PATH)
stations = conn.execute('''
    SELECT b.code_sta, b.aire_km2, b.polygone_wkt, s.river_name
    FROM bv_data b
    JOIN stations_insitu s ON b.code_sta = s.code_sta
    WHERE b.polygone_wkt IS NOT NULL
    ORDER BY RANDOM()
    LIMIT 50
''').fetchall()
conn.close()

print(f"{'code_sta':15s} {'river_name':35s} {'aire_km2':>10s} {'perim_km':>10s} {'K':>7s} {'K_rect':>7s} {'forme'}")
print("─" * 105)

stats = {'quasi-circulaire': 0, 'ovale': 0, 'allongé': 0, 'très allongé': 0}

for code_sta, aire_km2, wkt, river_name in stations:
    try:
        polygone = loads(wkt)
        k, perim, k_rect = gravelius(polygone, aire_km2)

        if k < 1.25:
            forme = "quasi-circulaire"
        elif k < 1.50:
            forme = "ovale"
        elif k < 1.75:
            forme = "allongé"
        else:
            forme = "très allongé"

        stats[forme] += 1
        print(f"{code_sta:15s} {river_name[:35]:35s} {aire_km2:>10.1f} {perim:>10.1f} "
              f"{k:>7.3f} {k_rect:>7.3f} {forme}")

    except Exception as e:
        print(f"{code_sta:15s} ❌ {e}")

print("─" * 105)
print(f"\n── Répartition des formes ──")
for forme, n in stats.items():
    print(f"  {forme:20s} : {n:3d} ({n/50*100:.0f}%)")