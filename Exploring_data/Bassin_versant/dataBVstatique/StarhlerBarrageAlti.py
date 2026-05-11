#!/usr/bin/env python3
"""
add_strahler_roe_satellite.py
Calcule et insère dans hydro_data.db :
  - L'ordre de Strahler (depuis RiverATLAS)
  - La distance au barrage le plus proche (depuis roe_obstacles dans insitu_data.db)
pour les 222 stations satellite.

Fonctions BDD utilisées depuis db_hydro.py :
  - ajouter_colonne_strahler(conn)
  - mettre_a_jour_strahler(conn, updates)
  - ajouter_colonne_dist_barrage(conn)
  - mettre_a_jour_distances_barrages(conn, roe_conn)
"""

import sqlite3
import geopandas as gpd
import pandas as pd

from data_processing.db_manager import (
    ajouter_colonne_strahler,
    mettre_a_jour_strahler,
    ajouter_colonne_dist_barrage,
    mettre_a_jour_distances_barrages,
)

# ─── Chemins ────────────────────────────────────────────────────────────────
DB_PATH      = "./data/hydro_data.db"
DB_INSITU    = "./data/insitu_data.db"   # contient roe_obstacles
RIVER_ATLAS  = "./data/HydroSHED/RiverATLAS_v10_eu.shp"
FRANCE_BBOX  = (-5.5, 41.0, 10.0, 51.5)
DIST_SEUIL_M = 5000

conn = sqlite3.connect(DB_PATH)

# ═══════════════════════════════════════════════════════════════
# 1. STRAHLER
# ═══════════════════════════════════════════════════════════════
print("=" * 55)
print("STRAHLER")
print("=" * 55)

print("📂 Chargement RiverATLAS (France uniquement)...")
rivers = gpd.read_file(RIVER_ATLAS, bbox=FRANCE_BBOX)
print(f"   {len(rivers)} tronçons chargés")

if "ORD_STRA" not in rivers.columns:
    raise ValueError("❌ Colonne ORD_STRA introuvable dans RiverATLAS")

# Charger les stations satellite avec coordonnées
stations = pd.read_sql("""
    SELECT station_code, reference_longitude AS lon, reference_latitude AS lat
    FROM stations
    WHERE reference_longitude IS NOT NULL AND reference_latitude IS NOT NULL
""", conn)
print(f"   {len(stations)} stations satellite avec coordonnées")

# GeoDataFrame + reprojection mètres
stations_gdf = gpd.GeoDataFrame(
    stations,
    geometry=gpd.points_from_xy(stations.lon, stations.lat),
    crs="EPSG:4326"
).to_crs("EPSG:3857")

rivers = rivers.to_crs("EPSG:3857")

print("\n🔍 Matching stations → tronçons RiverATLAS...")
result = gpd.sjoin_nearest(
    stations_gdf,
    rivers[["geometry", "ORD_STRA"]],
    how="left",
    distance_col="dist_m"
)

# Contrôle qualité
suspects = result[result["dist_m"] > DIST_SEUIL_M]
if not suspects.empty:
    print(f"\n⚠️  {len(suspects)} stations à plus de {DIST_SEUIL_M/1000:.0f}km d'un tronçon :")
    print(suspects[["station_code", "dist_m", "ORD_STRA"]].to_string(index=False))

# Préparer et insérer
valid   = result.dropna(subset=["ORD_STRA"])
updates = [(int(row["ORD_STRA"]), row["station_code"]) for _, row in valid.iterrows()]

ajouter_colonne_strahler(conn)
mettre_a_jour_strahler(conn, updates)

# Vérification
print("\n📊 Distribution Strahler :")
df_stats = pd.read_sql("""
    SELECT strahler, COUNT(*) as nb_stations
    FROM stations
    WHERE strahler IS NOT NULL
    GROUP BY strahler ORDER BY strahler
""", conn)
print(df_stats.to_string(index=False))

# ═══════════════════════════════════════════════════════════════
# 2. DISTANCE BARRAGES ROE
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 55)
print("DISTANCE BARRAGES ROE")
print("=" * 55)

roe_conn = sqlite3.connect(DB_INSITU)

# Vérifier que la table ROE existe dans insitu
n_roe = roe_conn.execute("SELECT COUNT(*) FROM roe_obstacles").fetchone()[0]
print(f"   {n_roe} barrages dans roe_obstacles")

ajouter_colonne_dist_barrage(conn)
mettre_a_jour_distances_barrages(conn, roe_conn)
roe_conn.close()

# Vérification
print("\n📊 Distribution distances barrages (seuils) :")
for seuil, label in [(100, "< 100m"), (500, "< 500m"), (1000, "< 1km"), (5000, "< 5km")]:
    n = conn.execute(
        "SELECT COUNT(*) FROM stations WHERE dist_barrage_m < ?", (seuil,)
    ).fetchone()[0]
    print(f"   {label:<10} : {n} stations")

# ═══════════════════════════════════════════════════════════════
# RÉSUMÉ FINAL
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 55)
print("RÉSUMÉ")
print("=" * 55)
n_strahler = conn.execute("SELECT COUNT(*) FROM stations WHERE strahler IS NOT NULL").fetchone()[0]
n_dist     = conn.execute("SELECT COUNT(*) FROM stations WHERE dist_barrage_m IS NOT NULL").fetchone()[0]
n_total    = conn.execute("SELECT COUNT(*) FROM stations").fetchone()[0]
print(f"   Stations totales      : {n_total}")
print(f"   Avec Strahler         : {n_strahler}")
print(f"   Avec dist_barrage_m   : {n_dist}")

conn.close()
print("\n✅ Terminé !")