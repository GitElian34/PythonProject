#!/usr/bin/env python3
"""
Calcule l'ordre de Strahler pour toutes les stations insitu
depuis RiverATLAS et l'insère dans la BDD.

Usage : python add_strahler.py
"""

import sqlite3
import geopandas as gpd
import pandas as pd

# ─── Chemins ───────────────────────────────────────────────────────────────
DB_PATH      = "./data/insitu_data.db"
RIVER_ATLAS  = "./data/HydroSHED/RiverATLAS_v10_eu.shp"

# Bounding box France métropolitaine avec marge
FRANCE_BBOX  = (-5.5, 41.0, 10.0, 51.5)

# Seuil d'alerte : si une station est à plus de 5km d'un tronçon,
# le matching est probablement faux
DIST_SEUIL_M = 5000


def main():

    # ── 1. Charger RiverATLAS uniquement sur la France ──────────────────────
    # Le paramètre bbox évite de charger tout le fichier Europe en mémoire,
    # ce qui est important car RiverATLAS EU peut peser plusieurs Go
    print("📂 Chargement RiverATLAS (France uniquement)...")
    rivers = gpd.read_file(RIVER_ATLAS, bbox=FRANCE_BBOX)
    print(f"   {len(rivers)} tronçons chargés")

    # Vérification que la colonne Strahler existe bien dans ce fichier
    if "ORD_STRA" not in rivers.columns:
        raise ValueError("❌ Colonne ORD_STRA introuvable dans RiverATLAS — vérifie le fichier")

    # ── 2. Charger les stations depuis la BDD ───────────────────────────────
    print("\n📂 Chargement des stations depuis la BDD...")
    conn = sqlite3.connect(DB_PATH)
    stations = pd.read_sql("""
        SELECT code_sta, lon, lat
        FROM stations_insitu
        WHERE lon IS NOT NULL AND lat IS NOT NULL
    """, conn)
    print(f"   {len(stations)} stations avec coordonnées")

    if stations.empty:
        print("❌ Aucune station avec coordonnées — lance d'abord add_coordinates_from_gpkg()")
        conn.close()
        return

    # ── 3. Convertir en GeoDataFrame et reprojeter en mètres ───────────────
    # On reprojette en EPSG:3857 (mètres) pour que sjoin_nearest calcule
    # des distances métriques plutôt que des degrés décimaux
    stations_gdf = gpd.GeoDataFrame(
        stations,
        geometry=gpd.points_from_xy(stations.lon, stations.lat),
        crs="EPSG:4326"
    ).to_crs("EPSG:3857")

    rivers = rivers.to_crs("EPSG:3857")

    # ── 4. Trouver le tronçon le plus proche pour chaque station ────────────
    print("\n🔍 Matching stations → tronçons RiverATLAS...")
    result = gpd.sjoin_nearest(
        stations_gdf,
        rivers[["geometry", "ORD_STRA"]],
        how="left",
        distance_col="dist_m"   # distance au tronçon en mètres pour QC
    )

    # ── 5. Contrôle qualité : stations trop loin d'un tronçon ───────────────
    # Si une station est à >5km d'un tronçon RiverATLAS, c'est suspect —
    # probablement un petit cours d'eau non référencé ou une erreur de coords
    suspects = result[result["dist_m"] > DIST_SEUIL_M]
    if not suspects.empty:
        print(f"\n⚠️  {len(suspects)} stations à plus de {DIST_SEUIL_M/1000:.0f}km d'un tronçon :")
        print(suspects[["code_sta", "dist_m", "ORD_STRA"]].to_string(index=False))

    # ── 6. Préparer les updates pour la BDD ─────────────────────────────────
    # On ne met à jour que les stations avec un Strahler valide
    valid = result.dropna(subset=["ORD_STRA"])
    updates = [
        (int(row["ORD_STRA"]), row["code_sta"])
        for _, row in valid.iterrows()
    ]

    # ── 7. Insérer dans la BDD ──────────────────────────────────────────────
    print("\n💾 Insertion dans la BDD...")

    # Ajouter la colonne si elle n'existe pas encore
    try:
        conn.execute("ALTER TABLE stations_insitu ADD COLUMN strahler INTEGER")
        conn.commit()
        print("   Colonne strahler créée")
    except Exception:
        print("   Colonne strahler déjà existante")

    # Mise à jour en batch (beaucoup plus rapide qu'une boucle UPDATE)
    conn.executemany(
        "UPDATE stations_insitu SET strahler = ? WHERE code_sta = ?",
        updates
    )
    conn.commit()
    print(f"✅ {len(updates)} stations mises à jour")

    # ── 8. Vérification finale ──────────────────────────────────────────────
    print("\n📊 Distribution des ordres de Strahler dans la BDD :")
    df_stats = pd.read_sql("""
        SELECT strahler, COUNT(*) as nb_stations
        FROM stations_insitu
        WHERE strahler IS NOT NULL
        GROUP BY strahler
        ORDER BY strahler
    """, conn)
    print(df_stats.to_string(index=False))

    # Afficher quelques exemples pour vérification visuelle
    print("\n🔎 Exemples (10 premières stations) :")
    df_check = pd.read_sql("""
        SELECT code_sta, river_name, strahler
        FROM stations_insitu
        WHERE strahler IS NOT NULL
        ORDER BY strahler DESC
        LIMIT 10
    """, conn)
    print(df_check.to_string(index=False))

    conn.close()


if __name__ == "__main__":
    main()