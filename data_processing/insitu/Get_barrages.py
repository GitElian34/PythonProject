"""
Télécharge le ROE et remplit la table roe_obstacles dans la BDD.
Filtré sur les barrages uniquement (CdTypeOuvr commençant par '1.1')
"""

import sqlite3
import requests
import zipfile
import io
import geopandas as gpd
from pathlib import Path

from db_insitu import creer_table_roe, inserer_roe, get_roe_count

DB_PATH = "./data/insitu_data.db"
ROE_DIR = Path("./data/insitu/Barrages/dataset")

ROE_URLS = [
    "https://www.data.gouv.fr/api/1/datasets/r/b7f5faef-6f41-4e78-9c41-826c09c72d52",
    "https://www.data.gouv.fr/api/1/datasets/r/2fe5ad95-480b-4d65-884b-f08a272d73bc",
]

# ═══════════════════════════════════════════════════════════════
# ÉTAPE 1 — Téléchargement
# ═══════════════════════════════════════════════════════════════
ROE_DIR.mkdir(parents=True, exist_ok=True)
shp_files = list(ROE_DIR.glob("*.shp"))

if shp_files:
    roe_shp = shp_files[0]
    print(f"ROE déjà présent : {roe_shp}")
else:
    print("Téléchargement du ROE France Métropole...")
    roe_shp = None
    for url in ROE_URLS:
        try:
            print(f"  → {url}")
            resp = requests.get(url, timeout=120)
            resp.raise_for_status()
            if resp.content[:4] == b'PK\x03\x04':
                z = zipfile.ZipFile(io.BytesIO(resp.content))
                z.extractall(ROE_DIR)
                shp_files = list(ROE_DIR.glob("*.shp"))
                if shp_files:
                    roe_shp = shp_files[0]
                    print(f"  ✅ Extrait → {roe_shp}")
                    break
            else:
                roe_shp = ROE_DIR / "roe_france.shp"
                with open(roe_shp, 'wb') as f:
                    f.write(resp.content)
                print(f"  ✅ Téléchargé → {roe_shp}")
                break
        except Exception as e:
            print(f"  ⚠️  Erreur : {e}")

    if roe_shp is None:
        print("Téléchargez manuellement depuis :")
        print("  https://www.data.gouv.fr/fr/datasets/obstacles-a-lecoulement-issus-du-roe-en-france-metropole/")
        exit(1)

# ═══════════════════════════════════════════════════════════════
# ÉTAPE 2 — Lecture + FILTRE BARRAGES
# ═══════════════════════════════════════════════════════════════
print("\nLecture du shapefile...")
gdf = gpd.read_file(roe_shp).to_crs(epsg=4326)
print(f"  {len(gdf)} obstacles au total")

# Filtre sur les barrages uniquement (codes 1.1.x)
gdf = gdf[gdf['CdTypeOuvr'].str.startswith('1.1', na=False)].copy()
print(f"  {len(gdf)} barrages retenus (CdTypeOuvr 1.1.x)")

gdf['lon'] = gdf.geometry.x
gdf['lat'] = gdf.geometry.y
gdf = gdf.dropna(subset=['lon', 'lat'])

# ═══════════════════════════════════════════════════════════════
# ÉTAPE 3 — Insertion en BDD
# ═══════════════════════════════════════════════════════════════
print("\nInsertion dans la BDD...")
conn = sqlite3.connect(DB_PATH)

conn.execute("DROP TABLE IF EXISTS roe_obstacles")
creer_table_roe(conn)

# Colonnes correctes identifiées depuis le shapefile réel
batch = [
    (
        str(row['CdObstEcou']),
        str(row['NomPrincip']) if row['NomPrincip'] else None,
        str(row['LbTypeOuvr']),
        float(row['lon']),
        float(row['lat']),
    )
    for _, row in gdf.iterrows()
]

conn.executemany('''
    INSERT INTO roe_obstacles (roe_id, nom, type, lon, lat)
    VALUES (?, ?, ?, ?, ?)
''', batch)
conn.commit()

count = get_roe_count(conn)
conn.close()

print(f"\n✅ {count} barrages insérés dans roe_obstacles")