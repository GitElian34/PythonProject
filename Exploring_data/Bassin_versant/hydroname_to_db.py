import sqlite3
import geopandas as gpd
import pandas as pd

from data_processing.db_manager import add_hydroweb_names

# ── 1. Charger le shapefile complet ──
stations_shp = gpd.read_file('./data/hydroweb/shp/hydroweb.shp')
stations_shp = stations_shp.cx[-5.2:9.6, 41.3:51.1].reset_index(drop=True)

# ── 2. Connexion à la BDD et appel de la fonction ──


conn = sqlite3.connect('./data/hydro_data.db')
add_hydroweb_names(conn, './data/hydroweb/shp/hydroweb.shp')

# ── 3. Vérification ──
result = pd.read_sql(
    "SELECT station_code, hydroweb_name, basin_name, river_name FROM stations WHERE hydroweb_name IS NOT NULL LIMIT 10",
    conn
)
print(f"\nAperçu des stations avec hydroweb_name :")
print(result.to_string())

total = pd.read_sql("SELECT COUNT(*) as total FROM stations WHERE hydroweb_name IS NOT NULL", conn)
print(f"\nTotal stations avec hydroweb_name : {total['total'].iloc[0]}")

conn.close()