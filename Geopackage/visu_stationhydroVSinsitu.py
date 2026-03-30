import geopandas as gpd
import pandas as pd
import sqlite3
import folium
from shapely.geometry import Point

DB_PATH = './data/hydro_data.db'
GPKG_PATH = './data/insitu/shp/station_schapi_alti_ref_2025.gpkg'

# Paires trouvées
paires = [
    ('0000000010852', 1.8557, 47.3753, 'H300000101',  'SEINE'),
    ('0000000010843', 1.8557, 47.3753, 'H501012001',  'SEINE'),
    ('0000000008762', 5.5664, 47.4476, 'U331001001',  'SAONE'),
    ('0000000010838', 1.8557, 47.3753, 'H509101002',  'MARNE'),
    ('0000000005744', 1.1044, 44.5053, 'O795151001',  'LOT'),
    ('0000000006358', 0.0934, 48.2514, 'K338201001',  'SIOULE'),
    ('112558',        1.8557, 47.3753, 'M323091020',  'MAYENNE'),
    ('0000000006361', 0.5507, 46.7151, 'L320061001',  'VIENNE'),
    ('0000000008748', 5.5664, 47.4476, 'V720001002',  'RHONE'),
    ('0000000202497', 3.0217, 47.0733, 'M530001010',  'LOIRE'),
]

# ─── Charger les coordonnées des stations insitu depuis le GeoPackage ───
print("📂 Chargement des stations insitu...")
gdf_insitu = gpd.read_file(GPKG_PATH).to_crs('EPSG:4326')
gdf_insitu = gdf_insitu.cx[-5.5:10.0, 41.0:51.5]

# ─── Charger les coordonnées des stations hydro depuis la BDD ───
print("📂 Chargement des stations hydro...")
conn = sqlite3.connect(DB_PATH)
df_hydro = pd.read_sql_query(
    "SELECT station_code, reference_longitude, reference_latitude FROM stations",
    conn
)
conn.close()
df_hydro = df_hydro.set_index('station_code')

# ─── Carte Folium ───
m = folium.Map(location=[46.5, 2.5], zoom_start=6, tiles="OpenStreetMap")

for station_hydro, _, _, station_insitu, riviere in paires:

    # Coordonnées station hydro
    if station_hydro in df_hydro.index:
        lon_h = df_hydro.loc[station_hydro, 'reference_longitude']
        lat_h = df_hydro.loc[station_hydro, 'reference_latitude']
    else:
        continue

    # Coordonnées station insitu
    row_insitu = gdf_insitu[gdf_insitu['code_sta'] == station_insitu]
    if row_insitu.empty:
        continue
    lon_i = row_insitu.iloc[0].geometry.x
    lat_i = row_insitu.iloc[0].geometry.y
    river_name = row_insitu.iloc[0].get('river_name', riviere)

    # Station hydro — marqueur rouge
    folium.Marker(
        location=[lat_h, lon_h],
        tooltip=f"HYDRO : {station_hydro} | {riviere}",
        popup=folium.Popup(
            f"<b>Station hydro</b><br>{station_hydro}<br>Rivière : {riviere}",
            max_width=200
        ),
        icon=folium.Icon(color='red', icon='satellite', prefix='fa')
    ).add_to(m)

    # Station insitu — marqueur bleu
    folium.Marker(
        location=[lat_i, lon_i],
        tooltip=f"INSITU : {station_insitu} | {river_name}",
        popup=folium.Popup(
            f"<b>Station insitu</b><br>{station_insitu}<br>Rivière : {river_name}",
            max_width=200
        ),
        icon=folium.Icon(color='blue', icon='tint', prefix='fa')
    ).add_to(m)

    # Ligne reliant les deux stations
    folium.PolyLine(
        locations=[[lat_h, lon_h], [lat_i, lon_i]],
        color='gray',
        weight=2,
        opacity=0.6,
        tooltip=f"{station_hydro} ↔ {station_insitu}"
    ).add_to(m)

# Légende
legend_html = """
<div style="position: fixed; bottom: 30px; left: 30px; z-index: 1000;
     background-color: white; padding: 12px; border-radius: 8px;
     border: 1px solid grey; font-size: 13px;">
  <b>Légende</b><br>
  <span style="color:red;">📍</span> Station hydro (satellite)<br>
  <span style="color:blue;">📍</span> Station insitu<br>
  <span style="color:gray;">—</span> Paire associée
</div>
"""
m.get_root().html.add_child(folium.Element(legend_html))

output = './data/insitu/visualisation/hydro_insitu/carte_paires_hydro_insitu.html'
m.save(output)
print(f"📊 Carte sauvegardée : {output}")