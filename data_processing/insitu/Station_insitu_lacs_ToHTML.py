import geopandas as gpd
import sqlite3
import pandas as pd
from shapely.geometry import Point
import folium
from data_processing import get_station_coords

# Charger stations depuis la BDD
conn = sqlite3.connect("./data/insitu_data.db")
df = pd.read_sql_query("SELECT code_sta, dans_lac FROM stations_insitu", conn)
conn.close()

# Ajouter les coordonnées
coords = []
flags = []
codes = []
for code_sta, flag in zip(df['code_sta'], df['dans_lac']):
    lon, lat = get_station_coords(code_sta)
    if lon is not None:
        coords.append(Point(lon, lat))
        flags.append(flag)
        codes.append(code_sta)

gdf_points = gpd.GeoDataFrame(
    {'code_sta': codes, 'flag': flags},
    geometry=coords,
    crs="EPSG:4326"
)

# Filtrer
gdf_plot = gdf_points[gdf_points['flag'].isin(['dans_lac', 'proche_lac'])]

# Couleur selon le flag
color_map = {
    'dans_lac': 'red',
    'proche_lac': 'orange'
}

# Créer la carte centrée sur les stations
center_lat = gdf_plot.geometry.y.mean()
center_lon = gdf_plot.geometry.x.mean()

m = folium.Map(location=[center_lat, center_lon], zoom_start=6,
               tiles="OpenStreetMap")

# Ajouter les stations
for _, row in gdf_plot.iterrows():
    folium.CircleMarker(
        location=[row.geometry.y, row.geometry.x],
        radius=7,
        color=color_map.get(row['flag'], 'gray'),
        fill=True,
        fill_color=color_map.get(row['flag'], 'gray'),
        fill_opacity=0.8,
        popup=folium.Popup(f"<b>{row['code_sta']}</b><br>Flag : {row['flag']}",
                           max_width=200),
        tooltip=row['code_sta']
    ).add_to(m)

# Légende simple
legend_html = """
<div style="position: fixed; bottom: 30px; left: 30px; z-index: 1000;
     background-color: white; padding: 10px; border-radius: 5px;
     border: 1px solid grey; font-size: 13px;">
  <b>Légende</b><br>
  <span style="color:red;">●</span> Dans un lac<br>
  <span style="color:orange;">●</span> Proche d'un lac
</div>
"""
m.get_root().html.add_child(folium.Element(legend_html))

# Sauvegarder
m.save("stations_altimetriques_lac.html")
print("Carte sauvegardée : stations_altimetriques_lac.html")