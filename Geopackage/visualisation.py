import geopandas as gpd
from shapely.geometry import Point
import sys

# Charger les stations (une seule fois)
print("Chargement des stations...")
gdf = gpd.read_file("./data/insitu/shp/station_schapi_alti_ref_2025_river.gpkg")


def station_la_plus_proche(lon, lat):
    """Retourne le code de la station la plus proche des coordonnées données"""
    point = Point(lon, lat)
    gdf_proj = gdf.to_crs("EPSG:2154")
    point_proj = gpd.GeoSeries([point], crs="EPSG:4326").to_crs("EPSG:2154")[0]
    gdf_proj['distance_m'] = gdf_proj.geometry.distance(point_proj)
    closest = gdf_proj.nsmallest(1, 'distance_m').iloc[0]
    return closest['code_sta'], closest['distance_m']
#REFERENCE LONGITUDE:: 1.7671
#REFERENCE LATITUDE:: 43.7652
lon, lat = 1.7671, 43.7652

print(f"\n📍 Recherche de la station la plus proche de ({lon}, {lat})...")
