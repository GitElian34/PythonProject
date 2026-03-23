import geopandas as gpd
from shapely.geometry import Point
import sys

from Geopackage.Sword_request import point_dans_riviere

# Charger les stations (une seule fois)
print("Chargement des stations...")
gdf = gpd.read_file("./data/insitu/shp/station_schapi_alti_ref_2025_river.gpkg")
print(gdf.columns.tolist())
print(gdf.head(2))

def station_la_plus_proche(lon, lat, riviere_origine=None):
    point = Point(lon, lat)
    gdf_proj = gdf.to_crs("EPSG:2154")
    point_proj = gpd.GeoSeries([point], crs="EPSG:4326").to_crs("EPSG:2154")[0]
    gdf_proj['distance_m'] = gdf_proj.geometry.distance(point_proj)
    gdf_sorted = gdf_proj.nsmallest(len(gdf_proj), 'distance_m')

    premier_valide = None  # fallback : première station dans une rivière
    tentatives_riviere = 0

    for _, closest in gdf_sorted.iterrows():
        station_point = closest.geometry
        station_geom = gpd.GeoSeries([station_point], crs="EPSG:2154").to_crs("EPSG:4326")[0]
        station_lon, station_lat = station_geom.x, station_geom.y

        if point_dans_riviere(station_lon, station_lat):
            # Sauvegarder le premier valide comme fallback
            if premier_valide is None:
                premier_valide = (closest['code_sta'], closest['distance_m'], lon, lat)

            print(f"  La station hydro : {riviere_origine}")
            print(f"  La station insitu : {closest['river_name']}")

            if riviere_origine is None or riviere_origine.lower() in closest['river_name'].lower():
                return closest['code_sta'], closest['distance_m'], lon, lat

            tentatives_riviere += 1
            if tentatives_riviere >= 4:
                print(f"⚠️  Aucune correspondance rivière après 4 tentatives, fallback sur la plus proche")
                return premier_valide

    if premier_valide:
        print(f"⚠️  Fin de liste, fallback sur la plus proche")
        return premier_valide

    print("❌ Aucune station trouvée dans une rivière")
    return None, None, lon, lat
#REFERENCE LONGITUDE:: 1.7671
#REFERENCE LATITUDE:: 43.7652
lon, lat = 1.7671, 43.7652

print(f"\n📍 Recherche de la station la plus proche de ({lon}, {lat})...")
