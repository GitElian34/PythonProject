import geopandas as gpd
from shapely.geometry import Point

fichier = "./data/insitu/sword/global_reaches_SWORD_v16_poly.gpkg"
fichier2 = "./data/insitu/shp/sword_France_all_rivers.gpkg"

def point_dans_riviere(lon, lat, fichier=fichier):
    """
    Vérifie si un point est dans un polygone SWORD ou proche (tolérance ~10m)
    """
    point = Point(lon, lat)

    # Charger les rivières autour du point
    bbox = (lon - 0.01, lat - 0.01, lon + 0.01, lat + 0.01)
    gdf_local = gpd.read_file(fichier, bbox=bbox)

    if gdf_local.empty:
        #print(f"❌ Aucune rivière trouvée autour de ({lon}, {lat})")
        return False

    # Vérifier si le point est DANS un polygone
    gdf_point = gpd.GeoDataFrame(geometry=[point], crs="EPSG:4326")
    result = gpd.sjoin(gdf_point, gdf_local, how="left", predicate="within")

    if not result['reach_id'].isna().all():
        row = result.iloc[0]
        print(f"✅ Point DANS la rivière : {row['river_name']}")
        print(f"   reach_id : {row['reach_id']}")
        return True

    # Sinon, vérifier si c'est juste à côté (intersects avec un petit buffer)
    point_buffer = point.buffer(0.001)  # ~10m en WGS84 (approximatif)
    gdf_buffer = gpd.GeoDataFrame(geometry=[point_buffer], crs="EPSG:4326")
    result_proche = gpd.sjoin(gdf_buffer, gdf_local, how="inner", predicate="intersects")

    if not result_proche.empty:
        row = result_proche.iloc[0]
        print(f"⚠️  Point PROCHE de la rivière : {row['river_name']}")
        print(f"   reach_id : {row['reach_id']}")
        return True

    print(f"❌ Point loin de toute rivière")
    return False


if __name__ == "__main__":
    # Test
    lon, lat = 1.439, 43.6009
    point_dans_riviere(lon, lat)