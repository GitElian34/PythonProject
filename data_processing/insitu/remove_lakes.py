import sqlite3

import geopandas as gpd
from shapely.geometry import Point

# Charger UNE FOIS les lacs en France
bbox_france = (-6, 40, 10, 52)
gdf_lacs = gpd.read_file("./data/PLD_EU.shp", bbox=bbox_france)
gdf_insitu = gpd.read_file("./data/insitu/shp/station_schapi_alti_ref_2025_river.gpkg")




def get_station_coords(code_sta):
    """
    Retourne (lon, lat) d'une station à partir de son code
    """

    row = gdf_insitu[gdf_insitu['code_sta'] == code_sta]

    if row.empty:
        print("❌ Station introuvable")
        return None, None

    # récupérer la géométrie
    geom = row.iloc[0].geometry

    # reprojeter en WGS84 si nécessaire
    point_wgs84 = gpd.GeoSeries([geom], crs=gdf_insitu.crs).to_crs("EPSG:4326")[0]

    lon, lat = point_wgs84.x, point_wgs84.y

    return lon, lat


def point_dans_lac_flag(lon, lat):
    """
    Vérifie si un point est dans un lac ou proche (~100m)
    + renvoie un flag :
        'dans_lac', 'proche_lac', 'non', 'hors_metropole'
    """

    # 🇫🇷 Filtre France métropolitaine
    if not (-6 <= lon <= 10 and 40 <= lat <= 52):
        return 'hors_metropole'

    point = Point(lon, lat)

    # Filtrer localement (petite bbox autour du point)
    bbox = (lon - 0.01, lat - 0.01, lon + 0.01, lat + 0.01)
    gdf_local = gdf_lacs.cx[bbox[0]:bbox[2], bbox[1]:bbox[3]]

    if gdf_local.empty:
        return 'non'

    gdf_point = gpd.GeoDataFrame(geometry=[point], crs="EPSG:4326")

    # Test : dans un lac
    result_within = gpd.sjoin(gdf_point, gdf_local, how="left", predicate="within")
    if not result_within['lake_id'].isna().all():
        return 'dans_lac'

    # Test proximité (~100m)
    point_buffer = point.buffer(0.001)
    gdf_buffer = gpd.GeoDataFrame(geometry=[point_buffer], crs="EPSG:4326")
    result_proche = gpd.sjoin(gdf_buffer, gdf_local, how="inner", predicate="intersects")
    if not result_proche.empty:
        return 'proche_lac'

    return 'non'
if __name__ == "__main__":

    conn = sqlite3.connect("./data/insitu_data.db")
    cursor = conn.cursor()

    cursor.execute("SELECT code_sta FROM stations_insitu")
    stations = cursor.fetchall()

    for (code_sta,) in stations:
        lon, lat = get_station_coords(code_sta)

        if lon is None:
            flag = 'coordonnees_manquantes'
        else:
            flag = point_dans_lac_flag(lon, lat)

        cursor.execute(
            "UPDATE stations_insitu SET dans_lac = ? WHERE code_sta = ?",
            (flag, code_sta)
        )
        print(f"Station {code_sta} → flag = {flag}")

    conn.commit()
    conn.close()
    print("✅ Tous les flags ont été mis à jour.")

