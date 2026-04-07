import geopandas as gpd
import numpy as np
from pyproj import Transformer
from shapely.geometry import Point


from Geopackage.Sword_request import point_dans_riviere

# Charger les stations (une seule fois)
print("Chargement des stations...")
gdf = gpd.read_file("./data/insitu/shp/station_schapi_alti_ref_2025_river.gpkg")
print(gdf.columns.tolist())
print(gdf.head(2))
print("Chargement IRIS France...")
iris = gpd.read_file('./data/insitu/correction_de_pentes/IRIS_2.6_france.gpkg')
iris_proj = iris[['reach_id', 'avg_combined_slope', 'geometry']].to_crs('EPSG:2154')

sword_nodes = gpd.read_file('./data/insitu/sword/sword_nodes_france.gpkg')
sword_nodes_proj = sword_nodes[['node_id', 'reach_id', 'dist_out', 'geometry']].to_crs('EPSG:2154')


def distance_curviligne_sword(lon_hydro, lat_hydro, lon_insitu, lat_insitu):
    """
    Calcule la distance curviligne entre deux points via dist_out des nodes SWORD.
    dist_out = distance cumulée depuis l'exutoire en mètres.
    """
    point_hydro  = gpd.GeoSeries([Point(lon_hydro,  lat_hydro)],  crs='EPSG:4326').to_crs('EPSG:2154')[0]
    point_insitu = gpd.GeoSeries([Point(lon_insitu, lat_insitu)], crs='EPSG:4326').to_crs('EPSG:2154')[0]

    # Node le plus proche de chaque station
    idx1 = sword_nodes_proj.geometry.distance(point_hydro).idxmin()
    idx2 = sword_nodes_proj.geometry.distance(point_insitu).idxmin()

    dist_out_hydro  = sword_nodes_proj.loc[idx1, 'dist_out']
    dist_out_insitu = sword_nodes_proj.loc[idx2, 'dist_out']

    # Distance curviligne = différence des dist_out
    distance_m = abs(dist_out_hydro - dist_out_insitu)

    print(f"    node hydro  : {sword_nodes_proj.loc[idx1, 'node_id']} | dist_out={dist_out_hydro:.0f} m")
    print(f"    node insitu : {sword_nodes_proj.loc[idx2, 'node_id']} | dist_out={dist_out_insitu:.0f} m")
    print(f"    distance curviligne : {distance_m:.0f} m")

    return distance_m
def verifier_meme_riviere(lon_h, lat_h, lon_i, lat_i, seuil_dist_km=10):
    """
    Vérifie si deux stations sont sur la même rivière via SWORD.
    Retourne (bool, dict) avec les détails.
    """
    point_h = gpd.GeoSeries([Point(lon_h, lat_h)], crs='EPSG:4326').to_crs('EPSG:2154')[0]
    point_i = gpd.GeoSeries([Point(lon_i, lat_i)], crs='EPSG:4326').to_crs('EPSG:2154')[0]

    idx_h = sword_nodes_proj.geometry.distance(point_h).idxmin()
    idx_i = sword_nodes_proj.geometry.distance(point_i).idxmin()

    reach_h = sword_nodes_proj.loc[idx_h, 'reach_id']
    reach_i = sword_nodes_proj.loc[idx_i, 'reach_id']

    # Même reach = même tronçon de rivière
    meme_reach = (reach_h == reach_i)

    # Distance curviligne
    dist_out_h = sword_nodes_proj.loc[idx_h, 'dist_out']
    dist_out_i = sword_nodes_proj.loc[idx_i, 'dist_out']
    dist_km    = abs(dist_out_h - dist_out_i) / 1000

    # Même rivière si même reach OU distance curviligne raisonnable
    meme_riviere = meme_reach or (dist_km <= seuil_dist_km)

    return meme_riviere, {
        'reach_h':    reach_h,
        'reach_i':    reach_i,
        'meme_reach': meme_reach,
        'dist_km':    round(dist_km, 2),
    }





def distance_euclidienne_m(lon1, lat1, lon2, lat2):
    transformer = Transformer.from_crs('EPSG:4326', 'EPSG:2154', always_xy=True)
    x1, y1 = transformer.transform(lon1, lat1)
    x2, y2 = transformer.transform(lon2, lat2)
    return np.sqrt((x1 - x2)**2 + (y1 - y2)**2)

def calculer_correction_pente(lon_hydro, lat_hydro, lon_insitu, lat_insitu):
    point_hydro  = gpd.GeoSeries([Point(lon_hydro,  lat_hydro)],  crs='EPSG:4326').to_crs('EPSG:2154')[0]
    point_insitu = gpd.GeoSeries([Point(lon_insitu, lat_insitu)], crs='EPSG:4326').to_crs('EPSG:2154')[0]

    # Trouver le tronçon IRIS le plus proche
    distances = iris_proj.geometry.distance(point_hydro)
    idx = distances.idxmin()
    troncon = iris_proj.loc[idx]
    pente = troncon['avg_combined_slope']  # mm/km

    # Nodes SWORD pour distance curviligne
    idx1 = sword_nodes_proj.geometry.distance(point_hydro).idxmin()
    idx2 = sword_nodes_proj.geometry.distance(point_insitu).idxmin()
    dist_out_hydro  = sword_nodes_proj.loc[idx1, 'dist_out']
    dist_out_insitu = sword_nodes_proj.loc[idx2, 'dist_out']
    distance_km = abs(dist_out_hydro - dist_out_insitu) / 1000
    signe = np.sign(dist_out_insitu - dist_out_hydro)

    correction = 0.001 * signe * distance_km * pente
    return correction, pente, distance_km, signe


def station_la_plus_proche(lon, lat, riviere_origine=None):
    point = Point(lon, lat)
    gdf_proj = gdf.to_crs("EPSG:2154")
    point_proj = gpd.GeoSeries([point], crs="EPSG:4326").to_crs("EPSG:2154")[0]
    gdf_proj['distance_m'] = gdf_proj.geometry.distance(point_proj)
    gdf_sorted = gdf_proj.nsmallest(len(gdf_proj), 'distance_m')

    for _, closest in gdf_sorted.iterrows():
        station_point = closest.geometry
        station_geom = gpd.GeoSeries([station_point], crs="EPSG:2154").to_crs("EPSG:4326")[0]
        station_lon, station_lat = station_geom.x, station_geom.y

        if point_dans_riviere(station_lon, station_lat):
            dist = distance_curviligne_sword(lon, lat, station_lon, station_lat)
            return closest['code_sta'], dist, station_lon, station_lat

    print("❌ Aucune station trouvée dans une rivière")
    return None, None, lon, lat
#REFERENCE LONGITUDE:: 1.7671
#REFERENCE LATITUDE:: 43.7652
lon, lat = 1.7671, 43.7652

print(f"\n📍 Recherche de la station la plus proche de ({lon}, {lat})...")
