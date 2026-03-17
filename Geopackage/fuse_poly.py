import geopandas as gpd
from shapely.ops import unary_union
import networkx as nx
import os

def merge_touching_polygons(gdf):
    """Fusionne les polygones qui se touchent en groupes."""
    gdf = gdf.reset_index(drop=True)
    G = nx.Graph()
    G.add_nodes_from(gdf.index)
    for i, geom_i in enumerate(gdf.geometry):
        for j in range(i + 1, len(gdf)):
            if geom_i.intersects(gdf.geometry[j]):
                G.add_edge(i, j)
    merged = []
    for comp in nx.connected_components(G):
        merged_geom = unary_union(gdf.loc[list(comp), "geometry"])
        merged.append(merged_geom)
    return gpd.GeoDataFrame(geometry=merged, crs=gdf.crs)

# --------------------
# 1. Chargement
# --------------------
gdf1 = gpd.read_file("/data/sar_hydro/sad/world-administrative-boundaries/world-administrative-boundaries.shp")
gdf3 = gpd.read_file("/data/sar_hydro/Lineique/SWORD/global_reaches_SWORD_v16_poly.shp")

# --------------------
# 2. Filtrer la France
# --------------------
france = gdf1[gdf1["name"] == "France"]

# Intersection SWORD x France
gdf3_fr = gpd.overlay(gdf3, france, how="intersection")

# --------------------
# 3. Buffer sur les rivières (100 m, projection métrique)
# --------------------
proj_m = "EPSG:2154"  # Lambert 93, adapté à la France métropolitaine
gdf3_buf = gdf3_fr.to_crs(proj_m)
gdf3_buf["geometry"] = gdf3_buf.buffer(100)  # 100 m
gdf3_buf = gdf3_buf.to_crs(4326)

# --------------------
# 4. Fusionner les polygones contigus
# --------------------
merged_polys = merge_touching_polygons(gdf3_buf)

# Ajouter un ID unique
merged_polys["unique_id"] = [f"river_{i + 1}" for i in range(len(merged_polys))]

# --------------------
# 5. Export
# --------------------
output_dir = "/home/sar_hydro/STUDIES/EtudesEB/PythonProject/data/insitu/shp"
os.makedirs(output_dir, exist_ok=True)

output_path = os.path.join(output_dir, "sword_France_all_rivers.gpkg")
merged_polys.to_file(output_path, driver="GPKG")
print(f"Exporté : {output_path}")