import geopandas as gpd

bbox_france = (-5.5, 41.0, 10.0, 51.5)

print("Chargement SWORD nodes France uniquement...")
sword_nodes = gpd.read_file(
    './data/insitu/sword/sword_v17b_all_nodes.gpkg',
    bbox=bbox_france
)

print(sword_nodes.columns.tolist())
print(sword_nodes.head(3))
print(sword_nodes['dist_out'].describe())

# Sauvegarde
sword_nodes.to_file('./data/insitu/sword/sword_nodes_france.gpkg', driver='GPKG')
print(f"\n✅ {len(sword_nodes)} nodes sauvegardés : sword_nodes_france.gpkg")