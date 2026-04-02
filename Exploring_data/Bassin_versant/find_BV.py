import os

import geopandas as gpd
from pysheds.grid import Grid
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
from shapely.geometry import shape
import contextily as ctx

# ─────────────────────────────────────────
# 1. Charger et filtrer les stations
# ─────────────────────────────────────────
stations = gpd.read_file('./data/hydroweb/shp/hydroweb.shp')
if stations.crs is None:
    stations = stations.set_crs('EPSG:4326')

stations = stations.cx[-5.2:9.6, 41.3:51.1]

# ─────────────────────────────────────────
# 2. Choisir UNE station (change le nom ici)
# ─────────────────────────────────────────
NOM_STATION ='R_GARONNE_GARONNE_KM0084'   # ou mets directement 'L_nom_station'

station_test = stations[stations['name'] == NOM_STATION].iloc[0]
lon, lat = station_test['lon'], station_test['lat']
print(f"Station sélectionnée : {NOM_STATION} ({lon}, {lat})")

# ─────────────────────────────────────────
# 3. Charger les fichiers HydroSHEDS
# ─────────────────────────────────────────
DIR_PATH = '/home/sar_hydro/STUDIES/EtudesEB/HydroSHED/other/hyd_eu_dir_15s.tif'
ACC_PATH = '/home/sar_hydro/STUDIES/EtudesEB/HydroSHED/other/hyd_eu_acc_15s.tif'

grid = Grid.from_raster(DIR_PATH)
fdir = grid.read_raster(DIR_PATH)
acc  = grid.read_raster(ACC_PATH)

# ─────────────────────────────────────────
# 4. Calculer le bassin versant
# ─────────────────────────────────────────
# Snap : accroche le point au pixel rivière le plus proche
x_snap, y_snap = grid.snap_to_mask(acc > 500, (lon, lat))

# Remonter tous les pixels en amont depuis ce point
catch = grid.catchment(x=x_snap, y=y_snap, fdir=fdir)

# Calculer l'aire (chaque pixel 15s ≈ 0.0625 km²)
aire_km2 = catch.sum() * 0.0625
print(f"Aire drainée ≈ {aire_km2:.0f} km²")

# ─────────────────────────────────────────
# 5. Convertir le raster en polygone vecteur
# ─────────────────────────────────────────
grid.clip_to(catch)
catch_view = grid.view(catch).astype('uint8')  # conversion explicite en entier 0/1
shapes = grid.polygonize(catch_view)

polygones = [shape(s) for s, v in shapes if v == 1]
bv_gdf = gpd.GeoDataFrame(geometry=polygones, crs='EPSG:4326').dissolve()

# Reprojection en Web Mercator pour le fond de carte
bv_web = bv_gdf.to_crs('EPSG:3857')
station_gdf = gpd.GeoDataFrame(
    [{'name': NOM_STATION}],
    geometry=gpd.points_from_xy([lon], [lat]),
    crs='EPSG:4326'
).to_crs('EPSG:3857')

# ─────────────────────────────────────────
# 6. Visualisation
# ─────────────────────────────────────────
fig, ax = plt.subplots(1, 1, figsize=(12, 10))

bv_web.plot(ax=ax, color='steelblue', alpha=0.4, edgecolor='navy', linewidth=2)
station_gdf.plot(ax=ax, color='red', markersize=120, marker='v', zorder=5)

ctx.add_basemap(ax, source=ctx.providers.OpenStreetMap.Mapnik, zoom=8)

ax.set_title(
    f'Bassin versant de la station {NOM_STATION}\nAire drainée ≈ {aire_km2:.0f} km²',
    fontsize=16, fontweight='bold', pad=20
)
ax.set_axis_off()

legend_elements = [
    Patch(facecolor='steelblue', edgecolor='navy', alpha=0.6,
          label=f'Bassin versant ({aire_km2:.0f} km²)'),
    plt.Line2D([0], [0], marker='v', color='w', markerfacecolor='red',
               markersize=12, label=f'Station : {NOM_STATION}')
]
ax.legend(handles=legend_elements, loc='lower right', fontsize=12,
          framealpha=0.9, edgecolor='gray')

plt.tight_layout()
os.makedirs('./data/hydroweb/bassins_versants', exist_ok=True)
plt.savefig(f'./data/hydroweb/bassins_versants/carte_{NOM_STATION}.png',
            dpi=150, bbox_inches='tight')
plt.show()
print("Carte sauvegardée !")