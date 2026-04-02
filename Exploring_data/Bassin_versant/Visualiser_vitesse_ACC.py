import numpy as np
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import contextily as ctx
from shapely.wkt import loads
from shapely.geometry import Point, box
import rasterio
from rasterio.windows import from_bounds

# ─────────────────────────────────────────
# PARAMÈTRES
# ─────────────────────────────────────────
NOM_STATION = 'R_GARONNE_GARONNE_KM0084'
CSV_BV      = './data/hydroweb/bassins_versants/bassins_versants_GARONNE_10.csv'
ACC_PATH    = '/home/sar_hydro/STUDIES/EtudesEB/HydroSHED/other/hyd_eu_acc_15s.tif'
NODATA      = 4294967295

# ─────────────────────────────────────────
# 1. Charger le polygone
# ─────────────────────────────────────────
df_bv       = pd.read_csv(CSV_BV)
row_bv      = df_bv[df_bv['name'] == NOM_STATION].iloc[0]
polygone_bv = loads(row_bv['polygone'])
lon, lat    = row_bv['lon'], row_bv['lat']

minx, miny, maxx, maxy = polygone_bv.bounds
print(f"Station : {NOM_STATION}")
print(f"Bounds BV : {minx:.2f}, {miny:.2f}, {maxx:.2f}, {maxy:.2f}")

# ─────────────────────────────────────────
# 2. Charger ACC clippé sur le BV uniquement
# ─────────────────────────────────────────
print("Chargement ACC sur le BV...")
marge = 0.5  # marge en degrés autour du BV

with rasterio.open(ACC_PATH) as src:
    window    = from_bounds(
        minx - marge, miny - marge,
        maxx + marge, maxy + marge,
        src.transform
    )
    acc_data  = src.read(1, window=window).astype(np.float64)
    transform = src.window_transform(window)

acc_data[acc_data == NODATA] = np.nan

# Grille de coordonnées HydroSHEDS
nrows, ncols = acc_data.shape
lons_grid = np.array([transform.c + (j + 0.5) * transform.a for j in range(ncols)])
lats_grid = np.array([transform.f + (i + 0.5) * transform.e for i in range(nrows)])

print(f"Grille HydroSHEDS : {nrows} x {ncols} pixels")

# ─────────────────────────────────────────
# 3. Masquer les pixels hors du polygone
# ─────────────────────────────────────────
print("Masquage des pixels hors BV...")
catch_mask = np.zeros((nrows, ncols), dtype=bool)

mask_lon = (lons_grid >= minx) & (lons_grid <= maxx)
mask_lat = (lats_grid >= miny) & (lats_grid <= maxy)

for i in np.where(mask_lat)[0]:
    for j in np.where(mask_lon)[0]:
        if polygone_bv.contains(Point(lons_grid[j], lats_grid[i])):
            catch_mask[i, j] = True

acc_data[~catch_mask] = np.nan
print(f"Pixels HydroSHEDS dans le BV : {catch_mask.sum()}")

# ─────────────────────────────────────────
# 4. Construire le GeoDataFrame des pixels
# ─────────────────────────────────────────
print("Construction du GeoDataFrame...")
rows_idx, cols_idx = np.where(catch_mask & ~np.isnan(acc_data))

df_pixels = pd.DataFrame({
    'lon'    : lons_grid[cols_idx],
    'lat'    : lats_grid[rows_idx],
    'acc'    : acc_data[rows_idx, cols_idx],
    'acc_log': np.log10(acc_data[rows_idx, cols_idx] + 1)
})

print(f"ACC min : {df_pixels['acc'].min():.0f} | max : {df_pixels['acc'].max():.0f}")

# ─────────────────────────────────────────
# 5. Visualisation raster directe
#    (plus rapide que dessiner pixel par pixel)
# ─────────────────────────────────────────
bv_gdf = gpd.GeoDataFrame(
    geometry=[polygone_bv], crs='EPSG:4326'
).to_crs('EPSG:3857')

station_gdf = gpd.GeoDataFrame(
    [{'name': NOM_STATION}],
    geometry=gpd.points_from_xy([lon], [lat]),
    crs='EPSG:4326'
).to_crs('EPSG:3857')

bounds = bv_gdf.total_bounds
marge_m = 50000

# Préparer la grille ACC en log pour l'affichage
acc_log_grid = np.where(catch_mask, np.log10(acc_data + 1), np.nan)

# Convertir les coordonnées en Web Mercator pour imshow
from pyproj import Transformer
transformer = Transformer.from_crs('EPSG:4326', 'EPSG:3857', always_xy=True)
x_min, y_min = transformer.transform(lons_grid.min(), lats_grid.min())
x_max, y_max = transformer.transform(lons_grid.max(), lats_grid.max())

fig, ax = plt.subplots(figsize=(14, 11))

ax.set_xlim(bounds[0] - marge_m, bounds[2] + marge_m)
ax.set_ylim(bounds[1] - marge_m, bounds[3] + marge_m)

ctx.add_basemap(ax, source=ctx.providers.OpenStreetMap.Mapnik, zoom=7)

# Afficher le raster ACC comme image
im = ax.imshow(
    acc_log_grid,
    extent   = [x_min, x_max, y_min, y_max],
    origin   = 'upper',
    cmap     = 'YlOrRd',
    alpha    = 0.75,
    zorder   = 2,
    aspect   = 'auto'
)

# Contour du BV
bv_gdf.plot(ax=ax, color='none', edgecolor='navy', linewidth=2.5, zorder=3)

# Station
station_gdf.plot(ax=ax, color='red', markersize=150, marker='v', zorder=5)

# Colorbar
cbar = plt.colorbar(im, ax=ax, shrink=0.6, pad=0.02)
cbar.set_label('Accumulation de flux (échelle log)', fontsize=11)
vmin = np.nanmin(acc_log_grid)
vmax = np.nanmax(acc_log_grid)
ticks = np.linspace(vmin, vmax, 5)
cbar.set_ticks(ticks)
cbar.set_ticklabels([f'{10**t:.0f}' for t in ticks])

ax.set_title(
    f'Accumulation de flux HydroSHEDS — {NOM_STATION}\n'
    f'Jaune = versants (lent) | Rouge = grands cours d\'eau (rapide)',
    fontsize=13, fontweight='bold', pad=15
)
ax.set_axis_off()

import os
os.makedirs('./data/hydroweb/ERA5', exist_ok=True)
plt.tight_layout()
plt.savefig(f'./data/hydroweb/ERA5/acc_hydrosheds_{NOM_STATION}.png',
            dpi=150, bbox_inches='tight')
plt.show()
print("Carte sauvegardée !")