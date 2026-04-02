import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import contextily as ctx
from shapely.wkt import loads
from shapely.geometry import Point
import json

# ─────────────────────────────────────────
# PARAMÈTRES
# ─────────────────────────────────────────
CSV_PLUIE   = './data/hydroweb/ERA5/precipitations_GARONNE_2016-01-15.csv'
CSV_BV      = './data/hydroweb/bassins_versants/bassins_versants_GARONNE_10.csv'
NOM_STATION = 'R_GARONNE_AGOUT_KM0397'  # change ici

# ─────────────────────────────────────────
# 1. Charger les données
# ─────────────────────────────────────────
df_pluie = pd.read_csv(CSV_PLUIE)
df_bv    = pd.read_csv(CSV_BV)

# Récupérer la ligne de la station choisie
row_pluie = df_pluie[df_pluie['name'] == NOM_STATION].iloc[0]
row_bv    = df_bv[df_bv['name'] == NOM_STATION].iloc[0]

# Reconstruire les pixels depuis le JSON
pixels = pd.DataFrame(json.loads(row_pluie['pixels_detail']))
date   = row_pluie['date']

# Reconstruire le polygone du BV
polygone_bv = loads(row_bv['polygone'])
bv_gdf = gpd.GeoDataFrame(geometry=[polygone_bv], crs='EPSG:4326').to_crs('EPSG:3857')

# ─────────────────────────────────────────
# 2. Créer le GeoDataFrame des pixels ERA5
# ─────────────────────────────────────────
gdf_pixels = gpd.GeoDataFrame(
    pixels,
    geometry=gpd.points_from_xy(pixels.lon, pixels.lat),
    crs='EPSG:4326'
).to_crs('EPSG:3857')

# Taille approximative d'un pixel ERA5 0.1° en mètres (~8km à 45°N)
PIXEL_SIZE = 8000

# ─────────────────────────────────────────
# 3. Visualisation
# ─────────────────────────────────────────
fig, ax = plt.subplots(figsize=(14, 11))

# ── Fixer d'abord l'étendue sur le bassin versant ──
bounds = bv_gdf.total_bounds  # [xmin, ymin, xmax, ymax]
marge = 50000  # 50km de marge autour du BV
ax.set_xlim(bounds[0] - marge, bounds[2] + marge)
ax.set_ylim(bounds[1] - marge, bounds[3] + marge)

# ── Fond de carte APRÈS avoir fixé l'étendue ──
ctx.add_basemap(ax, source=ctx.providers.OpenStreetMap.Mapnik, zoom=7)

# ── Contour du bassin versant ──
bv_gdf.plot(ax=ax, color='none', edgecolor='navy', linewidth=2.5, zorder=3)

# ── Pixels ERA5 ──
# Recalculer la taille pixel selon l'étendue réelle du BV
PIXEL_SIZE = (bounds[2] - bounds[0]) / len(pixels['lon'].unique()) * 0.9
PIXEL_HEIGHT = PIXEL_SIZE * 1.4
norm = plt.Normalize(vmin=pixels['tp_mm'].min(), vmax=pixels['tp_mm'].max())
cmap = cm.get_cmap('Blues')

for _, pixel in gdf_pixels.iterrows():
    x, y = pixel.geometry.x, pixel.geometry.y
    color = cmap(norm(pixel['tp_mm']))
    rect = plt.Rectangle(
        (x - PIXEL_SIZE/2, y - PIXEL_HEIGHT/2),
        PIXEL_SIZE, PIXEL_HEIGHT,
        color=color, alpha=0.75, zorder=2
    )
    ax.add_patch(rect)
station_gdf = gpd.GeoDataFrame(
    [{'name': NOM_STATION}],
    geometry=gpd.points_from_xy([row_bv['lon']], [row_bv['lat']]),
    crs='EPSG:4326'
).to_crs('EPSG:3857')
station_gdf.plot(ax=ax, color='red', markersize=150, marker='v', zorder=5)
# ── Colorbar ──
sm = cm.ScalarMappable(cmap=cmap, norm=norm)
sm.set_array([])
cbar = plt.colorbar(sm, ax=ax, shrink=0.6, pad=0.02)
cbar.set_label('Cumul journalier (mm)', fontsize=12)

# ── Station ──
station_gdf.plot(ax=ax, color='red', markersize=150, marker='v', zorder=5)

# ── Mise en page ──
ax.set_title(
    f'Précipitations ERA5 — {NOM_STATION}\n'
    f'{date} | {len(pixels)} pixels | moy: {row_pluie["cumul_moy_mm"]} mm '
    f'| max: {row_pluie["cumul_max_mm"]} mm',
    fontsize=14, fontweight='bold', pad=15
)
ax.set_axis_off()

import os
os.makedirs('./data/hydroweb/ERA5', exist_ok=True)
plt.tight_layout()
plt.savefig(f'./data/hydroweb/ERA5/carte_{NOM_STATION}_{date}.png', dpi=150, bbox_inches='tight')
plt.show()
print("Carte sauvegardée !")