import os
import geopandas as gpd
from pysheds.grid import Grid
import pandas as pd
from shapely.geometry import shape
import rasterio
from rasterio.windows import from_bounds
import tempfile
import numpy as np

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
SHP_PATH    = './data/hydroweb/shp/hydroweb.shp'
DIR_PATH    = '/home/sar_hydro/STUDIES/EtudesEB/HydroSHED/other/hyd_eu_dir_15s.tif'
ACC_PATH    = '/home/sar_hydro/STUDIES/EtudesEB/HydroSHED/other/hyd_eu_acc_15s.tif'
OUTPUT_DIR  = './data/hydroweb/bassins_versants'
OUTPUT_FILE = 'bassins_versants_GARONNE_10.csv'
BBOX_FRANCE = {'left': -6.0, 'right': 10.0, 'bottom': 41.0, 'top': 52.0}

# ═══════════════════════════════════════════════════════════════
# FONCTIONS
# ═══════════════════════════════════════════════════════════════

def clipper_rasters_france(dir_path, acc_path, bbox):
    """
    Clippe DIR et ACC sur la France et les sauvegarde
    dans des fichiers temporaires pour pysheds.
    """
    print("Clipping des rasters sur la France...")
    tmp_dir = tempfile.mkdtemp()
    tmp_dir_path = os.path.join(tmp_dir, 'dir_france.tif')
    tmp_acc_path = os.path.join(tmp_dir, 'acc_france.tif')

    for src_path, dst_path in [(dir_path, tmp_dir_path), (acc_path, tmp_acc_path)]:
        with rasterio.open(src_path) as src:
            win      = from_bounds(bbox['left'], bbox['bottom'],
                                   bbox['right'], bbox['top'], src.transform)
            data     = src.read(1, window=win)
            transform = src.window_transform(win)
            profile  = src.profile.copy()
            profile.update({
                'height'   : data.shape[0],
                'width'    : data.shape[1],
                'transform': transform
            })
            with rasterio.open(dst_path, 'w', **profile) as dst:
                dst.write(data, 1)

    print(f"Fichiers temporaires créés dans {tmp_dir}")
    return tmp_dir_path, tmp_acc_path


def calculer_bv(lon, lat, dir_path, acc_path):
    """
    Calcule le BV d'une station.
    Réinitialise le grid à chaque appel pour éviter le bug clip_to.
    """
    grid = Grid.from_raster(dir_path)
    fdir = grid.read_raster(dir_path)
    acc  = grid.read_raster(acc_path)

    xs, ys = grid.snap_to_mask(acc > 500, (lon, lat))
    catch  = grid.catchment(x=xs, y=ys, fdir=fdir)
    aire   = round(float(catch.sum()) * 0.0625, 1)

    grid.clip_to(catch)
    shapes = grid.polygonize(grid.view(catch).astype('uint8'))
    poly   = gpd.GeoDataFrame(
        geometry=[shape(s) for s, v in shapes if v == 1],
        crs='EPSG:4326'
    ).dissolve().geometry.iloc[0]

    return aire, poly.wkt


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════
def main():
    # Charger et filtrer les stations
    stations = gpd.read_file(SHP_PATH)
    if stations.crs is None:
        stations = stations.set_crs('EPSG:4326')
    stations = stations.cx[-5.2:9.6, 41.3:51.1]
    stations = stations[stations['name'].str.contains('GARONNE')].head(10)
    print(f"Stations : {len(stations)}")

    # Clipper les rasters une seule fois sur la France
    tmp_dir_path, tmp_acc_path = clipper_rasters_france(
        DIR_PATH, ACC_PATH, BBOX_FRANCE
    )

    # Boucle sur les stations
    resultats = []
    for i, (_, s) in enumerate(stations.iterrows()):
        print(f"[{i+1}/{len(stations)}] {s['name']}...")
        try:
            aire, wkt = calculer_bv(
                s['lon'], s['lat'], tmp_dir_path, tmp_acc_path
            )
            print(f"  → {aire} km²")
            resultats.append({'name': s['name'], 'lon': s['lon'],
                              'lat': s['lat'], 'aire_km2': aire, 'polygone': wkt})
        except Exception as e:
            print(f"  → ERREUR : {e}")
            resultats.append({'name': s['name'], 'lon': s['lon'],
                              'lat': s['lat'], 'aire_km2': None, 'polygone': None})

    # Sauvegarder
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    pd.DataFrame(resultats).to_csv(f'{OUTPUT_DIR}/{OUTPUT_FILE}', index=False)
    print(f"\nTerminé ! {len(resultats)} stations sauvegardées.")


if __name__ == '__main__':
    main()