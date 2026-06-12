import os
import sqlite3
import geopandas as gpd
import pandas as pd
from pysheds.grid import Grid
from shapely.geometry import shape
import rasterio
from rasterio.windows import from_bounds
import tempfile

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
DB_PATH     = './data/hydro_data.db'
DIR_PATH    = '/home/sar_hydro/STUDIES/EtudesEB/HydroSHED/other/Fhyd_eu_dir_15s.tif'
ACC_PATH    = '/home/sar_hydro/STUDIES/EtudesEB/HydroSHED/other/hyd_eu_acc_15s.tif'
BBOX_FRANCE = {'left': -6.0, 'right': 10.0, 'bottom': 41.0, 'top': 52.0}

# ═══════════════════════════════════════════════════════════════
# FONCTIONS
# ═══════════════════════════════════════════════════════════════

def creer_table_bv(conn):
    """Crée la table bv_data si elle n'existe pas."""
    conn.execute('''
        CREATE TABLE IF NOT EXISTS bv_data (
            bv_id          INTEGER PRIMARY KEY AUTOINCREMENT,
            station_code   TEXT UNIQUE NOT NULL,
            hydroweb_name  TEXT,
            aire_km2       DECIMAL(10,2),
            polygone_wkt   TEXT,
            created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (station_code) REFERENCES stations(station_code)
        )
    ''')
    conn.commit()
    print("Table bv_data prête !")


def clipper_rasters_france(dir_path, acc_path, bbox):
    """Clippe DIR et ACC sur la France dans des fichiers temporaires."""
    print("Clipping des rasters sur la France...")
    tmp_dir      = tempfile.mkdtemp()
    tmp_dir_path = os.path.join(tmp_dir, 'dir_france.tif')
    tmp_acc_path = os.path.join(tmp_dir, 'acc_france.tif')

    for src_path, dst_path in [(dir_path, tmp_dir_path), (acc_path, tmp_acc_path)]:
        with rasterio.open(src_path) as src:
            win       = from_bounds(bbox['left'], bbox['bottom'],
                                    bbox['right'], bbox['top'], src.transform)
            data      = src.read(1, window=win)
            transform = src.window_transform(win)
            profile   = src.profile.copy()
            profile.update({'height': data.shape[0], 'width': data.shape[1],
                            'transform': transform})
            with rasterio.open(dst_path, 'w', **profile) as dst:
                dst.write(data, 1)

    print(f"Fichiers temporaires créés dans {tmp_dir}")
    return tmp_dir_path, tmp_acc_path


def calculer_bv(lon, lat, dir_path, acc_path):
    """Calcule le BV d'une station depuis les rasters clippés."""
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


def inserer_bv(conn, station_code, hydroweb_name, aire, wkt):
    """Insère ou met à jour le BV d'une station dans bv_data."""
    conn.execute('''
        INSERT INTO bv_data (station_code, hydroweb_name, aire_km2, polygone_wkt)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(station_code) DO UPDATE SET
            hydroweb_name = excluded.hydroweb_name,
            aire_km2      = excluded.aire_km2,
            polygone_wkt  = excluded.polygone_wkt
    ''', (station_code, hydroweb_name, aire, wkt))
    conn.commit()


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════
def main():
    # ── 1. Connexion BDD et création table ──
    conn = sqlite3.connect(DB_PATH)
    creer_table_bv(conn)

    # ── 2. Charger UNIQUEMENT les stations sans BV encore calculé ──
    stations = pd.read_sql('''
        SELECT s.station_code, s.hydroweb_name,
               s.reference_longitude AS lon,
               s.reference_latitude  AS lat
        FROM stations s
        LEFT JOIN bv_data b ON s.station_code = b.station_code
        WHERE s.hydroweb_name IS NOT NULL
          AND b.station_code IS NULL
    ''', conn)
    print(f"Stations restantes à calculer : {len(stations)}")

    # ── 3. Clipper les rasters une seule fois ──
    tmp_dir_path, tmp_acc_path = clipper_rasters_france(
        DIR_PATH, ACC_PATH, BBOX_FRANCE
    )

    # ── 4. Boucle sur les stations ──
    total = len(stations)
    for i, row in stations.iterrows():
        print(f"[{i+1}/{total}] {row['hydroweb_name']}...")
        try:
            aire, wkt = calculer_bv(
                row['lon'], row['lat'], tmp_dir_path, tmp_acc_path
            )
            inserer_bv(conn, row['station_code'], row['hydroweb_name'], aire, wkt)
            print(f"  → {aire} km²")
        except Exception as e:
            print(f"  → ERREUR : {e}")

    # ── 5. Vérification finale ──
    total_bv = pd.read_sql("SELECT COUNT(*) as total FROM bv_data", conn)
    print(f"\nTerminé ! {total_bv['total'].iloc[0]} BV stockés dans bv_data")
    conn.close()


if __name__ == '__main__':
    main()