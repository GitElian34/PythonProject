import os
import sqlite3
import geopandas as gpd
import pandas as pd
import numpy as np
from pysheds.grid import Grid
from shapely.geometry import shape, Point
import rasterio
from rasterio.windows import from_bounds
import tempfile

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
DB_PATH     = './data/insitu_data.db'
DIR_PATH    = '/home/sar_hydro/STUDIES/EtudesEB/HydroSHED/other/hyd_eu_dir_15s.tif'
ACC_PATH    = '/home/sar_hydro/STUDIES/EtudesEB/HydroSHED/other/hyd_eu_acc_15s.tif'
LDN_PATH    = '/home/sar_hydro/STUDIES/EtudesEB/HydroSHED/other/hyd_eu_ldn_15s.tif'
BBOX_FRANCE = {'left': -6.0, 'right': 10.0, 'bottom': 41.0, 'top': 52.0}
NODATA      = 4294967295
ERA5_RES    = 0.1

# ═══════════════════════════════════════════════════════════════
# FONCTIONS
# ═══════════════════════════════════════════════════════════════

def creer_tables(conn):
    """Crée les tables bv_data et era5_transfert."""
    conn.execute('''
        CREATE TABLE IF NOT EXISTS bv_data (
            bv_id        INTEGER PRIMARY KEY AUTOINCREMENT,
            code_sta     TEXT UNIQUE NOT NULL,
            aire_km2     DECIMAL(10,2),
            polygone_wkt TEXT,
            created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (code_sta) REFERENCES stations_insitu(code_sta)
        )
    ''')
    conn.execute('''
        CREATE TABLE IF NOT EXISTS era5_transfert (
            transfert_id INTEGER PRIMARY KEY AUTOINCREMENT,
            code_sta     TEXT NOT NULL,
            pixel_lon    DECIMAL(8,4) NOT NULL,
            pixel_lat    DECIMAL(8,4) NOT NULL,
            dist_km      DECIMAL(8,2),
            created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (code_sta) REFERENCES stations_insitu(code_sta)
        )
    ''')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_bv_code_sta ON bv_data(code_sta)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_transfert_code_sta ON era5_transfert(code_sta)')
    conn.commit()
    print("Tables prêtes !")


def clipper_rasters(dir_path, acc_path, ldn_path, bbox):
    """Clippe DIR, ACC et LDN sur la France dans des fichiers temporaires."""
    print("Clipping des rasters sur la France...")
    tmp_dir = tempfile.mkdtemp()
    paths   = {}
    for name, src_path in [('dir', dir_path), ('acc', acc_path), ('ldn', ldn_path)]:
        dst_path = os.path.join(tmp_dir, f'{name}_france.tif')
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
        paths[name] = dst_path
    print(f"Rasters clippés !")
    return paths['dir'], paths['acc'], paths['ldn']


def calculer_bv(lon, lat, dir_path, acc_path):
    """Calcule le BV d'une station."""
    grid = Grid.from_raster(dir_path)
    fdir = grid.read_raster(dir_path)
    acc  = grid.read_raster(acc_path)

    xs, ys = grid.snap_to_mask(
        acc > 500,
        (lon, lat),
        snap_threshold=1000  # en mètres
    )
    catch  = grid.catchment(x=xs, y=ys, fdir=fdir)
    aire   = round(float(catch.sum()) * 0.0625, 1)

    grid.clip_to(catch)
    shapes = grid.polygonize(grid.view(catch).astype('uint8'))
    poly   = gpd.GeoDataFrame(
        geometry=[shape(s) for s, v in shapes if v == 1],
        crs='EPSG:4326'
    ).dissolve().geometry.iloc[0]

    return aire, poly.wkt, poly


def calculer_pixels_era5(polygone, ldn_data, acc_data, lons_grid, lats_grid):
    """Calcule les pixels ERA5 dans le BV avec leur distance à la station."""
    nrows, ncols = len(lats_grid), len(lons_grid)
    minx, miny, maxx, maxy = polygone.bounds

    # Masque BV
    mask_lon   = (lons_grid >= minx) & (lons_grid <= maxx)
    mask_lat   = (lats_grid >= miny) & (lats_grid <= maxy)
    catch_mask = np.zeros((nrows, ncols), dtype=bool)

    for i in np.where(mask_lat)[0]:
        for j in np.where(mask_lon)[0]:
            if polygone.contains(Point(lons_grid[j], lats_grid[i])):
                catch_mask[i, j] = True

    # Distance via LDN
    ldn_view = ldn_data.astype(np.float64)
    ldn_view[ldn_view == NODATA] = np.nan
    ldn_view[~catch_mask]        = np.nan

    ldn_station = np.nanmin(ldn_view)
    dist_view   = ldn_view - ldn_station
    dist_view[dist_view < 0] = np.nan

    # Pixels ERA5 dans le BV
    lons_era5 = np.arange(-6.0, 10.1, ERA5_RES).round(1)
    lats_era5 = np.arange(41.0, 52.1, ERA5_RES).round(1)

    resultats = []
    for la in lats_era5[(lats_era5 >= miny) & (lats_era5 <= maxy)]:
        for lo in lons_era5[(lons_era5 >= minx) & (lons_era5 <= maxx)]:
            if not polygone.contains(Point(lo, la)):
                continue

            mask_lo  = (lons_grid >= lo - ERA5_RES/2) & (lons_grid < lo + ERA5_RES/2)
            mask_la  = (lats_grid >= la - ERA5_RES/2) & (lats_grid < la + ERA5_RES/2)
            dist_sub = dist_view[np.ix_(mask_la, mask_lo)]
            dist_val = dist_sub[~np.isnan(dist_sub)]

            if len(dist_val) == 0:
                continue

            resultats.append({
                'lon'    : lo,
                'lat'    : la,
                'dist_km': round(dist_val.mean() / 1000, 2)
            })

    return pd.DataFrame(resultats)


def inserer_bv(conn, code_sta, aire, wkt):
    """Insère ou met à jour le BV d'une station."""
    conn.execute('''
        INSERT INTO bv_data (code_sta, aire_km2, polygone_wkt)
        VALUES (?, ?, ?)
        ON CONFLICT(code_sta) DO UPDATE SET
            aire_km2     = excluded.aire_km2,
            polygone_wkt = excluded.polygone_wkt
    ''', (code_sta, aire, wkt))
    conn.commit()


def inserer_transfert(conn, code_sta, df_res):
    """Insère les pixels ERA5 avec distance."""
    conn.execute('DELETE FROM era5_transfert WHERE code_sta = ?', (code_sta,))
    for _, row in df_res.iterrows():
        conn.execute('''
            INSERT INTO era5_transfert (code_sta, pixel_lon, pixel_lat, dist_km)
            VALUES (?, ?, ?, ?)
        ''', (code_sta, row['lon'], row['lat'], row['dist_km']))
    conn.commit()


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════
def main():
    conn = sqlite3.connect(DB_PATH)
    creer_tables(conn)

    # ── 1. Charger 20 stations non encore traitées ──
    stations = pd.read_sql('''
        SELECT code_sta, river_name, lon, lat
        FROM stations_insitu
        WHERE lon IS NOT NULL
          AND lat IS NOT NULL
          AND code_sta NOT IN (SELECT code_sta FROM bv_data)
        LIMIT 3000
    ''', conn)
    print(f"Stations à traiter : {len(stations)}")

    # ── 2. Clipper les rasters une seule fois ──
    tmp_dir, tmp_acc, tmp_ldn = clipper_rasters(
        DIR_PATH, ACC_PATH, LDN_PATH, BBOX_FRANCE
    )

    # Charger LDN pour les distances ERA5
    with rasterio.open(tmp_ldn) as src:
        ldn_data  = src.read(1)
        transform = src.transform
    with rasterio.open(tmp_acc) as src:
        acc_data  = src.read(1)

    nrows, ncols = ldn_data.shape
    lons_grid = np.array([transform.c + (j + 0.5) * transform.a for j in range(ncols)])
    lats_grid = np.array([transform.f + (i + 0.5) * transform.e for i in range(nrows)])

    # ── 3. Boucle sur les stations ──
    total = len(stations)
    for i, row in stations.iterrows():
        code_sta = row['code_sta']
        lon, lat = row['lon'], row['lat']
        print(f"[{i+1}/{total}] {code_sta} ({row['river_name']})...")

        try:
            # BV
            aire, wkt, polygone = calculer_bv(lon, lat, tmp_dir, tmp_acc)
            inserer_bv(conn, code_sta, aire, wkt)
            print(f"  → BV : {aire} km²")

            # Pixels ERA5
            df_res = calculer_pixels_era5(polygone, ldn_data, acc_data,
                                          lons_grid, lats_grid)
            if not df_res.empty:
                inserer_transfert(conn, code_sta, df_res)
                print(f"  → {len(df_res)} pixels ERA5")
            else:
                print(f"  → Aucun pixel ERA5 dans le BV")

        except Exception as e:
            print(f"  → ERREUR : {e}")

    # ── 4. Vérification ──
    nb_bv  = pd.read_sql("SELECT COUNT(*) as n FROM bv_data", conn).iloc[0]['n']
    nb_era = pd.read_sql("SELECT COUNT(*) as n FROM era5_transfert", conn).iloc[0]['n']
    print(f"\nTerminé ! {nb_bv} BV | {nb_era} pixels ERA5 total")
    conn.close()


if __name__ == '__main__':
    main()