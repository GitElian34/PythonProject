import numpy as np
import pandas as pd
import geopandas as gpd
import sqlite3
import os
from shapely.wkt import loads
from shapely.geometry import Point
from rasterio.windows import from_bounds
import rasterio

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
DB_PATH  = './data/hydro_data.db'
ACC_PATH = '/home/sar_hydro/STUDIES/EtudesEB/HydroSHED/other/hyd_eu_acc_15s.tif'
LDN_PATH = '/home/sar_hydro/STUDIES/EtudesEB/HydroSHED/other/hyd_eu_ldn_15s.tif'
NODATA   = 4294967295
ERA5_RES = 0.1
K, N     = 0.5, 0.3
BBOX     = {'left': -6.0, 'right': 10.0, 'bottom': 41.0, 'top': 52.0}

# ═══════════════════════════════════════════════════════════════
# FONCTIONS
# ═══════════════════════════════════════════════════════════════

def creer_table_transfert(conn):
    """Crée la table era5_transfert si elle n'existe pas."""
    conn.execute('''
        CREATE TABLE IF NOT EXISTS era5_transfert (
            transfert_id  INTEGER PRIMARY KEY AUTOINCREMENT,
            station_code  TEXT NOT NULL,
            hydroweb_name TEXT,
            pixel_lon     DECIMAL(8,4) NOT NULL,
            pixel_lat     DECIMAL(8,4) NOT NULL,
            dist_km       DECIMAL(8,2),
            temps_A_h     DECIMAL(8,2),
            temps_A_j     DECIMAL(8,2),
            vitesse_B     DECIMAL(8,4),
            temps_B_h     DECIMAL(8,2),
            temps_B_j     DECIMAL(8,2),
            created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (station_code) REFERENCES stations(station_code)
        )
    ''')
    conn.execute('''
        CREATE INDEX IF NOT EXISTS idx_transfert_station
        ON era5_transfert(station_code)
    ''')
    conn.commit()
    print("Table era5_transfert prête !")


def load_clipped(path, bbox):
    """Charge un raster clippé sur la bbox."""
    with rasterio.open(path) as src:
        window    = from_bounds(bbox['left'], bbox['bottom'],
                                bbox['right'], bbox['top'], src.transform)
        data      = src.read(1, window=window)
        transform = src.window_transform(window)
    return data, transform


def construire_grille(transform, shape):
    """Construit les grilles lon/lat depuis le transform."""
    nrows, ncols = shape
    lons_grid = np.array([transform.c + (j + 0.5) * transform.a for j in range(ncols)])
    lats_grid = np.array([transform.f + (i + 0.5) * transform.e for i in range(nrows)])
    return lons_grid, lats_grid


def creer_masque_bv(polygone, lons_grid, lats_grid):
    """Crée le masque booléen des pixels HydroSHEDS dans le BV."""
    nrows, ncols = len(lats_grid), len(lons_grid)
    minx, miny, maxx, maxy = polygone.bounds

    mask_lon   = (lons_grid >= minx) & (lons_grid <= maxx)
    mask_lat   = (lats_grid >= miny) & (lats_grid <= maxy)
    catch_mask = np.zeros((nrows, ncols), dtype=bool)

    for i in np.where(mask_lat)[0]:
        for j in np.where(mask_lon)[0]:
            if polygone.contains(Point(lons_grid[j], lats_grid[i])):
                catch_mask[i, j] = True

    return catch_mask


def calculer_distances(ldn_data, acc_data, catch_mask):
    """Calcule dist_view et acc_view masqués sur le BV."""
    ldn_view = ldn_data.astype(np.float64)
    acc_view = acc_data.astype(np.float64)

    ldn_view[ldn_view == NODATA] = np.nan
    acc_view[acc_view == NODATA] = np.nan
    ldn_view[~catch_mask]        = np.nan
    acc_view[~catch_mask]        = np.nan

    ldn_station   = np.nanmin(ldn_view)
    dist_view     = ldn_view - ldn_station
    dist_view[dist_view < 0] = np.nan

    return dist_view, acc_view


def pixels_era5_dans_bv(polygone, lons_era5, lats_era5):
    """Retourne les pixels ERA5 qui tombent dans le polygone du BV."""
    minx, miny, maxx, maxy = polygone.bounds
    lats_f = lats_era5[(lats_era5 >= miny) & (lats_era5 <= maxy)]
    lons_f = lons_era5[(lons_era5 >= minx) & (lons_era5 <= maxx)]

    pixels = []
    for la in lats_f:
        for lo in lons_f:
            if polygone.contains(Point(lo, la)):
                pixels.append({'lon': lo, 'lat': la})
    return pd.DataFrame(pixels)


def calculer_temps_transfert(pixels, dist_view, acc_view, lons_grid, lats_grid):
    """Calcule temps de transfert A et B pour chaque pixel ERA5."""
    resultats = []

    for _, pixel in pixels.iterrows():
        lo, la = pixel['lon'], pixel['lat']

        mask_lon = (lons_grid >= lo - ERA5_RES/2) & (lons_grid < lo + ERA5_RES/2)
        mask_lat = (lats_grid >= la - ERA5_RES/2) & (lats_grid < la + ERA5_RES/2)

        dist_sub = dist_view[np.ix_(mask_lat, mask_lon)]
        acc_sub  = acc_view[np.ix_(mask_lat, mask_lon)]

        dist_val = dist_sub[~np.isnan(dist_sub)]
        acc_val  = acc_sub[~np.isnan(acc_sub)]

        if len(dist_val) == 0:
            continue

        dist_moy  = dist_val.mean()
        acc_moy   = acc_val.mean()
        temps_A_h = dist_moy / 1.0 / 3600
        vitesse_B = K * (max(acc_moy, 1) ** N)
        temps_B_h = dist_moy / vitesse_B / 3600

        resultats.append({
            'lon'      : lo,
            'lat'      : la,
            'dist_km'  : round(dist_moy / 1000, 2),
            'temps_A_h': round(temps_A_h, 2),
            'temps_A_j': round(temps_A_h / 24, 2),
            'vitesse_B': round(vitesse_B, 4),
            'temps_B_h': round(temps_B_h, 2),
            'temps_B_j': round(temps_B_h / 24, 2),
        })

    return pd.DataFrame(resultats)


def inserer_transfert(conn, station_code, hydroweb_name, df_res):
    """Insère les pixels ERA5 avec temps de transfert dans la BDD."""
    # Supprimer les anciens résultats pour cette station
    conn.execute('DELETE FROM era5_transfert WHERE station_code = ?', (station_code,))

    for _, row in df_res.iterrows():
        conn.execute('''
            INSERT INTO era5_transfert (
                station_code, hydroweb_name,
                pixel_lon, pixel_lat, dist_km,
                temps_A_h, temps_A_j,
                vitesse_B, temps_B_h, temps_B_j
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            station_code, hydroweb_name,
            row['lon'], row['lat'], row['dist_km'],
            row['temps_A_h'], row['temps_A_j'],
            row['vitesse_B'], row['temps_B_h'], row['temps_B_j']
        ))
    conn.commit()


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════
def main():
    conn = sqlite3.connect(DB_PATH)
    creer_table_transfert(conn)

    # ── 1. Charger les stations avec BV calculé ──
    stations = pd.read_sql('''
        SELECT s.station_code, s.hydroweb_name,
               s.reference_longitude AS lon,
               s.reference_latitude  AS lat,
               b.polygone_wkt
        FROM stations s
        JOIN bv_data b ON s.station_code = b.station_code
        WHERE s.hydroweb_name IS NOT NULL
          AND b.polygone_wkt IS NOT NULL
    ''', conn)
    print(f"Stations à traiter : {len(stations)}")

    # ── 2. Charger les rasters une seule fois ──
    print("Chargement HydroSHEDS...")
    ldn_data, ldn_transform = load_clipped(LDN_PATH, BBOX)
    acc_data, _             = load_clipped(ACC_PATH, BBOX)
    lons_grid, lats_grid    = construire_grille(ldn_transform, ldn_data.shape)

    # ── 3. Construire une grille ERA5 approximative ──
    # On utilise les coordonnées ERA5 France (0.1° de résolution)
    lons_era5 = np.arange(-6.0, 10.1, ERA5_RES).round(1)
    lats_era5 = np.arange(41.0, 52.1, ERA5_RES).round(1)

    # ── 4. Boucle sur les stations ──
    total = len(stations)
    for i, row in stations.iterrows():
        print(f"[{i+1}/{total}] {row['hydroweb_name']}...")
        try:
            polygone   = loads(row['polygone_wkt'])
            catch_mask = creer_masque_bv(polygone, lons_grid, lats_grid)
            dist_view, acc_view = calculer_distances(ldn_data, acc_data, catch_mask)

            pixels = pixels_era5_dans_bv(polygone, lons_era5, lats_era5)
            if pixels.empty:
                print(f"  → Aucun pixel ERA5 dans le BV")
                continue

            df_res = calculer_temps_transfert(pixels, dist_view, acc_view,
                                              lons_grid, lats_grid)
            if df_res.empty:
                print(f"  → Aucun résultat de transfert")
                continue

            inserer_transfert(conn, row['station_code'], row['hydroweb_name'], df_res)
            print(f"  → {len(df_res)} pixels ERA5 insérés")

        except Exception as e:
            print(f"  → ERREUR : {e}")

    # ── 5. Vérification finale ──
    total_pixels = pd.read_sql("SELECT COUNT(*) as total FROM era5_transfert", conn)
    print(f"\nTerminé ! {total_pixels['total'].iloc[0]} pixels ERA5 stockés dans era5_transfert")
    conn.close()


if __name__ == '__main__':
    main()