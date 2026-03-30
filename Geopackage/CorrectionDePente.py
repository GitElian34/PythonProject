import sqlite3
import pandas as pd
import glob
import os
import numpy as np
import geopandas as gpd
from shapely.geometry import Point

DB_HYDRO = './data/hydro_data.db'
CSV_DIR  = './data/insitu/data'
GPKG_PATH = './data/insitu/shp/station_schapi_alti_ref_2025.gpkg'
IRIS_PATH = './data/insitu/correction_de_pentes/IRIS_2.6_france.gpkg'
SWORD_PATH = './data/insitu/sword/sword_nodes_france.gpkg'

# Charger IRIS et SWORD une seule fois
print("📂 Chargement IRIS et SWORD...")
iris_proj        = gpd.read_file(IRIS_PATH)[['reach_id', 'avg_combined_slope', 'geometry']].to_crs('EPSG:2154')
sword_nodes_proj = gpd.read_file(SWORD_PATH)[['node_id', 'dist_out', 'geometry']].to_crs('EPSG:2154')
gdf_insitu       = gpd.read_file(GPKG_PATH).to_crs('EPSG:4326')

PAIRES = [
    ('0000000010843', 'H501012001', 'SEINE'),
    ('0000000010838', 'H509101002', 'MARNE'),
    ('0000000005744', 'O795151001', 'LOT'),
    ('112558',        'M323091020', 'MAYENNE'),
    ('0000000202497', 'M530001010', 'LOIRE'),
]


def charger_hydro(station_code):
    conn = sqlite3.connect(DB_HYDRO)
    cursor = conn.cursor()
    cursor.execute('SELECT geoid_ondulation, reference_longitude, reference_latitude FROM stations WHERE station_code = ?', (station_code,))
    row = cursor.fetchone()
    geoid = row[0] if row else 0
    lon_h = row[1] if row else None
    lat_h = row[2] if row else None

    df = pd.read_sql_query('''
        SELECT measure_date as date, ellipsoidal_height
        FROM measurements WHERE station_code = ?
        ORDER BY measure_date
    ''', conn, params=(station_code,))
    conn.close()

    df['date'] = pd.to_datetime(df['date'])
    df['hauteur_ngf'] = df['ellipsoidal_height'] - geoid
    print(f"  ✅ Hydro {station_code} : {len(df)} mesures | géoïde={geoid}m")
    return df[['date', 'ellipsoidal_height', 'hauteur_ngf']], lon_h, lat_h


def charger_insitu(station_code):
    pattern = os.path.join(CSV_DIR, f"*{station_code}*.csv")
    fichiers = glob.glob(pattern)
    if not fichiers:
        print(f"  ❌ Fichier non trouvé pour {station_code}")
        return None, None, None

    df = pd.read_csv(fichiers[0])
    df.columns = ['date', 'WSH', 'WSH_alt']
    df['date'] = pd.to_datetime(df['date'], utc=True).dt.tz_localize(None).dt.normalize()
    df = df.groupby('date').agg({'WSH': 'mean', 'WSH_alt': 'mean'}).reset_index()

    # Coordonnées depuis GeoPackage
    row = gdf_insitu[gdf_insitu['code_sta'] == station_code]
    lon_i = row.iloc[0].geometry.x if not row.empty else None
    lat_i = row.iloc[0].geometry.y if not row.empty else None

    print(f"  ✅ Insitu {station_code} : {len(df)} jours | WSH_alt~{df['WSH_alt'].mean():.2f}m")
    return df, lon_i, lat_i


def calculer_correction_pente(lon_h, lat_h, lon_i, lat_i):
    point_h = gpd.GeoSeries([Point(lon_h, lat_h)], crs='EPSG:4326').to_crs('EPSG:2154')[0]
    point_i = gpd.GeoSeries([Point(lon_i, lat_i)], crs='EPSG:4326').to_crs('EPSG:2154')[0]

    # Pente depuis IRIS
    idx_iris = iris_proj.geometry.distance(point_h).idxmin()
    pente    = iris_proj.loc[idx_iris, 'avg_combined_slope']  # mm/km

    # Distance curviligne depuis SWORD
    idx1         = sword_nodes_proj.geometry.distance(point_h).idxmin()
    idx2         = sword_nodes_proj.geometry.distance(point_i).idxmin()
    dist_out_h   = sword_nodes_proj.loc[idx1, 'dist_out']
    dist_out_i   = sword_nodes_proj.loc[idx2, 'dist_out']
    distance_km  = abs(dist_out_h - dist_out_i) / 1000
    signe        = np.sign(dist_out_i - dist_out_h)
    correction   = 0.001 * signe * distance_km * pente

    print(f"  📐 Pente IRIS    : {pente:.2f} mm/km")
    print(f"  📏 Distance SWORD: {distance_km:.3f} km")
    print(f"  🔧 Correction    : {correction:.4f} m")
    return correction, pente, distance_km


def aligner_series(df_hydro, df_insitu, correction):
    df_hydro['date']  = pd.to_datetime(df_hydro['date']).dt.normalize()
    df_insitu['date'] = pd.to_datetime(df_insitu['date']).dt.normalize()
    df = pd.merge(df_hydro, df_insitu, on='date', how='inner').dropna()

    # Avant correction
    df['ecart_avant'] = df['hauteur_ngf'] - df['WSH_alt']

    # Après correction
    df['hauteur_ngf_corr'] = df['hauteur_ngf'] - correction
    df['ecart_apres'] = df['hauteur_ngf_corr'] - df['WSH_alt']

    print(f"  📊 {len(df)} dates communes")
    print(f"  Écart moyen AVANT correction : {df['ecart_avant'].mean():.3f} m | std={df['ecart_avant'].std():.3f} m")
    print(f"  Écart moyen APRÈS correction : {df['ecart_apres'].mean():.3f} m | std={df['ecart_apres'].std():.3f} m")
    return df


if __name__ == "__main__":
    for station_hydro, station_insitu, riviere in PAIRES:
        print(f"\n{'='*60}")
        print(f"🔄 Paire : {station_hydro} ↔ {station_insitu} | {riviere}")

        df_hydro, lon_h, lat_h   = charger_hydro(station_hydro)
        df_insitu, lon_i, lat_i  = charger_insitu(station_insitu)

        if df_insitu is None or lon_h is None or lon_i is None:
            continue

        correction, pente, distance_km = calculer_correction_pente(lon_h, lat_h, lon_i, lat_i)
        df = aligner_series(df_hydro, df_insitu, correction)

        print(f"\n  Aperçu :")
        print(df[['date', 'hauteur_ngf', 'hauteur_ngf_corr', 'WSH_alt', 'ecart_avant', 'ecart_apres']].head(5).to_string(index=False))