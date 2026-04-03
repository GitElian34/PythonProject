import sqlite3

import pandas as pd



def create_insitu_db(db_path="./data/insitu_data.db"):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stations_insitu (
            code_sta TEXT PRIMARY KEY,
            river_name TEXT,
            lon REAL,                          -- NOUVEAU
            lat REAL,                          -- NOUVEAU
            dans_lac TEXT DEFAULT 'inconnu',
            qualite_sauts TEXT,
            signal_plat INTEGER,
            gap_max_jours INTEGER
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS mesures_insitu (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code_sta TEXT NOT NULL,
            date DATE NOT NULL,
            h_01h_wsh REAL,
            h_09h_wsh REAL,
            h_17h_wsh REAL,
            h_01h_alt REAL,
            h_09h_alt REAL,
            h_17h_alt REAL,
            h_med_wsh REAL, 
            UNIQUE(code_sta, date),
            FOREIGN KEY (code_sta) REFERENCES stations_insitu(code_sta)
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS era5_insitu (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code_sta TEXT NOT NULL,
            date DATE NOT NULL,
            temp_min_jour REAL,
            temp_max_jour REAL,
            temp_moy_jour REAL,
            precip_jour REAL,
            temp_moy_10j REAL,
            precip_moy_10j REAL,
            UNIQUE(code_sta, date),
            FOREIGN KEY (code_sta) REFERENCES stations_insitu(code_sta)
        )
    ''')
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_mesures_station_date
        ON mesures_insitu(code_sta, date)
    ''')
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_era5_station_date
        ON era5_insitu(code_sta, date)
    ''')
    conn.commit()
    conn.close()


def insert_station_insitu(conn, code_sta, river_name):
    cursor = conn.cursor()
    cursor.execute('''
        INSERT OR IGNORE INTO stations_insitu (code_sta, river_name)
        VALUES (?, ?)
    ''', (code_sta, river_name))
    conn.commit()

def station_era5_complete(conn, code_sta):
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM era5_insitu WHERE code_sta = ?', (code_sta,))
    return cursor.fetchone()[0] > 0

def insert_mesure_insitu(conn, code_sta, date,
                         h01_wsh, h09_wsh, h17_wsh,
                         h01_alt, h09_alt, h17_alt):
    cursor = conn.cursor()
    cursor.execute('''
        INSERT OR IGNORE INTO mesures_insitu
            (code_sta, date, h_01h_wsh, h_09h_wsh, h_17h_wsh, h_01h_alt, h_09h_alt, h_17h_alt)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (code_sta, str(date), h01_wsh, h09_wsh, h17_wsh, h01_alt, h09_alt, h17_alt))
    conn.commit()


def insert_era5(conn, code_sta, date, metadata):
    conn.execute('''
        INSERT OR IGNORE INTO era5_insitu
            (code_sta, date, temp_min_jour, temp_max_jour, temp_moy_jour,
             precip_jour, temp_moy_10j, precip_moy_10j)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (code_sta, date,
          float(metadata['temp_min_jour']),
          float(metadata['temp_max_jour']),
          float(metadata['temp_moy_jour']),
          float(metadata['precip_jour']),
          float(metadata['temp_moy10j']),
          float(metadata['precip_moy10j'])))
    conn.commit()


def get_mesures_insitu(conn, code_sta):
    cursor = conn.cursor()
    cursor.execute('''
        SELECT date, h_01h_wsh, h_09h_wsh, h_17h_wsh, h_01h_alt, h_09h_alt, h_17h_alt
        FROM mesures_insitu
        WHERE code_sta = ?
        ORDER BY date
    ''', (code_sta,))
    return cursor.fetchall()

def get_donnees_station(code_sta, db_path="./data/insitu_data.db"):
    """Récupère et joint les données insitu + ERA5 pour une station"""
    conn = sqlite3.connect(db_path)

    query = '''
        SELECT 
            m.date,
            m.h_01h_wsh,
            m.h_09h_wsh,
            m.h_17h_wsh,
            m.h_med_wsh,
            e.precip_jour,
            e.temp_min_jour,
            e.temp_max_jour,
            e.temp_moy_jour,
            e.temp_moy_10j,
            e.precip_moy_10j
        FROM mesures_insitu m
        LEFT JOIN era5_insitu e ON m.code_sta = e.code_sta AND m.date = e.date
        WHERE m.code_sta = ?
        ORDER BY m.date
    '''

    df = pd.read_sql_query(query, conn, params=(code_sta,))
    conn.close()

    df['date'] = pd.to_datetime(df['date'])
    df = df.dropna()
    df = df.sort_values('date').reset_index(drop=True)

    print(f"✅ {len(df)} lignes récupérées pour la station {code_sta}")
    print(df.head())
    return df

def get_era5_bv(code_sta, db_path="./data/insitu_data.db"):
    """
    Récupère uniquement les 50 features ERA5-BV (5 tranches x 10 jours)
    pour une station, indexées par date.
    """
    conn = sqlite3.connect(db_path)

    df_bv = pd.read_sql_query('''
        SELECT m.date, p.tranche_km,
               p.J0, p.J1, p.J2, p.J3, p.J4,
               p.J5, p.J6, p.J7, p.J8, p.J9
        FROM era5_pluie_bv p
        JOIN mesures_insitu m ON p.mesure_id = m.id
        WHERE p.code_sta = ?
        ORDER BY m.date
    ''', conn, params=(code_sta,))

    conn.close()

    if df_bv.empty:
        print(f"⚠️  {code_sta} — pas de données ERA5-BV")
        return None

    # Pivoter : une ligne par date, 50 colonnes
    df_pivot = df_bv.pivot(
        index='date', columns='tranche_km',
        values=['J0','J1','J2','J3','J4','J5','J6','J7','J8','J9']
    )
    df_pivot.columns = [f'{j}_{t}' for j, t in df_pivot.columns]
    df_pivot = df_pivot.reset_index()
    df_pivot['date'] = pd.to_datetime(df_pivot['date'])

    print(f"✅ {len(df_pivot)} lignes | {df_pivot.shape[1]-1} features ERA5-BV pour {code_sta}")
    return df_pivot

def get_donnees_station_bv(code_sta, db_path="./data/insitu_data.db"):
    """
    Récupère les données insitu + ERA5 + features ERA5-BV pour une station.
    Remplace precip_moy_10j par les 50 features ERA5-BV.
    """
    # Données de base sans precip_moy_10j
    df = get_donnees_station(code_sta, db_path)
    if df is None:
        return None

    # Features BV
    df_bv = get_era5_bv(code_sta, db_path)
    if df_bv is None:
        print(f"⚠️  {code_sta} — pas de données BV, retour baseline")
        return df

    # Joindre et supprimer precip_moy_10j
    df = df.merge(df_bv, on='date', how='inner')
    df = df.drop(columns=['precip_moy_10j'], errors='ignore')
    df = df.dropna().reset_index(drop=True)

    print(f"✅ {len(df)} lignes | {df.shape[1]} colonnes (avec BV) pour {code_sta}")
    return df


def add_coordinates_from_gpkg(db_path='./data/insitu_data.db',
                               gpkg_path='./data/insitu/shp/station_schapi_alti_ref_2025.gpkg'):
    """
    Ajoute les coordonnées lon/lat dans stations_insitu
    depuis le fichier gpkg SCHAPI.
    """
    import geopandas as gpd

    gdf    = gpd.read_file(gpkg_path)
    gdf_fr = gdf.cx[-5.2:9.6, 41.3:51.1]

    conn   = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Ajouter les colonnes si BDD existante sans ces colonnes
    for col in ['lon', 'lat']:
        try:
            cursor.execute(f'ALTER TABLE stations_insitu ADD COLUMN {col} REAL')
        except Exception:
            pass

    updated = 0
    for _, row in gdf_fr.iterrows():
        cursor.execute(
            'UPDATE stations_insitu SET lon=?, lat=? WHERE code_sta=?',
            (row['lon'], row['lat'], row['code_sta'])
        )
        updated += cursor.rowcount

    conn.commit()
    conn.close()
    print(f"✅ {updated} stations mises à jour avec lon/lat")
    return updated

def get_stations_insitu(conn):
    cursor = conn.cursor()
    cursor.execute('SELECT code_sta, river_name FROM stations_insitu')
    return cursor.fetchall()


def get_era5_insitu(conn, code_sta):
    cursor = conn.cursor()
    cursor.execute('''
        SELECT date, temp_min_jour, temp_max_jour, temp_moy_jour,
               precip_jour, temp_moy_10j, precip_moy_10j
        FROM era5_insitu
        WHERE code_sta = ?
        ORDER BY date
    ''', (code_sta,))
    return cursor.fetchall()