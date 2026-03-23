import sqlite3

import pandas as pd



def create_insitu_db(db_path="./data/insitu_data.db"):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stations_insitu (
            code_sta TEXT PRIMARY KEY,
            river_name TEXT
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