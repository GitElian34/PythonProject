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

def creer_table_corine(conn):
    """Crée la table bv_corine pour stocker les fractions CORINE + texture des sols."""
    conn.execute('''
        CREATE TABLE IF NOT EXISTS bv_corine (
            corine_id         INTEGER PRIMARY KEY AUTOINCREMENT,
            code_sta          TEXT UNIQUE NOT NULL,
            frac_urban        DECIMAL(5,4),
            frac_agriculture  DECIMAL(5,4),
            frac_forest       DECIMAL(5,4),
            frac_semi_natural DECIMAL(5,4),
            frac_wetland      DECIMAL(5,4),
            frac_water        DECIMAL(5,4),
            nb_pixels         INTEGER,
            sg_clay_0_30cm    DECIMAL(5,2),
            sg_sand_0_30cm    DECIMAL(5,2),
            sg_silt_0_30cm    DECIMAL(5,2),
            created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (code_sta) REFERENCES stations_insitu(code_sta)
        )
    ''')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_corine_code_sta ON bv_corine(code_sta)')
    conn.commit()


def inserer_corine(conn, code_sta, fractions, nb_pixels, soil=None):
    """Insère ou met à jour les fractions CORINE + texture sols d'une station."""
    soil = soil or {}
    conn.execute('''
        INSERT INTO bv_corine (
            code_sta, frac_urban, frac_agriculture, frac_forest,
            frac_semi_natural, frac_wetland, frac_water, nb_pixels,
            sg_clay_0_30cm, sg_sand_0_30cm, sg_silt_0_30cm
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(code_sta) DO UPDATE SET
            frac_urban        = excluded.frac_urban,
            frac_agriculture  = excluded.frac_agriculture,
            frac_forest       = excluded.frac_forest,
            frac_semi_natural = excluded.frac_semi_natural,
            frac_wetland      = excluded.frac_wetland,
            frac_water        = excluded.frac_water,
            nb_pixels         = excluded.nb_pixels,
            sg_clay_0_30cm    = excluded.sg_clay_0_30cm,
            sg_sand_0_30cm    = excluded.sg_sand_0_30cm,
            sg_silt_0_30cm    = excluded.sg_silt_0_30cm
    ''', (
        code_sta,
        round(fractions['urban'],        4),
        round(fractions['agriculture'],  4),
        round(fractions['forest'],       4),
        round(fractions['semi_natural'], 4),
        round(fractions['wetland'],      4),
        round(fractions['water'],        4),
        nb_pixels,
        round(soil.get('clay', 0), 2) if soil.get('clay') else None,
        round(soil.get('sand', 0), 2) if soil.get('sand') else None,
        round(soil.get('silt', 0), 2) if soil.get('silt') else None,
    ))
    conn.commit()


def get_corine_bv(code_sta, db_path="./data/insitu_data.db"):
    """Récupère les fractions CORINE + texture sols d'une station."""
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query('''
        SELECT frac_urban, frac_agriculture, frac_forest,
               frac_semi_natural, frac_wetland, frac_water,
               sg_clay_0_30cm, sg_sand_0_30cm, sg_silt_0_30cm
        FROM bv_corine WHERE code_sta = ?
    ''', conn, params=(code_sta,))
    conn.close()
    if df.empty:
        return None
    return df.iloc[0].to_dict()


def creer_table_era5_bv_jour(conn):
    conn.execute('''
        CREATE TABLE IF NOT EXISTS era5_bv_jour (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            mesure_id      INTEGER NOT NULL,
            code_sta       TEXT NOT NULL,
            mesure_date    DATE NOT NULL,
            temp_moy_bv    DECIMAL(6,3),
            precip_sum_bv  DECIMAL(8,3),
            pet_sum_bv     DECIMAL(8,3),
            nb_pixels      INTEGER,
            created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(mesure_id),
            FOREIGN KEY (mesure_id) REFERENCES mesures_insitu(id),
            FOREIGN KEY (code_sta)  REFERENCES stations_insitu(code_sta)
        )
    ''')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_era5_bv_jour_sta_date ON era5_bv_jour(code_sta, mesure_date)')
    conn.commit()


def inserer_era5_bv_jour(conn, mesure_id, code_sta, mesure_date,
                          temp_moy, precip_sum, pet_sum, nb_pixels):
    conn.execute('''
        INSERT INTO era5_bv_jour (
            mesure_id, code_sta, mesure_date,
            temp_moy_bv, precip_sum_bv, pet_sum_bv, nb_pixels
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(mesure_id) DO UPDATE SET
            temp_moy_bv   = excluded.temp_moy_bv,
            precip_sum_bv = excluded.precip_sum_bv,
            pet_sum_bv    = excluded.pet_sum_bv,
            nb_pixels     = excluded.nb_pixels
    ''', (mesure_id, code_sta, mesure_date,
          round(temp_moy, 3)   if temp_moy   is not None else None,
          round(precip_sum, 3) if precip_sum is not None else None,
          round(pet_sum, 3)    if pet_sum    is not None else None,
          nb_pixels))
    conn.commit()


def get_era5_bv_jour(code_sta, db_path="./data/insitu_data.db"):
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query('''
        SELECT mesure_date as date, temp_moy_bv, precip_sum_bv, pet_sum_bv, nb_pixels
        FROM era5_bv_jour
        WHERE code_sta = ?
        ORDER BY mesure_date
    ''', conn, params=(code_sta,))
    conn.close()
    if df.empty:
        print(f"⚠️  {code_sta} — pas de données ERA5-BV-jour")
        return None
    df['date'] = pd.to_datetime(df['date'])
    print(f"✅ {len(df)} lignes ERA5-BV-jour pour {code_sta}")
    return df


def creer_table_roe(conn):
    """Crée la table roe_obstacles dans la BDD."""
    conn.execute('''
        CREATE TABLE IF NOT EXISTS roe_obstacles (
            id      INTEGER PRIMARY KEY AUTOINCREMENT,
            roe_id  TEXT,
            nom     TEXT,
            type    TEXT,
            lon     REAL NOT NULL,
            lat     REAL NOT NULL
        )
    ''')
    conn.execute("CREATE INDEX IF NOT EXISTS idx_roe_lat ON roe_obstacles(lat)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_roe_lon ON roe_obstacles(lon)")
    conn.commit()


def inserer_roe(conn, roe_id, nom, type_ouvrage, lon, lat):
    """Insère un obstacle ROE dans la table."""
    conn.execute('''
        INSERT INTO roe_obstacles (roe_id, nom, type, lon, lat)
        VALUES (?, ?, ?, ?, ?)
    ''', (roe_id, nom, type_ouvrage, lon, lat))


def get_roe_count(conn):
    """Retourne le nombre d'obstacles dans la table."""
    return conn.execute("SELECT COUNT(*) FROM roe_obstacles").fetchone()[0]



def ajouter_colonne_dist_barrage(conn):
    """Ajoute la colonne dist_barrage_m dans stations_insitu si elle n'existe pas."""
    try:
        conn.execute("ALTER TABLE stations_insitu ADD COLUMN dist_barrage_m INTEGER")
        conn.commit()
        print("✅ Colonne dist_barrage_m ajoutée")
    except Exception:
        pass  # colonne déjà existante


def mettre_a_jour_distances_barrages(conn):
    """
    Calcule la distance de chaque station au barrage ROE le plus proche
    et met à jour la colonne dist_barrage_m dans stations_insitu.
    """
    import numpy as np
    import pandas as pd

    df_stations = pd.read_sql(
        "SELECT code_sta, lon, lat FROM stations_insitu WHERE lon IS NOT NULL AND lat IS NOT NULL",
        conn
    )
    df_roe = pd.read_sql("SELECT lon, lat FROM roe_obstacles", conn)

    R       = 6_371_000.0
    roe_lat = np.radians(df_roe['lat'].values)
    roe_lon = np.radians(df_roe['lon'].values)

    updates = []
    for _, sta in df_stations.iterrows():
        lat1 = np.radians(sta['lat'])
        lon1 = np.radians(sta['lon'])
        dlat = roe_lat - lat1
        dlon = roe_lon - lon1
        a    = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(roe_lat) * np.sin(dlon/2)**2
        dist = R * 2 * np.arcsin(np.sqrt(np.clip(a, 0, 1)))
        updates.append((int(dist.min()), sta['code_sta']))

    conn.executemany(
        "UPDATE stations_insitu SET dist_barrage_m = ? WHERE code_sta = ?",
        updates
    )
    conn.commit()
    print(f"✅ {len(updates)} stations mises à jour avec dist_barrage_m")


def ajouter_colonne_strahler(conn):
    """
    Ajoute la colonne strahler dans stations_insitu si elle n'existe pas déjà.
    Le try/except est nécessaire car SQLite ne permet pas de vérifier
    l'existence d'une colonne avant de l'ajouter.
    """
    try:
        conn.execute("ALTER TABLE stations_insitu ADD COLUMN strahler INTEGER")
        conn.commit()
        print("✅ Colonne strahler ajoutée")
    except Exception:
        pass  # colonne déjà existante, on continue silencieusement


def mettre_a_jour_strahler(conn, updates):
    """
    Met à jour l'ordre de Strahler pour une liste de stations.

    Args:
        conn: Connexion SQLite
        updates: liste de tuples (strahler, code_sta)
                 ex: [(7, 'O568501002'), (4, 'O123456789'), ...]
    """
    conn.executemany(
        "UPDATE stations_insitu SET strahler = ? WHERE code_sta = ?",
        updates
    )
    conn.commit()
    print(f"✅ {len(updates)} stations mises à jour avec strahler")


def get_strahler_stats(conn):
    """
    Affiche la distribution des ordres de Strahler dans la BDD —
    utile pour vérifier que les valeurs sont cohérentes.
    """
    import pandas as pd
    df = pd.read_sql("""
        SELECT strahler, COUNT(*) as nb_stations
        FROM stations_insitu
        WHERE strahler IS NOT NULL
        GROUP BY strahler
        ORDER BY strahler
    """, conn)
    print("\nDistribution Strahler :")
    print(df.to_string(index=False))
    return df

"""
Fonctions à ajouter dans db_insitu.py
═══════════════════════════════════════════════════════════════════════════
"""


def ajouter_colonnes_elevation_slope(conn):
    """
    Ajoute les colonnes elevation_mean, elevation_std, slope_mean, slope_std
    dans stations_insitu si elles n'existent pas déjà.
    """
    for col in ['elevation_mean', 'elevation_std', 'slope_mean', 'slope_std']:
        try:
            conn.execute(f"ALTER TABLE stations_insitu ADD COLUMN {col} REAL")
            print(f"✅ Colonne {col} ajoutée")
        except Exception:
            pass  # déjà existante
    conn.commit()


def get_bv_a_traiter_elevation(conn):
    """
    Récupère les stations dont les polygones BV sont disponibles
    et qui n'ont pas encore d'elevation/slope calculés.

    Returns:
        list de tuples (code_sta, polygone_wkt)
    """
    cursor = conn.execute('''
        SELECT b.code_sta, b.polygone_wkt
        FROM bv_data b
        JOIN stations_insitu s ON b.code_sta = s.code_sta
        WHERE b.polygone_wkt IS NOT NULL
          AND (s.elevation_mean IS NULL OR s.slope_mean IS NULL)
    ''')
    return cursor.fetchall()


def mettre_a_jour_elevation_slope(conn, code_sta, elev_mean, elev_std, slope_mean, slope_std):
    """
    Met à jour elevation et slope pour une station donnée.
    """
    conn.execute('''
        UPDATE stations_insitu
        SET elevation_mean = ?, elevation_std = ?,
            slope_mean = ?, slope_std = ?
        WHERE code_sta = ?
    ''', (
        round(float(elev_mean), 2)  if elev_mean  is not None else None,
        round(float(elev_std), 2)   if elev_std   is not None else None,
        round(float(slope_mean), 3) if slope_mean is not None else None,
        round(float(slope_std), 3)  if slope_std  is not None else None,
        code_sta
    ))


def get_elevation_slope_stats(conn):
    """
    Affiche la distribution d'elevation et slope pour vérification.
    """
    import pandas as pd
    df = pd.read_sql('''
        SELECT elevation_mean, slope_mean
        FROM stations_insitu
        WHERE elevation_mean IS NOT NULL
    ''', conn)

    if df.empty:
        print("⚠️  Aucune station avec elevation/slope calculés")
        return df

    print(f"\nElevation (m) : médiane={df['elevation_mean'].median():.0f}, "
          f"min={df['elevation_mean'].min():.0f}, max={df['elevation_mean'].max():.0f}")
    print(f"Slope (%)     : médiane={df['slope_mean'].median():.2f}, "
          f"min={df['slope_mean'].min():.2f}, max={df['slope_mean'].max():.2f}")
    print(f"Total stations : {len(df)}")
    return df

"""
Fonctions à ajouter dans db_insitu.py
═══════════════════════════════════════════════════════════════════════════
Pour la table era5_snow_bv_jour : snow_depth + snowmelt agrégés par BV
═══════════════════════════════════════════════════════════════════════════
"""


def creer_table_era5_snow_bv_jour(conn):
    """
    Crée la table era5_snow_bv_jour pour stocker snow_depth et snowmelt
    moyennés sur le bassin versant pour chaque mesure insitu.
    """
    conn.execute('''
        CREATE TABLE IF NOT EXISTS era5_snow_bv_jour (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            mesure_id       INTEGER NOT NULL,
            code_sta        TEXT NOT NULL,
            mesure_date     DATE NOT NULL,
            snow_depth_bv   DECIMAL(8,4),
            snowmelt_bv     DECIMAL(8,4),
            nb_pixels       INTEGER,
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(mesure_id),
            FOREIGN KEY (mesure_id) REFERENCES mesures_insitu(id),
            FOREIGN KEY (code_sta)  REFERENCES stations_insitu(code_sta)
        )
    ''')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_snow_bv_sta_date '
                 'ON era5_snow_bv_jour(code_sta, mesure_date)')
    conn.commit()


def inserer_era5_snow_bv_jour(conn, mesure_id, code_sta, mesure_date,
                               snow_depth, snowmelt, nb_pixels):
    """Insère une ligne snow agrégée par BV (avec ON CONFLICT pour update)."""
    conn.execute('''
        INSERT INTO era5_snow_bv_jour (
            mesure_id, code_sta, mesure_date,
            snow_depth_bv, snowmelt_bv, nb_pixels
        ) VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(mesure_id) DO UPDATE SET
            snow_depth_bv = excluded.snow_depth_bv,
            snowmelt_bv   = excluded.snowmelt_bv,
            nb_pixels     = excluded.nb_pixels
    ''', (mesure_id, code_sta, mesure_date,
          round(snow_depth, 4) if snow_depth is not None else None,
          round(snowmelt, 4)   if snowmelt   is not None else None,
          nb_pixels))


def get_era5_snow_bv_jour(code_sta, db_path="./data/insitu_data.db"):
    """Récupère les données snow par BV pour une station."""
    import sqlite3
    import pandas as pd

    conn = sqlite3.connect(db_path)
    df = pd.read_sql('''
        SELECT mesure_date as date, snow_depth_bv, snowmelt_bv, nb_pixels
        FROM era5_snow_bv_jour
        WHERE code_sta = ?
        ORDER BY mesure_date
    ''', conn, params=(code_sta,))
    conn.close()

    if df.empty:
        print(f"⚠️  {code_sta} — pas de données snow BV")
        return None
    df['date'] = pd.to_datetime(df['date'])
    print(f"✅ {len(df)} lignes snow BV pour {code_sta}")
    return df

"""
Fonctions à ajouter dans db_insitu.py
═══════════════════════════════════════════════════════════════════════════
Pour flagger les stations avec un problème de capteur (spike, série plate, etc.)
═══════════════════════════════════════════════════════════════════════════
"""


def ajouter_colonne_flag_capteur(conn):
    """
    Ajoute une colonne flag_capteur dans stations_insitu si elle n'existe pas.
    Stocke la raison du flag (TEXT) ou NULL si pas de problème.

    Valeurs possibles :
      - 'spike'        : un ou plusieurs pics isolés dans une série plate
      - 'plat'         : signal quasi-constant
      - 'hors_periode' : peu de données dans la période d'intérêt
      - autre raison libre
    """
    try:
        conn.execute("ALTER TABLE stations_insitu ADD COLUMN flag_capteur TEXT")
        conn.commit()
        print("✅ Colonne flag_capteur ajoutée")
    except Exception:
        pass  # déjà existante


def flagger_stations_capteur(conn, codes, raison='spike'):
    """
    Flagge une liste de stations avec une raison donnée.

    Args:
        conn   : connexion SQLite
        codes  : liste de codes_sta à flagger
        raison : motif du flag (ex: 'spike', 'plat', 'hors_periode')
    """
    updates = [(raison, code) for code in codes]
    conn.executemany(
        "UPDATE stations_insitu SET flag_capteur = ? WHERE code_sta = ?",
        updates
    )
    conn.commit()
    print(f"✅ {len(updates)} stations flaggées avec '{raison}'")


def get_stations_non_flaggees(conn):
    """Retourne la liste des codes_sta sans flag_capteur."""
    import pandas as pd
    df = pd.read_sql(
        "SELECT code_sta FROM stations_insitu WHERE flag_capteur IS NULL",
        conn
    )
    return df['code_sta'].tolist()


def get_stations_flaggees(conn):
    """Retourne le DataFrame des stations flaggées avec leur raison."""
    import pandas as pd
    return pd.read_sql(
        "SELECT code_sta, flag_capteur FROM stations_insitu "
        "WHERE flag_capteur IS NOT NULL "
        "ORDER BY flag_capteur, code_sta",
        conn
    )


def reset_flag_capteur(conn, code_sta=None):
    """
    Remet le flag à NULL pour une station ou pour toutes.

    Args:
        code_sta : code_sta spécifique, ou None pour tout réinitialiser
    """
    if code_sta:
        conn.execute("UPDATE stations_insitu SET flag_capteur = NULL "
                     "WHERE code_sta = ?", (code_sta,))
        print(f"✅ Flag retiré pour {code_sta}")
    else:
        conn.execute("UPDATE stations_insitu SET flag_capteur = NULL")
        print(f"✅ Tous les flags retirés")
    conn.commit()

"""
Fonctions à ajouter dans db_insitu.py
═══════════════════════════════════════════════════════════════════════════
Table climatologie_wl : moyenne et std du water_level normalisé par DOY
═══════════════════════════════════════════════════════════════════════════
"""


def creer_table_climatologie_wl(conn):
    """
    Crée la table climatologie_wl : 365 lignes par station,
    contenant la moyenne et std interannuelle du water_level normalisé
    pour chaque jour de l'année (DOY 1-365).
    """
    conn.execute('''
        CREATE TABLE IF NOT EXISTS climatologie_wl (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            code_sta    TEXT NOT NULL,
            doy         INTEGER NOT NULL,
            wl_mean     DECIMAL(8,5),
            wl_std      DECIMAL(8,5),
            n_years     INTEGER,
            created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(code_sta, doy),
            FOREIGN KEY (code_sta) REFERENCES stations_insitu(code_sta)
        )
    ''')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_clim_wl_sta_doy '
                 'ON climatologie_wl(code_sta, doy)')
    conn.commit()


def inserer_climatologie_wl(conn, code_sta, doy, wl_mean, wl_std, n_years):
    """Insère ou met à jour une ligne de climatologie pour un DOY donné."""
    conn.execute('''
        INSERT INTO climatologie_wl (code_sta, doy, wl_mean, wl_std, n_years)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(code_sta, doy) DO UPDATE SET
            wl_mean  = excluded.wl_mean,
            wl_std   = excluded.wl_std,
            n_years  = excluded.n_years
    ''', (code_sta, doy,
          round(wl_mean, 5) if wl_mean is not None else None,
          round(wl_std, 5)  if wl_std  is not None else None,
          n_years))


def inserer_climatologie_wl_batch(conn, rows):
    """
    Insertion batch de la climatologie pour une station.

    Args:
        rows: liste de tuples (code_sta, doy, wl_mean, wl_std, n_years)
    """
    conn.executemany('''
        INSERT INTO climatologie_wl (code_sta, doy, wl_mean, wl_std, n_years)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(code_sta, doy) DO UPDATE SET
            wl_mean  = excluded.wl_mean,
            wl_std   = excluded.wl_std,
            n_years  = excluded.n_years
    ''', rows)
    conn.commit()


def get_climatologie_wl(conn, code_sta):
    """
    Récupère la climatologie (365 lignes) pour une station.

    Returns:
        DataFrame avec colonnes [doy, wl_mean, wl_std, n_years] ou None
    """
    import pandas as pd
    df = pd.read_sql_query('''
        SELECT doy, wl_mean, wl_std, n_years
        FROM climatologie_wl
        WHERE code_sta = ?
        ORDER BY doy
    ''', conn, params=(code_sta,))
    if df.empty:
        return None
    return df


def supprimer_climatologie_wl(conn, code_sta=None):
    """
    Supprime la climatologie d'une station ou de toutes.
    """
    if code_sta:
        conn.execute("DELETE FROM climatologie_wl WHERE code_sta = ?", (code_sta,))
        print(f"✅ Climatologie supprimée pour {code_sta}")
    else:
        conn.execute("DELETE FROM climatologie_wl")
        print("✅ Toute la table climatologie_wl vidée")
    conn.commit()