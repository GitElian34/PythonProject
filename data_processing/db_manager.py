#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Gestionnaire de base de données pour les stations hydrologiques
"""

import sqlite3


def create_climate_table(cursor):
    """
    Crée la table climate_data pour stocker les données météo associées aux mesures

    Args:
        conn: Connexion SQLite
    """

    cursor.execute('''
CREATE TABLE IF NOT EXISTS climate_data (
    climate_id INTEGER PRIMARY KEY AUTOINCREMENT,
    measurement_id INTEGER NOT NULL,
    station_code TEXT NOT NULL,
    measure_date DATE NOT NULL,
    temp_min_jour DECIMAL(5,2),
    temp_max_jour DECIMAL(5,2),
    temp_moy_jour DECIMAL(5,2),
    precip_jour DECIMAL(8,2),
    temp_moy_10j DECIMAL(5,2),
    precip_moy_10j DECIMAL(8,2),
    date_debut_10j DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (measurement_id) REFERENCES measurements(measurement_id),
    FOREIGN KEY (station_code) REFERENCES stations(station_code)
)
''')






def create_tables(conn):
    """

    Crée les tables stations et measurements si elles n'existent pas

    Args:
        conn: Connexion SQLite
    """
    cursor = conn.cursor()
    create_climate_table(cursor)
    # Table stations
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS stations (
        station_id INTEGER PRIMARY KEY AUTOINCREMENT,
        station_code TEXT UNIQUE NOT NULL,
        hydroweb_name TEXT,
        basin_name TEXT NOT NULL,
        river_name TEXT NOT NULL,
        reference_longitude DECIMAL(10,6),
        reference_latitude DECIMAL(10,6),
        reference_distance_km DECIMAL(8,2),
        width_approx_m INTEGER,
        upstream_watershed_km2 DECIMAL(10,2),
        mean_altitude DECIMAL(8,2),
        mean_slope_mm_per_km DECIMAL(8,2),
        geoid_ondulation DECIMAL(8,2),
        tributary_of TEXT,
        reference_ellipsoid TEXT,
        geoid_model TEXT,
        mission_track TEXT,
        status TEXT,
        validation_criteria TEXT,
        product_version TEXT,
        product_citation TEXT,
        first_date DATE,
        last_date DATE,
        nb_measurements INTEGER,
        production_date DATE,
        rating_curve_A DECIMAL(10,4),
        rating_curve_b DECIMAL(10,4),
        rating_curve_Zo DECIMAL(10,4),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    # Table measurements
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS measurements (
        measurement_id INTEGER PRIMARY KEY AUTOINCREMENT,
        station_code TEXT NOT NULL,
        measure_date DATE NOT NULL,
        measure_time TIME,
        orthometric_height DECIMAL(8,2),
        uncertainty DECIMAL(8,2),
        longitude DECIMAL(10,6),
        latitude DECIMAL(10,6),
        ellipsoidal_height DECIMAL(8,2),
        geoidal_ondulation DECIMAL(8,2),
        distance_to_ref_km DECIMAL(8,2),
        satellite TEXT,
        orbit_mission TEXT,
        track_number INTEGER,
        cycle_number INTEGER,
        retracking_algorithm TEXT,
        gdr_version TEXT,
        is_valid BOOLEAN DEFAULT TRUE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (station_code) REFERENCES stations(station_code)
    )
    ''')

    # Index pour accélérer les requêtes
    cursor.execute('''
    CREATE INDEX IF NOT EXISTS idx_measurements_station_date 
    ON measurements(station_code, measure_date)
    ''')
    cursor.execute('''
    CREATE INDEX IF NOT EXISTS idx_measurements_date 
    ON measurements(measure_date)
    ''')
    cursor.execute('''
    CREATE INDEX IF NOT EXISTS idx_stations_basin 
    ON stations(basin_name)
    ''')
    cursor.execute('''
    CREATE INDEX IF NOT EXISTS idx_stations_river 
    ON stations(river_name)
    ''')

    conn.commit()


def insert_station(conn, metadata, measurements):
    """
    Insère une station et ses mesures dans la base

    Args:
        conn: Connexion SQLite
        metadata (dict): Métadonnées de la station
        measurements (list): Liste des mesures

    Returns:
        bool: True si succès, False sinon
    """
    cursor = conn.cursor()

    # Extraire le code station (ID)
    station_code = metadata.get('ID')
    if not station_code:
        print("❌ Erreur: Pas d'ID de station trouvé")
        return False

    # Vérifier si la station existe déjà
    cursor.execute('SELECT station_id FROM stations WHERE station_code = ?', (station_code,))
    if cursor.fetchone():
        print(f"⚠️  Station {station_code} existe déjà, mise à jour ignorée")
        return False

    # Extraire les paramètres de courbe de tarage (3 valeurs)
    rating_params = metadata.get('RATING CURVE PARAMETERS A,b,Zo such that Q(m3/s) = A[H(m)-Zo]^b', 'NA NA NA')
    rating_parts = rating_params.split()
    rating_A = rating_parts[0] if len(rating_parts) > 0 and rating_parts[0] != 'NA' else None
    rating_b = rating_parts[1] if len(rating_parts) > 1 and rating_parts[1] != 'NA' else None
    rating_Zo = rating_parts[2] if len(rating_parts) > 2 and rating_parts[2] != 'NA' else None

    # Insertion station
    cursor.execute('''
    INSERT INTO stations (
        station_code, basin_name, river_name,
        reference_longitude, reference_latitude, reference_distance_km,
        width_approx_m, upstream_watershed_km2, mean_altitude,
        mean_slope_mm_per_km, geoid_ondulation, tributary_of,
        reference_ellipsoid, geoid_model, mission_track,
        status, validation_criteria, product_version,
        product_citation, first_date, last_date,
        nb_measurements, production_date,
        rating_curve_A, rating_curve_b, rating_curve_Zo
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        station_code,
        metadata.get('BASIN'),
        metadata.get('RIVER'),
        metadata.get('REFERENCE LONGITUDE'),
        metadata.get('REFERENCE LATITUDE'),
        metadata.get('REFERENCE DISTANCE (km)'),
        metadata.get('APPROX. WIDTH OF REACH (m)'),
        metadata.get('SURFACE OF UPSTREAM WATERSHED (km2)'),
        metadata.get('MEAN ALTITUDE(M.mm)'),
        metadata.get('MEAN SLOPE (mm/km)'),
        metadata.get('GEOID ONDULATION AT REF POSITION(M.mm)'),
        metadata.get('TRIBUTARY OF'),
        metadata.get('REFERENCE ELLIPSOID'),
        metadata.get('GEOID MODEL'),
        metadata.get('MISSION(S)-TRACK(S)'),
        metadata.get('STATUS'),
        metadata.get('VALIDATION CRITERIA'),
        metadata.get('PRODUCT VERSION'),
        metadata.get('PRODUCT CITATION'),
        metadata.get('FIRST DATE IN DATASET'),
        metadata.get('LAST DATE IN DATASET'),
        metadata.get('NUMBER OF MEASUREMENTS IN DATASET'),
        metadata.get('PRODUCTION DATE'),
        rating_A, rating_b, rating_Zo
    ))

    # Insertion des mesures
    for m in measurements:
        cursor.execute('''
        INSERT INTO measurements (
            station_code, measure_date, measure_time,
            orthometric_height, uncertainty,
            longitude, latitude, ellipsoidal_height,
            geoidal_ondulation, distance_to_ref_km,
            satellite, orbit_mission, track_number,
            cycle_number, retracking_algorithm, gdr_version,
            is_valid
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            station_code,
            m['date'],
            m['time'],
            m['height'],
            m['uncertainty'],
            m['longitude'],
            m['latitude'],
            m['ellipsoidal_height'],
            m['geoidal_ondulation'],
            m['distance_to_ref'],
            m['satellite'],
            m['orbit_mission'],
            m['track_number'],
            m['cycle_number'],
            m['retracking_algo'],
            m['gdr_version'],
            m['is_valid']
        ))

    conn.commit()
    print(f"✅ Station {station_code} importée avec {len(measurements)} mesures")
    return True


def insert_climate_data(conn, station_code, measure_date, climate_dict, date_debut_10j):
    """
    Insère les données climatiques pour une mesure spécifique (sans doublons)

    Args:
        conn: Connexion SQLite
        station_code (str): Code de la station
        measure_date (str): Date de la mesure (YYYY-MM-DD)
        climate_dict (dict): Dictionnaire avec les valeurs climatiques
        date_debut_10j (str): Date de début de la période 10j
        date_fin_10j (str): Date de fin de la période 10j
    """
    cursor = conn.cursor()

    # 1. D'abord, récupérer le measurement_id correspondant
    cursor.execute('''
                   SELECT measurement_id
                   FROM measurements
                   WHERE station_code = ?
                     AND measure_date = ?
                   ORDER BY measurement_id LIMIT 1
                   ''', (station_code, measure_date))

    result = cursor.fetchone()
    if not result:
        print(f"❌ Aucune mesure trouvée pour {station_code} le {measure_date}")
        return False

    measurement_id = result[0]

    # 2. Vérifier si une entrée existe déjà pour ce measurement_id
    cursor.execute('''
                   SELECT climate_id
                   FROM climate_data
                   WHERE measurement_id = ?
                   ''', (measurement_id,))

    existing = cursor.fetchone()

    if existing:
        print(
            f"⚠️  Données déjà existantes pour {station_code} le {measure_date} (ID: {existing[0]}) - Mise à jour ignorée")
        return False

    # 3. Insérer les données climatiques (pas de doublon)
    cursor.execute('''
                   INSERT INTO climate_data (measurement_id,
                                             station_code,
                                             measure_date,
                                             temp_min_jour,
                                             temp_max_jour,
                                             temp_moy_jour,
                                             precip_jour,
                                             temp_moy_10j,
                                             precip_moy_10j,
                                             date_debut_10j)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ''', (
                       measurement_id,
                       station_code,
                       measure_date,
                       float(climate_dict['temp_min_jour']),
                       float(climate_dict['temp_max_jour']),
                       float(climate_dict['temp_moy_jour']),
                       float(climate_dict['precip_jour']),
                       float(climate_dict['temp_moy10j']),
                       float(climate_dict['precip_moy10j']),
                       date_debut_10j
                   ))

    conn.commit()
    print(f"✅ Données climatiques insérées pour {station_code} le {measure_date}")
    return True
def get_stations_by_basin_river(conn, basin_name=None, river_name=None):
    """
    Récupère les IDs des stations filtrées par bassin et/ou rivière

    Args:
        conn: Connexion SQLite
        basin_name (str, optional): Nom du bassin
        river_name (str, optional): Nom de la rivière

    Returns:
        list: Liste des station_code (IDs) des stations correspondantes
    """
    cursor = conn.cursor()

    query = "SELECT station_code FROM stations WHERE 1=1"
    params = []

    if basin_name:
        query += " AND basin_name = ?"
        params.append(basin_name)

    if river_name:
        query += " AND river_name = ?"
        params.append(river_name)

    cursor.execute(query, params)

    # Récupère tous les résultats et extrait le premier élément de chaque tuple
    return [row[0] for row in cursor.fetchall()]

def get_station_coordinates(conn, station_code):
    """
    Récupère la longitude et latitude d'une station

    Args:
        conn: Connexion SQLite
        station_code (str): Code de la station (ex: "O568501002")

    Returns:
        tuple: (longitude, latitude) ou None si station non trouvée
    """
    cursor = conn.cursor()

    cursor.execute(
        'SELECT reference_longitude, reference_latitude, river_name FROM stations WHERE station_code = ?',
        (station_code,)
    )

    result = cursor.fetchone()

    if result is None:
        print(f"❌ Station {station_code} non trouvée")
        return None

    longitude, latitude,river_name = result
    print(f"✅ Coordonnées de {station_code} : lon={longitude}, lat={latitude}")
    return longitude, latitude, river_name
def deduplicate_climate_data(db_path='./data/hydro_data.db'):
    """
    Supprime les doublons dans la table climate_data basés sur measurement_id

    Args:
        db_path (str): Chemin vers la base de données

    Returns:
        dict: Statistiques sur les doublons supprimés
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    print("🔍 Recherche des doublons dans climate_data...")

    # 1. Vérifier combien de doublons
    cursor.execute('''
                   SELECT measurement_id, COUNT(*) as count
                   FROM climate_data
                   GROUP BY measurement_id
                   HAVING COUNT (*) > 1
                   ''')

    doublons = cursor.fetchall()
    nb_doublons_total = sum([c[1] for c in doublons]) - len(doublons)

    print(f"📊 {len(doublons)} measurement_id avec doublons")
    print(f"📊 {nb_doublons_total} enregistrements en double à supprimer")

    if not doublons:
        print("✅ Aucun doublon trouvé !")
        conn.close()
        return {'supprimes': 0, 'doublons': []}

    # 2. Afficher quelques exemples
    print("\n🔍 Exemples de doublons:")
    for meas_id, count in doublons[:5]:  # 5 premiers
        cursor.execute('''
                       SELECT climate_id, measure_date, station_code
                       FROM climate_data
                       WHERE measurement_id = ?
                       ORDER BY climate_id
                       ''', (meas_id,))
        records = cursor.fetchall()
        print(f"  measurement_id {meas_id}: {count} copies")
        for r in records:
            print(f"    - ID {r[0]}, date {r[1]}, station {r[2]}")

    # 3. Supprimer les doublons (garder le plus petit ID)
    print("\n🗑️  Suppression des doublons...")

    cursor.execute('''
                   DELETE
                   FROM climate_data
                   WHERE climate_id NOT IN (SELECT MIN(climate_id)
                                            FROM climate_data
                                            GROUP BY measurement_id)
                   ''')

    nb_supprimes = cursor.rowcount
    conn.commit()

    # 4. Vérification finale
    cursor.execute('SELECT COUNT(*) FROM climate_data')
    total_restant = cursor.fetchone()[0]

    cursor.execute('''
                   SELECT COUNT(*)
                   FROM (SELECT measurement_id
                         FROM climate_data
                         GROUP BY measurement_id)
                   ''')
    total_uniques = cursor.fetchone()[0]

    print(f"\n✅ {nb_supprimes} doublons supprimés")
    print(f"📊 Total restant: {total_restant} enregistrements")
    print(f"📊 Dont {total_uniques} measurement_id uniques")

    conn.close()

    return {
        'supprimes': nb_supprimes,
        'restant': total_restant,
        'uniques': total_uniques
    }

def add_hydroweb_names(conn, shp_path):
    """
    Fait le matching entre la BDD et le shapefile HydroWeb
    pour ajouter le nom lisible (ex: R_GARONNE_GARONNE_KM0084)
    à chaque station.
    """
    import geopandas as gpd
    import pandas as pd

    # Charger le shapefile
    stations_shp = gpd.read_file(shp_path)
    stations_shp['lon_r'] = stations_shp['lon'].round(4)
    stations_shp['lat_r'] = stations_shp['lat'].round(4)

    # Charger les stations de la BDD
    stations_db = pd.read_sql(
        "SELECT station_code, reference_longitude, reference_latitude FROM stations",
        conn
    )
    stations_db['lon_r'] = stations_db['reference_longitude'].round(4)
    stations_db['lat_r'] = stations_db['reference_latitude'].round(4)

    # Matching par coordonnées
    merged = stations_db.merge(
        stations_shp[['name', 'lon_r', 'lat_r']],
        on=['lon_r', 'lat_r'],
        how='left'
    )

    # Ajouter la colonne si elle n'existe pas
    cursor = conn.cursor()
    try:
        cursor.execute("ALTER TABLE stations ADD COLUMN hydroweb_name TEXT")
    except Exception:
        pass  # colonne existe déjà

    # Mettre à jour
    updated = 0
    for _, row in merged.iterrows():
        if pd.notna(row.get('name')):
            cursor.execute(
                "UPDATE stations SET hydroweb_name = ? WHERE station_code = ?",
                (row['name'], row['station_code'])
            )
            updated += 1

    conn.commit()
    print(f"✅ {updated} stations mises à jour avec leur hydroweb_name")
    return updated
def get_climate_data_matrix(station_id, db_path='./data/hydro_data.db'):
    """
    Récupère les données climatiques d'une station sous forme de matrice avec la hauteur orthométrique

    Args:
        station_id (str): Code de la station
        db_path (str): Chemin vers la base de données

    Returns:
        list: Matrice avec [date, temp_min, temp_max, temp_moy, precip, temp_moy10j, precip_moy10j, orthometric_height]
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    query = '''
            SELECT 
                c.measure_date,
                c.temp_min_jour,
                c.temp_max_jour,
                c.temp_moy_jour,
                c.precip_jour,
                c.temp_moy_10j,
                c.precip_moy_10j,
                m.orthometric_height
            FROM climate_data c
            LEFT JOIN measurements m ON c.measurement_id = m.measurement_id
            WHERE c.station_code = ?
            ORDER BY c.measure_date
            '''

    cursor.execute(query, (station_id,))
    data = cursor.fetchall()
    conn.close()

    print(f"✅ {len(data)} enregistrements récupérés pour la station {station_id}")
    return data


def get_station_measurements(conn, station_code):
    """
    Récupère les mesures orthométriques d'une station spécifique

    Args:
        conn: Connexion SQLite
        station_code (str): Code de la station (ex: "O568501002")

    Returns:
        list: Liste de tuples (measure_date, measure_time, orthometric_height)
              ou liste vide si aucun résultat
    """
    cursor = conn.cursor()

    # Requête SQL pour récupérer les données
    query = '''
            SELECT measure_date, \
                   measure_time, \
                   ellipsoidal_height
            FROM measurements
            WHERE station_code = ?
            ORDER BY measure_date, measure_time \
            '''

    cursor.execute(query, (station_code,))
    results = cursor.fetchall()

    print(f"✅ {len(results)} mesures trouvées pour la station {station_code}")

    return results

def get_stats(conn):
    """
    Récupère des statistiques sur la base

    Args:
        conn: Connexion SQLite

    Returns:
        tuple: (nb_stations, nb_measures)
    """
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM stations')
    nb_stations = cursor.fetchone()[0]
    cursor.execute('SELECT COUNT(*) FROM measurements')
    nb_measures = cursor.fetchone()[0]
    return nb_stations, nb_measures


def creer_table_corine(conn):
    """Crée la table bv_corine dans hydro_data.db."""
    conn.execute('''
        CREATE TABLE IF NOT EXISTS bv_corine (
            corine_id         INTEGER PRIMARY KEY AUTOINCREMENT,
            station_code      TEXT UNIQUE NOT NULL,
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
            FOREIGN KEY (station_code) REFERENCES stations(station_code)
        )
    ''')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_corine_station ON bv_corine(station_code)')
    conn.commit()
    print("✅ Table bv_corine créée")


def inserer_corine(conn, station_code, fractions, nb_pixels, soil=None):
    """Insère ou met à jour les fractions CORINE + texture sols."""
    soil = soil or {}
    conn.execute('''
        INSERT INTO bv_corine (
            station_code, frac_urban, frac_agriculture, frac_forest,
            frac_semi_natural, frac_wetland, frac_water, nb_pixels,
            sg_clay_0_30cm, sg_sand_0_30cm, sg_silt_0_30cm
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(station_code) DO UPDATE SET
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
        station_code,
        round(fractions['urban'], 4),
        round(fractions['agriculture'], 4),
        round(fractions['forest'], 4),
        round(fractions['semi_natural'], 4),
        round(fractions['wetland'], 4),
        round(fractions['water'], 4),
        nb_pixels,
        round(soil.get('clay', 0), 2) if soil.get('clay') else None,
        round(soil.get('sand', 0), 2) if soil.get('sand') else None,
        round(soil.get('silt', 0), 2) if soil.get('silt') else None,
    ))
    conn.commit()


def get_corine(conn, station_code):
    """Récupère les fractions CORINE + sols d'une station."""
    import pandas as pd
    df = pd.read_sql_query('''
        SELECT frac_urban, frac_agriculture, frac_forest,
               frac_semi_natural, frac_wetland, frac_water,
               sg_clay_0_30cm, sg_sand_0_30cm, sg_silt_0_30cm
        FROM bv_corine WHERE station_code = ?
    ''', conn, params=(station_code,))
    return df.iloc[0].to_dict() if not df.empty else None


# ═══════════════════════════════════════════════════════════════
# STRAHLER
# ═══════════════════════════════════════════════════════════════

def ajouter_colonne_strahler(conn):
    """Ajoute la colonne strahler dans stations si elle n'existe pas."""
    try:
        conn.execute("ALTER TABLE stations ADD COLUMN strahler INTEGER")
        conn.commit()
        print("✅ Colonne strahler ajoutée dans stations")
    except Exception:
        pass  # colonne déjà existante


def mettre_a_jour_strahler(conn, updates):
    """
    Met à jour l'ordre de Strahler pour une liste de stations.
    updates : liste de tuples (strahler, station_code)
    """
    conn.executemany(
        "UPDATE stations SET strahler = ? WHERE station_code = ?",
        updates
    )
    conn.commit()
    print(f"✅ {len(updates)} stations mises à jour avec strahler")


# ═══════════════════════════════════════════════════════════════
# DISTANCE BARRAGES
# ═══════════════════════════════════════════════════════════════

def ajouter_colonne_dist_barrage(conn):
    """Ajoute la colonne dist_barrage_m dans stations si elle n'existe pas."""
    try:
        conn.execute("ALTER TABLE stations ADD COLUMN dist_barrage_m INTEGER")
        conn.commit()
        print("✅ Colonne dist_barrage_m ajoutée dans stations")
    except Exception:
        pass  # colonne déjà existante


def mettre_a_jour_distances_barrages(conn, roe_conn):
    """
    Calcule la distance de chaque station satellite au barrage ROE
    le plus proche et met à jour dist_barrage_m dans stations.
    roe_conn : connexion à insitu_data.db qui contient roe_obstacles
    """
    import numpy as np
    import pandas as pd

    df_stations = pd.read_sql(
        """SELECT station_code,
                  reference_longitude AS lon,
                  reference_latitude  AS lat
           FROM stations
           WHERE reference_longitude IS NOT NULL""",
        conn
    )
    df_roe = pd.read_sql("SELECT lon, lat FROM roe_obstacles", roe_conn)

    R = 6_371_000.0
    roe_lat = np.radians(df_roe['lat'].values)
    roe_lon = np.radians(df_roe['lon'].values)

    updates = []
    for _, sta in df_stations.iterrows():
        lat1 = np.radians(sta['lat'])
        lon1 = np.radians(sta['lon'])
        dlat = roe_lat - lat1
        dlon = roe_lon - lon1
        a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(roe_lat) * np.sin(dlon / 2) ** 2
        dist = R * 2 * np.arcsin(np.sqrt(np.clip(a, 0, 1)))
        updates.append((int(dist.min()), sta['station_code']))

    conn.executemany(
        "UPDATE stations SET dist_barrage_m = ? WHERE station_code = ?",
        updates
    )
    conn.commit()
    print(f"✅ {len(updates)} stations mises à jour avec dist_barrage_m")


# ═══════════════════════════════════════════════════════════════
# ERA5 QUOTIDIEN SUR BV
# ═══════════════════════════════════════════════════════════════

def creer_table_era5_bv_jour(conn):
    """Crée la table era5_bv_jour pour ERA5 moyenné sur le BV."""
    conn.execute('''
        CREATE TABLE IF NOT EXISTS era5_bv_jour (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            measurement_id INTEGER NOT NULL,
            station_code   TEXT NOT NULL,
            mesure_date    DATE NOT NULL,
            temp_moy_bv    DECIMAL(6,3),
            precip_sum_bv  DECIMAL(8,3),
            pet_sum_bv     DECIMAL(8,3),
            nb_pixels      INTEGER,
            created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(measurement_id),
            FOREIGN KEY (measurement_id) REFERENCES measurements(measurement_id),
            FOREIGN KEY (station_code)   REFERENCES stations(station_code)
        )
    ''')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_era5_bv_jour_sat ON era5_bv_jour(station_code, mesure_date)')
    conn.commit()
    print("✅ Table era5_bv_jour créée")


def inserer_era5_bv_jour(conn, measurement_id, station_code, mesure_date,
                         temp_moy, precip_sum, pet_sum, nb_pixels):
    """Insère ou met à jour ERA5 quotidien sur BV pour une mesure."""
    conn.execute('''
        INSERT INTO era5_bv_jour (
            measurement_id, station_code, mesure_date,
            temp_moy_bv, precip_sum_bv, pet_sum_bv, nb_pixels
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(measurement_id) DO UPDATE SET
            temp_moy_bv   = excluded.temp_moy_bv,
            precip_sum_bv = excluded.precip_sum_bv,
            pet_sum_bv    = excluded.pet_sum_bv,
            nb_pixels     = excluded.nb_pixels
    ''', (measurement_id, station_code, mesure_date,
          round(temp_moy, 3) if temp_moy is not None else None,
          round(precip_sum, 3) if precip_sum is not None else None,
          round(pet_sum, 3) if pet_sum is not None else None,
          nb_pixels))
    conn.commit()


def get_era5_bv_jour(conn, station_code):
    """Récupère ERA5 quotidien sur BV pour une station."""
    import pandas as pd
    df = pd.read_sql_query('''
        SELECT mesure_date AS date, temp_moy_bv, precip_sum_bv, pet_sum_bv
        FROM era5_bv_jour
        WHERE station_code = ?
        ORDER BY mesure_date
    ''', conn, params=(station_code,))
    if df.empty:
        print(f"⚠️  {station_code} — pas de données ERA5-BV-jour")
        return None
    return df

def creer_table_era5_j0_j10(conn):
    """Crée la table era5_j0_j10 — ERA5 de J0 à J-10 pour chaque mesure satellite."""
    conn.execute('''
        CREATE TABLE IF NOT EXISTS era5_j0_j10 (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            measurement_id INTEGER NOT NULL,
            station_code   TEXT NOT NULL,
            measure_date   DATE NOT NULL,
            j_offset       INTEGER NOT NULL,  -- 0=J0, -1=J-1, ..., -10=J-10
            temp_moy_bv    DECIMAL(6,3),
            precip_sum_bv  DECIMAL(8,3),
            pet_sum_bv     DECIMAL(8,3),
            nb_pixels      INTEGER,
            created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(measurement_id, j_offset),
            FOREIGN KEY (measurement_id) REFERENCES measurements(measurement_id),
            FOREIGN KEY (station_code)   REFERENCES stations(station_code)
        )
    ''')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_era5_j0j10_sta ON era5_j0_j10(station_code, measure_date)')
    conn.commit()
    print("✅ Table era5_j0_j10 créée")

"""
Fonctions à ajouter dans db_hydro.py
═══════════════════════════════════════════════════════════════════════════
Pour les 222 stations satellite dans hydro_data.db
═══════════════════════════════════════════════════════════════════════════
"""


def ajouter_colonnes_elevation_slope_satellite(conn):
    """
    Ajoute les colonnes elevation_mean, elevation_std, slope_mean, slope_std
    dans la table stations si elles n'existent pas déjà.
    """
    for col in ['elevation_mean', 'elevation_std', 'slope_mean', 'slope_std']:
        try:
            conn.execute(f"ALTER TABLE stations ADD COLUMN {col} REAL")
            print(f"✅ Colonne {col} ajoutée")
        except Exception:
            pass  # déjà existante
    conn.commit()


def get_bv_satellite_a_traiter_elevation(conn):
    """
    Récupère les stations satellite dont les polygones BV sont disponibles
    et qui n'ont pas encore d'elevation/slope calculés.

    Returns:
        list de tuples (station_code, polygone_wkt)
    """
    cursor = conn.execute('''
        SELECT b.station_code, b.polygone_wkt
        FROM bv_data b
        JOIN stations s ON b.station_code = s.station_code
        WHERE b.polygone_wkt IS NOT NULL
          AND (s.elevation_mean IS NULL OR s.slope_mean IS NULL)
    ''')
    return cursor.fetchall()


def mettre_a_jour_elevation_slope_satellite(conn, station_code, elev_mean, elev_std, slope_mean, slope_std):
    """
    Met à jour elevation et slope pour une station satellite.
    """
    conn.execute('''
        UPDATE stations
        SET elevation_mean = ?, elevation_std = ?,
            slope_mean = ?, slope_std = ?
        WHERE station_code = ?
    ''', (
        round(float(elev_mean), 2)  if elev_mean  is not None else None,
        round(float(elev_std), 2)   if elev_std   is not None else None,
        round(float(slope_mean), 3) if slope_mean is not None else None,
        round(float(slope_std), 3)  if slope_std  is not None else None,
        station_code
    ))


def get_elevation_slope_stats_satellite(conn):
    """
    Affiche la distribution d'elevation et slope pour les stations satellite.
    """
    import pandas as pd
    df = pd.read_sql('''
        SELECT elevation_mean, slope_mean
        FROM stations
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
Fonctions à ajouter dans db_manager.py (ou db_hydro.py)
═══════════════════════════════════════════════════════════════════════════
Pour la table era5_bv_jour : une ligne par jour de la période 2016-2025
                              avec ERA5 (P/T/PET) agrégé sur le BV satellite.
═══════════════════════════════════════════════════════════════════════════
"""


def creer_table_era5_bv_jour(conn):
    """
    Crée la table era5_bv_jour pour stocker ERA5 quotidien agrégé sur les BV.
    Une ligne par (station_code, date) — pas lié à une mesure satellite.
    """
    conn.execute('''
        CREATE TABLE IF NOT EXISTS era5_bv_jour (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            station_code    TEXT NOT NULL,
            mesure_date     DATE NOT NULL,
            temp_moy_bv     DECIMAL(8,3),
            precip_sum_bv   DECIMAL(8,3),
            pet_sum_bv      DECIMAL(8,3),
            nb_pixels       INTEGER,
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(station_code, mesure_date),
            FOREIGN KEY (station_code) REFERENCES stations(station_code)
        )
    ''')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_era5_bv_jour_sta_date '
                 'ON era5_bv_jour(station_code, mesure_date)')
    conn.commit()


def get_era5_bv_jour(station_code, db_path="./data/hydro_data.db"):
    """Récupère la série ERA5 quotidienne pour une station satellite."""
    import sqlite3
    import pandas as pd

    conn = sqlite3.connect(db_path)
    df = pd.read_sql('''
        SELECT mesure_date as date, temp_moy_bv, precip_sum_bv, pet_sum_bv
        FROM era5_bv_jour
        WHERE station_code = ?
        ORDER BY mesure_date
    ''', conn, params=(station_code,))
    conn.close()

    if df.empty:
        print(f"⚠️  {station_code} — pas de données ERA5 quotidiennes")
        return None
    df['date'] = pd.to_datetime(df['date'])
    return df