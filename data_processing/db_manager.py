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
        'SELECT reference_longitude, reference_latitude FROM stations WHERE station_code = ?',
        (station_code,)
    )

    result = cursor.fetchone()

    if result is None:
        print(f"❌ Station {station_code} non trouvée")
        return None

    longitude, latitude = result
    print(f"✅ Coordonnées de {station_code} : lon={longitude}, lat={latitude}")
    return longitude, latitude
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
                   orthometric_height
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