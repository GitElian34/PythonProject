#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Gestionnaire de base de données pour les stations hydrologiques
"""

import sqlite3

def create_tables(conn):
    """
    Crée les tables stations et measurements si elles n'existent pas

    Args:
        conn: Connexion SQLite
    """
    cursor = conn.cursor()

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