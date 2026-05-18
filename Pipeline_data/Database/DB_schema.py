#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
═══════════════════════════════════════════════════════════════════════════
db_schema.py — Schéma de la BDD pipeline
═══════════════════════════════════════════════════════════════════════════

Crée les tables stations, measurements et measure_attributes.
Ne fait aucune insertion de données.

Usage direct (pour créer une BDD vide) :
    python db_schema.py                          → crée ./data/test.db
    python db_schema.py --db ./data/autre.db     → chemin custom
    python db_schema.py --reset                  → supprime et recrée
═══════════════════════════════════════════════════════════════════════════
"""

import argparse
import logging
import sqlite3
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("db_schema")

DEFAULT_DB_PATH = Path("./data/test.db")


def create_database(db_path: Path = DEFAULT_DB_PATH, reset: bool = False) -> sqlite3.Connection:
    """
    Crée (ou recrée) la BDD avec toutes les tables du pipeline.

    Args:
        db_path: Chemin vers le fichier .db
        reset: Si True, supprime la BDD existante et repart de zéro

    Returns:
        Connexion SQLite ouverte
    """
    if reset and db_path.exists():
        db_path.unlink()
        log.info(f"BDD supprimée : {db_path}")

    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")

    _create_tables(conn)

    log.info(f"BDD prête : {db_path}")
    return conn


def _create_tables(conn: sqlite3.Connection):
    """Crée toutes les tables et index."""

    conn.executescript("""

        -- ═══════════════════════════════════════════
        -- TABLE STATIONS
        -- ═══════════════════════════════════════════
        CREATE TABLE IF NOT EXISTS stations (
            station_id              INTEGER PRIMARY KEY AUTOINCREMENT,
            station_code            TEXT UNIQUE NOT NULL,
            hydroweb_name           TEXT,
            basin_name              TEXT,
            river_name              TEXT,
            reference_longitude     REAL,
            reference_latitude      REAL,
            reference_distance_km   REAL,
            width_approx_m          INTEGER,
            upstream_watershed_km2  REAL,
            mean_altitude           REAL,
            mean_slope_mm_per_km    REAL,
            geoid_ondulation        REAL,
            tributary_of            TEXT,
            reference_ellipsoid     TEXT,
            geoid_model             TEXT,
            mission_track           TEXT,
            status                  TEXT,
            validation_criteria     TEXT,
            product_version         TEXT,
            product_citation        TEXT,
            first_date              DATE,
            last_date               DATE,
            nb_measurements         INTEGER,
            production_date         DATE,

            -- Attributs de surface (Corine / SoilGrids)
            frac_urban              REAL,
            frac_forest             REAL,
            frac_agriculture        REAL,
            sg_clay_0_30cm          REAL,
            sg_sand_0_30cm          REAL,
            sg_silt_0_30cm          REAL,

            -- Attributs topographiques (remplis par étapes suivantes)
            strahler                INTEGER,
            dist_barrage_m          INTEGER,
            elevation_mean          REAL,
            elevation_std           REAL,
            slope_mean              REAL,
            slope_std               REAL,
            flag_capteur            INTEGER,

            created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- ═══════════════════════════════════════════
        -- TABLE MEASUREMENTS (mesures de niveau d'eau)
        -- ═══════════════════════════════════════════
        CREATE TABLE IF NOT EXISTS measurements (
            measurement_id          INTEGER PRIMARY KEY AUTOINCREMENT,
            station_code            TEXT NOT NULL,
            measure_date            DATE NOT NULL,
            measure_time            TIME,
            orthometric_height      REAL,
            uncertainty             REAL,
            longitude               REAL,
            latitude                REAL,
            ellipsoidal_height      REAL,
            geoidal_ondulation      REAL,
            distance_to_ref_km      REAL,
            satellite               TEXT,
            orbit_mission           TEXT,
            track_number            INTEGER,
            cycle_number            INTEGER,
            retracking_algorithm    TEXT,
            gdr_version             TEXT,
            is_valid                BOOLEAN DEFAULT TRUE,
            created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (station_code) REFERENCES stations(station_code)
        );

        -- ═══════════════════════════════════════════
        -- TABLE MEASURE_ATTRIBUTES (variables météo/clim par mesure)
        -- Remplie par les étapes suivantes de la pipeline
        -- ═══════════════════════════════════════════
        CREATE TABLE IF NOT EXISTS measure_attributes (
            attribute_id            INTEGER PRIMARY KEY AUTOINCREMENT,
            measurement_id          INTEGER NOT NULL UNIQUE,
            station_code            TEXT NOT NULL,
            measure_date            DATE NOT NULL,

            -- Météo J0
            precipitation_J0        REAL,
            temperature_J0          REAL,
            pet_J0                  REAL,

            -- Moyennes glissantes J3
            precip_mean_J3          REAL,
            pet_mean_J3             REAL,
            temp_mean_J3            REAL,

            -- Moyennes glissantes J10 / J27
            precip_mean_J10         REAL,
            temp_mean_J10           REAL,
            precip_mean_J27         REAL,

            -- Climatologie fenêtrée
            clim_mean_20j           REAL,
            clim_std_20j            REAL,

            -- Précipitation fine
            precip_max_J27          REAL,
            precip_last7            REAL,

            created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

            FOREIGN KEY (measurement_id) REFERENCES measurements(measurement_id),
            FOREIGN KEY (station_code)   REFERENCES stations(station_code)
        );

        -- ═══════════════════════════════════════════
        -- TABLE BV_DATA (bassins versants délimités)
        -- ═══════════════════════════════════════════
        CREATE TABLE IF NOT EXISTS bv_data (
            bv_id           INTEGER PRIMARY KEY AUTOINCREMENT,
            station_code    TEXT UNIQUE NOT NULL,
            hydroweb_name   TEXT,
            aire_km2        REAL,
            polygone_wkt    TEXT,
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (station_code) REFERENCES stations(station_code)
        );

        -- ═══════════════════════════════════════════
        -- TABLE ROE_OBSTACLES (barrages)
        -- ═══════════════════════════════════════════
        CREATE TABLE IF NOT EXISTS roe_obstacles (
            roe_id      TEXT PRIMARY KEY,
            nom         TEXT,
            type        TEXT,
            lon         REAL NOT NULL,
            lat         REAL NOT NULL,
            created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- ═══════════════════════════════════════════
        -- TABLE ERA5_TRANSFERT (pixels ERA5 par BV)
        -- ═══════════════════════════════════════════
        CREATE TABLE IF NOT EXISTS era5_transfert (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            station_code    TEXT NOT NULL,
            pixel_lon       REAL NOT NULL,
            pixel_lat       REAL NOT NULL,
            UNIQUE(station_code, pixel_lon, pixel_lat),
            FOREIGN KEY (station_code) REFERENCES stations(station_code)
        );

        -- ═══════════════════════════════════════════
        -- TABLE ERA5_BV_JOUR (ERA5 quotidien agrégé sur BV)
        -- ═══════════════════════════════════════════
        CREATE TABLE IF NOT EXISTS era5_bv_jour (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            station_code    TEXT NOT NULL,
            date            DATE NOT NULL,
            temp_moy_bv     REAL,
            precip_sum_bv   REAL,
            pet_sum_bv      REAL,
            snow_depth_bv   REAL,
            snowmelt_bv     REAL,
            nb_pixels       INTEGER,
            UNIQUE(station_code, date),
            FOREIGN KEY (station_code) REFERENCES stations(station_code)
        );

        -- ═══════════════════════════════════════════
        -- INDEX
        -- ═══════════════════════════════════════════
        CREATE INDEX IF NOT EXISTS idx_meas_station_date
            ON measurements(station_code, measure_date);
        CREATE INDEX IF NOT EXISTS idx_meas_date
            ON measurements(measure_date);
        CREATE INDEX IF NOT EXISTS idx_stations_basin
            ON stations(basin_name);
        CREATE INDEX IF NOT EXISTS idx_stations_river
            ON stations(river_name);
        CREATE INDEX IF NOT EXISTS idx_attrs_station_date
            ON measure_attributes(station_code, measure_date);
        CREATE INDEX IF NOT EXISTS idx_attrs_measurement
            ON measure_attributes(measurement_id);
        CREATE INDEX IF NOT EXISTS idx_bv_station
            ON bv_data(station_code);
        CREATE INDEX IF NOT EXISTS idx_roe_coords
            ON roe_obstacles(lon, lat);
        CREATE INDEX IF NOT EXISTS idx_era5_trans_station
            ON era5_transfert(station_code);
        CREATE INDEX IF NOT EXISTS idx_era5_bv_jour_sta_date
            ON era5_bv_jour(station_code, date);
    """)

    conn.commit()


def get_table_info(conn: sqlite3.Connection) -> dict:
    """Retourne le nombre de lignes par table."""
    tables = ["stations", "measurements", "measure_attributes", "bv_data", "roe_obstacles", "era5_transfert", "era5_bv_jour"]
    info = {}
    for t in tables:
        try:
            n = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            info[t] = n
        except Exception:
            info[t] = 0
    return info


# ═══════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Créer la BDD pipeline (tables vides)")
    parser.add_argument("--db", type=str, default=str(DEFAULT_DB_PATH), help="Chemin BDD")
    parser.add_argument("--reset", action="store_true", help="Supprimer et recréer")
    args = parser.parse_args()

    conn = create_database(Path(args.db), reset=args.reset)
    info = get_table_info(conn)
    for table, n in info.items():
        log.info(f"  {table}: {n} lignes")
    conn.close()