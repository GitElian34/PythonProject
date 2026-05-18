#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
═══════════════════════════════════════════════════════════════════════════
db_operations.py — Fonctions CRUD pour la BDD pipeline
═══════════════════════════════════════════════════════════════════════════

Insertion, suppression, modification et lecture pour les tables
stations, measurements et measure_attributes.

Aucune création de table ici → voir db_schema.py
═══════════════════════════════════════════════════════════════════════════
"""

import logging
import sqlite3
from pathlib import Path

log = logging.getLogger("db_ops")


# ═══════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════

def _to_float(val) -> float | None:
    if val is None or val in ("NA", "9999.999", ""):
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _to_int(val) -> int | None:
    if val is None or val in ("NA", ""):
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


# ═══════════════════════════════════════════════════════════════════════
# STATIONS — INSERT
# ═══════════════════════════════════════════════════════════════════════

def insert_station(conn: sqlite3.Connection, metadata: dict) -> bool:
    """
    Insère une station dans la table stations à partir des métadonnées HydroWeb.

    Args:
        conn: Connexion SQLite
        metadata: Dict des métadonnées (clé = champ HydroWeb, valeur = str ou None)

    Returns:
        True si insertion réussie, False si station existe déjà ou erreur
    """
    station_code = metadata.get("ID")
    if not station_code:
        log.error("Pas d'ID trouvé dans les métadonnées")
        return False

    # Vérifier doublon
    existing = conn.execute(
        "SELECT 1 FROM stations WHERE station_code = ?", (station_code,)
    ).fetchone()
    if existing:
        log.warning(f"{station_code} existe déjà → ignorée")
        return False

    conn.execute("""
        INSERT INTO stations (
            station_code, basin_name, river_name,
            reference_longitude, reference_latitude, reference_distance_km,
            width_approx_m, upstream_watershed_km2, mean_altitude,
            mean_slope_mm_per_km, geoid_ondulation, tributary_of,
            reference_ellipsoid, geoid_model, mission_track,
            status, validation_criteria, product_version,
            product_citation, first_date, last_date,
            nb_measurements, production_date
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        station_code,
        metadata.get("BASIN"),
        metadata.get("RIVER"),
        _to_float(metadata.get("REFERENCE LONGITUDE")),
        _to_float(metadata.get("REFERENCE LATITUDE")),
        _to_float(metadata.get("REFERENCE DISTANCE (km)")),
        _to_int(metadata.get("APPROX. WIDTH OF REACH (m)")),
        _to_float(metadata.get("SURFACE OF UPSTREAM WATERSHED (km2)")),
        _to_float(metadata.get("MEAN ALTITUDE(M.mm)")),
        _to_float(metadata.get("MEAN SLOPE (mm/km)")),
        _to_float(metadata.get("GEOID ONDULATION AT REF POSITION(M.mm)")),
        metadata.get("TRIBUTARY OF"),
        metadata.get("REFERENCE ELLIPSOID"),
        metadata.get("GEOID MODEL"),
        metadata.get("MISSION(S)-TRACK(S)"),
        metadata.get("STATUS"),
        metadata.get("VALIDATION CRITERIA"),
        metadata.get("PRODUCT VERSION"),
        metadata.get("PRODUCT CITATION"),
        metadata.get("FIRST DATE IN DATASET"),
        metadata.get("LAST DATE IN DATASET"),
        _to_int(metadata.get("NUMBER OF MEASUREMENTS IN DATASET")),
        metadata.get("PRODUCTION DATE"),
    ))

    conn.commit()
    log.info(f"Station {station_code} insérée")
    return True


# ═══════════════════════════════════════════════════════════════════════
# STATIONS — UPDATE
# ═══════════════════════════════════════════════════════════════════════

def update_station_field(conn: sqlite3.Connection, station_code: str,
                         field: str, value) -> bool:
    """Met à jour un champ unique d'une station."""
    allowed = {
        "hydroweb_name", "basin_name", "river_name",
        "frac_urban", "frac_forest", "frac_agriculture",
        "sg_clay_0_30cm", "sg_sand_0_30cm", "sg_silt_0_30cm",
        "strahler", "dist_barrage_m",
        "elevation_mean", "elevation_std", "slope_mean", "slope_std",
        "flag_capteur", "upstream_watershed_km2",
    }
    if field not in allowed:
        log.error(f"Champ '{field}' non autorisé pour update")
        return False

    conn.execute(
        f"UPDATE stations SET {field} = ? WHERE station_code = ?",
        (value, station_code)
    )
    conn.commit()
    return True


def update_station_fields(conn: sqlite3.Connection, station_code: str,
                          fields: dict) -> bool:
    """Met à jour plusieurs champs d'une station d'un coup."""
    for field, value in fields.items():
        if not update_station_field(conn, station_code, field, value):
            return False
    return True


# ═══════════════════════════════════════════════════════════════════════
# STATIONS — DELETE
# ═══════════════════════════════════════════════════════════════════════

def delete_station(conn: sqlite3.Connection, station_code: str) -> bool:
    """
    Supprime une station et toutes ses données associées
    (measurements + measure_attributes en cascade).
    """
    # Supprimer measure_attributes liés
    conn.execute(
        "DELETE FROM measure_attributes WHERE station_code = ?",
        (station_code,)
    )
    # Supprimer measurements
    conn.execute(
        "DELETE FROM measurements WHERE station_code = ?",
        (station_code,)
    )
    # Supprimer la station
    cursor = conn.execute(
        "DELETE FROM stations WHERE station_code = ?",
        (station_code,)
    )
    conn.commit()

    if cursor.rowcount > 0:
        log.info(f"Station {station_code} supprimée (+ mesures + attributs)")
        return True
    else:
        log.warning(f"Station {station_code} non trouvée")
        return False


# ═══════════════════════════════════════════════════════════════════════
# STATIONS — GET
# ═══════════════════════════════════════════════════════════════════════

def get_station(conn: sqlite3.Connection, station_code: str) -> dict | None:
    """Récupère toutes les infos d'une station."""
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT * FROM stations WHERE station_code = ?", (station_code,)
    ).fetchone()
    conn.row_factory = None
    return dict(row) if row else None


def get_station_coordinates(conn: sqlite3.Connection, station_code: str) -> tuple | None:
    """Retourne (lon, lat) d'une station, ou None."""
    row = conn.execute(
        "SELECT reference_longitude, reference_latitude FROM stations WHERE station_code = ?",
        (station_code,)
    ).fetchone()
    return (row[0], row[1]) if row else None


def get_all_station_codes(conn: sqlite3.Connection) -> list[str]:
    """Retourne la liste de tous les station_code."""
    rows = conn.execute("SELECT station_code FROM stations ORDER BY station_code").fetchall()
    return [r[0] for r in rows]


def get_stations_by_basin(conn: sqlite3.Connection, basin_name: str) -> list[str]:
    """Retourne les station_code d'un bassin."""
    rows = conn.execute(
        "SELECT station_code FROM stations WHERE basin_name = ?", (basin_name,)
    ).fetchall()
    return [r[0] for r in rows]


def get_stations_by_river(conn: sqlite3.Connection, river_name: str) -> list[str]:
    """Retourne les station_code d'une rivière."""
    rows = conn.execute(
        "SELECT station_code FROM stations WHERE river_name = ?", (river_name,)
    ).fetchall()
    return [r[0] for r in rows]


# ═══════════════════════════════════════════════════════════════════════
# MEASUREMENTS — INSERT
# ═══════════════════════════════════════════════════════════════════════

def insert_measurements(conn: sqlite3.Connection, station_code: str,
                        measurements: list[dict]) -> int:
    """
    Insère une liste de mesures pour une station (batch).

    Args:
        conn: Connexion SQLite
        station_code: Code de la station
        measurements: Liste de dicts avec les champs de mesure

    Returns:
        Nombre de mesures insérées
    """
    if not measurements:
        return 0

    conn.executemany("""
        INSERT INTO measurements (
            station_code, measure_date, measure_time,
            orthometric_height, uncertainty,
            longitude, latitude, ellipsoidal_height,
            geoidal_ondulation, distance_to_ref_km,
            satellite, orbit_mission, track_number,
            cycle_number, retracking_algorithm, gdr_version,
            is_valid
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, [
        (
            station_code,
            m["date"], m["time"],
            m["height"], m["uncertainty"],
            m["longitude"], m["latitude"], m["ellipsoidal_height"],
            m["geoidal_ondulation"], m["distance_to_ref"],
            m["satellite"], m["orbit_mission"], m["track_number"],
            m["cycle_number"], m["retracking_algo"], m["gdr_version"],
            m["is_valid"],
        )
        for m in measurements
    ])

    conn.commit()
    log.info(f"  {station_code} — {len(measurements)} mesures insérées")
    return len(measurements)


# ═══════════════════════════════════════════════════════════════════════
# MEASUREMENTS — GET
# ═══════════════════════════════════════════════════════════════════════

def get_measurements(conn: sqlite3.Connection, station_code: str) -> list[tuple]:
    """Récupère toutes les mesures d'une station (date, time, height)."""
    return conn.execute("""
        SELECT measure_date, measure_time, orthometric_height
        FROM measurements
        WHERE station_code = ?
        ORDER BY measure_date
    """, (station_code,)).fetchall()


def get_measurement_id(conn: sqlite3.Connection, station_code: str,
                       measure_date: str) -> int | None:
    """Récupère le measurement_id pour une station + date."""
    row = conn.execute("""
        SELECT measurement_id FROM measurements
        WHERE station_code = ? AND measure_date = ?
        ORDER BY measurement_id LIMIT 1
    """, (station_code, measure_date)).fetchone()
    return row[0] if row else None


def get_measurement_dates(conn: sqlite3.Connection, station_code: str) -> list[str]:
    """Retourne la liste des dates de mesure d'une station."""
    rows = conn.execute("""
        SELECT DISTINCT measure_date FROM measurements
        WHERE station_code = ?
        ORDER BY measure_date
    """, (station_code,)).fetchall()
    return [r[0] for r in rows]


# ═══════════════════════════════════════════════════════════════════════
# MEASUREMENTS — DELETE
# ═══════════════════════════════════════════════════════════════════════

def delete_measurements(conn: sqlite3.Connection, station_code: str) -> int:
    """Supprime toutes les mesures d'une station. Retourne le nb supprimé."""
    # D'abord les attributs liés
    conn.execute(
        "DELETE FROM measure_attributes WHERE station_code = ?",
        (station_code,)
    )
    cursor = conn.execute(
        "DELETE FROM measurements WHERE station_code = ?",
        (station_code,)
    )
    conn.commit()
    return cursor.rowcount


# ═══════════════════════════════════════════════════════════════════════
# MEASURE_ATTRIBUTES — INSERT
# ═══════════════════════════════════════════════════════════════════════

def insert_measure_attributes(conn: sqlite3.Connection, measurement_id: int,
                              station_code: str, measure_date: str,
                              attrs: dict) -> bool:
    """
    Insère les attributs météo/clim pour une mesure.

    Args:
        conn: Connexion SQLite
        measurement_id: ID de la mesure
        station_code: Code station
        measure_date: Date de la mesure
        attrs: Dict avec les noms de colonnes comme clés
               Ex: {"precipitation_J0": 5.2, "temperature_J0": 15.3, ...}

    Returns:
        True si insertion réussie
    """
    # Vérifier doublon
    existing = conn.execute(
        "SELECT 1 FROM measure_attributes WHERE measurement_id = ?",
        (measurement_id,)
    ).fetchone()
    if existing:
        log.warning(f"Attributs déjà existants pour measurement_id={measurement_id}")
        return False

    columns = [
        "precipitation_J0", "temperature_J0", "pet_J0",
        "precip_mean_J3", "pet_mean_J3", "temp_mean_J3",
        "precip_mean_J10", "temp_mean_J10", "precip_mean_J27",
        "clim_mean_20j", "clim_std_20j",
        "precip_max_J27", "precip_last7",
    ]

    values = [attrs.get(col) for col in columns]

    placeholders = ", ".join(["?"] * (3 + len(columns)))
    col_names = ", ".join(columns)

    conn.execute(f"""
        INSERT INTO measure_attributes (
            measurement_id, station_code, measure_date,
            {col_names}
        ) VALUES ({placeholders})
    """, [measurement_id, station_code, measure_date] + values)

    conn.commit()
    return True


def insert_measure_attributes_batch(conn: sqlite3.Connection,
                                    rows: list[dict]) -> int:
    """
    Insertion batch des attributs pour plusieurs mesures.

    Args:
        rows: Liste de dicts avec au minimum
              {"measurement_id", "station_code", "measure_date", ...variables...}

    Returns:
        Nombre de lignes insérées
    """
    columns = [
        "precipitation_J0", "temperature_J0", "pet_J0",
        "precip_mean_J3", "pet_mean_J3", "temp_mean_J3",
        "precip_mean_J10", "temp_mean_J10", "precip_mean_J27",
        "clim_mean_20j", "clim_std_20j",
        "precip_max_J27", "precip_last7",
    ]

    col_names = ", ".join(columns)
    placeholders = ", ".join(["?"] * (3 + len(columns)))

    data = []
    for row in rows:
        values = [row["measurement_id"], row["station_code"], row["measure_date"]]
        values += [row.get(col) for col in columns]
        data.append(values)

    conn.executemany(f"""
        INSERT OR IGNORE INTO measure_attributes (
            measurement_id, station_code, measure_date,
            {col_names}
        ) VALUES ({placeholders})
    """, data)

    conn.commit()
    inserted = len(data)
    log.info(f"  {inserted} attributs insérés")
    return inserted


# ═══════════════════════════════════════════════════════════════════════
# MEASURE_ATTRIBUTES — UPDATE
# ═══════════════════════════════════════════════════════════════════════

def update_measure_attribute(conn: sqlite3.Connection, measurement_id: int,
                             field: str, value) -> bool:
    """Met à jour un champ d'attribut pour une mesure."""
    allowed = {
        "precipitation_J0", "temperature_J0", "pet_J0",
        "precip_mean_J3", "pet_mean_J3", "temp_mean_J3",
        "precip_mean_J10", "temp_mean_J10", "precip_mean_J27",
        "clim_mean_20j", "clim_std_20j",
        "precip_max_J27", "precip_last7",
    }
    if field not in allowed:
        log.error(f"Champ '{field}' non autorisé")
        return False

    conn.execute(
        f"UPDATE measure_attributes SET {field} = ? WHERE measurement_id = ?",
        (value, measurement_id)
    )
    conn.commit()
    return True


# ═══════════════════════════════════════════════════════════════════════
# MEASURE_ATTRIBUTES — GET
# ═══════════════════════════════════════════════════════════════════════

def get_measure_attributes(conn: sqlite3.Connection,
                           station_code: str) -> list[dict]:
    """Récupère tous les attributs d'une station."""
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT * FROM measure_attributes
        WHERE station_code = ?
        ORDER BY measure_date
    """, (station_code,)).fetchall()
    conn.row_factory = None
    return [dict(r) for r in rows]


# ═══════════════════════════════════════════════════════════════════════
# BV_DATA — INSERT / GET
# ═══════════════════════════════════════════════════════════════════════

def insert_bv(conn: sqlite3.Connection, station_code: str,
              hydroweb_name: str | None, aire_km2: float,
              polygone_wkt: str) -> bool:
    """Insère ou met à jour le BV d'une station."""
    conn.execute("""
        INSERT INTO bv_data (station_code, hydroweb_name, aire_km2, polygone_wkt)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(station_code) DO UPDATE SET
            hydroweb_name = excluded.hydroweb_name,
            aire_km2      = excluded.aire_km2,
            polygone_wkt  = excluded.polygone_wkt
    """, (station_code, hydroweb_name, aire_km2, polygone_wkt))
    conn.commit()
    return True


def get_bv(conn: sqlite3.Connection, station_code: str) -> dict | None:
    """Récupère le BV d'une station."""
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT * FROM bv_data WHERE station_code = ?", (station_code,)
    ).fetchone()
    conn.row_factory = None
    return dict(row) if row else None


def get_stations_without_bv(conn: sqlite3.Connection) -> list[dict]:
    """Retourne les stations qui n'ont pas encore de BV calculé."""
    rows = conn.execute("""
        SELECT s.station_code, s.hydroweb_name,
               s.reference_longitude AS lon,
               s.reference_latitude AS lat
        FROM stations s
        LEFT JOIN bv_data b ON s.station_code = b.station_code
        WHERE s.reference_longitude IS NOT NULL
          AND s.reference_latitude IS NOT NULL
          AND b.station_code IS NULL
    """).fetchall()
    return [{"station_code": r[0], "hydroweb_name": r[1],
             "lon": r[2], "lat": r[3]} for r in rows]


def get_stations_with_bv(conn: sqlite3.Connection) -> list[dict]:
    """Retourne les stations avec BV (pour les étapes suivantes)."""
    rows = conn.execute("""
        SELECT b.station_code, b.polygone_wkt, b.aire_km2,
               s.river_name, s.reference_longitude AS lon,
               s.reference_latitude AS lat
        FROM bv_data b
        JOIN stations s ON b.station_code = s.station_code
        WHERE b.polygone_wkt IS NOT NULL
    """).fetchall()
    return [{"station_code": r[0], "polygone_wkt": r[1], "aire_km2": r[2],
             "river_name": r[3], "lon": r[4], "lat": r[5]} for r in rows]


def get_stations_without_field(conn: sqlite3.Connection, field: str) -> list[dict]:
    """Retourne les stations avec BV mais sans un champ donné (strahler, elevation, etc.)."""
    allowed = {"strahler", "elevation_mean", "slope_mean", "dist_barrage_m",
               "frac_urban", "sg_clay_0_30cm"}
    if field not in allowed:
        log.error(f"Champ '{field}' non autorisé")
        return []
    rows = conn.execute(f"""
        SELECT s.station_code, s.reference_longitude AS lon,
               s.reference_latitude AS lat
        FROM stations s
        JOIN bv_data b ON s.station_code = b.station_code
        WHERE s.{field} IS NULL
    """).fetchall()
    return [{"station_code": r[0], "lon": r[1], "lat": r[2]} for r in rows]


# ═══════════════════════════════════════════════════════════════════════
# ROE_OBSTACLES — INSERT / GET
# ═══════════════════════════════════════════════════════════════════════

def insert_roe_batch(conn: sqlite3.Connection,
                     obstacles: list[tuple]) -> int:
    """
    Insère les barrages ROE en batch.
    obstacles : liste de tuples (roe_id, nom, type, lon, lat)
    """
    conn.executemany("""
        INSERT OR IGNORE INTO roe_obstacles (roe_id, nom, type, lon, lat)
        VALUES (?, ?, ?, ?, ?)
    """, obstacles)
    conn.commit()
    n = conn.execute("SELECT COUNT(*) FROM roe_obstacles").fetchone()[0]
    log.info(f"ROE : {n} barrages en BDD")
    return n


def get_roe_count(conn: sqlite3.Connection) -> int:
    """Retourne le nombre de barrages en BDD."""
    return conn.execute("SELECT COUNT(*) FROM roe_obstacles").fetchone()[0]


def get_roe_coordinates(conn: sqlite3.Connection):
    """Retourne les coordonnées de tous les barrages (lon, lat) en arrays numpy."""
    import numpy as np
    rows = conn.execute("SELECT lon, lat FROM roe_obstacles").fetchall()
    if not rows:
        return np.array([]), np.array([])
    lons = np.array([r[0] for r in rows])
    lats = np.array([r[1] for r in rows])
    return lons, lats


# ═══════════════════════════════════════════════════════════════════════
# UPDATE BATCH — pour les étapes 2b-2e
# ═══════════════════════════════════════════════════════════════════════

def update_strahler_batch(conn: sqlite3.Connection,
                          updates: list[tuple]) -> int:
    """updates : liste de (strahler, station_code)."""
    conn.executemany(
        "UPDATE stations SET strahler = ? WHERE station_code = ?",
        updates
    )
    conn.commit()
    log.info(f"Strahler : {len(updates)} stations mises à jour")
    return len(updates)


def update_elevation_slope(conn: sqlite3.Connection, station_code: str,
                           elev_mean, elev_std, slope_mean, slope_std):
    """Met à jour elevation et slope pour une station."""
    conn.execute("""
        UPDATE stations
        SET elevation_mean = ?, elevation_std = ?,
            slope_mean = ?, slope_std = ?
        WHERE station_code = ?
    """, (
        round(float(elev_mean), 2) if elev_mean is not None else None,
        round(float(elev_std), 2) if elev_std is not None else None,
        round(float(slope_mean), 3) if slope_mean is not None else None,
        round(float(slope_std), 3) if slope_std is not None else None,
        station_code,
    ))


def update_corine_soilgrids(conn: sqlite3.Connection, station_code: str,
                            fractions: dict, soil: dict):
    """Met à jour Corine + SoilGrids dans stations."""
    conn.execute("""
        UPDATE stations
        SET frac_urban = ?, frac_forest = ?, frac_agriculture = ?,
            sg_clay_0_30cm = ?, sg_sand_0_30cm = ?, sg_silt_0_30cm = ?
        WHERE station_code = ?
    """, (
        round(fractions.get("urban", 0), 4),
        round(fractions.get("forest", 0), 4),
        round(fractions.get("agriculture", 0), 4),
        round(soil.get("clay", 0), 2) if soil.get("clay") else None,
        round(soil.get("sand", 0), 2) if soil.get("sand") else None,
        round(soil.get("silt", 0), 2) if soil.get("silt") else None,
        station_code,
    ))


def update_dist_barrage(conn: sqlite3.Connection, station_code: str,
                        dist_m: int):
    """Met à jour la distance au barrage le plus proche."""
    conn.execute(
        "UPDATE stations SET dist_barrage_m = ? WHERE station_code = ?",
        (dist_m, station_code)
    )


# ═══════════════════════════════════════════════════════════════════════
# ERA5_TRANSFERT — pixels ERA5 par BV
# ═══════════════════════════════════════════════════════════════════════

def insert_era5_pixels(conn: sqlite3.Connection, station_code: str,
                       pixels: list[tuple]) -> int:
    """
    Insère les pixels ERA5 d'un BV.
    pixels : liste de (lon, lat)
    """
    conn.executemany("""
        INSERT OR IGNORE INTO era5_transfert (station_code, pixel_lon, pixel_lat)
        VALUES (?, ?, ?)
    """, [(station_code, lon, lat) for lon, lat in pixels])
    conn.commit()
    return len(pixels)


def get_era5_pixels(conn: sqlite3.Connection, station_code: str) -> list[tuple]:
    """Retourne les pixels ERA5 d'une station [(lon, lat), ...]."""
    rows = conn.execute(
        "SELECT pixel_lon, pixel_lat FROM era5_transfert WHERE station_code = ?",
        (station_code,)
    ).fetchall()
    return [(r[0], r[1]) for r in rows]


def get_stations_without_era5_pixels(conn: sqlite3.Connection) -> list[str]:
    """Retourne les station_code avec BV mais sans pixels ERA5."""
    rows = conn.execute("""
        SELECT b.station_code
        FROM bv_data b
        LEFT JOIN era5_transfert e ON b.station_code = e.station_code
        WHERE b.polygone_wkt IS NOT NULL
          AND e.station_code IS NULL
    """).fetchall()
    return [r[0] for r in rows]


# ═══════════════════════════════════════════════════════════════════════
# ERA5_BV_JOUR — ERA5 quotidien agrégé sur BV
# ═══════════════════════════════════════════════════════════════════════

def insert_era5_bv_jour_batch(conn: sqlite3.Connection,
                              rows: list[tuple]) -> int:
    """
    Insertion batch dans era5_bv_jour.
    rows : liste de (station_code, date, temp, precip, pet, snow_depth, snowmelt, nb_pixels)
    """
    conn.executemany("""
        INSERT OR IGNORE INTO era5_bv_jour
        (station_code, date, temp_moy_bv, precip_sum_bv, pet_sum_bv,
         snow_depth_bv, snowmelt_bv, nb_pixels)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)
    conn.commit()
    return len(rows)


def update_era5_snow(conn: sqlite3.Connection, station_code: str,
                     date: str, snow_depth: float, snowmelt: float):
    """Met à jour les colonnes neige d'une ligne era5_bv_jour existante."""
    conn.execute("""
        UPDATE era5_bv_jour
        SET snow_depth_bv = ?, snowmelt_bv = ?
        WHERE station_code = ? AND date = ?
    """, (snow_depth, snowmelt, station_code, date))


def get_era5_bv_jour(conn: sqlite3.Connection, station_code: str):
    """Récupère la série ERA5 quotidienne complète d'une station."""
    import pandas as pd
    df = pd.read_sql("""
        SELECT date, temp_moy_bv, precip_sum_bv, pet_sum_bv,
               snow_depth_bv, snowmelt_bv
        FROM era5_bv_jour
        WHERE station_code = ?
        ORDER BY date
    """, conn, params=(station_code,))
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"])
    return df


def get_era5_bv_jour_count(conn: sqlite3.Connection) -> dict:
    """Stats sur era5_bv_jour."""
    total = conn.execute("SELECT COUNT(*) FROM era5_bv_jour").fetchone()[0]
    n_stations = conn.execute(
        "SELECT COUNT(DISTINCT station_code) FROM era5_bv_jour"
    ).fetchone()[0]
    n_snow = conn.execute(
        "SELECT COUNT(*) FROM era5_bv_jour WHERE snow_depth_bv IS NOT NULL"
    ).fetchone()[0]
    return {"total": total, "stations": n_stations, "with_snow": n_snow}


# ═══════════════════════════════════════════════════════════════════════
# STATS / RAPPORT
# ═══════════════════════════════════════════════════════════════════════

def get_stats(conn: sqlite3.Connection) -> dict:
    """Retourne les stats globales de la BDD."""
    nb_stations = conn.execute("SELECT COUNT(*) FROM stations").fetchone()[0]
    nb_measures = conn.execute("SELECT COUNT(*) FROM measurements").fetchone()[0]
    nb_attrs = conn.execute("SELECT COUNT(*) FROM measure_attributes").fetchone()[0]
    return {
        "stations": nb_stations,
        "measurements": nb_measures,
        "measure_attributes": nb_attrs,
    }


def print_report(conn: sqlite3.Connection):
    """Affiche un résumé de la BDD."""
    stats = get_stats(conn)

    # Stats par bassin
    basins = conn.execute("""
        SELECT basin_name, COUNT(*) as n,
               MIN(first_date) as debut, MAX(last_date) as fin
        FROM stations
        GROUP BY basin_name
        ORDER BY n DESC
    """).fetchall()

    # Période globale
    date_range = conn.execute("""
        SELECT MIN(measure_date), MAX(measure_date)
        FROM measurements
    """).fetchone()

    print("\n" + "═" * 60)
    print("  RAPPORT BDD PIPELINE")
    print("═" * 60)
    print(f"  Stations           : {stats['stations']}")
    print(f"  Mesures            : {stats['measurements']}")
    print(f"  Attributs remplis  : {stats['measure_attributes']}")
    if date_range[0]:
        print(f"  Période            : {date_range[0]} → {date_range[1]}")

    if basins:
        print(f"\n  Par bassin :")
        for basin, n, debut, fin in basins:
            print(f"    {basin or '?':<20s} {n:>4d} stations  ({debut} → {fin})")

    # Mesures invalides
    nb_invalid = conn.execute(
        "SELECT COUNT(*) FROM measurements WHERE is_valid = 0"
    ).fetchone()[0]
    if nb_invalid and stats['measurements']:
        pct = 100 * nb_invalid / stats['measurements']
        print(f"\n  Mesures invalides  : {nb_invalid} ({pct:.1f}%)")

    print("═" * 60 + "\n")