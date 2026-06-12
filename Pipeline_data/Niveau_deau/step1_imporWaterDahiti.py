#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
═══════════════════════════════════════════════════════════════════════════
step1_import_dahiti.py — Étape 1 : Import DAHITI API → BDD (schéma HydroWeb)
═══════════════════════════════════════════════════════════════════════════

Réutilise exactement db_schema.py et db_operations.py de la pipeline
HydroWeb. Les champs non disponibles dans DAHITI sont laissés à NULL.

Mapping DAHITI → HydroWeb :
  station_code        ← str(dahiti_id).zfill(13)
  hydroweb_name       ← target_name
  river_name          ← target_name (partie avant la virgule)
  basin_name          ← target_name (idem, meilleure approximation)
  reference_longitude ← longitude
  reference_latitude  ← latitude
  mission_track       ← mission inférée (J3/S6A ou S3A/S3B)
  first_date          ← min(measure_date)
  last_date           ← max(measure_date)
  nb_measurements     ← COUNT(mesures)
  orthometric_height  ← wse
  uncertainty         ← wse_u
  measure_date        ← datetime[:10]
  measure_time        ← datetime[11:]
  satellite           ← mission inférée
  is_valid            ← True par défaut

Tout le reste (geoid, ellipsoidal_height, track_number, etc.) → NULL

Usage standalone :
    python step1_import_dahiti.py
    python step1_import_dahiti.py --country france --continent Europe
    python step1_import_dahiti.py --db ./data/dahiti.db --reset
    python step1_import_dahiti.py --types River

Usage depuis la pipeline :
    from step1_import_dahiti import run_step1_dahiti
    run_step1_dahiti(country="france", db_path="./data/dahiti.db", reset=True)
═══════════════════════════════════════════════════════════════════════════
"""

import argparse
import json
import logging
import statistics
import time
from datetime import date as dt_date
from pathlib import Path

import requests
import sys

# ── Import pipeline HydroWeb existante ──────────────────────────────────
# Adapter le chemin selon l'arborescence du projet
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from Pipeline_data.Database.DB_schema    import create_database, get_table_info
from Pipeline_data.Database.db_operations import (
    insert_station, insert_measurements, print_report, update_station_field
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("step1_dahiti")

# ═══════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════
API_KEY           = "D0EBD81E7279ACA2C6597A8C5153E8B20013DF650855CB39B19695C8E80BB484"
BASE_URL          = "https://dahiti.dgfi.tum.de/api/v2/"
DEFAULT_DB_PATH   = Path("./data/dahiti.db")
DEFAULT_COUNTRY   = "france"
DEFAULT_CONTINENT = "Europe"
DEFAULT_TYPES     = ["River", "Lake", "Reservoir"]
PAUSE_API         = 0.25
MIN_MEASUREMENTS  = 5


# ═══════════════════════════════════════════════════════════════════════
# HELPERS API
# ═══════════════════════════════════════════════════════════════════════

def _api_post(endpoint: str, args: dict) -> dict:
    args["api_key"] = API_KEY
    try:
        r = requests.post(BASE_URL + endpoint, json=args, timeout=30)
        if r.status_code == 200:
            return json.loads(r.text)
        log.warning(f"  HTTP {r.status_code} — {endpoint} : {r.text[:200]}")
        return {}
    except Exception as e:
        log.error(f"  Erreur API {endpoint} : {e}")
        return {}


def _to_float(val) -> float | None:
    try:
        return float(val) if val is not None else None
    except (ValueError, TypeError):
        return None


# ═══════════════════════════════════════════════════════════════════════
# COLLECTE STATIONS
# ═══════════════════════════════════════════════════════════════════════

def fetch_stations(continent: str, country: str,
                   types: list[str] | None = None) -> list[dict]:
    """Récupère les stations DAHITI filtrées par continent, pays et type."""
    log.info(f"Récupération stations — continent={continent}, country={country}")
    resp     = _api_post("list-targets/", {"continent": continent})
    all_sta  = resp.get("data", [])
    log.info(f"  {len(all_sta)} stations {continent} récupérées")

    stations = [
        s for s in all_sta
        if s.get("country", "").lower() == country.lower()
        and s.get("data_access", {}).get("water_level_altimetry") == "public"
    ]
    log.info(f"  {len(stations)} stations {country} avec altimétrie publique")

    if types:
        stations = [s for s in stations if s.get("type") in types]
        log.info(f"  {len(stations)} après filtre type={types}")

    return stations


def fetch_water_level(dahiti_id: int) -> list[dict]:
    """Télécharge la série WSE d'une station DAHITI."""
    resp = _api_post("download-water-level/", {
        "dahiti_id": dahiti_id,
        "format"   : "json",
    })
    return resp.get("data", [])


# ═══════════════════════════════════════════════════════════════════════
# MAPPING DAHITI → FORMAT HYDROWEB
# ═══════════════════════════════════════════════════════════════════════

def _infer_mission(serie: list[dict]) -> tuple[str | None, float | None]:
    """
    Infère la mission satellite depuis l'intervalle médian entre mesures.
    Retourne (mission_str, median_days).
    """
    if len(serie) < 3:
        return None, None

    dates = sorted([
        dt_date.fromisoformat(m["datetime"][:10])
        for m in serie if m.get("datetime")
    ])
    intervals = [
        (dates[i+1] - dates[i]).days
        for i in range(len(dates) - 1)
        if (dates[i+1] - dates[i]).days > 0
    ]
    if not intervals:
        return None, None

    med = statistics.median(intervals)
    if med <= 15:
        mission = "J3/S6A"
    elif med <= 32:
        mission = "S3A/S3B"
    else:
        mission = f"autre_{med:.0f}j"

    return mission, med


def build_station_metadata(sta: dict, serie: list[dict]) -> dict:
    """
    Construit le dict metadata au format attendu par insert_station()
    (mêmes clés que les métadonnées HydroWeb parsées).

    Champs non disponibles dans DAHITI → None (insert_station les laisse NULL).
    """
    dahiti_id  = sta["dahiti_id"]
    name       = sta.get("target_name", "")
    river_name = name.split(",")[0].strip() if name else None
    mission, _ = _infer_mission(serie)

    dates = sorted([
        m["datetime"][:10] for m in serie if m.get("datetime")
    ])
    first_date = dates[0]  if dates else None
    last_date  = dates[-1] if dates else None

    return {
        # Champs mappés
        "ID"                                     : str(dahiti_id).zfill(13),
        "BASIN"                                  : river_name,   # meilleure approx dispo
        "RIVER"                                  : river_name,
        "REFERENCE LONGITUDE"                    : sta.get("longitude"),
        "REFERENCE LATITUDE"                     : sta.get("latitude"),
        "MISSION(S)-TRACK(S)"                    : mission,
        "STATUS"                                 : "operational",
        "FIRST DATE IN DATASET"                  : first_date,
        "LAST DATE IN DATASET"                   : last_date,
        "NUMBER OF MEASUREMENTS IN DATASET"      : len(serie),
        # Champs HydroWeb sans équivalent DAHITI → None = NULL en BDD
        "REFERENCE DISTANCE (km)"                : None,
        "APPROX. WIDTH OF REACH (m)"             : None,
        "SURFACE OF UPSTREAM WATERSHED (km2)"    : None,
        "MEAN ALTITUDE(M.mm)"                    : None,
        "MEAN SLOPE (mm/km)"                     : None,
        "GEOID ONDULATION AT REF POSITION(M.mm)" : None,
        "TRIBUTARY OF"                           : None,
        "REFERENCE ELLIPSOID"                    : None,
        "GEOID MODEL"                            : None,
        "VALIDATION CRITERIA"                    : None,
        "PRODUCT VERSION"                        : "DAHITI_v2",
        "PRODUCT CITATION"                       : "DAHITI — DGFI-TUM",
        "PRODUCTION DATE"                        : None,
    }


def build_measurements(dahiti_id: int, serie: list[dict],
                       mission: str | None) -> list[dict]:
    """
    Construit la liste de dicts mesures au format attendu par insert_measurements()
    (mêmes clés que les mesures HydroWeb parsées).

    Champs non disponibles dans DAHITI → None.
    """
    rows = []
    for m in serie:
        dt_str = m.get("datetime", "")
        if not dt_str:
            continue
        wse   = _to_float(m.get("wse"))
        wse_u = _to_float(m.get("wse_u"))
        if wse is None:
            continue

        rows.append({
            "date"              : dt_str[:10],
            "time"              : dt_str[11:] if len(dt_str) > 10 else None,
            "height"            : wse,
            "uncertainty"       : wse_u,
            # Champs géo non fournis par DAHITI
            "longitude"         : None,
            "latitude"          : None,
            "ellipsoidal_height": None,
            "geoidal_ondulation": None,
            "distance_to_ref"   : None,
            # Mission inférée → satellite + orbit_mission
            "satellite"         : mission,
            "orbit_mission"     : mission,
            "track_number"      : None,
            "cycle_number"      : None,
            "retracking_algo"   : None,
            "gdr_version"       : None,
            "is_valid"          : True,
        })
    return rows


# ═══════════════════════════════════════════════════════════════════════
# FONCTION PRINCIPALE
# ═══════════════════════════════════════════════════════════════════════

def run_step1_dahiti(
    country   : str       = DEFAULT_COUNTRY,
    continent : str       = DEFAULT_CONTINENT,
    types     : list[str] = DEFAULT_TYPES,
    db_path   : Path      = DEFAULT_DB_PATH,
    reset     : bool      = False,
) -> dict:
    """
    Étape 1 DAHITI : collecte API et insertion en BDD (schéma HydroWeb).

    Args:
        country   : Pays cible en minuscules (ex: "france")
        continent : Continent DAHITI (ex: "Europe")
        types     : Types de corps d'eau à inclure
        db_path   : Chemin vers la BDD SQLite
        reset     : Si True, supprime et recrée la BDD

    Returns:
        Dict {"inserted": n, "skipped": n, "errors": n, "total_measurements": n}
    """
    db_path = Path(db_path)

    # 1. Créer / ouvrir la BDD (schéma HydroWeb existant)
    conn = create_database(db_path, reset=reset)

    # 2. Récupérer les stations depuis l'API
    stations = fetch_stations(continent, country, types)
    if not stations:
        log.warning("Aucune station récupérée")
        conn.close()
        return {"inserted": 0, "skipped": 0, "errors": 0, "total_measurements": 0}

    # 3. Boucle station par station
    inserted = skipped = errors = total_meas = 0

    for i, sta in enumerate(stations):
        dahiti_id = sta.get("dahiti_id")
        name      = sta.get("target_name", f"ID_{dahiti_id}")
        typ       = sta.get("type", "?")

        try:
            # Télécharger la série
            time.sleep(PAUSE_API)
            serie = fetch_water_level(dahiti_id)

            if len(serie) < MIN_MEASUREMENTS:
                log.info(f"  [{i+1:3d}/{len(stations)}] {name} — "
                         f"trop peu de mesures ({len(serie)}) → ignoré")
                skipped += 1
                continue

            # Inférer la mission
            mission, med_interval = _infer_mission(serie)

            # Construire les dicts au format HydroWeb
            metadata     = build_station_metadata(sta, serie)
            measurements = build_measurements(dahiti_id, serie, mission)

            if not measurements:
                skipped += 1
                continue

            # Insérer via les fonctions HydroWeb existantes
            ok = insert_station(conn, metadata)
            if not ok:
                # Station déjà présente → on continue quand même les mesures
                pass

            nb = insert_measurements(conn, metadata["ID"], measurements)

            # Stocker le target_name dans hydroweb_name (champ non mappé par insert_station)
            update_station_field(conn, metadata["ID"], "hydroweb_name", name)

            total_meas += nb
            inserted   += 1

            log.info(
                f"  [{i+1:3d}/{len(stations)}] {name:45s} {typ:10s} | "
                f"{nb} mesures | mission={mission} (~{med_interval:.0f}j)"
                if med_interval else
                f"  [{i+1:3d}/{len(stations)}] {name:45s} {typ:10s} | "
                f"{nb} mesures | mission={mission}"
            )

        except Exception as e:
            log.error(f"  [{i+1:3d}/{len(stations)}] {name} — erreur : {e}")
            errors += 1

    # 4. Rapport final
    log.info(
        f"\nÉtape 1 terminée : {inserted} stations insérées, "
        f"{skipped} ignorées, {errors} erreurs, "
        f"{total_meas} mesures au total"
    )
    print_report(conn)
    conn.close()

    return {
        "inserted"          : inserted,
        "skipped"           : skipped,
        "errors"            : errors,
        "total_measurements": total_meas,
    }


# ═══════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Étape 1 DAHITI — Import API → SQLite (schéma HydroWeb)",
        epilog="""
Exemples :
  python step1_import_dahiti.py
  python step1_import_dahiti.py --country france --continent Europe
  python step1_import_dahiti.py --db ./data/dahiti.db --reset
  python step1_import_dahiti.py --types River
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--country",   type=str, default=DEFAULT_COUNTRY,
                        help=f"Pays cible en minuscules (défaut: {DEFAULT_COUNTRY})")
    parser.add_argument("--continent", type=str, default=DEFAULT_CONTINENT,
                        help=f"Continent DAHITI (défaut: {DEFAULT_CONTINENT})")
    parser.add_argument("--types",     type=str, nargs="+", default=DEFAULT_TYPES,
                        help=f"Types de corps d'eau (défaut: {DEFAULT_TYPES})")
    parser.add_argument("--db",        type=str, default=str(DEFAULT_DB_PATH),
                        help=f"Chemin BDD (défaut: {DEFAULT_DB_PATH})")
    parser.add_argument("--reset",     action="store_true",
                        help="Supprimer et recréer la BDD")
    args = parser.parse_args()

    run_step1_dahiti(
        country   = args.country,
        continent = args.continent,
        types     = args.types,
        db_path   = Path(args.db),
        reset     = args.reset,
    )