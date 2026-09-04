#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
═══════════════════════════════════════════════════════════════════════════
import_pegelonline_germany.py — Import stations in situ Allemagne (PEGELONLINE)
═══════════════════════════════════════════════════════════════════════════

Récupère la liste des stations de niveau d'eau (W) via l'API REST publique
de PEGELONLINE, puis télécharge l'historique complet (2016-2025, résolution
~15 min) via l'endpoint "historische-zeitreihen/prepare-download" (celui
utilisé par le bouton "Download langfristiger Wasserstände (Rohdaten)" sur
le site), et insère le tout dans une BDD SQLite.

Endpoint découvert via inspection réseau (onglet Network du navigateur) :
    POST https://www.wasserstaende.de/gast/historische-zeitreihen/prepare-download
    Form data (x-www-form-urlencoded) :
        uuid       = <uuid station>
        parameter  = WASSERSTAND ROHDATEN
        start      = 2015-12-31T23:00:00.000Z   (ISO8601 UTC, 'Z')
        end        = 2025-12-31T22:59:59.000Z
        format     = json
    -> redirige (303) vers une URL de téléchargement du fichier généré
       (zip ou json direct selon les cas, gérés tous les deux ici).

Licence des données : DL-DE->Zero-2.0 (donnees ouvertes, pas d'inscription requise)

⚠️ Limite connue : PEGELONLINE ne couvre que les pegels des voies
navigables fédérales (~640-740 stations sur les grands fleuves/canaux).
Les pegels gérés par les Länder (régions) ne sont pas inclus.

⚠️ Temps de génération : le serveur peut prendre 15s à plusieurs minutes
par station pour générer 10 ans de données 15-min (~350 000 points).
Prévoir un temps de run long sur l'ensemble des stations.

Prérequis : pip install requests --break-system-packages

Usage :
    # 1. Test rapide sur 1 station
    python import_pegelonline_germany.py --db ./data/pegelonline.db --test-only

    # 2. Run complet
    python import_pegelonline_germany.py --db ./data/pegelonline.db \
        --start 2016-01-01 --end 2025-12-31
═══════════════════════════════════════════════════════════════════════════
"""

import argparse
import io
import json
import logging
import sqlite3
import time
import zipfile
from datetime import datetime, timezone

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("pegelonline")

STATIONS_URL = "https://www.wasserstaende.de/webservices/rest-api/v2/stations.json"
PREPARE_DOWNLOAD_URL = "https://www.wasserstaende.de/gast/historische-zeitreihen/prepare-download"

REQUEST_TIMEOUT = 180  # generation cote serveur peut etre longue sur 10 ans de donnees
SLEEP_BETWEEN_STATIONS = 1.0  # politesse envers le serveur
MAX_RETRIES = 2


# ═══════════════════════════════════════════════════════════════
# BDD
# ═══════════════════════════════════════════════════════════════
def create_tables(conn: sqlite3.Connection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stations_insitu (
            station_code TEXT PRIMARY KEY,
            uuid         TEXT,
            shortname    TEXT,
            longname     TEXT,
            water_name   TEXT,
            river_km     REAL,
            agency       TEXT,
            lon          REAL,
            lat          REAL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS measurements_insitu (
            station_code TEXT,
            timestamp    TEXT,
            water_level_cm REAL,
            PRIMARY KEY (station_code, timestamp)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS import_progress (
            station_code TEXT PRIMARY KEY,
            status       TEXT,
            n_measurements INTEGER,
            last_attempt TEXT
        )
    """)
    conn.commit()


def insert_station(conn: sqlite3.Connection, sta: dict):
    conn.execute("""
        INSERT OR REPLACE INTO stations_insitu
            (station_code, uuid, shortname, longname, water_name, river_km, agency, lon, lat)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        sta["number"], sta["uuid"], sta["shortname"], sta["longname"],
        sta.get("water", {}).get("longname"), sta.get("km"), sta.get("agency"),
        sta.get("longitude"), sta.get("latitude"),
    ))


def insert_measurements(conn: sqlite3.Connection, station_code: str, measurements: list) -> int:
    rows = [
        (station_code, m["timestamp"], m["value"])
        for m in measurements if m.get("value") is not None
    ]
    conn.executemany(
        "INSERT OR REPLACE INTO measurements_insitu (station_code, timestamp, water_level_cm) VALUES (?, ?, ?)",
        rows
    )
    return len(rows)


def mark_progress(conn: sqlite3.Connection, station_code: str, status: str, n: int = 0):
    conn.execute("""
        INSERT OR REPLACE INTO import_progress (station_code, status, n_measurements, last_attempt)
        VALUES (?, ?, ?, ?)
    """, (station_code, status, n, datetime.now().isoformat()))
    conn.commit()


def already_done(conn: sqlite3.Connection, station_code: str) -> bool:
    row = conn.execute(
        "SELECT status FROM import_progress WHERE station_code = ?", (station_code,)
    ).fetchone()
    return row is not None and row[0] == "ok"


# ═══════════════════════════════════════════════════════════════
# API PEGELONLINE
# ═══════════════════════════════════════════════════════════════
def fetch_stations() -> list:
    """Recupere toutes les stations ayant une serie temporelle de niveau d'eau (W)."""
    log.info("Récupération de la liste des stations PEGELONLINE...")
    r = requests.get(STATIONS_URL, params={"includeTimeseries": "true"}, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    all_stations = r.json()

    stations_w = [
        sta for sta in all_stations
        if "W" in [ts.get("shortname") for ts in sta.get("timeseries", [])]
    ]
    log.info(f"{len(all_stations)} stations au total, {len(stations_w)} avec niveau d'eau (W)")
    return stations_w


def to_iso_utc(date_str: str, end_of_day: bool = False) -> str:
    """Convertit 'YYYY-MM-DD' en 'YYYY-MM-DDTHH:MM:SS.000Z' (UTC)."""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    if end_of_day:
        dt = dt.replace(hour=23, minute=59, second=59)
    dt = dt.replace(tzinfo=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def parse_downloaded_content(content: bytes) -> list:
    """
    Le fichier retourné peut être un zip contenant un .json, ou un .json brut.
    Retourne la liste [{"timestamp":..., "value":...}, ...].
    """
    if zipfile.is_zipfile(io.BytesIO(content)):
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            json_names = [n for n in zf.namelist() if n.endswith(".json")]
            if not json_names:
                raise ValueError(f"Aucun .json trouvé dans le zip (contenu: {zf.namelist()})")
            with zf.open(json_names[0]) as f:
                return json.load(f)
    else:
        return json.loads(content)


def download_station_history(session: requests.Session, uuid: str, start: str, end: str) -> list:
    """
    Lance la génération + récupère l'historique complet pour une station,
    via l'endpoint prepare-download (suit la redirection automatiquement).
    """
    payload = {
        "uuid": uuid,
        "parameter": "WASSERSTAND ROHDATEN",
        "start": to_iso_utc(start),
        "end": to_iso_utc(end, end_of_day=True),
        "format": "json",
    }

    r = session.post(PREPARE_DOWNLOAD_URL, data=payload, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()  # suit les redirections (303 -> download?filename=...) automatiquement

    return parse_downloaded_content(r.content)


# ═══════════════════════════════════════════════════════════════
# ORCHESTRATION
# ═══════════════════════════════════════════════════════════════
def run_import(db_path: str, start: str, end: str, test_only: bool = False) -> dict:
    conn = sqlite3.connect(db_path)
    create_tables(conn)

    stations = fetch_stations()

    if test_only:
        stations = stations[:1]
        log.info(f"Mode --test-only : 1 seule station testée ({stations[0]['longname']})")

    session = requests.Session()

    total_inserted = 0
    stations_ok = 0
    stations_empty = 0
    errors = 0

    for i, sta in enumerate(stations):
        code = sta["number"]

        if already_done(conn, code):
            log.info(f"[{i+1}/{len(stations)}] {code} — déjà importé, skip")
            stations_ok += 1
            continue

        log.info(f"[{i+1}/{len(stations)}] {code} — {sta['longname']} "
                 f"({sta.get('water', {}).get('longname', '?')})")

        insert_station(conn, sta)
        conn.commit()

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                measurements = download_station_history(session, sta["uuid"], start, end)

                if not measurements:
                    log.warning(f"  Aucune mesure trouvée pour {code}")
                    mark_progress(conn, code, "empty")
                    stations_empty += 1
                    break

                n = insert_measurements(conn, code, measurements)
                conn.commit()
                total_inserted += n
                stations_ok += 1
                mark_progress(conn, code, "ok", n)
                log.info(f"  {n} mesures insérées "
                         f"(première: {measurements[0]['timestamp']}, "
                         f"dernière: {measurements[-1]['timestamp']})")
                break

            except Exception as e:
                if attempt < MAX_RETRIES:
                    log.warning(f"  Tentative {attempt} échouée pour {code} : {e} — retry...")
                    time.sleep(3)
                else:
                    log.error(f"  ERREUR définitive {code} : {e}")
                    mark_progress(conn, code, "error")
                    errors += 1

        time.sleep(SLEEP_BETWEEN_STATIONS)

    log.info(f"\nImport terminé : {stations_ok} stations OK, {stations_empty} sans données, "
             f"{errors} erreurs, {total_inserted} mesures au total")

    conn.close()
    return {
        "stations_ok": stations_ok,
        "stations_empty": stations_empty,
        "errors": errors,
        "total_measurements": total_inserted,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Import stations in situ Allemagne via PEGELONLINE (historique long terme)",
        epilog="""
Exemples :
  python import_pegelonline_germany.py --db ./data/pegelonline.db --test-only
  python import_pegelonline_germany.py --db ./data/pegelonline.db --start 2016-01-01 --end 2025-12-31

Le script est reprenable : relancer la meme commande apres interruption
reprend automatiquement aux stations non terminees (table import_progress).
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--db", type=str, default="./data/pegelonline.db")
    parser.add_argument("--start", type=str, default="2016-01-01")
    parser.add_argument("--end", type=str, default="2025-12-31")
    parser.add_argument("--test-only", action="store_true",
                        help="Ne teste que sur 1 station")
    args = parser.parse_args()

    run_import(args.db, args.start, args.end, test_only=args.test_only)