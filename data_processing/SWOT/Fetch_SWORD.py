#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
═══════════════════════════════════════════════════════════════════════════
fetch_swot_hydrocron_france.py — Récupère les hauteurs d'eau SWOT (France)
═══════════════════════════════════════════════════════════════════════════

Étape 2 du plan SWOT France : pour une liste de reach_id SWORD (obtenue
via l'étape 1 - filtrage du fichier SWORD Europe sur la bbox France),
interroge l'API Hydrocron (PO.DAAC) pour récupérer la série temporelle
de hauteur d'eau (wse) de chaque tronçon, et insère en SQLite.

Doc Hydrocron : https://podaac.github.io/hydrocron/timeseries.html
Nécessite un compte NASA Earthdata gratuit (urs.earthdata.nasa.gov).

Prérequis : pip install requests --break-system-packages

Usage :
    python fetch_swot_hydrocron_france.py --reach-ids-file reach_ids_france.txt \
        --db ./data/swot_france.db --start 2023-07-28 --end 2025-12-31
═══════════════════════════════════════════════════════════════════════════
"""

import argparse
import csv
import io
import logging
import sqlite3
import time

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("hydrocron")

HYDROCRON_URL = "https://soto.podaac.earthdatacloud.nasa.gov/hydrocron/v1/timeseries"
FIELDS = "reach_id,time_str,wse,width,slope"
SLEEP_BETWEEN_REQUESTS = 0.5
REQUEST_TIMEOUT = 60


def create_tables(conn: sqlite3.Connection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS swot_reaches (
            reach_id TEXT,
            time_str TEXT,
            wse REAL,
            width REAL,
            slope REAL,
            PRIMARY KEY (reach_id, time_str)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS swot_progress (
            reach_id TEXT PRIMARY KEY,
            status TEXT,
            n_obs INTEGER
        )
    """)
    conn.commit()


def already_done(conn: sqlite3.Connection, reach_id: str) -> bool:
    row = conn.execute(
        "SELECT status FROM swot_progress WHERE reach_id = ?", (reach_id,)
    ).fetchone()
    return row is not None and row[0] in ("ok", "no_data")


def fetch_reach_timeseries(reach_id: str, start: str, end: str) -> list:
    """Interroge Hydrocron pour un reach_id, retourne une liste de dicts."""
    params = {
        "feature": "Reach",
        "feature_id": reach_id,
        "start_time": f"{start}T00:00:00Z",
        "end_time": f"{end}T00:00:00Z",
        "fields": FIELDS,
        "output": "csv",
    }
    r = requests.get(HYDROCRON_URL, params=params, timeout=REQUEST_TIMEOUT)

    if r.status_code == 400:
        # Hydrocron renvoie 400 quand aucune donnée n'est trouvée pour la periode/reach
        return []

    r.raise_for_status()
    data = r.json()
    csv_text = data.get("results", {}).get("csv", "")
    if not csv_text.strip():
        return []

    reader = csv.DictReader(io.StringIO(csv_text))
    return [row for row in reader if row.get("time_str") != "no_data"]


def run_fetch(reach_ids: list, db_path: str, start: str, end: str) -> dict:
    conn = sqlite3.connect(db_path)
    create_tables(conn)

    total_obs = 0
    reaches_ok = 0
    reaches_empty = 0
    errors = 0

    for i, reach_id in enumerate(reach_ids):
        if already_done(conn, reach_id):
            log.info(f"[{i+1}/{len(reach_ids)}] {reach_id} — déjà fait, skip")
            continue

        try:
            rows = fetch_reach_timeseries(reach_id, start, end)

            if not rows:
                conn.execute(
                    "INSERT OR REPLACE INTO swot_progress VALUES (?, 'no_data', 0)",
                    (reach_id,)
                )
                conn.commit()
                reaches_empty += 1
                log.info(f"[{i+1}/{len(reach_ids)}] {reach_id} — aucune donnée")
                continue

            for row in rows:
                conn.execute("""
                    INSERT OR REPLACE INTO swot_reaches (reach_id, time_str, wse, width, slope)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    reach_id, row["time_str"],
                    float(row["wse"]) if row.get("wse") else None,
                    float(row["width"]) if row.get("width") else None,
                    float(row["slope"]) if row.get("slope") else None,
                ))

            conn.execute(
                "INSERT OR REPLACE INTO swot_progress VALUES (?, 'ok', ?)",
                (reach_id, len(rows))
            )
            conn.commit()

            total_obs += len(rows)
            reaches_ok += 1
            log.info(f"[{i+1}/{len(reach_ids)}] {reach_id} — {len(rows)} observations")

        except Exception as e:
            log.error(f"[{i+1}/{len(reach_ids)}] {reach_id} — ERREUR : {e}")
            conn.execute(
                "INSERT OR REPLACE INTO swot_progress VALUES (?, 'error', 0)",
                (reach_id,)
            )
            conn.commit()
            errors += 1

        time.sleep(SLEEP_BETWEEN_REQUESTS)

    log.info(f"\nTerminé : {reaches_ok} reaches OK, {reaches_empty} sans données, "
             f"{errors} erreurs, {total_obs} observations au total")

    conn.close()
    return {"reaches_ok": reaches_ok, "reaches_empty": reaches_empty,
            "errors": errors, "total_obs": total_obs}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Récupère les hauteurs d'eau SWOT via Hydrocron pour une liste de reach_id"
    )
    parser.add_argument("--reach-ids-file", type=str, required=True,
                        help="Fichier texte, un reach_id par ligne")
    parser.add_argument("--db", type=str, default="./data/swot_france.db")
    parser.add_argument("--start", type=str, default="2023-07-28",
                        help="Début (SWOT science phase démarre fin juillet 2023)")
    parser.add_argument("--end", type=str, default="2025-12-31")
    args = parser.parse_args()

    with open(args.reach_ids_file) as f:
        reach_ids = [line.strip() for line in f if line.strip()]

    log.info(f"{len(reach_ids)} reach_id à traiter")
    run_fetch(reach_ids, args.db, args.start, args.end)