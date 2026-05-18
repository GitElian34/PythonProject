#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
═══════════════════════════════════════════════════════════════════════════
step1_import_hydroweb.py — Étape 1 : Import HydroWeb → BDD
═══════════════════════════════════════════════════════════════════════════

Orchestre le parsing des fichiers .txt HydroWeb et l'insertion en BDD.
Expose une fonction run_step1() appelable depuis la pipeline globale.

Usage standalone :
    python step1_import_hydroweb.py ./data/stations_hw/
    python step1_import_hydroweb.py ./data/stations_hw/ --db ./data/test.db --reset

Usage depuis la pipeline :
    from step1_import_hydroweb import run_step1
    run_step1(dossier="./data/stations_hw/", db_path="./data/test.db", reset=True)
═══════════════════════════════════════════════════════════════════════════
"""

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from Pipeline_data.Database.DB_schema import create_database
from Pipeline_data.Database.db_operations import insert_station, insert_measurements, print_report
from Pipeline_data.Niveau_deau.Parser import parse_hydroweb_directory

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("step1")


def run_step1(dossier: str, db_path: str = "./data/test.db",
              reset: bool = False) -> dict:
    """
    Étape 1 de la pipeline : parse les .txt HydroWeb et insère en BDD.

    Args:
        dossier: Chemin vers le dossier contenant les .txt
        db_path: Chemin vers la BDD SQLite
        reset: Si True, supprime et recrée la BDD

    Returns:
        Dict avec les compteurs {"inserted": n, "skipped": n, "errors": n}
    """
    dossier = Path(dossier)
    db_path = Path(db_path)

    if not dossier.is_dir():
        log.error(f"Dossier introuvable : {dossier}")
        return {"inserted": 0, "skipped": 0, "errors": 1}

    # 1. Créer / ouvrir la BDD
    conn = create_database(db_path, reset=reset)

    # 2. Parser tous les fichiers
    results = parse_hydroweb_directory(dossier)
    if not results:
        log.warning("Aucun fichier parsé")
        conn.close()
        return {"inserted": 0, "skipped": 0, "errors": 0}

    # 3. Insérer en BDD
    inserted, skipped, errors = 0, 0, 0

    for filepath, metadata, measurements in results:
        try:
            station_code = metadata.get("ID")
            if not station_code:
                log.warning(f"  {filepath.name} — pas d'ID, ignoré")
                skipped += 1
                continue

            if not measurements:
                log.warning(f"  {filepath.name} — aucune mesure, ignoré")
                skipped += 1
                continue

            # Insérer la station
            success = insert_station(conn, metadata)
            if not success:
                skipped += 1
                continue

            # Insérer les mesures
            nb = insert_measurements(conn, station_code, measurements)
            if nb > 0:
                inserted += 1
            else:
                skipped += 1

        except Exception as e:
            log.error(f"  Erreur sur {filepath.name}: {e}")
            errors += 1

    # 4. Rapport
    log.info(f"Étape 1 terminée : {inserted} stations insérées, "
             f"{skipped} ignorées, {errors} erreurs")
    print_report(conn)

    conn.close()

    return {"inserted": inserted, "skipped": skipped, "errors": errors}


# ═══════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Étape 1 — Import HydroWeb .txt → SQLite",
        epilog="""
Exemples :
  python step1_import_hydroweb.py ./data/stations_hw/
  python step1_import_hydroweb.py ./data/stations_hw/ --db ./data/test.db --reset
        """,
    )
    parser.add_argument("dossier", type=str, help="Dossier avec les .txt HydroWeb")
    parser.add_argument("--db", type=str, default="./data/test.db", help="Chemin BDD")
    parser.add_argument("--reset", action="store_true", help="Supprimer et recréer la BDD")
    args = parser.parse_args()

    run_step1(dossier=args.dossier, db_path=args.db, reset=args.reset)