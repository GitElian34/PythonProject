#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
═══════════════════════════════════════════════════════════════════════════
step1_import_hydroweb_next.py — Étape 1 : Import HydroWeb Next → BDD
═══════════════════════════════════════════════════════════════════════════

Télécharge les stations HydroWeb Next via py_hydroweb (API CNES/Theia),
parse les fichiers .txt (même format que HydroWeb classique) et insère
en BDD via les fonctions existantes de la pipeline.

Différences vs step1_import_hydroweb.py :
  - Collecte via py_hydroweb au lieu de fichiers .txt manuels
  - BDD séparée : hydroweb_next.db
  - Reprocessing HySOpe (PRODUCT VERSION 2.0)

Usage standalone :
    python step1_import_hydroweb_next.py
    python step1_import_hydroweb_next.py --db ./data/hydroweb_next.db --reset
    python step1_import_hydroweb_next.py --bbox -5.5 41.0 9.5 51.5

Usage depuis la pipeline :
    from step1_import_hydroweb_next import run_step1_hydroweb_next
    run_step1_hydroweb_next(db_path="./data/hydroweb_next.db", reset=True)
═══════════════════════════════════════════════════════════════════════════
"""

import argparse
import logging
import shutil
import sys
import zipfile
from pathlib import Path

import py_hydroweb

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from Pipeline_data.Database.DB_schema    import create_database
from Pipeline_data.Database.db_operations import (
    insert_station, insert_measurements, print_report
)
from Pipeline_data.Niveau_deau.Parser import parse_hydroweb_file

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("step1_hw_next")

# ═══════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════
API_KEY         = "AJerWWCpm4wIaH8CMgPZlf67hNBC0VRMeCeeB1KgkaDHctfvYP"
COLLECTION_ID   = "HYDROWEB_RIVERS_OPE"
DEFAULT_DB_PATH = Path("./data/hydroweb_next_Allemagne.db")
# DEFAULT_BBOX    = [-5.5, 41.0, 9.5, 51.5]# France métropolitaine
DEFAULT_BBOX = [5.5, 47.0, 15.5, 55.5] #Allemagne
DOWNLOAD_DIR    = Path("./data/hydroweb_next/downloads")
MIN_MEASUREMENTS = 5


# ═══════════════════════════════════════════════════════════════
# TÉLÉCHARGEMENT VIA PY_HYDROWEB
# ═══════════════════════════════════════════════════════════════
def download_zip(bbox: list[float], output_dir: Path) -> Path:
    """
    Télécharge le zip HydroWeb Next pour la bbox donnée.
    Retourne le chemin du zip.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    zip_path = output_dir / "hydroweb_next_download.zip"

    log.info(f"Connexion à HydroWeb Next (collection={COLLECTION_ID})...")
    client = py_hydroweb.Client(api_key=API_KEY)
    basket = py_hydroweb.DownloadBasket("hw_next_import")
    basket.add_collection(COLLECTION_ID, bbox=bbox)

    log.info("Soumission et téléchargement...")
    client.submit_and_download_zip(
        basket,
        zip_filename=zip_path.name,
        output_folder=str(output_dir),
    )
    log.info(f"✅ Zip téléchargé → {zip_path}")
    return zip_path


# ═══════════════════════════════════════════════════════════════
# EXTRACTION DU ZIP
# ═══════════════════════════════════════════════════════════════
def extract_txt_files(zip_path: Path, extract_dir: Path) -> list[Path]:
    """
    Extrait les fichiers .txt du zip dans extract_dir.
    Retourne la liste des fichiers .txt extraits.
    """
    if extract_dir.exists():
        shutil.rmtree(extract_dir)
    extract_dir.mkdir(parents=True)

    with zipfile.ZipFile(zip_path) as z:
        txt_names = [n for n in z.namelist() if n.endswith(".txt")]
        log.info(f"  {len(txt_names)} fichiers .txt dans le zip")
        z.extractall(extract_dir)

    txt_files = list(extract_dir.rglob("*.txt"))
    log.info(f"  {len(txt_files)} fichiers .txt extraits → {extract_dir}")
    return txt_files


# ═══════════════════════════════════════════════════════════════
# FONCTION PRINCIPALE
# ═══════════════════════════════════════════════════════════════
def run_step1_hydroweb_next(
    bbox        : list[float] = DEFAULT_BBOX,
    db_path     : Path        = DEFAULT_DB_PATH,
    reset       : bool        = False,
    keep_files  : bool        = False,
) -> dict:
    """
    Étape 1 HydroWeb Next : téléchargement + parse + insertion BDD.

    Args:
        bbox       : Bounding box [lon_min, lat_min, lon_max, lat_max]
        db_path    : Chemin vers la BDD SQLite
        reset      : Si True, supprime et recrée la BDD
        keep_files : Si True, conserve les fichiers extraits après import

    Returns:
        Dict {"inserted": n, "skipped": n, "errors": n, "total_measurements": n}
    """
    db_path = Path(db_path)

    # 1. Créer / ouvrir la BDD (même schéma que HydroWeb classique)
    conn = create_database(db_path, reset=reset)
    log.info(f"BDD : {db_path}")

    # 2. Télécharger le zip
    zip_path = download_zip(bbox, DOWNLOAD_DIR)

    # 3. Extraire les .txt
    extract_dir = DOWNLOAD_DIR / "extracted"
    txt_files   = extract_txt_files(zip_path, extract_dir)

    if not txt_files:
        log.warning("Aucun fichier .txt trouvé dans le zip")
        conn.close()
        return {"inserted": 0, "skipped": 0, "errors": 0, "total_measurements": 0}

    # 4. Parser + insérer station par station
    log.info(f"\nParsing + insertion ({len(txt_files)} fichiers)...")
    inserted = skipped = errors = total_meas = 0

    for i, filepath in enumerate(sorted(txt_files)):
        try:
            metadata, measurements = parse_hydroweb_file(filepath)

            station_code = metadata.get("ID")
            if not station_code:
                log.warning(f"  [{i+1:3d}/{len(txt_files)}] {filepath.name} — pas d'ID → skip")
                skipped += 1
                continue

            if len(measurements) < MIN_MEASUREMENTS:
                log.info(f"  [{i+1:3d}/{len(txt_files)}] {station_code} — "
                         f"trop peu de mesures ({len(measurements)}) → skip")
                skipped += 1
                continue

            ok = insert_station(conn, metadata)
            if not ok:
                log.debug(f"  [{i+1:3d}/{len(txt_files)}] {station_code} — déjà présente")

            nb = insert_measurements(conn, station_code, measurements)
            total_meas += nb
            inserted   += 1

            log.info(f"  [{i+1:3d}/{len(txt_files)}] {station_code:15s} | "
                     f"{metadata.get('RIVER', '?'):30s} | "
                     f"{nb} mesures | "
                     f"{metadata.get('MISSION(S)-TRACK(S)', '?')}")

        except Exception as e:
            log.error(f"  [{i+1:3d}/{len(txt_files)}] {filepath.name} — erreur : {e}")
            errors += 1

    # 5. Nettoyage optionnel
    if not keep_files:
        shutil.rmtree(extract_dir, ignore_errors=True)
        log.info("Fichiers extraits supprimés (--keep_files pour conserver)")

    # 6. Rapport
    log.info(f"\nÉtape 1 terminée : {inserted} stations insérées, "
             f"{skipped} ignorées, {errors} erreurs, "
             f"{total_meas} mesures au total")
    print_report(conn)
    conn.close()

    return {
        "inserted"          : inserted,
        "skipped"           : skipped,
        "errors"            : errors,
        "total_measurements": total_meas,
    }


# ═══════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Étape 1 HydroWeb Next — Téléchargement + Import → SQLite",
        epilog="""
Exemples :
  python step1_import_hydroweb_next.py
  python step1_import_hydroweb_next.py --db ./data/hydroweb_next.db --reset
  python step1_import_hydroweb_next.py --bbox -5.5 41.0 9.5 51.5
  python step1_import_hydroweb_next.py --keep_files
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--db",    type=str, default=str(DEFAULT_DB_PATH),
                        help=f"Chemin BDD (défaut: {DEFAULT_DB_PATH})")
    parser.add_argument("--bbox",  type=float, nargs=4,
                        default=DEFAULT_BBOX,
                        metavar=("LON_MIN", "LAT_MIN", "LON_MAX", "LAT_MAX"),
                        help=f"Bounding box (défaut: France {DEFAULT_BBOX})")
    parser.add_argument("--reset", action="store_true",
                        help="Supprimer et recréer la BDD")
    parser.add_argument("--keep_files", action="store_true",
                        help="Conserver les fichiers .txt après import")
    args = parser.parse_args()

    run_step1_hydroweb_next(
        bbox       = args.bbox,
        db_path    = Path(args.db),
        reset      = args.reset,
        keep_files = args.keep_files,
    )