#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
═══════════════════════════════════════════════════════════════════════════
test_step1.py — Test de l'étape 1 (import HydroWeb → BDD)
═══════════════════════════════════════════════════════════════════════════

Usage :
    python test_step1.py ./data/stations_hw/ test.db
    python test_step1.py ./data/Garonne_hw/ ma_base.db
═══════════════════════════════════════════════════════════════════════════
"""

import argparse
import sqlite3
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from Pipeline_data.Database.DB_schema import create_database
from Pipeline_data.Database.db_operations import (
    get_stats, get_all_station_codes, get_station,
    get_measurements, get_measurement_dates, print_report,
)
from Pipeline_data.Niveau_deau.Parser import parse_hydroweb_file, parse_hydroweb_directory
from Pipeline_data.Niveau_deau.step1_importWaterLvL import run_step1

def test_parser(dossier: Path):
    """Test 1 : le parser lit bien les fichiers."""
    print("\n" + "─" * 60)
    print("  TEST 1 — Parser")
    print("─" * 60)

    fichiers = sorted(dossier.glob("*.txt"))
    if not fichiers:
        print(f"  ECHEC : aucun .txt dans {dossier}")
        return False

    print(f"  {len(fichiers)} fichiers .txt trouvés")

    # Tester le premier fichier en détail
    f = fichiers[0]
    metadata, measurements = parse_hydroweb_file(f)

    print(f"\n  Fichier test : {f.name}")
    print(f"  Métadonnées  : {len(metadata)} champs")
    print(f"  Mesures      : {len(measurements)}")

    # Vérifier les champs obligatoires
    required = ["ID", "BASIN", "RIVER", "REFERENCE LATITUDE", "REFERENCE LONGITUDE"]
    missing = [k for k in required if k not in metadata]
    if missing:
        print(f"  ATTENTION : champs manquants dans le header : {missing}")

    if metadata.get("ID"):
        print(f"  ID           : {metadata['ID']}")
    if metadata.get("RIVER"):
        print(f"  Rivière      : {metadata['RIVER']}")
    if metadata.get("REFERENCE LATITUDE"):
        print(f"  Coordonnées  : {metadata['REFERENCE LATITUDE']}, {metadata['REFERENCE LONGITUDE']}")

    if measurements:
        m0 = measurements[0]
        mN = measurements[-1]
        print(f"  1ère mesure  : {m0['date']} h={m0['height']}m")
        print(f"  Dernière     : {mN['date']} h={mN['height']}m")

        # Compter les invalides
        nb_invalid = sum(1 for m in measurements if not m["is_valid"])
        if nb_invalid:
            print(f"  Invalides    : {nb_invalid}/{len(measurements)}")

    # Test du parsing dossier complet
    results = parse_hydroweb_directory(dossier)
    total_meas = sum(len(m) for _, _, m in results)
    print(f"\n  Dossier complet : {len(results)} fichiers, {total_meas} mesures au total")

    ok = len(metadata) > 0 and len(measurements) > 0
    print(f"\n  → Parser : {'OK' if ok else 'ECHEC'}")
    return ok


def test_db_creation(db_path: Path):
    """Test 2 : la BDD se crée correctement."""
    print("\n" + "─" * 60)
    print("  TEST 2 — Création BDD")
    print("─" * 60)

    conn = create_database(db_path, reset=True)

    # Vérifier que les 3 tables existent
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    table_names = [t[0] for t in tables]

    expected = ["measure_attributes", "measurements", "stations"]
    ok = all(t in table_names for t in expected)

    print(f"  Tables créées : {table_names}")
    print(f"  Tables attendues présentes : {'OUI' if ok else 'NON'}")

    # Vérifier les colonnes de stations
    cols_stations = conn.execute("PRAGMA table_info(stations)").fetchall()
    col_names = [c[1] for c in cols_stations]
    print(f"  Colonnes stations : {len(col_names)}")

    # Vérifier les nouvelles colonnes
    for col in ["frac_urban", "frac_forest", "frac_agriculture",
                "sg_clay_0_30cm", "sg_sand_0_30cm", "sg_silt_0_30cm"]:
        if col not in col_names:
            print(f"  ATTENTION : colonne {col} manquante dans stations")
            ok = False

    # Vérifier que rating_curve n'est PAS là
    for col in ["rating_curve_A", "rating_curve_b", "rating_curve_Zo"]:
        if col in col_names:
            print(f"  ATTENTION : colonne {col} ne devrait pas être là")
            ok = False

    # Vérifier les colonnes de measure_attributes
    cols_attrs = conn.execute("PRAGMA table_info(measure_attributes)").fetchall()
    attr_names = [c[1] for c in cols_attrs]
    print(f"  Colonnes measure_attributes : {len(attr_names)}")

    for col in ["precipitation_J0", "clim_mean_20j", "precip_max_J27", "precip_last7"]:
        if col not in attr_names:
            print(f"  ATTENTION : colonne {col} manquante dans measure_attributes")
            ok = False

    conn.close()
    print(f"\n  → Création BDD : {'OK' if ok else 'ECHEC'}")
    return ok


def test_import(dossier: Path, db_path: Path):
    """Test 3 : l'import complet fonctionne."""
    print("\n" + "─" * 60)
    print("  TEST 3 — Import complet")
    print("─" * 60)

    result = run_step1(
        dossier=str(dossier),
        db_path=str(db_path),
        reset=True,
    )

    print(f"\n  Résultat : {result}")

    ok = result["inserted"] > 0 and result["errors"] == 0

    # Vérifications post-import
    conn = sqlite3.connect(str(db_path))

    stats = get_stats(conn)
    print(f"  Stations en BDD      : {stats['stations']}")
    print(f"  Mesures en BDD       : {stats['measurements']}")
    print(f"  Attributs en BDD     : {stats['measure_attributes']} (attendu: 0)")

    # Vérifier qu'on peut relire les données
    codes = get_all_station_codes(conn)
    if codes:
        # Tester sur la première station
        code = codes[0]
        station = get_station(conn, code)
        measures = get_measurements(conn, code)
        dates = get_measurement_dates(conn, code)

        print(f"\n  Station test         : {code}")
        print(f"  Rivière              : {station.get('river_name', '?')}")
        print(f"  Coordonnées          : {station.get('reference_latitude')}, "
              f"{station.get('reference_longitude')}")
        print(f"  Mesures récupérées   : {len(measures)}")
        print(f"  Dates distinctes     : {len(dates)}")
        if dates:
            print(f"  Période              : {dates[0]} → {dates[-1]}")

    # Rapport complet
    print_report(conn)

    conn.close()
    print(f"  → Import : {'OK' if ok else 'ECHEC'}")
    return ok


def main():
    parser = argparse.ArgumentParser(
        description="Test de l'étape 1 de la pipeline",
        epilog="Exemple : python test_step1.py ./data/stations_hw/ test.db",
    )
    parser.add_argument("dossier", type=str, help="Dossier contenant les .txt HydroWeb")
    parser.add_argument("db_name", type=str, help="Nom de la BDD (sera créée dans ./data/)")
    args = parser.parse_args()

    dossier = Path(args.dossier)
    db_path = Path("./data") / args.db_name

    if not dossier.is_dir():
        print(f"Dossier introuvable : {dossier}")
        sys.exit(1)

    print("═" * 60)
    print("  TEST ÉTAPE 1 — Import HydroWeb → SQLite")
    print(f"  Dossier : {dossier}")
    print(f"  BDD     : {db_path}")
    print("═" * 60)

    r1 = test_parser(dossier)
    r2 = test_db_creation(db_path)
    r3 = test_import(dossier, db_path)

    print("\n" + "═" * 60)
    print("  BILAN")
    print("═" * 60)
    print(f"  Parser       : {'✅' if r1 else '❌'}")
    print(f"  Création BDD : {'✅' if r2 else '❌'}")
    print(f"  Import       : {'✅' if r3 else '❌'}")
    print("═" * 60)

    if all([r1, r2, r3]):
        print("\n  Tout est bon, prêt pour l'étape 2 (délinéation BV)")
    else:
        print("\n  Des erreurs à corriger avant de continuer")
        sys.exit(1)


if __name__ == "__main__":
    main()