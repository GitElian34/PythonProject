#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
═══════════════════════════════════════════════════════════════════════════
step3_run_all.py — Étape 3 : ERA5 sur BV + features
═══════════════════════════════════════════════════════════════════════════

Orchestre les 4 sous-étapes :
    3a. Pixels ERA5 par BV (grille 0.1° dans le polygone)
    3b. ERA5 météo quotidien (precip/temp/pet) → era5_bv_jour
    3c. ERA5 neige quotidien (snow_depth/snowmelt) → era5_bv_jour
    3d. Calcul des features → measure_attributes

Usage standalone :
    python step3_run_all.py --db ./data/test.db --era5 ./data/ERA5/usable_data_LAND_France
    python step3_run_all.py --db ./data/test.db --reset   (vide 3a/3b/3c/3d puis quitte)

Usage depuis la pipeline :
    from step3_run_all import run_step3
    run_step3(db_path="./data/test.db", era5_base="./data/ERA5/usable_data_LAND_France")

⚠️ CHANGEMENT vs version précédente :
    Il n'y a plus de valeur par defaut implicite pour era5_base. La
    version précédente pointait silencieusement vers le dossier France
    si --era5 n'était pas précisé, ce qui a déjà causé un mauvais
    forçage météo joint sur des stations allemandes sans erreur visible.
    Maintenant, era5_base est obligatoire : le script s'arrête avec un
    message clair plutôt que de continuer avec les mauvaises données.
═══════════════════════════════════════════════════════════════════════════
"""

import argparse
import logging
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from Pipeline_data.ERA5.Pixels import run_step3a
from Pipeline_data.ERA5.Meteo_Temp_Precip_ETP  import run_step3b
from Pipeline_data.ERA5.Snow import run_step3c
from Pipeline_data.ERA5.Compute_features import run_step3d

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("step3")


def reset_step3(conn: sqlite3.Connection) -> dict:
    """
    Vide les tables produites par l'étape 3 (era5_transfert, era5_bv_jour,
    measure_attributes) pour repartir de zéro. Ne touche ni measurements
    ni stations ni bv_data.
    """
    counts = {}
    for table in ["era5_transfert", "era5_bv_jour", "measure_attributes"]:
        n = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        conn.execute(f"DELETE FROM {table}")
        counts[table] = n
    conn.commit()
    log.info(f"Reset étape 3 : {counts}")
    return counts


def run_step3(db_path: str = "./data/test.db",
              era5_base: str | None = None) -> dict:
    """
    Étape 3 complète : ERA5 sur BV + features.

    Args:
        db_path: Chemin vers la BDD SQLite
        era5_base: Chemin vers le dossier ERA5 usable (obligatoire, pas de
            défaut implicite — cf. note en tête de fichier sur le bug
            précédent lié à un défaut silencieux "France").

    Returns:
        Dict avec les résultats de chaque sous-étape
    """
    if not era5_base:
        raise ValueError(
            "era5_base est obligatoire (ex: ./data/ERA5/usable_data_LAND_Allemagne). "
            "Aucun défaut implicite n'est utilisé, pour éviter de joindre "
            "silencieusement les mauvaises données météo sur les stations."
        )

    conn = sqlite3.connect(db_path)
    results = {}

    log.info(f"Dossier ERA5 utilisé : {era5_base}")

    # ── 3a. Pixels ERA5 ────────────────────────────────────────
    print("\n" + "═" * 60)
    print("  ÉTAPE 3a — Pixels ERA5 par BV")
    print("═" * 60)
    try:
        results["3a_pixels"] = run_step3a(conn)
    except Exception as e:
        log.error(f"Étape 3a échouée : {e}")
        results["3a_pixels"] = {"error": str(e)}

    # ── 3b. ERA5 météo ─────────────────────────────────────────
    print("\n" + "═" * 60)
    print("  ÉTAPE 3b — ERA5 météo quotidien")
    print("═" * 60)
    try:
        results["3b_meteo"] = run_step3b(conn, era5_base)
    except Exception as e:
        log.error(f"Étape 3b échouée : {e}")
        results["3b_meteo"] = {"error": str(e)}

    # ── 3c. ERA5 neige ─────────────────────────────────────────
    print("\n" + "═" * 60)
    print("  ÉTAPE 3c — ERA5 neige quotidien")
    print("═" * 60)
    try:
        results["3c_snow"] = run_step3c(conn, era5_base)
    except Exception as e:
        log.error(f"Étape 3c échouée : {e}")
        results["3c_snow"] = {"error": str(e)}

    # ── 3d. Features ───────────────────────────────────────────
    print("\n" + "═" * 60)
    print("  ÉTAPE 3d — Calcul features → measure_attributes")
    print("═" * 60)
    try:
        results["3d_features"] = run_step3d(conn)
    except Exception as e:
        log.error(f"Étape 3d échouée : {e}")
        results["3d_features"] = {"error": str(e)}

    # ── Rapport ────────────────────────────────────────────────
    print("\n" + "═" * 60)
    print("  RAPPORT ÉTAPE 3")
    print("═" * 60)

    n_pixels = conn.execute("SELECT COUNT(*) FROM era5_transfert").fetchone()[0]
    n_sta_pixels = conn.execute(
        "SELECT COUNT(DISTINCT station_code) FROM era5_transfert"
    ).fetchone()[0]
    n_era5_days = conn.execute("SELECT COUNT(*) FROM era5_bv_jour").fetchone()[0]
    n_snow = conn.execute(
        "SELECT COUNT(*) FROM era5_bv_jour WHERE snow_depth_bv IS NOT NULL"
    ).fetchone()[0]
    n_attrs = conn.execute("SELECT COUNT(*) FROM measure_attributes").fetchone()[0]
    n_measures = conn.execute("SELECT COUNT(*) FROM measurements").fetchone()[0]

    print(f"  Pixels ERA5          : {n_pixels} ({n_sta_pixels} stations)")
    print(f"  ERA5 jours (météo)   : {n_era5_days}")
    print(f"  ERA5 jours (neige)   : {n_snow}")
    print(f"  Measure attributes   : {n_attrs}/{n_measures} mesures remplies")

    pct = 100 * n_attrs / n_measures if n_measures else 0
    status = "✅" if pct > 90 else "⚠️ "
    print(f"  {status} Complétude : {pct:.0f}%")
    print("═" * 60)

    conn.close()
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Étape 3 — ERA5 + features",
        epilog="""
Exemples :
  python step3_run_all.py --db ./data/hydroweb_next_Allemagne.db --era5 ./data/ERA5/usable_data_LAND_Allemagne
  python step3_run_all.py --db ./data/hydroweb_next_Allemagne.db --reset
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--db", type=str, default="./data/test.db")
    parser.add_argument("--era5", type=str, default=None,
                        help="Dossier ERA5 usable (obligatoire sauf avec --reset)")
    parser.add_argument("--reset", action="store_true",
                        help="Vide era5_transfert/era5_bv_jour/measure_attributes puis quitte")
    args = parser.parse_args()

    if args.reset:
        conn = sqlite3.connect(args.db)
        reset_step3(conn)
        conn.close()
        raise SystemExit(0)

    run_step3(db_path=args.db, era5_base=args.era5)