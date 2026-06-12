#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
═══════════════════════════════════════════════════════════════════════════
pipeline.py — Pipeline complète de collecte de données
═══════════════════════════════════════════════════════════════════════════

Orchestre les 3 étapes :
    1. Import HydroWeb (.txt → stations + mesures)
    2. Attributs station (BV + Strahler + Elevation + Corine + ROE)
    3. ERA5 sur BV + features (pixels + météo + neige + moyennes glissantes)

Usage :
    python pipeline.py ./data/mes_stations/ --db ./data/test.db --reset
    python pipeline.py ./data/mes_stations/ --db ./data/test.db
    python pipeline.py ./data/mes_stations/ --db ./data/test.db --step 2   (reprendre à l'étape 2)
    python pipeline.py ./data/mes_stations/ --db ./data/test.db --step 3   (reprendre à l'étape 3)

═══════════════════════════════════════════════════════════════════════════
"""

import argparse
import logging
import sqlite3
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from Pipeline_data.Niveau_deau.step1_importWaterLvL import run_step1
from Pipeline_data.Bassin_Versant.step2_data_Watershed import run_step2
from Pipeline_data.ERA5.Step3_ERA5_compute import run_step3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("pipeline")


def format_duration(seconds: float) -> str:
    """Formate une durée en secondes en HH:MM:SS."""
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    if h > 0:
        return f"{h}h{m:02d}m{s:02d}s"
    elif m > 0:
        return f"{m}m{s:02d}s"
    return f"{s}s"


def print_final_report(db_path: str):
    """Affiche le rapport final de la BDD complète."""
    conn = sqlite3.connect(db_path)

    n_stations = conn.execute("SELECT COUNT(*) FROM stations").fetchone()[0]
    n_measures = conn.execute("SELECT COUNT(*) FROM measurements").fetchone()[0]
    n_bv = conn.execute("SELECT COUNT(*) FROM bv_data").fetchone()[0]
    n_pixels = conn.execute("SELECT COUNT(*) FROM era5_transfert").fetchone()[0]
    n_era5 = conn.execute("SELECT COUNT(*) FROM era5_bv_jour").fetchone()[0]
    n_attrs = conn.execute("SELECT COUNT(*) FROM measure_attributes").fetchone()[0]
    n_roe = conn.execute("SELECT COUNT(*) FROM roe_obstacles").fetchone()[0]

    # Complétude des attributs station
    fields = {
        "strahler": "SELECT COUNT(*) FROM stations WHERE strahler IS NOT NULL",
        "elevation": "SELECT COUNT(*) FROM stations WHERE elevation_mean IS NOT NULL",
        "corine": "SELECT COUNT(*) FROM stations WHERE frac_urban IS NOT NULL",
        "dist_barrage": "SELECT COUNT(*) FROM stations WHERE dist_barrage_m IS NOT NULL",
    }

    # Période
    period = conn.execute("SELECT MIN(measure_date), MAX(measure_date) FROM measurements").fetchone()

    print("\n")
    print("╔" + "═" * 58 + "╗")
    print("║" + "  RAPPORT FINAL — PIPELINE COMPLÈTE".center(58) + "║")
    print("╠" + "═" * 58 + "╣")
    print(f"║  Stations              : {n_stations:>6d}" + " " * 24 + "║")
    print(f"║  Mesures               : {n_measures:>6d}" + " " * 24 + "║")
    if period[0]:
        print(f"║  Période               : {period[0]} → {period[1]}" + " " * (58 - 41 - len(period[0]) - len(period[1])) + "║")
    print("╠" + "═" * 58 + "╣")
    print(f"║  Bassins versants      : {n_bv:>6d}" + " " * 24 + "║")
    print(f"║  Pixels ERA5           : {n_pixels:>6d}" + " " * 24 + "║")
    print(f"║  Jours ERA5            : {n_era5:>6d}" + " " * 24 + "║")
    print(f"║  Barrages ROE          : {n_roe:>6d}" + " " * 24 + "║")
    print("╠" + "═" * 58 + "╣")

    # Attributs station
    for label, query in fields.items():
        n = conn.execute(query).fetchone()[0]
        pct = 100 * n / n_stations if n_stations else 0
        status = "✅" if n == n_stations else "⚠️ "
        print(f"║  {status} {label:<22s}: {n:>4d}/{n_stations} ({pct:.0f}%)" + " " * 14 + "║")

    # Measure attributes
    pct_attrs = 100 * n_attrs / n_measures if n_measures else 0
    status = "✅" if pct_attrs > 90 else "⚠️ "
    print("╠" + "═" * 58 + "╣")
    print(f"║  {status} measure_attributes   : {n_attrs:>4d}/{n_measures} ({pct_attrs:.0f}%)" + " " * 14 + "║")
    print("╚" + "═" * 58 + "╝")

    conn.close()


def run_pipeline(dossier: str, db_path: str = "./data/test.db",
                 reset: bool = False, start_step: int = 1,
                 era5_base: str = "./data/ERA5/usable_data_LAND_France") -> dict:
    """
    Lance la pipeline complète.

    Args:
        dossier: Chemin vers les fichiers .txt HydroWeb
        db_path: Chemin vers la BDD
        reset: Si True, recrée la BDD from scratch
        start_step: Étape à laquelle commencer (1, 2 ou 3)
        era5_base: Chemin racine des fichiers ERA5

    Returns:
        Dict avec les résultats de chaque étape
    """
    results = {}
    t_start = time.time()

    # ══════════════════════════════════════════════════════════
    # ÉTAPE 1 — Import HydroWeb
    # ══════════════════════════════════════════════════════════
    if start_step <= 1:
        print("\n" + "█" * 60)
        print("  ÉTAPE 1/3 — Import HydroWeb")
        print("█" * 60)
        t1 = time.time()
        results["step1"] = run_step1(
            dossier=dossier, db_path=db_path, reset=reset
        )
        log.info(f"Étape 1 terminée en {format_duration(time.time() - t1)}")

    # ══════════════════════════════════════════════════════════
    # ÉTAPE 2 — Attributs station
    # ══════════════════════════════════════════════════════════
    if start_step <= 2:
        print("\n" + "█" * 60)
        print("  ÉTAPE 2/3 — Attributs station (BV + Strahler + ...)")
        print("█" * 60)
        t2 = time.time()
        results["step2"] = run_step2(db_path=db_path)
        log.info(f"Étape 2 terminée en {format_duration(time.time() - t2)}")

    # ══════════════════════════════════════════════════════════
    # ÉTAPE 3 — ERA5 + features
    # ══════════════════════════════════════════════════════════
    if start_step <= 3:
        print("\n" + "█" * 60)
        print("  ÉTAPE 3/3 — ERA5 sur BV + features")
        print("█" * 60)
        t3 = time.time()
        results["step3"] = run_step3(db_path=db_path, era5_base=era5_base)
        log.info(f"Étape 3 terminée en {format_duration(time.time() - t3)}")

    # ══════════════════════════════════════════════════════════
    # RAPPORT FINAL
    # ══════════════════════════════════════════════════════════
    total_time = time.time() - t_start
    print(f"\n⏱️  Pipeline complète en {format_duration(total_time)}")
    print_final_report(db_path)

    return results


# ══════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Pipeline complète : HydroWeb → BDD prête pour NeuralHydrology",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples :
  # Pipeline complète from scratch
  python pipeline.py ./data/stations_hw/ --db ./data/test.db --reset

  # Ajouter des stations sans toucher à l'existant
  python pipeline.py ./data/nouvelles_stations/ --db ./data/test.db

  # Reprendre à l'étape 2 (BV déjà calculés ? non, attributs)
  python pipeline.py ./data/stations_hw/ --db ./data/test.db --step 2

  # Reprendre à l'étape 3 seulement (ERA5)
  python pipeline.py ./data/stations_hw/ --db ./data/test.db --step 3
        """,
    )
    parser.add_argument("dossier", type=str,
                        help="Dossier contenant les fichiers HydroWeb")
    parser.add_argument("--db", type=str, default="./data/test.db",
                        help="Chemin vers la BDD (défaut: ./data/test.db)")
    parser.add_argument("--reset", action="store_true",
                        help="Supprimer et recréer la BDD")
    parser.add_argument("--step", type=int, default=1, choices=[1, 2, 3],
                        help="Étape à laquelle commencer (défaut: 1)")
    parser.add_argument("--era5", type=str,
                        default="./data/ERA5/usable_data_LAND_France",
                        help="Chemin racine ERA5")
    args = parser.parse_args()

    run_pipeline(
        dossier=args.dossier,
        db_path=args.db,
        reset=args.reset,
        start_step=args.step,
        era5_base=args.era5,
    )