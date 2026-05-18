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
    python step3_run_all.py --db ./data/test.db

Usage depuis la pipeline :
    from step3_run_all import run_step3
    run_step3(db_path="./data/test.db")
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

DEFAULT_ERA5_BASE = "./data/ERA5/usable_data_LAND_France"


def run_step3(db_path: str = "./data/test.db",
              era5_base: str = DEFAULT_ERA5_BASE) -> dict:
    """
    Étape 3 complète : ERA5 sur BV + features.

    Returns:
        Dict avec les résultats de chaque sous-étape
    """
    conn = sqlite3.connect(db_path)
    results = {}

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
    parser = argparse.ArgumentParser(description="Étape 3 — ERA5 + features")
    parser.add_argument("--db", type=str, default="./data/test.db")
    parser.add_argument("--era5", type=str, default=DEFAULT_ERA5_BASE)
    args = parser.parse_args()

    run_step3(db_path=args.db, era5_base=args.era5)