#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
═══════════════════════════════════════════════════════════════════════════
step2_run_all.py — Étape 2 : Attributs station (BV + Strahler + ...)
═══════════════════════════════════════════════════════════════════════════

Orchestre les 5 sous-étapes :
    2a. Délinéation du bassin versant (pysheds + HydroSHEDS)
    2b. Ordre de Strahler (RiverATLAS)
    2c. Elevation / Slope (SRTM)
    2d. Corine Land Cover + SoilGrids
    2e. Distance au barrage (ROE)

Usage standalone :
    python step2_run_all.py --db ./data/test.db

Usage depuis la pipeline :
    from step2_run_all import run_step2
    run_step2(db_path="./data/test.db")
═══════════════════════════════════════════════════════════════════════════
"""

import argparse
import logging
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from Pipeline_data.Bassin_Versant.delineate_bv import run_step2a
from Pipeline_data.Bassin_Versant.strahler import run_step2b
from Pipeline_data.Bassin_Versant.elevation_slope import run_step2c
from Pipeline_data.Bassin_Versant.corine_soilgrids import run_step2d
from Pipeline_data.Bassin_Versant.dist_barrage import run_step2e


DEFAULT_DIR_PATH = "/home/sar_hydro/STUDIES/EtudesEB/HydroSHED/other/hyd_eu_dir_15s.tif"
DEFAULT_ACC_PATH = "/home/sar_hydro/STUDIES/EtudesEB/HydroSHED/other/hyd_eu_acc_15s.tif"


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("step2")


def run_step2(db_path: str = "./data/test.db", **kwargs) -> dict:
    """
    Étape 2 complète : enrichit les stations avec tous les attributs.

    kwargs optionnels :
        dir_path, acc_path     → step2a
        river_atlas_path       → step2b
        dem_path, slope_path   → step2c
        corine_path, soilgrids_dir → step2d

    Returns:
        Dict avec les résultats de chaque sous-étape
    """
    conn = sqlite3.connect(db_path)
    results = {}

    # ── 2a. Bassin Versant ──────────────────────────────────────────
    print("\n" + "═" * 60)
    print("  ÉTAPE 2a — Délinéation des bassins versants")
    print("═" * 60)
    try:
        results["2a_bv"] = run_step2a(
            conn,
            dir_path=kwargs.get("dir_path", DEFAULT_DIR_PATH),
            acc_path=kwargs.get("acc_path", DEFAULT_ACC_PATH),
        )
    except Exception as e:
        log.error(f"Étape 2a échouée : {e}")
        results["2a_bv"] = {"computed": 0, "errors": 1, "error_msg": str(e)}

    # ── 2b. Strahler ────────────────────────────────────────────────
    print("\n" + "═" * 60)
    print("  ÉTAPE 2b — Ordre de Strahler")
    print("═" * 60)
    try:
        results["2b_strahler"] = run_step2b(
            conn,
            river_atlas_path=kwargs.get("river_atlas_path", "./data/HydroSHED/RiverATLAS_v10_eu.shp"),
        )
    except Exception as e:
        log.error(f"Étape 2b échouée : {e}")
        results["2b_strahler"] = {"updated": 0, "error_msg": str(e)}

    # ── 2c. Elevation / Slope ───────────────────────────────────────
    print("\n" + "═" * 60)
    print("  ÉTAPE 2c — Elevation / Slope")
    print("═" * 60)
    try:
        results["2c_elev_slope"] = run_step2c(
            conn,
            dem_path=kwargs.get("dem_path", "./data/Elevation/srtm_france.tif"),
            slope_path=kwargs.get("slope_path", "./data/Elevation/slope_france.tif"),
        )
    except Exception as e:
        log.error(f"Étape 2c échouée : {e}")
        results["2c_elev_slope"] = {"updated": 0, "error_msg": str(e)}

    # ── 2d. Corine + SoilGrids ─────────────────────────────────────
    print("\n" + "═" * 60)
    print("  ÉTAPE 2d — Corine + SoilGrids")
    print("═" * 60)
    try:
        results["2d_corine"] = run_step2d(
            conn,
            corine_path=kwargs.get("corine_path", "./data/Bassin_Versants/Corine/u2018_clc2018_v2020_20u1_raster100m/DATA/U2018_CLC2018_V2020_20u1.tif"),
            soilgrids_dir=kwargs.get("soilgrids_dir", "./data/Bassin_Versants/SoilGrids/"),
        )
    except Exception as e:
        log.error(f"Étape 2d échouée : {e}")
        results["2d_corine"] = {"updated": 0, "error_msg": str(e)}

    # ── 2e. Distance barrages ──────────────────────────────────────
    print("\n" + "═" * 60)
    print("  ÉTAPE 2e — Distance barrages ROE")
    print("═" * 60)
    try:
        results["2e_barrage"] = run_step2e(conn)
    except Exception as e:
        log.error(f"Étape 2e échouée : {e}")
        results["2e_barrage"] = {"updated": 0, "error_msg": str(e)}

    # ── Rapport final ──────────────────────────────────────────────
    print("\n" + "═" * 60)
    print("  RAPPORT ÉTAPE 2")
    print("═" * 60)

    n_total = conn.execute("SELECT COUNT(*) FROM stations").fetchone()[0]
    fields = {
        "BV calculés": ("bv_data", "SELECT COUNT(*) FROM bv_data"),
        "Strahler": ("stations", "SELECT COUNT(*) FROM stations WHERE strahler IS NOT NULL"),
        "Elevation": ("stations", "SELECT COUNT(*) FROM stations WHERE elevation_mean IS NOT NULL"),
        "Corine": ("stations", "SELECT COUNT(*) FROM stations WHERE frac_urban IS NOT NULL"),
        "Dist barrage": ("stations", "SELECT COUNT(*) FROM stations WHERE dist_barrage_m IS NOT NULL"),
    }

    for label, (table, query) in fields.items():
        n = conn.execute(query).fetchone()[0]
        pct = 100 * n / n_total if n_total else 0
        status = "✅" if n == n_total else "⚠️ "
        print(f"  {status} {label:<20s} : {n:>4d}/{n_total} ({pct:.0f}%)")

    print("═" * 60)

    conn.close()
    return results


# ═══════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Étape 2 — Tous les attributs station")
    parser.add_argument("--db", type=str, default="./data/test.db")
    args = parser.parse_args()

    run_step2(db_path=args.db)