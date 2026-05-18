#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
═══════════════════════════════════════════════════════════════════════════
Test_step2.py — Test de l'étape 2 (Attributs station : BV + Strahler + ...)
═══════════════════════════════════════════════════════════════════════════

Vérifie que chaque sous-étape a bien rempli les données attendues.
Suppose que l'étape 1 a déjà été exécutée (stations + mesures en BDD).

Usage :
    python Test_step2.py ./data/test.db
    python Test_step2.py ./data/test.db --skip-compute   (vérifie sans relancer)
═══════════════════════════════════════════════════════════════════════════
"""

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from Pipeline_data.Database.db_operations import get_stats, get_all_station_codes, get_station


def get_conn(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def check_prerequisite(conn) -> bool:
    """Vérifie que l'étape 1 a été exécutée."""
    print("\n" + "─" * 60)
    print("  PRÉREQUIS — Étape 1 exécutée ?")
    print("─" * 60)

    stats = get_stats(conn)
    ok = stats["stations"] > 0 and stats["measurements"] > 0
    print(f"  Stations    : {stats['stations']}")
    print(f"  Mesures     : {stats['measurements']}")
    print(f"  → {'OK' if ok else 'ECHEC — lance d abord l étape 1'}")
    return ok


def test_2a_bv(conn) -> bool:
    """Test 2a : les BV sont calculés."""
    print("\n" + "─" * 60)
    print("  TEST 2a — Bassins versants")
    print("─" * 60)

    n_stations = conn.execute("SELECT COUNT(*) FROM stations").fetchone()[0]
    n_bv = conn.execute("SELECT COUNT(*) FROM bv_data").fetchone()[0]
    n_with_area = conn.execute(
        "SELECT COUNT(*) FROM bv_data WHERE aire_km2 IS NOT NULL AND aire_km2 > 0"
    ).fetchone()[0]
    n_with_poly = conn.execute(
        "SELECT COUNT(*) FROM bv_data WHERE polygone_wkt IS NOT NULL"
    ).fetchone()[0]

    print(f"  Stations totales   : {n_stations}")
    print(f"  BV calculés        : {n_bv}")
    print(f"  Avec aire > 0      : {n_with_area}")
    print(f"  Avec polygone      : {n_with_poly}")

    # Exemples
    examples = conn.execute("""
        SELECT b.station_code, s.river_name, b.aire_km2
        FROM bv_data b
        JOIN stations s ON b.station_code = s.station_code
        ORDER BY b.aire_km2 DESC
        LIMIT 5
    """).fetchall()
    if examples:
        print(f"\n  Top 5 BV (par aire) :")
        for row in examples:
            print(f"    {row[0]}  {row[1] or '?':<15s}  {row[2]:>8.1f} km²")

    # Cohérence : upstream_watershed_km2 synchro ?
    n_synced = conn.execute("""
        SELECT COUNT(*) FROM stations s
        JOIN bv_data b ON s.station_code = b.station_code
        WHERE s.upstream_watershed_km2 IS NOT NULL
    """).fetchone()[0]
    print(f"\n  Aire synchro stations ↔ bv_data : {n_synced}/{n_bv}")

    ok = n_bv > 0 and n_with_poly == n_bv
    print(f"\n  → 2a BV : {'OK' if ok else 'ECHEC'}")
    return ok


def test_2b_strahler(conn) -> bool:
    """Test 2b : Strahler rempli."""
    print("\n" + "─" * 60)
    print("  TEST 2b — Strahler")
    print("─" * 60)

    n_total = conn.execute("SELECT COUNT(*) FROM stations").fetchone()[0]
    n_strahler = conn.execute(
        "SELECT COUNT(*) FROM stations WHERE strahler IS NOT NULL"
    ).fetchone()[0]

    print(f"  Stations avec Strahler : {n_strahler}/{n_total}")

    # Distribution
    dist = conn.execute("""
        SELECT strahler, COUNT(*) as n
        FROM stations WHERE strahler IS NOT NULL
        GROUP BY strahler ORDER BY strahler
    """).fetchall()
    if dist:
        print(f"\n  Distribution :")
        for order, n in dist:
            bar = "█" * n
            print(f"    Strahler {order} : {n:>3d}  {bar}")

    # Sanity check : pas de Strahler = 0
    n_zero = conn.execute(
        "SELECT COUNT(*) FROM stations WHERE strahler = 0"
    ).fetchone()[0]
    if n_zero:
        print(f"\n  ⚠️  {n_zero} stations avec Strahler = 0 (suspect)")

    ok = n_strahler > 0
    print(f"\n  → 2b Strahler : {'OK' if ok else 'ECHEC'}")
    return ok


def test_2c_elevation(conn) -> bool:
    """Test 2c : elevation et slope remplis."""
    print("\n" + "─" * 60)
    print("  TEST 2c — Elevation / Slope")
    print("─" * 60)

    n_total = conn.execute("SELECT COUNT(*) FROM stations").fetchone()[0]
    n_elev = conn.execute(
        "SELECT COUNT(*) FROM stations WHERE elevation_mean IS NOT NULL"
    ).fetchone()[0]
    n_slope = conn.execute(
        "SELECT COUNT(*) FROM stations WHERE slope_mean IS NOT NULL"
    ).fetchone()[0]

    print(f"  Avec elevation : {n_elev}/{n_total}")
    print(f"  Avec slope     : {n_slope}/{n_total}")

    # Stats
    stats = conn.execute("""
        SELECT MIN(elevation_mean) as emin, MAX(elevation_mean) as emax,
               AVG(elevation_mean) as eavg,
               MIN(slope_mean) as smin, MAX(slope_mean) as smax,
               AVG(slope_mean) as savg
        FROM stations WHERE elevation_mean IS NOT NULL
    """).fetchone()

    if stats and stats[0] is not None:
        print(f"\n  Elevation (m) : min={stats[0]:.0f}  moy={stats[2]:.0f}  max={stats[1]:.0f}")
        print(f"  Slope (%)     : min={stats[3]:.2f}  moy={stats[5]:.2f}  max={stats[4]:.2f}")

    # Sanity check : elevation négative ?
    n_neg = conn.execute(
        "SELECT COUNT(*) FROM stations WHERE elevation_mean < 0"
    ).fetchone()[0]
    if n_neg:
        print(f"\n  ⚠️  {n_neg} stations avec elevation < 0 (suspect)")

    ok = n_elev > 0 and n_slope > 0
    print(f"\n  → 2c Elevation : {'OK' if ok else 'ECHEC'}")
    return ok


def test_2d_corine(conn) -> bool:
    """Test 2d : Corine + SoilGrids remplis."""
    print("\n" + "─" * 60)
    print("  TEST 2d — Corine / SoilGrids")
    print("─" * 60)

    n_total = conn.execute("SELECT COUNT(*) FROM stations").fetchone()[0]
    n_corine = conn.execute(
        "SELECT COUNT(*) FROM stations WHERE frac_urban IS NOT NULL"
    ).fetchone()[0]
    n_soil = conn.execute(
        "SELECT COUNT(*) FROM stations WHERE sg_clay_0_30cm IS NOT NULL"
    ).fetchone()[0]

    print(f"  Avec Corine    : {n_corine}/{n_total}")
    print(f"  Avec SoilGrids : {n_soil}/{n_total}")

    # Stats moyennes
    stats = conn.execute("""
        SELECT AVG(frac_urban) as urban, AVG(frac_agriculture) as agri,
               AVG(frac_forest) as forest,
               AVG(sg_clay_0_30cm) as clay, AVG(sg_sand_0_30cm) as sand
        FROM stations WHERE frac_urban IS NOT NULL
    """).fetchone()

    if stats and stats[0] is not None:
        print(f"\n  Moyennes Corine :")
        print(f"    Urban={stats[0]:.1%}  Agri={stats[1]:.1%}  Forest={stats[2]:.1%}")
        print(f"  Moyennes SoilGrids :")
        print(f"    Clay={stats[3]:.1f}%  Sand={stats[4]:.1f}%")

    # Sanity check : fractions > 1 ?
    n_bad = conn.execute(
        "SELECT COUNT(*) FROM stations WHERE frac_urban > 1 OR frac_forest > 1"
    ).fetchone()[0]
    if n_bad:
        print(f"\n  ⚠️  {n_bad} stations avec fraction > 1 (erreur)")

    ok = n_corine > 0
    print(f"\n  → 2d Corine : {'OK' if ok else 'ECHEC'}")
    return ok


def test_2e_barrage(conn) -> bool:
    """Test 2e : distance barrages + ROE en BDD."""
    print("\n" + "─" * 60)
    print("  TEST 2e — Distance barrages ROE")
    print("─" * 60)

    n_roe = conn.execute("SELECT COUNT(*) FROM roe_obstacles").fetchone()[0]
    n_total = conn.execute("SELECT COUNT(*) FROM stations").fetchone()[0]
    n_dist = conn.execute(
        "SELECT COUNT(*) FROM stations WHERE dist_barrage_m IS NOT NULL"
    ).fetchone()[0]

    print(f"  Barrages ROE en BDD  : {n_roe}")
    print(f"  Stations avec dist   : {n_dist}/{n_total}")

    # Distribution par seuil
    if n_dist > 0:
        print(f"\n  Distribution distances :")
        for seuil, label in [(100, "< 100m"), (500, "< 500m"), (1000, "< 1km"),
                             (5000, "< 5km"), (10000, "< 10km")]:
            n = conn.execute(
                "SELECT COUNT(*) FROM stations WHERE dist_barrage_m < ?", (seuil,)
            ).fetchone()[0]
            print(f"    {label:<10s} : {n:>3d} stations")

    ok = n_roe > 0 and n_dist > 0
    print(f"\n  → 2e Barrage : {'OK' if ok else 'ECHEC'}")
    return ok


def test_completude(conn) -> bool:
    """Vérifie la complétude globale de l'étape 2."""
    print("\n" + "─" * 60)
    print("  COMPLÉTUDE GLOBALE")
    print("─" * 60)

    n_total = conn.execute("SELECT COUNT(*) FROM stations").fetchone()[0]

    fields = [
        ("BV (bv_data)", "SELECT COUNT(*) FROM bv_data"),
        ("strahler", "SELECT COUNT(*) FROM stations WHERE strahler IS NOT NULL"),
        ("elevation_mean", "SELECT COUNT(*) FROM stations WHERE elevation_mean IS NOT NULL"),
        ("slope_mean", "SELECT COUNT(*) FROM stations WHERE slope_mean IS NOT NULL"),
        ("frac_urban", "SELECT COUNT(*) FROM stations WHERE frac_urban IS NOT NULL"),
        ("sg_clay_0_30cm", "SELECT COUNT(*) FROM stations WHERE sg_clay_0_30cm IS NOT NULL"),
        ("dist_barrage_m", "SELECT COUNT(*) FROM stations WHERE dist_barrage_m IS NOT NULL"),
    ]

    all_complete = True
    for label, query in fields:
        n = conn.execute(query).fetchone()[0]
        pct = 100 * n / n_total if n_total else 0
        status = "✅" if n == n_total else "⚠️ "
        if n < n_total:
            all_complete = False
        print(f"  {status} {label:<20s} : {n:>4d}/{n_total} ({pct:.0f}%)")

    # Station exemple complète
    example = conn.execute("""
        SELECT s.station_code, s.river_name, b.aire_km2,
               s.strahler, s.elevation_mean, s.slope_mean,
               s.frac_forest, s.sg_clay_0_30cm, s.dist_barrage_m
        FROM stations s
        JOIN bv_data b ON s.station_code = b.station_code
        WHERE s.strahler IS NOT NULL
          AND s.elevation_mean IS NOT NULL
          AND s.frac_urban IS NOT NULL
          AND s.dist_barrage_m IS NOT NULL
        LIMIT 1
    """).fetchone()

    if example:
        print(f"\n  Exemple station complète : {example[0]}")
        print(f"    Rivière      : {example[1]}")
        print(f"    Aire BV      : {example[2]:.1f} km²")
        print(f"    Strahler     : {example[3]}")
        print(f"    Elevation    : {example[4]:.0f} m")
        print(f"    Slope        : {example[5]:.2f} %")
        print(f"    Forest       : {example[6]:.1%}")
        print(f"    Clay         : {example[7]:.1f} %")
        print(f"    Dist barrage : {example[8]} m")

    return all_complete


def main():
    parser = argparse.ArgumentParser(
        description="Test de l'étape 2 de la pipeline",
        epilog="Exemple : python Test_step2.py ./data/test.db",
    )
    parser.add_argument("db_path", type=str, help="Chemin vers la BDD")
    parser.add_argument("--skip-compute", action="store_true",
                        help="Ne pas relancer les calculs, vérifier seulement")
    args = parser.parse_args()

    db_path = Path(args.db_path)
    if not db_path.exists():
        print(f"BDD introuvable : {db_path}")
        sys.exit(1)

    conn = get_conn(str(db_path))

    print("═" * 60)
    print("  TEST ÉTAPE 2 — Attributs station")
    print(f"  BDD : {db_path}")
    print("═" * 60)

    # Prérequis
    if not check_prerequisite(conn):
        conn.close()
        sys.exit(1)

    # Optionnel : relancer les calculs
    if not args.skip_compute:
        print("\n" + "═" * 60)
        print("  EXÉCUTION ÉTAPE 2")
        print("═" * 60)
        try:
            from Pipeline_data.Bassin_Versant.step2_data_Watershed import run_step2
            conn.close()
            run_step2(db_path=str(db_path))
            conn = get_conn(str(db_path))
        except Exception as e:
            print(f"\n  ⚠️  Erreur lors de l'exécution : {e}")
            print("  On continue avec la vérification...")
            conn = get_conn(str(db_path))

    # Tests
    r_bv = test_2a_bv(conn)
    r_str = test_2b_strahler(conn)
    r_elev = test_2c_elevation(conn)
    r_cor = test_2d_corine(conn)
    r_bar = test_2e_barrage(conn)
    r_comp = test_completude(conn)

    # Bilan
    print("\n" + "═" * 60)
    print("  BILAN")
    print("═" * 60)
    print(f"  2a BV            : {'✅' if r_bv else '❌'}")
    print(f"  2b Strahler      : {'✅' if r_str else '❌'}")
    print(f"  2c Elevation     : {'✅' if r_elev else '❌'}")
    print(f"  2d Corine/Soil   : {'✅' if r_cor else '❌'}")
    print(f"  2e Dist barrage  : {'✅' if r_bar else '❌'}")
    print(f"  Complétude       : {'✅' if r_comp else '⚠️  Partielle'}")
    print("═" * 60)

    all_ok = all([r_bv, r_str, r_elev, r_cor, r_bar])
    if all_ok:
        print("\n  Tout est bon, prêt pour l'étape 3 (ERA5 sur BV)")
    else:
        print("\n  Des sous-étapes ont échoué — vérifie les données sources")
        sys.exit(1)

    conn.close()


if __name__ == "__main__":
    main()