#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
═══════════════════════════════════════════════════════════════════════════
Test_step3.py — Test de l'étape 3 (ERA5 sur BV + features)
═══════════════════════════════════════════════════════════════════════════

Vérifie que chaque sous-étape a bien rempli les données attendues.
Suppose que les étapes 1 et 2 ont déjà été exécutées.

Usage :
    python Test_step3.py ./data/test.db
    python Test_step3.py ./data/test.db --skip-compute
    python Test_step3.py ./data/test.db --era5 ./data/ERA5/usable_data_LAND_France
═══════════════════════════════════════════════════════════════════════════
"""

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from Pipeline_data.Database.db_operations import get_stats, get_all_station_codes


def get_conn(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def check_prerequisites(conn) -> bool:
    """Vérifie que les étapes 1 et 2 ont été exécutées."""
    print("\n" + "─" * 60)
    print("  PRÉREQUIS — Étapes 1 + 2 exécutées ?")
    print("─" * 60)

    stats = get_stats(conn)
    n_bv = conn.execute("SELECT COUNT(*) FROM bv_data").fetchone()[0]

    print(f"  Stations    : {stats['stations']}")
    print(f"  Mesures     : {stats['measurements']}")
    print(f"  BV calculés : {n_bv}")

    ok = stats["stations"] > 0 and stats["measurements"] > 0 and n_bv > 0
    print(f"  → {'OK' if ok else 'ECHEC — lance d abord les étapes 1 et 2'}")
    return ok


def test_3a_pixels(conn) -> bool:
    """Test 3a : pixels ERA5 calculés pour chaque BV."""
    print("\n" + "─" * 60)
    print("  TEST 3a — Pixels ERA5 par BV")
    print("─" * 60)

    n_bv = conn.execute("SELECT COUNT(*) FROM bv_data WHERE polygone_wkt IS NOT NULL").fetchone()[0]
    n_sta_pixels = conn.execute(
        "SELECT COUNT(DISTINCT station_code) FROM era5_transfert"
    ).fetchone()[0]
    n_pixels_total = conn.execute("SELECT COUNT(*) FROM era5_transfert").fetchone()[0]

    print(f"  BV avec polygone       : {n_bv}")
    print(f"  Stations avec pixels   : {n_sta_pixels}")
    print(f"  Pixels totaux          : {n_pixels_total}")

    if n_sta_pixels > 0:
        avg_pixels = n_pixels_total / n_sta_pixels
        print(f"  Moyenne pixels/station : {avg_pixels:.1f}")

    # Distribution du nombre de pixels par station
    dist = conn.execute("""
        SELECT 
            CASE 
                WHEN cnt = 1 THEN '1 pixel (BV petit)'
                WHEN cnt BETWEEN 2 AND 10 THEN '2-10 pixels'
                WHEN cnt BETWEEN 11 AND 50 THEN '11-50 pixels'
                WHEN cnt BETWEEN 51 AND 200 THEN '51-200 pixels'
                ELSE '200+ pixels'
            END as tranche,
            COUNT(*) as nb_stations
        FROM (
            SELECT station_code, COUNT(*) as cnt
            FROM era5_transfert GROUP BY station_code
        )
        GROUP BY tranche
        ORDER BY MIN(cnt)
    """).fetchall()

    if dist:
        print(f"\n  Distribution pixels/station :")
        for tranche, n in dist:
            print(f"    {tranche:<25s} : {n:>3d} stations")

    # Exemples
    examples = conn.execute("""
        SELECT e.station_code, s.river_name, COUNT(*) as nb_px
        FROM era5_transfert e
        JOIN stations s ON e.station_code = s.station_code
        GROUP BY e.station_code
        ORDER BY nb_px DESC
        LIMIT 3
    """).fetchall()
    if examples:
        print(f"\n  Top 3 (nb pixels) :")
        for code, river, nb in examples:
            print(f"    {code}  {river or '?':<15s}  {nb:>4d} pixels")

    ok = n_sta_pixels > 0 and n_sta_pixels == n_bv
    print(f"\n  → 3a Pixels : {'OK' if ok else 'ECHEC'}")
    return ok


def test_3b_meteo(conn) -> bool:
    """Test 3b : ERA5 météo quotidien rempli."""
    print("\n" + "─" * 60)
    print("  TEST 3b — ERA5 météo quotidien")
    print("─" * 60)

    n_total = conn.execute("SELECT COUNT(*) FROM era5_bv_jour").fetchone()[0]
    n_stations = conn.execute(
        "SELECT COUNT(DISTINCT station_code) FROM era5_bv_jour"
    ).fetchone()[0]

    print(f"  Lignes era5_bv_jour   : {n_total}")
    print(f"  Stations couvertes    : {n_stations}")

    if n_total > 0:
        avg_days = n_total / n_stations if n_stations else 0
        print(f"  Moyenne jours/station : {avg_days:.0f}")

    # Période couverte
    period = conn.execute("""
        SELECT MIN(date), MAX(date) FROM era5_bv_jour
    """).fetchone()
    if period[0]:
        print(f"  Période               : {period[0]} → {period[1]}")

    # Vérifier les NaN
    n_null_temp = conn.execute(
        "SELECT COUNT(*) FROM era5_bv_jour WHERE temp_moy_bv IS NULL"
    ).fetchone()[0]
    n_null_precip = conn.execute(
        "SELECT COUNT(*) FROM era5_bv_jour WHERE precip_sum_bv IS NULL"
    ).fetchone()[0]
    n_null_pet = conn.execute(
        "SELECT COUNT(*) FROM era5_bv_jour WHERE pet_sum_bv IS NULL"
    ).fetchone()[0]

    print(f"\n  NaN temp     : {n_null_temp}")
    print(f"  NaN precip   : {n_null_precip}")
    print(f"  NaN pet      : {n_null_pet}")

    # Stats sur une station exemple
    example = conn.execute("""
        SELECT station_code, COUNT(*) as n,
               AVG(temp_moy_bv) as avg_temp,
               AVG(precip_sum_bv) as avg_precip,
               AVG(pet_sum_bv) as avg_pet
        FROM era5_bv_jour
        GROUP BY station_code
        LIMIT 1
    """).fetchone()
    if example:
        print(f"\n  Station exemple : {example[0]}")
        print(f"    Jours      : {example[1]}")
        print(f"    Temp moy   : {example[2]:.1f} °C")
        print(f"    Precip moy : {example[3]:.2f} mm/j")
        print(f"    PET moy    : {example[4]:.2f} mm/j")

    # Sanity checks
    bad_temp = conn.execute(
        "SELECT COUNT(*) FROM era5_bv_jour WHERE temp_moy_bv < -50 OR temp_moy_bv > 50"
    ).fetchone()[0]
    bad_precip = conn.execute(
        "SELECT COUNT(*) FROM era5_bv_jour WHERE precip_sum_bv < 0 OR precip_sum_bv > 500"
    ).fetchone()[0]
    if bad_temp:
        print(f"\n  ⚠️  {bad_temp} lignes avec temp hors [-50, 50]°C")
    if bad_precip:
        print(f"  ⚠️  {bad_precip} lignes avec precip hors [0, 500] mm")

    ok = n_total > 0 and n_stations > 0
    print(f"\n  → 3b Météo : {'OK' if ok else 'ECHEC'}")
    return ok


def test_3c_snow(conn) -> bool:
    """Test 3c : neige remplie dans era5_bv_jour."""
    print("\n" + "─" * 60)
    print("  TEST 3c — ERA5 neige")
    print("─" * 60)

    n_total = conn.execute("SELECT COUNT(*) FROM era5_bv_jour").fetchone()[0]
    n_snow = conn.execute(
        "SELECT COUNT(*) FROM era5_bv_jour WHERE snow_depth_bv IS NOT NULL"
    ).fetchone()[0]
    n_no_snow = n_total - n_snow

    pct = 100 * n_snow / n_total if n_total else 0
    print(f"  Jours avec neige remplie : {n_snow}/{n_total} ({pct:.0f}%)")
    print(f"  Jours sans neige         : {n_no_snow}")

    # Stats neige
    stats = conn.execute("""
        SELECT AVG(snow_depth_bv) as avg_sd,
               MAX(snow_depth_bv) as max_sd,
               AVG(snowmelt_bv) as avg_smlt,
               MAX(snowmelt_bv) as max_smlt
        FROM era5_bv_jour
        WHERE snow_depth_bv IS NOT NULL
    """).fetchone()

    if stats and stats[0] is not None:
        print(f"\n  Snow depth  : moy={stats[0]:.2f} mm  max={stats[1]:.1f} mm")
        print(f"  Snowmelt    : moy={stats[2]:.4f} mm  max={stats[3]:.2f} mm")

    ok = n_snow > 0
    print(f"\n  → 3c Neige : {'OK' if ok else 'ECHEC'}")
    return ok


def test_3d_features(conn) -> bool:
    """Test 3d : measure_attributes rempli."""
    print("\n" + "─" * 60)
    print("  TEST 3d — Features (measure_attributes)")
    print("─" * 60)

    n_measures = conn.execute("SELECT COUNT(*) FROM measurements").fetchone()[0]
    n_attrs = conn.execute("SELECT COUNT(*) FROM measure_attributes").fetchone()[0]
    pct = 100 * n_attrs / n_measures if n_measures else 0

    print(f"  Mesures totales        : {n_measures}")
    print(f"  Attributes remplis     : {n_attrs} ({pct:.0f}%)")

    # Vérifier chaque colonne
    columns = [
        "precipitation_J0", "temperature_J0", "pet_J0",
        "precip_mean_J3", "pet_mean_J3", "temp_mean_J3",
        "precip_mean_J10", "temp_mean_J10", "precip_mean_J27",
        "clim_mean_20j", "clim_std_20j",
        "precip_max_J27", "precip_last7",
    ]

    print(f"\n  Complétude par colonne :")
    all_filled = True
    for col in columns:
        n_filled = conn.execute(
            f"SELECT COUNT(*) FROM measure_attributes WHERE {col} IS NOT NULL"
        ).fetchone()[0]
        col_pct = 100 * n_filled / n_attrs if n_attrs else 0
        status = "✅" if col_pct > 95 else "⚠️ "
        if col_pct <= 95:
            all_filled = False
        print(f"    {status} {col:<25s} : {n_filled:>5d}/{n_attrs} ({col_pct:.0f}%)")

    # Exemple station complète
    example = conn.execute("""
        SELECT ma.station_code, ma.measure_date,
               ma.precipitation_J0, ma.temperature_J0, ma.pet_J0,
               ma.precip_mean_J3, ma.precip_mean_J10, ma.precip_mean_J27,
               ma.clim_mean_20j, ma.clim_std_20j,
               ma.precip_max_J27, ma.precip_last7
        FROM measure_attributes ma
        WHERE ma.precipitation_J0 IS NOT NULL
          AND ma.clim_mean_20j IS NOT NULL
          AND ma.precip_max_J27 IS NOT NULL
        LIMIT 1
    """).fetchone()

    if example:
        print(f"\n  Exemple mesure complète :")
        print(f"    Station        : {example[0]}")
        print(f"    Date           : {example[1]}")
        print(f"    Precip J0      : {example[2]:.2f} mm")
        print(f"    Temp J0        : {example[3]:.1f} °C")
        print(f"    PET J0         : {example[4]:.2f} mm")
        print(f"    Precip moy J3  : {example[5]:.2f} mm")
        print(f"    Precip moy J10 : {example[6]:.2f} mm")
        print(f"    Precip moy J27 : {example[7]:.2f} mm")
        print(f"    Clim mean ±20j : {example[8]:.4f}")
        print(f"    Clim std ±20j  : {example[9]:.4f}")
        print(f"    Precip max J27 : {example[10]:.2f} mm")
        print(f"    Precip last 7j : {example[11]:.2f} mm")

    # Sanity checks
    bad_precip = conn.execute(
        "SELECT COUNT(*) FROM measure_attributes WHERE precipitation_J0 < 0"
    ).fetchone()[0]
    bad_clim_std = conn.execute(
        "SELECT COUNT(*) FROM measure_attributes WHERE clim_std_20j <= 0"
    ).fetchone()[0]
    if bad_precip:
        print(f"\n  ⚠️  {bad_precip} mesures avec precip_J0 < 0")
    if bad_clim_std:
        print(f"  ⚠️  {bad_clim_std} mesures avec clim_std ≤ 0")

    ok = n_attrs > 0 and pct > 50
    print(f"\n  → 3d Features : {'OK' if ok else 'ECHEC'}")
    return ok


def test_coherence(conn) -> bool:
    """Vérifie la cohérence entre era5_bv_jour et measure_attributes."""
    print("\n" + "─" * 60)
    print("  COHÉRENCE ERA5 ↔ FEATURES")
    print("─" * 60)

    # Vérifier que les features correspondent aux données ERA5
    check = conn.execute("""
        SELECT ma.station_code, ma.measure_date,
               ma.precipitation_J0, e.precip_sum_bv
        FROM measure_attributes ma
        JOIN era5_bv_jour e ON ma.station_code = e.station_code
                            AND ma.measure_date = e.date
        WHERE ma.precipitation_J0 IS NOT NULL
        LIMIT 5
    """).fetchall()

    if check:
        print(f"  Vérification precip_J0 = era5 precip du jour :")
        all_match = True
        for code, date, j0, era5 in check:
            match = abs((j0 or 0) - (era5 or 0)) < 0.01
            status = "✅" if match else "❌"
            if not match:
                all_match = False
            print(f"    {status} {code} {date} : J0={j0:.3f}  ERA5={era5:.3f}")

        ok = all_match
    else:
        print(f"  Pas de données pour vérifier la cohérence")
        ok = False

    # Vérifier que J3 < J10 < J27 en moyenne (la moyenne glissante lisse)
    avg_check = conn.execute("""
        SELECT AVG(precip_mean_J3), AVG(precip_mean_J10), AVG(precip_mean_J27)
        FROM measure_attributes
        WHERE precip_mean_J3 IS NOT NULL
    """).fetchone()

    if avg_check and avg_check[0] is not None:
        print(f"\n  Moyennes globales (vérification lissage) :")
        print(f"    Precip J3  : {avg_check[0]:.3f} mm")
        print(f"    Precip J10 : {avg_check[1]:.3f} mm")
        print(f"    Precip J27 : {avg_check[2]:.3f} mm")

    print(f"\n  → Cohérence : {'OK' if ok else 'À VÉRIFIER'}")
    return ok


def main():
    parser = argparse.ArgumentParser(
        description="Test de l'étape 3 de la pipeline",
        epilog="Exemple : python Test_step3.py ./data/test.db",
    )
    parser.add_argument("db_path", type=str, help="Chemin vers la BDD")
    parser.add_argument("--skip-compute", action="store_true",
                        help="Ne pas relancer les calculs, vérifier seulement")
    parser.add_argument("--era5", type=str,
                        default="./data/ERA5/usable_data_LAND_France",
                        help="Chemin racine ERA5")
    args = parser.parse_args()

    db_path = Path(args.db_path)
    if not db_path.exists():
        print(f"BDD introuvable : {db_path}")
        sys.exit(1)

    conn = get_conn(str(db_path))

    print("═" * 60)
    print("  TEST ÉTAPE 3 — ERA5 sur BV + Features")
    print(f"  BDD  : {db_path}")
    print(f"  ERA5 : {args.era5}")
    print("═" * 60)

    # Prérequis
    if not check_prerequisites(conn):
        conn.close()
        sys.exit(1)

    # Optionnel : relancer les calculs
    if not args.skip_compute:
        print("\n" + "═" * 60)
        print("  EXÉCUTION ÉTAPE 3")
        print("═" * 60)
        try:
            from Pipeline_data.ERA5.Step3_ERA5_compute import run_step3
            conn.close()
            run_step3(db_path=str(db_path), era5_base=args.era5)
            conn = get_conn(str(db_path))
        except Exception as e:
            print(f"\n  ⚠️  Erreur lors de l'exécution : {e}")
            print("  On continue avec la vérification...")
            conn = get_conn(str(db_path))

    # Tests
    r_pix = test_3a_pixels(conn)
    r_met = test_3b_meteo(conn)
    r_snw = test_3c_snow(conn)
    r_feat = test_3d_features(conn)
    r_coh = test_coherence(conn)

    # Bilan
    print("\n" + "═" * 60)
    print("  BILAN")
    print("═" * 60)
    print(f"  3a Pixels ERA5   : {'✅' if r_pix else '❌'}")
    print(f"  3b Météo         : {'✅' if r_met else '❌'}")
    print(f"  3c Neige         : {'✅' if r_snw else '❌'}")
    print(f"  3d Features      : {'✅' if r_feat else '❌'}")
    print(f"  Cohérence        : {'✅' if r_coh else '⚠️'}")
    print("═" * 60)

    all_ok = all([r_pix, r_met, r_snw, r_feat])
    if all_ok:
        print("\n  Tout est bon, la BDD est complète !")
        print("  Prochaine étape : création du dataset NeuralHydrology")
    else:
        print("\n  Des sous-étapes ont échoué — vérifie les données ERA5")
        sys.exit(1)

    conn.close()


if __name__ == "__main__":
    main()