#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
═══════════════════════════════════════════════════════════════════════════
step3d_compute_features.py — Calcul des features pour measure_attributes
═══════════════════════════════════════════════════════════════════════════

À partir de era5_bv_jour (quotidien), calcule pour chaque date de mesure
les moyennes glissantes et features dérivées, puis remplit measure_attributes.

Features calculées :
    - precipitation_J0, temperature_J0, pet_J0 (valeurs du jour)
    - precip_mean_J3, pet_mean_J3, temp_mean_J3 (moyenne 3 derniers jours)
    - precip_mean_J10, temp_mean_J10 (moyenne 10 derniers jours)
    - precip_mean_J27 (moyenne 27 derniers jours)
    - clim_mean_20j, clim_std_20j (climatologie fenêtrée ±20j, leave-one-year-out)
    - precip_max_J27 (pic journalier max sur 27j)
    - precip_last7 (moyenne des 7 derniers jours)

Fix v2 : gestion des doublons de dates (multi-mission DAHITI) —
  tous les measurement_id d'une même date reçoivent les mêmes attributs.
═══════════════════════════════════════════════════════════════════════════
"""

import logging
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd

log = logging.getLogger("step3d")


def compute_rolling_features(era5_df: pd.DataFrame, measure_dates: list[str]) -> list[dict]:
    """
    Calcule toutes les features pour une station à partir de son ERA5 quotidien.

    Args:
        era5_df: DataFrame avec colonnes [date, temp_moy_bv, precip_sum_bv, pet_sum_bv, ...]
        measure_dates: Liste des dates de mesure uniques (YYYY-MM-DD)

    Returns:
        Liste de dicts, un par date de mesure, avec toutes les features
    """
    if era5_df.empty:
        return []

    df = era5_df.copy()
    df = df.sort_values("date").set_index("date")

    # Pré-calculer les rolling means sur toute la série
    df["precip_J3"]    = df["precip_sum_bv"].rolling(3,  min_periods=1).mean()
    df["pet_J3"]       = df["pet_sum_bv"].rolling(3,     min_periods=1).mean()
    df["temp_J3"]      = df["temp_moy_bv"].rolling(3,    min_periods=1).mean()
    df["precip_J10"]   = df["precip_sum_bv"].rolling(10, min_periods=1).mean()
    df["temp_J10"]     = df["temp_moy_bv"].rolling(10,   min_periods=1).mean()
    df["precip_J27"]   = df["precip_sum_bv"].rolling(27, min_periods=1).mean()
    df["precip_last7"] = df["precip_sum_bv"].rolling(7,  min_periods=1).mean()
    df["precip_max27"] = df["precip_sum_bv"].rolling(27, min_periods=1).max()
    df["doy"]          = df.index.dayofyear

    results = []
    for date_str in measure_dates:
        date = pd.Timestamp(date_str)
        if date not in df.index:
            continue

        row = df.loc[date]

        # Climatologie fenêtrée ±20j (leave-one-year-out)
        doy     = date.dayofyear
        year    = date.year
        doy_min = doy - 20
        doy_max = doy + 20

        if doy_min < 1:
            mask_doy = (df["doy"] >= (365 + doy_min)) | (df["doy"] <= doy_max)
        elif doy_max > 365:
            mask_doy = (df["doy"] >= doy_min) | (df["doy"] <= (doy_max - 365))
        else:
            mask_doy = (df["doy"] >= doy_min) & (df["doy"] <= doy_max)

        mask_year  = df.index.year != year
        clim_data  = df.loc[mask_doy & mask_year, "precip_sum_bv"]
        clim_mean  = float(clim_data.mean()) if len(clim_data) >= 3 else 0.0
        clim_std   = float(clim_data.std())  if len(clim_data) >= 3 else 1.0

        features = {
            "precipitation_J0": _round(row.get("precip_sum_bv")),
            "temperature_J0"  : _round(row.get("temp_moy_bv")),
            "pet_J0"          : _round(row.get("pet_sum_bv")),
            "precip_mean_J3"  : _round(row.get("precip_J3")),
            "pet_mean_J3"     : _round(row.get("pet_J3")),
            "temp_mean_J3"    : _round(row.get("temp_J3")),
            "precip_mean_J10" : _round(row.get("precip_J10")),
            "temp_mean_J10"   : _round(row.get("temp_J10")),
            "precip_mean_J27" : _round(row.get("precip_J27")),
            "clim_mean_20j"   : _round(clim_mean),
            "clim_std_20j"    : _round(clim_std),
            "precip_max_J27"  : _round(row.get("precip_max27")),
            "precip_last7"    : _round(row.get("precip_last7")),
        }

        results.append({"date": date_str, **features})

    return results


def _round(val, decimals=4):
    """Round avec gestion des NaN."""
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return None
    return round(float(val), decimals)


def run_step3d(conn: sqlite3.Connection) -> dict:
    """
    Étape 3d : calcule les features et remplit measure_attributes.

    Fix v2 : gère les doublons de dates (multi-mission) — tous les
    measurement_id d'une même date reçoivent les mêmes attributs ERA5.

    Returns:
        {"stations": n, "measures_filled": n, "errors": n}
    """
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from Pipeline_data.Database.db_operations import (
        get_all_station_codes, get_era5_bv_jour,
        get_measurement_dates, insert_measure_attributes,
    )

    station_codes = get_all_station_codes(conn)
    log.info(f"{len(station_codes)} stations à traiter")

    total_filled = 0
    errors       = 0

    for sta_idx, code in enumerate(station_codes):
        try:
            # ERA5 quotidien
            era5_df = get_era5_bv_jour(conn, code)
            if era5_df.empty:
                continue

            # Dates de mesure uniques (pour le calcul des features)
            measure_dates = get_measurement_dates(conn, code)
            if not measure_dates:
                continue

            # Calculer les features une fois par date unique
            features_list    = compute_rolling_features(era5_df, measure_dates)
            features_by_date = {f["date"]: f for f in features_list}

            # Récupérer TOUS les measurement_id sans attributs
            # (gère les doublons multi-mission : plusieurs IDs par date)
            rows = conn.execute("""
                SELECT m.measurement_id, m.measure_date
                FROM measurements m
                LEFT JOIN measure_attributes a ON m.measurement_id = a.measurement_id
                WHERE m.station_code = ?
                  AND a.measurement_id IS NULL
                ORDER BY m.measure_date
            """, (code,)).fetchall()

            for measurement_id, date_str in rows:
                feat = features_by_date.get(date_str)
                if feat is None:
                    continue
                f = {k: v for k, v in feat.items() if k != "date"}
                insert_measure_attributes(conn, measurement_id, code, date_str, f)
                total_filled += 1

        except Exception as e:
            log.error(f"  {code} — ERREUR : {e}")
            errors += 1

        if (sta_idx + 1) % 10 == 0:
            log.info(f"  {sta_idx+1}/{len(station_codes)} stations "
                     f"({total_filled} mesures remplies)")

    log.info(f"Étape 3d terminée : {total_filled} mesures remplies, {errors} erreurs")
    return {"stations": len(station_codes), "measures_filled": total_filled, "errors": errors}


if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")

    parser = argparse.ArgumentParser(description="Étape 3d — Calcul features")
    parser.add_argument("--db", type=str, default="./data/test.db")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    run_step3d(conn)
    conn.close()