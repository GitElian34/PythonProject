#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
═══════════════════════════════════════════════════════════════════════════
step3b_era5_meteo.py — ERA5 météo quotidien sur les BV
═══════════════════════════════════════════════════════════════════════════

Extrait temperature, precipitation et PET depuis les fichiers ERA5-Land
mensuels et les agrège (moyenne spatiale) sur chaque BV pour chaque jour
de 2016-2025. Résultat dans era5_bv_jour.

Structure attendue des fichiers ERA5 :
    {ERA5_BASE}/{annee}/{mois}/data_0.nc
    Variables : tp (precip m), t2m (temp K), pev (PET m, négatif)

Prérequis : pip install xarray netCDF4 numpy pandas
═══════════════════════════════════════════════════════════════════════════
"""

import logging
import os
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr

log = logging.getLogger("step3b")

DEFAULT_ERA5_BASE = "./data/ERA5/usable_data_LAND_France"
YEARS = range(2016, 2026)
MONTHS = [f"{m:02d}" for m in range(1, 13)]


def load_era5_month(era5_base: str, year: int, month: str) -> dict | None:
    """
    Charge un fichier ERA5 mensuel et prépare les données quotidiennes.

    - tp, pev : sélection h23 (cumul journalier)
    - t2m : moyenne journalière
    """
    path = f"{era5_base}/{year}/{month}/data_0.nc"
    if not os.path.exists(path):
        return None

    ds = xr.open_dataset(path)
    if not all(v in ds.data_vars for v in ["tp", "t2m", "pev"]):
        log.warning(f"Variables manquantes dans {path}: {list(ds.data_vars)}")
        ds.close()
        return None

    # h23 = cumul journalier (ERA5 accumulation reset à minuit)
    h23 = ds.sel(valid_time=ds.valid_time.dt.hour == 23)

    data = {
        "tp": h23["tp"] * 1000,  # m → mm
        "t2m": (ds["t2m"] - 273.15).resample(valid_time="1D").mean(),  # K → °C, moy jour
        "pev": np.abs(h23["pev"]) * 1000,  # m → mm, abs car négatif dans ERA5
    }

    ds.close()
    return data


def extract_bv_means(data_month: dict, pixels: list[tuple]) -> dict:
    """
    Extrait les moyennes spatiales sur les pixels du BV pour chaque jour du mois.

    Returns:
        {date_str: (temp_moy, precip_sum, pet_sum)}
    """
    lons = xr.DataArray([p[0] for p in pixels], dims="pixel")
    lats = xr.DataArray([p[1] for p in pixels], dims="pixel")

    tp_bv = data_month["tp"].sel(longitude=lons, latitude=lats, method="nearest").values
    t2m_bv = data_month["t2m"].sel(longitude=lons, latitude=lats, method="nearest").values
    pev_bv = data_month["pev"].sel(longitude=lons, latitude=lats, method="nearest").values

    dates = data_month["tp"].valid_time.values

    results = {}
    for i, t in enumerate(dates):
        date_str = str(t)[:10]
        results[date_str] = (
            round(float(np.nanmean(t2m_bv[i])), 3),
            round(float(np.nanmean(tp_bv[i])), 3),
            round(float(np.nanmean(pev_bv[i])), 3),
        )
    return results

def run_step3b(conn: sqlite3.Connection,
               era5_base: str = DEFAULT_ERA5_BASE) -> dict:
    """
    Étape 3b optimisée : boucle mois → stations (au lieu de stations → mois).
    Chaque .nc est chargé UNE SEULE FOIS pour toutes les stations.
    """
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from Pipeline_data.Database.db_operations import (
        get_era5_pixels, insert_era5_bv_jour_batch,
    )

    # Charger tous les pixels par station en mémoire
    stations = conn.execute("""
        SELECT station_code, COUNT(*) as nb_pixels
        FROM era5_transfert GROUP BY station_code
    """).fetchall()

    log.info(f"{len(stations)} stations avec pixels ERA5")

    # Pré-charger tous les pixels (dict station_code → [(lon, lat), ...])
    all_pixels = {}
    for station_code, nb_pixels in stations:
        all_pixels[station_code] = get_era5_pixels(conn, station_code)

    # Dernière date par station (pour skip)
    last_dates = {}
    for station_code, _ in stations:
        row = conn.execute(
            "SELECT MAX(date) FROM era5_bv_jour WHERE station_code = ?",
            (station_code,)
        ).fetchone()
        last_dates[station_code] = row[0] if row[0] else None

    total_inserted = 0
    errors = 0

    # Boucle MOIS → stations (un seul chargement .nc par mois)
    for year in YEARS:
        for month in MONTHS:
            data = load_era5_month(era5_base, year, month)
            if data is None:
                continue

            batch = []

            for station_code, nb_pixels in stations:
                pixels = all_pixels[station_code]
                last_date = last_dates[station_code]

                try:
                    results = extract_bv_means(data, pixels)
                except Exception as e:
                    errors += 1
                    continue

                for date_str, (temp, precip, pet) in results.items():
                    if last_date and date_str <= last_date:
                        continue
                    batch.append((
                        station_code, date_str,
                        temp, precip, pet,
                        None, None,
                        nb_pixels,
                    ))

            if batch:
                insert_era5_bv_jour_batch(conn, batch)
                total_inserted += len(batch)

            log.info(f"  {year}/{month} — {len(batch)} jours insérés (total: {total_inserted})")

    log.info(f"Étape 3b terminée : {len(stations)} stations, "
             f"{total_inserted} jours insérés, {errors} erreurs")
    return {"stations": len(stations), "days_inserted": total_inserted, "errors": errors}

    log.info(f"Étape 3b terminée : {len(stations)} stations, "
             f"{total_inserted} jours insérés, {errors} erreurs")
    return {"stations": len(stations), "days_inserted": total_inserted, "errors": errors}


if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    parser = argparse.ArgumentParser(description="Étape 3b — ERA5 météo quotidien")
    parser.add_argument("--db", type=str, default="./data/test.db")
    parser.add_argument("--era5", type=str, default=DEFAULT_ERA5_BASE)
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    run_step3b(conn, args.era5)
    conn.close()