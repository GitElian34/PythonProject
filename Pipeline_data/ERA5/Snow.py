#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
═══════════════════════════════════════════════════════════════════════════
step3c_era5_snow.py — ERA5 neige quotidien sur les BV
═══════════════════════════════════════════════════════════════════════════

Extrait snow_depth et snowmelt depuis ERA5-Land (dossier Snow) et
complète les colonnes snow_depth_bv et snowmelt_bv dans era5_bv_jour.

Structure attendue :
    {ERA5_BASE}/Snow/{annee}/{mois}/data_0.nc
    Variables : sde/sd (snow depth m), smlt (snowmelt m)

Prérequis : pip install xarray netCDF4 numpy
═══════════════════════════════════════════════════════════════════════════
"""

import logging
import os
import sqlite3
from pathlib import Path

import numpy as np
import xarray as xr

log = logging.getLogger("step3c")

DEFAULT_ERA5_BASE = "./data/ERA5/usable_data_LAND_France"
YEARS = range(2016, 2026)
MONTHS = [f"{m:02d}" for m in range(1, 13)]


def load_snow_month(era5_base: str, year: int, month: str) -> dict | None:
    """
    Charge le fichier snow ERA5-Land mensuel.
    - sde/sd : snow_depth en m (instantané à 23h) → mm
    - smlt   : snowmelt en m eq. eau (cumul à 23h) → mm
    """
    path = f"{era5_base}/Snow/{year}/{month}/data_0.nc"
    if not os.path.exists(path):
        return None

    ds = xr.open_dataset(path)

    var_sd = next((v for v in ds.data_vars if v in ("sde", "sd", "snow_depth")), None)
    var_smlt = next((v for v in ds.data_vars if v in ("smlt", "snowmelt")), None)

    if var_sd is None or var_smlt is None:
        log.warning(f"Variables snow manquantes dans {path}: {list(ds.data_vars)}")
        ds.close()
        return None

    data = {
        "sd": ds[var_sd] * 1000,    # m → mm
        "smlt": ds[var_smlt] * 1000,  # m → mm
    }
    ds.close()
    return data


def extract_snow_bv_means(data_month: dict, pixels: list[tuple]) -> dict:
    """
    Extrait les moyennes spatiales snow sur les pixels du BV.

    Returns:
        {date_str: (snow_depth_mm, snowmelt_mm)}
    """
    lons = xr.DataArray([p[0] for p in pixels], dims="pixel")
    lats = xr.DataArray([p[1] for p in pixels], dims="pixel")

    sd_bv = data_month["sd"].sel(longitude=lons, latitude=lats, method="nearest").values
    smlt_bv = data_month["smlt"].sel(longitude=lons, latitude=lats, method="nearest").values
    dates = data_month["sd"].valid_time.values

    results = {}
    for i, t in enumerate(dates):
        date_str = str(t)[:10]
        results[date_str] = (
            round(float(np.nanmean(sd_bv[i])), 4),
            round(float(np.nanmean(smlt_bv[i])), 4),
        )
    return results


def run_step3c(conn: sqlite3.Connection,
               era5_base: str = DEFAULT_ERA5_BASE) -> dict:
    """
    Étape 3c : complète les colonnes snow dans era5_bv_jour.

    Returns:
        {"stations": n, "days_updated": n, "errors": n}
    """
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from Pipeline_data.Database.db_operations import get_era5_pixels, update_era5_snow

    # Stations avec pixels ERA5
    stations = conn.execute("""
        SELECT station_code, COUNT(*) as nb_pixels
        FROM era5_transfert
        GROUP BY station_code
    """).fetchall()

    log.info(f"{len(stations)} stations à traiter pour la neige")

    total_updated = 0
    errors = 0

    for sta_idx, (station_code, nb_pixels) in enumerate(stations):
        pixels = get_era5_pixels(conn, station_code)

        # Dates sans neige pour cette station
        dates_missing = conn.execute("""
            SELECT date FROM era5_bv_jour
            WHERE station_code = ? AND snow_depth_bv IS NULL
            ORDER BY date
        """, (station_code,)).fetchall()

        if not dates_missing:
            continue

        dates_set = {r[0] for r in dates_missing}
        cache_month = {}
        updated = 0

        for year in YEARS:
            for month in MONTHS:
                month_key = f"{year}/{month}"

                if month_key not in cache_month:
                    data = load_snow_month(era5_base, year, month)
                    cache_month[month_key] = data

                data = cache_month[month_key]
                if data is None:
                    continue

                try:
                    results = extract_snow_bv_means(data, pixels)
                except Exception as e:
                    errors += 1
                    continue

                for date_str, (sd, smlt) in results.items():
                    if date_str in dates_set:
                        update_era5_snow(conn, station_code, date_str, sd, smlt)
                        updated += 1

            cache_month.clear()

        conn.commit()
        total_updated += updated

        if (sta_idx + 1) % 10 == 0:
            log.info(f"  {sta_idx+1}/{len(stations)} stations "
                     f"({total_updated} jours mis à jour)")

    log.info(f"Étape 3c terminée : {total_updated} jours avec neige, {errors} erreurs")
    return {"stations": len(stations), "days_updated": total_updated, "errors": errors}


if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    parser = argparse.ArgumentParser(description="Étape 3c — ERA5 neige")
    parser.add_argument("--db", type=str, default="./data/test.db")
    parser.add_argument("--era5", type=str, default=DEFAULT_ERA5_BASE)
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    run_step3c(conn, args.era5)
    conn.close()