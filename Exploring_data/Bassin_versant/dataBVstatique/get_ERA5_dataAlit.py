#!/usr/bin/env python3
"""
extract_era5_bv_jour_satellite.py
═══════════════════════════════════════════════════════════════════════════
Extrait ERA5 quotidien (P/T/PET) sur les BV des stations satellite,
pour TOUS les jours de la période 2016-2025 (pas seulement les dates
de mesures satellite).

Inspiré de extract_era5_satellite.py mais :
  - Une ligne par (station, date) au lieu d'une par mesure
  - Couvre toute la période 2016-2025 en continu
  - Permet ensuite de construire des datasets sur grille régulière 10D
═══════════════════════════════════════════════════════════════════════════
"""

import os
import sqlite3
import pandas as pd
import numpy as np
import xarray as xr
from multiprocessing import Pool, cpu_count

from data_processing.db_manager import creer_table_era5_bv_jour

DB_PATH    = './data/hydro_data.db'
ERA5_BASE  = './data/ERA5/usable_data_LAND_France'
N_WORKERS  = 4
DATE_DEB   = '2016-01-01'
DATE_FIN   = '2025-12-31'

# ═══════════════════════════════════════════════════════════════
# CONNEXION
# ═══════════════════════════════════════════════════════════════
def get_conn():
    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


# ═══════════════════════════════════════════════════════════════
# CHARGEMENT ERA5 — identique au script insitu
# ═══════════════════════════════════════════════════════════════
def precharger_era5_mois(era5_base, annee, mois):
    path = f'{era5_base}/{annee}/{mois}/data_0.nc'
    if not os.path.exists(path):
        return None
    ds = xr.open_dataset(path)
    if not all(v in ds.data_vars for v in ['tp', 't2m', 'pev']):
        return None

    h23 = ds.sel(valid_time=ds.valid_time.dt.hour == 23)
    return {
        'tp':  h23['tp']  * 1000,                              # m -> mm
        't2m': (ds['t2m'] - 273.15).resample(valid_time='1D').mean(),  # K -> °C
        'pev': np.abs(h23['pev']) * 1000,                      # m -> mm
    }


def extraire_pixels_mois(data_mois, pixels):
    lons = xr.DataArray(pixels['pixel_lon'].values, dims='pixel')
    lats = xr.DataArray(pixels['pixel_lat'].values, dims='pixel')

    tp_bv  = data_mois['tp'].sel(longitude=lons,  latitude=lats, method='nearest').values
    t2m_bv = data_mois['t2m'].sel(longitude=lons, latitude=lats, method='nearest').values
    pev_bv = data_mois['pev'].sel(longitude=lons, latitude=lats, method='nearest').values
    dates  = data_mois['tp'].valid_time.values

    resultats = {}
    for i, t in enumerate(dates):
        date_str = str(t)[:10]
        resultats[date_str] = (
            round(float(np.nanmean(t2m_bv[i])), 3),
            round(float(np.nanmean(tp_bv[i])),  3),
            round(float(np.nanmean(pev_bv[i])), 3),
        )
    return resultats


# ═══════════════════════════════════════════════════════════════
# TRAITEMENT D'UNE STATION
# ═══════════════════════════════════════════════════════════════
def traiter_station(args):
    station_code, nb_pixels = args
    conn = get_conn()

    try:
        # Reprendre depuis la dernière date traitée
        derniere_date = conn.execute(
            'SELECT MAX(mesure_date) FROM era5_bv_jour WHERE station_code = ?',
            (station_code,)
        ).fetchone()[0]

        date_debut = derniere_date if derniere_date else DATE_DEB

        # Liste de toutes les dates à traiter
        if derniere_date:
            dates_a_traiter = pd.date_range(
                pd.Timestamp(derniere_date) + pd.Timedelta(days=1),
                DATE_FIN,
                freq='D'
            )
        else:
            dates_a_traiter = pd.date_range(DATE_DEB, DATE_FIN, freq='D')

        if len(dates_a_traiter) == 0:
            return station_code, 0, 0, f"déjà à jour jusqu'à {derniere_date}"

        pixels = pd.read_sql('''
            SELECT pixel_lon, pixel_lat
            FROM era5_transfert WHERE station_code = ?
        ''', conn, params=(station_code,))

        if pixels.empty:
            return station_code, 0, 0, "⚠️ pas de pixels ERA5"

        cache_mois = {}
        ok, ko = 0, 0
        batch = []

        # Grouper les dates par mois pour optimiser le cache
        df_dates = pd.DataFrame({'date': dates_a_traiter})
        df_dates['annee_mois'] = df_dates['date'].dt.strftime('%Y/%m')

        for cle, groupe in df_dates.groupby('annee_mois'):
            if cle not in cache_mois:
                annee, mois = cle.split('/')
                data = precharger_era5_mois(ERA5_BASE, annee, mois)
                if data is None:
                    ko += len(groupe)
                    continue
                cache_mois[cle] = extraire_pixels_mois(data, pixels)

            resultats_mois = cache_mois[cle]

            for _, row in groupe.iterrows():
                date_str = row['date'].strftime('%Y-%m-%d')
                if date_str in resultats_mois:
                    temp_moy, precip_sum, pet_sum = resultats_mois[date_str]
                    batch.append((
                        station_code,
                        date_str,
                        temp_moy,
                        precip_sum,
                        pet_sum,
                        nb_pixels
                    ))
                    ok += 1
                else:
                    ko += 1

        if batch:
            with conn:
                conn.executemany('''
                    INSERT OR IGNORE INTO era5_bv_jour
                    (station_code, mesure_date,
                     temp_moy_bv, precip_sum_bv, pet_sum_bv, nb_pixels)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', batch)

        return station_code, ok, ko, f"✅ {ok} jours insérés | ⚠️ {ko} sans données"

    except Exception as e:
        return station_code, 0, 0, f"❌ ERREUR : {e}"

    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════
def main():
    conn = get_conn()
    creer_table_era5_bv_jour(conn)
    conn.close()

    conn = get_conn()
    stations = pd.read_sql('''
        SELECT e.station_code, COUNT(*) as nb_pixels
        FROM era5_transfert e
        JOIN bv_data b    ON e.station_code = b.station_code
        JOIN stations s   ON e.station_code = s.station_code
        GROUP BY e.station_code
        HAVING COUNT(*) >= 1
        ORDER BY RANDOM()
    ''', conn)
    conn.close()

    print(f"Stations à traiter : {len(stations)}")
    print(f"Période            : {DATE_DEB} → {DATE_FIN}")
    print(f"CPUs utilisés      : {N_WORKERS} / {cpu_count()} disponibles")
    print(f"Volume estimé      : ~{len(stations) * 3650} lignes\n")

    args = [(row['station_code'], row['nb_pixels']) for _, row in stations.iterrows()]

    with Pool(processes=N_WORKERS) as pool:
        for station_code, ok, ko, message in pool.imap_unordered(traiter_station, args):
            print(f"  {station_code} — {message}")

    conn = get_conn()
    nb = conn.execute("SELECT COUNT(*) FROM era5_bv_jour").fetchone()[0]
    n_sta = conn.execute("SELECT COUNT(DISTINCT station_code) FROM era5_bv_jour").fetchone()[0]
    conn.close()
    print(f"\n✅ Terminé ! {nb} lignes dans era5_bv_jour ({n_sta} stations)")


if __name__ == '__main__':
    main()