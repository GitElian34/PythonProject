"""
extract_snow_bv_insitu.py
═══════════════════════════════════════════════════════════════════════════
Extrait snow_depth et snowmelt depuis ERA5-Land et les agrège sur le BV
de chaque station insitu, pour chaque date de mesure.

Inspiré de extract_era5_bv_insitu.py — mais pour la neige.
═══════════════════════════════════════════════════════════════════════════
"""

import os
import sqlite3
import pandas as pd
import numpy as np
import xarray as xr
from multiprocessing import Pool, cpu_count

from data_processing.insitu.db_insitu import (
    creer_table_era5_snow_bv_jour,
    inserer_era5_snow_bv_jour,
)

DB_PATH    = './data/insitu_data.db'
ERA5_BASE  = './data/ERA5/usable_data_LAND_France/Snow'
N_WORKERS  = 30

# ═══════════════════════════════════════════════════════════════
# CONNEXION SQLITE AVEC WAL ET TIMEOUT
# ═══════════════════════════════════════════════════════════════
def get_conn():
    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


# ═══════════════════════════════════════════════════════════════
# CHARGEMENT ERA5 SNOW
# ═══════════════════════════════════════════════════════════════
def precharger_snow_mois(era5_base, annee, mois):
    """
    Charge le fichier snow ERA5 mensuel.
    - sde / sd : snow_depth en mètres (instantané, valeur à 23:00)
    - smlt     : snowmelt en m d'équivalent eau (cumul 0-23h, valeur à 23:00)

    Comme on a téléchargé seulement à 23:00, le fichier contient déjà
    une valeur par jour.
    """
    path = f'{era5_base}/{annee}/{mois}/data_0.nc'
    if not os.path.exists(path):
        return None
    ds = xr.open_dataset(path)

    # Identifier les variables (noms peuvent varier)
    var_sd   = next((v for v in ds.data_vars if v in ('sde', 'sd', 'snow_depth')), None)
    var_smlt = next((v for v in ds.data_vars if v in ('smlt', 'snowmelt')), None)

    if var_sd is None or var_smlt is None:
        print(f"  ⚠️  Variables manquantes dans {path} : {list(ds.data_vars)}")
        return None

    return {
        'sd':   ds[var_sd]   * 1000,   # m → mm
        'smlt': ds[var_smlt] * 1000,   # m → mm
    }


def extraire_pixels_snow_mois(data_mois, pixels):
    """Extraction des valeurs aux pixels du BV, moyennées par jour."""
    lons = xr.DataArray(pixels['pixel_lon'].values, dims='pixel')
    lats = xr.DataArray(pixels['pixel_lat'].values, dims='pixel')

    sd_bv   = data_mois['sd'].sel(longitude=lons, latitude=lats, method='nearest').values
    smlt_bv = data_mois['smlt'].sel(longitude=lons, latitude=lats, method='nearest').values
    dates   = data_mois['sd'].valid_time.values

    resultats = {}
    for i, t in enumerate(dates):
        date_str = str(t)[:10]
        resultats[date_str] = (
            round(float(np.nanmean(sd_bv[i])),   4),
            round(float(np.nanmean(smlt_bv[i])), 4),
        )
    return resultats


# ═══════════════════════════════════════════════════════════════
# TRAITEMENT D'UNE STATION
# ═══════════════════════════════════════════════════════════════
def traiter_station(args):
    code_sta, nb_pixels = args
    conn = get_conn()

    try:
        derniere_date = conn.execute(
            'SELECT MAX(mesure_date) FROM era5_snow_bv_jour WHERE code_sta = ?',
            (code_sta,)
        ).fetchone()[0]

        date_debut = derniere_date if derniere_date else '2015-12-31'

        mesures = pd.read_sql('''
            SELECT id as mesure_id, date as mesure_date
            FROM mesures_insitu WHERE code_sta = ?
            AND date > ? AND date <= '2025-12-31'
            ORDER BY date
        ''', conn, params=(code_sta, date_debut))

        if len(mesures) == 0:
            return code_sta, 0, 0, f"déjà à jour jusqu'à {derniere_date}"

        pixels = pd.read_sql('''
            SELECT pixel_lon, pixel_lat
            FROM era5_transfert WHERE code_sta = ?
        ''', conn, params=(code_sta,))

        cache_mois = {}
        ok, ko = 0, 0
        batch = []

        mesures['annee_mois'] = mesures['mesure_date'].str[:7]

        for annee_mois, groupe in mesures.groupby('annee_mois'):
            annee, mois = annee_mois.split('-')
            cle = f'{annee}/{mois}'

            if cle not in cache_mois:
                data = precharger_snow_mois(ERA5_BASE, annee, mois)
                if data is None:
                    ko += len(groupe)
                    continue
                cache_mois[cle] = extraire_pixels_snow_mois(data, pixels)

            resultats_mois = cache_mois[cle]

            for _, meas in groupe.iterrows():
                date_str  = meas['mesure_date'][:10]
                mesure_id = meas['mesure_id']

                if date_str in resultats_mois:
                    sd, smlt = resultats_mois[date_str]
                    batch.append((mesure_id, code_sta, date_str,
                                  sd, smlt, nb_pixels))
                    ok += 1
                else:
                    ko += 1

        if batch:
            with conn:
                conn.executemany('''
                    INSERT OR IGNORE INTO era5_snow_bv_jour
                    (mesure_id, code_sta, mesure_date,
                     snow_depth_bv, snowmelt_bv, nb_pixels)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', batch)

        return code_sta, ok, ko, f"✅ {ok} jours insérés | ⚠️ {ko} sans données"

    except Exception as e:
        return code_sta, 0, 0, f"❌ ERREUR : {e}"

    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════
def main():
    conn = get_conn()
    creer_table_era5_snow_bv_jour(conn)
    conn.close()

    conn = get_conn()
    stations = pd.read_sql('''
        SELECT e.code_sta, COUNT(*) as nb_pixels
        FROM era5_transfert e
        JOIN bv_data b ON e.code_sta = b.code_sta
        JOIN stations_insitu s ON e.code_sta = s.code_sta
        GROUP BY e.code_sta
        HAVING COUNT(*) >= 1
        ORDER BY RANDOM()
    ''', conn)
    conn.close()

    print(f"Stations à traiter : {len(stations)}")
    print(f"CPUs utilisés      : {N_WORKERS} / {cpu_count()} disponibles")

    args = [(row['code_sta'], row['nb_pixels']) for _, row in stations.iterrows()]

    with Pool(processes=N_WORKERS) as pool:
        for code_sta, ok, ko, message in pool.imap_unordered(traiter_station, args):
            print(f"  {code_sta} — {message}")

    conn = get_conn()
    nb = pd.read_sql("SELECT COUNT(*) as n FROM era5_snow_bv_jour", conn).iloc[0]['n']
    print(f"\n✅ Terminé ! {nb} lignes dans era5_snow_bv_jour")
    conn.close()


if __name__ == '__main__':
    main()