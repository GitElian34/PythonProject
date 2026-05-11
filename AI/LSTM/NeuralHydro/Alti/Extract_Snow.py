#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
extract_snow_bv_satellite.py
═══════════════════════════════════════════════════════════════════════════
Extrait snow_depth et snowmelt depuis ERA5-Land et les agrège sur le BV
de chaque station satellite, pour CHAQUE JOUR de 2016-2025.

Comme era5_bv_jour (quotidien), pas seulement aux dates de mesures.
Réutilise le mapping pixels de era5_transfert dans hydro_data.db.

Table créée : era5_snow_bv_jour
  - station_code, mesure_date, snow_depth_bv (mm), snowmelt_bv (mm), nb_pixels

Reprise automatique si interrompu.
═══════════════════════════════════════════════════════════════════════════
"""

import os
import sqlite3
import pandas as pd
import numpy as np
import xarray as xr
from multiprocessing import Pool, cpu_count

DB_PATH    = './data/hydro_data.db'
ERA5_BASE  = './data/ERA5/usable_data_LAND_France/Snow'
N_WORKERS  = 4       # modéré pour satellite (222 stations)
DATE_DEB   = '2016-01-01'
DATE_FIN   = '2025-12-31'


# ═══════════════════════════════════════════════════════════════
# BDD
# ═══════════════════════════════════════════════════════════════
def get_conn():
    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def creer_table_era5_snow_bv_jour(conn):
    """Crée la table era5_snow_bv_jour pour le snow quotidien sur BV satellite."""
    conn.execute('''
        CREATE TABLE IF NOT EXISTS era5_snow_bv_jour (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            station_code    TEXT NOT NULL,
            mesure_date     DATE NOT NULL,
            snow_depth_bv   DECIMAL(8,4),
            snowmelt_bv     DECIMAL(8,4),
            nb_pixels       INTEGER,
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(station_code, mesure_date),
            FOREIGN KEY (station_code) REFERENCES stations(station_code)
        )
    ''')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_snow_bv_jour_sta_date '
                 'ON era5_snow_bv_jour(station_code, mesure_date)')
    conn.commit()
    print("✅ Table era5_snow_bv_jour créée")


# ═══════════════════════════════════════════════════════════════
# CHARGEMENT ERA5 SNOW
# ═══════════════════════════════════════════════════════════════
def precharger_snow_mois(era5_base, annee, mois):
    """Charge le fichier snow ERA5 mensuel."""
    path = f'{era5_base}/{annee}/{mois}/data_0.nc'
    if not os.path.exists(path):
        return None
    ds = xr.open_dataset(path)

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
        # Reprise : dernière date traitée
        derniere_date = conn.execute(
            'SELECT MAX(mesure_date) FROM era5_snow_bv_jour WHERE station_code = ?',
            (code_sta,)
        ).fetchone()[0]

        date_debut = derniere_date if derniere_date else '2015-12-31'

        # Grille quotidienne à traiter
        all_dates = pd.date_range(
            max(pd.Timestamp(date_debut) + pd.Timedelta(days=1), pd.Timestamp(DATE_DEB)),
            DATE_FIN, freq='D'
        )

        if len(all_dates) == 0:
            return code_sta, 0, 0, f"déjà à jour"

        # Pixels du BV
        pixels = pd.read_sql('''
            SELECT pixel_lon, pixel_lat
            FROM era5_transfert WHERE station_code = ?
        ''', conn, params=(code_sta,))

        if pixels.empty:
            return code_sta, 0, 0, "pas de pixels ERA5"

        cache_mois = {}
        ok, ko = 0, 0
        batch = []

        # Grouper par mois
        df_dates = pd.DataFrame({'date': all_dates})
        df_dates['annee'] = df_dates['date'].dt.year.astype(str)
        df_dates['mois']  = df_dates['date'].dt.month.astype(str).str.zfill(2)

        for (annee, mois), groupe in df_dates.groupby(['annee', 'mois']):
            cle = f'{annee}/{mois}'

            if cle not in cache_mois:
                data = precharger_snow_mois(ERA5_BASE, annee, mois)
                if data is None:
                    ko += len(groupe)
                    continue
                cache_mois[cle] = extraire_pixels_snow_mois(data, pixels)

            resultats_mois = cache_mois[cle]

            for _, row in groupe.iterrows():
                date_str = row['date'].strftime('%Y-%m-%d')
                if date_str in resultats_mois:
                    sd, smlt = resultats_mois[date_str]
                    batch.append((code_sta, date_str, sd, smlt, nb_pixels))
                    ok += 1
                else:
                    ko += 1

            # Libérer le cache du mois précédent pour la mémoire
            if len(cache_mois) > 2:
                oldest = list(cache_mois.keys())[0]
                del cache_mois[oldest]

        # Insertion batch
        if batch:
            with conn:
                conn.executemany('''
                    INSERT OR IGNORE INTO era5_snow_bv_jour
                    (station_code, mesure_date, snow_depth_bv, snowmelt_bv, nb_pixels)
                    VALUES (?, ?, ?, ?, ?)
                ''', batch)

        return code_sta, ok, ko, f"✅ {ok} jours insérés"

    except Exception as e:
        return code_sta, 0, 0, f"❌ ERREUR : {e}"

    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════
def main():
    print("=" * 60)
    print("EXTRACTION SNOW ERA5-LAND POUR BV SATELLITE")
    print("=" * 60)

    conn = get_conn()
    creer_table_era5_snow_bv_jour(conn)
    conn.close()

    conn = get_conn()
    stations = pd.read_sql('''
        SELECT e.station_code, COUNT(*) as nb_pixels
        FROM era5_transfert e
        JOIN stations s ON e.station_code = s.station_code
        GROUP BY e.station_code
        HAVING COUNT(*) >= 1
        ORDER BY e.station_code
    ''', conn)
    conn.close()

    print(f"\n  Stations à traiter : {len(stations)}")
    print(f"  Période            : {DATE_DEB} → {DATE_FIN}")
    print(f"  Workers            : {N_WORKERS}")
    print(f"  Jours estimés      : ~{len(stations) * 3650} lignes\n")

    args = [(row['station_code'], row['nb_pixels']) for _, row in stations.iterrows()]

    with Pool(processes=N_WORKERS) as pool:
        for i, (code_sta, ok, ko, message) in enumerate(pool.imap_unordered(traiter_station, args)):
            if (i + 1) % 20 == 0 or ok == 0:
                print(f"  [{i+1}/{len(stations)}] {code_sta} — {message}")

    conn = get_conn()
    nb = conn.execute("SELECT COUNT(*) FROM era5_snow_bv_jour").fetchone()[0]
    nb_sta = conn.execute("SELECT COUNT(DISTINCT station_code) FROM era5_snow_bv_jour").fetchone()[0]
    print(f"\n✅ Terminé ! {nb} lignes pour {nb_sta} stations dans era5_snow_bv_jour")
    conn.close()


if __name__ == '__main__':
    main()