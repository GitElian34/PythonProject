import os
import sqlite3
import pandas as pd
import numpy as np
import xarray as xr
from multiprocessing import Pool, cpu_count

from data_processing.insitu.db_insitu import creer_table_era5_bv_jour, inserer_era5_bv_jour

DB_PATH    = './data/insitu_data.db'
ERA5_BASE  = './data/ERA5/usable_data_LAND_France'
N_WORKERS  = 30

# ═══════════════════════════════════════════════════════════════
# CONNEXION SQLITE AVEC WAL ET TIMEOUT
# ═══════════════════════════════════════════════════════════════
def get_conn():
    """Connexion SQLite avec WAL et timeout pour éviter les locks."""
    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


# ═══════════════════════════════════════════════════════════════
# CHARGEMENT ERA5
# ═══════════════════════════════════════════════════════════════
def precharger_era5_mois(era5_base, annee, mois):
    """
    Charge le fichier ERA5 mensuel.
    - tp et pev : on sélectionne h23 uniquement (accumulation totale du jour à 23h)
    - t2m       : moyenne sur toutes les heures du jour
    """
    path = f'{era5_base}/{annee}/{mois}/data_0.nc'
    if not os.path.exists(path):
        return None
    ds = xr.open_dataset(path)
    if not all(v in ds.data_vars for v in ['tp', 't2m', 'pev']):
        return None

    # Sélection de h23 uniquement — accumulation totale du jour J à 23h
    # Évite le problème de l'accumulateur ERA5 qui repart à 0 à minuit
    h23 = ds.sel(valid_time=ds.valid_time.dt.hour == 23)

    return {
        'tp':  h23['tp']  * 1000,                        # m → mm, h23 = total jour J
        't2m': (ds['t2m'] - 273.15).resample(valid_time='1D').mean(),  # moyenne journalière °C
        'pev': np.abs(h23['pev']) * 1000,                # m → mm, abs car ERA5 retourne négatif
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
    code_sta, nb_pixels = args
    conn = get_conn()

    try:
        derniere_date = conn.execute(
            'SELECT MAX(mesure_date) FROM era5_bv_jour WHERE code_sta = ?',
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
                data = precharger_era5_mois(ERA5_BASE, annee, mois)
                if data is None:
                    ko += len(groupe)
                    continue
                cache_mois[cle] = extraire_pixels_mois(data, pixels)

            resultats_mois = cache_mois[cle]

            for _, meas in groupe.iterrows():
                date_str  = meas['mesure_date'][:10]
                mesure_id = meas['mesure_id']

                if date_str in resultats_mois:
                    temp_moy, precip_moy, pet_moy = resultats_mois[date_str]
                    batch.append((mesure_id, code_sta, date_str,
                                  temp_moy, precip_moy, pet_moy, nb_pixels))
                    ok += 1
                else:
                    ko += 1

        # Écriture en une seule transaction
        if batch:
            with conn:
                conn.executemany('''
                    INSERT OR IGNORE INTO era5_bv_jour
                    (mesure_id, code_sta, mesure_date,
                     temp_moy_bv, precip_sum_bv, pet_sum_bv, nb_pixels)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
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
    creer_table_era5_bv_jour(conn)
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
    nb = pd.read_sql("SELECT COUNT(*) as n FROM era5_bv_jour", conn).iloc[0]['n']
    print(f"\n✅ Terminé ! {nb} lignes dans era5_bv_jour")
    conn.close()


if __name__ == '__main__':
    main()