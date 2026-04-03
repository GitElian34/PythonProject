import os
import sqlite3
import pandas as pd
import numpy as np
import xarray as xr
from datetime import datetime, timedelta

DB_PATH   = './data/insitu_data.db'
ERA5_BASE = './data/ERA5/usable_data_LAND_France'
TRANCHES  = [(0, 40), (40, 80), (80, 150), (150, 300), (300, None)]
NB_JOURS  = 10

def tranche_label(dist_km):
    for debut, fin in TRANCHES:
        if fin is None:
            return '>300km'
        if debut <= dist_km < fin:
            return f'{debut}-{fin}km'
    return '>300km'


def creer_table_pluie(conn):
    conn.execute('''
        CREATE TABLE IF NOT EXISTS era5_pluie_bv (
            pluie_id    INTEGER PRIMARY KEY AUTOINCREMENT,
            mesure_id   INTEGER NOT NULL,
            code_sta    TEXT NOT NULL,
            mesure_date DATE NOT NULL,
            tranche_km  TEXT NOT NULL,
            J0 DECIMAL(8,3), J1 DECIMAL(8,3), J2 DECIMAL(8,3),
            J3 DECIMAL(8,3), J4 DECIMAL(8,3), J5 DECIMAL(8,3),
            J6 DECIMAL(8,3), J7 DECIMAL(8,3), J8 DECIMAL(8,3),
            J9 DECIMAL(8,3),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (mesure_id) REFERENCES mesures_insitu(id),
            FOREIGN KEY (code_sta)  REFERENCES stations_insitu(code_sta)
        )
    ''')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_pluie_mesure ON era5_pluie_bv(mesure_id)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_pluie_station_date ON era5_pluie_bv(code_sta, mesure_date)')
    conn.commit()
    print("Table era5_pluie_bv prête !")


def precharger_era5_mois(era5_base, annee, mois):
    path = f'{era5_base}/{annee}/{mois}/data_0.nc'
    if not os.path.exists(path):
        return None
    ds = xr.open_dataset(path)
    tp = ds['tp'] * 1000
    tp_daily = tp.resample(valid_time='1D').last()
    return tp_daily


def calculer_pluie_par_tranche(pixels, mesure_date, cache_mensuel, era5_base):
    dt_ref       = datetime.strptime(mesure_date, '%Y-%m-%d')
    pixels_lons  = pixels['pixel_lon'].values
    pixels_lats  = pixels['pixel_lat'].values
    tranches_pix = pixels['tranche_km'].values

    valeurs_jours = []

    for j in range(NB_JOURS):
        date_j   = dt_ref - timedelta(days=j)
        annee    = date_j.strftime('%Y')
        mois     = date_j.strftime('%m')
        date_str = str(date_j.date())
        cle_mois = f'{annee}/{mois}'

        if cle_mois not in cache_mensuel:
            cache_mensuel[cle_mois] = precharger_era5_mois(era5_base, annee, mois)

        tp_daily = cache_mensuel[cle_mois]
        if tp_daily is None:
            valeurs_jours.append(np.zeros(len(pixels)))
            continue

        try:
            tp_jour = tp_daily.sel(valid_time=date_str)
            lons_xr = xr.DataArray(pixels_lons, dims='pixel')
            lats_xr = xr.DataArray(pixels_lats, dims='pixel')
            vals    = tp_jour.sel(
                longitude=lons_xr,
                latitude=lats_xr,
                method='nearest'
            ).values
            valeurs_jours.append(vals)
        except Exception:
            valeurs_jours.append(np.zeros(len(pixels)))

    valeurs_array = np.array(valeurs_jours)

    labels_uniques = [f'{d}-{f}km' for d, f in TRANCHES if f is not None] + ['>300km']
    resultats = {}

    for tranche in labels_uniques:
        mask = tranches_pix == tranche
        if not mask.any():
            resultats[tranche] = [0.0] * NB_JOURS
        else:
            resultats[tranche] = [
                round(float(np.nansum(valeurs_array[j, mask])), 3)
                for j in range(NB_JOURS)
            ]

    return resultats


def inserer_pluie(conn, mesure_id, code_sta, mesure_date, resultats):
    conn.execute('DELETE FROM era5_pluie_bv WHERE mesure_id = ?', (mesure_id,))
    for tranche, jours in resultats.items():
        conn.execute('''
            INSERT INTO era5_pluie_bv (
                mesure_id, code_sta, mesure_date, tranche_km,
                J0, J1, J2, J3, J4, J5, J6, J7, J8, J9
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (mesure_id, code_sta, mesure_date, tranche, *jours))
    conn.commit()


def main():
    conn = sqlite3.connect(DB_PATH)
    creer_table_pluie(conn)

    stations = pd.read_sql('''
        SELECT e.code_sta, COUNT(*) as nb_pixels
        FROM era5_transfert e
        JOIN bv_data b ON e.code_sta = b.code_sta
        GROUP BY e.code_sta
        HAVING COUNT(*) >= 50
          AND e.code_sta NOT IN (SELECT DISTINCT code_sta FROM era5_pluie_bv)
        LIMIT 100
    ''', conn)
    print(f"Stations à traiter : {len(stations)}")

    cache_mensuel = {}

    for _, row in stations.iterrows():
        code_sta  = row['code_sta']
        nb_pixels = row['nb_pixels']
        print(f"\nStation : {code_sta} ({nb_pixels} pixels ERA5)")

        # ── Pixels ERA5 depuis la DB (dist_km corrigée) ──
        pixels = pd.read_sql('''
            SELECT pixel_lon, pixel_lat, dist_km
            FROM era5_transfert WHERE code_sta = ?
        ''', conn, params=(code_sta,))

        pixels['tranche_km'] = pixels['dist_km'].apply(tranche_label)

        print(f"  Distribution tranches :")
        print(pixels.groupby('tranche_km').size().rename('nb').to_string())

        mesures = pd.read_sql('''
            SELECT id as mesure_id, date as mesure_date
            FROM mesures_insitu WHERE code_sta = ?
            ORDER BY date
        ''', conn, params=(code_sta,))
        print(f"  Mesures à traiter : {len(mesures)}")

        total = len(mesures)
        for i, meas in mesures.iterrows():
            mesure_id   = meas['mesure_id']
            mesure_date = meas['mesure_date'][:10]

            if i % 100 == 0:
                print(f"  [{i+1}/{total}] {mesure_date}...")

            try:
                resultats = calculer_pluie_par_tranche(
                    pixels, mesure_date, cache_mensuel, ERA5_BASE
                )
                inserer_pluie(conn, mesure_id, code_sta, mesure_date, resultats)
            except Exception as e:
                print(f"    → ERREUR {mesure_date} : {e}")

    nb = pd.read_sql("SELECT COUNT(*) as n FROM era5_pluie_bv", conn).iloc[0]['n']
    print(f"\nTerminé ! {nb} lignes dans era5_pluie_bv")
    conn.close()


if __name__ == '__main__':
    main()