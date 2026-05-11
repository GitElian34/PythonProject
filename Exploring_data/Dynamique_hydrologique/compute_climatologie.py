"""
Calcule la climatologie du water_level normalisé (mean + std par DOY)
pour toutes les stations non flaggées, et insère dans la table climatologie_wl.

Usage : python compute_climatologie_wl.py
"""

import sqlite3
import pandas as pd
import numpy as np
import sys
import time
import torch

torch.set_num_threads(8)
DB_PATH = "./data/insitu_data.db"
MIN_YEARS = 2  # minimum d'années avec donnée pour un DOY donné


def compute_station_climatology(conn, code_sta):
    """
    Calcule la climatologie normalisée pour une station.

    Returns:
        liste de tuples (code_sta, doy, wl_mean, wl_std, n_years) ou None
    """
    df = pd.read_sql_query("""
        SELECT date, h_med_wsh, h_01h_wsh, h_09h_wsh, h_17h_wsh
        FROM mesures_insitu
        WHERE code_sta = ?
        ORDER BY date
    """, conn, params=(code_sta,))

    if len(df) < 365:
        return None

    df['date'] = pd.to_datetime(df['date'])

    # Water level : h_med_wsh si dispo, sinon moyenne des 3
    if df['h_med_wsh'].notna().sum() > len(df) * 0.5:
        df['wl'] = df['h_med_wsh']
    else:
        df['wl'] = df[['h_01h_wsh', 'h_09h_wsh', 'h_17h_wsh']].mean(axis=1)

    df = df.dropna(subset=['wl'])
    if len(df) < 365:
        return None

    # Normalisation z-score
    mu, sigma = df['wl'].mean(), df['wl'].std()
    if sigma < 1e-6:
        return None
    df['wl_norm'] = (df['wl'] - mu) / sigma

    # DOY (on ignore le 366 des années bissextiles → rattaché au 365)
    df['doy'] = df['date'].dt.dayofyear.clip(upper=365)
    df['year'] = df['date'].dt.year

    # Agrégation par DOY
    clim = df.groupby('doy').agg(
        wl_mean=('wl_norm', 'mean'),
        wl_std=('wl_norm', 'std'),
        n_years=('year', 'nunique')
    ).reset_index()

    # Filtrer les DOY avec trop peu d'années
    clim = clim[clim['n_years'] >= MIN_YEARS]

    # Remplir les DOY manquants par interpolation
    full_doy = pd.DataFrame({'doy': range(1, 366)})
    clim = full_doy.merge(clim, on='doy', how='left')
    clim['wl_mean'] = clim['wl_mean'].interpolate(method='linear')
    clim['wl_std'] = clim['wl_std'].interpolate(method='linear')
    clim['n_years'] = clim['n_years'].fillna(0).astype(int)

    # Construire les tuples pour insertion batch
    rows = [
        (code_sta, int(r.doy), float(r.wl_mean), float(r.wl_std), int(r.n_years))
        for _, r in clim.iterrows()
        if pd.notna(r.wl_mean)
    ]

    return rows if len(rows) == 365 else None


def main():
    conn = sqlite3.connect(DB_PATH)

    # Importer les fonctions BDD (à adapter selon ton organisation)
    # from db_insitu import creer_table_climatologie_wl, inserer_climatologie_wl_batch
    # En attendant, on les copie ici directement :
    conn.execute('''
        CREATE TABLE IF NOT EXISTS climatologie_wl (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            code_sta    TEXT NOT NULL,
            doy         INTEGER NOT NULL,
            wl_mean     DECIMAL(8,5),
            wl_std      DECIMAL(8,5),
            n_years     INTEGER,
            created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(code_sta, doy)
        )
    ''')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_clim_wl_sta_doy '
                 'ON climatologie_wl(code_sta, doy)')
    conn.commit()

    # Stations non flaggées
    stations = pd.read_sql_query("""
        SELECT code_sta FROM stations_insitu
        WHERE flag_capteur IS NULL
        AND code_sta IN (
            SELECT DISTINCT code_sta FROM mesures_insitu
            GROUP BY code_sta HAVING COUNT(*) > 365
        )
    """, conn)

    n_total = len(stations)
    n_ok, n_skip = 0, 0
    t0 = time.time()

    print(f"Calcul de la climatologie pour {n_total} stations...\n")

    for i, row in stations.iterrows():
        code = row['code_sta']
        rows = compute_station_climatology(conn, code)

        if rows is None:
            n_skip += 1
            continue

        # Insertion batch
        conn.executemany('''
            INSERT INTO climatologie_wl (code_sta, doy, wl_mean, wl_std, n_years)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(code_sta, doy) DO UPDATE SET
                wl_mean = excluded.wl_mean,
                wl_std  = excluded.wl_std,
                n_years = excluded.n_years
        ''', rows)
        conn.commit()
        n_ok += 1

        if (n_ok + n_skip) % 100 == 0:
            elapsed = time.time() - t0
            print(f"  {n_ok + n_skip}/{n_total}  ({n_ok} OK, {n_skip} skip)  "
                  f"[{elapsed:.0f}s]")

    elapsed = time.time() - t0

    # Vérification
    count = conn.execute("SELECT COUNT(DISTINCT code_sta) FROM climatologie_wl").fetchone()[0]
    total_rows = conn.execute("SELECT COUNT(*) FROM climatologie_wl").fetchone()[0]

    conn.close()

    print(f"\n{'='*60}")
    print(f"Terminé en {elapsed:.0f}s")
    print(f"  Stations traitées : {n_ok}")
    print(f"  Stations skippées : {n_skip}")
    print(f"  Total en BDD      : {count} stations × 365 = {total_rows} lignes")


if __name__ == "__main__":
    main()