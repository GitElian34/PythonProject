import os
import numpy as np
import pandas as pd
import sqlite3
import xarray as xr
from shapely.wkt import loads
from shapely.geometry import Point
from datetime import datetime, timedelta
from rasterio.windows import from_bounds
import rasterio

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
DB_PATH     = './data/hydro_data.db'
ERA5_BASE   = './data/ERA5/usable_data_LAND_France'
NOM_STATION = 'R_GARONNE_GARONNE_KM0084'
HEURE_REF   = 12
FENETRES    = [(0, 12), (12, 36), (36, 60), (60, 84), (84, 108)]

# ═══════════════════════════════════════════════════════════════
# FONCTIONS
# ═══════════════════════════════════════════════════════════════

def creer_table_cumuls(conn):
    """Crée la table era5_cumuls_bv — une ligne par measurement."""
    conn.execute('''
        CREATE TABLE IF NOT EXISTS era5_cumuls_bv (
            cumul_id       INTEGER PRIMARY KEY AUTOINCREMENT,
            measurement_id INTEGER NOT NULL,
            station_code   TEXT NOT NULL,
            measure_date   DATE NOT NULL,

            -- Méthode A (vitesse constante 1 m/s)
            A_0_12h   DECIMAL(8,3),
            A_12_36h  DECIMAL(8,3),
            A_36_60h  DECIMAL(8,3),
            A_60_84h  DECIMAL(8,3),
            A_84_108h DECIMAL(8,3),
            A_sup108h DECIMAL(8,3),

            -- Méthode B (vitesse variable)
            B_0_12h   DECIMAL(8,3),
            B_12_36h  DECIMAL(8,3),
            B_36_60h  DECIMAL(8,3),
            B_60_84h  DECIMAL(8,3),
            B_84_108h DECIMAL(8,3),
            B_sup108h DECIMAL(8,3),

            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (measurement_id) REFERENCES measurements(measurement_id),
            FOREIGN KEY (station_code)   REFERENCES stations(station_code)
        )
    ''')
    conn.commit()
    print("Table era5_cumuls_bv prête !")


def charger_era5_jour(era5_base, date):
    """Charge le cumul journalier ERA5 (dernière heure = cumul total)."""
    annee, mois = date[:4], date[5:7]
    ds = xr.open_dataset(f'{era5_base}/{annee}/{mois}/data_0.nc')
    return ds['tp'].sel(valid_time=date).isel(valid_time=-1) * 1000


def determiner_jours(temps_h, date_ref, heure_ref):
    """Retourne les jours ERA5 à prendre selon la fenêtre temporelle."""
    dt_ref = datetime.strptime(f'{date_ref} {heure_ref:02d}:00', '%Y-%m-%d %H:%M')

    for i, (debut, fin) in enumerate(FENETRES):
        if debut <= temps_h < fin:
            if i == 0:
                return [str(dt_ref.date())]
            j1 = dt_ref.date() - timedelta(days=i - 1)
            j2 = dt_ref.date() - timedelta(days=i)
            return [str(j1), str(j2)]

    n  = len(FENETRES)
    j1 = dt_ref.date() - timedelta(days=n - 1)
    j2 = dt_ref.date() - timedelta(days=n)
    return [str(j1), str(j2)]


def fenetre_col(temps_h, methode):
    """Retourne le nom de colonne BDD pour une fenêtre et une méthode."""
    for debut, fin in FENETRES:
        if debut <= temps_h < fin:
            return f'{methode}_{debut}_{fin}h'
    return f'{methode}_sup108h'


def calculer_cumuls(pixels_era5, date_ref, heure_ref, era5_base, cache_era5):
    """
    Pour chaque pixel ERA5, calcule le cumul adapté pour méthode A et B.
    Retourne un dict avec les cumuls moyens par fenêtre et méthode.
    """
    # Accumulateurs par colonne
    buckets = {
        f'{m}_{d}_{f}h': [] for m in ['A', 'B'] for d, f in FENETRES
    }
    buckets['A_sup108h'] = []
    buckets['B_sup108h'] = []

    for _, pixel in pixels_era5.iterrows():
        lo, la = pixel['pixel_lon'], pixel['pixel_lat']

        for methode, col_temps in [('A', 'temps_A_h'), ('B', 'temps_B_h')]:
            temps_h = pixel[col_temps]
            jours   = determiner_jours(temps_h, date_ref, heure_ref)
            col     = fenetre_col(temps_h, methode)

            cumul = 0.0
            for jour in jours:
                try:
                    if jour not in cache_era5:
                        cache_era5[jour] = charger_era5_jour(era5_base, jour)
                    cumul += float(cache_era5[jour].sel(
                        latitude=la, longitude=lo, method='nearest'
                    ).values)
                except Exception:
                    pass

            buckets[col].append(cumul)

    # Moyenne par fenêtre
    return {col: round(np.sum(vals), 3) if vals else None
            for col, vals in buckets.items()}


def inserer_cumuls(conn, measurement_id, station_code, measure_date, cumuls):
    """Insère ou met à jour les cumuls pour un measurement."""
    conn.execute(
        'DELETE FROM era5_cumuls_bv WHERE measurement_id = ?',
        (measurement_id,)
    )
    conn.execute('''
        INSERT INTO era5_cumuls_bv (
            measurement_id, station_code, measure_date,
            A_0_12h,  A_12_36h, A_36_60h, A_60_84h, A_84_108h, A_sup108h,
            B_0_12h,  B_12_36h, B_36_60h, B_60_84h, B_84_108h, B_sup108h
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        measurement_id, station_code, measure_date,
        cumuls.get('A_0_12h'),  cumuls.get('A_12_36h'),
        cumuls.get('A_36_60h'), cumuls.get('A_60_84h'),
        cumuls.get('A_84_108h'),cumuls.get('A_sup108h'),
        cumuls.get('B_0_12h'),  cumuls.get('B_12_36h'),
        cumuls.get('B_36_60h'), cumuls.get('B_60_84h'),
        cumuls.get('B_84_108h'),cumuls.get('B_sup108h'),
    ))
    conn.commit()


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════
def main():
    conn = sqlite3.connect(DB_PATH)
    creer_table_cumuls(conn)

    # ── 1. Récupérer la station et son BV ──
    station = pd.read_sql('''
        SELECT s.station_code, s.hydroweb_name
        FROM stations s
        JOIN bv_data b ON s.station_code = b.station_code
        WHERE s.hydroweb_name = ?
    ''', conn, params=(NOM_STATION,))

    if station.empty:
        print(f"❌ Station {NOM_STATION} non trouvée ou sans BV")
        return

    station_code = station.iloc[0]['station_code']
    print(f"Station : {NOM_STATION} ({station_code})")

    # ── 2. Récupérer les pixels ERA5 avec temps de transfert ──
    pixels_era5 = pd.read_sql('''
        SELECT pixel_lon, pixel_lat, temps_A_h, temps_B_h
        FROM era5_transfert
        WHERE station_code = ?
    ''', conn, params=(station_code,))

    if pixels_era5.empty:
        print("❌ Aucun pixel ERA5 — lance d'abord compute_transfert.py")
        return
    print(f"Pixels ERA5 : {len(pixels_era5)}")

    # ── 3. Récupérer les measurements ──
    measurements = pd.read_sql('''
        SELECT measurement_id, measure_date, measure_time
        FROM measurements
        WHERE station_code = ?
        ORDER BY measure_date
    ''', conn, params=(station_code,))
    print(f"Measurements à traiter : {len(measurements)}")

    # ── 4. Boucle sur les measurements ──
    cache_era5 = {}
    total      = len(measurements)

    for i, meas in measurements.iterrows():
        measurement_id = meas['measurement_id']
        measure_date   = meas['measure_date'][:10]

        try:
            heure = int(meas['measure_time'][:2]) if meas['measure_time'] else HEURE_REF
        except Exception:
            heure = HEURE_REF

        print(f"[{i+1}/{total}] {measure_date} {heure}h...")

        # Vérifier que ERA5 existe pour cette date
        annee, mois = measure_date[:4], measure_date[5:7]
        if not os.path.exists(f'{ERA5_BASE}/{annee}/{mois}/data_0.nc'):
            print(f"  → ERA5 manquant")
            continue

        try:
            cumuls = calculer_cumuls(
                pixels_era5, measure_date, heure, ERA5_BASE, cache_era5
            )
            inserer_cumuls(conn, measurement_id, station_code, measure_date, cumuls)
            print(f"  → OK")
        except Exception as e:
            print(f"  → ERREUR : {e}")

    # ── 5. Vérification ──
    total_cumuls = pd.read_sql(
        "SELECT COUNT(*) as total FROM era5_cumuls_bv WHERE station_code = ?",
        conn, params=(station_code,)
    )
    print(f"\nTerminé ! {total_cumuls['total'].iloc[0]} lignes dans era5_cumuls_bv")
    conn.close()


if __name__ == '__main__':
    main()