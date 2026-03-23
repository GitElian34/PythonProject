import os
import sqlite3
import geopandas as gpd


from Exploring_data.get_dataF import analyse_point, analyse_point_fast
from db_insitu import create_insitu_db, insert_era5, station_era5_complete
import xarray as xr
import pandas as pd
from itertools import groupby

cache_nc = {}

DB_PATH = "./data/insitu_data.db"
GPKG_PATH = "./data/insitu/shp/station_schapi_alti_ref_2025_river.gpkg"

gdf_insitu = gpd.read_file(GPKG_PATH)


def est_en_france_metropolitaine(lat, lon):
    return 40 <= lat <= 52 and -6 <= lon <= 10


def analyse_station_mois(lat, lon, ds):
    """Extrait toutes les données du mois en une seule opération pour un point"""
    ds['t2m_c'] = ds['t2m'] - 273.15
    ds['tp_mm'] = ds['tp'] * 1000

    # Extraction du point une seule fois pour tout le mois
    temp_point = ds['t2m_c'].sel(latitude=lat, longitude=lon, method='nearest')
    precip_point = ds['tp_mm'].sel(latitude=lat, longitude=lon, method='nearest')

    # Resample par jour en une seule opération
    temp_daily = temp_point.resample(valid_time='1D')
    precip_daily = precip_point.resample(valid_time='1D')

    return {
        'temp_min': temp_daily.min().to_pandas(),
        'temp_max': temp_daily.max().to_pandas(),
        'temp_moy': temp_daily.mean().to_pandas(),
        'precip': precip_daily.mean().to_pandas()
    }


def get_dataset(annee, mois):
    key = f"{annee}/{mois}"
    if key not in cache_nc:
        fichier = f'/home/sar_hydro/STUDIES/EtudesEB/PythonProject/data/ERA5/usable_data_LAND_France/{annee}/{mois}/data_0.nc'
        if not os.path.exists(fichier):
            return None
        cache_nc[key] = xr.open_dataset(fichier)
    return cache_nc[key]

def charger_era5_insitu(db_path=DB_PATH, skip=0, station_unique=None):
    create_insitu_db(db_path)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute('SELECT code_sta FROM stations_insitu')
    stations = [row[0] for row in cursor.fetchall()]
    if station_unique:
        stations = [station_unique]
    for i, code_sta in enumerate(stations[skip:], start=skip):
        print(f"\n🔄 [{i + 1}/{len(stations)}] Station {code_sta}")

        if station_era5_complete(conn, code_sta):
            print(f"  ⏭️  Déjà traitée, skip")
            continue

        row = gdf_insitu[gdf_insitu['code_sta'] == code_sta]
        if row.empty:
            continue
        lat, lon = row.iloc[0]['lat'], row.iloc[0]['lon']

        if not est_en_france_metropolitaine(lat, lon):
            continue

        cursor.execute('SELECT DISTINCT date FROM mesures_insitu WHERE code_sta = ? ORDER BY date', (code_sta,))
        dates = [r[0] for r in cursor.fetchall()]

        # Grouper par mois
        from itertools import groupby
        dates_par_mois = {}
        for d in dates:
            annee, mois, _ = d.split('-')
            key = (annee, mois)
            dates_par_mois.setdefault(key, []).append(d)

        rows = []
        for (annee, mois), dates_mois in dates_par_mois.items():
            ds = get_dataset(annee, mois)
            if ds is None:
                continue

            # Extraire toutes les données du mois en une fois
            stats_mois = analyse_station_mois(lat, lon, ds)

            # Dataset mois précédent pour les 10j
            date_debut = pd.Timestamp(f"{annee}-{mois}-01") - pd.Timedelta(days=10)
            mois_prec = str(date_debut.month).zfill(2)
            annee_prec = str(date_debut.year)
            dsy = get_dataset(annee_prec, mois_prec) if mois_prec != mois else None
            stats_mois_prec = analyse_station_mois(lat, lon, dsy) if dsy else None

            for d in dates_mois:
                try:
                    date_ts = pd.Timestamp(d)
                    date_debut_10j = date_ts - pd.Timedelta(days=10)

                    temp_min = stats_mois['temp_min'].get(d)
                    temp_max = stats_mois['temp_max'].get(d)
                    temp_moy = stats_mois['temp_moy'].get(d)
                    precip = stats_mois['precip'].get(d)

                    # Moyenne 10j en combinant les deux mois si nécessaire
                    dates_10j = pd.date_range(date_debut_10j, date_ts, freq='D')
                    temp_10j_vals = []
                    precip_10j_vals = []
                    for dd in dates_10j:
                        dd_str = str(dd.date())
                        dd_mois = str(dd.month).zfill(2)
                        if dd_mois == mois:
                            t = stats_mois['temp_moy'].get(dd_str)
                            p = stats_mois['precip'].get(dd_str)
                        elif stats_mois_prec:
                            t = stats_mois_prec['temp_moy'].get(dd_str)
                            p = stats_mois_prec['precip'].get(dd_str)
                        else:
                            continue
                        if t is not None: temp_10j_vals.append(t)
                        if p is not None: precip_10j_vals.append(p)

                    temp_moy10j = sum(temp_10j_vals) / len(temp_10j_vals) if temp_10j_vals else None
                    precip_moy10j = sum(precip_10j_vals) / len(precip_10j_vals) if precip_10j_vals else None

                    if any(v is None for v in [temp_min, temp_max, temp_moy, precip]):
                        continue

                    rows.append((code_sta, d,
                                 float(temp_min), float(temp_max), float(temp_moy),
                                 float(precip), float(temp_moy10j) if temp_moy10j else None,
                                 float(precip_moy10j) if precip_moy10j else None))
                except Exception as e:
                    print(f"  ⚠️  {d}: {e}")

        # Insert en batch
        cursor.executemany('''
            INSERT OR IGNORE INTO era5_insitu
                (code_sta, date, temp_min_jour, temp_max_jour, temp_moy_jour,
                 precip_jour, temp_moy_10j, precip_moy_10j)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', rows)
        conn.commit()
        print(f"  ✅ {len(rows)} dates insérées")

    conn.close()
    print("\n✅ Terminé")


if __name__ == "__main__":
    charger_era5_insitu()