import re
import sqlite3
import glob
import os
import random
import pandas as pd
import geopandas as gpd

from data_processing.insitu.Era5_insitu import charger_era5_insitu
from db_insitu import create_insitu_db, insert_station_insitu, insert_mesure_insitu

GPKG_PATH = "./data/insitu/shp/station_schapi_alti_ref_2025_river.gpkg"
CSV_DIR = "./data/insitu/data"
DB_PATH = "./data/insitu_data.db"

# Charger le GeoPackage une seule fois
gdf_insitu = gpd.read_file(GPKG_PATH)

def extract_station_code(fichier):
    basename = os.path.basename(fichier)
    match = re.match(r'WSH_([A-Z0-9]\d{9})\.csv', basename)
    if match:
        return match.group(1)
    return None
def get_river_name_from_gpkg(station_code):
    row = gdf_insitu[gdf_insitu['code_sta'] == station_code]
    if row.empty:
        return None
    return row.iloc[0]['river_name']





def charger_station(fichier, conn):
    station_code = extract_station_code(fichier)
    if station_code is None:
        print(f"  ⚠️  Nom de fichier non reconnu : {fichier}")
        return

    river_name = get_river_name_from_gpkg(station_code)
    insert_station_insitu(conn, station_code, river_name)

    df = pd.read_csv(fichier)
    df['Date'] = pd.to_datetime(df['Date'], utc=True)
    df['date_only'] = df['Date'].dt.date

    # Grouper par date une seule fois
    grouped = df.groupby('date_only')

    rows = []
    for date, jour in grouped:
        def mesure_proche(heure_cible):
            cible = pd.Timestamp(f"{date} {heure_cible}").tz_localize("UTC")
            diff = abs(jour['Date'] - cible)
            idx = diff.idxmin()
            if diff[idx] > pd.Timedelta(hours=4):
                return None
            return jour.loc[idx, 'WSH']

        h01_wsh = mesure_proche("01:00:00")
        h09_wsh = mesure_proche("09:00:00")
        h17_wsh = mesure_proche("17:00:00")

        if any(v is not None for v in [h01_wsh, h09_wsh, h17_wsh]):
            rows.append((station_code, str(date),
                         h01_wsh, h09_wsh, h17_wsh
                         ))

    # Insert en batch + un seul commit
    cursor = conn.cursor()
    cursor.executemany('''
                       INSERT
                       OR IGNORE INTO mesures_insitu 
            (code_sta, date, h_01h_wsh, h_09h_wsh, h_17h_wsh)
        VALUES (?, ?, ?, ?, ?)
                       ''', rows)
    conn.commit()

    print(f"  ✅ {len(rows)}/{len(grouped)} jours insérés | Rivière: {river_name}")


if __name__ == "__main__":
    create_insitu_db(DB_PATH)
    fichiers = glob.glob(os.path.join(CSV_DIR, "*.csv"))
    print(f"📂 {len(fichiers)} fichiers CSV trouvés")

    fichiers_sample = random.sample(fichiers, 200)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    for i, fichier in enumerate(fichiers_sample):
        station_code = extract_station_code(fichier)
        print(f"\n🔄 [{i + 1}/{len(fichiers_sample)}] Station {station_code}")

        try:
            # Vérifier si la station existe déjà complètement
            cursor.execute('SELECT COUNT(*) FROM mesures_insitu WHERE code_sta = ?', (station_code,))
            nb_mesures = cursor.fetchone()[0]

            if nb_mesures > 0:
                print(f"  ⏭️  Mesures déjà présentes ({nb_mesures} jours), skip insitu")
            else:
                charger_station(fichier, conn)

            # ERA5 a déjà son propre check interne
            charger_era5_insitu(db_path=DB_PATH, station_unique=station_code)

        except Exception as e:
            print(f"  ⚠️  Erreur: {e}")

    conn.close()
    print(f"\n✅ Terminé — base : {DB_PATH}")