"""
generate_sigma0_config.py
─────────────────────────
Génère un CSV de config avec une ligne par station :
  station_code, satellite, pass_number, lat, lon, first_cycle, last_cycle

À lancer dans ton venv normal.
Produit : ./sigma0_stations_config.csv
"""

import sqlite3
import pandas as pd

HYDRO_DB_PATH = "./data/hydro_data.db"
STATIONS_FILE = "./AI/LSTM/NeuralHydro_satellite_27D/stations_27j.txt"
OUT_CSV       = "./sigma0_stations_config.csv"

CYCLE_RANGES = {
    'S3A': (7,  136),
    'S3B': (20, 117),
    'J3':  (1,  226),
    'S6A': (1,  80),
    'ENV': (1,  100),
}

with open(STATIONS_FILE) as f:
    stations = [l.strip() for l in f if l.strip()]

conn = sqlite3.connect(HYDRO_DB_PATH)
df = pd.read_sql_query("""
    SELECT station_code, mission_track, reference_latitude, reference_longitude
    FROM stations WHERE station_code IN ({})
""".format(','.join(f'"{s}"' for s in stations)), conn)
conn.close()

df['satellite']   = df['mission_track'].str.split('-').str[0]
df['pass_number'] = df['mission_track'].str.split('-').str[1].astype(int)
df['first_cycle'] = df['satellite'].map(lambda s: CYCLE_RANGES.get(s, (1,100))[0])
df['last_cycle']  = df['satellite'].map(lambda s: CYCLE_RANGES.get(s, (1,100))[1])

df = df.rename(columns={
    'reference_latitude':  'lat',
    'reference_longitude': 'lon',
})
df[['station_code','satellite','pass_number','lat','lon',
    'first_cycle','last_cycle']].to_csv(OUT_CSV, index=False)

print(f"✅ {len(df)} stations → {OUT_CSV}")
print(df['satellite'].value_counts().to_string())