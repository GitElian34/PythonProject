import sqlite3
import random
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import os

DB_PATH    = "./data/insitu_data.db"
OUTPUT_DIR = "./data/insitu/visualisation/comparaison_h09_mediane"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def get_stations_rivieres(db_path):
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query("""
        SELECT code_sta FROM stations_insitu
        WHERE dans_lac IS NULL OR dans_lac NOT IN ('dans_lac', 'proche_lac')
    """, conn)
    conn.close()
    return df['code_sta'].tolist()

def get_donnees_station(station_code, db_path):
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query("""
        SELECT date, h_09h_wsh, h_med_wsh
        FROM mesures_insitu
        WHERE code_sta = ?
          AND (h_09h_wsh IS NOT NULL OR h_med_wsh IS NOT NULL)
        ORDER BY date
    """, conn, params=(station_code,))
    conn.close()
    df['date'] = pd.to_datetime(df['date'])
    return df

def plot_station(station_code, df, output_dir):
    fig, ax = plt.subplots(figsize=(14, 4))

    ax.plot(df['date'], df['h_09h_wsh'],
            color='#2196F3', linewidth=0.8, alpha=0.9,
            label='h_09h_wsh (mesure 9h)')

    ax.plot(df['date'], df['h_med_wsh'],
            color='#FF5722', linewidth=0.8, alpha=0.9,
            linestyle='--', label='h_med_wsh (médiane journalière)')

    ax.set_title(f"Station {station_code} — évolution du niveau d'eau",
                 fontsize=11, fontweight='bold')
    ax.set_ylabel("Hauteur d'eau (m)", fontsize=9)
    ax.set_xlabel("Date", fontsize=9)
    ax.legend(fontsize=8)
    ax.grid(alpha=0.3)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
    plt.xticks(rotation=45, ha='right', fontsize=7)
    plt.tight_layout()

    path = os.path.join(output_dir, f"{station_code}.png")
    plt.savefig(path, dpi=130, bbox_inches='tight')
    plt.close()
    print(f"  ✅ {station_code} → {path}")

if __name__ == "__main__":
    stations  = get_stations_rivieres(DB_PATH)
    selection = random.sample(stations, min(10, len(stations)))

    print(f"📊 Stations sélectionnées : {selection}\n")

    for code in selection:
        df = get_donnees_station(code, DB_PATH)
        if len(df) < 10:
            print(f"  ⚠️  {code} — pas assez de données, skip")
            continue
        plot_station(code, df, OUTPUT_DIR)

    print(f"\n✅ 10 plots sauvegardés dans : {OUTPUT_DIR}")