

"""
Cycle hydrologique annuel moyen (water_level normalisé) pour 10 stations aléatoires.
Sauvegarde 10 PNG dans ./Exploring_data/Dynamique_hydrologique/stations/

Usage : python plot_dynamique_hydro.py
"""

import sqlite3
import os
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd

DB_PATH = "./data/insitu_data.db"
OUT_DIR = "./Exploring_data/Dynamique_hydrologique/stations"
N_STATIONS = 10

os.makedirs(OUT_DIR, exist_ok=True)

conn = sqlite3.connect(DB_PATH)

# --- 1) Récupérer 10 stations au hasard (non flaggées, avec assez de données) ---
stations = pd.read_sql_query("""
    SELECT code_sta FROM stations_insitu
    WHERE flag_capteur IS NULL
    AND code_sta IN (
        SELECT DISTINCT code_sta FROM mesures_insitu
        GROUP BY code_sta HAVING COUNT(*) > 365
    )
    ORDER BY RANDOM()
    LIMIT ?
""", conn, params=(N_STATIONS,))

print(f"Stations sélectionnées : {stations['code_sta'].tolist()}")

for _, row in stations.iterrows():
    code = row['code_sta']

    # --- 2) Récupérer le water level ---
    df = pd.read_sql_query("""
        SELECT date, h_med_wsh, h_01h_wsh, h_09h_wsh, h_17h_wsh
        FROM mesures_insitu
        WHERE code_sta = ?
        ORDER BY date
    """, conn, params=(code,))

    df['date'] = pd.to_datetime(df['date'])

    # Utiliser h_med_wsh si dispo, sinon moyenne des 3
    if df['h_med_wsh'].notna().sum() > 100:
        df['wl'] = df['h_med_wsh']
    else:
        df['wl'] = df[['h_01h_wsh', 'h_09h_wsh', 'h_17h_wsh']].mean(axis=1)

    df = df.dropna(subset=['wl'])
    if len(df) < 365:
        print(f"  ⚠️  {code} — pas assez de données ({len(df)} pts), skip")
        continue

    # --- 3) Normalisation (z-score) ---
    mu, sigma = df['wl'].mean(), df['wl'].std()
    if sigma < 1e-6:
        print(f"  ⚠️  {code} — signal plat, skip")
        continue
    df['wl_norm'] = (df['wl'] - mu) / sigma

    # --- 4) Jour de l'année + moyenne interannuelle ---
    df['doy'] = df['date'].dt.dayofyear
    cycle = df.groupby('doy')['wl_norm'].agg(['mean', 'std']).reset_index()
    cycle.columns = ['doy', 'mean', 'std']

    # Lissage rolling 7j
    cycle['mean_smooth'] = cycle['mean'].rolling(7, center=True, min_periods=1).mean()
    cycle['std_smooth'] = cycle['std'].rolling(7, center=True, min_periods=1).mean()

    # --- 5) Plot ---
    n_years = df['date'].dt.year.nunique()

    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(cycle['doy'], cycle['mean_smooth'], color='steelblue', lw=1.5)
    ax.fill_between(cycle['doy'],
                    cycle['mean_smooth'] - cycle['std_smooth'],
                    cycle['mean_smooth'] + cycle['std_smooth'],
                    alpha=0.2, color='steelblue', label='±1 std')
    ax.axhline(0, color='gray', ls='--', lw=0.5)
    ax.set_xlabel("Jour de l'année")
    ax.set_ylabel("Niveau d'eau normalisé (z-score)")
    ax.set_title(f"Cycle hydrologique annuel moyen — {code}  ({n_years} années)")
    ax.set_xlim(1, 366)
    ax.legend(loc='upper right', fontsize=8)
    ax.grid(True, alpha=0.3)

    # Mois en x
    mois = ['Jan', 'Fév', 'Mar', 'Avr', 'Mai', 'Jun',
            'Jul', 'Aoû', 'Sep', 'Oct', 'Nov', 'Déc']
    ticks = [1, 32, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335]
    ax.set_xticks(ticks)
    ax.set_xticklabels(mois)

    out_path = os.path.join(OUT_DIR, f"{code}_cycle_annuel.png")
    fig.savefig(out_path, dpi=150, bbox_inches='tight')
    plt.close(fig)
    print(f"  ✅ {out_path}")

conn.close()
print(f"\nTerminé — {N_STATIONS} fichiers dans {OUT_DIR}")