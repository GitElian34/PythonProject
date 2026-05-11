"""
analyse_stations_27j.py
═══════════════════════════════════════════════════════════════════════════
Scan les 222 stations satellite, identifie celles à fréquence ~27j,
génère un PNG par station et la liste stations_27j.txt.

Critère : mode des intervalles entre mesures dans [25, 29] jours (27 ± 2j)

Produit :
  - ./figures_zeroshot_satellite/stations27j/<station>.png
  - ./figures_zeroshot_satellite/stations27j/distribution_intervalles_27j.png
  - ./figures_zeroshot_satellite/stations27j/recapitulatif_stations_27j.png
  - ./Exploring_data/Stations_27j/stations_27j.txt
  - ./Exploring_data/Stations_27j/synthese_stations_27j.csv

Usage :
  python analyse_stations_27j.py
═══════════════════════════════════════════════════════════════════════════
"""

import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from pathlib import Path
from collections import Counter

# ─── Paramètres ─────────────────────────────────────────────────────────────
DB_PATH    = './data/hydro_data.db'
FIG_DIR    = Path('./figures_zeroshot_satellite/stations27j/')
LIST_DIR   = Path('./Exploring_data/Stations_27j/')
TARGET_FREQ = 27    # fréquence cible en jours
TOLERANCE   = 2     # ± tolérance pour le mode

FIG_DIR.mkdir(parents=True, exist_ok=True)
LIST_DIR.mkdir(parents=True, exist_ok=True)

conn = sqlite3.connect(DB_PATH)

# ═══════════════════════════════════════════════════════════════
# 1. Scanner toutes les stations
# ═══════════════════════════════════════════════════════════════
print("=" * 70)
print("ANALYSE DES STATIONS SATELLITE — DÉTECTION FRÉQUENCE ~27j")
print("=" * 70)

stations = pd.read_sql('''
    SELECT station_code, hydroweb_name, river_name, basin_name,
           reference_longitude AS lon, reference_latitude AS lat,
           upstream_watershed_km2 AS aire_km2, mean_altitude, strahler
    FROM stations
    ORDER BY station_code
''', conn)

print(f"\n  {len(stations)} stations dans la BDD\n")

# ═══════════════════════════════════════════════════════════════
# 2. Calculer le mode des intervalles par station
# ═══════════════════════════════════════════════════════════════
results = []

for _, sta in stations.iterrows():
    code = sta['station_code']

    df_mes = pd.read_sql('''
        SELECT measure_date, orthometric_height
        FROM measurements
        WHERE station_code = ? AND is_valid = 1
              AND orthometric_height IS NOT NULL
        ORDER BY measure_date
    ''', conn, params=(code,))

    if len(df_mes) < 10:
        continue

    df_mes['measure_date'] = pd.to_datetime(df_mes['measure_date'])
    df_mes = df_mes.sort_values('measure_date').drop_duplicates('measure_date')

    # Intervalles
    intervals = df_mes['measure_date'].diff().dt.days.dropna().values
    if len(intervals) < 5:
        continue

    # Mode des intervalles
    counter = Counter(intervals.astype(int))
    mode_interval = counter.most_common(1)[0][0]

    # Stats
    wl = df_mes['orthometric_height'].values
    wl_range = np.nanmax(wl) - np.nanmin(wl)
    wl_std = np.nanstd(wl)

    # Couverture temporelle
    date_min = df_mes['measure_date'].min()
    date_max = df_mes['measure_date'].max()
    span_days = (date_max - date_min).days
    expected_n = span_days / mode_interval if mode_interval > 0 else 0
    couverture = len(df_mes) / expected_n * 100 if expected_n > 0 else 0

    # Gaps
    gap_max = np.max(intervals) if len(intervals) > 0 else 0
    nb_gaps_60j = np.sum(intervals > 60)

    results.append({
        'station_code': code,
        'hydroweb_name': sta['hydroweb_name'],
        'river_name': sta['river_name'],
        'lon': sta['lon'],
        'lat': sta['lat'],
        'aire_km2': sta['aire_km2'],
        'mean_altitude': sta['mean_altitude'],
        'strahler': sta['strahler'],
        'n_mesures': len(df_mes),
        'mode_interval': mode_interval,
        'interval_mean': np.mean(intervals),
        'interval_std': np.std(intervals),
        'interval_max': gap_max,
        'nb_gaps_60j': nb_gaps_60j,
        'date_min': date_min,
        'date_max': date_max,
        'couverture_pct': round(couverture, 1),
        'wl_range': round(wl_range, 2),
        'wl_std': round(wl_std, 2),
    })

df_all = pd.DataFrame(results)
print(f"  {len(df_all)} stations analysées")

# Distribution des modes
print(f"\n  Distribution des modes d'intervalle :")
for mode, count in sorted(Counter(df_all['mode_interval']).items()):
    marker = " ◄── 27j" if abs(mode - TARGET_FREQ) <= TOLERANCE else ""
    print(f"    {mode:3d}j : {count:3d} stations{marker}")

# ═══════════════════════════════════════════════════════════════
# 3. Filtrer les stations ~27j
# ═══════════════════════════════════════════════════════════════
mask_27j = df_all['mode_interval'].between(TARGET_FREQ - TOLERANCE, TARGET_FREQ + TOLERANCE)
df_27j = df_all[mask_27j].copy().reset_index(drop=True)

print(f"\n  {len(df_27j)} stations retenues (mode {TARGET_FREQ}±{TOLERANCE}j)")

# ═══════════════════════════════════════════════════════════════
# 4. Stats des stations 27j
# ═══════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print(f"PROFIL DES {len(df_27j)} STATIONS ~27j")
print(f"{'='*70}")
print(f"  Couverture médiane  : {df_27j['couverture_pct'].median():.1f}%")
print(f"  Mesures médianes    : {df_27j['n_mesures'].median():.0f}")
print(f"  Gap max médian      : {df_27j['interval_max'].median():.0f}j")
print(f"  Gap max global      : {df_27j['interval_max'].max():.0f}j")
print(f"  WL range médian     : {df_27j['wl_range'].median():.2f}m")
print(f"  Nb gaps >60j médian : {df_27j['nb_gaps_60j'].median():.0f}")

# ═══════════════════════════════════════════════════════════════
# 5. PNG par station
# ═══════════════════════════════════════════════════════════════
print(f"\nGénération des figures...\n")

for idx, row in df_27j.iterrows():
    code = row['station_code']

    df_mes = pd.read_sql('''
        SELECT measure_date, orthometric_height
        FROM measurements
        WHERE station_code = ? AND is_valid = 1
              AND orthometric_height IS NOT NULL
        ORDER BY measure_date
    ''', conn, params=(code,))

    df_mes['measure_date'] = pd.to_datetime(df_mes['measure_date'])
    df_mes = df_mes.sort_values('measure_date').drop_duplicates('measure_date')

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 6), gridspec_kw={'height_ratios': [2, 1]})

    # Série temporelle
    ax1.plot(df_mes['measure_date'], df_mes['orthometric_height'],
             '-o', color='steelblue', markersize=2, linewidth=0.8)
    ax1.set_title(f"{code}  —  {row['river_name']}  |  {row['n_mesures']} mes  |  "
                  f"couv {row['couverture_pct']:.0f}%  |  range {row['wl_range']:.1f}m",
                  fontsize=10, fontweight='bold')
    ax1.set_ylabel('Hauteur (m)')
    ax1.grid(True, alpha=0.3)
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    ax1.xaxis.set_major_locator(mdates.YearLocator())

    # Cycle annuel
    doy = df_mes['measure_date'].dt.dayofyear
    wl_norm = (df_mes['orthometric_height'] - df_mes['orthometric_height'].mean()) / df_mes['orthometric_height'].std()
    ax2.scatter(doy, wl_norm, s=8, alpha=0.5, color='steelblue')
    ax2.set_xlabel('Jour de l\'année')
    ax2.set_ylabel('WL normalisé')
    ax2.set_xlim(1, 365)
    ax2.grid(True, alpha=0.3)
    ax2.axhline(y=0, color='grey', linewidth=0.5, linestyle='--')

    plt.tight_layout()
    fig.savefig(FIG_DIR / f"{code}.png", dpi=120, bbox_inches='tight')
    plt.close()

    print(f"  [{idx+1:3d}/{len(df_27j)}] {code:>15s} | {row['river_name']:>20s} | "
          f"{row['n_mesures']:3d} mes | couv {row['couverture_pct']:5.1f}%")

# ═══════════════════════════════════════════════════════════════
# 6. Figure récap : distribution des intervalles
# ═══════════════════════════════════════════════════════════════
fig, ax = plt.subplots(figsize=(10, 5))
all_intervals = []
for code in df_27j['station_code']:
    df_tmp = pd.read_sql('''
        SELECT measure_date FROM measurements
        WHERE station_code = ? AND is_valid = 1 AND orthometric_height IS NOT NULL
        ORDER BY measure_date
    ''', conn, params=(code,))
    df_tmp['measure_date'] = pd.to_datetime(df_tmp['measure_date'])
    ints = df_tmp['measure_date'].diff().dt.days.dropna().values
    all_intervals.extend(ints)

all_intervals = np.array(all_intervals)
ax.hist(all_intervals, bins=range(0, 120), color='steelblue', edgecolor='white', alpha=0.8)
ax.axvline(x=27, color='red', linewidth=2, linestyle='--', label='27j')
ax.set_xlabel('Intervalle (jours)')
ax.set_ylabel('Count')
ax.set_title(f'Distribution des intervalles — {len(df_27j)} stations ~27j', fontweight='bold')
ax.legend()
ax.grid(True, alpha=0.2)
ax.set_xlim(0, 100)

plt.tight_layout()
fig.savefig(FIG_DIR / 'distribution_intervalles_27j.png', dpi=150, bbox_inches='tight')
plt.close()

pct_in_range = np.sum((all_intervals >= 25) & (all_intervals <= 29)) / len(all_intervals) * 100
print(f"\n  {pct_in_range:.1f}% des intervalles dans [25-29]j")

# ═══════════════════════════════════════════════════════════════
# 7. Sauvegarder la liste et la synthèse
# ═══════════════════════════════════════════════════════════════
# Liste des stations
stations_list = sorted(df_27j['station_code'].tolist())
with open(LIST_DIR / 'stations_27j.txt', 'w') as f:
    f.write('\n'.join(stations_list))
print(f"\n✅ {LIST_DIR / 'stations_27j.txt'} — {len(stations_list)} stations")

# Synthèse CSV
df_27j.to_csv(LIST_DIR / 'synthese_stations_27j.csv', index=False)
print(f"✅ {LIST_DIR / 'synthese_stations_27j.csv'}")

# ═══════════════════════════════════════════════════════════════
# 8. Résumé
# ═══════════════════════════════════════════════════════════════
print(f"""
{'='*60}
RÉSUMÉ
{'='*60}
  Stations ~27j trouvées : {len(df_27j)}
  Figures dans           : {FIG_DIR}
  Liste dans             : {LIST_DIR / 'stations_27j.txt'}
  Synthèse dans          : {LIST_DIR / 'synthese_stations_27j.csv'}

  → Inspecte visuellement les PNG pour identifier les stations inexploitables
  → Puis crée le dataset avec create_dataset_satellite_27D.py
""")

conn.close()