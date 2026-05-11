import sqlite3
import pandas as pd
import random
import os

DB_PATH    = './data/insitu_data.db'
OUTPUT_DIR = './data/IA/NeuralHydrology_feat10j/'
BASINS_DIR = './AI/LSTM/NeuralHydro_feat10j/'
DATE_DEB   = '2016-01-01'
DATE_FIN   = '2025-12-31'

N_STATIONS    = 800   # stations de base (avant décalages)
VAL_RATIO     = 0.2
SEED          = 42
MIN_GAP_JOURS = 60
MIN_DIST_M    = 500

FREQ_JOURS    = 10

os.makedirs(BASINS_DIR, exist_ok=True)
random.seed(SEED)

conn = sqlite3.connect(DB_PATH)

print(f"Sélection des candidats (dist_barrage >= {MIN_DIST_M}m, gap <= {MIN_GAP_JOURS}j)...\n")

candidats = pd.read_sql('''
    SELECT DISTINCT e.code_sta
    FROM era5_bv_jour e
    JOIN mesures_insitu m  ON e.mesure_id = m.id
    JOIN bv_data b         ON e.code_sta = b.code_sta
    JOIN bv_corine c       ON e.code_sta = c.code_sta
    JOIN stations_insitu s ON e.code_sta = s.code_sta
    WHERE e.mesure_date >= ? AND e.mesure_date <= ?
      AND s.lon IS NOT NULL AND s.lat IS NOT NULL
      AND (s.gap_max_jours IS NULL OR s.gap_max_jours <= ?)
      AND (s.dist_barrage_m IS NULL OR s.dist_barrage_m >= ?)
    GROUP BY e.code_sta
    HAVING COUNT(DISTINCT e.mesure_date) >= 1000
       AND MIN(e.mesure_date) <= ?
       AND MAX(e.mesure_date) >= ?
''', conn, params=(DATE_DEB, DATE_FIN, MIN_GAP_JOURS, MIN_DIST_M, DATE_DEB, DATE_FIN))
conn.close()

print(f"{len(candidats)} candidats trouvés")

# Garder uniquement ceux qui ont un _d0.nc disponible
nc_existants = {
    f.replace('_d0.nc', '')
    for f in os.listdir(os.path.join(OUTPUT_DIR, 'time_series'))
    if f.endswith('_d0.nc')
}
candidats = candidats[candidats['code_sta'].isin(nc_existants)].reset_index(drop=True)
print(f"{len(candidats)} candidats avec .nc disponible")

candidats = candidats.sample(frac=1, random_state=SEED).reset_index(drop=True)
stations_base = candidats['code_sta'].tolist()[:N_STATIONS]
print(f"{len(stations_base)} stations de base sélectionnées\n")

# ── Train / Val split 80/20 au niveau station de base ─────────
# Important : on split AVANT d'étendre aux décalages pour éviter
# qu'une même station soit en train ET en val
random.shuffle(stations_base)
n_val        = max(1, int(len(stations_base) * VAL_RATIO))
n_train      = len(stations_base) - n_val
train_base   = stations_base[:n_train]
val_base     = stations_base[n_train:]

# Vérifier quels décalages existent réellement pour chaque station
set_existants = {
    f.replace('.nc', '')
    for f in os.listdir(os.path.join(OUTPUT_DIR, 'time_series'))
    if f.endswith('.nc')
}

train_ids = [f"{s}_d{d}" for s in train_base
             for d in range(FREQ_JOURS)
             if f"{s}_d{d}" in set_existants]
val_ids   = [f"{s}_d{d}" for s in val_base
             for d in range(FREQ_JOURS)
             if f"{s}_d{d}" in set_existants]

with open(os.path.join(BASINS_DIR, 'train_basins.txt'), 'w') as f:
    f.write('\n'.join(train_ids))
with open(os.path.join(BASINS_DIR, 'val_basins.txt'), 'w') as f:
    f.write('\n'.join(val_ids))

print(f"✅ train_basins.txt — {len(train_ids)} ids ({len(train_base)} stations × {FREQ_JOURS} décalages)")
print(f"✅ val_basins.txt   — {len(val_ids)} ids ({len(val_base)} stations × {FREQ_JOURS} décalages)")
print(f"\nFiltres appliqués :")
print(f"  dist_barrage_m >= {MIN_DIST_M} m")
print(f"  gap_max_jours  <= {MIN_GAP_JOURS} j")
print(f"  .nc existant   : oui")
print(f"  N stations base : {N_STATIONS}")