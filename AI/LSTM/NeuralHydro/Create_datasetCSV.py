import sqlite3
import pandas as pd
import random
import os

DB_PATH    = './data/insitu_data.db'
OUTPUT_DIR = './data/IA/NeuralHydrology/'
BASINS_DIR = './AI/LSTM/NeuralHydro/'
DATE_DEB   = '2016-01-01'
DATE_FIN   = '2025-12-31'
N_STATIONS    = 200
VAL_RATIO     = 0.2
SEED          = 42
MIN_GAP_JOURS = 60
MIN_DIST_M    = 500   # distance minimale au barrage le plus proche

os.makedirs(BASINS_DIR, exist_ok=True)

random.seed(SEED)
conn = sqlite3.connect(DB_PATH)

# ═══════════════════════════════════════════════════════════════
# SÉLECTION — stations avec .nc + filtres qualité + distance barrage
# ═══════════════════════════════════════════════════════════════
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

# Garder uniquement ceux qui ont un .nc
nc_existants = {f.replace('.nc', '') for f in os.listdir(os.path.join(OUTPUT_DIR, 'time_series'))
                if f.endswith('.nc')}
candidats = candidats[candidats['code_sta'].isin(nc_existants)].reset_index(drop=True)
print(f"{len(candidats)} candidats avec .nc disponible")

# Tirage aléatoire
candidats = candidats.sample(frac=1, random_state=SEED).reset_index(drop=True)
stations_ok = candidats['code_sta'].tolist()[:N_STATIONS]
print(f"{len(stations_ok)} stations sélectionnées\n")

# ═══════════════════════════════════════════════════════════════
# TRAIN / VAL SPLIT — 80/20
# ═══════════════════════════════════════════════════════════════
random.shuffle(stations_ok)
n_val        = max(1, int(len(stations_ok) * VAL_RATIO))
n_train      = len(stations_ok) - n_val
train_basins = stations_ok[:n_train]
val_basins   = stations_ok[n_train:]

with open(os.path.join(BASINS_DIR, 'train_basins.txt'), 'w') as f:
    f.write('\n'.join(train_basins))

with open(os.path.join(BASINS_DIR, 'val_basins.txt'), 'w') as f:
    f.write('\n'.join(val_basins))

print(f"✅ train_basins.txt — {len(train_basins)} stations")
print(f"✅ val_basins.txt   — {len(val_basins)} stations")
print(f"\nFiltres appliqués :")
print(f"  dist_barrage_m >= {MIN_DIST_M} m")
print(f"  gap_max_jours  <= {MIN_GAP_JOURS} j")
print(f"  .nc existant   : oui")