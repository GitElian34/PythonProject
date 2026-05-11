"""
select_500_stations_27j.py
═══════════════════════════════════════════════════════════════════════════
Sélectionne 500 stations au hasard (non flaggées, gap <= 60j, dist_barrage >= 500m)
et réécrit train_basins.txt et val_basins.txt dans NeuralHydro_feat27j_low/.
Usage : python select_500_stations_27j.py
═══════════════════════════════════════════════════════════════════════════
"""
import sqlite3
import random
from pathlib import Path

DB_PATH    = "./data/insitu_data.db"
BASINS_DIR = Path("./AI/LSTM/NeuralHydro_feat27j_low/")
N_STATIONS = 500
SEED       = 42
FREQ       = 27  # décalages _d0..._d26

random.seed(SEED)

# ═══════════════════════════════════════════════════════════════
# 1. Récupérer les stations éligibles depuis la BDD
# ═══════════════════════════════════════════════════════════════
conn = sqlite3.connect(DB_PATH)
cursor = conn.execute("""
    SELECT s.code_sta
    FROM stations_insitu s
    WHERE s.flag_capteur IS NULL
      AND (s.gap_max_jours IS NULL OR s.gap_max_jours <= 60)
      AND (s.dist_barrage_m IS NULL OR s.dist_barrage_m >= 500)
      AND s.lon IS NOT NULL
      AND s.lat IS NOT NULL
""")
eligible = [row[0] for row in cursor.fetchall()]
conn.close()
print(f"Stations éligibles : {len(eligible)}")

# Vérifier que les .nc existent (dossier 27j)
ts_dir = Path("./data/IA/NeuralHydrology_feat27j/time_series/")
eligible = [s for s in eligible if (ts_dir / f"{s}_d0.nc").exists()]
print(f"Avec .nc disponible : {len(eligible)}")

# ═══════════════════════════════════════════════════════════════
# 2. Tirer 500 au hasard, split 80/20
# ═══════════════════════════════════════════════════════════════
random.shuffle(eligible)
selected = eligible[:N_STATIONS]

n_val   = int(len(selected) * 0.2)
n_train = len(selected) - n_val
train_bases = selected[:n_train]
val_bases   = selected[n_train:]

# Générer les IDs avec décalages
train_ids = [f"{s}_d{d}" for s in train_bases for d in range(FREQ)]
val_ids   = [f"{s}_d{d}" for s in val_bases   for d in range(FREQ)]

# ═══════════════════════════════════════════════════════════════
# 3. Écrire les fichiers
# ═══════════════════════════════════════════════════════════════
BASINS_DIR.mkdir(parents=True, exist_ok=True)

with open(BASINS_DIR / "train_basins.txt", "w") as f:
    f.write("\n".join(train_ids))
with open(BASINS_DIR / "val_basins.txt", "w") as f:
    f.write("\n".join(val_ids))

print(f"\n✅ train_basins.txt : {len(train_ids)} ids ({n_train} stations × {FREQ} décalages)")
print(f"✅ val_basins.txt   : {len(val_ids)} ids ({n_val} stations × {FREQ} décalages)")
print(f"Dossier : {BASINS_DIR}")