"""
select_stations_27j_high.py
═══════════════════════════════════════════════════════════════════════════
Sélectionne les stations avec elevation_mean > 500m pour entraîner un
modèle spécialisé montagne.

Usage : python select_stations_27j_high.py
═══════════════════════════════════════════════════════════════════════════
"""
import sqlite3
import random
from pathlib import Path

DB_PATH    = "./data/insitu_data.db"
BASINS_DIR = Path("./AI/LSTM/NeuralHydro_feat27j_high/")
ELEV_MIN   = 1    # seuil altitude en mètres
SEED       = 42
FREQ       = 27

random.seed(SEED)

# ═══════════════════════════════════════════════════════════════
# 1. Récupérer les stations éligibles avec altitude > 500m
# ═══════════════════════════════════════════════════════════════
conn = sqlite3.connect(DB_PATH)
cursor = conn.execute("""
    SELECT s.code_sta, s.elevation_mean
    FROM stations_insitu s
    WHERE s.flag_capteur IS NULL
      AND (s.gap_max_jours IS NULL OR s.gap_max_jours <= 200)
      AND (s.dist_barrage_m IS NULL OR s.dist_barrage_m >= 500)
      AND s.lon IS NOT NULL
      AND s.lat IS NOT NULL
      AND s.elevation_mean IS NOT NULL
      AND s.elevation_mean >= ?
""", (ELEV_MIN,))
results = cursor.fetchall()
conn.close()

eligible = [row[0] for row in results]
elevations = {row[0]: row[1] for row in results}
print(f"Stations éligibles (elev >= {ELEV_MIN}m) : {len(eligible)}")

# Vérifier que les .nc existent
ts_dir = Path("./data/IA/NeuralHydrology_feat27j/time_series/")
eligible = [s for s in eligible if (ts_dir / f"{s}_d0.nc").exists()]
print(f"Avec .nc disponible : {len(eligible)}")

if len(eligible) == 0:
    print("❌ Aucune station trouvée !")
    exit(1)

# Stats
elevs = [elevations[s] for s in eligible if s in elevations]
print(f"Elevation : min={min(elevs):.0f}m, max={max(elevs):.0f}m, médiane={sorted(elevs)[len(elevs)//2]:.0f}m")

# ═══════════════════════════════════════════════════════════════
# 2. Split 80/20
# ═══════════════════════════════════════════════════════════════
random.shuffle(eligible)
n_val   = max(1, int(len(eligible) * 0.2))
n_train = len(eligible) - n_val
train_bases = eligible[:n_train]
val_bases   = eligible[n_train:]

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