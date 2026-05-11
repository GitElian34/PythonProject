"""
Filtre simple : garde les 100 premières stations pour un test rapide
après le shift des dates.

Crée un dossier NeuralHydro_feat10j_test/ avec les .txt limités.
Le yaml pointe vers ce dossier en gardant le même data_dir.
"""

import random
from pathlib import Path

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
BASINS_DIR_OLD = Path("./AI/LSTM/NeuralHydro_feat10j/")
BASINS_DIR_NEW = Path("./AI/LSTM/NeuralHydro_feat10j_test/")
N_TRAIN        = 80    # 80 stations en train
N_VAL          = 20    # 20 stations en val
SEED           = 42

BASINS_DIR_NEW.mkdir(parents=True, exist_ok=True)
random.seed(SEED)

# ═══════════════════════════════════════════════════════════════
# Lire les fichiers existants et extraire les stations de base
# ═══════════════════════════════════════════════════════════════
def load_unique_stations(path):
    with open(path) as f:
        ids = [l.strip() for l in f if l.strip()]
    # Garder une seule occurence par station de base
    return sorted(set(i.rsplit("_d", 1)[0] for i in ids)), ids

train_stations, _ = load_unique_stations(BASINS_DIR_OLD / "train_basins.txt")
val_stations,   _ = load_unique_stations(BASINS_DIR_OLD / "val_basins.txt")

print(f"Train disponibles : {len(train_stations)} stations")
print(f"Val disponibles   : {len(val_stations)} stations")

# ═══════════════════════════════════════════════════════════════
# Échantillonnage aléatoire
# ═══════════════════════════════════════════════════════════════
random.shuffle(train_stations)
random.shuffle(val_stations)

train_kept = train_stations[:N_TRAIN]
val_kept   = val_stations[:N_VAL]

# Reconstituer les IDs avec leurs 10 décalages
train_ids = [f"{s}_d{d}" for s in train_kept for d in range(10)]
val_ids   = [f"{s}_d{d}" for s in val_kept   for d in range(10)]

# ═══════════════════════════════════════════════════════════════
# Écriture
# ═══════════════════════════════════════════════════════════════
with open(BASINS_DIR_NEW / "train_basins.txt", "w") as f:
    f.write("\n".join(train_ids))
with open(BASINS_DIR_NEW / "val_basins.txt", "w") as f:
    f.write("\n".join(val_ids))

print(f"\n✅ train_basins.txt : {N_TRAIN} stations × 10 = {len(train_ids)} ids")
print(f"✅ val_basins.txt   : {N_VAL} stations × 10 = {len(val_ids)} ids")
print(f"\n📋 Pour le yaml :")
print(f"   train_basin_file: {BASINS_DIR_NEW}train_basins.txt")
print(f"   validation_basin_file: {BASINS_DIR_NEW}train_basins.txt")
print(f"   test_basin_file: {BASINS_DIR_NEW}val_basins.txt")