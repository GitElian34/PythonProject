"""
Réduit le test_basins.txt à 200 stations échantillonnées aléatoirement
pour accélérer le permutation importance.

Backup le fichier original.
"""

import random
from pathlib import Path

TEST_PATH   = Path("./AI/LSTM/NeuralHydro_feat10j/val_basins.txt")
TEST_BAK    = Path("./AI/LSTM/NeuralHydro_feat10j/val_basins_FULL.txt")
N_STATIONS  = 200
SEED        = 42

# Backup
if not TEST_BAK.exists():
    import shutil
    shutil.copy(TEST_PATH, TEST_BAK)
    print(f"✅ Backup : {TEST_BAK}")

# Charger
with open(TEST_PATH) as f:
    ids = [l.strip() for l in f if l.strip()]

# Extraire les stations réelles uniques
stations = sorted(set(i.rsplit("_d", 1)[0] for i in ids))
print(f"Stations réelles dispo : {len(stations)}")

# Échantillonnage
random.seed(SEED)
random.shuffle(stations)
selected = stations[:N_STATIONS]

# Reconstituer les ids avec décalages présents dans le fichier original
ids_set = set(ids)
new_ids = [f"{s}_d{d}" for s in selected for d in range(10) if f"{s}_d{d}" in ids_set]

with open(TEST_PATH, "w") as f:
    f.write("\n".join(new_ids))

print(f"✅ test_basins.txt : {N_STATIONS} stations × ~10 décalages = {len(new_ids)} ids")
print(f"\nPour restaurer plus tard :")
print(f"   cp {TEST_BAK} {TEST_PATH}")