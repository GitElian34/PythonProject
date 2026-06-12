"""
select_stations_27j_multi.py
═══════════════════════════════════════════════════════════════════════════
Crée des train_basins.txt de tailles variées (20, 50, 100, 200, 500)
pour tester l'impact du nombre de stations sur les performances.

Toutes les sélections sont des sous-ensembles imbriqués :
  20 ⊂ 50 ⊂ 100 ⊂ 200 ⊂ 500

Sortie :
  ./AI/LSTM/NeuralHydro_feat27j_high/train_basins_20.txt
  ./AI/LSTM/NeuralHydro_feat27j_high/train_basins_50.txt
  ./AI/LSTM/NeuralHydro_feat27j_high/train_basins_100.txt
  ./AI/LSTM/NeuralHydro_feat27j_high/train_basins_200.txt
  ./AI/LSTM/NeuralHydro_feat27j_high/train_basins_500.txt

Usage : python select_stations_27j_multi.py
═══════════════════════════════════════════════════════════════════════════
"""
import sqlite3
import random
from pathlib import Path

DB_PATH    = "./data/insitu_data.db"
BASINS_DIR = Path("./AI/LSTM/NeuralHydro_feat27j_high/")
SIZES      = [20, 50, 100, 200, 500]
SEED       = 42
FREQ       = 27

random.seed(SEED)

# ═══════════════════════════════════════════════════════════════
# 1. Récupérer toutes les stations éligibles
# ═══════════════════════════════════════════════════════════════
conn = sqlite3.connect(DB_PATH)
cursor = conn.execute("""
    SELECT s.code_sta
    FROM stations_insitu s
    WHERE s.flag_capteur IS NULL
      AND (s.gap_max_jours IS NULL OR s.gap_max_jours <= 200)
      AND (s.dist_barrage_m IS NULL OR s.dist_barrage_m >= 500)
      AND s.lon IS NOT NULL
      AND s.lat IS NOT NULL
      AND s.elevation_mean IS NOT NULL
      AND s.elevation_mean >= 1
""")
eligible = [row[0] for row in cursor.fetchall()]
conn.close()

print(f"Stations éligibles : {len(eligible)}")

# Vérifier que les .nc existent
ts_dir = Path("./data/IA/NeuralHydrology_feat27j/time_series/")
eligible = [s for s in eligible if (ts_dir / f"{s}_d0.nc").exists()]
print(f"Avec .nc disponible : {len(eligible)}")

# Mélanger une seule fois
random.shuffle(eligible)

# ═══════════════════════════════════════════════════════════════
# 2. Créer les fichiers pour chaque taille
# ═══════════════════════════════════════════════════════════════
BASINS_DIR.mkdir(parents=True, exist_ok=True)

for n in SIZES:
    if n > len(eligible):
        print(f"\n⚠️  {n} demandées mais seulement {len(eligible)} disponibles → prend tout")
        selected = eligible
    else:
        selected = eligible[:n]

    train_ids = [f"{s}_d{d}" for s in selected for d in range(FREQ)]

    out_file = BASINS_DIR / f"train_basins_{n}.txt"
    with open(out_file, "w") as f:
        f.write("\n".join(train_ids))

    print(f"✅ {out_file.name:25s} : {len(train_ids):6d} ids ({len(selected)} stations × {FREQ})")

print(f"\nDossier : {BASINS_DIR}")
print(f"Les sélections sont imbriquées : 20 ⊂ 50 ⊂ 100 ⊂ 200 ⊂ 500")