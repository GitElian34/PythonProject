#!/usr/bin/env python3
"""
Ajoute la colonne 'strahler' dans attributes.csv
depuis la BDD insitu_data.db
"""

import sqlite3
import pandas as pd

DB_PATH    = "./data/insitu_data.db"
ATTRS_PATH = "./data/IA/NeuralHydrology/attributes/attributes.csv"

# 1. Charger le attributes.csv existant
attrs = pd.read_csv(ATTRS_PATH)
print(f"✅ {len(attrs)} stations dans attributes.csv")

# 2. Récupérer le Strahler depuis la BDD pour toutes les stations
conn = sqlite3.connect(DB_PATH)
strahler = pd.read_sql("""
    SELECT code_sta AS station_id, strahler
    FROM stations_insitu
    WHERE strahler IS NOT NULL
""", conn)
conn.close()
print(f"✅ {len(strahler)} stations avec Strahler dans la BDD")

# 3. Joindre sur station_id — les stations sans Strahler auront NaN
attrs = attrs.merge(strahler, on="station_id", how="left")

# 4. Vérification rapide
n_missing = attrs["strahler"].isna().sum()
if n_missing > 0:
    print(f"⚠️  {n_missing} stations sans Strahler (NaN) — lance d'abord add_strahler.py")

# 5. Sauvegarder
attrs.to_csv(ATTRS_PATH, index=False)
print(f"✅ attributes.csv mis à jour avec colonne strahler")
print(f"\nDistribution :\n{attrs['strahler'].value_counts().sort_index()}")