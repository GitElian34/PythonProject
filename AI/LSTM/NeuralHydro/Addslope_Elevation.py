#!/usr/bin/env python3
"""
Ajoute les colonnes 'elevation_mean' et 'slope_mean' dans attributes.csv
depuis la BDD insitu_data.db
"""
import sqlite3
import pandas as pd

DB_PATH    = "./data/insitu_data.db"
ATTRS_PATH = "./data/IA/NeuralHydrology/attributes/attributes.csv"

# 1. Charger le attributes.csv existant
attrs = pd.read_csv(ATTRS_PATH)
print(f"✅ {len(attrs)} stations dans attributes.csv")

# 2. Récupérer elevation/slope depuis la BDD
conn = sqlite3.connect(DB_PATH)
elev_slope = pd.read_sql("""
    SELECT code_sta, elevation_mean, slope_mean
    FROM stations_insitu
    WHERE elevation_mean IS NOT NULL AND slope_mean IS NOT NULL
""", conn)
conn.close()
print(f"✅ {len(elev_slope)} stations avec elevation/slope dans la BDD")

# 3. Extraire la station de base (sans _d0..._d9) pour faire le merge
attrs["code_sta"] = attrs["station_id"].str.replace(r"_d\d+$", "", regex=True)

# 4. Joindre sur code_sta
attrs = attrs.merge(elev_slope, on="code_sta", how="left")
attrs = attrs.drop(columns=["code_sta"])

# 5. Vérification
n_missing_elev = attrs["elevation_mean"].isna().sum()
n_missing_slope = attrs["slope_mean"].isna().sum()
if n_missing_elev > 0 or n_missing_slope > 0:
    print(f"⚠️  {n_missing_elev} stations sans elevation, {n_missing_slope} sans slope")
    print(f"    Lance d'abord add_elevation_slope.py si nécessaire")

# 6. Sauvegarder
attrs.to_csv(ATTRS_PATH, index=False)
print(f"✅ attributes.csv mis à jour avec colonnes elevation_mean et slope_mean")

# 7. Stats
print(f"\nElevation (m) : médiane={attrs['elevation_mean'].median():.0f}, "
      f"min={attrs['elevation_mean'].min():.0f}, max={attrs['elevation_mean'].max():.0f}")
print(f"Slope (%)     : médiane={attrs['slope_mean'].median():.2f}, "
      f"min={attrs['slope_mean'].min():.2f}, max={attrs['slope_mean'].max():.2f}")