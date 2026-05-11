"""
flag_stations_capteur.py
═══════════════════════════════════════════════════════════════════════════
Flagge dans la BDD les stations avec un problème de capteur identifié
visuellement après inspection des graphiques.

Une fois flaggées, ces stations peuvent être facilement exclues dans
select_basins, create_dataset_feat10j, etc.
═══════════════════════════════════════════════════════════════════════════
"""

import sqlite3

from data_processing.insitu.db_insitu import (
    ajouter_colonne_flag_capteur,
    flagger_stations_capteur,
    get_stations_flaggees,
)

DB_PATH = './data/insitu_data.db'

# ═══════════════════════════════════════════════════════════════
# Liste des stations identifiées visuellement comme problématiques
# (capteur défaillant : spike isolé dans série plate, inversions, etc.)
# ═══════════════════════════════════════════════════════════════
STATIONS_SPIKE = [
    'F437000201',     # ← à compléter avec le vrai code
    'F712000102',
    'H226000101',
    'H227000102',
    'H300000201',
    'J351401001',
    'K070001020',
    'K401301001',
    'O622251001',
    'S011401003',
    'V372401001',
    'V504503001',
    'X302001001',
    'X345401001',
    'Y033400101',
    'Y046600501',
    'Y047403001',
    'Y145201001',
]

# ═══════════════════════════════════════════════════════════════
# Application
# ═══════════════════════════════════════════════════════════════
conn = sqlite3.connect(DB_PATH)

# Préparer la colonne
ajouter_colonne_flag_capteur(conn)

# Flagger les stations
flagger_stations_capteur(conn, STATIONS_SPIKE, raison='spike')

# Vérification
df = get_stations_flaggees(conn)
print("\n" + "="*55)
print(f"STATIONS FLAGGÉES ({len(df)} au total)")
print("="*55)
print(df.to_string(index=False))

conn.close()