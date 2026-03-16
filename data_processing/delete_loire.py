import sqlite3
import shutil
from datetime import datetime

# === CONFIGURATION ===
db_path = './data/hydro_data.db'
# === 1. BACKUP AUTO ===
shutil.copy2(db_path, f"{db_path}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
print("✅ Backup créé")

# === 2. CONNEXION ===
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# === 3. SUPPRESSION ===
cursor.execute('''
DELETE FROM climate_data 
WHERE station_code IN (SELECT station_code FROM stations WHERE basin_name = 'LOIRE')
''')

# === 4. RÉSULTAT ===
print(f"✅ {cursor.rowcount} enregistrements supprimés")
conn.commit()
conn.close()