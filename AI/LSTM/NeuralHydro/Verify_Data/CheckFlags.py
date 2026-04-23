"""
Affiche les flags qualité (dans_lac, qualite_sauts, signal_plat, gap_max_jours)
pour les 5 meilleures et 5 pires stations.
"""

import sqlite3
import pandas as pd

DB_PATH = "./data/insitu_data.db"

FLOP5 = ["Y101202001", "O455000201", "S421401001", "X350001001", "H207311001"]
TOP5  = ["X031001001", "N430062201", "I922102001", "J783301030", "N330413010"]

stations = FLOP5 + TOP5

conn = sqlite3.connect(DB_PATH)
df = pd.read_sql(f'''
    SELECT code_sta, river_name, dans_lac, qualite_sauts, signal_plat, gap_max_jours
    FROM stations_insitu
    WHERE code_sta IN ({','.join('?' for _ in stations)})
''', conn, params=stations)
conn.close()

# Réordonner selon l'ordre top/flop et ajouter le label
df = df.set_index('code_sta').reindex(stations).reset_index()
df.insert(1, 'groupe', ['FLOP'] * 5 + ['TOP'] * 5)
df.insert(2, 'rang',   list(range(1, 6)) * 2)

print("\n" + "=" * 85)
print("FLAGS QUALITÉ — TOP 5 vs FLOP 5")
print("=" * 85)
print(df.to_string(index=False))
print()

# Résumé par groupe
print("─" * 85)
print("RÉSUMÉ PAR GROUPE")
print("─" * 85)
for groupe, group_df in df.groupby('groupe'):
    print(f"\n{groupe} :")
    print(f"  dans_lac      : {group_df['dans_lac'].value_counts().to_dict()}")
    print(f"  qualite_sauts : {group_df['qualite_sauts'].value_counts().to_dict()}")
    print(f"  signal_plat   : {group_df['signal_plat'].value_counts().to_dict()}")
    print(f"  gap_max_jours : min={group_df['gap_max_jours'].min():.0f}  "
          f"max={group_df['gap_max_jours'].max():.0f}  "
          f"moy={group_df['gap_max_jours'].mean():.1f}")