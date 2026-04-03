import sqlite3
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
import matplotlib.pyplot as plt

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
DB_PATH     = './data/insitu_data.db'
NB_STATIONS = 20
MIN_PIXELS  = 100

# ═══════════════════════════════════════════════════════════════
# 1. Charger les données
# ═══════════════════════════════════════════════════════════════
conn     = sqlite3.connect(DB_PATH)
stations = pd.read_sql('''
    SELECT e.code_sta FROM era5_transfert e
    WHERE e.code_sta IN (SELECT DISTINCT code_sta FROM era5_pluie_bv)
    GROUP BY e.code_sta HAVING COUNT(*) >= ?
    LIMIT ?
''', conn, params=(MIN_PIXELS, NB_STATIONS))
print(stations)
all_data = []
for _, row in stations.iterrows():
    code_sta = row['code_sta']

    pluie = pd.read_sql('''
        SELECT mesure_date, tranche_km,
               J0,J1,J2,J3,J4,J5,J6,J7,J8
        FROM era5_pluie_bv WHERE code_sta = ?
    ''', conn, params=(code_sta,))

    pluie_pivot = pluie.pivot(
        index='mesure_date', columns='tranche_km',
        values=['J0','J1','J2','J3','J4','J5','J6','J7','J8']
    )
    pluie_pivot.columns = [f'{j}_{t}' for j, t in pluie_pivot.columns]
    pluie_pivot = pluie_pivot.reset_index()

    mesures = pd.read_sql('''
        SELECT date, h_med_wsh FROM mesures_insitu
        WHERE code_sta = ? ORDER BY date
    ''', conn, params=(code_sta,))
    mesures['delta_h'] = mesures['h_med_wsh'].shift(-1) - mesures['h_med_wsh']
    mesures = mesures.rename(columns={'date': 'mesure_date'}).dropna()

    merged = pluie_pivot.merge(mesures[['mesure_date', 'delta_h']],
                               on='mesure_date', how='inner')
    all_data.append(merged)

conn.close()

df = pd.concat(all_data).dropna()
print(f"Dataset : {len(df)} lignes")

# ═══════════════════════════════════════════════════════════════
# 2. Random Forest — juste pour la feature importance
# ═══════════════════════════════════════════════════════════════
feature_cols = [c for c in df.columns if c.startswith('J') and '_' in c and c[1].isdigit()]
X = df[feature_cols].values
y = df['delta_h'].values

rf = RandomForestRegressor(n_estimators=100, n_jobs=-1, random_state=42)
rf.fit(X, y)

# ═══════════════════════════════════════════════════════════════
# 3. Visualisation feature importance
# ═══════════════════════════════════════════════════════════════
importances = pd.DataFrame({
    'feature'   : feature_cols,
    'importance': rf.feature_importances_
}).sort_values('importance', ascending=False)

print(importances.to_string())

fig, ax = plt.subplots(figsize=(12, 10))
ax.barh(importances['feature'], importances['importance'], color='steelblue')
ax.set_xlabel('Importance')
ax.set_title('Influence de chaque variable ERA5 sur la variation de hauteur')
ax.invert_yaxis()
ax.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig('./data/feature_importance.png', dpi=150, bbox_inches='tight')
plt.show()
print("Graphique sauvegardé !")