import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os

from data_processing.insitu.db_insitu import get_donnees_station, get_era5_bv

DB_PATH    = "./data/insitu_data.db"
OUTPUT_DIR = "./data/IA/Visualisation/Exploration/"
CODE_STA   = "A375005050"
os.makedirs(OUTPUT_DIR, exist_ok=True)

TRANCHES = ['0-40km', '40-80km', '80-150km', '150-300km', '>300km']
COULEURS = {
    '0-40km'   : '#1a9850',
    '40-80km'  : '#91cf60',
    '80-150km' : '#fee08b',
    '150-300km': '#fc8d59',
    '>300km'   : '#d73027'
}

# ═══════════════════════════════════════════════════════════════
# CHARGEMENT
# ═══════════════════════════════════════════════════════════════
conn = sqlite3.connect(DB_PATH)
river_name = pd.read_sql_query(
    "SELECT river_name FROM stations_insitu WHERE code_sta = ?",
    conn, params=(CODE_STA,)
).iloc[0]['river_name']
conn.close()

df_base = get_donnees_station(CODE_STA)
df_bv   = get_era5_bv(CODE_STA)

df = df_base.merge(df_bv, on='date', how='inner')
df['delta_h'] = df['h_med_wsh'].shift(-1) - df['h_med_wsh']
df = df.dropna().reset_index(drop=True)
print(f"Dataset : {len(df)} jours | {df.shape[1]} colonnes")

# ═══════════════════════════════════════════════════════════════
# CORRÉLATIONS : tranche × jour de pluie vs delta_h lag=0
# ═══════════════════════════════════════════════════════════════
JOURS = [f'J{i}' for i in range(10)]
corr_matrix = np.zeros((len(TRANCHES), len(JOURS)))

for ti, tranche in enumerate(TRANCHES):
    for ji, jour in enumerate(JOURS):
        col = f'{jour}_{tranche}'
        if col not in df.columns:
            corr_matrix[ti, ji] = np.nan
            continue
        corr = df[col].corr(df['delta_h'])
        corr_matrix[ti, ji] = corr if not np.isnan(corr) else 0

# ═══════════════════════════════════════════════════════════════
# VISUALISATION
# ═══════════════════════════════════════════════════════════════
fig, axes = plt.subplots(1, 2, figsize=(16, 6))
fig.suptitle(
    f'{CODE_STA} — {river_name}\n'
    f'Corrélation pluie Jx par tranche × delta_h (lag=0)',
    fontsize=13, fontweight='bold'
)

# ── Heatmap ──
ax = axes[0]
im = ax.imshow(corr_matrix, aspect='auto', cmap='RdBu_r', vmin=-1, vmax=1)

ax.set_yticks(range(len(TRANCHES)))
ax.set_yticklabels(TRANCHES, fontsize=10)
ax.set_xticks(range(len(JOURS)))
ax.set_xticklabels(JOURS, fontsize=9)

for ti in range(len(TRANCHES)):
    for ji in range(len(JOURS)):
        val = corr_matrix[ti, ji]
        if np.isnan(val):
            ax.text(ji, ti, 'N/A', ha='center', va='center', fontsize=7, color='gray')
        else:
            ax.text(ji, ti, f'{val:.2f}', ha='center', va='center',
                    fontsize=8, color='black' if abs(val) < 0.5 else 'white')

plt.colorbar(im, ax=ax, label='Corrélation de Pearson')
ax.set_xlabel('Jour de pluie (J0=aujourd\'hui, J1=hier...)', fontsize=11)
ax.set_ylabel('Tranche de distance', fontsize=11)
ax.set_title('Heatmap', fontsize=11, fontweight='bold')

# ── Profil par tranche ──
ax = axes[1]
for ti, tranche in enumerate(TRANCHES):
    vals = corr_matrix[ti, :]
    if np.all(np.isnan(vals)):
        continue
    ax.plot(range(len(JOURS)), vals,
            color=COULEURS[tranche], linewidth=2,
            marker='o', markersize=5, label=tranche)

ax.axhline(0, color='black', linewidth=0.8, linestyle='--')
ax.set_xlabel('Jour de pluie (J0=aujourd\'hui, J1=hier...)', fontsize=11)
ax.set_ylabel('Corrélation de Pearson', fontsize=11)
ax.set_title('Profil par tranche', fontsize=11, fontweight='bold')
ax.legend(fontsize=9)
ax.grid(True, alpha=0.3)
ax.set_xticks(range(len(JOURS)))
ax.set_xticklabels(JOURS)

plt.tight_layout()
path = os.path.join(OUTPUT_DIR, f'heatmap_v2_{CODE_STA}.png')
plt.savefig(path, dpi=150, bbox_inches='tight')
plt.show()
print(f"Sauvegardé : {path}")