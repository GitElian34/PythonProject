"""
plot_outliers_27j.py
═══════════════════════════════════════════════════════════════════════════
1. Extrait les résidus depuis le validation_results.p du modèle 27j
2. Détecte les outliers (|résidu normalisé| > seuil)
3. Génère un plot zoomé par année contenant des outliers

Produit :
  - ./data/outlier_detection/residuals_27j_all_stations.csv
  - ./figures_zeroshot_satellite/<MODEL>/Outlier_27j/<station>/
      outlier_<station>_<année>.png
═══════════════════════════════════════════════════════════════════════════
"""

import pickle
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from pathlib import Path

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
MODEL  = "arlstm_feat27jHigh_modele2_2205_152119"
EPOCH  = 5
PERIOD = "validation"

RUN_DIR   = Path(f"./runs/{MODEL}")
RESULTS_P = RUN_DIR / PERIOD / f"model_epoch{EPOCH:03d}" / f"{PERIOD}_results.p"

OUT_CSV  = Path("./data/outlier_detection/residuals_27j_all_stations.csv")
BASE_DIR = Path(f"./figures_zeroshot_satellite/{MODEL}/Outlier_27j")
OUT_CSV.parent.mkdir(parents=True, exist_ok=True)

OUTLIER_THRESHOLD = 3.0
TARGET_VAR        = "water_level"


# ═══════════════════════════════════════════════════════════════
# 1. EXTRACTION DES RÉSIDUS
# ═══════════════════════════════════════════════════════════════
print("=" * 60)
print("EXTRACTION RÉSIDUS — MODÈLE 27J")
print("=" * 60)

if not RESULTS_P.exists():
    print(f"❌ Pas de résultats : {RESULTS_P}")
    exit(1)

print(f"Chargement de {RESULTS_P}...")
with open(RESULTS_P, 'rb') as f:
    results = pickle.load(f)

rows = []
for sid, sub in results.items():
    try:
        freq = list(sub.keys())[0]
        ds   = sub[freq]['xr']
        obs_var = f"{TARGET_VAR}_obs"
        sim_var = f"{TARGET_VAR}_sim"
        if obs_var not in ds or sim_var not in ds:
            continue

        dates = pd.to_datetime(ds.date.values)
        obs   = ds[obs_var].values.flatten()
        pred  = ds[sim_var].values.flatten()

        for d, o, p in zip(dates, obs, pred):
            rows.append({
                'station':  str(sid),
                'date':     d,
                'obs':      o,
                'pred':     p,
                'residual': o - p if not (np.isnan(o) or np.isnan(p)) else np.nan,
            })
    except Exception as e:
        print(f"  ⚠  {sid} : {e}")

df = pd.DataFrame(rows)
df['date'] = pd.to_datetime(df['date'])

# Normalisation du résidu par station
def norm_residuals(grp):
    std = np.nanstd(grp['residual'])
    grp['residual_norm'] = grp['residual'] / std if std > 0 else np.nan
    return grp

df = df.groupby('station', group_keys=False).apply(norm_residuals)
df['is_outlier'] = df['residual_norm'].abs() > OUTLIER_THRESHOLD
df['year']       = df['date'].dt.year

# Sauvegarde CSV
df.to_csv(OUT_CSV, index=False)
print(f"\n✅ {len(df)} lignes → {OUT_CSV}")
print(f"   {df['station'].nunique()} stations")
print(f"   {df['is_outlier'].sum()} outliers détectés "
      f"({df['is_outlier'].mean()*100:.1f}%)")


# ═══════════════════════════════════════════════════════════════
# 2. PLOTS PAR ANNÉE
# ═══════════════════════════════════════════════════════════════
print(f"\nGénération des figures...")

stations = sorted(df['station'].unique())
print(f"📊 {len(stations)} stations\n")

n_plots = 0

for sta in stations:
    grp      = df[df['station'] == sta].sort_values('date')
    outliers = grp[grp['is_outlier']]

    if len(outliers) == 0:
        print(f"  {sta:>15s} | 0 outliers → skip")
        continue

    years_with_outliers = sorted(outliers['year'].unique())
    sta_dir = BASE_DIR / sta
    sta_dir.mkdir(parents=True, exist_ok=True)

    for year in years_with_outliers:
        grp_year = grp[grp['year'] == year]
        out_year = outliers[outliers['year'] == year]

        fig, ax = plt.subplots(figsize=(12, 4))

        ax.plot(grp_year['date'], grp_year['obs'], '-o', color='#5B9BD5',
                markersize=5, linewidth=1, label='Observé', zorder=3)
        ax.plot(grp_year['date'], grp_year['pred'], '-o', color='#E88B8B',
                markersize=5, linewidth=1, label='Prédit', zorder=2)

        for _, row in out_year.iterrows():
            ax.plot([row['date'], row['date']], [row['obs'], row['pred']],
                    color='red', linewidth=2, alpha=0.7, zorder=4)
            ax.scatter(row['date'], row['obs'], s=150, facecolors='none',
                       edgecolors='red', linewidths=2, zorder=5)
            ax.annotate(f"{row['residual_norm']:+.1f}σ",
                        xy=(row['date'], row['obs']),
                        xytext=(0, 12 if row['residual'] > 0 else -14),
                        textcoords='offset points',
                        fontsize=9, color='red', fontweight='bold',
                        ha='center',
                        va='bottom' if row['residual'] > 0 else 'top')

        n_out = len(out_year)
        ax.set_title(
            f"Station {sta}  —  {year}  —  "
            f"{n_out} outlier{'s' if n_out > 1 else ''}",
            fontsize=11, fontweight='bold'
        )
        ax.set_ylabel('Water level (z-score)')
        ax.set_xlabel('Date')
        ax.legend(loc='upper right', fontsize=8)
        ax.grid(True, alpha=0.3, linestyle='--')
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%b'))
        ax.xaxis.set_major_locator(mdates.MonthLocator())
        ax.axhline(y=0, color='grey', linewidth=0.5, linestyle='--')

        plt.tight_layout()
        out_path = sta_dir / f"outlier_{sta}_{year}.png"
        fig.savefig(out_path, dpi=150, bbox_inches='tight')
        plt.close(fig)
        n_plots += 1

    print(f"  {sta:>15s} | {len(outliers):2d} outliers | "
          f"{len(years_with_outliers)} années → {sta_dir.name}/")

print(f"\n✅ {n_plots} figures dans {BASE_DIR}")