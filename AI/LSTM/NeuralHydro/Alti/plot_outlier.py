"""
plot_outliers.py
═══════════════════════════════════════════════════════════════════════════
Pour chaque station, génère un plot zoomé par année contenant des outliers.
Si 2 outliers la même année → un seul plot pour cette année.

Produit : ./figures_zeroshot_satellite/<MODEL>/Outlier/<station>/
            outlier_<station>_<année>.png

Usage :
  python plot_outliers.py
═══════════════════════════════════════════════════════════════════════════
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from pathlib import Path

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
MODEL = "arlstm_feat10jLow_modele2_0605_140952"
CSV_PATH = Path("./data/outlier_detection/residuals_all_stations.csv")
BASE_DIR = Path(f"./figures_zeroshot_satellite/{MODEL}/Outlier")

OUTLIER_THRESHOLD = 3.0

# ═══════════════════════════════════════════════════════════════
# 1. Charger les données
# ═══════════════════════════════════════════════════════════════
df = pd.read_csv(CSV_PATH, parse_dates=['date'])
df['station'] = df['station'].astype(str)
df['is_outlier'] = df['is_outlier'].astype(bool)
df['year'] = df['date'].dt.year

stations = sorted(df['station'].unique())
print(f"📊 {len(stations)} stations\n")

n_plots = 0

for sta in stations:
    grp = df[df['station'] == sta].sort_values('date')
    outliers = grp[grp['is_outlier']]

    if len(outliers) == 0:
        print(f"  {sta:>15s} | 0 outliers → skip")
        continue

    # Années contenant au moins un outlier
    years_with_outliers = sorted(outliers['year'].unique())

    sta_dir = BASE_DIR / sta
    sta_dir.mkdir(parents=True, exist_ok=True)

    for year in years_with_outliers:
        # Données de l'année
        mask_year = grp['year'] == year
        grp_year = grp[mask_year]
        out_year = outliers[outliers['year'] == year]

        # --- Figure ---
        fig, ax = plt.subplots(figsize=(12, 4))

        # Courbes obs et pred
        ax.plot(grp_year['date'], grp_year['obs'], '-o', color='#5B9BD5',
                markersize=5, linewidth=1, label='Observé', zorder=3)
        ax.plot(grp_year['date'], grp_year['pred'], '-o', color='#E88B8B',
                markersize=5, linewidth=1, label='Prédit', zorder=2)

        # Outliers : trait vertical + cercle rouge + annotation
        for _, row in out_year.iterrows():
            ax.plot([row['date'], row['date']], [row['obs'], row['pred']],
                    color='red', linewidth=2, alpha=0.7, zorder=4)
            ax.scatter(row['date'], row['obs'], s=150, facecolors='none',
                       edgecolors='red', linewidths=2, zorder=5)
            # Annotation avec le résidu normalisé
            offset = 0.15 * (1 if row['residual'] > 0 else -1)
            ax.annotate(f"{row['residual_norm']:+.1f}σ",
                        xy=(row['date'], row['obs']),
                        xytext=(0, 12 if row['residual'] > 0 else -14),
                        textcoords='offset points',
                        fontsize=9, color='red', fontweight='bold',
                        ha='center', va='bottom' if row['residual'] > 0 else 'top')

        n_out = len(out_year)
        ax.set_title(f"Station {sta}  —  {year}  —  {n_out} outlier{'s' if n_out > 1 else ''}",
                     fontsize=11, fontweight='bold')
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

    print(f"  {sta:>15s} | {len(outliers):2d} outliers | {len(years_with_outliers)} années → {sta_dir.name}/")

print(f"\n✅ {n_plots} figures générées dans {BASE_DIR}")