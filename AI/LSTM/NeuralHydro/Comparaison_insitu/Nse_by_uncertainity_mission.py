"""
nse_by_mission_uncertainty.py
═══════════════════════════════════════════════════════════════════════════
Calcule le NSE alti↔insitu :
  1. Par mission satellite dominante (J3, S6A, S3A, S3B, ...)
  2. Par bin d'uncertainty médiane
  3. Par satellite × période (10j / 27j)

Lit directement satellite_quality_full.csv (déjà généré).
Si le CSV n'existe pas, relance les requêtes BDD minimales.
═══════════════════════════════════════════════════════════════════════════
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from pathlib import Path
from scipy import stats

# ═══════════════════════════════════════════════════════════════
# CHARGEMENT
# ═══════════════════════════════════════════════════════════════
OUTPUT_DIR = Path("./data/outlier_detection")
CSV_FULL   = OUTPUT_DIR / "satellite_quality_full.csv"

df = pd.read_csv(CSV_FULL)
df['station'] = df['station'].astype(str).str.zfill(13)
df = df[df['status'] == 'ok'].copy()

# Colonne NSE
NSE_COL = 'nse_alti_insitu'
if NSE_COL not in df.columns:
    raise RuntimeError(f"Colonne '{NSE_COL}' absente — relancez analyse_satellite_quality.py d'abord.")

df_valid = df.dropna(subset=[NSE_COL, 'dominant_satellite', 'unc_median']).copy()
print(f"Stations valides pour l'analyse : {len(df_valid)} (10j={( df_valid['period']==10).sum()}, 27j={(df_valid['period']==27).sum()})")

# ═══════════════════════════════════════════════════════════════
# 1. NSE PAR MISSION SATELLITE DOMINANTE
# ═══════════════════════════════════════════════════════════════
print("\n── NSE par satellite dominant ──")
mission_stats = []
for sat, grp in df_valid.groupby('dominant_satellite'):
    nse = grp[NSE_COL].dropna()
    periods = grp['period'].value_counts().to_dict()
    unc = grp['unc_median'].dropna()
    if len(nse) < 2:
        continue
    mission_stats.append({
        'satellite'   : sat,
        'n_stations'  : len(nse),
        'n_10j'       : periods.get(10, 0),
        'n_27j'       : periods.get(27, 0),
        'nse_median'  : nse.median(),
        'nse_mean'    : nse.mean(),
        'nse_std'     : nse.std(),
        'nse_p25'     : nse.quantile(0.25),
        'nse_p75'     : nse.quantile(0.75),
        'nse_gt05'    : (nse > 0.5).sum(),
        'nse_lt0'     : (nse < 0).sum(),
        'unc_median'  : unc.median(),
        'nse_values'  : nse.values,  # pour les boxplots
    })
    print(f"  {sat:8s} | n={len(nse):3d} (10j={periods.get(10,0)}, 27j={periods.get(27,0)}) "
          f"| NSE médian={nse.median():.3f} | unc médiane={unc.median():.3f}m "
          f"| NSE>0.5: {(nse>0.5).sum()} | NSE<0: {(nse<0).sum()}")

df_missions = pd.DataFrame(mission_stats).drop(columns='nse_values')
df_missions = df_missions.sort_values('nse_median', ascending=False)
df_missions.to_csv(OUTPUT_DIR / "nse_by_mission.csv", index=False)
print(f"\n✅ nse_by_mission.csv exporté")

# ═══════════════════════════════════════════════════════════════
# 2. NSE PAR BIN D'UNCERTAINTY
# ═══════════════════════════════════════════════════════════════
print("\n── NSE par bin d'uncertainty ──")

# Bins définis sur l'ensemble des stations
unc_bins   = [0, 0.05, 0.10, 0.15, 0.20, 0.30, 0.50, np.inf]
unc_labels = ['<5cm', '5-10cm', '10-15cm', '15-20cm', '20-30cm', '30-50cm', '>50cm']

df_valid['unc_bin'] = pd.cut(df_valid['unc_median'], bins=unc_bins, labels=unc_labels)

unc_stats = []
for period in [10, 27, 'all']:
    sub = df_valid if period == 'all' else df_valid[df_valid['period'] == period]
    for b, grp in sub.groupby('unc_bin', observed=True):
        nse = grp[NSE_COL].dropna()
        if len(nse) == 0:
            continue
        unc_stats.append({
            'period'    : str(period),
            'unc_bin'   : b,
            'n'         : len(nse),
            'nse_median': nse.median(),
            'nse_mean'  : nse.mean(),
            'nse_p25'   : nse.quantile(0.25),
            'nse_p75'   : nse.quantile(0.75),
        })

df_unc = pd.DataFrame(unc_stats)
df_unc.to_csv(OUTPUT_DIR / "nse_by_uncertainty_bin.csv", index=False)

print("  Période | Bin unc    |  n  | NSE médian | NSE moyen")
print("  " + "-"*55)
for _, row in df_unc[df_unc['period'] != 'all'].iterrows():
    print(f"  {row['period']:6s}  | {row['unc_bin']:10s} | {row['n']:3.0f} "
          f"| {row['nse_median']:10.3f} | {row['nse_mean']:.3f}")

# ═══════════════════════════════════════════════════════════════
# 3. TEST STATISTIQUE : NSE par satellite (Kruskal-Wallis)
# ═══════════════════════════════════════════════════════════════
print("\n── Kruskal-Wallis NSE par satellite ──")
groups_kw = [s['nse_values'] for s in mission_stats if len(s['nse_values']) >= 3]
labels_kw  = [s['satellite'] for s in mission_stats if len(s['nse_values']) >= 3]
if len(groups_kw) >= 2:
    h_stat, p_kw = stats.kruskal(*groups_kw)
    print(f"  H={h_stat:.2f}, p={p_kw:.4f} → "
          + ("Différences significatives entre satellites ✅" if p_kw < 0.05
             else "Pas significatif ⚠️"))

# Pairwise Mann-Whitney si significatif ou pour info
if len(labels_kw) >= 2:
    print("\n  Mann-Whitney pairwise (p-values) :")
    for i in range(len(labels_kw)):
        for j in range(i+1, len(labels_kw)):
            u, p = stats.mannwhitneyu(groups_kw[i], groups_kw[j], alternative='two-sided')
            sig = "✅" if p < 0.05 else "  "
            print(f"  {sig} {labels_kw[i]:6s} vs {labels_kw[j]:6s} : p={p:.4f}")

# ═══════════════════════════════════════════════════════════════
# FIGURE
# ═══════════════════════════════════════════════════════════════
COLORS_PERIOD = {10: '#3A9CC9', 27: '#E88B1A'}
SAT_PALETTE   = {
    'J3' : '#2ecc71', 'S6A': '#27ae60',
    'S3A': '#e74c3c', 'S3B': '#c0392b',
}
DEFAULT_COLOR = '#95a5a6'

fig = plt.figure(figsize=(18, 13))
gs  = gridspec.GridSpec(2, 3, figure=fig, hspace=0.45, wspace=0.35)

ax1 = fig.add_subplot(gs[0, :2])   # Boxplot NSE par satellite (large)
ax2 = fig.add_subplot(gs[0, 2])    # Scatter unc médiane vs NSE médian par satellite
ax3 = fig.add_subplot(gs[1, 0])    # NSE par bin unc — 10j
ax4 = fig.add_subplot(gs[1, 1])    # NSE par bin unc — 27j
ax5 = fig.add_subplot(gs[1, 2])    # Tableau synthèse (texte)

rng = np.random.default_rng(42)

# ── Panel 1 : Boxplot NSE par satellite ─────────────────────
sats_sorted = sorted(mission_stats, key=lambda x: x['nse_median'], reverse=True)
sat_names   = [s['satellite'] for s in sats_sorted]
sat_data    = [s['nse_values'] for s in sats_sorted]
sat_colors  = [SAT_PALETTE.get(s, DEFAULT_COLOR) for s in sat_names]

bp = ax1.boxplot(sat_data, tick_labels=sat_names, patch_artist=True,
                 medianprops={'color': 'black', 'linewidth': 2.5},
                 widths=0.55, vert=True)
for box, color in zip(bp['boxes'], sat_colors):
    box.set_facecolor(color)
    box.set_alpha(0.75)

for i, (s, data) in enumerate(zip(sats_sorted, sat_data), 1):
    jitter = rng.uniform(-0.18, 0.18, len(data))
    # Colorier les points selon période
    sat_obj = next(x for x in mission_stats if x['satellite'] == s['satellite'])
    sub_df  = df_valid[df_valid['dominant_satellite'] == s['satellite']]
    for _, row_s in sub_df.iterrows():
        nse_v = row_s[NSE_COL]
        if np.isnan(nse_v):
            continue
        c = COLORS_PERIOD.get(row_s['period'], 'grey')
        ax1.scatter(i + rng.uniform(-0.18, 0.18), nse_v,
                    color=c, alpha=0.7, s=35, zorder=3)
    # Médiane annotée
    med = np.nanmedian(data)
    ax1.text(i, med + 0.04, f'{med:.2f}', ha='center', fontsize=9,
             fontweight='bold', color='black')

ax1.axhline(0,   color='red',   lw=1.2, ls='--', alpha=0.6, label='NSE=0')
ax1.axhline(0.5, color='green', lw=1.2, ls='--', alpha=0.6, label='NSE=0.5')

# Légende période
from matplotlib.lines import Line2D
legend_elems = [
    Line2D([0],[0], marker='o', color='w', markerfacecolor=COLORS_PERIOD[10], markersize=9, label='10j (J3/S6A)'),
    Line2D([0],[0], marker='o', color='w', markerfacecolor=COLORS_PERIOD[27], markersize=9, label='27j (S3A/S3B)'),
]
ax1.legend(handles=legend_elems + [
    Line2D([0],[0], color='red',   lw=1.2, ls='--', label='NSE=0'),
    Line2D([0],[0], color='green', lw=1.2, ls='--', label='NSE=0.5'),
], fontsize=9, loc='lower right')

ax1.set_title('NSE alti ↔ insitu par satellite dominant\n(points colorés par période : bleu=10j, orange=27j)',
              fontsize=11, fontweight='bold')
ax1.set_ylabel('NSE alti ↔ insitu', fontsize=10)
ax1.set_xlabel('Satellite dominant', fontsize=10)
ax1.grid(True, alpha=0.3, axis='y')
# Annotation n= sous chaque satellite
for i, s in enumerate(sats_sorted, 1):
    ax1.text(i, ax1.get_ylim()[0] + 0.02,
             f"n={s['n_stations']}\nunc={s['unc_median']:.3f}m",
             ha='center', fontsize=8, color='#444')

# ── Panel 2 : Scatter uncertainty vs NSE par satellite ───────
for s in mission_stats:
    color = SAT_PALETTE.get(s['satellite'], DEFAULT_COLOR)
    ax2.scatter(s['unc_median'], s['nse_median'],
                s=s['n_stations']*15, color=color, alpha=0.85,
                edgecolors='white', linewidth=1.5, zorder=3)
    ax2.annotate(s['satellite'],
                 xy=(s['unc_median'], s['nse_median']),
                 xytext=(4, 4), textcoords='offset points',
                 fontsize=9, fontweight='bold',
                 color=SAT_PALETTE.get(s['satellite'], '#333'))

ax2.axhline(0,   color='red',   lw=1, ls='--', alpha=0.5)
ax2.axhline(0.5, color='green', lw=1, ls='--', alpha=0.5)
ax2.set_xlabel('Uncertainty médiane (m)', fontsize=9)
ax2.set_ylabel('NSE médian alti↔insitu', fontsize=9)
ax2.set_title('Uncertainty vs NSE\npar satellite (taille ∝ n stations)',
              fontsize=10, fontweight='bold')
ax2.grid(True, alpha=0.3)
ax2.text(0.05, 0.05, '(meilleur en haut à gauche)', transform=ax2.transAxes,
         fontsize=8, color='grey', style='italic')

# ── Panels 3-4 : NSE par bin uncertainty (10j et 27j) ───────
for ax_b, period, color in [(ax3, 10, COLORS_PERIOD[10]), (ax4, 27, COLORS_PERIOD[27])]:
    sub = df_unc[df_unc['period'] == str(period)]
    if sub.empty:
        ax_b.set_visible(False)
        continue
    x    = range(len(sub))
    bars = ax_b.bar(x, sub['nse_median'], color=color, alpha=0.75,
                    edgecolor='white', linewidth=1)
    ax_b.errorbar(x,
                  sub['nse_median'],
                  yerr=[sub['nse_median'] - sub['nse_p25'],
                        sub['nse_p75'] - sub['nse_median']],
                  fmt='none', color='#333', capsize=4, linewidth=1.5, zorder=4)
    # Annoter n=
    for xi, (_, row) in zip(x, sub.iterrows()):
        ax_b.text(xi, max(row['nse_median'] + 0.03, 0.03),
                  f"n={row['n']:.0f}", ha='center', fontsize=8)
    ax_b.set_xticks(list(x))
    ax_b.set_xticklabels(sub['unc_bin'], rotation=30, ha='right', fontsize=8)
    ax_b.axhline(0,   color='red',   lw=1, ls='--', alpha=0.6)
    ax_b.axhline(0.5, color='green', lw=1, ls='--', alpha=0.6)
    ax_b.set_title(f'NSE médian par bin uncertainty — {period}j\n(barres d\'erreur = IQR)',
                   fontsize=10, fontweight='bold')
    ax_b.set_xlabel('Uncertainty médiane de la station', fontsize=9)
    ax_b.set_ylabel('NSE médian alti↔insitu', fontsize=9)
    ax_b.grid(True, alpha=0.3, axis='y')

# ── Panel 5 : Tableau récapitulatif ──────────────────────────
ax5.axis('off')
table_data = [['Satellite', 'n', 'Période', 'NSE\nmédian', 'Unc\nmédiane (m)', 'NSE>0.5', 'NSE<0']]
for s in sats_sorted:
    period_str = f"10j={s['n_10j']}/27j={s['n_27j']}"
    table_data.append([
        s['satellite'],
        str(s['n_stations']),
        period_str,
        f"{s['nse_median']:.3f}",
        f"{s['unc_median']:.3f}",
        str(s['nse_gt05']),
        str(s['nse_lt0']),
    ])

tbl = ax5.table(cellText=table_data[1:], colLabels=table_data[0],
                cellLoc='center', loc='center', bbox=[0, 0, 1, 1])
tbl.auto_set_font_size(False)
tbl.set_fontsize(9)
for (row, col), cell in tbl.get_celld().items():
    if row == 0:
        cell.set_facecolor('#2c3e50')
        cell.set_text_props(color='white', fontweight='bold')
    else:
        sat = table_data[row][0]
        cell.set_facecolor(SAT_PALETTE.get(sat, '#ecf0f1') + '40')
    cell.set_edgecolor('#bdc3c7')
ax5.set_title('Récapitulatif par satellite', fontsize=10, fontweight='bold', pad=10)

fig.suptitle('NSE alti ↔ insitu : analyse par satellite et uncertainty\n'
             '(J3=Jason-3 · S6A=Sentinel-6A · S3A/S3B=Sentinel-3)',
             fontsize=13, fontweight='bold', y=1.01)

fig_path = OUTPUT_DIR / "nse_by_mission_uncertainty.png"
fig.savefig(fig_path, dpi=150, bbox_inches='tight')
plt.close(fig)
print(f"\n✅ Figure → {fig_path}")
print(f"✅ nse_by_mission.csv + nse_by_uncertainty_bin.csv dans {OUTPUT_DIR}/")