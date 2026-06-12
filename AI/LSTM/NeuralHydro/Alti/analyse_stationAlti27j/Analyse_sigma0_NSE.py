"""
analyse_sigma0_nse.py
═══════════════════════════════════════════════════════════════════════════
Corrélation entre sigma0 médian par station et NSE du modèle 27j.

Entrées :
  - ./data/sigma0/sigma0_all_stations.csv
  - validation_metrics.csv du run 27j

Produit :
  - scatter sigma0 vs NSE avec droite de tendance
  - boxplot NSE par quartile de sigma0
  - stats terminales (Spearman r, p-value)
═══════════════════════════════════════════════════════════════════════════
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
from scipy import stats

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES — À MODIFIER
# ═══════════════════════════════════════════════════════════════
SIGMA0_CSV  = "./data/sigma0/sigma0_all_stations.csv"

MODEL       = "arlstm_feat27jHigh_modele2_2205_152119"
EPOCH       = 5
PERIOD      = "validation"
METRICS_CSV = f"./runs/{MODEL}/{PERIOD}/model_epoch{EPOCH:03d}/{PERIOD}_metrics.csv"

OUT_DIR     = Path(f"./figures_sigma0_nse")
OUT_DIR.mkdir(parents=True, exist_ok=True)


# ═══════════════════════════════════════════════════════════════
# CHARGEMENT
# ═══════════════════════════════════════════════════════════════
print("Chargement sigma0...")
df_sigma = pd.read_csv(SIGMA0_CSV)
df_sigma['station_code_norm'] = df_sigma['station_code'].astype(str).str.zfill(13)
df_sigma['station_code_raw']  = df_sigma['station_code'].astype(str)

# Agrégation sigma0 par station (deux clés de merge)
df_sigma_sta = (df_sigma
                .groupby(['station_code_norm', 'station_code_raw'])['sigma0']
                .agg(sigma0_median='median',
                     sigma0_mean='mean',
                     sigma0_std='std',
                     sigma0_min='min',
                     sigma0_max='max',
                     n_mesures='count')
                .reset_index())
print(f"  {len(df_sigma_sta)} stations avec sigma0")

print("Chargement NSE...")
df_nse = pd.read_csv(METRICS_CSV, header=None, names=["station", "NSE", "KGE"])
df_nse = df_nse[df_nse["station"] != "basin"]  # retire le header parasite
df_nse["NSE"] = pd.to_numeric(df_nse["NSE"], errors="coerce")
df_nse["KGE"] = pd.to_numeric(df_nse["KGE"], errors="coerce")
df_nse["station"] = df_nse["station"].astype(str)
print(f"  {len(df_nse)} stations avec NSE")

# Merge : essaie d'abord zfill(13), puis format brut
df = df_nse.merge(df_sigma_sta, left_on="station", right_on="station_code_norm", how="left")
# Pour les stations non matchées, essaie le format brut
mask_missing = df['sigma0_median'].isna()
if mask_missing.sum() > 0:
    stations_missing = df.loc[mask_missing, 'station'].tolist()
    df_fallback = df_nse[df_nse['station'].isin(stations_missing)].merge(
        df_sigma_sta, left_on="station", right_on="station_code_raw", how="inner"
    )
    df = pd.concat([df[~mask_missing], df_fallback], ignore_index=True)

df = df.dropna(subset=["NSE", "sigma0_median"])
print(f"  {len(df)} stations après merge\n")

if len(df) < 5:
    print("❌ Pas assez de stations pour l'analyse")
    exit(1)


# ═══════════════════════════════════════════════════════════════
# STATS
# ═══════════════════════════════════════════════════════════════
r_sp, p_sp = stats.spearmanr(df['sigma0_median'], df['NSE'])
r_pe, p_pe = stats.pearsonr(df['sigma0_median'],  df['NSE'])
r_sp_mean, p_sp_mean = stats.spearmanr(df['sigma0_mean'], df['NSE'])
r_pe_mean, p_pe_mean = stats.pearsonr(df['sigma0_mean'],  df['NSE'])
r_sp_std, p_sp_std   = stats.spearmanr(df['sigma0_std'],  df['NSE'])
r_pe_std, p_pe_std   = stats.pearsonr(df['sigma0_std'],   df['NSE'])

print("═" * 60)
print("CORRÉLATION SIGMA0 ↔ NSE")
print("═" * 60)
print(f"\n  {'Métrique':<20} {'Spearman r':>12} {'p':>8}   {'Pearson r':>12} {'p':>8}")
print(f"  {'─'*56}")
for label, rsp, psp, rpe, ppe in [
    ('sigma0 médian',  r_sp,      p_sp,      r_pe,      p_pe),
    ('sigma0 moyenne', r_sp_mean, p_sp_mean, r_pe_mean, p_pe_mean),
    ('sigma0 std',     r_sp_std,  p_sp_std,  r_pe_std,  p_pe_std),
]:
    sig_sp = '✅' if psp < 0.05 else '❌'
    sig_pe = '✅' if ppe < 0.05 else '❌'
    print(f"  {label:<20} {rsp:>+12.3f} {psp:>7.4f}{sig_sp}  {rpe:>+12.3f} {ppe:>7.4f}{sig_pe}")
print(f"\n  sigma0 médian : {df['sigma0_median'].median():.2f} dB")
print(f"  NSE médian    : {df['NSE'].median():.3f}")

# Quartiles sigma0
q1, q2, q3 = df['sigma0_median'].quantile([0.25, 0.5, 0.75])
df['sigma0_quartile'] = pd.cut(
    df['sigma0_median'],
    bins=[df['sigma0_median'].min()-1, q1, q2, q3, df['sigma0_median'].max()+1],
    labels=['Q1\n(faible)', 'Q2', 'Q3', 'Q4\n(fort)']
)
print(f"\n  NSE médian par quartile sigma0 :")
for q, grp in df.groupby('sigma0_quartile', observed=True):
    print(f"    {q:12s} : NSE={grp['NSE'].median():.3f}  "
          f"(n={len(grp)}, sigma0={grp['sigma0_median'].median():.1f} dB)")


# ═══════════════════════════════════════════════════════════════
# FIGURES
# ═══════════════════════════════════════════════════════════════
fig, axes = plt.subplots(1, 3, figsize=(18, 6))
fig.suptitle(f"Sigma0 médian ↔ NSE modèle 27j  (n={len(df)} stations)\n"
             f"Spearman r={r_sp:+.3f} (p={p_sp:.3f})  |  "
             f"Pearson r={r_pe:+.3f} (p={p_pe:.3f})",
             fontsize=12, fontweight='bold')
# ── 1. Scatter sigma0 vs NSE ─────────────────────────────────────────────
ax = axes[0]
sc = ax.scatter(df['sigma0_median'], df['NSE'],
                c=df['NSE'], cmap='RdYlGn', vmin=-0.3, vmax=1.0,
                s=70, edgecolors='white', linewidths=0.5, zorder=3)
plt.colorbar(sc, ax=ax, label='NSE')

# Droite de tendance
x, y = df['sigma0_median'].values, df['NSE'].values
z = np.polyfit(x, y, 1)
xl = np.linspace(x.min(), x.max(), 100)
ax.plot(xl, np.poly1d(z)(xl), color='tomato', lw=2, ls='--', label='Tendance')

ax.axhline(0,   color='gray', lw=0.8, ls=':', alpha=0.5)
ax.axhline(0.5, color='steelblue', lw=0.8, ls=':', alpha=0.5, label='NSE=0.5')
ax.set_xlabel("Sigma0 médian (dB)", fontsize=11)
ax.set_ylabel("NSE", fontsize=11)
ax.set_title("Scatter sigma0 vs NSE", fontsize=11)
ax.legend(fontsize=9)
ax.grid(True, alpha=0.3)

# Annotations extrêmes
for _, row in df.iterrows():
    if row['NSE'] < -0.1 or row['sigma0_median'] < df['sigma0_median'].quantile(0.1):
        ax.annotate(str(row['station'])[-6:],
                    (row['sigma0_median'], row['NSE']),
                    textcoords="offset points", xytext=(5, 4),
                    fontsize=7, color='gray')

# ── 2. Boxplot NSE par quartile sigma0 ──────────────────────────────────
ax = axes[1]
groups   = [df[df['sigma0_quartile'] == q]['NSE'].dropna().values
            for q in ['Q1\n(faible)', 'Q2', 'Q3', 'Q4\n(fort)']]
labels_q = ['Q1\n(faible)', 'Q2', 'Q3', 'Q4\n(fort)']
bp = ax.boxplot(groups, labels=labels_q, patch_artist=True,
                medianprops=dict(color='black', lw=2))
colors_q = ['#EF5350', '#FFA726', '#66BB6A', '#42A5F5']
for patch, color in zip(bp['boxes'], colors_q):
    patch.set_facecolor(color)
    patch.set_alpha(0.7)
for i, grp in enumerate(groups):
    ax.text(i + 1, ax.get_ylim()[0] if ax.get_ylim()[0] > -2 else -2,
            f'n={len(grp)}', ha='center', va='bottom', fontsize=9, color='gray')
ax.axhline(0,   color='gray', lw=0.8, ls=':', alpha=0.5)
ax.axhline(0.5, color='steelblue', lw=0.8, ls=':', alpha=0.5)
ax.set_xlabel("Quartile sigma0 médian", fontsize=11)
ax.set_ylabel("NSE", fontsize=11)
ax.set_title("NSE par quartile sigma0", fontsize=11)
ax.grid(axis='y', alpha=0.3)

# ── 3. Distribution sigma0 colorée par NSE ──────────────────────────────
ax = axes[2]
# Sépare bonnes (NSE≥0.5) et mauvaises stations
good = df[df['NSE'] >= 0.5]['sigma0_median']
bad  = df[df['NSE'] <  0.5]['sigma0_median']
bins = np.linspace(df['sigma0_median'].min(), df['sigma0_median'].max(), 20)
ax.hist(good, bins=bins, color='#43A047', alpha=0.7,
        label=f'NSE ≥ 0.5 (n={len(good)})', edgecolor='white')
ax.hist(bad,  bins=bins, color='#E53935', alpha=0.7,
        label=f'NSE < 0.5 (n={len(bad)})',  edgecolor='white')
ax.axvline(df['sigma0_median'].median(), color='black', lw=1.5,
           ls='--', label=f'Médiane={df["sigma0_median"].median():.1f}dB')
ax.set_xlabel("Sigma0 médian (dB)", fontsize=11)
ax.set_ylabel("Nb stations", fontsize=11)
ax.set_title("Distribution sigma0\n(bonnes vs mauvaises stations)", fontsize=11)
ax.legend(fontsize=9)
ax.grid(True, alpha=0.3)

plt.tight_layout()
out_path = OUT_DIR / "sigma0_median_vs_nse.png"
plt.savefig(out_path, dpi=150, bbox_inches='tight')
plt.close()
print(f"\n✅ Figure médiane : {out_path}")

# ── Figure sigma0 moyenne et std ─────────────────────────────────────────
fig, axes = plt.subplots(1, 2, figsize=(14, 6))
fig.suptitle(f"Sigma0 moyenne et std ↔ NSE  (n={len(df)} stations)",
             fontsize=12, fontweight='bold')

for ax, col, r_sp_v, p_sp_v, r_pe_v, p_pe_v, label, color in [
    (axes[0], 'sigma0_mean', r_sp_mean, p_sp_mean, r_pe_mean, p_pe_mean,
     'Sigma0 moyenne (dB)', '#1565C0'),
    (axes[1], 'sigma0_std',  r_sp_std,  p_sp_std,  r_pe_std,  p_pe_std,
     'Sigma0 std (dB)',     '#6A1B9A'),
]:
    sc = ax.scatter(df[col], df['NSE'],
                    c=df['NSE'], cmap='RdYlGn', vmin=-0.3, vmax=1.0,
                    s=70, edgecolors='white', linewidths=0.5, zorder=3)
    plt.colorbar(sc, ax=ax, label='NSE')

    x, y = df[col].values, df['NSE'].values
    mask = ~(np.isnan(x) | np.isnan(y))
    if mask.sum() > 3:
        z  = np.polyfit(x[mask], y[mask], 1)
        xl = np.linspace(x[mask].min(), x[mask].max(), 100)
        ax.plot(xl, np.poly1d(z)(xl), color='tomato', lw=2, ls='--')

    ax.axhline(0,   color='gray', lw=0.8, ls=':', alpha=0.5)
    ax.axhline(0.5, color='steelblue', lw=0.8, ls=':', alpha=0.5)
    ax.set_xlabel(label, fontsize=11)
    ax.set_ylabel("NSE", fontsize=11)
    sig_sp = '✅' if p_sp_v < 0.05 else '❌'
    sig_pe = '✅' if p_pe_v < 0.05 else '❌'
    ax.set_title(f"Spearman r={r_sp_v:+.3f} (p={p_sp_v:.3f}){sig_sp}\n"
                 f"Pearson  r={r_pe_v:+.3f} (p={p_pe_v:.3f}){sig_pe}",
                 fontsize=10)
    ax.grid(True, alpha=0.3)

plt.tight_layout()
out_path2 = OUT_DIR / "sigma0_mean_std_vs_nse.png"
plt.savefig(out_path2, dpi=150, bbox_inches='tight')
plt.close()
print(f"✅ Figure moyenne/std : {out_path2}")