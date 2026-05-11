#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
analyse_zeroshot_facteurs.py
═══════════════════════════════════════════════════════════════════════════
Analyse croisée : qu'est-ce qui fait qu'une station satellite est bien
prédite en zero-shot vs mal prédite ?

Croise les métriques NSE/KGE avec :
  - Attributs statiques (aire, strahler, elevation, slope, lat/lon, sol...)
  - Profil des données (couverture, nb mesures, trous, amplitude WL...)
  - Variables dynamiques (régime de précip, température, variabilité...)

Sortie :
  ./Exploring_data/Analyse_zeroshot/correlation_NSE.png
  ./Exploring_data/Analyse_zeroshot/comparaison_bonnes_mauvaises.png
  ./Exploring_data/Analyse_zeroshot/tableau_complet.csv
═══════════════════════════════════════════════════════════════════════════
"""

import sqlite3
import pandas as pd
import numpy as np
import xarray as xr
import matplotlib.pyplot as plt
from pathlib import Path
from scipy import stats
import warnings
import os
warnings.filterwarnings('ignore')

# ─── Config ─────────────────────────────────────────────────────────────────
DB_PATH     = './data/hydro_data.db'
NC_DIR      = './data/IA/NeuralHydrology_satellite_10D/time_series/'
ATTR_CSV    = './data/IA/NeuralHydrology_satellite_10D/attributes/attributes.csv'
OUT_DIR     = Path('./Exploring_data/Analyse_zeroshot')
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Métriques zero-shot (copiées depuis tes résultats)
METRICS = {
    '0000000005729': 0.81, '0000000005735': 0.79, '0000000005736': 0.74,
    '0000000006310': 0.58, '0000000006315': 0.22, '0000000006325': 0.83,
    '0000000006326': 0.87, '0000000008740': -0.03, '0000000008748': -0.01,
    '0000000008751': 0.59, '0000000008761': 0.38, '0000000010836': 0.67,
    '0000000010837': 0.70, '0000000010842': 0.33, '0000000010843': 0.62,
    '0000000010844': 0.39, '0000000010860': 0.55, '110986': 0.34,
    '110987': 0.59, '111157': 0.27, '111158': 0.55, '111159': 0.78,
    '111511': 0.73, '112064': 0.25, '112065': 0.11, '112066': 0.54,
    '112556': 0.53, '112557': 0.59, '112558': 0.57, '113102': 0.43,
    '113449': 0.41, '113450': 0.52, '113598': 0.49, '113599': 0.47,
}

conn = sqlite3.connect(DB_PATH)

print("╔══════════════════════════════════════════════════════════════╗")
print("║  ANALYSE DES FACTEURS DE SUCCÈS DU ZERO-SHOT               ║")
print("╚══════════════════════════════════════════════════════════════╝\n")

# ═══════════════════════════════════════════════════════════════
# 1. COLLECTE DES CARACTÉRISTIQUES PAR STATION
# ═══════════════════════════════════════════════════════════════
print("1. Collecte des caractéristiques...\n")

rows = []
for code_sta, nse in METRICS.items():

    row = {'station_id': code_sta, 'NSE': nse}

    # ── Attributs statiques depuis la BDD ───────────────────────────────
    sta_info = pd.read_sql('''
        SELECT s.reference_longitude AS lon, s.reference_latitude AS lat,
               s.strahler, s.elevation_mean, s.slope_mean,
               s.dist_barrage_m, s.mean_altitude,
               b.aire_km2,
               c.frac_urban, c.frac_agriculture, c.frac_forest,
               c.frac_semi_natural, c.sg_clay_0_30cm, c.sg_sand_0_30cm
        FROM stations s
        LEFT JOIN bv_data b ON s.station_code = b.station_code
        LEFT JOIN bv_corine c ON s.station_code = c.station_code
        WHERE s.station_code = ?
    ''', conn, params=(code_sta,))

    if not sta_info.empty:
        for col in sta_info.columns:
            row[col] = sta_info[col].iloc[0]

    # ── Profil des mesures satellite (depuis measurements) ──────────────
    df_mes = pd.read_sql('''
        SELECT measure_date, orthometric_height
        FROM measurements
        WHERE station_code = ? AND is_valid = 1
              AND orthometric_height IS NOT NULL
        ORDER BY measure_date
    ''', conn, params=(code_sta,))

    if not df_mes.empty:
        df_mes['measure_date'] = pd.to_datetime(df_mes['measure_date'])
        wl = df_mes['orthometric_height'].astype(float)

        row['nb_mesures_raw'] = len(df_mes)
        row['date_debut'] = df_mes['measure_date'].min().strftime('%Y-%m-%d')
        row['date_fin'] = df_mes['measure_date'].max().strftime('%Y-%m-%d')
        row['duree_ans'] = (df_mes['measure_date'].max() - df_mes['measure_date'].min()).days / 365.25

        # Intervalles
        intervals = df_mes['measure_date'].diff().dt.days.dropna().values
        if len(intervals) > 0:
            row['interval_median'] = np.median(intervals)
            row['interval_mean'] = np.mean(intervals)
            row['interval_std'] = np.std(intervals)
            row['interval_max'] = np.max(intervals)
            row['nb_gaps_30j'] = np.sum(intervals > 30)
            row['nb_gaps_60j'] = np.sum(intervals > 60)
            row['pct_gaps_30j'] = np.sum(intervals > 30) / len(intervals) * 100

        # Amplitude et variabilité du water level
        row['wl_mean'] = wl.mean()
        row['wl_std'] = wl.std()
        row['wl_range'] = wl.max() - wl.min()
        row['wl_iqr'] = wl.quantile(0.75) - wl.quantile(0.25)
        row['wl_cv'] = wl.std() / abs(wl.mean()) if abs(wl.mean()) > 0.01 else np.nan

        # Saisonnalité (amplitude du cycle annuel moyen)
        df_mes['month'] = df_mes['measure_date'].dt.month
        monthly_mean = df_mes.groupby('month')['orthometric_height'].mean()
        row['wl_seasonal_range'] = monthly_mean.max() - monthly_mean.min() if len(monthly_mean) >= 6 else np.nan

    # ── Profil du .nc (couverture après snap) ───────────────────────────
    nc_path = os.path.join(NC_DIR, f'{code_sta}.nc')
    if os.path.exists(nc_path):
        ds = xr.open_dataset(nc_path)
        wl_nc = ds['water_level'].values
        n_total = len(wl_nc)
        n_valid = np.sum(~np.isnan(wl_nc))
        row['nb_pas_10D'] = n_total
        row['nb_mesures_snap'] = n_valid
        row['couverture_pct'] = n_valid / n_total * 100

        # Trous consécutifs dans le .nc
        is_nan = np.isnan(wl_nc)
        if is_nan.any():
            # Longueur du plus grand trou consécutif (en pas de 10j)
            nan_runs = []
            count = 0
            for v in is_nan:
                if v:
                    count += 1
                else:
                    if count > 0:
                        nan_runs.append(count)
                    count = 0
            if count > 0:
                nan_runs.append(count)
            row['max_trou_consecutif_10D'] = max(nan_runs) if nan_runs else 0
            row['nb_trous'] = len(nan_runs)
        else:
            row['max_trou_consecutif_10D'] = 0
            row['nb_trous'] = 0

        # Stats ERA5 sur la station
        for var in ['precipitation_J0', 'precip_mean_J10', 'temperature_J0']:
            if var in ds:
                vals = ds[var].values
                row[f'{var}_mean'] = np.nanmean(vals)
                row[f'{var}_std'] = np.nanstd(vals)

        ds.close()

    rows.append(row)

import os
df = pd.DataFrame(rows)
print(f"  {len(df)} stations avec {len(df.columns)} caractéristiques collectées\n")

# ═══════════════════════════════════════════════════════════════
# 2. CORRÉLATIONS AVEC LE NSE
# ═══════════════════════════════════════════════════════════════
print("=" * 70)
print("2. CORRÉLATIONS AVEC LE NSE")
print("=" * 70)

num_cols = [c for c in df.columns if c not in ['station_id', 'NSE', 'date_debut', 'date_fin']
            and df[c].dtype in [np.float64, np.int64, float, int]]

correlations = []
for col in num_cols:
    valid = df[['NSE', col]].dropna()
    if len(valid) < 5:
        continue
    r, p = stats.pearsonr(valid['NSE'], valid[col])
    rho, p_s = stats.spearmanr(valid['NSE'], valid[col])
    correlations.append({
        'variable': col,
        'pearson_r': r,
        'pearson_p': p,
        'spearman_rho': rho,
        'spearman_p': p_s,
        'n': len(valid),
    })

df_corr = pd.DataFrame(correlations).sort_values('spearman_rho', ascending=False, key=abs)

print(f"\n  {'Variable':<30} {'Spearman ρ':>11} {'p-value':>9} {'Pearson r':>10} {'Verdict'}")
print("  " + "─" * 80)
for _, row in df_corr.iterrows():
    sig = "***" if row['spearman_p'] < 0.001 else "**" if row['spearman_p'] < 0.01 else "*" if row['spearman_p'] < 0.05 else ""
    verdict = ""
    if abs(row['spearman_rho']) > 0.4 and row['spearman_p'] < 0.05:
        direction = "↑" if row['spearman_rho'] > 0 else "↓"
        verdict = f"IMPORTANT {direction}"
    print(f"  {row['variable']:<30} {row['spearman_rho']:>+10.3f} {row['spearman_p']:>9.4f} "
          f"{row['pearson_r']:>+9.3f}  {sig} {verdict}")

# ═══════════════════════════════════════════════════════════════
# 3. COMPARAISON BONNES vs MAUVAISES STATIONS
# ═══════════════════════════════════════════════════════════════
print(f"\n{'=' * 70}")
print("3. COMPARAISON BONNES vs MAUVAISES STATIONS")
print("=" * 70)

seuil = df['NSE'].median()
df['groupe'] = np.where(df['NSE'] >= seuil, 'BONNE', 'MAUVAISE')

bonnes = df[df['groupe'] == 'BONNE']
mauvaises = df[df['groupe'] == 'MAUVAISE']

print(f"\n  Seuil : NSE médian = {seuil:.3f}")
print(f"  Bonnes : {len(bonnes)} stations (NSE {bonnes['NSE'].min():.2f} → {bonnes['NSE'].max():.2f})")
print(f"  Mauvaises : {len(mauvaises)} stations (NSE {mauvaises['NSE'].min():.2f} → {mauvaises['NSE'].max():.2f})")

# Variables à comparer
compare_vars = [v for v in num_cols if v in df.columns and df[v].notna().sum() >= 10]

print(f"\n  {'Variable':<30} {'Bonnes':>10} {'Mauvaises':>10} {'Diff%':>8} {'p-value':>9}")
print("  " + "─" * 72)

for var in compare_vars:
    b = bonnes[var].dropna()
    m = mauvaises[var].dropna()
    if len(b) < 3 or len(m) < 3:
        continue

    med_b = b.median()
    med_m = m.median()
    diff_pct = ((med_b - med_m) / abs(med_m) * 100) if abs(med_m) > 0.001 else 0

    # Test Mann-Whitney
    try:
        _, p = stats.mannwhitneyu(b, m, alternative='two-sided')
    except:
        p = 1.0

    sig = "***" if p < 0.001 else "**" if p < 0.01 else "*" if p < 0.05 else ""
    print(f"  {var:<30} {med_b:>10.2f} {med_m:>10.2f} {diff_pct:>+7.0f}% {p:>8.4f} {sig}")

# ═══════════════════════════════════════════════════════════════
# 4. FIGURES
# ═══════════════════════════════════════════════════════════════
print(f"\n{'=' * 70}")
print("4. FIGURES")
print("=" * 70)

# ── Figure 1 : top corrélations (scatter plots) ────────────────────────
top_vars = df_corr.head(8)['variable'].tolist()
n_plots = len(top_vars)
n_cols_fig = 4
n_rows_fig = (n_plots + n_cols_fig - 1) // n_cols_fig

fig, axes = plt.subplots(n_rows_fig, n_cols_fig, figsize=(18, 4.5 * n_rows_fig))
axes = axes.flatten()
fig.suptitle("Corrélations avec le NSE zero-shot", fontsize=14, fontweight='bold')

for idx, var in enumerate(top_vars):
    ax = axes[idx]
    valid = df[['NSE', var, 'station_id']].dropna()
    rho = df_corr[df_corr['variable'] == var]['spearman_rho'].values[0]
    p_val = df_corr[df_corr['variable'] == var]['spearman_p'].values[0]

    colors = ['#4CAF50' if nse >= seuil else '#F44336' for nse in valid['NSE']]
    ax.scatter(valid[var], valid['NSE'], c=colors, s=40, alpha=0.7, edgecolors='white')
    ax.set_xlabel(var, fontsize=9)
    ax.set_ylabel("NSE")

    sig_str = f"p={p_val:.3f}" if p_val >= 0.001 else "p<0.001"
    ax.set_title(f"ρ={rho:+.3f} ({sig_str})", fontsize=10)
    ax.axhline(seuil, color='gray', linestyle='--', linewidth=0.8, alpha=0.5)
    ax.grid(True, alpha=0.2)

for idx in range(n_plots, len(axes)):
    axes[idx].set_visible(False)

plt.tight_layout()
plt.savefig(OUT_DIR / "correlation_NSE_scatter.png", dpi=150, bbox_inches='tight')
plt.close()
print(f"  → {OUT_DIR / 'correlation_NSE_scatter.png'}")

# ── Figure 2 : barplot des corrélations ─────────────────────────────────
fig, ax = plt.subplots(figsize=(12, 6))
df_corr_plot = df_corr.head(15).sort_values('spearman_rho')
colors = ['#4CAF50' if r > 0 else '#F44336' for r in df_corr_plot['spearman_rho']]
ax.barh(range(len(df_corr_plot)), df_corr_plot['spearman_rho'], color=colors, alpha=0.8)
ax.set_yticks(range(len(df_corr_plot)))
ax.set_yticklabels(df_corr_plot['variable'], fontsize=9)
ax.set_xlabel("Spearman ρ avec NSE")
ax.set_title("Top 15 corrélations avec le NSE zero-shot")
ax.axvline(0, color='black', linewidth=0.5)
ax.grid(True, alpha=0.3, axis='x')

# Marquer les significatives
for i, (_, row) in enumerate(df_corr_plot.iterrows()):
    if row['spearman_p'] < 0.05:
        ax.text(row['spearman_rho'] + 0.02 * np.sign(row['spearman_rho']),
                i, "*", fontsize=14, ha='center', va='center', color='red')

plt.tight_layout()
plt.savefig(OUT_DIR / "correlation_NSE_barplot.png", dpi=150, bbox_inches='tight')
plt.close()
print(f"  → {OUT_DIR / 'correlation_NSE_barplot.png'}")

# ── Figure 3 : comparaison bonnes vs mauvaises (boxplots) ──────────────
key_vars = [v for v in ['couverture_pct', 'nb_mesures_snap', 'elevation_mean',
            'aire_km2', 'strahler', 'wl_range', 'wl_seasonal_range',
            'interval_max', 'nb_gaps_30j', 'max_trou_consecutif_10D',
            'slope_mean', 'duree_ans'] if v in df.columns]

n_kv = len(key_vars)
fig, axes = plt.subplots(2, (n_kv + 1) // 2, figsize=(18, 8))
axes = axes.flatten()
fig.suptitle(f"Bonnes (NSE≥{seuil:.2f}) vs Mauvaises stations", fontsize=14, fontweight='bold')

for idx, var in enumerate(key_vars):
    ax = axes[idx]
    data = [bonnes[var].dropna(), mauvaises[var].dropna()]
    bp = ax.boxplot(data, labels=['Bonnes', 'Mauvaises'], patch_artist=True,
                    widths=0.5)
    bp['boxes'][0].set_facecolor('#4CAF50')
    bp['boxes'][0].set_alpha(0.6)
    bp['boxes'][1].set_facecolor('#F44336')
    bp['boxes'][1].set_alpha(0.6)
    ax.set_title(var, fontsize=10)
    ax.grid(True, alpha=0.3, axis='y')

for idx in range(n_kv, len(axes)):
    axes[idx].set_visible(False)

plt.tight_layout()
plt.savefig(OUT_DIR / "comparaison_bonnes_mauvaises.png", dpi=150, bbox_inches='tight')
plt.close()
print(f"  → {OUT_DIR / 'comparaison_bonnes_mauvaises.png'}")

# ── Figure 4 : carte géographique colorée par NSE ──────────────────────
if 'lon' in df.columns and 'lat' in df.columns:
    fig, ax = plt.subplots(figsize=(10, 8))
    scatter = ax.scatter(df['lon'], df['lat'], c=df['NSE'], cmap='RdYlGn',
                         s=80, edgecolors='black', linewidth=0.5, vmin=-0.1, vmax=0.9)
    plt.colorbar(scatter, ax=ax, label='NSE zero-shot')

    for _, row in df.iterrows():
        ax.annotate(f"{row['NSE']:.2f}", (row['lon'], row['lat']),
                    fontsize=6, ha='center', va='bottom',
                    xytext=(0, 5), textcoords='offset points')

    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.set_title("NSE zero-shot par station satellite")
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(OUT_DIR / "carte_NSE_zeroshot.png", dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  → {OUT_DIR / 'carte_NSE_zeroshot.png'}")

# ═══════════════════════════════════════════════════════════════
# 5. EXPORT CSV COMPLET
# ═══════════════════════════════════════════════════════════════
csv_path = OUT_DIR / "tableau_complet_zeroshot.csv"
df.to_csv(csv_path, index=False, float_format='%.3f')
print(f"\n  → Tableau complet : {csv_path}")

# ═══════════════════════════════════════════════════════════════
# 6. RÉSUMÉ TEXTUEL
# ═══════════════════════════════════════════════════════════════
print(f"\n{'=' * 70}")
print("RÉSUMÉ DES FACTEURS CLÉS")
print("=" * 70)

sig_vars = df_corr[(df_corr['spearman_p'] < 0.05) & (df_corr['spearman_rho'].abs() > 0.3)]
if not sig_vars.empty:
    print("\n  Facteurs significativement corrélés au NSE (|ρ|>0.3, p<0.05) :")
    for _, row in sig_vars.iterrows():
        direction = "AIDE" if row['spearman_rho'] > 0 else "NUIT"
        print(f"    {row['variable']:<30} ρ={row['spearman_rho']:+.3f}  → {direction}")
else:
    print("\n  Aucune corrélation fortement significative trouvée (|ρ|>0.3, p<0.05)")
    print("  Top 5 tendances :")
    for _, row in df_corr.head(5).iterrows():
        direction = "+" if row['spearman_rho'] > 0 else "-"
        print(f"    {row['variable']:<30} ρ={row['spearman_rho']:+.3f} (p={row['spearman_p']:.3f}) {direction}")

print(f"\n✅ Analyse terminée. Tous les fichiers dans {OUT_DIR}/")

conn.close()