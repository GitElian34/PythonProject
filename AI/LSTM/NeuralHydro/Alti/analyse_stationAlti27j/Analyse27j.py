"""
analyse_stations_alti_complete.py
═══════════════════════════════════════════════════════════════════════════
Analyse les caractéristiques intrinsèques de TOUTES les stations
altimétriques (222 stations) pour identifier des profils, clusters,
et spécificités.

Produit :
  ./Exploring_data/Analyse_stations_alti/
    - profil_global.png           (distributions des variables clés)
    - carte_stations.png          (carte colorée par altitude/strahler/aire)
    - clusters_stations.png       (PCA + clustering)
    - comparaison_10j_vs_27j.png  (différences entre les deux fréquences)
    - matrice_correlations.png
    - tableau_complet.csv

Usage :
  python analyse_stations_alti_complete.py
═══════════════════════════════════════════════════════════════════════════
"""

import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from pathlib import Path
from collections import Counter
from scipy import stats
import warnings
warnings.filterwarnings('ignore')

# ─── Config ─────────────────────────────────────────────────────────────────
DB_PATH = './data/hydro_data.db'
OUT_DIR = Path('./Exploring_data/Analyse_stations_alti')
OUT_DIR.mkdir(parents=True, exist_ok=True)

conn = sqlite3.connect(DB_PATH)

print("╔══════════════════════════════════════════════════════════════╗")
print("║  ANALYSE COMPLÈTE DES STATIONS ALTIMÉTRIQUES               ║")
print("╚══════════════════════════════════════════════════════════════╝\n")

# ═══════════════════════════════════════════════════════════════
# 1. COLLECTE DE TOUTES LES DONNÉES
# ═══════════════════════════════════════════════════════════════
print("1. Collecte des données...\n")

stations = pd.read_sql('''
    SELECT station_code, hydroweb_name, river_name, basin_name,
           reference_longitude AS lon, reference_latitude AS lat,
           upstream_watershed_km2 AS aire_km2, mean_altitude, strahler,
           elevation_mean, slope_mean, dist_barrage_m
    FROM stations
    ORDER BY station_code
''', conn)

rows = []
for _, sta in stations.iterrows():
    code = sta['station_code']
    row = sta.to_dict()

    # ── Corine / sol ────────────────────────────────────────────────
    corine = pd.read_sql('''
        SELECT frac_urban, frac_agriculture, frac_forest, frac_semi_natural,
               frac_wetland, frac_water, sg_clay_0_30cm, sg_sand_0_30cm, sg_silt_0_30cm
        FROM bv_corine WHERE station_code = ?
    ''', conn, params=(code,))
    if not corine.empty:
        for col in corine.columns:
            row[col] = corine[col].iloc[0]

    # ── Mesures satellite ───────────────────────────────────────────
    df_mes = pd.read_sql('''
        SELECT measure_date, orthometric_height, satellite
        FROM measurements
        WHERE station_code = ? AND is_valid = 1
              AND orthometric_height IS NOT NULL
        ORDER BY measure_date
    ''', conn, params=(code,))

    if not df_mes.empty:
        df_mes['measure_date'] = pd.to_datetime(df_mes['measure_date'])
        wl = df_mes['orthometric_height'].astype(float)

        row['nb_mesures'] = len(df_mes)
        row['date_debut'] = df_mes['measure_date'].min()
        row['date_fin'] = df_mes['measure_date'].max()
        row['duree_ans'] = (row['date_fin'] - row['date_debut']).days / 365.25

        # Satellites utilisés
        row['satellites'] = ','.join(sorted(df_mes['satellite'].dropna().unique()))
        row['nb_satellites'] = df_mes['satellite'].dropna().nunique()

        # Intervalles
        intervals = df_mes['measure_date'].diff().dt.days.dropna().values
        if len(intervals) > 0:
            row['interval_mode'] = Counter(intervals.astype(int)).most_common(1)[0][0]
            row['interval_median'] = np.median(intervals)
            row['interval_mean'] = np.mean(intervals)
            row['interval_std'] = np.std(intervals)
            row['interval_max'] = np.max(intervals)
            row['interval_min'] = np.min(intervals)
            row['nb_gaps_30j'] = int(np.sum(intervals > 30))
            row['nb_gaps_60j'] = int(np.sum(intervals > 60))
            row['nb_gaps_90j'] = int(np.sum(intervals > 90))
            row['pct_regulier'] = np.sum((intervals >= row['interval_mode'] - 2) &
                                         (intervals <= row['interval_mode'] + 2)) / len(intervals) * 100

        # Water level
        row['wl_mean'] = wl.mean()
        row['wl_std'] = wl.std()
        row['wl_range'] = wl.max() - wl.min()
        row['wl_iqr'] = wl.quantile(0.75) - wl.quantile(0.25)
        row['wl_skew'] = wl.skew()
        row['wl_kurtosis'] = wl.kurtosis()

        # Saisonnalité
        df_mes['month'] = df_mes['measure_date'].dt.month
        monthly = df_mes.groupby('month')['orthometric_height'].mean()
        if len(monthly) >= 6:
            row['wl_seasonal_range'] = monthly.max() - monthly.min()
            row['month_max_wl'] = monthly.idxmax()
            row['month_min_wl'] = monthly.idxmin()

        # Tendance linéaire
        if len(wl) >= 20:
            x = np.arange(len(wl))
            slope, intercept, r_value, p_value, std_err = stats.linregress(x, wl.values)
            row['wl_trend_slope'] = slope
            row['wl_trend_r2'] = r_value**2
            row['wl_trend_pvalue'] = p_value

    # ── Classification fréquence ────────────────────────────────────
    if 'interval_mode' in row:
        mode = row['interval_mode']
        if 8 <= mode <= 12:
            row['freq_class'] = '10j'
        elif 25 <= mode <= 29:
            row['freq_class'] = '27j'
        elif 19 <= mode <= 23:
            row['freq_class'] = '21j'
        elif 33 <= mode <= 37:
            row['freq_class'] = '35j'
        else:
            row['freq_class'] = f'{mode}j'

    rows.append(row)

df = pd.DataFrame(rows)
print(f"  {len(df)} stations collectées avec {len(df.columns)} variables\n")

# ═══════════════════════════════════════════════════════════════
# 2. PROFIL GLOBAL
# ═══════════════════════════════════════════════════════════════
print("=" * 70)
print("2. PROFIL GLOBAL")
print("=" * 70)

print(f"\n  Stations totales : {len(df)}")
if 'freq_class' in df.columns:
    print(f"\n  Distribution par fréquence :")
    for freq, count in df['freq_class'].value_counts().items():
        print(f"    {freq:>5s} : {count:3d} stations")

print(f"\n  {'Variable':<25s} {'Médiane':>10s} {'Moy':>10s} {'Min':>10s} {'Max':>10s} {'Std':>10s}")
print("  " + "─" * 70)
for var in ['aire_km2', 'mean_altitude', 'elevation_mean', 'slope_mean', 'strahler',
            'nb_mesures', 'duree_ans', 'interval_mode', 'wl_range', 'wl_std',
            'wl_seasonal_range', 'nb_gaps_60j', 'pct_regulier']:
    if var in df.columns:
        v = df[var].dropna()
        if len(v) > 0:
            print(f"  {var:<25s} {v.median():10.1f} {v.mean():10.1f} {v.min():10.1f} {v.max():10.1f} {v.std():10.1f}")

# ═══════════════════════════════════════════════════════════════
# 3. COMPARAISON 10j vs 27j
# ═══════════════════════════════════════════════════════════════
if 'freq_class' in df.columns:
    print(f"\n{'=' * 70}")
    print("3. COMPARAISON 10j vs 27j")
    print("=" * 70)

    df_10j = df[df['freq_class'] == '10j']
    df_27j = df[df['freq_class'] == '27j']

    print(f"\n  {'Variable':<25s} {'10j (n={})'.format(len(df_10j)):>12s} {'27j (n={})'.format(len(df_27j)):>12s} {'p-value':>9s}")
    print("  " + "─" * 60)

    compare_vars = ['aire_km2', 'mean_altitude', 'elevation_mean', 'slope_mean',
                    'strahler', 'lon', 'lat', 'nb_mesures', 'wl_range', 'wl_std',
                    'wl_seasonal_range', 'nb_gaps_60j', 'pct_regulier',
                    'frac_agriculture', 'frac_forest', 'sg_clay_0_30cm', 'sg_sand_0_30cm']

    for var in compare_vars:
        if var not in df.columns:
            continue
        v10 = df_10j[var].dropna()
        v27 = df_27j[var].dropna()
        if len(v10) < 3 or len(v27) < 3:
            continue
        try:
            _, p = stats.mannwhitneyu(v10, v27, alternative='two-sided')
        except:
            p = 1.0
        sig = "***" if p < 0.001 else "**" if p < 0.01 else "*" if p < 0.05 else ""
        print(f"  {var:<25s} {v10.median():12.2f} {v27.median():12.2f} {p:9.4f} {sig}")

# ═══════════════════════════════════════════════════════════════
# 4. FIGURES
# ═══════════════════════════════════════════════════════════════
print(f"\n{'=' * 70}")
print("4. FIGURES")
print("=" * 70)

# ── Figure 1 : distributions des variables clés ───────────────────────
fig, axes = plt.subplots(3, 4, figsize=(18, 12))
axes = axes.flatten()
fig.suptitle(f'Profil des {len(df)} stations altimétriques', fontsize=14, fontweight='bold')

hist_vars = ['aire_km2', 'mean_altitude', 'elevation_mean', 'slope_mean',
             'strahler', 'nb_mesures', 'wl_range', 'wl_std',
             'wl_seasonal_range', 'interval_mode', 'pct_regulier', 'nb_gaps_60j']

for idx, var in enumerate(hist_vars):
    ax = axes[idx]
    if var not in df.columns:
        ax.set_visible(False)
        continue
    data = df[var].dropna()
    if len(data) == 0:
        ax.set_visible(False)
        continue

    if 'freq_class' in df.columns:
        for freq, color, label in [('10j', '#2196F3', '10j'), ('27j', '#FF9800', '27j')]:
            sub = df[df['freq_class'] == freq][var].dropna()
            if len(sub) > 0:
                ax.hist(sub, bins=20, alpha=0.5, color=color, label=label, edgecolor='white')
        ax.legend(fontsize=7)
    else:
        ax.hist(data, bins=20, color='steelblue', alpha=0.7, edgecolor='white')

    ax.set_title(var, fontsize=10)
    ax.grid(True, alpha=0.2)

plt.tight_layout()
fig.savefig(OUT_DIR / 'profil_global.png', dpi=150, bbox_inches='tight')
plt.close()
print(f"  → {OUT_DIR / 'profil_global.png'}")

# ── Figure 2 : cartes géographiques ───────────────────────────────────
if 'lon' in df.columns and 'lat' in df.columns:
    fig, axes = plt.subplots(2, 2, figsize=(16, 14))

    for ax, var, cmap, title in [
        (axes[0, 0], 'mean_altitude', 'terrain', 'Altitude moyenne (m)'),
        (axes[0, 1], 'aire_km2', 'YlOrRd', 'Surface BV (km²)'),
        (axes[1, 0], 'wl_range', 'coolwarm', 'Amplitude WL (m)'),
        (axes[1, 1], 'strahler', 'viridis', 'Ordre Strahler'),
    ]:
        if var not in df.columns:
            ax.set_visible(False)
            continue
        valid = df[['lon', 'lat', var]].dropna()
        if var == 'aire_km2':
            sc = ax.scatter(valid['lon'], valid['lat'], c=np.log10(valid[var].clip(lower=1)),
                           cmap=cmap, s=30, edgecolors='black', linewidth=0.3, alpha=0.8)
            plt.colorbar(sc, ax=ax, label=f'log10({var})')
        else:
            sc = ax.scatter(valid['lon'], valid['lat'], c=valid[var],
                           cmap=cmap, s=30, edgecolors='black', linewidth=0.3, alpha=0.8)
            plt.colorbar(sc, ax=ax, label=var)
        ax.set_title(title, fontsize=11, fontweight='bold')
        ax.set_xlabel('Longitude')
        ax.set_ylabel('Latitude')
        ax.grid(True, alpha=0.2)

    plt.tight_layout()
    fig.savefig(OUT_DIR / 'carte_stations.png', dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  → {OUT_DIR / 'carte_stations.png'}")

# ── Figure 3 : carte 10j vs 27j ──────────────────────────────────────
if 'freq_class' in df.columns and 'lon' in df.columns:
    fig, ax = plt.subplots(figsize=(12, 9))
    colors_map = {'10j': '#2196F3', '27j': '#FF9800', '21j': '#4CAF50', '35j': '#9C27B0'}

    for freq in df['freq_class'].unique():
        sub = df[df['freq_class'] == freq]
        color = colors_map.get(freq, '#999999')
        ax.scatter(sub['lon'], sub['lat'], c=color, s=40, label=f'{freq} (n={len(sub)})',
                   edgecolors='black', linewidth=0.3, alpha=0.8)

    ax.legend(fontsize=10, loc='upper left')
    ax.set_title('Répartition géographique par fréquence de mesure', fontsize=12, fontweight='bold')
    ax.set_xlabel('Longitude')
    ax.set_ylabel('Latitude')
    ax.grid(True, alpha=0.2)
    plt.tight_layout()
    fig.savefig(OUT_DIR / 'carte_freq.png', dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  → {OUT_DIR / 'carte_freq.png'}")

# ── Figure 4 : boxplots 10j vs 27j ───────────────────────────────────
if 'freq_class' in df.columns:
    key_vars = [v for v in ['aire_km2', 'mean_altitude', 'elevation_mean', 'slope_mean',
                'strahler', 'wl_range', 'wl_seasonal_range', 'nb_mesures',
                'pct_regulier', 'nb_gaps_60j'] if v in df.columns]

    fig, axes = plt.subplots(2, 5, figsize=(20, 8))
    axes = axes.flatten()
    fig.suptitle('10j vs 27j — Comparaison des caractéristiques', fontsize=14, fontweight='bold')

    for idx, var in enumerate(key_vars):
        ax = axes[idx]
        data_10 = df_10j[var].dropna()
        data_27 = df_27j[var].dropna()
        bp = ax.boxplot([data_10, data_27], labels=['10j', '27j'], patch_artist=True, widths=0.5)
        bp['boxes'][0].set_facecolor('#2196F3')
        bp['boxes'][0].set_alpha(0.6)
        bp['boxes'][1].set_facecolor('#FF9800')
        bp['boxes'][1].set_alpha(0.6)
        ax.set_title(var, fontsize=9)
        ax.grid(True, alpha=0.3, axis='y')

    for idx in range(len(key_vars), len(axes)):
        axes[idx].set_visible(False)

    plt.tight_layout()
    fig.savefig(OUT_DIR / 'comparaison_10j_vs_27j.png', dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  → {OUT_DIR / 'comparaison_10j_vs_27j.png'}")

# ── Figure 5 : matrice de corrélations ────────────────────────────────
corr_vars = [v for v in ['aire_km2', 'mean_altitude', 'elevation_mean', 'slope_mean',
             'strahler', 'lon', 'lat', 'wl_range', 'wl_std', 'wl_seasonal_range',
             'nb_mesures', 'nb_gaps_60j', 'frac_agriculture', 'frac_forest',
             'sg_clay_0_30cm', 'sg_sand_0_30cm'] if v in df.columns]

if len(corr_vars) >= 4:
    corr_matrix = df[corr_vars].corr(method='spearman')

    fig, ax = plt.subplots(figsize=(12, 10))
    im = ax.imshow(corr_matrix, cmap='RdBu_r', vmin=-1, vmax=1, aspect='auto')
    plt.colorbar(im, ax=ax, label='Spearman ρ')

    ax.set_xticks(range(len(corr_vars)))
    ax.set_yticks(range(len(corr_vars)))
    ax.set_xticklabels(corr_vars, rotation=45, ha='right', fontsize=8)
    ax.set_yticklabels(corr_vars, fontsize=8)

    for i in range(len(corr_vars)):
        for j in range(len(corr_vars)):
            val = corr_matrix.iloc[i, j]
            color = 'white' if abs(val) > 0.5 else 'black'
            ax.text(j, i, f'{val:.2f}', ha='center', va='center', fontsize=6, color=color)

    ax.set_title('Matrice de corrélations (Spearman) — Stations altimétriques', fontweight='bold')
    plt.tight_layout()
    fig.savefig(OUT_DIR / 'matrice_correlations.png', dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  → {OUT_DIR / 'matrice_correlations.png'}")

# ── Figure 6 : stations par bassin versant ────────────────────────────
if 'basin_name' in df.columns:
    basin_counts = df['basin_name'].value_counts().head(15)
    fig, ax = plt.subplots(figsize=(12, 6))

    if 'freq_class' in df.columns:
        basins_top = basin_counts.index.tolist()
        df_top = df[df['basin_name'].isin(basins_top)]
        pivot = df_top.groupby(['basin_name', 'freq_class']).size().unstack(fill_value=0)
        pivot = pivot.loc[basins_top]
        pivot.plot(kind='barh', ax=ax, stacked=True,)
    else:
        basin_counts.plot(kind='barh', ax=ax, color='steelblue')

    ax.set_xlabel('Nombre de stations')
    ax.set_title('Top 15 bassins versants', fontsize=12, fontweight='bold')
    ax.grid(True, alpha=0.2, axis='x')
    plt.tight_layout()
    fig.savefig(OUT_DIR / 'stations_par_bassin.png', dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  → {OUT_DIR / 'stations_par_bassin.png'}")

# ═══════════════════════════════════════════════════════════════
# 5. EXPORT
# ═══════════════════════════════════════════════════════════════
# Convertir les dates pour le CSV
for col in ['date_debut', 'date_fin']:
    if col in df.columns:
        df[col] = df[col].astype(str)

csv_path = OUT_DIR / 'tableau_complet_stations_alti.csv'
df.to_csv(csv_path, index=False, float_format='%.3f')
print(f"\n  → Tableau : {csv_path}")

# ═══════════════════════════════════════════════════════════════
# 6. RÉSUMÉ
# ═══════════════════════════════════════════════════════════════
print(f"\n{'=' * 70}")
print("RÉSUMÉ")
print("=" * 70)

if 'freq_class' in df.columns:
    for freq in ['10j', '27j']:
        sub = df[df['freq_class'] == freq]
        if len(sub) == 0:
            continue
        print(f"\n  --- Stations {freq} ({len(sub)}) ---")
        print(f"    Aire BV médiane     : {sub['aire_km2'].median():.0f} km²")
        print(f"    Altitude médiane    : {sub['mean_altitude'].median():.0f} m")
        print(f"    Strahler médian     : {sub['strahler'].median():.0f}")
        print(f"    WL range médian     : {sub['wl_range'].median():.2f} m")
        print(f"    Nb mesures médian   : {sub['nb_mesures'].median():.0f}")
        print(f"    Gaps >60j médian    : {sub['nb_gaps_60j'].median():.0f}")
        print(f"    Régularité médiane  : {sub['pct_regulier'].median():.0f}%")

print(f"\n✅ Analyse terminée. Tous les fichiers dans {OUT_DIR}/")

conn.close()