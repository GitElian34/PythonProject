"""
Diagnostic complet — stations très bonnes (NSE > 0.7) vs très mauvaises (NSE < 0)
1. Attributs statiques
2. Données dynamiques ERA5
3. Signal hydrologique (water_level)
"""

import sqlite3
import numpy as np
import pandas as pd
import xarray as xr
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from pathlib import Path
from scipy import stats

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
CSV_PATH   = Path("./nse_scores_stations.csv")
NC_DIR     = Path("./data/IA/NeuralHydrology/time_series")
DB_PATH    = "./data/insitu_data.db"
TRAIN_START = "2016-01-01"
TRAIN_END   = "2023-12-31"
TEST_START  = "2024-01-01"
TEST_END    = "2025-12-31"

# ── Chargement des scores ──────────────────────────────────────
df_scores = pd.read_csv(CSV_PATH)
good = df_scores[df_scores['NSE'] >  0.7]['station_id'].tolist()
bad  = df_scores[df_scores['NSE'] <  0.0]['station_id'].tolist()
all_stations = good + bad

print(f"Stations très bonnes (NSE > 0.7) : {len(good)}")
print(f"Stations très mauvaises (NSE < 0) : {len(bad)}")

# ═══════════════════════════════════════════════════════════════
# 1. ATTRIBUTS STATIQUES
# ═══════════════════════════════════════════════════════════════
print("\n" + "="*70)
print("1. ATTRIBUTS STATIQUES")
print("="*70)

conn = sqlite3.connect(DB_PATH)
ph   = ','.join(['?' for _ in all_stations])
df_attrs = pd.read_sql(f'''
    SELECT b.code_sta AS station_id, b.aire_km2,
           s.lon, s.lat,
           c.frac_urban, c.frac_agriculture, c.frac_forest,
           c.frac_semi_natural, c.frac_wetland, c.frac_water,
           c.sg_clay_0_30cm, c.sg_sand_0_30cm, c.sg_silt_0_30cm
    FROM bv_data b
    JOIN bv_corine c       ON b.code_sta = c.code_sta
    JOIN stations_insitu s ON b.code_sta = s.code_sta
    WHERE b.code_sta IN ({ph})
''', conn, params=all_stations)
conn.close()

df_attrs['groupe'] = df_attrs['station_id'].apply(
    lambda x: 'BONNE' if x in good else 'MAUVAISE'
)

VARS_STATIC = ['aire_km2', 'lon', 'lat', 'frac_urban', 'frac_agriculture',
               'frac_forest', 'frac_semi_natural', 'sg_clay_0_30cm',
               'sg_sand_0_30cm', 'sg_silt_0_30cm']

g_bon  = df_attrs[df_attrs['groupe'] == 'BONNE']
g_mauv = df_attrs[df_attrs['groupe'] == 'MAUVAISE']

print(f"\n  {'Variable':<22} {'BONNE med':>10} {'MAUVAISE med':>13} {'BONNE moy':>10} {'MAUVAISE moy':>13} {'p-val':>8}")
print(f"  {'-'*78}")

for var in VARS_STATIC:
    if var not in df_attrs.columns:
        continue
    b_vals = g_bon[var].dropna()
    m_vals = g_mauv[var].dropna()
    if len(b_vals) == 0 or len(m_vals) == 0:
        continue
    _, pval = stats.mannwhitneyu(b_vals, m_vals, alternative='two-sided')
    sig  = "***" if pval < 0.01 else ("**" if pval < 0.05 else ("*" if pval < 0.1 else ""))
    flag = " ◄" if sig else ""
    print(f"  {var:<22} {b_vals.median():>10.3f} {m_vals.median():>13.3f} "
          f"{b_vals.mean():>10.3f} {m_vals.mean():>13.3f} {sig:>8}{flag}")

# Occupation du sol dominante
corine_cols = ['frac_urban', 'frac_agriculture', 'frac_forest',
               'frac_semi_natural', 'frac_wetland', 'frac_water']
for groupe, gdf in df_attrs.groupby('groupe'):
    dominant = gdf[corine_cols].idxmax(axis=1).str.replace('frac_', '')
    print(f"\n  Occupation dominante {groupe} : {dominant.value_counts().to_dict()}")

# Distribution aire_km2
print(f"\n  Distribution aire_km2 :")
for groupe, gdf in df_attrs.groupby('groupe'):
    q = gdf['aire_km2'].quantile([0.25, 0.5, 0.75])
    print(f"    {groupe:<9} : min={gdf['aire_km2'].min():>7.0f}  "
          f"Q25={q[0.25]:>7.0f}  med={q[0.50]:>7.0f}  "
          f"Q75={q[0.75]:>7.0f}  max={gdf['aire_km2'].max():>7.0f}")

# ═══════════════════════════════════════════════════════════════
# 2. DONNÉES DYNAMIQUES ERA5
# ═══════════════════════════════════════════════════════════════
print("\n\n" + "="*70)
print("2. DONNÉES DYNAMIQUES ERA5 (période test 2024-2025)")
print("="*70)

def load_dynamic_stats(stations, period_start, period_end):
    records = []
    for sid in stations:
        nc_path = NC_DIR / f"{sid}.nc"
        if not nc_path.exists():
            continue
        try:
            ds   = xr.open_dataset(nc_path)
            ds_p = ds.sel(date=slice(period_start, period_end))
            rec  = {'station_id': sid}
            for var in ['precipitation', 'temperature', 'pet']:
                if var not in ds_p:
                    continue
                vals  = ds_p[var].values
                valid = vals[~np.isnan(vals)]
                if len(valid) == 0:
                    continue
                rec[f'{var}_mean']    = np.mean(valid)
                rec[f'{var}_std']     = np.std(valid)
                rec[f'{var}_p95']     = np.percentile(valid, 95)
                rec[f'{var}_nan_pct'] = np.mean(np.isnan(vals)) * 100
            ds.close()
            records.append(rec)
        except Exception:
            continue
    return pd.DataFrame(records)

bon_dyn  = load_dynamic_stats(good, TEST_START, TEST_END)
mauv_dyn = load_dynamic_stats(bad,  TEST_START, TEST_END)

VARS_DYN = ['precipitation_mean', 'precipitation_std', 'precipitation_p95',
            'temperature_mean', 'pet_mean', 'pet_std']

print(f"\n  {'Variable':<25} {'BONNE moy':>10} {'MAUVAISE moy':>13} {'Δ':>8}  {'p-val':>6}")
print(f"  {'-'*65}")

for var in VARS_DYN:
    if var not in bon_dyn.columns or var not in mauv_dyn.columns:
        continue
    b = bon_dyn[var].dropna()
    m = mauv_dyn[var].dropna()
    if len(b) == 0 or len(m) == 0:
        continue
    _, pval = stats.mannwhitneyu(b, m, alternative='two-sided')
    sig  = "***" if pval < 0.01 else ("**" if pval < 0.05 else ("*" if pval < 0.1 else ""))
    flag = " ◄" if sig else ""
    print(f"  {var:<25} {b.mean():>10.3f} {m.mean():>13.3f} "
          f"{b.mean()-m.mean():>+8.3f}  {sig:>6}{flag}")

# ═══════════════════════════════════════════════════════════════
# 3. SIGNAL HYDROLOGIQUE — WATER LEVEL
# ═══════════════════════════════════════════════════════════════
print("\n\n" + "="*70)
print("3. SIGNAL HYDROLOGIQUE — WATER LEVEL")
print("="*70)

def load_wl_stats(stations, period_start, period_end):
    records = []
    for sid in stations:
        nc_path = NC_DIR / f"{sid}.nc"
        if not nc_path.exists():
            continue
        try:
            ds   = xr.open_dataset(nc_path)
            ds_p = ds.sel(date=slice(period_start, period_end))
            wl   = ds_p['water_level'].values
            ds.close()
            valid = wl[~np.isnan(wl)]
            if len(valid) < 30:
                continue
            autocorr = pd.Series(valid).autocorr(lag=1)
            # Dérive train→test
            ds2      = xr.open_dataset(nc_path)
            wl_train = ds2.sel(date=slice(TRAIN_START, TRAIN_END))['water_level'].values
            ds2.close()
            valid_tr = wl_train[~np.isnan(wl_train)]
            records.append({
                'station_id'  : sid,
                'wl_mean'     : np.mean(valid),
                'wl_std'      : np.std(valid),
                'wl_p05'      : np.percentile(valid, 5),
                'wl_p95'      : np.percentile(valid, 95),
                'wl_range'    : np.percentile(valid, 95) - np.percentile(valid, 5),
                'wl_autocorr' : autocorr,
                'wl_nan_pct'  : np.mean(np.isnan(wl)) * 100,
                'wl_mean_train': np.mean(valid_tr) if len(valid_tr) > 0 else np.nan,
                'wl_std_train' : np.std(valid_tr)  if len(valid_tr) > 0 else np.nan,
                'drift_mean'  : np.mean(valid) - (np.mean(valid_tr) if len(valid_tr) > 0 else np.nan),
                'drift_std'   : np.std(valid)  - (np.std(valid_tr)  if len(valid_tr) > 0 else np.nan),
            })
        except Exception:
            continue
    return pd.DataFrame(records)

bon_wl  = load_wl_stats(good, TEST_START, TEST_END)
mauv_wl = load_wl_stats(bad,  TEST_START, TEST_END)

VARS_WL = ['wl_mean', 'wl_std', 'wl_p05', 'wl_p95', 'wl_range',
           'wl_autocorr', 'wl_nan_pct', 'drift_mean', 'drift_std']

print(f"\n  {'Variable':<20} {'BONNE moy':>10} {'MAUVAISE moy':>13} {'Δ':>8}  {'p-val':>6}")
print(f"  {'-'*60}")

for var in VARS_WL:
    if var not in bon_wl.columns or var not in mauv_wl.columns:
        continue
    b = bon_wl[var].dropna()
    m = mauv_wl[var].dropna()
    if len(b) == 0 or len(m) == 0:
        continue
    _, pval = stats.mannwhitneyu(b, m, alternative='two-sided')
    sig  = "***" if pval < 0.01 else ("**" if pval < 0.05 else ("*" if pval < 0.1 else ""))
    flag = " ◄" if sig else ""
    print(f"  {var:<20} {b.mean():>10.3f} {m.mean():>13.3f} "
          f"{b.mean()-m.mean():>+8.3f}  {sig:>6}{flag}")

# ═══════════════════════════════════════════════════════════════
# PLOTS
# ═══════════════════════════════════════════════════════════════
print("\nGénération des plots...")

fig = plt.figure(figsize=(18, 12))
fig.suptitle("Diagnostic complet — BONNES (NSE>0.7) vs MAUVAISES (NSE<0)", fontsize=13)
gs  = gridspec.GridSpec(3, 4, figure=fig, hspace=0.45, wspace=0.35)

colors = {'BONNE': 'steelblue', 'MAUVAISE': 'crimson'}

def hist_compare(ax, b_vals, m_vals, title, xlabel):
    ax.hist(b_vals.dropna(), bins=12, alpha=0.6, color='steelblue',
            label=f'BONNE (n={len(b_vals.dropna())})', density=True)
    ax.hist(m_vals.dropna(), bins=12, alpha=0.6, color='crimson',
            label=f'MAUVAISE (n={len(m_vals.dropna())})', density=True)
    ax.axvline(b_vals.mean(), color='steelblue', lw=2, ls='--')
    ax.axvline(m_vals.mean(), color='crimson',   lw=2, ls='--')
    ax.set_title(title, fontsize=9)
    ax.set_xlabel(xlabel, fontsize=8)
    ax.legend(fontsize=7)
    ax.grid(True, alpha=0.3)

# Ligne 1 — Statiques
hist_compare(fig.add_subplot(gs[0, 0]), g_bon['aire_km2'],   g_mauv['aire_km2'],   "aire_km2",         "km²")
hist_compare(fig.add_subplot(gs[0, 1]), g_bon['lon'],         g_mauv['lon'],         "Longitude",        "°")
hist_compare(fig.add_subplot(gs[0, 2]), g_bon['frac_agriculture'], g_mauv['frac_agriculture'], "frac_agriculture", "")
hist_compare(fig.add_subplot(gs[0, 3]), g_bon['frac_forest'], g_mauv['frac_forest'], "frac_forest",      "")

# Ligne 2 — ERA5
hist_compare(fig.add_subplot(gs[1, 0]), bon_dyn['precipitation_mean'], mauv_dyn['precipitation_mean'], "Précip. moy.", "mm/j")
hist_compare(fig.add_subplot(gs[1, 1]), bon_dyn['temperature_mean'],   mauv_dyn['temperature_mean'],   "Temp. moy.",   "°C")
hist_compare(fig.add_subplot(gs[1, 2]), bon_dyn['pet_mean'],           mauv_dyn['pet_mean'],           "PET moy.",     "mm/j")
hist_compare(fig.add_subplot(gs[1, 3]), bon_dyn['precipitation_p95'],  mauv_dyn['precipitation_p95'],  "Précip. P95",  "mm/j")

# Ligne 3 — Water level
hist_compare(fig.add_subplot(gs[2, 0]), bon_wl['wl_std'],      mauv_wl['wl_std'],      "WL std",          "norm.")
hist_compare(fig.add_subplot(gs[2, 1]), bon_wl['wl_range'],    mauv_wl['wl_range'],    "WL range P5-P95", "norm.")
hist_compare(fig.add_subplot(gs[2, 2]), bon_wl['wl_autocorr'], mauv_wl['wl_autocorr'], "WL autocorr lag1","")
hist_compare(fig.add_subplot(gs[2, 3]), bon_wl['drift_mean'],  mauv_wl['drift_mean'],  "Dérive moy. train→test", "norm.")

out_path = Path("./diagnostic_complet.png")
fig.savefig(out_path, dpi=150, bbox_inches='tight')
plt.close()
print(f"  ✅ Plot → {out_path}")