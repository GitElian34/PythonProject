"""
analyse_outliers_vs_nooutliers.py
═══════════════════════════════════════════════════════════════════════════
Comparaison outliers vs non-outliers sur les variables dynamiques.
Lit les fichiers déjà générés — pas besoin de relancer l'évaluation.

Entrées :
  - data/outlier_detection/diagnostic_global_27j.csv  (outliers + variables)
  - data/outlier_detection/residuals_27j_all_stations.csv  (toutes les dates)
  - data/IA/NeuralHydrology_feat27j/time_series/  (.nc pour les non-outliers)

Sorties :
  - data/outlier_detection/diagnostic_comparison_27j.csv
═══════════════════════════════════════════════════════════════════════════
"""

import numpy as np
import pandas as pd
import xarray as xr
import warnings
from pathlib import Path
from scipy import stats as scipy_stats

warnings.filterwarnings('ignore')

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
GLOBAL_CSV     = Path("./data/outlier_detection/diagnostic_global_27j.csv")
RESIDUALS_CSV  = Path("./data/outlier_detection/residuals_27j_all_stations.csv")
NC_DIR         = Path("./data/IA/NeuralHydrology_feat27j")
OUT_CSV        = Path("./data/outlier_detection/diagnostic_comparison_27j.csv")

DIAG_VARS = [
    'precipitation_J0', 'temperature_J0', 'pet_J0',
    'precip_mean_J3', 'pet_mean_J3', 'temp_mean_J3',
    'precip_mean_J27', 'precip_mean_J10', 'temp_mean_J10',
    'clim_mean_20j', 'clim_std_20j',
    'precip_max_J27', 'precip_last7', 'nb_jours_pluie_J27', 'precip_mean_J14',
]

# ═══════════════════════════════════════════════════════════════
# CHARGEMENT
# ═══════════════════════════════════════════════════════════════
print("Chargement des fichiers...")
df_global = pd.read_csv(GLOBAL_CSV, parse_dates=['date'])
df_global['station'] = df_global['station'].astype(str)
df_residuals = pd.read_csv(RESIDUALS_CSV, parse_dates=['date'])
df_residuals['station'] = df_residuals['station'].astype(str)

print(f"  Outliers      : {len(df_global)} ({df_global['station'].nunique()} stations)")
print(f"  Toutes dates  : {len(df_residuals)}")

# ═══════════════════════════════════════════════════════════════
# EXTRACTION DES VALEURS NON-OUTLIERS
# ═══════════════════════════════════════════════════════════════
print("\nExtraction des valeurs non-outliers depuis les .nc...")

non_out_rows = []
stations = df_global['station'].unique()

for i, sta in enumerate(stations):
    # Chercher le .nc
    nc_path = None
    for candidate in [sta, sta.lstrip('0'), sta.zfill(13)]:
        p = NC_DIR / 'time_series' / f"{candidate}.nc"
        if p.exists():
            nc_path = p
            break
    if nc_path is None:
        print(f"  ⚠  .nc introuvable pour {sta}")
        continue

    ds = xr.open_dataset(nc_path)
    dates_nc   = pd.to_datetime(ds.date.values)
    vars_dispo = [v for v in DIAG_VARS if v in ds]

    # Dates outliers de cette station
    dates_out = set(
        df_global[df_global['station'] == sta]['date'].dt.normalize()
    )

    for j, d in enumerate(dates_nc):
        if pd.Timestamp(d).normalize() in dates_out:
            continue
        # Vérifie que cette date a bien une obs (pas NaN water_level)
        if 'water_level' in ds:
            wl = float(ds['water_level'].values.flatten()[j])
            if np.isnan(wl):
                continue
        row = {'station': sta}
        for var in vars_dispo:
            val = float(ds[var].values.flatten()[j])
            row[f'{var}_val'] = val if not np.isnan(val) else np.nan
        non_out_rows.append(row)

    ds.close()
    print(f"  [{i+1:3d}/{len(stations)}] {sta} — {len(dates_out)} outliers exclus")

df_non_out = pd.DataFrame(non_out_rows)
print(f"\n  Non-outliers : {len(df_non_out)} observations")

# ═══════════════════════════════════════════════════════════════
# COMPARAISON VARIABLE PAR VARIABLE
# ═══════════════════════════════════════════════════════════════
print(f"\n{'='*75}")
print(f"COMPARAISON OUTLIERS vs NON-OUTLIERS")
print(f"{'='*75}")
print(f"  Outliers     : {len(df_global)}")
print(f"  Non-outliers : {len(df_non_out)}")
print(f"\n  {'Variable':<30} {'Moy OUT':>9} {'Moy NON':>9} {'Diff%':>7} {'p-value':>10} {'Cohen d':>9} {'Verdict'}")
print(f"  {'-'*90}")

comparison_rows = []

for var in DIAG_VARS:
    col_val = f'{var}_val'

    # Outliers : valeur dans diagnostic_global
    col_out = col_val if col_val in df_global.columns else f'{var}_val'
    if col_out not in df_global.columns:
        # essayer sans _val (colonne directe)
        if var in df_global.columns:
            out_vals = df_global[var].dropna().values
        else:
            continue
    else:
        out_vals = df_global[col_out].dropna().values

    if col_val not in df_non_out.columns:
        continue
    non_out_vals = df_non_out[col_val].dropna().values

    if len(out_vals) < 5 or len(non_out_vals) < 5:
        continue

    # Mann-Whitney U
    try:
        _, p_val = scipy_stats.mannwhitneyu(out_vals, non_out_vals, alternative='two-sided')
    except Exception:
        p_val = 1.0

    # Cohen's d
    mean_out = np.mean(out_vals)
    mean_non = np.mean(non_out_vals)
    pooled_std = np.sqrt((np.std(out_vals)**2 + np.std(non_out_vals)**2) / 2)
    cohens_d = (mean_out - mean_non) / pooled_std if pooled_std > 0 else 0.0
    diff_pct = (mean_out - mean_non) / abs(mean_non) * 100 if abs(mean_non) > 1e-6 else 0.0

    # Verdict
    if p_val < 0.05 and abs(cohens_d) > 0.5:
        verdict = "★ SIGNIFICATIF"
    elif p_val < 0.05 and abs(cohens_d) > 0.2:
        verdict = "~ modéré"
    elif p_val < 0.05:
        verdict = "~ faible"
    else:
        verdict = ""

    sig = "***" if p_val < 0.001 else "**" if p_val < 0.01 else "*" if p_val < 0.05 else "  "
    print(f"  {var:<30} {mean_out:>9.3f} {mean_non:>9.3f} {diff_pct:>+6.1f}% "
          f"{p_val:>9.4f} {sig}  {cohens_d:>+7.3f}  {verdict}")

    comparison_rows.append({
        'variable':          var,
        'mean_outlier':      round(mean_out, 4),
        'mean_non_outlier':  round(mean_non, 4),
        'diff_pct':          round(diff_pct, 2),
        'p_value':           round(p_val, 6),
        'cohens_d':          round(cohens_d, 4),
        'significant':       p_val < 0.05 and abs(cohens_d) > 0.2,
        'n_outliers':        len(out_vals),
        'n_non_outliers':    len(non_out_vals),
    })

df_comparison = pd.DataFrame(comparison_rows).sort_values('cohens_d', key=abs, ascending=False)

# ═══════════════════════════════════════════════════════════════
# RÉSUMÉ
# ═══════════════════════════════════════════════════════════════
print(f"\n{'='*75}")
print(f"VARIABLES SIGNIFICATIVEMENT DIFFÉRENTES (p<0.05, |d|>0.2)")
print(f"{'='*75}")
sig_vars = df_comparison[df_comparison['significant']]

if sig_vars.empty:
    print("\n  Aucune variable significativement différente.")
    print("  → Les outliers ne coïncident pas avec des conditions météo particulières.")
    print("  → Probable erreur instrumentale satellite plutôt que signal hydrologique.")
else:
    for _, row in sig_vars.iterrows():
        direction = "PLUS ÉLEVÉ" if row['cohens_d'] > 0 else "PLUS BAS"
        print(f"  {row['variable']:<30} d={row['cohens_d']:+.3f}  "
              f"p={row['p_value']:.4f}  → {direction} aux outliers ({row['diff_pct']:+.1f}%)")

# ═══════════════════════════════════════════════════════════════
# SAUVEGARDE
# ═══════════════════════════════════════════════════════════════
df_comparison.to_csv(OUT_CSV, index=False)
print(f"\n✅ Comparaison → {OUT_CSV}")