"""
Analyse des données dynamiques (précipitation, température, PET, water_level)
pour les 20 meilleures et 20 pires stations — sur la période de test 2024-2025.
"""

import numpy as np
import pandas as pd
import xarray as xr
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from pathlib import Path
from scipy import stats

NC_DIR     = Path("./data/IA/NeuralHydrology/time_series")
OUTPUT_DIR = Path("./data/IA/NeuralHydrology/")
TEST_START = "2024-01-01"
TEST_END   = "2025-12-31"
TRAIN_START = "2016-01-01"
TRAIN_END   = "2023-12-31"

TOP20 = [
    "O787401001", "O303521001", "M322301010", "P613402001", "M010401010",
    "M814401010", "J341303001", "M351401010", "K212301002", "P821501001",
    "O504251002", "O709401002", "Y210002001", "J360181001", "H703301001",
    "L056301001", "A455000201", "M038401020", "H030101001", "A133003001",
]

FLOP20 = [
    "Y047403001", "Y503201001", "Y046600501", "O546431001", "P246401001",
    "V343401001", "A243003001", "J701063001", "Y067406001", "U234502001",
    "K457221001", "H760201001", "O723403001", "J321302002", "Y551404001",
    "U221502001", "K640252001", "K437311001", "A623201001", "K035631001",
]

VARS = ['precipitation', 'temperature', 'pet', 'water_level']

# ═══════════════════════════════════════════════════════════════
# CHARGEMENT
# ═══════════════════════════════════════════════════════════════
def load_stats(stations, period_start, period_end):
    """Charge les stats dynamiques pour un groupe de stations sur une période."""
    records = []
    for sid in stations:
        nc_path = NC_DIR / f"{sid}.nc"
        if not nc_path.exists():
            continue
        try:
            ds  = xr.open_dataset(nc_path)
            ds_p = ds.sel(date=slice(period_start, period_end))
            rec = {'station_id': sid}
            for var in VARS:
                if var not in ds_p:
                    continue
                vals = ds_p[var].values
                valid = vals[~np.isnan(vals)]
                if len(valid) == 0:
                    continue
                rec[f'{var}_mean']   = np.mean(valid)
                rec[f'{var}_std']    = np.std(valid)
                rec[f'{var}_median'] = np.median(valid)
                rec[f'{var}_p95']    = np.percentile(valid, 95)
                rec[f'{var}_p05']    = np.percentile(valid, 5)
                rec[f'{var}_nan_pct']= np.mean(np.isnan(vals)) * 100
                # Autocorrélation lag-1 (mémoire du signal)
                if len(valid) > 10:
                    rec[f'{var}_autocorr'] = pd.Series(valid).autocorr(lag=1)
            ds.close()
            records.append(rec)
        except Exception as e:
            print(f"  ⚠️  {sid} : {e}")
            continue
    return pd.DataFrame(records)

print("Chargement données test (2024-2025)...")
top_test  = load_stats(TOP20,  TEST_START,  TEST_END)
flop_test = load_stats(FLOP20, TEST_START,  TEST_END)

print("Chargement données train (2016-2023)...")
top_train  = load_stats(TOP20,  TRAIN_START, TRAIN_END)
flop_train = load_stats(FLOP20, TRAIN_START, TRAIN_END)

top_test['groupe']   = 'TOP'
flop_test['groupe']  = 'FLOP'
top_train['groupe']  = 'TOP'
flop_train['groupe'] = 'FLOP'

df_test  = pd.concat([top_test,  flop_test],  ignore_index=True)
df_train = pd.concat([top_train, flop_train], ignore_index=True)

# ═══════════════════════════════════════════════════════════════
# TABLEAU COMPARATIF — PÉRIODE DE TEST
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("DONNÉES DYNAMIQUES — PÉRIODE TEST (2024-2025)")
print("=" * 80)

metrics = ['mean', 'std', 'p05', 'median', 'p95', 'nan_pct', 'autocorr']

for var in VARS:
    cols = [f'{var}_{m}' for m in metrics if f'{var}_{m}' in df_test.columns]
    if not cols:
        continue

    print(f"\n── {var.upper()} ──────────────────────────────────────────")
    print(f"  {'Métrique':<20} {'TOP moy':>10} {'FLOP moy':>11} {'Δ':>8}  {'sign.':>6}")
    print(f"  {'-'*58}")

    top_g  = df_test[df_test['groupe'] == 'TOP']
    flop_g = df_test[df_test['groupe'] == 'FLOP']

    for col in cols:
        metric_name = col.replace(f'{var}_', '')
        t_vals = top_g[col].dropna()
        f_vals = flop_g[col].dropna()
        if len(t_vals) == 0 or len(f_vals) == 0:
            continue
        t_moy = t_vals.mean()
        f_moy = f_vals.mean()
        delta = t_moy - f_moy

        # Test de Wilcoxon (non-paramétrique)
        try:
            _, pval = stats.mannwhitneyu(t_vals, f_vals, alternative='two-sided')
            sig = "***" if pval < 0.01 else ("**" if pval < 0.05 else ("*" if pval < 0.1 else ""))
        except Exception:
            sig = ""

        flag = " ◄" if sig in ["*", "**", "***"] else ""
        print(f"  {metric_name:<20} {t_moy:>10.3f} {f_moy:>11.3f} {delta:>+8.3f}  {sig:>6}{flag}")

# ═══════════════════════════════════════════════════════════════
# DÉRIVE TRAIN → TEST (distribution shift)
# ═══════════════════════════════════════════════════════════════
print("\n\n" + "=" * 80)
print("DÉRIVE TRAIN→TEST — water_level (distribution shift)")
print("=" * 80)
print(f"  {'Groupe':<8} {'moy_train':>10} {'moy_test':>10} {'Δ_mean':>8} "
      f"{'std_train':>10} {'std_test':>10} {'Δ_std':>8}")
print(f"  {'-'*68}")

for groupe, g_train, g_test in [('TOP',  top_train,  top_test),
                                  ('FLOP', flop_train, flop_test)]:
    col = 'water_level_mean'
    if col not in g_train.columns or col not in g_test.columns:
        continue
    m_train = g_train[col].mean()
    m_test  = g_test[col].mean()
    s_train = g_train['water_level_std'].mean()
    s_test  = g_test['water_level_std'].mean()
    print(f"  {groupe:<8} {m_train:>10.3f} {m_test:>10.3f} {m_test-m_train:>+8.3f} "
          f"{s_train:>10.3f} {s_test:>10.3f} {s_test-s_train:>+8.3f}")

# ═══════════════════════════════════════════════════════════════
# PLOTS
# ═══════════════════════════════════════════════════════════════
print("\nGénération des plots...")

fig, axes = plt.subplots(2, 4, figsize=(18, 8), constrained_layout=True)
fig.suptitle("Distributions dynamiques — TOP 20 vs FLOP 20 (2024-2025)", fontsize=13)

plot_metrics = [
    ('water_level_mean',   'WL mean (norm.)'),
    ('water_level_std',    'WL std (norm.)'),
    ('water_level_autocorr', 'WL autocorr lag-1'),
    ('water_level_nan_pct',  'WL NaN %'),
    ('precipitation_mean', 'Précip. moy. (mm/j)'),
    ('temperature_mean',   'Temp. moy. (°C)'),
    ('pet_mean',           'PET moy. (mm/j)'),
    ('precipitation_std',  'Précip. std'),
]

colors = {'TOP': 'steelblue', 'FLOP': 'crimson'}

for ax, (col, label) in zip(axes.flat, plot_metrics):
    for groupe, gdf in df_test.groupby('groupe'):
        vals = gdf[col].dropna()
        if len(vals) == 0:
            continue
        ax.hist(vals, bins=12, alpha=0.55, color=colors[groupe],
                label=f"{groupe} (n={len(vals)})", density=True)
        ax.axvline(vals.mean(), color=colors[groupe], lw=2, ls='--')

    ax.set_title(label, fontsize=9)
    ax.legend(fontsize=7)
    ax.grid(True, alpha=0.3)

out_path = Path("./plots_dynamic_analysis.png")
fig.savefig(out_path, dpi=150, bbox_inches='tight')
plt.close()
print(f"  ✅ Plot sauvegardé → {out_path}")