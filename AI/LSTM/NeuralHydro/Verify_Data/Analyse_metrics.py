"""
Diagnostic complet — stations très bonnes (NSE > 0.7) vs très mauvaises (NSE < 0)
Pour le dataset feat10j et le run arlstm_feat10j_modele2_2704_112827 epoch 5.

⚠️ Les NSE/KGE sont d'abord agrégés par station réelle (moyenne des _d0 à _d9)
   pour éviter qu'une station pathologique compte 10 fois.

1. Attributs statiques (incluant elevation/slope/strahler)
2. Données dynamiques ERA5 (J0, J3, J10)
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
CSV_PATH    = Path("./runs/arlstm_feat10jLow_modele2_3004_130415/test/model_epoch002/test_metrics.csv")
NC_DIR      = Path("./data/IA/NeuralHydrology_feat10j/time_series")
ATTRS_PATH  = Path("./data/IA/NeuralHydrology_feat10j/attributes/attributes.csv")
TRAIN_START = "2016-01-01"
TRAIN_END   = "2023-12-31"
TEST_START  = "2024-01-09"
TEST_END    = "2025-12-31"

# ═══════════════════════════════════════════════════════════════
# CHARGEMENT DES SCORES + AGRÉGATION PAR STATION RÉELLE
# ═══════════════════════════════════════════════════════════════
df_scores = pd.read_csv(CSV_PATH, header=None, names=["station_d", "NSE", "KGE"])
df_scores["NSE"] = pd.to_numeric(df_scores["NSE"], errors="coerce")
df_scores["KGE"] = pd.to_numeric(df_scores["KGE"], errors="coerce")
df_scores = df_scores.dropna(subset=["NSE", "KGE"])

# Extraire la station réelle (sans _d0..._d9)
df_scores["station"] = df_scores["station_d"].str.replace(r"_d\d+$", "", regex=True)

# Agréger par moyenne sur les 10 décalages
df_agg = df_scores.groupby("station").agg(
    NSE_mean=("NSE", "mean"),
    NSE_median=("NSE", "median"),
    KGE_mean=("KGE", "mean"),
    n_decalages=("NSE", "count")
).reset_index()

print(f"Lignes brutes (décalages) : {len(df_scores)}")
print(f"Stations réelles uniques  : {len(df_agg)}")
print()

good = df_agg[df_agg["NSE_mean"] >  0.7]["station"].tolist()
bad  = df_agg[df_agg["NSE_mean"] <  0.0]["station"].tolist()

print(f"Stations très bonnes (NSE moyen > 0.7) : {len(good)}")
print(f"Stations très mauvaises (NSE moyen < 0) : {len(bad)}")

# ═══════════════════════════════════════════════════════════════
# 1. ATTRIBUTS STATIQUES (depuis attributes.csv)
# ═══════════════════════════════════════════════════════════════
print("\n" + "="*70)
print("1. ATTRIBUTS STATIQUES")
print("="*70)

df_attrs_raw = pd.read_csv(ATTRS_PATH)
# Extraire la station réelle pour merge
df_attrs_raw["station"] = df_attrs_raw["station_id"].str.replace(r"_d\d+$", "", regex=True)
# On prend une seule ligne par station (les attrs sont identiques sur les décalages)
df_attrs = df_attrs_raw.drop_duplicates(subset=["station"]).copy()
df_attrs = df_attrs[df_attrs["station"].isin(good + bad)].copy()
df_attrs["groupe"] = df_attrs["station"].apply(lambda x: "BONNE" if x in good else "MAUVAISE")

VARS_STATIC = ["aire_km2", "lon", "lat", "strahler", "elevation_mean", "slope_mean",
               "frac_urban", "frac_agriculture", "frac_forest",
               "frac_semi_natural", "sg_clay_0_30cm", "sg_sand_0_30cm",
               "sg_silt_0_30cm", "dist_barrage_m"]

g_bon  = df_attrs[df_attrs["groupe"] == "BONNE"]
g_mauv = df_attrs[df_attrs["groupe"] == "MAUVAISE"]

print(f"\n  {'Variable':<22} {'BONNE med':>10} {'MAUVAISE med':>13} {'BONNE moy':>10} {'MAUVAISE moy':>13} {'p-val':>8}")
print(f"  {'-'*82}")

for var in VARS_STATIC:
    if var not in df_attrs.columns:
        continue
    b_vals = g_bon[var].dropna()
    m_vals = g_mauv[var].dropna()
    if len(b_vals) == 0 or len(m_vals) == 0:
        continue
    _, pval = stats.mannwhitneyu(b_vals, m_vals, alternative="two-sided")
    sig = "***" if pval < 0.01 else ("**" if pval < 0.05 else ("*" if pval < 0.1 else ""))
    flag = " ◄" if sig else ""
    print(f"  {var:<22} {b_vals.median():>10.3f} {m_vals.median():>13.3f} "
          f"{b_vals.mean():>10.3f} {m_vals.mean():>13.3f} {sig:>8}{flag}")

# Occupation du sol dominante
corine_cols = ["frac_urban", "frac_agriculture", "frac_forest",
               "frac_semi_natural", "frac_wetland", "frac_water"]
corine_cols = [c for c in corine_cols if c in df_attrs.columns]
for groupe, gdf in df_attrs.groupby("groupe"):
    dominant = gdf[corine_cols].idxmax(axis=1).str.replace("frac_", "")
    print(f"\n  Occupation dominante {groupe} : {dominant.value_counts().to_dict()}")

print(f"\n  Distribution aire_km2 :")
for groupe, gdf in df_attrs.groupby("groupe"):
    q = gdf["aire_km2"].quantile([0.25, 0.5, 0.75])
    print(f"    {groupe:<9} : min={gdf['aire_km2'].min():>7.0f}  "
          f"Q25={q[0.25]:>7.0f}  med={q[0.50]:>7.0f}  "
          f"Q75={q[0.75]:>7.0f}  max={gdf['aire_km2'].max():>7.0f}")

# ═══════════════════════════════════════════════════════════════
# 2. DONNÉES DYNAMIQUES ERA5
# ═══════════════════════════════════════════════════════════════
print("\n\n" + "="*70)
print("2. DONNÉES DYNAMIQUES ERA5 (période test 2024-2025)")
print("="*70)

DYN_VARS = ["precipitation_J0", "temperature_J0", "pet_J0",
            "precip_mean_J3", "precip_mean_J10"]

def load_dynamic_stats(stations, period_start, period_end):
    """Lit le _d0 de chaque station (suffit car d0..d9 ont les mêmes ERA5 en gros)."""
    records = []
    for sid in stations:
        nc_path = NC_DIR / f"{sid}_d0.nc"
        if not nc_path.exists():
            continue
        try:
            ds   = xr.open_dataset(nc_path)
            ds_p = ds.sel(date=slice(period_start, period_end))
            rec  = {"station": sid}
            for var in DYN_VARS:
                if var not in ds_p:
                    continue
                vals  = ds_p[var].values
                valid = vals[~np.isnan(vals)]
                if len(valid) == 0:
                    continue
                rec[f"{var}_mean"]    = np.mean(valid)
                rec[f"{var}_std"]     = np.std(valid)
                rec[f"{var}_p95"]     = np.percentile(valid, 95)
            ds.close()
            records.append(rec)
        except Exception:
            continue
    return pd.DataFrame(records)

bon_dyn  = load_dynamic_stats(good, TEST_START, TEST_END)
mauv_dyn = load_dynamic_stats(bad,  TEST_START, TEST_END)

VARS_DYN_PLOT = ["precipitation_J0_mean", "precipitation_J0_std", "precipitation_J0_p95",
                 "temperature_J0_mean",   "pet_J0_mean", "pet_J0_std",
                 "precip_mean_J10_mean"]

print(f"\n  {'Variable':<28} {'BONNE moy':>10} {'MAUVAISE moy':>13} {'Δ':>9}  {'p-val':>6}")
print(f"  {'-'*70}")

for var in VARS_DYN_PLOT:
    if var not in bon_dyn.columns or var not in mauv_dyn.columns:
        continue
    b = bon_dyn[var].dropna()
    m = mauv_dyn[var].dropna()
    if len(b) == 0 or len(m) == 0:
        continue
    _, pval = stats.mannwhitneyu(b, m, alternative="two-sided")
    sig  = "***" if pval < 0.01 else ("**" if pval < 0.05 else ("*" if pval < 0.1 else ""))
    flag = " ◄" if sig else ""
    print(f"  {var:<28} {b.mean():>10.3f} {m.mean():>13.3f} "
          f"{b.mean()-m.mean():>+9.3f}  {sig:>6}{flag}")

# ═══════════════════════════════════════════════════════════════
# 3. SIGNAL HYDROLOGIQUE — WATER LEVEL
# ═══════════════════════════════════════════════════════════════
print("\n\n" + "="*70)
print("3. SIGNAL HYDROLOGIQUE — WATER LEVEL")
print("="*70)

def load_wl_stats(stations, period_start, period_end):
    records = []
    for sid in stations:
        nc_path = NC_DIR / f"{sid}_d0.nc"
        if not nc_path.exists():
            continue
        try:
            ds      = xr.open_dataset(nc_path)
            ds_test = ds.sel(date=slice(period_start, period_end))
            ds_tr   = ds.sel(date=slice(TRAIN_START, TRAIN_END))
            wl_test = ds_test["water_level"].values
            wl_tr   = ds_tr["water_level"].values
            ds.close()

            valid_t = wl_test[~np.isnan(wl_test)]
            valid_r = wl_tr[~np.isnan(wl_tr)]
            if len(valid_t) < 10 or len(valid_r) < 30:
                continue
            autocorr = pd.Series(valid_t).autocorr(lag=1)

            records.append({
                "station"      : sid,
                "wl_mean_test" : np.mean(valid_t),
                "wl_std_test"  : np.std(valid_t),
                "wl_range_test": np.percentile(valid_t, 95) - np.percentile(valid_t, 5),
                "wl_autocorr"  : autocorr,
                "wl_nan_pct"   : np.mean(np.isnan(wl_test)) * 100,
                "drift_mean"   : np.mean(valid_t) - np.mean(valid_r),
                "drift_std"    : np.std(valid_t)  - np.std(valid_r),
                "n_pts_test"   : len(valid_t),
                "n_pts_train"  : len(valid_r),
            })
        except Exception:
            continue
    return pd.DataFrame(records)

bon_wl  = load_wl_stats(good, TEST_START, TEST_END)
mauv_wl = load_wl_stats(bad,  TEST_START, TEST_END)

VARS_WL = ["wl_mean_test", "wl_std_test", "wl_range_test", "wl_autocorr",
           "wl_nan_pct", "drift_mean", "drift_std", "n_pts_test"]

print(f"\n  {'Variable':<20} {'BONNE moy':>10} {'MAUVAISE moy':>13} {'Δ':>9}  {'p-val':>6}")
print(f"  {'-'*62}")

for var in VARS_WL:
    if var not in bon_wl.columns or var not in mauv_wl.columns:
        continue
    b = bon_wl[var].dropna()
    m = mauv_wl[var].dropna()
    if len(b) == 0 or len(m) == 0:
        continue
    _, pval = stats.mannwhitneyu(b, m, alternative="two-sided")
    sig  = "***" if pval < 0.01 else ("**" if pval < 0.05 else ("*" if pval < 0.1 else ""))
    flag = " ◄" if sig else ""
    print(f"  {var:<20} {b.mean():>10.3f} {m.mean():>13.3f} "
          f"{b.mean()-m.mean():>+9.3f}  {sig:>6}{flag}")

# ═══════════════════════════════════════════════════════════════
# PLOTS
# ═══════════════════════════════════════════════════════════════
print("\nGénération des plots...")

fig = plt.figure(figsize=(18, 12))
fig.suptitle(f"Diagnostic feat10j (par station réelle) — "
             f"BONNES (NSE moyen>0.7, n={len(good)}) "
             f"vs MAUVAISES (NSE moyen<0, n={len(bad)})", fontsize=12)
gs = gridspec.GridSpec(3, 4, figure=fig, hspace=0.45, wspace=0.35)

def hist_compare(ax, b_vals, m_vals, title, xlabel):
    ax.hist(b_vals.dropna(), bins=15, alpha=0.6, color="steelblue",
            label=f"BONNE (n={len(b_vals.dropna())})", density=True)
    ax.hist(m_vals.dropna(), bins=15, alpha=0.6, color="crimson",
            label=f"MAUVAISE (n={len(m_vals.dropna())})", density=True)
    ax.axvline(b_vals.mean(), color="steelblue", lw=2, ls="--")
    ax.axvline(m_vals.mean(), color="crimson",   lw=2, ls="--")
    ax.set_title(title, fontsize=9)
    ax.set_xlabel(xlabel, fontsize=8)
    ax.legend(fontsize=7)
    ax.grid(True, alpha=0.3)

# Ligne 1 — Statiques topographiques
hist_compare(fig.add_subplot(gs[0, 0]), g_bon["aire_km2"], g_mauv["aire_km2"], "aire_km2", "km²")
hist_compare(fig.add_subplot(gs[0, 1]), g_bon["strahler"], g_mauv["strahler"], "strahler", "")
hist_compare(fig.add_subplot(gs[0, 2]), g_bon["elevation_mean"], g_mauv["elevation_mean"], "elevation_mean", "m")
hist_compare(fig.add_subplot(gs[0, 3]), g_bon["slope_mean"], g_mauv["slope_mean"], "slope_mean", "%")

# Ligne 2 — Occupation + ERA5
hist_compare(fig.add_subplot(gs[1, 0]), g_bon["frac_agriculture"], g_mauv["frac_agriculture"], "frac_agriculture", "")
hist_compare(fig.add_subplot(gs[1, 1]), g_bon["frac_forest"],      g_mauv["frac_forest"],      "frac_forest", "")
hist_compare(fig.add_subplot(gs[1, 2]), bon_dyn["precipitation_J0_mean"], mauv_dyn["precipitation_J0_mean"], "Précip. J0 moy.", "mm/j")
hist_compare(fig.add_subplot(gs[1, 3]), bon_dyn["temperature_J0_mean"],   mauv_dyn["temperature_J0_mean"],   "Temp. J0 moy.", "°C")

# Ligne 3 — Water level
hist_compare(fig.add_subplot(gs[2, 0]), bon_wl["wl_std_test"],   mauv_wl["wl_std_test"],   "WL std test",     "norm.")
hist_compare(fig.add_subplot(gs[2, 1]), bon_wl["wl_range_test"], mauv_wl["wl_range_test"], "WL range P5-P95 test", "norm.")
hist_compare(fig.add_subplot(gs[2, 2]), bon_wl["wl_autocorr"],   mauv_wl["wl_autocorr"],   "WL autocorr lag1 test", "")
hist_compare(fig.add_subplot(gs[2, 3]), bon_wl["drift_mean"],    mauv_wl["drift_mean"],    "Dérive moy. train→test", "norm.")

out_path = Path("./diagnostic_feat10j.png")
fig.savefig(out_path, dpi=150, bbox_inches="tight")
plt.close()
print(f"  ✅ Plot → {out_path}")

# ═══════════════════════════════════════════════════════════════
# LISTE DES PIRES STATIONS (par NSE moyen sur les décalages)
# ═══════════════════════════════════════════════════════════════
print("\n" + "="*70)
print("TOP 15 PIRES STATIONS (par NSE moyen)")
print("="*70)
worst = df_agg.nsmallest(15, "NSE_mean")
print(worst.to_string(index=False))