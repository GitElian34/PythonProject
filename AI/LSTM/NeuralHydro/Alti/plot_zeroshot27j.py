"""
plot_zeroshot_predictions_27j.py
═══════════════════════════════════════════════════════════════════════════
Affiche pour chaque station satellite ~27j :
  - Les précipitations (barres inversées, style hyétogramme)
  - La série observée (water_level normalisée)
  - La série prédite par le modèle insitu en zero-shot
  - Les métriques NSE et KGE

Lit les validation_results.p + les .nc pour les précipitations.
═══════════════════════════════════════════════════════════════════════════
"""

import pickle
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import xarray as xr
from pathlib import Path

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
MODEL = "arlstm_feat27jHigh_modele2_1205_153310"
RUN_DIR     = Path(f"./runs/{MODEL}")
EPOCH       = 9
PERIOD      = "validation"
NC_DIR      = Path("./data/IA/NeuralHydrology_satellite_27D/time_series")

RESULTS_P   = RUN_DIR / PERIOD / f"model_epoch{EPOCH:03d}" / f"{PERIOD}_results.p"
METRICS_CSV = RUN_DIR / PERIOD / f"model_epoch{EPOCH:03d}" / f"{PERIOD}_metrics.csv"

OUT_DIR     = Path(f"./figures_zeroshot_satellite/{MODEL}")
OUT_DIR.mkdir(parents=True, exist_ok=True)

TARGET_VAR  = "water_level"

# ═══════════════════════════════════════════════════════════════
# Chargement des résultats
# ═══════════════════════════════════════════════════════════════
print("=" * 60)
print("PLOT ZERO-SHOT — STATIONS SATELLITE 27J (+ PRÉCIPITATIONS)")
print("=" * 60)

if not RESULTS_P.exists():
    print(f"❌ Pas de résultats trouvés : {RESULTS_P}")
    exit(1)

print(f"\n📂 Chargement de {RESULTS_P}...")
with open(RESULTS_P, 'rb') as f:
    results = pickle.load(f)

df_metrics = pd.read_csv(METRICS_CSV, header=None, names=["station", "NSE", "KGE"])
df_metrics["NSE"] = pd.to_numeric(df_metrics["NSE"], errors="coerce")
df_metrics["KGE"] = pd.to_numeric(df_metrics["KGE"], errors="coerce")
df_metrics = df_metrics.set_index("station")

print(f"\nMédiane NSE : {df_metrics['NSE'].median():.3f}")
print(f"Médiane KGE : {df_metrics['KGE'].median():.3f}")
print(f"Stations NSE > 0.5 : {(df_metrics['NSE'] > 0.5).sum()}")
print(f"Stations NSE < 0.0 : {(df_metrics['NSE'] < 0.0).sum()}")

print(f"\nGénération des figures...")

stations = sorted(results.keys())

for sid in stations:
    try:
        sub = results[sid]
        freqs = list(sub.keys())
        if not freqs:
            continue
        ds = sub[freqs[0]]['xr']

        obs_var = f"{TARGET_VAR}_obs"
        sim_var = f"{TARGET_VAR}_sim"
        if obs_var not in ds or sim_var not in ds:
            continue

        dates = ds.date.values
        obs = ds[obs_var].values.flatten()
        sim = ds[sim_var].values.flatten()

        nse = df_metrics.loc[sid, 'NSE'] if sid in df_metrics.index else np.nan
        kge = df_metrics.loc[sid, 'KGE'] if sid in df_metrics.index else np.nan

        # ── Charger les précipitations depuis le .nc ────────────────────
        precip = None
        precip_dates = None
        for nc_name in [f"{sid}.nc", f"{str(sid).zfill(13)}.nc"]:
            nc_path = NC_DIR / nc_name
            if nc_path.exists():
                ds_nc = xr.open_dataset(nc_path)
                if 'precipitation_J0' in ds_nc:
                    precip = ds_nc['precipitation_J0'].values
                    precip_dates = pd.to_datetime(ds_nc.date.values)
                ds_nc.close()
                break

        # ── Figure ──────────────────────────────────────────────────────
        fig, (ax_p, ax_wl) = plt.subplots(
            2, 1, figsize=(14, 6),
            gridspec_kw={'height_ratios': [1, 3], 'hspace': 0.05},
            sharex=True
        )

        # Panel précipitations (barres inversées)
        if precip is not None:
            ax_p.bar(precip_dates, precip, width=20, color='#4A90D9',
                     alpha=0.7, edgecolor='none')
            ax_p.invert_yaxis()
            ax_p.set_ylabel('Précip\n(mm/j)', fontsize=8)
            ax_p.grid(True, alpha=0.2)
            ax_p.spines['bottom'].set_visible(False)
            ax_p.tick_params(axis='x', labelbottom=False)
            p95 = np.nanpercentile(precip, 95)
            if p95 > 0:
                ax_p.set_ylim(min(p95 * 1.5, np.nanmax(precip)), 0)
        else:
            ax_p.set_visible(False)

        # Panel water level
        ax_wl.plot(dates, obs, 'o-', color="steelblue", lw=1, ms=3,
                   label=f"Observé (n={(~np.isnan(obs)).sum()})")
        ax_wl.plot(dates, sim, 's-', color="crimson", lw=1, ms=3, alpha=0.7,
                   label="Prédit (zero-shot)")
        ax_wl.axhline(0, color="gray", lw=0.5, ls="--")
        ax_wl.set_xlabel("Date")
        ax_wl.set_ylabel("Water level (z-score)")
        ax_wl.legend(fontsize=8, loc='upper right')
        ax_wl.grid(True, alpha=0.3)
        ax_wl.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
        ax_wl.xaxis.set_major_locator(mdates.YearLocator())

        fig.suptitle(f"{sid}  —  NSE = {nse:.3f}  |  KGE = {kge:.3f}",
                     fontsize=11, fontweight='bold', y=0.98)

        plt.savefig(OUT_DIR / f"{sid}.png", dpi=120, bbox_inches='tight')
        plt.close()
        print(f"  ✅ {sid} (NSE={nse:.2f})")

    except Exception as e:
        print(f"  ❌ {sid} : {e}")
        continue

# ═══════════════════════════════════════════════════════════════
# Histogramme global
# ═══════════════════════════════════════════════════════════════
fig, axes = plt.subplots(1, 2, figsize=(13, 4.5))

axes[0].hist(df_metrics["NSE"].dropna(), bins=20, color="steelblue", edgecolor="white")
axes[0].axvline(df_metrics["NSE"].median(), color="red", lw=2, ls="--",
                label=f"Médiane = {df_metrics['NSE'].median():.2f}")
axes[0].axvline(0, color="gray", lw=1, ls=":")
axes[0].set_xlabel("NSE")
axes[0].set_ylabel("Nb stations")
axes[0].set_title(f"Distribution NSE — Zero-shot satellite ~27j (n={len(df_metrics)})")
axes[0].legend()
axes[0].grid(True, alpha=0.3)

axes[1].hist(df_metrics["KGE"].dropna(), bins=20, color="forestgreen", edgecolor="white")
axes[1].axvline(df_metrics["KGE"].median(), color="red", lw=2, ls="--",
                label=f"Médiane = {df_metrics['KGE'].median():.2f}")
axes[1].axvline(0, color="gray", lw=1, ls=":")
axes[1].set_xlabel("KGE")
axes[1].set_ylabel("Nb stations")
axes[1].set_title("Distribution KGE")
axes[1].legend()
axes[1].grid(True, alpha=0.3)

plt.tight_layout()
plt.savefig(OUT_DIR / "_distribution_metrics.png", dpi=120)
plt.close()
print(f"\n✅ Toutes les figures dans : {OUT_DIR}")