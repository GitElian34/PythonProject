"""
plot_zeroshot_predictions.py
═══════════════════════════════════════════════════════════════════════════
Affiche pour chaque station satellite ~10j :
  - La série observée (water_level normalisée)
  - La série prédite par le modèle insitu en zero-shot
  - Les métriques NSE et KGE

Lit les test_results.p générés par nh-run evaluate.
═══════════════════════════════════════════════════════════════════════════
"""

import pickle
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from pathlib import Path

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
MODEL = "/arlstm_feat10jLow_modele2_0505_171608"
RUN_DIR        = Path(f"./runs{MODEL}")
EPOCH          = 6
PERIOD      = "test"

RESULTS_P   = RUN_DIR / PERIOD / f"model_epoch{EPOCH:03d}" / "test_results.p"
METRICS_CSV = RUN_DIR / PERIOD / f"model_epoch{EPOCH:03d}" / "test_metrics.csv"

OUT_DIR     = Path(f"./figures_zeroshot_satellite{MODEL}")
OUT_DIR.mkdir(exist_ok=True)

TARGET_VAR  = "water_level"   # nom de la variable cible

# ═══════════════════════════════════════════════════════════════
# Chargement des résultats
# ═══════════════════════════════════════════════════════════════
print("="*60)
print("PLOT ZERO-SHOT — STATIONS SATELLITE 10J")
print("="*60)

if not RESULTS_P.exists():
    print(f"❌ Pas de résultats trouvés : {RESULTS_P}")
    print("   Lance d'abord : python zeroshot_evaluation.py")
    exit(1)

print(f"\n📂 Chargement de {RESULTS_P}...")
with open(RESULTS_P, 'rb') as f:
    results = pickle.load(f)

# Métriques
df_metrics = pd.read_csv(METRICS_CSV, header=None, names=["station", "NSE", "KGE"])
df_metrics["NSE"] = pd.to_numeric(df_metrics["NSE"], errors="coerce")
df_metrics["KGE"] = pd.to_numeric(df_metrics["KGE"], errors="coerce")
df_metrics = df_metrics.set_index("station")

# ═══════════════════════════════════════════════════════════════
# Stats globales
# ═══════════════════════════════════════════════════════════════
print(f"\n{'='*60}")
print(f"STATS GLOBALES SUR {len(df_metrics)} STATIONS")
print(f"{'='*60}")
print(df_metrics.describe().round(3))

print(f"\nMédiane NSE : {df_metrics['NSE'].median():.3f}")
print(f"Médiane KGE : {df_metrics['KGE'].median():.3f}")
print(f"Stations avec NSE > 0.5 : {(df_metrics['NSE'] > 0.5).sum()}")
print(f"Stations avec NSE > 0.0 : {(df_metrics['NSE'] > 0.0).sum()}")
print(f"Stations avec NSE < 0.0 : {(df_metrics['NSE'] < 0.0).sum()}")

# ═══════════════════════════════════════════════════════════════
# Plot pour chaque station
# ═══════════════════════════════════════════════════════════════
print(f"\nGénération des figures...")

stations = sorted(results.keys())

for sid in stations:
    try:
        # Structure NeuralHydrology : results[sid][freq]['xr']
        # avec dataset xarray contenant {var}_obs et {var}_sim
        sub = results[sid]
        # Trouver la fréquence
        freqs = list(sub.keys())
        if not freqs:
            continue
        freq = freqs[0]
        ds = sub[freq]['xr']

        obs_var = f"{TARGET_VAR}_obs"
        sim_var = f"{TARGET_VAR}_sim"

        if obs_var not in ds or sim_var not in ds:
            print(f"  ⚠️  {sid} : variables {obs_var}/{sim_var} absentes")
            continue

        dates = ds.date.values
        obs = ds[obs_var].values.flatten()
        sim = ds[sim_var].values.flatten()

        # Métriques
        nse = df_metrics.loc[sid, 'NSE'] if sid in df_metrics.index else np.nan
        kge = df_metrics.loc[sid, 'KGE'] if sid in df_metrics.index else np.nan

        # ── Figure ──────────────────────────────────────────────────────────
        fig, ax = plt.subplots(figsize=(13, 4))
        ax.plot(dates, obs, 'o-', color="steelblue", lw=1, ms=3,
                label=f"Observé (n={(~np.isnan(obs)).sum()})")
        ax.plot(dates, sim, 's-', color="crimson", lw=1, ms=3, alpha=0.7,
                label=f"Prédit (zero-shot)")
        ax.axhline(0, color="gray", lw=0.5, ls="--")
        ax.set_title(f"{sid}  —  NSE = {nse:.3f}  |  KGE = {kge:.3f}",
                     fontsize=11)
        ax.set_xlabel("Date")
        ax.set_ylabel("Water level (z-score)")
        ax.legend(fontsize=9)
        ax.grid(True, alpha=0.3)

        plt.tight_layout()
        plt.savefig(OUT_DIR / f"{sid}.png", dpi=110)
        plt.close()
        print(f"  ✅ {sid} (NSE={nse:.2f})")

    except Exception as e:
        print(f"  ❌ {sid} : {e}")
        continue

# ═══════════════════════════════════════════════════════════════
# Histogramme global des NSE
# ═══════════════════════════════════════════════════════════════
fig, axes = plt.subplots(1, 2, figsize=(13, 4.5))

axes[0].hist(df_metrics["NSE"].dropna(), bins=20,
             color="steelblue", edgecolor="white")
axes[0].axvline(df_metrics["NSE"].median(), color="red", lw=2, ls="--",
                label=f"Médiane = {df_metrics['NSE'].median():.2f}")
axes[0].axvline(0, color="gray", lw=1, ls=":")
axes[0].set_xlabel("NSE")
axes[0].set_ylabel("Nb stations")
axes[0].set_title(f"Distribution NSE — Zero-shot satellite ~10j (n={len(df_metrics)})")
axes[0].legend()
axes[0].grid(True, alpha=0.3)

axes[1].hist(df_metrics["KGE"].dropna(), bins=20,
             color="forestgreen", edgecolor="white")
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
print(f"\n✅ Distribution sauvegardée : {OUT_DIR}/_distribution_metrics.png")
print(f"\n📂 Toutes les figures dans : {OUT_DIR}")