"""
zeroshot_eval_and_plot.py
═══════════════════════════════════════════════════════════════════════════
Évalue le modèle insitu en zero-shot sur les stations satellite ~10j,
puis génère les plots obs vs pred + histogrammes.

Étapes :
  1. Backup du config.yml du run insitu
  2. Modifier le config pour pointer vers le dataset satellite
  3. Lancer nh-run evaluate
  4. Restaurer le config original
  5. Plotter chaque station + distribution des métriques
═══════════════════════════════════════════════════════════════════════════
"""

import shutil
import subprocess
import pickle
import yaml
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
RUN_DIR        = Path("./runs/arlstm_feat10jLow_modele2_0605_140952")
EPOCH          = 18
PERIOD         = "test"
SAT_DATA_DIR   = "./data/IA/NeuralHydrology_satellite_10D"
SAT_BASIN_FILE = "./AI/LSTM/NeuralHydro_satellite_10D/stations_10j.txt"
TARGET_VAR     = "water_level"

MODEL_NAME     = RUN_DIR.name
CONFIG_PATH    = RUN_DIR / "config.yml"
CONFIG_BACKUP  = RUN_DIR / "config_BACKUP.yml"
RESULTS_P      = RUN_DIR / PERIOD / f"model_epoch{EPOCH:03d}" / f"{PERIOD}_results.p"
METRICS_CSV    = RUN_DIR / PERIOD / f"model_epoch{EPOCH:03d}" / f"{PERIOD}_metrics.csv"
OUT_DIR        = Path(f"./figures_zeroshot_satellite/{MODEL_NAME}")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ═══════════════════════════════════════════════════════════════
# PARTIE 1 — ÉVALUATION ZERO-SHOT
# ═══════════════════════════════════════════════════════════════
print("=" * 60)
print("ZERO-SHOT ÉVALUATION SUR STATIONS SATELLITE 10J")
print("=" * 60)

# 1. Backup
if not CONFIG_BACKUP.exists():
    shutil.copy(CONFIG_PATH, CONFIG_BACKUP)
    print(f"✅ Config sauvegardé : {CONFIG_BACKUP}")
else:
    print(f"⚠️  Backup existe déjà : {CONFIG_BACKUP}")

# 2. Modifier le config
with open(CONFIG_PATH) as f:
    cfg = yaml.safe_load(f)

orig_data_dir  = cfg.get('data_dir')
orig_test_file = cfg.get('test_basin_file')

cfg['data_dir']        = SAT_DATA_DIR
cfg['test_basin_file'] = SAT_BASIN_FILE
cfg['test_start_date'] = '01/01/2016'
cfg['test_end_date']   = '31/12/2025'

with open(CONFIG_PATH, 'w') as f:
    yaml.dump(cfg, f)

print(f"\n📝 Config modifié :")
print(f"   data_dir        : {orig_data_dir} → {SAT_DATA_DIR}")
print(f"   test_basin_file : {orig_test_file} → {SAT_BASIN_FILE}")

# 3. Lancer l'évaluation
print(f"\n🚀 Lancement de nh-run evaluate (epoch {EPOCH}, period {PERIOD})...")

cmd = ["nh-run", "evaluate",
       "--run-dir", str(RUN_DIR),
       "--epoch", str(EPOCH),
       "--period", PERIOD]

eval_ok = False
try:
    subprocess.run(cmd, check=True)
    eval_ok = True
    print("\n✅ Évaluation terminée")
except subprocess.CalledProcessError as e:
    print(f"\n❌ Erreur évaluation : {e}")
finally:
    # 4. Restaurer le config (toujours)
    shutil.copy(CONFIG_BACKUP, CONFIG_PATH)
    print(f"✅ Config original restauré")

if not eval_ok:
    print("Arrêt : l'évaluation a échoué.")
    exit(1)

# ═══════════════════════════════════════════════════════════════
# PARTIE 2 — PLOTS
# ═══════════════════════════════════════════════════════════════
print(f"\n{'=' * 60}")
print("GÉNÉRATION DES PLOTS")
print(f"{'=' * 60}")

# Charger résultats
print(f"\n📂 Chargement de {RESULTS_P}...")
with open(RESULTS_P, 'rb') as f:
    results = pickle.load(f)

# Métriques
df_metrics = pd.read_csv(METRICS_CSV, header=None, names=["station", "NSE", "KGE"])
df_metrics["NSE"] = pd.to_numeric(df_metrics["NSE"], errors="coerce")
df_metrics["KGE"] = pd.to_numeric(df_metrics["KGE"], errors="coerce")
df_metrics = df_metrics.set_index("station")

# Stats globales
print(f"\n{'=' * 60}")
print(f"STATS GLOBALES SUR {len(df_metrics)} STATIONS")
print(f"{'=' * 60}")
print(df_metrics.describe().round(3))
print(f"\nMédiane NSE : {df_metrics['NSE'].median():.3f}")
print(f"Médiane KGE : {df_metrics['KGE'].median():.3f}")
print(f"Stations NSE > 0.5 : {(df_metrics['NSE'] > 0.5).sum()}")
print(f"Stations NSE > 0.0 : {(df_metrics['NSE'] > 0.0).sum()}")
print(f"Stations NSE < 0.0 : {(df_metrics['NSE'] < 0.0).sum()}")

# Plot par station
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
            print(f"  ⚠️  {sid} : variables {obs_var}/{sim_var} absentes")
            continue

        dates = ds.date.values
        obs = ds[obs_var].values.flatten()
        sim = ds[sim_var].values.flatten()

        nse = df_metrics.loc[sid, 'NSE'] if sid in df_metrics.index else np.nan
        kge = df_metrics.loc[sid, 'KGE'] if sid in df_metrics.index else np.nan

        fig, ax = plt.subplots(figsize=(13, 4))
        ax.plot(dates, obs, 'o-', color="steelblue", lw=1, ms=3,
                label=f"Observé (n={(~np.isnan(obs)).sum()})")
        ax.plot(dates, sim, 's-', color="crimson", lw=1, ms=3, alpha=0.7,
                label="Prédit (zero-shot)")
        ax.axhline(0, color="gray", lw=0.5, ls="--")
        ax.set_title(f"{sid}  —  NSE = {nse:.3f}  |  KGE = {kge:.3f}", fontsize=11)
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

# Histogrammes
fig, axes = plt.subplots(1, 2, figsize=(13, 4.5))

axes[0].hist(df_metrics["NSE"].dropna(), bins=20, color="steelblue", edgecolor="white")
axes[0].axvline(df_metrics["NSE"].median(), color="red", lw=2, ls="--",
                label=f"Médiane = {df_metrics['NSE'].median():.2f}")
axes[0].axvline(0, color="gray", lw=1, ls=":")
axes[0].set_xlabel("NSE")
axes[0].set_ylabel("Nb stations")
axes[0].set_title(f"Distribution NSE — Zero-shot satellite ~10j (n={len(df_metrics)})")
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
print(f"📂 Résultats dans : {RUN_DIR / PERIOD / f'model_epoch{EPOCH:03d}'}")