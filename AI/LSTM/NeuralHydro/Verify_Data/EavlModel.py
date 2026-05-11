#!/usr/bin/env python3
"""
Évaluation de l'EA-LSTM sur N stations de test externes
+ corrélation NSE ~ attributs statiques
Adapté depuis eval_500m_epochs.py pour le run EA-LSTM 2304_145549
"""

import pickle
import random
import torch
import sqlite3
import numpy as np
import pandas as pd
import xarray as xr
import matplotlib.pyplot as plt
from pathlib import Path
from scipy import stats
from ruamel.yaml import YAML
from neuralhydrology.utils.config import Config
from neuralhydrology.evaluation.evaluate import start_evaluation
torch.set_num_threads(10)
# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES — seuls ces lignes changent vs ton script original
# ═══════════════════════════════════════════════════════════════
RUN_DIR    = Path("./runs/satellite_water_level_test_2304_145549")  # ← EA-LSTM
OUTPUT_DIR = Path("./data/IA/NeuralHydrology/")
BASINS_DIR = Path("./AI/LSTM/NeuralHydro/")
DB_PATH    = "./data/insitu_data.db"
ATTRS_CSV  = Path("./data/IA/NeuralHydrology/attributes/attributes.csv")

EPOCH          = 20
N              = 1000        # ← on monte à 500 pour avoir plus de puissance statistique
SEED           = 42
MIN_DIST_M     = 500
MIN_VALID_DAYS = 300
MIN_STD        = 0.05

TEST_BASIN_FILE = BASINS_DIR / "test_500_ealstm.txt"
TEST_DATA_DIR   = OUTPUT_DIR.parent / "NeuralHydrology_test_ealstm"

# ═══════════════════════════════════════════════════════════════
# SÉLECTION — stations hors train/val + dist_barrage >= 500m
# ═══════════════════════════════════════════════════════════════
print(f"Sélection des {N} stations (dist_barrage >= {MIN_DIST_M}m)...")

with open(BASINS_DIR / "train_basins.txt") as f:
    used = set(f.read().splitlines())
with open(BASINS_DIR / "val_basins.txt") as f:
    used |= set(f.read().splitlines())

conn = sqlite3.connect(DB_PATH)
df_dist = pd.read_sql(
    "SELECT code_sta FROM stations_insitu "
    "WHERE dist_barrage_m >= ? AND dist_barrage_m IS NOT NULL AND lon IS NOT NULL",
    conn, params=(MIN_DIST_M,)
)
conn.close()

eligibles = set(df_dist['code_sta'].tolist()) - used

qualified = []
for sid in eligibles:
    nc_path = OUTPUT_DIR / "time_series" / f"{sid}.nc"
    if not nc_path.exists():
        continue
    try:
        ds   = xr.open_dataset(nc_path)
        wl   = ds.sel(date=slice("2024-01-01", "2025-12-31"))["water_level"].values
        ds.close()
        valid = wl[~np.isnan(wl)]
        if len(valid) >= MIN_VALID_DAYS and np.std(valid) >= MIN_STD:
            qualified.append(sid)
    except Exception:
        continue

print(f"  {len(qualified)} stations qualifiées → ", end="")
random.seed(SEED)
selected = random.sample(qualified, min(N, len(qualified)))
print(f"{len(selected)} sélectionnées")

with open(TEST_BASIN_FILE, 'w') as f:
    f.write('\n'.join(selected))

# ═══════════════════════════════════════════════════════════════
# DATA DIR
# ═══════════════════════════════════════════════════════════════
TEST_DATA_DIR.mkdir(exist_ok=True)
ts_link = TEST_DATA_DIR / "time_series"
if not ts_link.exists():
    ts_link.symlink_to((OUTPUT_DIR / "time_series").resolve())

attrs_dir = TEST_DATA_DIR / "attributes"
attrs_dir.mkdir(exist_ok=True)

ryaml = YAML()
with open(RUN_DIR / "config.yml", "r") as f:
    cfg_run = ryaml.load(f)
static_attrs = list(cfg_run.get("static_attributes", []))

# Charger attributes.csv complet et filtrer sur les stations sélectionnées
attrs_full = pd.read_csv(ATTRS_CSV)
attrs_full["station_id"] = attrs_full["station_id"].astype(str)
attrs_sel = attrs_full[attrs_full["station_id"].isin(selected)]

cols = ["station_id"] + [c for c in static_attrs if c in attrs_sel.columns]
attrs_sel[cols].to_csv(attrs_dir / "attributes.csv", index=False)
print(f"  ✅ attributes.csv — {len(attrs_sel)} stations")

# ═══════════════════════════════════════════════════════════════
# ÉVALUATION
# ═══════════════════════════════════════════════════════════════
print(f"\n{'═'*55}")
print(f"Évaluation EA-LSTM epoch {EPOCH} sur {len(selected)} stations...")

test_config = RUN_DIR / f"config_eval_test_ealstm_epoch{EPOCH:03d}.yml"
ryaml2 = YAML()
ryaml2.preserve_quotes = True
with open(RUN_DIR / "config.yml", "r") as f:
    cfg_dict = ryaml2.load(f)

cfg_dict["test_basin_file"] = str(TEST_BASIN_FILE.resolve())
cfg_dict["test_start_date"] = "01/01/2024"
cfg_dict["test_end_date"]   = "31/12/2025"
cfg_dict["data_dir"]        = str(TEST_DATA_DIR.resolve())
cfg_dict["run_dir"]         = str(RUN_DIR.resolve())
for key in ["train_basin_file", "validation_basin_file"]:
    cfg_dict.pop(key, None)

with open(test_config, "w") as f:
    ryaml2.dump(cfg_dict, f)

cfg = Config(test_config)
start_evaluation(cfg=cfg, run_dir=RUN_DIR, epoch=EPOCH, period="test")

# ═══════════════════════════════════════════════════════════════
# CHARGEMENT RÉSULTATS
# ═══════════════════════════════════════════════════════════════
candidates_p = list((RUN_DIR / "test").glob(f"*epoch{EPOCH:03d}*/*.p"))
with open(sorted(candidates_p)[-1], "rb") as f:
    raw = pickle.load(f)

records = []
for station, data in raw.items():
    try:
        nse = float(np.squeeze(data['1D']['NSE']))
        kge = float(np.squeeze(data['1D']['KGE']))
        if not np.isnan(nse):
            records.append({"station_id": station, "nse": nse, "kge": kge})
    except Exception:
        continue

df_metrics = pd.DataFrame(records)
nse_arr    = df_metrics["nse"].values
print(f"\n  NSE médian  : {np.median(nse_arr):.3f}")
print(f"  KGE médian  : {df_metrics['kge'].median():.3f}")
print(f"  N stations  : {len(df_metrics)}")

# Distribution
bins = [(-np.inf,0,"< 0","#d32f2f"),(0,0.3,"0–0.3","#f57c00"),
        (0.3,0.5,"0.3–0.5","#fbc02d"),(0.5,0.7,"0.5–0.7","#388e3c"),
        (0.7,np.inf,"> 0.7","#1565c0")]
print("\n  Distribution NSE :")
for lo, hi, label, _ in bins:
    n_cat = int(((nse_arr > lo) & (nse_arr <= hi)).sum())
    print(f"    {label:<10} : {n_cat:>4}  ({n_cat/len(nse_arr)*100:.1f}%)")

# ═══════════════════════════════════════════════════════════════
# CORRÉLATION NSE ~ ATTRIBUTS STATIQUES
# ═══════════════════════════════════════════════════════════════
print(f"\n{'═'*55}")
print("Corrélation NSE ~ attributs statiques...")

df = df_metrics.merge(attrs_full, on="station_id", how="inner").dropna(subset=["nse"])
ATTR_COLS = [c for c in attrs_full.columns if c != "station_id"]

corr_results = []
for col in ATTR_COLS:
    sub = df[["nse", col]].dropna()
    if len(sub) < 10 or sub[col].std() == 0:
        continue
    r, p = stats.spearmanr(sub["nse"], sub[col])
    corr_results.append({
        "attribut": col, "spearman_r": round(r, 4),
        "p_value": round(p, 4), "sig": "✅" if p < 0.05 else "❌"
    })

df_corr = pd.DataFrame(corr_results).sort_values("spearman_r", key=abs, ascending=False)
print(f"\n{'Attribut':<22} {'r':>8} {'p-value':>10} {'Sig':>5}")
print("-" * 50)
for _, row in df_corr.iterrows():
    print(f"{row['attribut']:<22} {row['spearman_r']:>8.3f} {row['p_value']:>10.4f} {row['sig']:>5}")

# ═══════════════════════════════════════════════════════════════
# PLOT — distribution + corrélations
# ═══════════════════════════════════════════════════════════════
fig, axes = plt.subplots(1, 2, figsize=(16, 6))
fig.suptitle(
    f"EA-LSTM epoch {EPOCH} — {len(df_metrics)} stations test\n"
    f"NSE médian = {np.median(nse_arr):.3f}  |  KGE médian = {df_metrics['kge'].median():.3f}",
    fontsize=13, fontweight="bold"
)

# Distribution NSE
ax = axes[0]
counts = [int(((nse_arr > lo) & (nse_arr <= hi)).sum()) for lo, hi, _, _ in bins]
labels = [lb for _, _, lb, _ in bins]
colors = [c  for _, _, _, c  in bins]
bars   = ax.bar(labels, counts, color=colors, edgecolor="white")
for bar, count in zip(bars, counts):
    pct = 100 * count / len(nse_arr)
    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5,
            f"{count}\n({pct:.0f}%)", ha="center", va="bottom", fontsize=9)
ax.set_title("Distribution NSE", fontweight="bold")
ax.set_ylabel("N stations")
ax.spines[["top", "right"]].set_visible(False)

# Corrélations
ax = axes[1]
colors_bar = ["#22c55e" if r > 0 else "#ef4444" for r in df_corr["spearman_r"]]
bars = ax.barh(df_corr["attribut"], df_corr["spearman_r"],
               color=colors_bar, edgecolor="white")
for bar, hatch in zip(bars, ["" if p < 0.05 else "///" for p in df_corr["p_value"]]):
    bar.set_hatch(hatch)
ax.axvline(0, color="black", linewidth=1)
ax.set_title("Corrélation NSE ~ attribut\n(hachuré = p>0.05)", fontweight="bold")
ax.set_xlabel("Spearman r")
ax.spines[["top", "right"]].set_visible(False)

plt.tight_layout()
out_png = f"eval_ealstm_test_{len(df_metrics)}stations_epoch{EPOCH}.png"
plt.savefig(out_png, dpi=150, bbox_inches="tight")
print(f"\n✅ Figure sauvegardée : {out_png}")

df_corr.to_csv(f"corr_ealstm_test_{len(df_metrics)}stations.csv", index=False)
df_metrics.to_csv(f"nse_ealstm_test_{len(df_metrics)}stations.csv", index=False)
print(f"✅ CSVs sauvegardés")
plt.show()