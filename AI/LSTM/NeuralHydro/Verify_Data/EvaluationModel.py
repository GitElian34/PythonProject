#!/usr/bin/env python3
"""
eval_zeroshot_satellite_27j.py
Évalue un modèle déjà entraîné sur les stations satellite 27j en zero-shot.
"""

import pickle
import numpy as np
import pandas as pd
from pathlib import Path
from neuralhydrology.utils.config import Config
from neuralhydrology.evaluation.evaluate import start_evaluation
import torch

torch.set_num_threads(8)

# ─── Paramètres ─────────────────────────────────────────────────────────────
MODEL   = "arlstm_feat27jHigh_modele2_2205_152119"
EPOCH   = 5
PERIOD  = "validation"

RUN_DIR            = Path(f"./runs/{MODEL}")
STATIONS_FILE      = Path("./AI/LSTM/NeuralHydro_satellite_27D/stations_27j.txt")
DATA_DIR_SATELLITE = Path("./data/IA/NeuralHydrology_feat27j")

# ─── Modifier temporairement le config pour pointer vers les données satellite ─
from ruamel.yaml import YAML
ryaml = YAML()
ryaml.preserve_quotes = True

config_path = RUN_DIR / "config.yml"
config_eval = RUN_DIR / "config_eval_satellite.yml"

with open(config_path) as f:
    cfg_dict = ryaml.load(f)

cfg_dict["validation_basin_file"] = str(STATIONS_FILE.resolve())
cfg_dict["data_dir"]              = str(DATA_DIR_SATELLITE.resolve())

with open(config_eval, "w") as f:
    ryaml.dump(cfg_dict, f)

# ─── Évaluation ─────────────────────────────────────────────────────────────
print(f"Run    : {MODEL}")
print(f"Epoch  : {EPOCH}")
print(f"Data   : {DATA_DIR_SATELLITE}")
print(f"Stations : {STATIONS_FILE}\n")

cfg = Config(config_eval)
start_evaluation(cfg=cfg, run_dir=RUN_DIR, epoch=EPOCH, period=PERIOD)

# ─── Lecture résultats ───────────────────────────────────────────────────────
results_p = RUN_DIR / PERIOD / f"model_epoch{EPOCH:03d}" / f"{PERIOD}_results.p"
with open(results_p, "rb") as f:
    raw = pickle.load(f)

records = []
for station, data in raw.items():
    try:
        freq_key = list(data.keys())[0]
        nse = float(np.squeeze(data[freq_key]["NSE"]))
        kge = float(np.squeeze(data[freq_key]["KGE"]))
        if not np.isnan(nse):
            records.append({"station": station, "NSE": nse, "KGE": kge})
    except Exception:
        continue

df = pd.DataFrame(records).sort_values("NSE", ascending=False)

print(f"\n{'='*50}")
print(f"RÉSULTATS ZERO-SHOT — {MODEL} epoch {EPOCH}")
print(f"{'='*50}")
print(f"  N stations  : {len(df)}")
print(f"  NSE médian  : {df['NSE'].median():.3f}")
print(f"  NSE moyen   : {df['NSE'].mean():.3f}")
print(f"  KGE médian  : {df['KGE'].median():.3f}")

print(f"\n  Distribution NSE :")
bins = [(-np.inf,0,"< 0"), (0,0.3,"0–0.3"), (0.3,0.5,"0.3–0.5"),
        (0.5,0.7,"0.5–0.7"), (0.7,np.inf,"> 0.7")]
for lo, hi, label in bins:
    n = int(((df["NSE"] > lo) & (df["NSE"] <= hi)).sum())
    print(f"    {label:<10} : {n:>4}  ({n/len(df)*100:.1f}%)")

print(f"\n  {'Station':<20} {'NSE':>8} {'KGE':>8}")
print(f"  {'-'*40}")
for _, row in df.iterrows():
    print(f"  {row['station']:<20} {row['NSE']:>8.3f} {row['KGE']:>8.3f}")