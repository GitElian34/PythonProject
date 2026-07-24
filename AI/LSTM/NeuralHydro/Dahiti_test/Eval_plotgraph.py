"""
eval_zeroshot_dahiti_27j.py
════════════════════════════════════════════════════════════════════════
Évaluation zero-shot du modèle AR-LSTM (entraîné sur HW Next) sur les
données DAHITI 27j.

Sorties :
  - ./data/outlier_detection/residuals_27j_dahiti.csv
════════════════════════════════════════════════════════════════════════
"""

import pickle
import numpy as np
import pandas as pd
import torch
from pathlib import Path
from ruamel.yaml import YAML
from neuralhydrology.utils.config import Config
from neuralhydrology.evaluation.evaluate import start_evaluation

torch.set_num_threads(8)

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
MODEL  = "arlstm_feat10jLow_modele2_0605_140952"

EPOCH  = 7

# MODEL  = "arlstm_feat27jHigh_modele2_0206_145147"  # ← modèle 27j (pas 10j !)
# EPOCH  = 27

PERIOD = "validation"

RUN_DIR       = Path(f"./runs/{MODEL}")
STATIONS_FILE = Path("./data/IA/NeuralHydrologyDahiti10jClean/stations_dahiti_10j.txt")
DATA_DIR      = Path("./data/IA/NeuralHydrologyDahiti10jClean/10j")
OUT_CSV       = Path("./data/outlier_detection/residuals_10j_dahiti_clean.csv")
#
# STATIONS_FILE = Path("./data/IA/NeuralHydrologyDahiti27jClean/stations_dahiti_27j.txt")
# DATA_DIR      = Path("./data/IA/NeuralHydrologyDahiti27jClean/27j")
# OUT_CSV       = Path("./data/outlier_detection/residuals_27j_dahiti_clean.csv")

TARGET_VAR    = "water_level"

OUT_CSV.parent.mkdir(parents=True, exist_ok=True)

# ═══════════════════════════════════════════════════════════════
# ÉTAPE 1 — ÉVALUATION
# ═══════════════════════════════════════════════════════════════
print("=" * 60)
print(f"ÉVALUATION ZERO-SHOT DAHITI 10j")
print(f"  Modèle   : {MODEL}  epoch {EPOCH}")
print(f"  Data     : {DATA_DIR}")
print(f"  Stations : {STATIONS_FILE}")
print("=" * 60)

# Recopie le config en changeant uniquement data_dir et stations
ryaml = YAML()
ryaml.preserve_quotes = True
config_path = RUN_DIR / "config.yml"
#
config_eval = RUN_DIR / "config_eval_dahiti_10j_clean.yml"
# config_eval   = RUN_DIR / "config_eval_dahiti_27j_clean.yml"

with open(config_path) as f:
    cfg_dict = ryaml.load(f)

cfg_dict["validation_basin_file"] = str(STATIONS_FILE.resolve())
cfg_dict["data_dir"]              = str(DATA_DIR.resolve())

with open(config_eval, "w") as f:
    ryaml.dump(cfg_dict, f)

cfg = Config(config_eval)
start_evaluation(cfg=cfg, run_dir=RUN_DIR, epoch=EPOCH, period=PERIOD)
print("✅ Évaluation terminée")

# ═══════════════════════════════════════════════════════════════
# ÉTAPE 2 — EXTRACTION DES RÉSIDUS
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("EXTRACTION DES RÉSIDUS")
print("=" * 60)

results_p = RUN_DIR / PERIOD / f"model_epoch{EPOCH:03d}" / f"{PERIOD}_results.p"
with open(results_p, "rb") as f:
    raw = pickle.load(f)

print(f"  {len(raw)} stations chargées depuis {results_p}")

rows = []
raw_by_station = {}

for sid, sub in raw.items():
    try:
        freq    = list(sub.keys())[0]
        ds      = sub[freq]["xr"]
        obs_var = f"{TARGET_VAR}_obs"
        sim_var = f"{TARGET_VAR}_sim"
        if obs_var not in ds or sim_var not in ds:
            print(f"  ⚠  {sid} : variables {obs_var}/{sim_var} manquantes")
            continue
        dates    = pd.to_datetime(ds.date.values)
        obs_arr  = ds[obs_var].values.flatten()
        pred_arr = ds[sim_var].values.flatten()
        raw_by_station[str(sid)] = {"dates": dates, "obs": obs_arr, "pred": pred_arr}
        for i, (d, o, p) in enumerate(zip(dates, obs_arr, pred_arr)):
            rows.append({
                "station":  str(sid),
                "date":     d,
                "obs":      float(o)    if not np.isnan(o) else np.nan,
                "pred":     float(p)    if not np.isnan(p) else np.nan,
                "residual": float(o - p) if not (np.isnan(o) or np.isnan(p)) else np.nan,
                "_i":       i,
            })
    except Exception as e:
        print(f"  ⚠  {sid} : {e}")

df = pd.DataFrame(rows)
df["date"] = pd.to_datetime(df["date"])

# Résidu normalisé par station
def norm_residuals(grp):
    std = np.nanstd(grp["residual"])
    grp["residual_norm"] = grp["residual"] / std if std > 0 else np.nan
    return grp

df = df.groupby("station", group_keys=False).apply(norm_residuals)

# Score outlier (même formule que HW Next)
def compute_score(residual_norm, obs_arr, pred_arr, i):
    if np.isnan(residual_norm) or i == 0:
        return abs(residual_norm) if not np.isnan(residual_norm) else np.nan
    j0_obs  = obs_arr[i - 1]
    j0_pred = pred_arr[i - 1]
    j0_ref  = j0_obs if not np.isnan(j0_obs) else j0_pred
    if np.isnan(j0_ref):
        return abs(residual_norm)
    j1_obs  = obs_arr[i]
    j1_pred = pred_arr[i]
    if np.isnan(j1_obs) or np.isnan(j1_pred):
        return abs(residual_norm)
    delta_alti  = j1_obs  - j0_ref
    delta_model = j1_pred - j0_ref
    eps     = 1e-8
    cos     = (delta_alti * delta_model) / (
        np.sqrt(delta_alti ** 2 + eps) * np.sqrt(delta_model ** 2 + eps))
    cos_pen = (1 - cos) / 2
    amp     = (abs(delta_alti) + abs(delta_model)) / 2
    penalite = cos_pen * np.tanh(amp)
    return abs(residual_norm) * (1 + penalite)

scores = []
for _, row in df.iterrows():
    sid      = row["station"]
    i        = int(row["_i"])
    obs_arr  = raw_by_station[sid]["obs"]
    pred_arr = raw_by_station[sid]["pred"]
    scores.append(compute_score(row["residual_norm"], obs_arr, pred_arr, i))

df["score"]      = scores
df["is_outlier"] = df["score"] > 2.5
df["year"]       = df["date"].dt.year
df = df.drop(columns=["_i"])

# ═══════════════════════════════════════════════════════════════
# EXPORT
# ═══════════════════════════════════════════════════════════════
df.to_csv(OUT_CSV, index=False)

# Stats sur les lignes propres
df_clean = df.dropna(subset=["obs", "pred"])

print(f"\n✅ CSV exporté : {OUT_CSV}")
print(f"   Lignes totales          : {len(df)}")
print(f"   Lignes obs & pred OK    : {len(df_clean)}  (après dropna)")
print(f"   Stations                : {df['station'].nunique()}")
print(f"   Outliers détectés       : {df_clean['is_outlier'].sum()} "
      f"({df_clean['is_outlier'].mean()*100:.1f}%)")

# Résumé NSE par station (sur lignes propres)
print(f"\n  NSE modèle (sur lignes propres) :")
nse_list = []
for sid, grp in df_clean.groupby("station"):
    o = grp["obs"].values
    p = grp["pred"].values
    mask = ~(np.isnan(o) | np.isnan(p))
    if mask.sum() < 5:
        continue
    o, p = o[mask], p[mask]
    denom = np.sum((o - o.mean()) ** 2)
    nse   = 1 - np.sum((o - p) ** 2) / denom if denom > 0 else np.nan
    nse_list.append(nse)

if nse_list:
    nse_arr = np.array(nse_list)
    print(f"    N stations  : {len(nse_arr)}")
    print(f"    NSE médian  : {np.nanmedian(nse_arr):.3f}")
    print(f"    NSE moyen   : {np.nanmean(nse_arr):.3f}")
    print(f"    NSE > 0.5   : {(nse_arr > 0.5).sum()} ({(nse_arr > 0.5).mean():.0%})")
    print(f"    NSE < 0     : {(nse_arr < 0).sum()} ({(nse_arr < 0).mean():.0%})")