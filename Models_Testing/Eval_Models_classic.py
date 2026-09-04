"""
eval_classic_10j_27j.py
════════════════════════════════════════════════════════════════════════
Évaluation zero-shot des modèles Classic (predict_last_n = 1) sur HW Next,
10j et 27j.

Sorties :
  Models_Testing/Classic/residus/residuals_10j_hwnext.csv
  Models_Testing/Classic/residus/residuals_27j_hwnext.csv
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

TARGET_VAR = "water_level"
PERIOD = "validation"
OUT_DIR = Path("./Models_Testing/Classic/residus")
OUT_DIR.mkdir(parents=True, exist_ok=True)

RUNS = [
    # {
    #     "label": "10j",
    #     "model_dir": "arlstm_feat10j_Final_ModeleT_3107_110910",
    #     "epoch": 25,
    #     "data_dir": "./data/IA/NeuralHydrology_hydroweb_next/10j",
    #     "stations_file": "./toutes_stations_hwnext_10j.txt",
    # },
    {
        "label": "27j",
        "model_dir": "arlstm_feat27jFinalModeleT_3107_111332",
        "epoch": 25,
        "data_dir": "./data/IA/NeuralHydrology_hydroweb_next/27j",
        "stations_file": "./toutes_stations_hwnext_27j.txt",
    },
]


def compute_score(residual_norm, obs_arr, pred_arr, i):
    if np.isnan(residual_norm) or i == 0:
        return abs(residual_norm) if not np.isnan(residual_norm) else np.nan
    j0_obs, j0_pred = obs_arr[i - 1], pred_arr[i - 1]
    j0_ref = j0_obs if not np.isnan(j0_obs) else j0_pred
    if np.isnan(j0_ref):
        return abs(residual_norm)
    j1_obs, j1_pred = obs_arr[i], pred_arr[i]
    if np.isnan(j1_obs) or np.isnan(j1_pred):
        return abs(residual_norm)
    delta_alti = j1_obs - j0_ref
    delta_model = j1_pred - j0_ref
    eps = 1e-8
    cos = (delta_alti * delta_model) / (
        np.sqrt(delta_alti ** 2 + eps) * np.sqrt(delta_model ** 2 + eps))
    cos_pen = (1 - cos) / 2
    amp = (abs(delta_alti) + abs(delta_model)) / 2
    penalite = cos_pen * np.tanh(amp)
    return abs(residual_norm) * (1 + penalite)


def run_one(run_cfg: dict):
    label = run_cfg["label"]
    run_dir = Path(f"./runs/{run_cfg['model_dir']}")
    epoch = run_cfg["epoch"]
    data_dir = Path(run_cfg["data_dir"])
    stations_file = Path(run_cfg["stations_file"])
    out_csv = OUT_DIR / f"residuals_{label}_hwnext.csv"

    print("\n" + "=" * 60)
    print(f"  ÉVALUATION CLASSIC {label}")
    print(f"  Modèle : {run_cfg['model_dir']}  epoch {epoch}")
    print("=" * 60)

    ryaml = YAML()
    ryaml.preserve_quotes = True
    config_path = run_dir / "config.yml"
    config_eval = run_dir / f"config_eval_classic_{label}_hwnext.yml"

    with open(config_path) as f:
        cfg_dict = ryaml.load(f)
    cfg_dict["validation_basin_file"] = str(stations_file.resolve())
    cfg_dict["data_dir"] = str(data_dir.resolve())
    with open(config_eval, "w") as f:
        ryaml.dump(cfg_dict, f)

    cfg = Config(config_eval)
    start_evaluation(cfg=cfg, run_dir=run_dir, epoch=epoch, period=PERIOD)
    print("  Évaluation terminée")

    results_p = run_dir / PERIOD / f"model_epoch{epoch:03d}" / f"{PERIOD}_results.p"
    with open(results_p, "rb") as f:
        raw = pickle.load(f)
    print(f"  {len(raw)} stations chargées")

    rows = []
    raw_by_station = {}

    for sid, sub in raw.items():
        try:
            freq_key = list(sub.keys())[0]
            ds = sub[freq_key]["xr"]
            obs_var, sim_var = f"{TARGET_VAR}_obs", f"{TARGET_VAR}_sim"
            if obs_var not in ds or sim_var not in ds:
                continue
            dates = pd.to_datetime(ds.date.values)
            obs_arr = ds[obs_var].values.flatten()
            pred_arr = ds[sim_var].values.flatten()
            raw_by_station[str(sid)] = {"obs": obs_arr, "pred": pred_arr}
            for i, (d, o, p) in enumerate(zip(dates, obs_arr, pred_arr)):
                rows.append({
                    "station": str(sid), "date": d,
                    "obs": float(o) if not np.isnan(o) else np.nan,
                    "pred": float(p) if not np.isnan(p) else np.nan,
                    "residual": float(o - p) if not (np.isnan(o) or np.isnan(p)) else np.nan,
                    "_i": i,
                })
        except Exception as e:
            print(f"  ⚠ {sid} : {e}")

    df = pd.DataFrame(rows)
    if df.empty:
        print(f"  ⚠ Aucune donnée extraite pour {label}")
        return
    df["date"] = pd.to_datetime(df["date"])

    def norm_residuals(grp):
        std = np.nanstd(grp["residual"])
        grp["residual_norm"] = grp["residual"] / std if std > 0 else np.nan
        return grp
    df = df.groupby("station", group_keys=False).apply(norm_residuals)

    scores = []
    for _, row in df.iterrows():
        sid, i = row["station"], int(row["_i"])
        scores.append(compute_score(row["residual_norm"],
                                     raw_by_station[sid]["obs"],
                                     raw_by_station[sid]["pred"], i))
    df["score"] = scores
    df["is_outlier"] = df["score"] > 2.5
    df["year"] = df["date"].dt.year
    df = df.drop(columns=["_i"])

    df.to_csv(out_csv, index=False)

    df_clean = df.dropna(subset=["obs", "pred"])
    print(f"  ✅ CSV -> {out_csv}")
    print(f"     Lignes : {len(df)} | OK : {len(df_clean)} | Stations : {df['station'].nunique()}")
    print(f"     Outliers : {df_clean['is_outlier'].sum()} ({df_clean['is_outlier'].mean()*100:.1f}%)")

    nse_list = []
    for sid, grp in df_clean.groupby("station"):
        o, p = grp["obs"].values, grp["pred"].values
        mask = ~(np.isnan(o) | np.isnan(p))
        if mask.sum() < 5:
            continue
        o, p = o[mask], p[mask]
        denom = np.sum((o - o.mean()) ** 2)
        nse = 1 - np.sum((o - p) ** 2) / denom if denom > 0 else np.nan
        nse_list.append(nse)
    if nse_list:
        nse_arr = np.array(nse_list)
        print(f"     NSE médian : {np.nanmedian(nse_arr):.3f} (n={len(nse_arr)}, "
              f"> 0.5 : {(nse_arr > 0.5).mean():.0%})")


if __name__ == "__main__":
    for run_cfg in RUNS:
        run_one(run_cfg)
    print("\n✅ Terminé.")