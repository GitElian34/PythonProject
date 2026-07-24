"""
pct_outliers_hwnext_DtoD80.py
═══════════════════════════════════════════════════════════════════════════
Calcule le pourcentage de points flaggés comme outliers (même méthode
que plot_outliers_hwnext_DtoD80.py : résidu normalisé + pénalité de
direction), pour le modèle DtoD80 sur HW Next — 10j, 27j, et global.

Pas de plots, juste les pourcentages en sortie.

Sources résidus :
  10j : ./data/outlier_detection/benchmark_DtoD_hwnext10j/residuals_hwnext_10j_80pct.csv
  27j : ./data/outlier_detection/benchmark_DtoD_hwnext27j/residuals_hwnext_27j_80pct.csv

Usage :
  python pct_outliers_hwnext_DtoD80.py
═══════════════════════════════════════════════════════════════════════════
"""

import pandas as pd
import numpy as np
from pathlib import Path

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
MASK_PCT = 80
MODEL_NAME = "arlstm_DtoD80_1506_150002"

SOURCES = {
    "10J": Path("./data/outlier_detection/benchmark_DtoD_hwnext10j/residuals_hwnext_10j_80pct.csv"),
    "27J": Path("./data/outlier_detection/benchmark_DtoD_hwnext27j/residuals_hwnext_27j_80pct.csv"),
}

OUTLIER_THRESHOLD = 3.0   # même seuil que plot_outliers_hwnext_DtoD80.py

# ═══════════════════════════════════════════════════════════════
# DÉTECTION D'OUTLIERS — résidu normalisé + pénalité de direction
# (identique à plot_outliers_hwnext_DtoD80.py)
# ═══════════════════════════════════════════════════════════════
def compute_score(residual_norm, obs_arr, pred_arr, i):
    if np.isnan(residual_norm):
        return np.nan

    j0_ref = np.nan
    for j in range(i - 1, -1, -1):
        if not np.isnan(obs_arr[j]):
            j0_ref = obs_arr[j]
            break

    if np.isnan(j0_ref):
        return abs(residual_norm)

    j1_obs  = obs_arr[i]
    j1_pred = pred_arr[i]
    if np.isnan(j1_obs) or np.isnan(j1_pred):
        return abs(residual_norm)

    delta_alti  = j1_obs  - j0_ref
    delta_model = j1_pred - j0_ref
    eps = 1e-8
    cos = (delta_alti * delta_model) / (
        np.sqrt(delta_alti**2 + eps) * np.sqrt(delta_model**2 + eps)
    )
    cos_pen  = (1 - cos) / 2
    amp      = (abs(delta_alti) + abs(delta_model)) / 2
    penalite = cos_pen * np.tanh(amp)
    return abs(residual_norm) * (1 + penalite)


def detect_outliers(df_station):
    df_station = df_station.sort_values("date").reset_index(drop=True).copy()

    df_station["residual"] = df_station["obs"] - df_station["pred"]
    std = df_station["residual"].std()
    df_station["residual_norm"] = df_station["residual"] / std if std > 0 else np.nan

    obs_arr  = df_station["obs"].values
    pred_arr = df_station["pred"].values
    rn_arr   = df_station["residual_norm"].values

    scores = np.full(len(df_station), np.nan)
    for i in range(len(df_station)):
        scores[i] = compute_score(rn_arr[i], obs_arr, pred_arr, i)

    df_station["score"]      = scores
    df_station["is_outlier"] = df_station["score"].abs() > OUTLIER_THRESHOLD
    return df_station

# ═══════════════════════════════════════════════════════════════
# TRAITEMENT PAR SOURCE (10J / 27J) + GLOBAL
# ═══════════════════════════════════════════════════════════════
print(f"Modèle : {MODEL_NAME}  |  Seuil outlier : {OUTLIER_THRESHOLD}\n")

n_total_global = 0
n_outliers_global = 0
resultats = []

for label, csv_path in SOURCES.items():
    if not csv_path.exists():
        print(f"⚠ {label} : fichier introuvable ({csv_path}) -> ignoré")
        continue

    df_all = pd.read_csv(csv_path, parse_dates=["date"])
    df_all["station"] = df_all["station"].astype(str)
    df_all = df_all.dropna(subset=["obs", "pred"])

    df_flagged = (
        df_all.groupby("station", group_keys=False)
        .apply(detect_outliers)
    )

    n_total = len(df_flagged)
    n_outliers = int(df_flagged["is_outlier"].sum())
    pct = n_outliers / n_total * 100 if n_total else np.nan

    resultats.append({"source": label, "n_total": n_total, "n_outliers": n_outliers, "pct_outliers": pct})
    n_total_global += n_total
    n_outliers_global += n_outliers

    print(f"  {label:<5} : {n_outliers:>6} / {n_total:<6} points flaggés outliers  ->  {pct:.2f}%")

pct_global = n_outliers_global / n_total_global * 100 if n_total_global else np.nan
resultats.append({"source": "GLOBAL", "n_total": n_total_global, "n_outliers": n_outliers_global, "pct_outliers": pct_global})

print(f"\n  {'GLOBAL':<5} : {n_outliers_global:>6} / {n_total_global:<6} points flaggés outliers  ->  {pct_global:.2f}%")

df_resultats = pd.DataFrame(resultats)
print(f"\n{df_resultats.to_string(index=False)}")