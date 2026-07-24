"""
eval_zeroshot_dahiti_10j_DtoD.py
════════════════════════════════════════════════════════════════════════
Évaluation zero-shot des modèles AR-LSTM DtoD sur les stations
DAHITI à fréquence ~10j.

Étapes :
  1. Charger la liste des stations 10j depuis le fichier txt
  2. Évaluer chaque modèle DtoD en zero-shot
  3. Extraire et sauvegarder les résidus (obs, pred, date)
  4. Comparer les métriques NSE / KGE

Sorties par modèle :
  residuals_dahiti_10j_{mask}pct.csv  → obs, pred, date, résidus
  results_per_station_{mask}pct.csv   → NSE/KGE par station
  benchmark_DtoD_dahiti10j.csv        → tableau comparatif global

  + une copie complète des résidus de chaque modèle dans
  ./data_processing/AnalyseModelsDtoD/residuals/residuals_dahiti_10j_{mask}pct.csv

Usage :
    python eval_zeroshot_dahiti_10j_DtoD.py
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

torch.set_num_threads(4)

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
DATA_DIR     = Path("./data/IA/NeuralHydrologyDahitiDtoD")
STATIONS_TXT = Path("./data/IA/NeuralHydrologyDahiti10jClean/stations_dahiti_10j.txt")
STATIONS_OUT = Path("./data/IA/NeuralHydrologyDahitiDtoD/stations_dahiti_10j_eval.txt")
OUTPUT_DIR   = Path("./data/outlier_detection/benchmark_DtoD_dahiti10j")
RUNS_DIR     = Path("./runs")

# Dossier centralisé pour les résidus complets de chaque modèle
RESIDUALS_DIR = Path("./data_processing/AnalyseModelsDtoD/residuals")
RESIDUALS_DIR.mkdir(parents=True, exist_ok=True)

MODELS = {
    # "arlstm_DtoD0_1506_145516"  : {"epoch": 12, "mask": 0},
    # "arlstm_DtoD20_1506_145831" : {"epoch": 12, "mask": 20},
    # "arlstm_DtoD50_1506_145950" : {"epoch": 12, "mask": 50},
    "arlstm_DtoD80_1506_150002" : {"epoch": 12, "mask": 80},
    "arlstm_DtoD90_1606_111709" : {"epoch": 14, "mask": 90},
    "arlstm_DtoD96_1606_164901" : {"epoch": 13, "mask": 96},
}


TARGET_VAR = "water_level"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ═══════════════════════════════════════════════════════════════
# ÉTAPE 1 — CHARGER LA LISTE DES STATIONS 10j
# ═══════════════════════════════════════════════════════════════
print("=" * 60)
print("  ÉTAPE 1 — Chargement stations DAHITI 10j")
print("=" * 60)

stations_10j = [s.strip() for s in STATIONS_TXT.read_text().split() if s.strip()]
STATIONS_OUT.write_text("\n".join(stations_10j))
print(f"  Stations retenues : {len(stations_10j)}")
print(f"  Source            : {STATIONS_TXT}")
print(f"  Fichier stations  : {STATIONS_OUT}\n")

if len(stations_10j) == 0:
    print("Aucune station — vérifier le fichier txt.")
    exit()

# ═══════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════
def extract_residuals(raw, stations_filter):
    rows = []
    for station, data in raw.items():
        if str(station) not in stations_filter:
            continue
        try:
            freq_key = list(data.keys())[0]
            ds       = data[freq_key]["xr"]
            obs_var  = f"{TARGET_VAR}_obs"
            sim_var  = f"{TARGET_VAR}_sim"
            if obs_var not in ds or sim_var not in ds:
                continue

            dates    = pd.to_datetime(ds.date.values)
            obs_arr  = ds[obs_var].values.flatten()
            pred_arr = ds[sim_var].values.flatten()

            for d, o, p in zip(dates, obs_arr, pred_arr):
                rows.append({
                    "station" : str(station),
                    "date"    : d,
                    "obs"     : float(o)  if not np.isnan(o) else np.nan,
                    "pred"    : float(p)  if not np.isnan(p) else np.nan,
                    "residual": float(o - p) if not (np.isnan(o) or np.isnan(p)) else np.nan,
                })
        except Exception as e:
            print(f"    ⚠ {station} : {e}")

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df["date"] = pd.to_datetime(df["date"])
    df["year"] = df["date"].dt.year

    def norm_grp(g):
        std = np.nanstd(g["residual"])
        g["residual_norm"] = g["residual"] / std if std > 0 else np.nan
        return g

    df = df.groupby("station", group_keys=False).apply(norm_grp)
    return df.dropna(subset=["obs", "pred"]).reset_index(drop=True)


def extract_residuals_full(raw, stations_filter):
    """
    Comme extract_residuals, mais conserve TOUTES les dates (y compris
    les jours où obs est NaN), pour permettre l'analyse de la
    prédiction quotidienne complète (water_level_sim tous les jours).
    """
    rows = []
    for station, data in raw.items():
        if str(station) not in stations_filter:
            continue
        try:
            freq_key = list(data.keys())[0]
            ds       = data[freq_key]["xr"]
            obs_var  = f"{TARGET_VAR}_obs"
            sim_var  = f"{TARGET_VAR}_sim"
            if obs_var not in ds or sim_var not in ds:
                continue

            dates    = pd.to_datetime(ds.date.values)
            obs_arr  = ds[obs_var].values.flatten()
            pred_arr = ds[sim_var].values.flatten()

            for d, o, p in zip(dates, obs_arr, pred_arr):
                rows.append({
                    "station" : str(station),
                    "date"    : d,
                    "obs"     : float(o) if not np.isnan(o) else np.nan,
                    "pred"    : float(p) if not np.isnan(p) else np.nan,
                })
        except Exception as e:
            print(f"    ⚠ {station} : {e}")

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])
    # Garde toutes les lignes où la prédiction existe (obs peut être NaN)
    return df.dropna(subset=["pred"]).reset_index(drop=True)


# ═══════════════════════════════════════════════════════════════
# ÉTAPE 2 — ÉVALUATION ZERO-SHOT PAR MODÈLE
# ═══════════════════════════════════════════════════════════════
print("=" * 60)
print("  ÉTAPE 2 — Évaluation zero-shot + extraction résidus")
print("=" * 60)

results_summary = []

for model_name, cfg_info in MODELS.items():
    epoch = cfg_info["epoch"]
    mask  = cfg_info["mask"]

    print(f"\n  [{mask}% NaN] {model_name}  epoch {epoch}")

    run_dir = RUNS_DIR / model_name
    if not run_dir.exists():
        print(f"  Run introuvable : {run_dir} → skip")
        continue

    ryaml = YAML()
    ryaml.preserve_quotes = True
    with open(run_dir / "config.yml") as f:
        cfg_dict = ryaml.load(f)

    cfg_dict["validation_basin_file"] = str(STATIONS_OUT.resolve())
    cfg_dict["data_dir"]              = str(DATA_DIR.resolve())

    config_eval = run_dir / "config_eval_dahiti_10j.yml"
    with open(config_eval, "w") as f:
        ryaml.dump(cfg_dict, f)

    cfg = Config(config_eval)
    start_evaluation(cfg=cfg, run_dir=run_dir, epoch=epoch, period="validation")
    print(f"  Evaluation terminee")

    results_path = (run_dir / "validation"
                    / f"model_epoch{epoch:03d}"
                    / "validation_results.p")

    if not results_path.exists():
        print(f"  Resultats introuvables : {results_path}")
        continue

    with open(results_path, "rb") as f:
        raw = pickle.load(f)

    nse_vals, kge_vals = [], []
    per_station = []

    for station, data in raw.items():
        if str(station) not in stations_10j:
            continue
        try:
            freq_key = list(data.keys())[0]
            nse = float(np.squeeze(data[freq_key]["NSE"]))
            kge = float(np.squeeze(data[freq_key]["KGE"]))
            if not np.isnan(nse):
                nse_vals.append(nse)
                kge_vals.append(kge)
                per_station.append({
                    "station" : str(station),
                    "mask_pct": mask,
                    "model"   : model_name,
                    "NSE"     : nse,
                    "KGE"     : kge,
                })
        except Exception:
            continue

    pd.DataFrame(per_station).to_csv(
        OUTPUT_DIR / f"results_per_station_{mask}pct.csv", index=False
    )
    print(f"  Métriques par station → results_per_station_{mask}pct.csv")

    # ── Résidus filtrés (obs ET pred non-NaN) — comportement existant ──
    df_res = extract_residuals(raw, stations_10j)
    df_res.to_csv(OUTPUT_DIR / f"residuals_dahiti_10j_{mask}pct.csv", index=False)
    print(f"  Résidus ({len(df_res)} lignes, {df_res['station'].nunique()} stations) → residuals_dahiti_10j_{mask}pct.csv")

    # ── Résidus complets (toutes les dates où pred existe) — nouveau ──
    df_full = extract_residuals_full(raw, stations_10j)
    full_csv = RESIDUALS_DIR / f"residuals_dahiti_10j_{mask}pct.csv"
    df_full.to_csv(full_csv, index=False)
    print(f"  Résidus complets ({len(df_full)} lignes, {df_full['station'].nunique() if not df_full.empty else 0} stations) → {full_csv}")

    nse_arr = np.array(nse_vals)
    kge_arr = np.array(kge_vals)

    row = {
        "model"    : model_name,
        "mask_pct" : mask,
        "n"        : len(nse_vals),
        "nse_med"  : float(np.median(nse_arr))  if len(nse_arr) else np.nan,
        "nse_mean" : float(np.mean(nse_arr))    if len(nse_arr) else np.nan,
        "nse_gt0"  : int(np.sum(nse_arr > 0))   if len(nse_arr) else 0,
        "nse_gt05" : int(np.sum(nse_arr > 0.5)) if len(nse_arr) else 0,
        "kge_med"  : float(np.median(kge_arr))  if len(kge_arr) else np.nan,
        "kge_mean" : float(np.mean(kge_arr))    if len(kge_arr) else np.nan,
    }
    results_summary.append(row)

    print(f"  N stations  : {row['n']}")
    print(f"  NSE median  : {row['nse_med']:.3f} | moyen : {row['nse_mean']:.3f}")
    print(f"  NSE > 0     : {row['nse_gt0']} | NSE > 0.5 : {row['nse_gt05']}")
    print(f"  KGE median  : {row['kge_med']:.3f}")

# ═══════════════════════════════════════════════════════════════
# ÉTAPE 3 — TABLEAU COMPARATIF
# ═══════════════════════════════════════════════════════════════
if not results_summary:
    print("\nAucun resultat — verifier les noms de runs.")
else:
    df_cmp = pd.DataFrame(results_summary).sort_values("mask_pct")
    df_cmp.to_csv(OUTPUT_DIR / "benchmark_DtoD_dahiti10j.csv", index=False)

    print(f"\n{'='*65}")
    print("  RESULTATS COMPARATIFS — Zero-shot DAHITI ~10j")
    print(f"{'='*65}")
    print(f"  {'masquage':>10} {'n':>5} {'NSE med':>9} {'NSE moy':>9}"
          f" {'NSE>0':>6} {'NSE>0.5':>8} {'KGE med':>9}")
    print(f"  {'-'*63}")
    for _, row in df_cmp.iterrows():
        print(f"  {int(row['mask_pct']):>9}% "
              f"{int(row['n']):>5} "
              f"{row['nse_med']:>9.3f} "
              f"{row['nse_mean']:>9.3f} "
              f"{int(row['nse_gt0']):>6} "
              f"{int(row['nse_gt05']):>8} "
              f"{row['kge_med']:>9.3f}")

    print(f"\n  Sorties dans : {OUTPUT_DIR}/")
    print(f"  Résidus complets dans : {RESIDUALS_DIR}/")
    print(f"\nDone")