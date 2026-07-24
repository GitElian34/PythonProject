"""
eval_zeroshot_hwnext_10j_DtoD.py
════════════════════════════════════════════════════════════════════════
Évaluation zero-shot des modèles AR-LSTM DtoD sur les stations
HW Next à fréquence ~10j.

Étapes :
  1. Filtrer les .nc HW Next avec gap médian entre 7 et 15j
  2. Évaluer chaque modèle DtoD en zero-shot
  3. Extraire et sauvegarder les résidus (obs, pred, date)
  4. Comparer les métriques NSE / KGE

Sorties par modèle :
  residuals_hwnext_10j_{mask}pct.csv  → obs, pred, date, résidus
  results_per_station_{mask}pct.csv   → NSE/KGE par station
  benchmark_DtoD_hwnext10j.csv        → tableau comparatif global

Usage :
    python eval_zeroshot_hwnext_10j_DtoD.py
════════════════════════════════════════════════════════════════════════
"""

import pickle
import numpy as np
import pandas as pd
import netCDF4 as nc
import torch
from pathlib import Path
from ruamel.yaml import YAML
from neuralhydrology.utils.config import Config
from neuralhydrology.evaluation.evaluate import start_evaluation

torch.set_num_threads(8)

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
NC_DIR       = Path("./data/IA/NeuralHydrologyHWNextDtoD/time_series")
DATA_DIR     = Path("./data/IA/NeuralHydrologyHWNextDtoD")
STATIONS_OUT = Path("./data/IA/NeuralHydrologyHWNextDtoD/stations_hwnext_10j_eval.txt")
OUTPUT_DIR   = Path("./data/outlier_detection/benchmark_DtoD_hwnext10j")
RUNS_DIR = Path("./runs")
GAP_MIN = 7     # même filtre que HW Next 10j
GAP_MAX = 15

MODELS = {
    # "arlstm_DtoD0_1506_145516"  : {"epoch": 12, "mask": 0},
    # "arlstm_DtoD20_1506_145831" : {"epoch": 12, "mask": 20},
    # "arlstm_DtoD50_1506_145950" : {"epoch": 12, "mask": 50},
    # "arlstm_DtoD80_1506_150002" : {"epoch": 12, "mask": 80},
    "arlstm_DtoD90_1606_111709" : {"epoch": 14, "mask": 90},
    "arlstm_DtoD96_1606_164901" : {"epoch": 13, "mask": 96}
}

TARGET_VAR = "water_level"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ═══════════════════════════════════════════════════════════════
# ÉTAPE 1 — FILTRE STATIONS ~10j
# ═══════════════════════════════════════════════════════════════
print("=" * 60)
print("  ÉTAPE 1 — Filtre stations HW Next ~10j")
print(f"  Gap médian attendu : {GAP_MIN}–{GAP_MAX}j")
print("=" * 60)

stations_10j = []

for f in sorted(NC_DIR.glob("*.nc")):
    ds    = nc.Dataset(f)
    dates = pd.to_datetime("2016-01-01") + pd.to_timedelta(
        ds.variables["date"][:], unit="D"
    )
    wl = ds.variables["water_level"][:]
    ds.close()

    mask    = ~np.isnan(wl)
    if mask.sum() < 10:
        continue

    dates_ok = pd.Series(dates[mask])
    gaps     = dates_ok.diff().dt.days.dropna().astype(int)
    gap_med  = gaps.median()

    if GAP_MIN <= gap_med <= GAP_MAX:
        stations_10j.append(f.stem)

STATIONS_OUT.write_text("\n".join(stations_10j))
print(f"  Stations retenues : {len(stations_10j)}")
print(f"  Fichier stations  : {STATIONS_OUT}\n")

if len(stations_10j) == 0:
    print("Aucune station retenue — vérifier le dossier NC_DIR et les seuils.")
    exit()

# ═══════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════
def extract_residuals(raw, stations_filter):
    """
    Extrait obs, pred, date depuis le fichier .p de NeuralHydrology.
    Retourne un DataFrame avec colonnes : station, date, obs, pred,
    residual, residual_norm, year.
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

    # Normalisation résidu par station
    def norm_grp(g):
        std = np.nanstd(g["residual"])
        g["residual_norm"] = g["residual"] / std if std > 0 else np.nan
        return g

    df = df.groupby("station", group_keys=False).apply(norm_grp)

    # Garder seulement les lignes avec obs ET pred
    return df.dropna(subset=["obs", "pred"]).reset_index(drop=True)


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

    # Config d'évaluation
    ryaml = YAML()
    ryaml.preserve_quotes = True
    with open(run_dir / "config.yml") as f:
        cfg_dict = ryaml.load(f)

    cfg_dict["validation_basin_file"] = str(STATIONS_OUT.resolve())
    cfg_dict["data_dir"]              = str(DATA_DIR.resolve())

    config_eval = run_dir / "config_eval_hwnext_10j.yml"
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

    # ── Métriques par station ──────────────────────────────────
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

    df_sta = pd.DataFrame(per_station)
    sta_csv = OUTPUT_DIR / f"results_per_station_{mask}pct.csv"
    df_sta.to_csv(sta_csv, index=False)
    print(f"  Métriques par station → {sta_csv}")

    # ── Résidus ───────────────────────────────────────────────
    df_res = extract_residuals(raw, stations_10j)
    res_csv = OUTPUT_DIR / f"residuals_hwnext_10j_{mask}pct.csv"
    df_res.to_csv(res_csv, index=False)
    print(f"  Résidus ({len(df_res)} lignes, {df_res['station'].nunique()} stations) → {res_csv}")

    # ── Résumé global ─────────────────────────────────────────
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
    csv_path = OUTPUT_DIR / "benchmark_DtoD_hwnext10j.csv"
    df_cmp.to_csv(csv_path, index=False)

    print(f"\n{'='*65}")
    print("  RESULTATS COMPARATIFS — Zero-shot HW Next ~10j")
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
    print(f"    benchmark_DtoD_hwnext10j.csv")
    print(f"    results_per_station_{{mask}}pct.csv")
    print(f"    residuals_hwnext_10j_{{mask}}pct.csv")
    print(f"\nDone")