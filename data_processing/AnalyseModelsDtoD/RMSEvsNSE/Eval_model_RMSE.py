"""
eval_zeroshot_generic_DtoD.py
════════════════════════════════════════════════════════════════════════
Évaluation zero-shot des modèles AR-LSTM DtoD — version générique
fusionnant les 4 combinaisons (HW Next / DAHITI) x (10j / 27j) via les
variables globales SOURCE et FREQ ci-dessous. Remplace les scripts
séparés eval_zeroshot_{hwnext,dahiti}_{10j,27j}_DtoD(_RMSE).py.

Ce qui change selon SOURCE/FREQ :
  - détermination de la liste de stations à évaluer :
      * hwnext : filtre les .nc par gap médian (7-15j ou 22-32j)
      * dahiti : charge une liste de stations déjà préparée (txt)
  - chemins de données et de sortie

Ce qui reste commun :
  - boucle d'évaluation NeuralHydrology (start_evaluation)
  - extraction des résidus (filtrés obs+pred non-NaN, et complets)
  - calcul des métriques NSE/KGE par station
  - tableau comparatif global

Sorties (dans OUTPUT_DIR, qui encode SOURCE/FREQ/LOSS_TYPE) :
  residuals_{source}_{freq}_{mask}pct.csv
  results_per_station_{mask}pct.csv
  benchmark_DtoD_{source}{freq}[_{loss_type}].csv
  + copie complète des résidus dans RESIDUALS_DIR (commun à tous les runs)

Usage :
    python eval_zeroshot_generic_DtoD.py
    (ajuster SOURCE, FREQ, LOSS_TYPE et MODELS ci-dessous avant de lancer)
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

torch.set_num_threads(2)

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES GLOBAUX — à modifier ici à chaque utilisation
# ═══════════════════════════════════════════════════════════════
SOURCE    = "dahiti"   # "hwnext" ou "dahiti"
FREQ      = "27j"      # "10j" ou "27j"
LOSS_TYPE = "periodic"     # juste pour annoter/nommer les sorties ("NSE", "RMSE", ou "" si non pertinent)

# ── Modèles à évaluer (mêmes checkpoints quel que soit SOURCE/FREQ — zero-shot) ──
# Adapter selon le run voulu. Exemple ci-dessous : les 3 modèles RMSE (80/90/96).
MODELS = {
    # "arlstm_DtoD80_2206_153130" : {"epoch": 10, "mask": 80},
    # "arlstm_DtoD90_2206_153059" : {"epoch": 9,  "mask": 90},
    # "arlstm_DtoD96_2206_153042" : {"epoch": 13, "mask": 96},
    "arlstm_DtoD80_periodic_0607_151150": {"epoch": 5, "mask": 80},
    "arlstm_DtoD90_periodic_0607_151027": {"epoch": 4, "mask": 90},
    # "arlstm_DtoD96_block_0607_150907": {"epoch": 10, "mask": 96},
}
# Pour mémoire, les équivalents NSE déjà entraînés étaient :
#   DtoD80 -> arlstm_DtoD80_1506_150002   epoch 12
#   DtoD90 -> arlstm_DtoD90_1606_111709   epoch 14
#   DtoD96 -> arlstm_DtoD96_1606_164901   epoch 13

RUNS_DIR   = Path("./runs")
TARGET_VAR = "water_level"

# Dossier centralisé pour les résidus complets de chaque modèle (commun à toutes les combinaisons)
RESIDUALS_DIR = Path("./data_processing/AnalyseModelsDtoD/residuals")
RESIDUALS_DIR.mkdir(parents=True, exist_ok=True)

# Filtre de gap médian pour HW Next (non utilisé pour DAHITI, qui a sa propre liste de stations)
GAP_RANGES = {"10j": (7, 15), "27j": (22, 32)}

# ═══════════════════════════════════════════════════════════════
# CHEMINS — dépendent de SOURCE / FREQ
# ═══════════════════════════════════════════════════════════════
suffix_out = f"_{LOSS_TYPE}" if LOSS_TYPE else ""

if SOURCE == "hwnext":
    NC_DIR       = Path("./data/IA/NeuralHydrologyHWNextDtoD/time_series")
    DATA_DIR     = Path("./data/IA/NeuralHydrologyHWNextDtoD")
    STATIONS_OUT = Path(f"./data/IA/NeuralHydrologyHWNextDtoD/stations_hwnext_{FREQ}_eval.txt")
    GAP_MIN, GAP_MAX = GAP_RANGES[FREQ]
elif SOURCE == "dahiti":
    DATA_DIR     = Path("./data/IA/NeuralHydrologyDahitiDtoD")
    # ⚠ Adapter ce chemin si la convention de nommage diffère pour le 27j
    STATIONS_TXT = Path(f"./data/IA/NeuralHydrologyDahiti{FREQ}Clean/stations_dahiti_{FREQ}.txt")
    STATIONS_OUT = Path(f"./data/IA/NeuralHydrologyDahitiDtoD/stations_dahiti_{FREQ}_eval.txt")
else:
    raise ValueError(f"SOURCE inconnu : {SOURCE} (attendu 'hwnext' ou 'dahiti')")

OUTPUT_DIR = Path(f"./data/outlier_detection/benchmark_DtoD_{SOURCE}{FREQ}{suffix_out}")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ═══════════════════════════════════════════════════════════════
# ÉTAPE 1 — DÉTERMINATION DE LA LISTE DE STATIONS
# ═══════════════════════════════════════════════════════════════
print("=" * 60)
print(f"  ÉTAPE 1 — Liste de stations  [{SOURCE.upper()} {FREQ} | loss={LOSS_TYPE or 'n/a'}]")
print("=" * 60)

if SOURCE == "hwnext":
    import netCDF4 as nc

    print(f"  Filtre HW Next par gap médian : {GAP_MIN}-{GAP_MAX}j")
    stations_eval = []
    for f in sorted(NC_DIR.glob("*.nc")):
        ds    = nc.Dataset(f)
        dates = pd.to_datetime("2016-01-01") + pd.to_timedelta(ds.variables["date"][:], unit="D")
        wl    = ds.variables["water_level"][:]
        ds.close()

        mask = ~np.isnan(wl)
        if mask.sum() < 10:
            continue

        dates_ok = pd.Series(dates[mask])
        gaps     = dates_ok.diff().dt.days.dropna().astype(int)
        gap_med  = gaps.median()

        if GAP_MIN <= gap_med <= GAP_MAX:
            stations_eval.append(f.stem)

else:  # dahiti
    print(f"  Chargement liste DAHITI déjà préparée : {STATIONS_TXT}")
    stations_eval = [s.strip() for s in STATIONS_TXT.read_text().split() if s.strip()]

STATIONS_OUT.write_text("\n".join(stations_eval))
print(f"  Stations retenues : {len(stations_eval)}")
print(f"  Fichier stations  : {STATIONS_OUT}\n")

if len(stations_eval) == 0:
    print("Aucune station retenue — vérifier les chemins/seuils ci-dessus.")
    exit()

# ═══════════════════════════════════════════════════════════════
# HELPERS — extraction résidus (communs aux 2 sources)
# ═══════════════════════════════════════════════════════════════
def extract_residuals(raw, stations_filter):
    """Résidus filtrés : seulement les dates où obs ET pred existent."""
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
                    "station" : str(station), "date": d,
                    "obs"     : float(o) if not np.isnan(o) else np.nan,
                    "pred"    : float(p) if not np.isnan(p) else np.nan,
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
    """Résidus complets : toutes les dates où pred existe (obs peut être NaN) —
    permet l'analyse de la prédiction quotidienne complète en boucle fermée."""
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
                    "station": str(station), "date": d,
                    "obs" : float(o) if not np.isnan(o) else np.nan,
                    "pred": float(p) if not np.isnan(p) else np.nan,
                })
        except Exception as e:
            print(f"    ⚠ {station} : {e}")

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])
    return df.dropna(subset=["pred"]).reset_index(drop=True)

# ═══════════════════════════════════════════════════════════════
# ÉTAPE 2 — ÉVALUATION ZERO-SHOT PAR MODÈLE
# ═══════════════════════════════════════════════════════════════
print("=" * 60)
print(f"  ÉTAPE 2 — Évaluation zero-shot  [{SOURCE.upper()} {FREQ} | loss={LOSS_TYPE or 'n/a'}]")
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

    config_eval = run_dir / f"config_eval_{SOURCE}_{FREQ}{suffix_out}.yml"
    with open(config_eval, "w") as f:
        ryaml.dump(cfg_dict, f)

    cfg = Config(config_eval)
    start_evaluation(cfg=cfg, run_dir=run_dir, epoch=epoch, period="validation")
    print(f"  Evaluation terminee")

    results_path = (run_dir / "validation" / f"model_epoch{epoch:03d}" / "validation_results.p")
    if not results_path.exists():
        print(f"  Resultats introuvables : {results_path}")
        continue

    with open(results_path, "rb") as f:
        raw = pickle.load(f)

    # ── Métriques par station ──────────────────────────────────
    nse_vals, kge_vals = [], []
    per_station = []

    for station, data in raw.items():
        if str(station) not in stations_eval:
            continue
        try:
            freq_key = list(data.keys())[0]
            nse = float(np.squeeze(data[freq_key]["NSE"]))
            kge = float(np.squeeze(data[freq_key]["KGE"]))
            if not np.isnan(nse):
                nse_vals.append(nse)
                kge_vals.append(kge)
                per_station.append({
                    "station": str(station), "mask_pct": mask, "model": model_name,
                    "loss_type": LOSS_TYPE, "NSE": nse, "KGE": kge,
                })
        except Exception:
            continue

    df_sta = pd.DataFrame(per_station)
    sta_csv = OUTPUT_DIR / f"results_per_station_{mask}pct.csv"
    df_sta.to_csv(sta_csv, index=False)
    print(f"  Métriques par station → {sta_csv}")

    # ── Résidus filtrés ─────────────────────────────────────────
    df_res = extract_residuals(raw, stations_eval)
    if not df_res.empty:
        df_res["model"] = model_name
        df_res["mask_pct"] = mask
        df_res["loss_type"] = LOSS_TYPE
    res_csv = OUTPUT_DIR / f"residuals_{SOURCE}_{FREQ}_{mask}pct.csv"
    df_res.to_csv(res_csv, index=False)
    print(f"  Résidus ({len(df_res)} lignes, {df_res['station'].nunique() if not df_res.empty else 0} stations) → {res_csv}")

    # ── Résidus complets (centralisés) ──────────────────────────
    df_full = extract_residuals_full(raw, stations_eval)
    full_csv = RESIDUALS_DIR / f"residuals_{SOURCE}_{FREQ}_{mask}pct{suffix_out}.csv"
    df_full.to_csv(full_csv, index=False)
    print(f"  Résidus complets ({len(df_full)} lignes) → {full_csv}")

    # ── Résumé global ─────────────────────────────────────────
    nse_arr = np.array(nse_vals)
    kge_arr = np.array(kge_vals)
    row = {
        "model": model_name, "loss_type": LOSS_TYPE, "mask_pct": mask, "n": len(nse_vals),
        "nse_med": float(np.median(nse_arr)) if len(nse_arr) else np.nan,
        "nse_mean": float(np.mean(nse_arr)) if len(nse_arr) else np.nan,
        "nse_gt0": int(np.sum(nse_arr > 0)) if len(nse_arr) else 0,
        "nse_gt05": int(np.sum(nse_arr > 0.5)) if len(nse_arr) else 0,
        "kge_med": float(np.median(kge_arr)) if len(kge_arr) else np.nan,
        "kge_mean": float(np.mean(kge_arr)) if len(kge_arr) else np.nan,
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
    csv_path = OUTPUT_DIR / f"benchmark_DtoD_{SOURCE}{FREQ}{suffix_out}.csv"
    df_cmp.to_csv(csv_path, index=False)

    print(f"\n{'='*65}")
    print(f"  RESULTATS COMPARATIFS — Zero-shot {SOURCE.upper()} {FREQ} — loss={LOSS_TYPE or 'n/a'}")
    print(f"{'='*65}")
    print(f"  {'masquage':>10} {'n':>5} {'NSE med':>9} {'NSE moy':>9}"
          f" {'NSE>0':>6} {'NSE>0.5':>8} {'KGE med':>9}")
    print(f"  {'-'*63}")
    for _, row in df_cmp.iterrows():
        print(f"  {int(row['mask_pct']):>9}% {int(row['n']):>5} "
              f"{row['nse_med']:>9.3f} {row['nse_mean']:>9.3f} "
              f"{int(row['nse_gt0']):>6} {int(row['nse_gt05']):>8} {row['kge_med']:>9.3f}")

    print(f"\n  Sorties dans : {OUTPUT_DIR}/")
    print(f"    benchmark_DtoD_{SOURCE}{FREQ}{suffix_out}.csv")
    print(f"    results_per_station_{{mask}}pct.csv")
    print(f"    residuals_{SOURCE}_{FREQ}_{{mask}}pct.csv")
    print(f"  Résidus complets centralisés dans : {RESIDUALS_DIR}/")
    print(f"\nDone")