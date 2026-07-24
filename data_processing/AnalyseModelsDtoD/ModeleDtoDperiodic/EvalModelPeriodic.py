"""
eval_zeroshot_DtoD.py
════════════════════════════════════════════════════════════════════════
Évaluation zero-shot des modèles AR-LSTM DtoD, sur DAHITI ou HW Next,
10j ou 27j.

PRINCIPE DE CONCEPTION (pour éviter le bug de collision de fichiers
rencontré avec la version précédente) :
--------------------------------------------------------------------
Chaque modèle à évaluer est identifié par un LABEL UNIQUE choisi par toi
(clé du dict MODELS ci-dessous), qui sert de nom de fichier PARTOUT en
sortie. Il n'y a plus d'indexation implicite par seulement "mask_pct" +
"loss_type" (l'ancienne convention qui faisait que deux modèles
différents avec le même mask_pct et le même LOSS_TYPE="" s'écrasaient
silencieusement l'un l'autre). Ici, deux modèles avec le même label
donneraient une erreur explicite (clé de dict dupliquée en Python), donc
la collision est impossible par construction plutôt qu'évitée par
convention.

Sorties, toutes indexées par LABEL (pas par mask_pct seul) :
  ./data/outlier_detection/DtoD_eval/{source}_{freq}/{label}/
      results_per_station.csv   (NSE/KGE bruts NeuralHydrology, par station)
      residuals_full.csv        (toutes les dates où pred existe)
  ./data_processing/AnalyseModelsDtoD/residuals/
      residuals_{label}_{source}_{freq}.csv   (résidus filtrés obs+pred,
      centralisés, utilisés en entrée du script de comparaison)

Usage :
    python eval_zeroshot_DtoD.py
    (ajuster SOURCE, FREQ et MODELS ci-dessous avant de lancer)
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
SOURCE = "dahiti"   # "hwnext" ou "dahiti"
FREQ   = "10j"      # "10j" ou "27j"

# ── Modèles à évaluer ────────────────────────────────────────────
# Clé du dict = LABEL UNIQUE (utilisé dans tous les noms de fichiers,
# les CSV, les graphes). Choisis-le explicite et distinctif : inclure
# la méthode de masquage si elle diffère (ex: "_periodic", "_block"),
# pas seulement le taux, pour ne jamais retomber dans le bug précédent.
MODELS = {
    # "DtoD80_NSE":      {"run_name": "arlstm_DtoD80_1506_150002",          "epoch": 12, "mask": 80},
    # "DtoD90_NSE":      {"run_name": "arlstm_DtoD90_1606_111709",          "epoch": 14, "mask": 90},
    # "DtoD96_NSE":      {"run_name": "arlstm_DtoD96_1606_164901",          "epoch": 13, "mask": 96},
    "DtoD80_periodic": {"run_name": "arlstm_DtoD80_periodic_0607_151150", "epoch": 18,  "mask": 80},
    "DtoD90_periodic": {"run_name": "arlstm_DtoD90_periodic_0607_151027", "epoch": 16,  "mask": 90},
    "DtoD96_block":    {"run_name": "arlstm_DtoD96_block_0607_150907",    "epoch": 20, "mask": 96},
}

RUNS_DIR   = Path("./runs")
TARGET_VAR = "water_level"

EVAL_ROOT     = Path(f"./data/outlier_detection/DtoD_eval/{SOURCE}_{FREQ}")
RESIDUALS_DIR = Path("./data_processing/AnalyseModelsDtoD/residuals")
RESIDUALS_DIR.mkdir(parents=True, exist_ok=True)

GAP_RANGES = {"10j": (7, 15), "27j": (22, 32)}  # filtre HW Next uniquement

# ═══════════════════════════════════════════════════════════════
# CHEMINS — dépendent de SOURCE / FREQ
# ═══════════════════════════════════════════════════════════════
if SOURCE == "hwnext":
    NC_DIR       = Path("./data/IA/NeuralHydrologyHWNextDtoD/time_series")
    DATA_DIR     = Path("./data/IA/NeuralHydrologyHWNextDtoD")
    STATIONS_OUT = Path(f"./data/IA/NeuralHydrologyHWNextDtoD/stations_hwnext_{FREQ}_eval.txt")
    GAP_MIN, GAP_MAX = GAP_RANGES[FREQ]
elif SOURCE == "dahiti":
    DATA_DIR     = Path("./data/IA/NeuralHydrologyDahitiDtoD")
    STATIONS_TXT = Path(f"./data/IA/NeuralHydrologyDahiti{FREQ}Clean/stations_dahiti_{FREQ}.txt")
    STATIONS_OUT = Path(f"./data/IA/NeuralHydrologyDahitiDtoD/stations_dahiti_{FREQ}_eval.txt")
else:
    raise ValueError(f"SOURCE inconnu : {SOURCE} (attendu 'hwnext' ou 'dahiti')")

EVAL_ROOT.mkdir(parents=True, exist_ok=True)

# ═══════════════════════════════════════════════════════════════
# ÉTAPE 1 — DÉTERMINATION DE LA LISTE DE STATIONS
# ═══════════════════════════════════════════════════════════════
print("=" * 60)
print(f"  ÉTAPE 1 — Liste de stations  [{SOURCE.upper()} {FREQ}]")
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
else:
    print(f"  Chargement liste DAHITI déjà préparée : {STATIONS_TXT}")
    stations_eval = [s.strip() for s in STATIONS_TXT.read_text().split() if s.strip()]

STATIONS_OUT.write_text("\n".join(stations_eval))
print(f"  Stations retenues : {len(stations_eval)}")
print(f"  Fichier stations  : {STATIONS_OUT}\n")

if len(stations_eval) == 0:
    raise SystemExit("Aucune station retenue — vérifier les chemins/seuils ci-dessus.")

# ═══════════════════════════════════════════════════════════════
# HELPERS — extraction résidus
# ═══════════════════════════════════════════════════════════════
def extract_residuals(raw, stations_filter, full=False):
    """
    full=False : uniquement les dates où obs ET pred existent (pour l'analyse
                 statistique en aval).
    full=True  : toutes les dates où pred existe, obs peut être NaN (pour
                 visualiser la prédiction en boucle fermée même sans vérité
                 terrain).
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
                    "station": str(station), "date": d,
                    "obs":  float(o) if not np.isnan(o) else np.nan,
                    "pred": float(p) if not np.isnan(p) else np.nan,
                })
        except Exception as e:
            print(f"    ⚠ {station} : {e}")

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])

    if full:
        return df.dropna(subset=["pred"]).reset_index(drop=True)
    return df.dropna(subset=["obs", "pred"]).reset_index(drop=True)

# ═══════════════════════════════════════════════════════════════
# ÉTAPE 2 — ÉVALUATION ZERO-SHOT PAR MODÈLE
# ═══════════════════════════════════════════════════════════════
print("=" * 60)
print(f"  ÉTAPE 2 — Évaluation zero-shot  [{SOURCE.upper()} {FREQ}]")
print("=" * 60)

results_summary = []

for label, cfg_info in MODELS.items():
    run_name = cfg_info["run_name"]
    epoch    = cfg_info["epoch"]
    mask     = cfg_info["mask"]

    print(f"\n  [{label}] run={run_name}  epoch={epoch}  mask={mask}%")

    run_dir = RUNS_DIR / run_name
    if not run_dir.exists():
        print(f"  ⚠ Run introuvable : {run_dir} → SKIP")
        continue

    label_dir = EVAL_ROOT / label
    label_dir.mkdir(parents=True, exist_ok=True)

    ryaml = YAML()
    ryaml.preserve_quotes = True
    with open(run_dir / "config.yml") as f:
        cfg_dict = ryaml.load(f)

    cfg_dict["validation_basin_file"] = str(STATIONS_OUT.resolve())
    cfg_dict["data_dir"]              = str(DATA_DIR.resolve())

    # Config d'éval nommée par LABEL (pas par mask/loss) -> pas de collision
    # possible même si deux labels partagent le même run_name/checkpoint.
    config_eval = run_dir / f"config_eval_{SOURCE}_{FREQ}_{label}.yml"
    with open(config_eval, "w") as f:
        ryaml.dump(cfg_dict, f)

    cfg = Config(config_eval)
    start_evaluation(cfg=cfg, run_dir=run_dir, epoch=epoch, period="validation")
    print("  Évaluation terminée")

    results_path = run_dir / "validation" / f"model_epoch{epoch:03d}" / "validation_results.p"
    if not results_path.exists():
        print(f"  ⚠ Résultats introuvables : {results_path}")
        continue

    with open(results_path, "rb") as f:
        raw = pickle.load(f)

    # ── Métriques par station (NSE/KGE natives NeuralHydrology) ──────
    # ATTENTION : le KGE ici utilise probablement le beta classique
    # (ratio de moyenne) -> peut exploser sur des données z-scorées.
    # Pour toute analyse fine du KGE, préférer le recalcul sans beta
    # fait dans le script de comparaison, à partir des résidus obs/pred.
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
                per_station.append({"station": str(station), "label": label,
                                     "mask_pct": mask, "NSE": nse, "KGE": kge})
        except Exception:
            continue

    df_sta = pd.DataFrame(per_station)
    df_sta.to_csv(label_dir / "results_per_station.csv", index=False)

    # ── Résidus filtrés -> centralisés, nommés par LABEL ──────────────
    df_res = extract_residuals(raw, stations_eval, full=False)
    if not df_res.empty:
        df_res["label"] = label
        df_res["mask_pct"] = mask
    res_csv = RESIDUALS_DIR / f"residuals_{label}_{SOURCE}_{FREQ}.csv"
    df_res.to_csv(res_csv, index=False)
    print(f"  Résidus filtrés ({len(df_res)} lignes, "
          f"{df_res['station'].nunique() if not df_res.empty else 0} stations) → {res_csv}")

    # ── Résidus complets (boucle fermée) -> dans le dossier du label ──
    df_full = extract_residuals(raw, stations_eval, full=True)
    df_full.to_csv(label_dir / "residuals_full.csv", index=False)
    print(f"  Résidus complets ({len(df_full)} lignes) → {label_dir / 'residuals_full.csv'}")

    # ── Résumé ────────────────────────────────────────────────────────
    nse_arr, kge_arr = np.array(nse_vals), np.array(kge_vals)
    row = {
        "label": label, "run_name": run_name, "mask_pct": mask, "n": len(nse_vals),
        "nse_med": float(np.median(nse_arr)) if len(nse_arr) else np.nan,
        "nse_mean": float(np.mean(nse_arr)) if len(nse_arr) else np.nan,
        "nse_gt0": int(np.sum(nse_arr > 0)) if len(nse_arr) else 0,
        "nse_gt05": int(np.sum(nse_arr > 0.5)) if len(nse_arr) else 0,
        "kge_med": float(np.median(kge_arr)) if len(kge_arr) else np.nan,
    }
    results_summary.append(row)
    print(f"  N stations : {row['n']}  |  NSE médian : {row['nse_med']:.3f}  |  "
          f"NSE>0.5 : {row['nse_gt05']}  |  KGE médian (natif) : {row['kge_med']:.3f}")

# ═══════════════════════════════════════════════════════════════
# ÉTAPE 3 — TABLEAU RÉCAP
# ═══════════════════════════════════════════════════════════════
if not results_summary:
    print("\nAucun résultat — vérifier les run_name/epoch dans MODELS.")
else:
    df_cmp = pd.DataFrame(results_summary).sort_values("label")
    summary_csv = EVAL_ROOT / "summary_all_labels.csv"
    df_cmp.to_csv(summary_csv, index=False)

    print(f"\n{'=' * 70}")
    print(f"  RÉSUMÉ — Zero-shot {SOURCE.upper()} {FREQ}")
    print(f"{'=' * 70}")
    print(f"  {'label':<18} {'n':>5} {'NSE med':>9} {'NSE moy':>9} {'NSE>0.5':>8} {'KGE med':>9}")
    for _, row in df_cmp.iterrows():
        print(f"  {row['label']:<18} {int(row['n']):>5} {row['nse_med']:>9.3f} "
              f"{row['nse_mean']:>9.3f} {int(row['nse_gt05']):>8} {row['kge_med']:>9.3f}")

    print(f"\n  Résumé → {summary_csv}")
    print(f"  Résidus centralisés (entrée du script de comparaison) → {RESIDUALS_DIR}/")
    print("\nDone")