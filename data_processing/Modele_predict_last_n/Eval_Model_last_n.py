"""
eval_single_model_predict_last_n.py
════════════════════════════════════════════════════════════════════════
Évaluation zero-shot des modèles predict_last_n (last10/last15, avec et
sans nan_handling_method) sur les 4 datasets neutres habituels du projet
(HW Next / DAHITI) x (10j / 27j), via SOURCE et FREQ ci-dessous — même
logique que eval_zeroshot_generic_DtoD.py.

Différences volontaires par rapport à eval_zeroshot_generic_DtoD.py :
  1. Un seul modèle actif à la fois dans MODELS (les 5 autres restent en
     commentaire) — pensé pour lancer chaque modèle dans un job SLURM
     séparé, sans dépendance entre les lancements.
  2. `validate_n_random_basins` est retiré de force de la config
     d'évaluation : on veut TOUTES les stations de la liste, jamais un
     tirage aléatoire (celui qui limitait la comparaison à 6 stations
     communes plus tôt).
  3. Gestion correcte de predict_last_n > 1 : extraction explicite du
     nowcast (dernier time_step, indice -1) au lieu d'un .flatten() qui
     mélangeait silencieusement les dimensions date et time_step pour les
     modèles last10/last15.
  4. Sorties dans ./data_processing/Modele_predict_last_n/residuals/,
     nommées avec le label du modèle + source + fréquence (anti-collision).

Usage :
    Ajuster SOURCE, FREQ, puis décommenter UN SEUL modèle dans MODELS,
    puis :
    python eval_single_model_predict_last_n.py
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
FREQ = "27j"        # "10j" ou "27j"
LOSS_TYPE = "predict_last_n"  # juste pour annoter/nommer les sorties

# ── Modèle à évaluer — décommenter UN SEUL à la fois ────────────────────
MODELS = {
    # "arlstm_DtoD90_last10_1607_162211": {"epoch": 12, "label": "DtoD90_last10"},
    # "arlstm_DtoD90_last15_1607_111125": {"epoch": 14, "label": "DtoD90_last15"},
    # "arlstm_DtoD90_last10_inputreplacingR_1707_154024": {"epoch": 21, "label": "DtoD90_last10_inputreplacing"},
    # "arlstm_DtoD90_last10_inputreplacing_synthetic_1707_154442": {"epoch": 25, "label": "DtoD90_last10_inputreplacing_synthetic"},
    # "arlstm_DtoD90_last10_maskedmean_1707_154212": {"epoch": 21, "label": "DtoD90_last10_maskedmean"},
    # "arlstm_DtoD90_last10_attention_1707_154232": {"epoch": 28, "label": "DtoD90_last10_attention"},

    # ── 4 nouveaux modèles à évaluer — décommenter UN SEUL à la fois ────
    # # "arlstm_DtoD90_inputreplacing_synthetic_2107_084848": {"epoch": 17, "label": "DtoD90_inputreplacing_synthetic"},
    # "arlstm_DtoD80_last10_attention_synthetic_2107_084815": {"epoch": 20, "label": "DtoD80_last10_attention_synthetic"},
    # "arlstm_DtoD90_last10_attention_synthetic_2107_084744": {"epoch": 19, "label": "DtoD90_last10_attention_synthetic"},
    "arlstm_DtoD96_last10_attention_synthetic_2107_084734": {"epoch": 14, "label": "DtoD96_last10_attention_synthetic"},
}

RUNS_DIR = Path("./runs")
TARGET_VAR = "water_level"

# Dossier de sortie demandé, centralisé pour tous les modèles/sources/fréquences
OUTPUT_DIR = Path("./data_processing/Modele_predict_last_n/residuals")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Filtre de gap médian pour HW Next (non utilisé pour DAHITI, qui a sa propre liste de stations)
GAP_RANGES = {"10j": (7, 15), "27j": (22, 32)}

# ═══════════════════════════════════════════════════════════════
# CHEMINS — dépendent de SOURCE / FREQ (identique à eval_zeroshot_generic_DtoD.py)
# ═══════════════════════════════════════════════════════════════
if SOURCE == "hwnext":
    NC_DIR = Path("./data/IA/NeuralHydrologyHWNextDtoD/time_series")
    DATA_DIR = Path("./data/IA/NeuralHydrologyHWNextDtoD")
    STATIONS_OUT = Path(f"./data/IA/NeuralHydrologyHWNextDtoD/stations_hwnext_{FREQ}_eval.txt")
    GAP_MIN, GAP_MAX = GAP_RANGES[FREQ]
elif SOURCE == "dahiti":
    DATA_DIR = Path("./data/IA/NeuralHydrologyDahitiDtoD")
    # ⚠ Adapter ce chemin si la convention de nommage diffère pour le 27j
    STATIONS_TXT = Path(f"./data/IA/NeuralHydrologyDahiti{FREQ}Clean/stations_dahiti_{FREQ}.txt")
    STATIONS_OUT = Path(f"./data/IA/NeuralHydrologyDahitiDtoD/stations_dahiti_{FREQ}_eval.txt")
else:
    raise ValueError(f"SOURCE inconnu : {SOURCE} (attendu 'hwnext' ou 'dahiti')")

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
        ds = nc.Dataset(f)
        dates = pd.to_datetime("2016-01-01") + pd.to_timedelta(ds.variables["date"][:], unit="D")
        wl = ds.variables["water_level"][:]
        ds.close()

        mask = ~np.isnan(wl)
        if mask.sum() < 10:
            continue

        dates_ok = pd.Series(dates[mask])
        gaps = dates_ok.diff().dt.days.dropna().astype(int)
        gap_med = gaps.median()

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

stations_eval_set = set(stations_eval)

# ═══════════════════════════════════════════════════════════════
# HELPERS — extraction résidus, gérant explicitement predict_last_n > 1
# ═══════════════════════════════════════════════════════════════
def extract_residuals(raw, stations_filter, model_label):
    """Résidus filtrés : dates où obs ET pred existent. Nowcast uniquement
    (dernier time_step) — jamais de .flatten() naïf sur (date, time_step)."""
    rows = []
    for station, data in raw.items():
        if str(station) not in stations_filter:
            continue
        try:
            freq_key = list(data.keys())[0]
            ds = data[freq_key]["xr"]
            obs_var = f"{TARGET_VAR}_obs"
            sim_var = f"{TARGET_VAR}_sim"
            if obs_var not in ds or sim_var not in ds:
                continue

            dates = pd.to_datetime(ds.date.values)
            obs_arr = ds[obs_var].values[:, -1]   # nowcast explicite
            pred_arr = ds[sim_var].values[:, -1]  # nowcast explicite

            if len(obs_arr) != len(dates):
                print(f"    ⚠ {station} : longueur obs != longueur dates, station ignorée")
                continue

            for d, o, p in zip(dates, obs_arr, pred_arr):
                rows.append({
                    "station": str(station), "date": d,
                    "obs": float(o) if not np.isnan(o) else np.nan,
                    "pred": float(p) if not np.isnan(p) else np.nan,
                    "residual": float(o - p) if not (np.isnan(o) or np.isnan(p)) else np.nan,
                })
        except Exception as e:
            print(f"    ⚠ {station} : {e}")

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df["date"] = pd.to_datetime(df["date"])
    df["year"] = df["date"].dt.year
    df["model"] = model_label

    def norm_grp(g):
        std = np.nanstd(g["residual"])
        g["residual_norm"] = g["residual"] / std if std > 0 else np.nan
        return g

    df = df.groupby("station", group_keys=False).apply(norm_grp)
    return df.dropna(subset=["obs", "pred"]).reset_index(drop=True)


def extract_residuals_full(raw, stations_filter, model_label):
    """Résidus complets : toutes les dates où pred existe (obs peut être NaN),
    nowcast uniquement — permet l'analyse de la boucle fermée quotidienne."""
    rows = []
    for station, data in raw.items():
        if str(station) not in stations_filter:
            continue
        try:
            freq_key = list(data.keys())[0]
            ds = data[freq_key]["xr"]
            obs_var = f"{TARGET_VAR}_obs"
            sim_var = f"{TARGET_VAR}_sim"
            if obs_var not in ds or sim_var not in ds:
                continue

            dates = pd.to_datetime(ds.date.values)
            obs_arr = ds[obs_var].values[:, -1]
            pred_arr = ds[sim_var].values[:, -1]

            if len(obs_arr) != len(dates):
                continue

            for d, o, p in zip(dates, obs_arr, pred_arr):
                rows.append({
                    "station": str(station), "date": d,
                    "obs": float(o) if not np.isnan(o) else np.nan,
                    "pred": float(p) if not np.isnan(p) else np.nan,
                })
        except Exception as e:
            print(f"    ⚠ {station} : {e}")

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])
    df["model"] = model_label
    return df.dropna(subset=["pred"]).reset_index(drop=True)


# ═══════════════════════════════════════════════════════════════
# ÉTAPE 2 — ÉVALUATION ZERO-SHOT (un seul modèle actif dans MODELS)
# ═══════════════════════════════════════════════════════════════
print("=" * 60)
print(f"  ÉTAPE 2 — Évaluation zero-shot  [{SOURCE.upper()} {FREQ}]")
print("=" * 60)

if len(MODELS) == 0:
    raise ValueError("Aucun modèle actif dans MODELS — décommenter une seule entrée.")
if len(MODELS) > 1:
    print(f"[ATTENTION] {len(MODELS)} modèles actifs dans MODELS — "
          f"prévu pour un seul à la fois, mais la boucle va tous les traiter.")

results_summary = []

for model_name, cfg_info in MODELS.items():
    epoch = cfg_info["epoch"]
    label = cfg_info["label"]

    print(f"\n  [{label}] {model_name}  epoch {epoch}")

    run_dir = RUNS_DIR / model_name
    if not run_dir.exists():
        print(f"  Run introuvable : {run_dir} → skip")
        continue

    ryaml = YAML()
    ryaml.preserve_quotes = True
    with open(run_dir / "config.yml") as f:
        cfg_dict = ryaml.load(f)

    cfg_dict["validation_basin_file"] = str(STATIONS_OUT.resolve())
    cfg_dict["data_dir"] = str(DATA_DIR.resolve())

    # Crucial : jamais de sous-échantillonnage aléatoire pour cette évaluation.
    # ATTENTION : ne pas supprimer la clé (le défaut interne semble être 0,
    # ce qui évalue ZÉRO bassin -- bug rencontré et confirmé). On la fixe à
    # une valeur volontairement bien plus grande que le nombre de stations :
    # la doc NeuralHydrology precise que toute valeur > n_basins est "clipped
    # to n_basins", donc ça garantit TOUTES les stations sans tirage aléatoire.
    old_value = cfg_dict.get("validate_n_random_basins", "absent")
    cfg_dict["validate_n_random_basins"] = len(stations_eval) + 10000
    print(f"  [INFO] validate_n_random_basins forcé à {cfg_dict['validate_n_random_basins']} "
          f"(était : {old_value}) -> sera clippé à {len(stations_eval)} par NeuralHydrology")

    config_eval = run_dir / f"config_eval_{SOURCE}_{FREQ}_{label}.yml"
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

    # ── Métriques par station (natives NeuralHydrology) ─────────────────
    nse_vals, kge_vals = [], []
    per_station = []

    for station, data in raw.items():
        if str(station) not in stations_eval_set:
            continue
        try:
            freq_key = list(data.keys())[0]
            nse = float(np.squeeze(data[freq_key]["NSE"]))
            kge = float(np.squeeze(data[freq_key]["KGE"]))
            if not np.isnan(nse):
                nse_vals.append(nse)
                kge_vals.append(kge)
                per_station.append({
                    "station": str(station), "model": label, "source": SOURCE, "freq": FREQ,
                    "NSE": nse, "KGE": kge,
                })
        except Exception:
            continue

    df_sta = pd.DataFrame(per_station)
    sta_csv = OUTPUT_DIR / f"results_per_station_{label}_{SOURCE}_{FREQ}.csv"
    df_sta.to_csv(sta_csv, index=False)
    print(f"  Métriques par station → {sta_csv}")

    # ── Résidus filtrés ─────────────────────────────────────────
    df_res = extract_residuals(raw, stations_eval_set, label)
    res_csv = OUTPUT_DIR / f"residuals_{label}_{SOURCE}_{FREQ}_filtered.csv"
    df_res.to_csv(res_csv, index=False)
    print(f"  Résidus ({len(df_res)} lignes, "
          f"{df_res['station'].nunique() if not df_res.empty else 0} stations) → {res_csv}")

    # ── Résidus complets (boucle fermée quotidienne) ─────────────
    df_full = extract_residuals_full(raw, stations_eval_set, label)
    full_csv = OUTPUT_DIR / f"residuals_{label}_{SOURCE}_{FREQ}_full.csv"
    df_full.to_csv(full_csv, index=False)
    print(f"  Résidus complets ({len(df_full)} lignes) → {full_csv}")

    # ── Résumé ─────────────────────────────────────────
    nse_arr = np.array(nse_vals)
    kge_arr = np.array(kge_vals)
    row = {
        "model": label, "source": SOURCE, "freq": FREQ, "n": len(nse_vals),
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
# ÉTAPE 3 — RÉSUMÉ FINAL
# ═══════════════════════════════════════════════════════════════
if not results_summary:
    print("\nAucun resultat — verifier les noms de runs.")
else:
    df_cmp = pd.DataFrame(results_summary)
    csv_path = OUTPUT_DIR / f"benchmark_{SOURCE}_{FREQ}_summary.csv"
    df_cmp.to_csv(csv_path, index=False)

    print(f"\n{'='*65}")
    print(f"  RESULTATS — Zero-shot {SOURCE.upper()} {FREQ}")
    print(f"{'='*65}")
    print(f"  {'modele':>35} {'n':>5} {'NSE med':>9} {'NSE moy':>9}"
          f" {'NSE>0':>6} {'NSE>0.5':>8} {'KGE med':>9}")
    print(f"  {'-'*90}")
    for _, row in df_cmp.iterrows():
        print(f"  {row['model']:>35} {int(row['n']):>5} "
              f"{row['nse_med']:>9.3f} {row['nse_mean']:>9.3f} "
              f"{int(row['nse_gt0']):>6} {int(row['nse_gt05']):>8} {row['kge_med']:>9.3f}")

    print(f"\n  Sorties dans : {OUTPUT_DIR}/")
    print(f"Done")