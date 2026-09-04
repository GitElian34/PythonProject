"""
eval_quantile_bands.py
════════════════════════════════════════════════════════════════════════
Variante dédiée aux modèles QUANTILE UNIQUEMENT : extrait les 5 niveaux
de quantile (Q5/Q25/Q50/Q75/Q95) par date, au lieu de seulement Q50
comme le fait eval_dtod_quantile.py. Nécessaire pour tracer la bande
d'incertitude complète (cf. plot_stations_outliers_quantile_bands.py).

Lit les variables xarray NOMMÉES par quantile (water_level_sim_q05,
_q25, _q50, _q75, _q95 -- structure réellement observée dans vos
résultats), chacune en (date, time_step) -- pas un tableau 3D empilé
avec un axe quantile comme les premières versions de ce script le
supposaient à tort.

Ne touche pas au pipeline existant (eval_dtod_quantile.py) -- écrit un
fichier séparé, suffixé "_bands", pour ne rien écraser :

    Models_Testing/Quantille/residus/residuals_{label}_{SOURCE}_{freq}_bands.csv
    colonnes : station, date, obs, pred_q05, pred_q25, pred_q50, pred_q75, pred_q95

Usage :
    python eval_quantile_bands.py
    (ajuster SOURCE, FREQS et MODELS ci-dessous avant de lancer)
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

TARGET_VAR = "water_level"
PERIOD = "validation"

QUANTILE_TARGETS = [0.05, 0.25, 0.5, 0.75, 0.95]
QUANTILE_COL_NAMES = {0.05: "pred_q05", 0.25: "pred_q25", 0.5: "pred_q50",
                       0.75: "pred_q75", 0.95: "pred_q95"}
# Suffixe de variable xarray par quantile cible -- structure réelle observée :
# chaque quantile est une variable SÉPARÉE (water_level_sim_q05, _q25, ...),
# PAS un seul tableau empilé avec un axe quantile.
QUANTILE_VAR_SUFFIX = {0.05: "q05", 0.25: "q25", 0.5: "q50", 0.75: "q75", 0.95: "q95"}

SOURCE = "hwnext"   # <-- "hwnext" ou "dahiti", doit matcher les autres scripts

SOURCE_CONFIG = {
    "hwnext": {
        "data_dir": {
            "10j": Path("./data/IA/NeuralHydrologyHWNextDtoD"),
            "27j": Path("./data/IA/NeuralHydrologyHWNextDtoD"),
        },
        "stations_file": {"10j": None, "27j": None},
    },
    "dahiti": {
        "data_dir": {
            "10j": Path("./data/IA/NeuralHydrologyDahitiDtoD"),
            "27j": Path("./data/IA/NeuralHydrologyDahitiDtoD"),
        },
        "stations_file": {
            "10j": Path("./data/IA/NeuralHydrologyDahiti10jClean/stations_dahiti_10j.txt"),
            "27j": Path("./data/IA/NeuralHydrologyDahiti27jClean/stations_dahiti_27j.txt"),
        },
    },
}

# Modèles Quantile UNIQUEMENT (les 3 sont nécessaires pour le consensus
# de plot_stations_outliers_quantile_bands.py -- REQUIRED_MODEL=Quantile96
# + au moins 1 des 2 autres)
MODELS = [
    {"label": "Quantile80", "model_dir": "arlstm_DtoD80_quantile_3006_155128", "epoch": 19},
    # {"label": "Quantile90", "model_dir": "arlstm_DtoD90_quantile_3006_154719", "epoch": 16},
    # {"label": "Quantile96", "model_dir": "arlstm_DtoD96_quantile_3006_155152", "epoch": 19},
]

FREQS = ["10j", "27j"]
GAP_RANGES = {"10j": (7, 15), "27j": (22, 32)}

OUT_DIR = Path("./Models_Testing/Quantille/residus")


# ═══════════════════════════════════════════════════════════════
# LISTE DE STATIONS (identique à eval_dtod_quantile.py)
# ═══════════════════════════════════════════════════════════════
def build_stations_list(freq: str) -> list:
    stations_file = SOURCE_CONFIG[SOURCE]["stations_file"][freq]
    if stations_file is not None:
        if not stations_file.exists():
            print(f"  ⚠ Fichier stations introuvable : {stations_file}")
            return []
        stations = [line.strip() for line in stations_file.read_text().splitlines() if line.strip()]
        print(f"  Liste pré-filtrée lue : {stations_file} ({len(stations)} stations)")
        return stations

    import netCDF4 as nc
    nc_dir = SOURCE_CONFIG[SOURCE]["data_dir"][freq] / "time_series"
    gap_min, gap_max = GAP_RANGES[freq]
    print(f"  Filtre par gap médian sur {nc_dir} : {gap_min}-{gap_max}j")
    stations_eval = []
    for f in sorted(nc_dir.glob("*.nc")):
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
        if gap_min <= gap_med <= gap_max:
            stations_eval.append(f.stem)
    return stations_eval



def inspect_structure(results_p: Path):
    with open(results_p, "rb") as f:
        raw = pickle.load(f)
    first_sid = next(iter(raw))
    freq_key = list(raw[first_sid].keys())[0]
    ds = raw[first_sid][freq_key]["xr"]
    print(f"\n  [INSPECTION] station exemple : {first_sid}")
    for var in ds.data_vars:
        print(f"    {var} : dims={ds[var].dims}, shape={ds[var].shape}")
    return ds


def extract_obs_bands(ds):
    """Retourne (dates, obs_arr, {target: pred_arr}) -- nowcast (dernier
    time_step) pour obs et pour CHAQUE quantile cible, lus depuis les
    variables nommées water_level_sim_q05/q25/q50/q75/q95 (structure
    réelle des runs Quantile -- pas un tableau 3D empilé)."""
    obs_var = f"{TARGET_VAR}_obs"
    if obs_var not in ds:
        return None, None, None

    missing = [suf for suf in QUANTILE_VAR_SUFFIX.values() if f"{TARGET_VAR}_sim_{suf}" not in ds]
    if missing:
        print(f"    ⚠ Variables manquantes pour les bandes : "
              f"{[f'{TARGET_VAR}_sim_{s}' for s in missing]}")
        return None, None, None

    dates = pd.to_datetime(ds.date.values)
    obs_vals = ds[obs_var].values
    obs_arr = obs_vals[:, -1] if obs_vals.ndim >= 2 else obs_vals.flatten()

    preds = {}
    for target, suffix in QUANTILE_VAR_SUFFIX.items():
        v = ds[f"{TARGET_VAR}_sim_{suffix}"].values
        preds[target] = v[:, -1] if v.ndim >= 2 else v.flatten()

    if len(obs_arr) != len(dates):
        return None, None, None

    return dates, obs_arr, preds


# ═══════════════════════════════════════════════════════════════
# ÉVALUATION D'UN MODÈLE (bandes complètes)
# ═══════════════════════════════════════════════════════════════
def run_one(model_cfg: dict, freq: str, stations_eval: list):
    label = model_cfg["label"]
    run_dir = Path(f"./runs/{model_cfg['model_dir']}")
    epoch = model_cfg["epoch"]
    stations_eval_set = set(stations_eval)
    data_dir = SOURCE_CONFIG[SOURCE]["data_dir"][freq]

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    print("\n" + "=" * 60)
    print(f"  BANDES QUANTILE {label} [{freq}]  source={SOURCE}")
    print(f"  Modèle : {model_cfg['model_dir']}  epoch {epoch}")
    print("=" * 60)

    if not run_dir.exists():
        print(f"  ⚠ Run introuvable : {run_dir} -> skip")
        return
    if not stations_eval or not data_dir.exists():
        print(f"  ⚠ Stations ou data_dir manquant -> skip")
        return

    stations_out = data_dir / f"stations_{SOURCE}_{freq}_eval.txt"
    data_dir.mkdir(parents=True, exist_ok=True)
    stations_out.write_text("\n".join(stations_eval))

    ryaml = YAML()
    ryaml.preserve_quotes = True
    with open(run_dir / "config.yml") as f:
        cfg_dict = ryaml.load(f)

    cfg_dict["validation_basin_file"] = str(stations_out.resolve())
    cfg_dict["data_dir"] = str(data_dir.resolve())
    cfg_dict["validate_n_random_basins"] = len(stations_eval) + 10000

    config_eval = run_dir / f"config_eval_bands_{SOURCE}_{freq}_{label}.yml"
    with open(config_eval, "w") as f:
        ryaml.dump(cfg_dict, f)

    cfg = Config(config_eval)
    start_evaluation(cfg=cfg, run_dir=run_dir, epoch=epoch, period=PERIOD)
    print("  Évaluation terminée")

    results_p = run_dir / PERIOD / f"model_epoch{epoch:03d}" / f"{PERIOD}_results.p"
    if not results_p.exists():
        print(f"  ⚠ Résultats introuvables : {results_p}")
        return

    ds_sample = inspect_structure(results_p)
    missing = [suf for suf in QUANTILE_VAR_SUFFIX.values() if f"{TARGET_VAR}_sim_{suf}" not in ds_sample]
    if missing:
        print(f"  ⚠ {label} : variables de quantile absentes "
              f"({[f'{TARGET_VAR}_sim_{s}' for s in missing]}) -> pas un modèle Quantile "
              f"avec cette structure, skip")
        return

    with open(results_p, "rb") as f:
        raw = pickle.load(f)
    print(f"  {len(raw)} stations chargées")

    rows = []
    for sid, sub in raw.items():
        if str(sid) not in stations_eval_set:
            continue
        try:
            freq_key = list(sub.keys())[0]
            ds = sub[freq_key]["xr"]
            dates, obs_arr, preds = extract_obs_bands(ds)
            if dates is None:
                continue
            for i, d in enumerate(dates):
                row = {"station": str(sid), "date": d,
                       "obs": float(obs_arr[i]) if not np.isnan(obs_arr[i]) else np.nan}
                for target, arr in preds.items():
                    col = QUANTILE_COL_NAMES[target]
                    row[col] = float(arr[i]) if not np.isnan(arr[i]) else np.nan
                rows.append(row)
        except Exception as e:
            print(f"  ⚠ {sid} : {e}")

    df = pd.DataFrame(rows)
    if df.empty:
        print(f"  ⚠ Aucune donnée extraite pour {label} [{freq}]")
        return
    df["date"] = pd.to_datetime(df["date"])

    out_csv = OUT_DIR / f"residuals_{label}_{SOURCE}_{freq}_bands.csv"
    df.to_csv(out_csv, index=False)
    print(f"  ✅ CSV bandes -> {out_csv}")
    print(f"     Lignes : {len(df)} | Stations : {df['station'].nunique()}")


# ═══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    stations_by_freq = {}
    for freq in FREQS:
        print(f"\n{'=' * 60}\n  LISTE STATIONS [{SOURCE} / {freq}]\n{'=' * 60}")
        stations_by_freq[freq] = build_stations_list(freq)
        print(f"  Stations retenues : {len(stations_by_freq[freq])}")

    for model_cfg in MODELS:
        for freq in FREQS:
            run_one(model_cfg, freq, stations_by_freq[freq])

    print("\n✅ Terminé.")