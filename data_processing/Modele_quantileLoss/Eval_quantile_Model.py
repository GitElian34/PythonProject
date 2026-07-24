"""
eval_zeroshot_quantile_DtoD.py
════════════════════════════════════════════════════════════════════════
Évaluation zero-shot des 3 nouveaux modèles AR-LSTM DtoD entraînés avec
la quantile loss (têtes QuantileRegression, sorties Q05/Q25/Q50/Q75/Q95).

Version dérivée de eval_zeroshot_generic_DtoD.py, avec 2 différences :
  1. Les 3 modèles sont les checkpoints quantile (DtoD80/90/96).
  2. Limité à un petit nombre de stations (MAX_STATIONS) pour un premier
     test rapide avant de lancer l'évaluation complète.
  3. Les résidus extraits incluent aussi les quantiles Q05/Q25/Q75/Q95
     (en plus de obs/pred=Q50), pour pouvoir calculer la couverture et
     la largeur d'intervalle en plus des métriques NSE/KGE classiques.

Usage :
    python eval_zeroshot_quantile_DtoD.py
    (ajuster SOURCE, FREQ et MAX_STATIONS ci-dessous avant de lancer)
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
FREQ      = "10j"      # "10j" ou "27j"
LOSS_TYPE = "quantile" # annoté dans les noms de sortie

# Limite du nombre de stations pour ce premier test (None = toutes les stations)
MAX_STATIONS = None

# ── Les 3 modèles quantile à évaluer (checkpoints du 30/06) ──
MODELS = {
    "arlstm_DtoD80_quantile_3006_155128": {"epoch": 9,  "mask": 80},
    "arlstm_DtoD90_quantile_3006_154719": {"epoch": 10, "mask": 90},
    "arlstm_DtoD96_quantile_3006_155152": {"epoch": 13, "mask": 96},
}

QUANTILES = [0.05, 0.25, 0.50, 0.75, 0.95]

RUNS_DIR   = Path("./runs")
TARGET_VAR = "water_level"

# Dossier centralisé pour les résidus complets de chaque modèle
RESIDUALS_DIR = Path("./data_processing/AnalyseModelsDtoD/residuals_quantile")
RESIDUALS_DIR.mkdir(parents=True, exist_ok=True)

# Filtre de gap médian pour HW Next (non utilisé pour DAHITI)
GAP_RANGES = {"10j": (7, 15), "27j": (22, 32)}

# ═══════════════════════════════════════════════════════════════
# CHEMINS — dépendent de SOURCE / FREQ
# ═══════════════════════════════════════════════════════════════
suffix_out = f"_{LOSS_TYPE}" if LOSS_TYPE else ""

if SOURCE == "hwnext":
    NC_DIR       = Path("./data/IA/NeuralHydrologyHWNextDtoD/time_series")
    DATA_DIR     = Path("./data/IA/NeuralHydrologyHWNextDtoD")
    STATIONS_OUT = Path(f"./data/IA/NeuralHydrologyHWNextDtoD/stations_hwnext_{FREQ}_eval_test5.txt")
    GAP_MIN, GAP_MAX = GAP_RANGES[FREQ]
elif SOURCE == "dahiti":
    DATA_DIR     = Path("./data/IA/NeuralHydrologyDahitiDtoD")
    STATIONS_TXT = Path(f"./data/IA/NeuralHydrologyDahiti{FREQ}Clean/stations_dahiti_{FREQ}.txt")
    STATIONS_OUT = Path(f"./data/IA/NeuralHydrologyDahitiDtoD/stations_dahiti_{FREQ}_eval_test5.txt")
else:
    raise ValueError(f"SOURCE inconnu : {SOURCE} (attendu 'hwnext' ou 'dahiti')")

OUTPUT_DIR = Path(f"./data/outlier_detection/benchmark_DtoD_{SOURCE}{FREQ}{suffix_out}_test{MAX_STATIONS or 'all'}")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ═══════════════════════════════════════════════════════════════
# ÉTAPE 1 — DÉTERMINATION DE LA LISTE DE STATIONS (limitée)
# ═══════════════════════════════════════════════════════════════
print("=" * 60)
print(f"  ÉTAPE 1 — Liste de stations  [{SOURCE.upper()} {FREQ} | quantile]")
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

print(f"  Stations disponibles avant limite : {len(stations_eval)}")

if MAX_STATIONS is not None:
    stations_eval = stations_eval[:MAX_STATIONS]
    print(f"  → Limité à {MAX_STATIONS} station(s) pour ce test")

STATIONS_OUT.write_text("\n".join(stations_eval))
print(f"  Stations retenues : {len(stations_eval)} → {stations_eval}")
print(f"  Fichier stations  : {STATIONS_OUT}\n")

if len(stations_eval) == 0:
    print("Aucune station retenue — vérifier les chemins/seuils ci-dessus.")
    exit()

# ═══════════════════════════════════════════════════════════════
# HELPERS — extraction résidus (avec quantiles)
# ═══════════════════════════════════════════════════════════════
def _get_quantile_col(ds, q):
    """Retourne le nom de colonne quantile s'il existe (ex: water_level_sim_q05)."""
    col = f"{TARGET_VAR}_sim_q{int(q * 100):02d}"
    return col if col in ds else None


def extract_residuals(raw, stations_filter):
    """Résidus filtrés (obs et pred=Q50 non-NaN), avec colonnes quantiles si présentes."""
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

            # colonnes quantile disponibles (None si absentes)
            q_cols = {q: _get_quantile_col(ds, q) for q in QUANTILES}
            q_arrs = {
                q: (ds[col].values.flatten() if col else None)
                for q, col in q_cols.items()
            }

            for i, (d, o, p) in enumerate(zip(dates, obs_arr, pred_arr)):
                row = {
                    "station" : str(station), "date": d,
                    "obs"     : float(o) if not np.isnan(o) else np.nan,
                    "pred"    : float(p) if not np.isnan(p) else np.nan,
                    "residual": float(o - p) if not (np.isnan(o) or np.isnan(p)) else np.nan,
                }
                for q, arr in q_arrs.items():
                    row[f"q{int(q*100):02d}"] = float(arr[i]) if arr is not None and not np.isnan(arr[i]) else np.nan
                rows.append(row)
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
    """Résidus complets (toutes les dates où pred existe), avec quantiles."""
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

            q_cols = {q: _get_quantile_col(ds, q) for q in QUANTILES}
            q_arrs = {
                q: (ds[col].values.flatten() if col else None)
                for q, col in q_cols.items()
            }

            for i, (d, o, p) in enumerate(zip(dates, obs_arr, pred_arr)):
                row = {
                    "station": str(station), "date": d,
                    "obs" : float(o) if not np.isnan(o) else np.nan,
                    "pred": float(p) if not np.isnan(p) else np.nan,
                }
                for q, arr in q_arrs.items():
                    row[f"q{int(q*100):02d}"] = float(arr[i]) if arr is not None and not np.isnan(arr[i]) else np.nan
                rows.append(row)
        except Exception as e:
            print(f"    ⚠ {station} : {e}")

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])
    return df.dropna(subset=["pred"]).reset_index(drop=True)


def compute_calibration(df):
    """Couverture empirique de l'intervalle [Q05, Q95] et largeur moyenne, si dispo."""
    if "q05" not in df.columns or "q95" not in df.columns:
        return None
    sub = df.dropna(subset=["obs", "q05", "q95"])
    if sub.empty:
        return None
    coverage_90 = float(((sub["obs"] >= sub["q05"]) & (sub["obs"] <= sub["q95"])).mean())
    sharpness   = float((sub["q95"] - sub["q05"]).mean())
    return {"coverage_90": coverage_90, "sharpness_90": sharpness, "n": len(sub)}

# ═══════════════════════════════════════════════════════════════
# ÉTAPE 2 — ÉVALUATION ZERO-SHOT PAR MODÈLE
# ═══════════════════════════════════════════════════════════════
print("=" * 60)
print(f"  ÉTAPE 2 — Évaluation zero-shot  [{SOURCE.upper()} {FREQ} | quantile]")
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

    config_eval = run_dir / f"config_eval_{SOURCE}_{FREQ}{suffix_out}_test.yml"
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

    # ── Métriques par station (calculées sur Q50, cf. alias water_level_sim) ──
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

    # ── Résidus filtrés (+ quantiles) ────────────────────────────
    df_res = extract_residuals(raw, stations_eval)
    if not df_res.empty:
        df_res["model"] = model_name
        df_res["mask_pct"] = mask
        df_res["loss_type"] = LOSS_TYPE
    res_csv = OUTPUT_DIR / f"residuals_{SOURCE}_{FREQ}_{mask}pct.csv"
    df_res.to_csv(res_csv, index=False)
    print(f"  Résidus ({len(df_res)} lignes, {df_res['station'].nunique() if not df_res.empty else 0} stations) → {res_csv}")

    # ── Calibration des intervalles [Q05, Q95] ───────────────────
    calib = compute_calibration(df_res)
    if calib:
        print(f"  Couverture [Q05,Q95] = {calib['coverage_90']:.1%} "
              f"(attendu ≈90%)  | largeur moyenne = {calib['sharpness_90']:.3f}  (n={calib['n']})")
    else:
        print("  Colonnes quantile absentes du CSV — calibration non calculée.")

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
        "coverage_90": calib["coverage_90"] if calib else np.nan,
        "sharpness_90": calib["sharpness_90"] if calib else np.nan,
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

    print(f"\n{'='*75}")
    print(f"  RESULTATS COMPARATIFS — Zero-shot {SOURCE.upper()} {FREQ} — quantile "
          f"(test {MAX_STATIONS or 'toutes'} stations)")
    print(f"{'='*75}")
    print(f"  {'masquage':>10} {'n':>5} {'NSE med':>9} {'NSE moy':>9}"
          f" {'NSE>0':>6} {'NSE>0.5':>8} {'KGE med':>9} {'cov90':>8} {'sharp90':>9}")
    print(f"  {'-'*73}")
    for _, row in df_cmp.iterrows():
        cov  = f"{row['coverage_90']:.1%}" if not np.isnan(row["coverage_90"]) else "n/a"
        shrp = f"{row['sharpness_90']:.3f}" if not np.isnan(row["sharpness_90"]) else "n/a"
        print(f"  {int(row['mask_pct']):>9}% {int(row['n']):>5} "
              f"{row['nse_med']:>9.3f} {row['nse_mean']:>9.3f} "
              f"{int(row['nse_gt0']):>6} {int(row['nse_gt05']):>8} {row['kge_med']:>9.3f} "
              f"{cov:>8} {shrp:>9}")

    print(f"\n  Sorties dans : {OUTPUT_DIR}/")
    print(f"    benchmark_DtoD_{SOURCE}{FREQ}{suffix_out}.csv")
    print(f"    results_per_station_{{mask}}pct.csv")
    print(f"    residuals_{SOURCE}_{FREQ}_{{mask}}pct.csv  (avec colonnes q05/q25/q50/q75/q95)")
    print(f"  Résidus complets centralisés dans : {RESIDUALS_DIR}/")
    print(f"\nDone")