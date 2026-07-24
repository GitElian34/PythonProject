"""
analyse_residus_NSE_vs_RMSE_insitu_generic.py
════════════════════════════════════════════════════════════════════════
Version générique fusionnant les 4 combinaisons (HW Next / DAHITI) x
(10j / 27j) via les variables globales SOURCE et FREQ ci-dessous.

Compare les 6 modèles DtoD80/90/96 (loss NSE vs loss RMSE) sur 4
métriques (NSE, KGE, RMSE, R²), calculées par station, sur 3 bases :

  1. Modèle  vs Alti    (obs/pred des résidus)
  2. Modèle  vs Insitu  (insitu LE PLUS PROCHE en distance, <= DIST_MAX_KM)
  3. Alti    vs Insitu  (baseline indépendante du modèle)

L'insitu utilisé est le plus proche en distance (comme dans
benchmark_hwnext_final.py / benchmark_dahiti_final.py), pas le
"meilleur NSE" empirique — pour éviter le risque de data dredging.

Hypothèse de nommage des fichiers d'entrée (produits par
eval_zeroshot_generic_DtoD.py) :
  ./data/outlier_detection/benchmark_DtoD_{SOURCE}{FREQ}[{LOSS_SUFFIX}]/
      residuals_{SOURCE}_{FREQ}_{mask}pct.csv
  avec LOSS_SUFFIX = "" pour NSE, "_RMSE" pour RMSE (cf. LOSS_SUFFIX_MAP).

Classements produits :
  - Classement des 6 modèles sur "Modèle vs Alti"
  - Classement des 6 modèles sur "Modèle vs Insitu" (le plus pertinent)

Sorties (dans OUTPUT_DIR, qui encode SOURCE/FREQ) :
  metrics_per_station.csv
  ranking_vs_alti.csv
  ranking_vs_insitu.csv
  baseline_alti_vs_insitu.csv
════════════════════════════════════════════════════════════════════════
"""

import sqlite3
import numpy as np
import pandas as pd
import geopandas as gpd
from pathlib import Path
from shapely.geometry import Point

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES GLOBAUX — à modifier ici à chaque utilisation
# ═══════════════════════════════════════════════════════════════
SOURCE = "hwnext"   # "hwnext" ou "dahiti"
FREQ   = "27j"      # "10j" ou "27j"

MASKS = [80, 90, 96]
# Suffixe de dossier selon la loss d'entraînement (cf. eval_zeroshot_generic_DtoD.py)
LOSS_SUFFIX_MAP = {"NSE": "", "RMSE": "_RMSE"}

HW_DB      = "./data/hydroweb_next.db"
DAHITI_DB  = "./data/dahiti.db"
INSITU_DB  = "./data/insitu_data.db"
INSITU_SHP = "./data/insitu/shp/station_schapi_alti_ref_2025_river.gpkg"

SAT_DB = HW_DB if SOURCE == "hwnext" else DAHITI_DB

DATE_MIN    = "2016-01-01"
DATE_MAX    = "2025-12-31"
DIST_MAX_KM = 10.0
WINDOW_DAYS = 7 if FREQ == "10j" else 14   # tolérance d'alignement temporel insitu
MIN_PAIRS   = 10

OUTPUT_DIR = Path(f"./data_processing/AnalyseModelsDtoD/nse_vs_rmse_{SOURCE}_{FREQ}")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

METRIC_DIRECTION = {"NSE": True, "KGE": True, "RMSE": False, "R2": True}

# ═══════════════════════════════════════════════════════════════
# CONSTRUCTION DYNAMIQUE DE MODEL_SOURCES (6 entrées : 3 masques x 2 loss)
# ═══════════════════════════════════════════════════════════════
MODEL_SOURCES = {}
for loss_type, loss_suffix in LOSS_SUFFIX_MAP.items():
    for mask in MASKS:
        label = f"DtoD{mask}_{loss_type}"
        csv_path = Path(
            f"./data/outlier_detection/benchmark_DtoD_{SOURCE}{FREQ}{loss_suffix}/"
            f"residuals_{SOURCE}_{FREQ}_{mask}pct.csv"
        )
        MODEL_SOURCES[label] = {"csv": csv_path, "loss_type": loss_type, "mask_pct": mask}

print(f"Combinaison : SOURCE={SOURCE}  FREQ={FREQ}")
print("MODEL_SOURCES généré :")
for label, info in MODEL_SOURCES.items():
    exist = "✓" if info["csv"].exists() else "✗ manquant"
    print(f"  {label:<14} {info['csv']}  [{exist}]")
print()

# ═══════════════════════════════════════════════════════════════
# HELPERS — métriques
# ═══════════════════════════════════════════════════════════════
def compute_metrics(obs, pred, kge_with_bias=True):
    """kge_with_bias=False -> KGE sans terme beta (référentiels différents, ex: vs insitu)."""
    n = len(obs)
    if n < MIN_PAIRS:
        return {"NSE": np.nan, "KGE": np.nan, "RMSE": np.nan, "R2": np.nan, "n": n}

    obs = np.asarray(obs, dtype=float)
    pred = np.asarray(pred, dtype=float)

    denom_nse = np.sum((obs - obs.mean()) ** 2)
    nse = 1 - np.sum((obs - pred) ** 2) / denom_nse if denom_nse > 0 else np.nan
    rmse = float(np.sqrt(np.mean((obs - pred) ** 2)))

    if obs.std() > 0 and pred.std() > 0:
        r = np.corrcoef(obs, pred)[0, 1]
        r2 = float(r ** 2)
    else:
        r, r2 = np.nan, np.nan

    if obs.std() > 0 and not np.isnan(r):
        alpha = pred.std() / obs.std()
        if kge_with_bias and obs.mean() != 0:
            beta = pred.mean() / obs.mean()
            kge = 1 - np.sqrt((r - 1) ** 2 + (alpha - 1) ** 2 + (beta - 1) ** 2)
        else:
            kge = 1 - np.sqrt((r - 1) ** 2 + (alpha - 1) ** 2)
    else:
        kge = np.nan

    return {
        "NSE": float(nse) if not np.isnan(nse) else np.nan,
        "KGE": float(kge) if not np.isnan(kge) else np.nan,
        "RMSE": rmse,
        "R2": r2,
        "n": n,
    }

def zscore(arr):
    arr = np.asarray(arr, dtype=float)
    m = ~np.isnan(arr)
    if m.sum() < 2:
        return arr * np.nan
    mu, sig = arr[m].mean(), arr[m].std()
    return (arr - mu) / sig if sig > 0 else arr * 0

# ═══════════════════════════════════════════════════════════════
# HELPERS — accès insitu / coords (insitu LE PLUS PROCHE)
# ═══════════════════════════════════════════════════════════════
print("Chargement shapefile insitu...")
gdf_insitu = gpd.read_file(INSITU_SHP).to_crs("EPSG:2154")
_cache_ins = {}
_cache_coords = {}

def get_coords(code):
    if code in _cache_coords:
        return _cache_coords[code]
    conn = sqlite3.connect(SAT_DB)
    for c in [str(code), str(code).zfill(13)]:
        df = pd.read_sql(
            "SELECT reference_longitude AS lon, reference_latitude AS lat FROM stations WHERE station_code = ?",
            conn, params=(c,)
        )
        if not df.empty:
            conn.close()
            _cache_coords[code] = (float(df.iloc[0]["lon"]), float(df.iloc[0]["lat"]))
            return _cache_coords[code]
    conn.close()
    _cache_coords[code] = (None, None)
    return None, None

def get_insitu_proche(lon, lat):
    pt   = gpd.GeoSeries([Point(lon, lat)], crs="EPSG:4326").to_crs("EPSG:2154")[0]
    dist = gdf_insitu.geometry.distance(pt)
    idx  = dist.idxmin()
    return gdf_insitu.loc[idx, "code_sta"], dist[idx] / 1000

def get_insitu_series(code_sta):
    if code_sta not in _cache_ins:
        conn = sqlite3.connect(INSITU_DB)
        df = pd.read_sql("""
            SELECT date, h_med_wsh AS wl FROM mesures_insitu
            WHERE code_sta = ? AND date >= ? AND date <= ?
            ORDER BY date
        """, conn, params=(code_sta, DATE_MIN, DATE_MAX))
        conn.close()
        df["date"] = pd.to_datetime(df["date"])
        df = df.dropna(subset=["wl"])
        _cache_ins[code_sta] = df if len(df) >= 5 else None
    return _cache_ins[code_sta]

def align_insitu(dates, df_ins, window_days):
    wl  = np.full(len(dates), np.nan)
    id_ = np.array(df_ins["date"].values, dtype="datetime64[D]")
    iv  = df_ins["wl"].values
    for i, d in enumerate(np.array(dates, dtype="datetime64[D]")):
        diff = np.abs((id_ - d).astype(float))
        idx  = int(np.argmin(diff))
        if diff[idx] <= window_days:
            wl[i] = iv[idx]
    return wl

# ═══════════════════════════════════════════════════════════════
# CHARGEMENT + CALCUL DES MÉTRIQUES (modèle vs alti, modèle vs insitu)
# ═══════════════════════════════════════════════════════════════
all_rows = []
baseline_rows = []
missing_models = []

for model_label, info in MODEL_SOURCES.items():
    csv_path = info["csv"]
    if not csv_path.exists():
        print(f"⚠ {model_label} : fichier introuvable ({csv_path}) -> ignoré")
        missing_models.append(model_label)
        continue

    df = pd.read_csv(csv_path)
    df["station"] = df["station"].astype(str)
    df = df.dropna(subset=["obs", "pred"])
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])

    for station, sub in df.groupby("station"):
        sub = sub.sort_values("date") if "date" in sub.columns else sub

        # ── Modèle vs Alti (même référentiel -> KGE complète) ──
        m_alti = compute_metrics(sub["obs"].values, sub["pred"].values, kge_with_bias=True)

        row = {
            "model": model_label, "loss_type": info["loss_type"], "mask_pct": info["mask_pct"],
            "station": station,
            "NSE_modele_alti": m_alti["NSE"], "KGE_modele_alti": m_alti["KGE"],
            "RMSE_modele_alti": m_alti["RMSE"], "R2_modele_alti": m_alti["R2"],
            "n_alti": m_alti["n"],
        }

        # ── Insitu le plus proche + alignement temporel ────────
        lon, lat = get_coords(station)
        if lon is not None and "date" in sub.columns:
            code_ins, dist_km = get_insitu_proche(lon, lat)
            row["insitu_code"] = code_ins
            row["dist_insitu_km"] = round(dist_km, 1)

            if dist_km <= DIST_MAX_KM:
                df_ins = get_insitu_series(code_ins)
                if df_ins is not None:
                    ins_wl = align_insitu(sub["date"].values, df_ins, WINDOW_DAYS)
                    n_pairs = int(np.sum(~np.isnan(ins_wl)))

                    if n_pairs >= MIN_PAIRS:
                        obs_z  = zscore(sub["obs"].values)
                        pred_z = zscore(sub["pred"].values)
                        ins_z  = zscore(ins_wl)

                        mask_common = ~(np.isnan(pred_z) | np.isnan(ins_z))
                        m_mod_ins = compute_metrics(ins_z[mask_common], pred_z[mask_common], kge_with_bias=False)
                        row.update({
                            "NSE_modele_insitu": m_mod_ins["NSE"], "KGE_modele_insitu": m_mod_ins["KGE"],
                            "RMSE_modele_insitu": m_mod_ins["RMSE"], "R2_modele_insitu": m_mod_ins["R2"],
                            "n_insitu": n_pairs,
                        })

                        mask_common2 = ~(np.isnan(obs_z) | np.isnan(ins_z))
                        m_alti_ins = compute_metrics(ins_z[mask_common2], obs_z[mask_common2], kge_with_bias=False)
                        baseline_rows.append({
                            "station": station, "insitu_code": code_ins, "dist_insitu_km": round(dist_km, 1),
                            "n_pairs": int(mask_common2.sum()),
                            "NSE_alti_insitu": m_alti_ins["NSE"], "KGE_alti_insitu": m_alti_ins["KGE"],
                            "RMSE_alti_insitu": m_alti_ins["RMSE"], "R2_alti_insitu": m_alti_ins["R2"],
                        })

        all_rows.append(row)

if not all_rows:
    raise SystemExit("Aucune métrique calculée — vérifier MODEL_SOURCES (chemins générés) et SOURCE/FREQ.")

df_metrics = pd.DataFrame(all_rows)
metrics_csv = OUTPUT_DIR / "metrics_per_station.csv"
df_metrics.to_csv(metrics_csv, index=False)
print(f"\nMétriques par station -> {metrics_csv}  ({len(df_metrics)} lignes)")

df_baseline = pd.DataFrame(baseline_rows).drop_duplicates(subset=["station"])
baseline_csv = OUTPUT_DIR / "baseline_alti_vs_insitu.csv"
df_baseline.to_csv(baseline_csv, index=False)
print(f"Baseline alti vs insitu -> {baseline_csv}  ({len(df_baseline)} stations)")
if len(df_baseline) > 0:
    print(f"  NSE médian alti vs insitu : {df_baseline['NSE_alti_insitu'].median():.3f}")

if missing_models:
    print(f"\n⚠ Modèles manquants : {missing_models}")

# ═══════════════════════════════════════════════════════════════
# HELPER — classement générique par rang moyen
# ═══════════════════════════════════════════════════════════════
def build_ranking(df_metrics, suffix, output_name):
    cols = {m: f"{m}{suffix}" for m in METRIC_DIRECTION}
    needed = list(cols.values())
    if not all(c in df_metrics.columns for c in needed):
        print(f"\n⚠ Colonnes manquantes pour le classement '{output_name}' -> ignoré")
        return None

    df_sub = df_metrics.dropna(subset=needed, how="all")
    models_present = df_sub["model"].unique().tolist()

    stations_par_modele = df_sub.groupby("model")["station"].apply(set)
    if len(stations_par_modele) < 2:
        print(f"\n⚠ Pas assez de modèles disponibles pour '{output_name}'")
        return None
    stations_communes = set.intersection(*stations_par_modele.tolist())

    df_common = df_sub[df_sub["station"].isin(stations_communes)]

    rank_rows = []
    for station in stations_communes:
        sub = df_common[df_common["station"] == station]
        for metric, higher_is_better in METRIC_DIRECTION.items():
            col = cols[metric]
            vals = sub.set_index("model")[col].dropna()
            if len(vals) < 2:
                continue
            ranks = vals.rank(ascending=not higher_is_better, method="average")
            for model, rank in ranks.items():
                rank_rows.append({"station": station, "metric": metric, "model": model, "rank": rank})

    df_ranks = pd.DataFrame(rank_rows)
    if df_ranks.empty:
        print(f"\n⚠ Pas assez de données pour '{output_name}'")
        return None

    classement = (df_ranks.groupby("model")["rank"].mean().sort_values()
                  .reset_index().rename(columns={"rank": "rang_moyen"}))
    classement["position"] = range(1, len(classement) + 1)

    rang_par_metrique = (df_ranks.groupby(["model", "metric"])["rank"]
                         .mean().unstack("metric").round(2))
    rang_par_metrique = rang_par_metrique.reindex(classement["model"])

    out_csv = OUTPUT_DIR / output_name
    classement.merge(rang_par_metrique, on="model").to_csv(out_csv, index=False)

    print(f"\n{'='*90}")
    print(f"  CLASSEMENT — {output_name}  [{SOURCE.upper()} {FREQ}]  "
          f"(sur {len(stations_communes)} stations communes, modèles : {models_present})")
    print(f"{'='*90}")
    for _, row in classement.iterrows():
        detail = rang_par_metrique.loc[row["model"]]
        detail_str = "  ".join(f"{m}={detail[m]:.2f}" for m in METRIC_DIRECTION if m in detail)
        print(f"  #{int(row['position'])}  {row['model']:<14} rang moyen = {row['rang_moyen']:.3f}   ({detail_str})")
    print(f"  => Meilleur modèle : {classement.iloc[0]['model']}")
    print(f"  -> {out_csv}")
    return classement

# ═══════════════════════════════════════════════════════════════
# CLASSEMENTS
# ═══════════════════════════════════════════════════════════════
build_ranking(df_metrics, "_modele_alti", "ranking_vs_alti.csv")
build_ranking(df_metrics, "_modele_insitu", "ranking_vs_insitu.csv")

print(f"\nNote : le classement 'vs_insitu' est le plus pertinent pour juger la qualité")
print(f"réelle des modèles (comparaison à une vérité terrain indépendante du satellite),")
print(f"tandis que 'vs_alti' mesure seulement la fidélité au signal satellite lui-même.")