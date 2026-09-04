"""
compare_other_models_vs_insitu.py
════════════════════════════════════════════════════════════════════════
Même logique que compare_classic_vs_insitu.py, adaptée aux modèles
DtoD (80/90/96) et Quantile (80/90/96), pour 10j ET 27j.

Supporte deux sources de données interchangeables via la variable
SOURCE ci-dessous, comme eval_dtod_quantile.py : "hwnext" ou "dahiti".
Les fichiers d'entrée/sortie sont désormais nommés {label}_{SOURCE}_{freq}
partout (au lieu de {label}_hwnext_{freq} en dur) -> aucune collision
possible si vous traitez les deux sources l'une après l'autre.

⚠️ POINT À VÉRIFIER/COMPLÉTER : COORDS_DB["dahiti"] ci-dessous. Les
coordonnées des stations HW Next viennent de hydroweb_next.db (table
"stations", colonnes station_code/reference_longitude/reference_latitude).
Pour DAHITI, je ne connais ni le chemin de la base équivalente, ni si le
schéma de table est identique -- get_coords_sat() suppose le même schéma
par défaut, à corriger si besoin (adapter la fonction si DAHITI stocke
ses coordonnées autrement, par exemple dans le dossier attributes/ du
dataset NeuralHydrology plutôt que dans une base SQLite séparée).

Différences vs Classic :
  - Les résidus sont déjà réduits à 1 valeur/date par
    eval_dtod_quantile.py (nowcast [:, -1], + q50 pour Quantile) ->
    pas de retraitement de dimension ici, juste recalage + matching insitu.
  - Un seul NC_DIR pour toutes les fréquences (dataset journalier
    commun à DtoD et Quantile, seule la LISTE de stations diffère selon
    le gap médian 10j/27j).

Entrées (produites par eval_dtod_quantile.py) :
  Models_Testing/DtoD/residus/residuals_{label}_{SOURCE}_{freq}.csv
  Models_Testing/Quantille/residus/residuals_{label}_{SOURCE}_{freq}.csv

Sorties (mêmes dossiers) :
  residuals_{label}_{SOURCE}_{freq}_recale.csv
  metrics_{label}_{SOURCE}_{freq}_sword_insitu.csv
  + résumé séparé 10j / 27j : summary_other_models_{SOURCE}_10j.csv, _27j.csv
════════════════════════════════════════════════════════════════════════
"""

import sqlite3
import sys
import numpy as np
import pandas as pd
import geopandas as gpd
from pathlib import Path
from shapely.geometry import Point

MODULE_DIR = "./data_processing/Sword_and_Insitu"
sys.path.insert(0, MODULE_DIR)
from Sword_connectivity import load_sword_reaches, build_graph, check_connectivity

# ═══════════════════════════════════════════════════════════════
# SOURCE DE DONNÉES — "hwnext" ou "dahiti"
# ═══════════════════════════════════════════════════════════════
SOURCE = "dahiti"   # <-- changer ici pour basculer hwnext <-> dahiti

# Base de coordonnées des stations satellite, par source.
COORDS_DB = {
    "hwnext": "./data/hydroweb_next.db",
    "dahiti": "./data/dahiti.db",  # ⚠️ chemin/schéma à vérifier -- cf. docstring
}

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
DATE_DEB = "2016-01-01"   # doit matcher DATE_DEB de create_dataset_DtoD.py

MODELS = [
    # {"label": "DtoD80", "subdir": "DtoD"},
    # {"label": "DtoD90", "subdir": "DtoD"},
    # {"label": "DtoD96", "subdir": "DtoD"},
    # {"label": "Quantile80", "subdir": "Quantille"},
    # {"label": "Quantile90", "subdir": "Quantille"},
    {"label": "Quantile96", "subdir": "Quantille"},
]

FREQS = [
    {"freq": "10j", "window_days": 5},
    {"freq": "27j", "window_days": 14},
]

# Dataset journalier commun à tous les modèles DtoD/Quantile (freq-agnostique,
# seule la liste de stations utilisée change selon 10j/27j)
# NB : variable non utilisée directement dans ce script (héritée de la
# version précédente) -- laissée pour référence/documentation.
NC_DIR = "./data/IA/NeuralHydrologyHWNextDtoD/time_series"

INSITU_DB = "./data/insitu_data.db"
INSITU_SHP = "./data/insitu/shp/station_schapi_alti_ref_2025_river.gpkg"

DATE_MIN, DATE_MAX = "2016-01-01", "2025-12-31"
DIST_MAX_KM = 50.0
BUFFER_DEG = 0.35
MIN_PAIRS = 10


# ═══════════════════════════════════════════════════════════════
# ⚠️ PAS DE RECALAGE DE DATES ICI
# Contrairement aux modèles Classic (use_frequencies: [27D]/[10D],
# grille théorique périodique pouvant glisser par rapport aux vraies
# dates), les modèles DtoD/Quantile utilisent use_frequencies: [1D]
# -> chaque pas de la grille EST un jour calendaire réel. Vérifié
# empiriquement : décalage = 0 sur un échantillon de 30 stations (HW Next
# -- à revalider sur un échantillon Dahiti si le dataset diffère).
# ═══════════════════════════════════════════════════════════════


# ═══════════════════════════════════════════════════════════════
# MÉTRIQUES (identique à compare_classic_vs_insitu.py)
# ═══════════════════════════════════════════════════════════════
def compute_metrics(obs, pred, kge_with_bias):
    n = len(obs)
    if n < MIN_PAIRS:
        return {"NSE": np.nan, "KGE": np.nan, "RMSE": np.nan, "R2": np.nan, "n": n}
    obs, pred = np.asarray(obs, dtype=float), np.asarray(pred, dtype=float)
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
    return {"NSE": float(nse) if not np.isnan(nse) else np.nan,
            "KGE": float(kge) if not np.isnan(kge) else np.nan,
            "RMSE": rmse, "R2": r2, "n": n}


def zscore(arr):
    arr = np.asarray(arr, dtype=float)
    m = ~np.isnan(arr)
    if m.sum() < 2:
        return arr * np.nan
    mu, sig = arr[m].mean(), arr[m].std()
    return (arr - mu) / sig if sig > 0 else arr * 0


# ═══════════════════════════════════════════════════════════════
# INSITU (identique à compare_classic_vs_insitu.py -- indépendant de SOURCE,
# le référentiel in situ national ne change pas selon le produit altimétrique)
# ═══════════════════════════════════════════════════════════════
print("Chargement shapefile insitu...")
gdf_insitu = gpd.read_file(INSITU_SHP).to_crs("EPSG:4326")
gdf_insitu_proj = gdf_insitu.to_crs("EPSG:2154")

_cache_coords, _cache_ins_series = {}, {}


def get_coords_sat(db_path, code):
    """
    ⚠️ Suppose le même schéma de table que hydroweb_next.db
    (table "stations", colonnes station_code/reference_longitude/
    reference_latitude). À adapter si la base DAHITI a un schéma différent.
    """
    key = (db_path, code)
    if key in _cache_coords:
        return _cache_coords[key]
    conn = sqlite3.connect(db_path)
    for c in [str(code), str(code).zfill(13)]:
        df = pd.read_sql(
            "SELECT reference_longitude AS lon, reference_latitude AS lat "
            "FROM stations WHERE station_code = ?", conn, params=(c,))
        if not df.empty:
            conn.close()
            _cache_coords[key] = (float(df.iloc[0]["lon"]), float(df.iloc[0]["lat"]))
            return _cache_coords[key]
    conn.close()
    _cache_coords[key] = (None, None)
    return None, None


def get_insitu_candidats(lon, lat, dist_max_km):
    pt = gpd.GeoSeries([Point(lon, lat)], crs="EPSG:4326").to_crs("EPSG:2154")[0]
    dist = gdf_insitu_proj.geometry.distance(pt) / 1000
    candidats = dist[dist <= dist_max_km].sort_values()
    return [(gdf_insitu_proj.loc[idx, "code_sta"], d,
              gdf_insitu.loc[idx, "geometry"].x, gdf_insitu.loc[idx, "geometry"].y)
            for idx, d in candidats.items()]


def select_insitu_sword(lon_a, lat_a):
    candidats = get_insitu_candidats(lon_a, lat_a, DIST_MAX_KM)
    if not candidats:
        return None
    try:
        local_bbox = (lon_a - BUFFER_DEG, lat_a - BUFFER_DEG, lon_a + BUFFER_DEG, lat_a + BUFFER_DEG)
        gdf_sword, gdf_sword_proj = load_sword_reaches(bbox=local_bbox)
        G, info = build_graph(gdf_sword)
        for code_ins, dist_km, lon_b, lat_b in candidats:
            res = check_connectivity(lon_a, lat_a, lon_b, lat_b, G, info, gdf_sword_proj)
            if res["connected"] and not res["has_confluence"]:
                return {"code_sta": code_ins, "dist_km": dist_km, "connectivity_validated": True}
    except Exception as e:
        print(f"    (SWORD indisponible/erreur : {e} -> fallback plus proche)")
    code_ins, dist_km, _, _ = candidats[0]
    return {"code_sta": code_ins, "dist_km": dist_km, "connectivity_validated": False}


def get_insitu_series(code_sta):
    if code_sta not in _cache_ins_series:
        conn = sqlite3.connect(INSITU_DB)
        df = pd.read_sql("""
            SELECT date, h_med_wsh AS wl FROM mesures_insitu
            WHERE code_sta = ? AND date >= ? AND date <= ? ORDER BY date
        """, conn, params=(code_sta, DATE_MIN, DATE_MAX))
        conn.close()
        df["date"] = pd.to_datetime(df["date"])
        df = df.dropna(subset=["wl"])
        _cache_ins_series[code_sta] = df if len(df) >= 5 else None
    return _cache_ins_series[code_sta]


def align_insitu(dates, df_ins, window_days):
    wl = np.full(len(dates), np.nan)
    id_ = np.array(df_ins["date"].values, dtype="datetime64[D]")
    iv = df_ins["wl"].values
    for i, d in enumerate(np.array(dates, dtype="datetime64[D]")):
        diff = np.abs((id_ - d).astype(float))
        idx = int(np.argmin(diff))
        if diff[idx] <= window_days:
            wl[i] = iv[idx]
    return wl


# ═══════════════════════════════════════════════════════════════
# ÉVALUATION D'UN (modèle, fréquence)
# ═══════════════════════════════════════════════════════════════
def process_one(model_cfg: dict, freq_cfg: dict) -> dict:
    label = model_cfg["label"]
    subdir = model_cfg["subdir"]
    freq = freq_cfg["freq"]
    window_days = freq_cfg["window_days"]

    residus_dir = Path(f"./Models_Testing/{subdir}/residus")
    csv_in = residus_dir / f"residuals_{label}_{SOURCE}_{freq}.csv"

    print(f"\n{'=' * 65}\n  {label} [{freq}]  source={SOURCE}\n{'=' * 65}")

    if not csv_in.exists():
        print(f"⚠ Fichier introuvable : {csv_in} -> ignoré")
        return None

    print("Chargement des résidus...")
    df = pd.read_csv(csv_in)
    df["date"] = pd.to_datetime(df["date"])
    df["station"] = df["station"].astype(str)

    # Filtre clé : ne garder que les dates où il y a une VRAIE observation
    # altimétrique (obs non-NaN) ET une prédiction (pred non-NaN). C'est ce
    # filtre qui garantit qu'on ne compare jamais sur des jours "inventés"
    # par le modèle sans donnée alti en face.
    df = df.dropna(subset=["obs", "pred"])
    print(f"  {df['station'].nunique()} stations, {len(df)} lignes "
          f"(obs et pred non-NaN uniquement)")

    print(f"\nMatching insitu + métriques...")

    results = []
    skip_reasons = {"too_few_pairs_raw": 0, "no_coords": 0, "no_insitu_candidate": 0,
                     "no_insitu_series": 0, "too_few_pairs_aligned": 0}

    for code in df["station"].unique():
        sub = df[df["station"] == code].sort_values("date").reset_index(drop=True)
        if len(sub) < MIN_PAIRS:
            skip_reasons["too_few_pairs_raw"] += 1
            continue

        lon, lat = get_coords_sat(COORDS_DB[SOURCE], code)
        if lon is None:
            skip_reasons["no_coords"] += 1
            continue

        sel = select_insitu_sword(lon, lat)
        if sel is None:
            skip_reasons["no_insitu_candidate"] += 1
            continue

        df_ins = get_insitu_series(sel["code_sta"])
        if df_ins is None:
            skip_reasons["no_insitu_series"] += 1
            continue

        ins_wl = align_insitu(sub["date"].values, df_ins, window_days)
        n_pairs = int(np.sum(~np.isnan(ins_wl)))
        if n_pairs < MIN_PAIRS:
            skip_reasons["too_few_pairs_aligned"] += 1
            continue

        obs_z, pred_z, ins_z = zscore(sub["obs"].values), zscore(sub["pred"].values), zscore(ins_wl)

        mask_mod = ~(np.isnan(pred_z) | np.isnan(ins_z))
        m_mod_ins = compute_metrics(ins_z[mask_mod], pred_z[mask_mod], kge_with_bias=False)

        mask_alti = ~(np.isnan(obs_z) | np.isnan(ins_z))
        m_alti_ins = compute_metrics(ins_z[mask_alti], obs_z[mask_alti], kge_with_bias=False)

        results.append({
            "station": code, "insitu_code": sel["code_sta"],
            "dist_insitu_km": round(sel["dist_km"], 1),
            "connectivity_validated": sel["connectivity_validated"],
            "n_pairs": n_pairs,
            "NSE": m_mod_ins["NSE"], "KGE": m_mod_ins["KGE"],
            "RMSE": m_mod_ins["RMSE"], "R2": m_mod_ins["R2"],
            "NSE_alti_insitu": m_alti_ins["NSE"], "KGE_alti_insitu": m_alti_ins["KGE"],
            "RMSE_alti_insitu": m_alti_ins["RMSE"], "R2_alti_insitu": m_alti_ins["R2"],
        })

    df_out = pd.DataFrame(results)
    out_path = residus_dir / f"metrics_{label}_{SOURCE}_{freq}_sword_insitu.csv"
    df_out.to_csv(out_path, index=False)

    n_total = df["station"].nunique()
    print(f"\n  Stations avec métriques : {len(df_out)}/{n_total}")
    for reason, n in skip_reasons.items():
        if n > 0:
            print(f"    {reason:<25} : {n}")

    summary_row = {"model": label, "freq": freq, "n_total": n_total, "n_ok": len(df_out)}
    if len(df_out):
        n_valid = df_out["connectivity_validated"].sum()
        summary_row["connectivity_validated_pct"] = round(100 * n_valid / len(df_out), 1)
        for m in ["NSE", "KGE", "RMSE", "R2"]:
            v = df_out[m].dropna()
            summary_row[f"{m}_median"] = round(v.median(), 3) if len(v) else np.nan
            va = df_out[f"{m}_alti_insitu"].dropna()
            summary_row[f"{m}_alti_insitu_median"] = round(va.median(), 3) if len(va) else np.nan
        for m, higher_is_better in [("NSE", True), ("KGE", True), ("RMSE", False), ("R2", True)]:
            merged = df_out[[m, f"{m}_alti_insitu"]].dropna()
            if len(merged):
                gain = (merged[m] - merged[f"{m}_alti_insitu"]) if higher_is_better \
                    else (merged[f"{m}_alti_insitu"] - merged[m])
                summary_row[f"gain_{m}_median"] = round(gain.median(), 3)
                summary_row[f"pct_modele_meilleur_{m}"] = round((gain > 0).mean() * 100, 1)

    print(f"  CSV -> {out_path}")
    return summary_row


# ═══════════════════════════════════════════════════════════════
# BOUCLE PRINCIPALE
# ═══════════════════════════════════════════════════════════════
all_summaries = []
for model_cfg in MODELS:
    for freq_cfg in FREQS:
        row = process_one(model_cfg, freq_cfg)
        if row:
            all_summaries.append(row)

df_all = pd.DataFrame(all_summaries)

# ═══════════════════════════════════════════════════════════════
# RÉCAPITULATIFS — SÉPARÉS 10j / 27j
# ═══════════════════════════════════════════════════════════════
for freq in ["10j", "27j"]:
    df_freq = df_all[df_all["freq"] == freq]
    if df_freq.empty:
        continue

    print(f"\n\n{'#' * 75}\n#  RÉCAPITULATIF — {SOURCE.upper()} {freq.upper()} (tous modèles DtoD/Quantile)\n{'#' * 75}")

    print(f"\n--- Modèle vs Insitu ---")
    print(f"{'modèle':<15} {'stations':>10} {'NSE':>8} {'KGE':>8} {'RMSE':>8} {'R2':>8}")
    for _, row in df_freq.iterrows():
        print(f"{row['model']:<15} {row['n_ok']:>5}/{row['n_total']:<4} "
              f"{row.get('NSE_median', np.nan):>8.3f} {row.get('KGE_median', np.nan):>8.3f} "
              f"{row.get('RMSE_median', np.nan):>8.3f} {row.get('R2_median', np.nan):>8.3f}")

    print(f"\n--- Alti vs Insitu (baseline) ---")
    print(f"{'modèle':<15} {'NSE':>8} {'KGE':>8} {'RMSE':>8} {'R2':>8}")
    for _, row in df_freq.iterrows():
        print(f"{row['model']:<15} "
              f"{row.get('NSE_alti_insitu_median', np.nan):>8.3f} {row.get('KGE_alti_insitu_median', np.nan):>8.3f} "
              f"{row.get('RMSE_alti_insitu_median', np.nan):>8.3f} {row.get('R2_alti_insitu_median', np.nan):>8.3f}")

    print(f"\n--- Gain médian Modèle - Alti ---")
    print(f"{'modèle':<15} {'NSE':>8} {'KGE':>8} {'RMSE':>8} {'R2':>8}   {'% modèle > alti':>16}")
    for _, row in df_freq.iterrows():
        print(f"{row['model']:<15} "
              f"{row.get('gain_NSE_median', np.nan):>8.3f} {row.get('gain_KGE_median', np.nan):>8.3f} "
              f"{row.get('gain_RMSE_median', np.nan):>8.3f} {row.get('gain_R2_median', np.nan):>8.3f}   "
              f"{row.get('pct_modele_meilleur_NSE', np.nan):>15.1f}%")

    out_summary = Path(f"./Models_Testing/summary_other_models_{SOURCE}_{freq}.csv")
    df_freq.to_csv(out_summary, index=False)
    print(f"\nRésumé {freq} -> {out_summary}")

print(f"\n✅ Terminé.")