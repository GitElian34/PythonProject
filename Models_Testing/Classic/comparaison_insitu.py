"""
compare_classic_vs_insitu.py
════════════════════════════════════════════════════════════════════════
Pour les modèles Classic (10j et 27j) :
  1. Recale les dates (décalage fixe par station, comme
     recale_dates_residuals_hwnext.py) -> corrige le décalage entre la
     date théorique interne de NeuralHydrology et la vraie date
     d'observation.
  2. Sélectionne l'insitu le plus proche par CONNECTIVITÉ SWORD (pas
     juste la distance euclidienne) -> évite de comparer deux points sur
     des cours d'eau différents.
  3. Calcule NSE, KGE, RMSE, R2 (médiane) pour Modèle vs Insitu ET
     Alti vs Insitu (baseline), + le gain médian du modèle par rapport
     à l'altimétrie brute.

Entrées :
  Models_Testing/Classic/residus/residuals_10j_hwnext.csv
  Models_Testing/Classic/residus/residuals_27j_hwnext.csv

Sorties :
  Models_Testing/Classic/residus/residuals_{freq}_hwnext_recale.csv
  Models_Testing/Classic/residus/metrics_{freq}_hwnext_sword_insitu.csv
  Models_Testing/Classic/residus/summary_classic_10j_27j.csv
════════════════════════════════════════════════════════════════════════
"""

import sqlite3
import sys
import numpy as np
import pandas as pd
import netCDF4 as ncdf
import geopandas as gpd
from pathlib import Path
from shapely.geometry import Point

MODULE_DIR = "./data_processing/Sword_and_Insitu"
sys.path.insert(0, MODULE_DIR)
from Sword_connectivity import load_sword_reaches, build_graph, check_connectivity

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
RESIDUALS_DIR = Path("./Models_Testing/Classic/residus")
DATE_DEB = "2016-01-01"   # doit matcher DATE_DEB de create_dataset_DtoD.py

RUNS = [
    {"freq": "10j", "window_days": 5,
     "nc_dir": "./data/IA/NeuralHydrology_hydroweb_next/10j/time_series"},
    {"freq": "27j", "window_days": 14,
     "nc_dir": "./data/IA/NeuralHydrology_hydroweb_next/27j/time_series"},
]

HW_DB = "./data/hydroweb_next.db"
INSITU_DB = "./data/insitu_data.db"
INSITU_SHP = "./data/insitu/shp/station_schapi_alti_ref_2025_river.gpkg"

DATE_MIN, DATE_MAX = "2016-01-01", "2025-12-31"
DIST_MAX_KM = 50.0
BUFFER_DEG = 0.35
MIN_PAIRS = 10


# ═══════════════════════════════════════════════════════════════
# ÉTAPE 1 — RECALAGE DES DATES (par station)
# ═══════════════════════════════════════════════════════════════
def recale_dates(csv_in: Path, nc_dir: str) -> pd.DataFrame:
    df_res = pd.read_csv(csv_in)
    df_res["date"] = pd.to_datetime(df_res["date"])
    df_res["station"] = df_res["station"].astype(str)

    stations = df_res["station"].unique()
    results = []
    n_ok, n_skip = 0, 0

    for code in stations:
        sub = df_res[df_res["station"] == code].sort_values("date").copy()

        sub_obs = sub.dropna(subset=["obs"])
        if len(sub_obs) == 0:
            n_skip += 1
            continue
        premiere_obs_csv = sub_obs["date"].iloc[0]

        nc_files = list(Path(nc_dir).glob(f"*{code}*.nc"))
        if not nc_files:
            n_skip += 1
            continue

        ds = ncdf.Dataset(nc_files[0])
        dates_nc = pd.to_datetime(DATE_DEB) + pd.to_timedelta(ds.variables["date"][:], unit="D")
        wl_nc = ds.variables["water_level"][:]
        ds.close()

        mask_nc = ~np.isnan(wl_nc)
        if mask_nc.sum() == 0:
            n_skip += 1
            continue

        premiere_nc = dates_nc[mask_nc][0]
        decalage_j = int((premiere_nc - premiere_obs_csv).days)

        sub["date_orig"] = sub["date"]
        sub["date_recalee"] = sub["date"] + pd.Timedelta(days=decalage_j)
        sub["decalage_j"] = decalage_j
        results.append(sub)
        n_ok += 1

    print(f"  Recalage : {n_ok} stations OK, {n_skip} skippées")
    if not results:
        return pd.DataFrame()

    df_out = pd.concat(results, ignore_index=True)
    cols = ["station", "date_orig", "date_recalee", "decalage_j",
            "obs", "pred", "residual", "residual_norm", "score", "is_outlier", "year"]
    cols_present = [c for c in cols if c in df_out.columns]
    df_out = df_out[cols_present]

    dec = df_out.drop_duplicates("station")["decalage_j"]
    print(f"  Décalages : médiane={dec.median():.0f}j, min={dec.min()}j, max={dec.max()}j")
    return df_out


# ═══════════════════════════════════════════════════════════════
# MÉTRIQUES
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
# INSITU : positions + séries + sélection par connectivité SWORD
# ═══════════════════════════════════════════════════════════════
print("Chargement shapefile insitu...")
gdf_insitu = gpd.read_file(INSITU_SHP).to_crs("EPSG:4326")
gdf_insitu_proj = gdf_insitu.to_crs("EPSG:2154")

_cache_coords, _cache_ins_series = {}, {}


def get_coords_sat(db_path, code):
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
# BOUCLE PRINCIPALE
# ═══════════════════════════════════════════════════════════════
summaries = []

for run in RUNS:
    freq = run["freq"]
    window_days = run["window_days"]
    csv_in = RESIDUALS_DIR / f"residuals_{freq}_hwnext.csv"
    csv_recale = RESIDUALS_DIR / f"residuals_{freq}_hwnext_recale.csv"

    print(f"\n{'=' * 65}\n  CLASSIC {freq.upper()}\n{'=' * 65}")

    if not csv_in.exists():
        print(f"⚠ Fichier introuvable : {csv_in} -> ignoré")
        continue

    print("Étape 1 — Recalage des dates...")
    df = recale_dates(csv_in, run["nc_dir"])
    if df.empty:
        print("⚠ Recalage vide -> ignoré")
        continue
    df.to_csv(csv_recale, index=False)
    print(f"  -> {csv_recale}")

    df["date_recalee"] = pd.to_datetime(df["date_recalee"])
    df = df.dropna(subset=["obs", "pred"])
    print(f"\nÉtape 2 — Matching insitu + métriques ({df['station'].nunique()} stations)...")

    results = []
    skip_reasons = {"too_few_pairs_raw": 0, "no_coords": 0, "no_insitu_candidate": 0,
                     "no_insitu_series": 0, "too_few_pairs_aligned": 0}

    for code in df["station"].unique():
        sub = df[df["station"] == code].sort_values("date_recalee").reset_index(drop=True)
        if len(sub) < MIN_PAIRS:
            skip_reasons["too_few_pairs_raw"] += 1
            continue

        lon, lat = get_coords_sat(HW_DB, code)
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

        ins_wl = align_insitu(sub["date_recalee"].values, df_ins, window_days)
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
    out_path = RESIDUALS_DIR / f"metrics_{freq}_hwnext_sword_insitu.csv"
    df_out.to_csv(out_path, index=False)

    n_total = df["station"].nunique()
    print(f"\n  Stations avec métriques : {len(df_out)}/{n_total}")
    for reason, n in skip_reasons.items():
        if n > 0:
            print(f"    {reason:<25} : {n}")

    summary_row = {"freq": freq, "n_total": n_total, "n_ok": len(df_out)}
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

    summaries.append(summary_row)
    print(f"  CSV -> {out_path}")

# ═══════════════════════════════════════════════════════════════
# RÉCAPITULATIF FINAL
# ═══════════════════════════════════════════════════════════════
df_summary = pd.DataFrame(summaries)

print(f"\n\n{'#' * 70}\n#  RÉCAPITULATIF — Modèle vs Insitu\n{'#' * 70}")
print(f"{'':10} {'stations':>10} {'NSE':>8} {'KGE':>8} {'RMSE':>8} {'R2':>8}")
for _, row in df_summary.iterrows():
    print(f"{row['freq']:<10} {row['n_ok']:>5}/{row['n_total']:<4} "
          f"{row.get('NSE_median', np.nan):>8.3f} {row.get('KGE_median', np.nan):>8.3f} "
          f"{row.get('RMSE_median', np.nan):>8.3f} {row.get('R2_median', np.nan):>8.3f}")

print(f"\n--- Alti vs Insitu (baseline) ---")
print(f"{'':10} {'NSE':>8} {'KGE':>8} {'RMSE':>8} {'R2':>8}")
for _, row in df_summary.iterrows():
    print(f"{row['freq']:<10} "
          f"{row.get('NSE_alti_insitu_median', np.nan):>8.3f} {row.get('KGE_alti_insitu_median', np.nan):>8.3f} "
          f"{row.get('RMSE_alti_insitu_median', np.nan):>8.3f} {row.get('R2_alti_insitu_median', np.nan):>8.3f}")

print(f"\n--- Gain médian Modèle - Alti ---")
print(f"{'':10} {'NSE':>8} {'KGE':>8} {'RMSE':>8} {'R2':>8}   {'% modèle > alti':>16}")
for _, row in df_summary.iterrows():
    print(f"{row['freq']:<10} "
          f"{row.get('gain_NSE_median', np.nan):>8.3f} {row.get('gain_KGE_median', np.nan):>8.3f} "
          f"{row.get('gain_RMSE_median', np.nan):>8.3f} {row.get('gain_R2_median', np.nan):>8.3f}   "
          f"{row.get('pct_modele_meilleur_NSE', np.nan):>15.1f}%")

df_summary.to_csv(RESIDUALS_DIR / "summary_classic_10j_27j.csv", index=False)
print(f"\n✅ Terminé.")