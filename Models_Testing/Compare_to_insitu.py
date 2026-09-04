"""
eval_metrics_hwnext_sword_insitu.py
════════════════════════════════════════════════════════════════════════
Évaluation Modèle vs Insitu, pour les modèles 10j ET 27j HW Next, en
utilisant la méthode de sélection de l'insitu par CONNECTIVITÉ SWORD
(comme carte_verification_sword.py), pas juste la distance euclidienne :
  - Parmi les candidats insitu triés par distance (<= DIST_MAX_KM),
    on prend le premier CONNECTÉ au réseau SWORD ET SANS confluence
    sur le chemin.
  - Si aucun candidat ne satisfait les deux conditions, fallback sur
    le plus proche "brut" (colonne connectivity_validated=False pour
    le signaler clairement, pas de perte silencieuse de stations).

Entrées :
  - ./Models_Testing/Residus/residuals_10j_hwnext.csv
  - ./Models_Testing/Residus/residuals_27j_hwnext.csv
  - ./data/hydroweb_next.db      (coords des stations alti)
  - ./data/insitu_data.db        (séries insitu, table mesures_insitu)
  - ./data/insitu/shp/station_schapi_alti_ref_2025_river.gpkg (positions insitu)
  - Sword_connectivity.py (load_sword_reaches, build_graph, check_connectivity)

Sorties (par fréquence) :
  ./Models_Testing/Residus/metrics_10j_hwnext_sword_insitu.csv
  ./Models_Testing/Residus/metrics_27j_hwnext_sword_insitu.csv

Métriques calculées (modèle vs insitu, sur données z-scorées par station,
KGE SANS terme beta -> évite l'explosion sur mean~0) :
  NSE, KGE, RMSE, R2
Colonnes additionnelles fournies en bonus pour contexte (alti vs insitu,
baseline indépendante du modèle) : NSE/KGE/RMSE/R2_alti_insitu.
════════════════════════════════════════════════════════════════════════
"""

import sqlite3
import sys
import numpy as np
import pandas as pd
import geopandas as gpd
from pathlib import Path
from shapely.geometry import Point

# ⚠ Adapter si besoin (mêmes réglages que carte_verification_sword.py)
MODULE_DIR = "./data_processing/Sword_and_Insitu"
sys.path.insert(0, MODULE_DIR)
from Sword_connectivity import load_sword_reaches, build_graph, check_connectivity

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
RESIDUALS_DIR = Path("./Models_Testing/Residus")

RUNS = [
    {"freq": "10j", "residuals_csv": RESIDUALS_DIR / "residuals_10j_hwnext_recale.csv", "window_days": 5},
    {"freq": "27j", "residuals_csv": RESIDUALS_DIR / "residuals_27j_hwnext_recale.csv", "window_days": 14},
]

HW_DB      = "./data/hydroweb_next.db"
INSITU_DB  = "./data/insitu_data.db"
INSITU_SHP = "./data/insitu/shp/station_schapi_alti_ref_2025_river.gpkg"

DATE_MIN, DATE_MAX = "2016-01-01", "2025-12-31"
DIST_MAX_KM   = 50.0     # rayon de recherche des candidats insitu
BUFFER_DEG    = 0.35     # marge locale pour charger SWORD autour de chaque station (~35 km)
MIN_PAIRS     = 10

# ═══════════════════════════════════════════════════════════════
# MÉTRIQUES
# ═══════════════════════════════════════════════════════════════
def compute_metrics(obs, pred, kge_with_bias):
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
            "FROM stations WHERE station_code = ?",
            conn, params=(c,))
        if not df.empty:
            conn.close()
            _cache_coords[key] = (float(df.iloc[0]["lon"]), float(df.iloc[0]["lat"]))
            return _cache_coords[key]
    conn.close()
    _cache_coords[key] = (None, None)
    return None, None


def get_insitu_candidats(lon, lat, dist_max_km):
    """Candidats insitu triés par distance croissante (<= dist_max_km)."""
    pt = gpd.GeoSeries([Point(lon, lat)], crs="EPSG:4326").to_crs("EPSG:2154")[0]
    dist = gdf_insitu_proj.geometry.distance(pt) / 1000
    candidats = dist[dist <= dist_max_km].sort_values()
    return [(gdf_insitu_proj.loc[idx, "code_sta"], d,
              gdf_insitu.loc[idx, "geometry"].x, gdf_insitu.loc[idx, "geometry"].y)
            for idx, d in candidats.items()]


def select_insitu_sword(lon_a, lat_a):
    """
    Sélectionne l'insitu par connectivité SWORD (premier candidat connecté
    ET sans confluence sur le chemin). Fallback sur le plus proche brut si
    aucun candidat ne satisfait les deux conditions.

    Returns:
        dict avec code_sta, dist_km, connectivity_validated (bool)
        ou None si aucun candidat dans le rayon.
    """
    candidats = get_insitu_candidats(lon_a, lat_a, DIST_MAX_KM)
    if not candidats:
        return None

    try:
        local_bbox = (lon_a - BUFFER_DEG, lat_a - BUFFER_DEG,
                      lon_a + BUFFER_DEG, lat_a + BUFFER_DEG)
        gdf_sword, gdf_sword_proj = load_sword_reaches(bbox=local_bbox)
        G, info = build_graph(gdf_sword)

        for code_ins, dist_km, lon_b, lat_b in candidats:
            res = check_connectivity(lon_a, lat_a, lon_b, lat_b, G, info, gdf_sword_proj)
            if res["connected"] and not res["has_confluence"]:
                return {"code_sta": code_ins, "dist_km": dist_km,
                        "connectivity_validated": True}
    except Exception as e:
        print(f"    (SWORD indisponible/erreur pour ce point : {e} -> fallback plus proche)")

    # Fallback : plus proche brut, non validé par connectivité
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
# BOUCLE PRINCIPALE — 10j PUIS 27j
# ═══════════════════════════════════════════════════════════════
summaries = []

for run in RUNS:
    freq = run["freq"]
    window_days = run["window_days"]
    csv_path = run["residuals_csv"]

    print(f"\n{'=' * 65}")
    print(f"  ÉVALUATION MODÈLE {freq.upper()} vs INSITU (connectivité SWORD)")
    print(f"{'=' * 65}")

    if not csv_path.exists():
        print(f"⚠ Fichier introuvable : {csv_path} -> ignoré")
        continue

    df = pd.read_csv(csv_path)
    df["station"] = df["station"].astype(str)
    df["date_recalee"] = pd.to_datetime(df["date_recalee"])
    df = df.dropna(subset=["obs", "pred"])
    print(f"  Lignes après dropna : {len(df)} | Stations : {df['station'].nunique()}")

    results = []
    skip_reasons = {"too_few_pairs_raw": 0, "no_coords": 0, "no_insitu_candidate": 0,
                     "no_insitu_series": 0, "too_few_pairs_aligned": 0}

    for i, code in enumerate(df["station"].unique()):
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

        obs_z, pred_z, ins_z = (zscore(sub["obs"].values),
                                 zscore(sub["pred"].values), zscore(ins_wl))

        mask_mod = ~(np.isnan(pred_z) | np.isnan(ins_z))
        m_mod_ins = compute_metrics(ins_z[mask_mod], pred_z[mask_mod], kge_with_bias=False)

        mask_alti = ~(np.isnan(obs_z) | np.isnan(ins_z))
        m_alti_ins = compute_metrics(ins_z[mask_alti], obs_z[mask_alti], kge_with_bias=False)

        results.append({
            "station": code,
            "insitu_code": sel["code_sta"],
            "dist_insitu_km": round(sel["dist_km"], 1),
            "connectivity_validated": sel["connectivity_validated"],
            "n_pairs": n_pairs,
            "NSE": m_mod_ins["NSE"], "KGE": m_mod_ins["KGE"],
            "RMSE": m_mod_ins["RMSE"], "R2": m_mod_ins["R2"],
            "NSE_alti_insitu": m_alti_ins["NSE"], "KGE_alti_insitu": m_alti_ins["KGE"],
            "RMSE_alti_insitu": m_alti_ins["RMSE"], "R2_alti_insitu": m_alti_ins["R2"],
        })

        if (i + 1) % 20 == 0:
            print(f"  {i+1}/{df['station'].nunique()} stations traitées...")

    df_out = pd.DataFrame(results)
    out_path = RESIDUALS_DIR / f"metrics_{freq}_hwnext_sword_insitu.csv"
    df_out.to_csv(out_path, index=False)

    print(f"\n  Stations avec métriques : {len(df_out)}")
    n_total_stations = df['station'].nunique()
    n_lost = n_total_stations - len(df_out)
    print(f"  Stations perdues en route : {n_lost}/{n_total_stations}")
    print(f"  Détail des raisons de rejet :")
    for reason, n in skip_reasons.items():
        if n > 0:
            print(f"    {reason:<25} : {n}")

    summary_row = {"freq": freq, "n_total": n_total_stations, "n_ok": len(df_out), "n_lost": n_lost}
    summary_row.update(skip_reasons)

    if len(df_out):
        n_valid = df_out["connectivity_validated"].sum()
        print(f"  Sélection validée par connectivité SWORD : {n_valid}/{len(df_out)} "
              f"({100*n_valid/len(df_out):.0f}%)")
        summary_row["connectivity_validated_pct"] = round(100 * n_valid / len(df_out), 1)
        print(f"\n  {'métrique':<8} {'médiane':>10} {'moyenne':>10}")
        for m in ["NSE", "KGE", "RMSE", "R2"]:
            v = df_out[m].dropna()
            print(f"  {m:<8} {v.median():>10.3f} {v.mean():>10.3f}")
            summary_row[f"{m}_median"] = round(v.median(), 3) if len(v) else np.nan
            summary_row[f"{m}_mean"] = round(v.mean(), 3) if len(v) else np.nan
    print(f"  CSV -> {out_path}")

    summaries.append(summary_row)

# ═══════════════════════════════════════════════════════════════
# RÉCAPITULATIF FINAL — 10j ET 27j CÔTE À CÔTE
# ═══════════════════════════════════════════════════════════════
print(f"\n\n{'#' * 70}")
print(f"#  RÉCAPITULATIF FINAL — 10j vs 27j")
print(f"{'#' * 70}")

df_summary = pd.DataFrame(summaries)
print(f"\n{'':10} {'stations':>10} {'connect.%':>10} {'NSE':>8} {'KGE':>8} {'RMSE':>8} {'R2':>8}")
print("-" * 70)
for _, row in df_summary.iterrows():
    print(f"{row['freq']:<10} {row['n_ok']:>5}/{row['n_total']:<4} "
          f"{row.get('connectivity_validated_pct', float('nan')):>9.0f}% "
          f"{row.get('NSE_median', float('nan')):>8.3f} "
          f"{row.get('KGE_median', float('nan')):>8.3f} "
          f"{row.get('RMSE_median', float('nan')):>8.3f} "
          f"{row.get('R2_median', float('nan')):>8.3f}")

summary_path = RESIDUALS_DIR / "summary_10j_27j_hwnext.csv"
df_summary.to_csv(summary_path, index=False)
print(f"\nRécapitulatif -> {summary_path}")

print("\n✅ Terminé.")