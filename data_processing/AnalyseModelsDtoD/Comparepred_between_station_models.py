"""
analyse_ecarts_nse_per_station_insitu.py
════════════════════════════════════════════════════════════════════════
Même analyse que analyse_ecarts_nse_per_station.py (écarts, regret, rang),
mais calculée sur le NSE modèle vs INSITU (et non vs obs satellite comme
dans results_per_station_{mask}pct.csv qui vient nativement de
NeuralHydrology).

Source : les CSV résidus (station, date, obs, pred), où "obs" est
l'observation satellite. On récupère les coordonnées satellite pour
trouver l'insitu le plus proche, puis on calcule le NSE modèle (pred) vs
insitu sur z-scores, aux dates satellite (mêmes dates qu'utilisées dans
les benchmarks précédents).

Sorties :
  ecarts_nse_insitu_par_station.csv
  summary_par_modele_insitu.csv

Usage :
    python analyse_ecarts_nse_per_station_insitu.py
════════════════════════════════════════════════════════════════════════
"""

import sqlite3
import numpy as np
import pandas as pd
import geopandas as gpd
from pathlib import Path
from shapely.geometry import Point

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES — adapter selon la source (hwnext / dahiti, 10j / 27j)
# ═══════════════════════════════════════════════════════════════
SOURCE       = "hwnext"     # "hwnext" ou "dahiti"
FREQ_LABEL   = "27j"        # "10j" ou "27j" — doit matcher le nom des fichiers résidus

RESIDUALS_DIR = Path(f"./data/outlier_detection/benchmark_DtoD_{SOURCE}{FREQ_LABEL}")
MASKS         = [50, 80, 90, 96]

HWNEXT_DB  = "./data/hydroweb_next.db"
DAHITI_DB  = "./data/dahiti.db"
INSITU_DB  = "./data/insitu_data.db"
INSITU_SHP = "./data/insitu/shp/station_schapi_alti_ref_2025_river.gpkg"

DATE_MIN    = "2016-01-01"
DATE_MAX    = "2025-12-31"
DIST_MAX_KM = 50.0
WINDOW_DAYS = 7 if FREQ_LABEL == "10j" else 14

TOP_N      = 20
CLIP_FLOOR = -1.0

# ═══════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════
def zscore(arr):
    arr = np.asarray(arr, dtype=float)
    m = ~np.isnan(arr)
    if m.sum() < 2:
        return arr * np.nan
    mu, sig = arr[m].mean(), arr[m].std()
    return (arr - mu) / sig if sig > 0 else arr * 0

def nse(obs, sim):
    m = ~(np.isnan(obs) | np.isnan(sim))
    if m.sum() < 5:
        return np.nan
    o, s = obs[m], sim[m]
    d = np.sum((o - o.mean()) ** 2)
    return float(1 - np.sum((o - s) ** 2) / d) if d > 0 else np.nan

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

def get_coords_station(source, code):
    db_path = HWNEXT_DB if source == "hwnext" else DAHITI_DB
    conn = sqlite3.connect(db_path)
    for c in [str(code), str(code).zfill(13)]:
        df = pd.read_sql(
            "SELECT reference_longitude AS lon, reference_latitude AS lat "
            "FROM stations WHERE station_code = ?",
            conn, params=(c,)
        )
        if not df.empty:
            conn.close()
            return float(df.iloc[0]["lon"]), float(df.iloc[0]["lat"])
    conn.close()
    return None, None

# ═══════════════════════════════════════════════════════════════
# CHARGEMENT INSITU + MATCHING
# ═══════════════════════════════════════════════════════════════
print("Chargement shapefile insitu...")
gdf        = gpd.read_file(INSITU_SHP).to_crs("EPSG:2154")
_cache_ins = {}

def get_insitu_proche(lon, lat):
    pt   = gpd.GeoSeries([Point(lon, lat)], crs="EPSG:4326").to_crs("EPSG:2154")[0]
    dist = gdf.geometry.distance(pt)
    idx  = dist.idxmin()
    return gdf.loc[idx, "code_sta"], dist[idx] / 1000

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

# ═══════════════════════════════════════════════════════════════
# CHARGEMENT RÉSIDUS PAR MODÈLE + CALCUL NSE VS INSITU
# ═══════════════════════════════════════════════════════════════
dfs_nse = {}

for mask in MASKS:
    csv_path = RESIDUALS_DIR / f"residuals_{SOURCE}_{FREQ_LABEL}_{mask}pct.csv"
    if not csv_path.exists():
        print(f"⚠ Fichier introuvable : {csv_path} → skip")
        continue

    df_model = pd.read_csv(csv_path)
    df_model["date"]    = pd.to_datetime(df_model["date"])
    df_model["station"] = df_model["station"].astype(str)
    df_model = df_model.dropna(subset=["obs", "pred"])

    rows = []
    for code in df_model["station"].unique():
        sub = df_model[df_model["station"] == code].sort_values("date")
        if len(sub) < 5:
            continue

        lon, lat = get_coords_station(SOURCE, code)
        if lon is None:
            continue
        code_ins, dist_km = get_insitu_proche(lon, lat)
        if dist_km > DIST_MAX_KM:
            continue
        df_ins = get_insitu_series(code_ins)
        if df_ins is None:
            continue

        ins = align_insitu(sub["date"].values, df_ins, WINDOW_DAYS)
        if np.sum(~np.isnan(ins)) < 5:
            continue

        nse_v = nse(zscore(ins), zscore(sub["pred"].values))
        rows.append({"station": code, f"NSE_{mask}pct": nse_v})

    df_mask = pd.DataFrame(rows)
    dfs_nse[mask] = df_mask
    print(f"  {mask}% : NSE vs insitu calculé pour {len(df_mask)} stations")

masks_ok = list(dfs_nse.keys())
if len(masks_ok) < 2:
    print("Pas assez de modèles chargés pour comparer.")
    exit()

# ═══════════════════════════════════════════════════════════════
# FUSION
# ═══════════════════════════════════════════════════════════════
df_merged = dfs_nse[masks_ok[0]]
for mask in masks_ok[1:]:
    df_merged = df_merged.merge(dfs_nse[mask], on="station", how="inner")

print(f"\n{len(df_merged)} stations communes aux {len(masks_ok)} modèles (NSE vs insitu)\n")

nse_cols = [f"NSE_{m}pct" for m in masks_ok]

# ═══════════════════════════════════════════════════════════════
# ÉCARTS, REGRET, RANG
# ═══════════════════════════════════════════════════════════════
df_merged["nse_min"]   = df_merged[nse_cols].min(axis=1)
df_merged["nse_max"]   = df_merged[nse_cols].max(axis=1)
df_merged["nse_ecart"] = df_merged["nse_max"] - df_merged["nse_min"]

df_merged["best_mask"]  = df_merged[nse_cols].idxmax(axis=1).str.replace("NSE_", "").str.replace("pct", "")
df_merged["worst_mask"] = df_merged[nse_cols].idxmin(axis=1).str.replace("NSE_", "").str.replace("pct", "")

nse_clipped  = df_merged[nse_cols].clip(lower=CLIP_FLOOR)
best_clipped = nse_clipped.max(axis=1)

for mask, col in zip(masks_ok, nse_cols):
    df_merged[f"regret_{mask}pct"]      = df_merged["nse_max"] - df_merged[col]
    df_merged[f"regret_clip_{mask}pct"] = best_clipped - nse_clipped[col]

ranks = df_merged[nse_cols].rank(axis=1, ascending=False, method="average")
ranks.columns = [f"rank_{m}pct" for m in masks_ok]
df_merged = pd.concat([df_merged, ranks], axis=1)

df_sorted = df_merged.sort_values("nse_ecart", ascending=False).reset_index(drop=True)
out_csv = RESIDUALS_DIR / "ecarts_nse_insitu_par_station.csv"
df_sorted.to_csv(out_csv, index=False)

# ═══════════════════════════════════════════════════════════════
# AFFICHAGE
# ═══════════════════════════════════════════════════════════════
print(f"{'='*90}")
print(f"  NSE calculé VS INSITU — {SOURCE.upper()} {FREQ_LABEL}")
print(f"{'='*90}")
print(f"  Écart médian   : {df_sorted['nse_ecart'].median():.3f}")
print(f"  Écart moyen    : {df_sorted['nse_ecart'].mean():.3f}")

print(f"\n{'='*90}")
print(f"  TOP {TOP_N} STATIONS — PLUS GROS ÉCARTS (NSE vs insitu)")
print(f"{'='*90}")
cols_display = ["station"] + nse_cols + ["nse_ecart", "best_mask", "worst_mask"]
print(df_sorted[cols_display].head(TOP_N).to_string(index=False, float_format=lambda x: f"{x:.3f}"))
print(f"\n  CSV complet → {out_csv}")

best_counts  = df_sorted["best_mask"].value_counts()
worst_counts = df_sorted["worst_mask"].value_counts()

summary_rows = []
for mask in masks_ok:
    mask_str = str(mask)
    summary_rows.append({
        "masquage"          : f"{mask}%",
        "n_best"            : int(best_counts.get(mask_str, 0)),
        "n_worst"           : int(worst_counts.get(mask_str, 0)),
        "nse_median"        : df_sorted[f"NSE_{mask}pct"].median(),
        "regret_moyen"      : df_sorted[f"regret_{mask}pct"].mean(),
        "regret_median"     : df_sorted[f"regret_{mask}pct"].median(),
        "regret_clip_moyen" : df_sorted[f"regret_clip_{mask}pct"].mean(),
        "rang_moyen"        : df_sorted[f"rank_{mask}pct"].mean(),
    })

df_summary = pd.DataFrame(summary_rows).sort_values("regret_clip_moyen")
summary_csv = RESIDUALS_DIR / "summary_par_modele_insitu.csv"
df_summary.to_csv(summary_csv, index=False)

print(f"\n{'='*90}")
print(f"  RÉSUMÉ PAR MODÈLE — NSE VS INSITU — {SOURCE.upper()} {FREQ_LABEL}")
print(f"{'='*90}")
print(df_summary.to_string(index=False, float_format=lambda x: f"{x:.3f}"))
print(f"\n  Trié par regret clippé moyen (croissant = meilleur)")
print(f"  CSV → {summary_csv}")