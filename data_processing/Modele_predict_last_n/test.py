"""
baseline_persistence_simple.py
----------------------------------
Version simplifiée : réutilise directement residuals_{label}_{source}_{freq}_full.csv
(déjà produit par eval_single_model_predict_last_n.py) comme source du vrai
water_level (colonne "obs"), au lieu de rouvrir les .nc bruts.

Baseline triviale : prédiction(date) = dernière valeur "obs" connue avant
cette date (persistance / LOCF). Compare le delta NSE/KGE entre offset le
plus ancien et le nowcast à celui du vrai modèle, contre l'insitu le plus
proche.

Usage :
    python baseline_persistence_simple.py
"""

import pickle
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point


# ──────────────────────────────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────────────────────────────

RUNS_ROOT = Path("./runs")
RUN_NAME = "arlstm_DtoD90_last10_attention_1707_154232"
EPOCH = 28
FREQ_KEY = "1D"

# Fichier déjà produit par eval_single_model_predict_last_n.py, qui marche.
RESIDUALS_FULL_CSV = Path(
    "./data_processing/Modele_predict_last_n/residuals/residuals_DtoD90_last10_attention_dahiti_27j_full.csv"
)

SOURCE = "dahiti"
DAHITI_DB = "./data/dahiti.db"
INSITU_DB = "./data/insitu_data.db"
INSITU_SHP = "./data/insitu/shp/station_schapi_alti_ref_2025_river.gpkg"
SAT_DB = DAHITI_DB

DATE_MIN = "2016-01-01"
DATE_MAX = "2025-12-31"
DIST_MAX_KM = 10.0
WINDOW_DAYS = 14
MIN_PAIRS = 10

OUT_DIR = Path("./data_processing/Modele_predict_last_n/per_timestep_vs_insitu")
OUT_DIR.mkdir(parents=True, exist_ok=True)


def nse(obs, sim):
    mask = ~np.isnan(obs) & ~np.isnan(sim)
    obs, sim = obs[mask], sim[mask]
    if len(obs) < 2:
        return np.nan
    denom = np.sum((obs - obs.mean()) ** 2)
    return 1 - np.sum((obs - sim) ** 2) / denom if denom > 0 else np.nan


def kge_no_beta(obs, sim):
    mask = ~np.isnan(obs) & ~np.isnan(sim)
    obs, sim = obs[mask], sim[mask]
    if len(obs) < 2 or obs.std() == 0 or sim.std() == 0:
        return np.nan
    r = np.corrcoef(obs, sim)[0, 1]
    alpha = sim.std() / obs.std()
    return 1 - np.sqrt((r - 1) ** 2 + (alpha - 1) ** 2)


def zscore(arr):
    arr = np.asarray(arr, dtype=float)
    m = ~np.isnan(arr)
    if m.sum() < 2:
        return arr * np.nan
    mu, sig = arr[m].mean(), arr[m].std()
    return (arr - mu) / sig if sig > 0 else arr * 0


print("Chargement shapefile insitu...")
gdf_insitu = gpd.read_file(INSITU_SHP).to_crs("EPSG:2154")
_cache_coords, _cache_ins_series, _cache_match = {}, {}, {}


def get_coords(code):
    if code in _cache_coords:
        return _cache_coords[code]
    conn = sqlite3.connect(SAT_DB)
    for c in [str(code), str(code).zfill(13)]:
        df = pd.read_sql("SELECT reference_longitude AS lon, reference_latitude AS lat "
                          "FROM stations WHERE station_code = ?", conn, params=(c,))
        if not df.empty:
            conn.close()
            _cache_coords[code] = (float(df.iloc[0]["lon"]), float(df.iloc[0]["lat"]))
            return _cache_coords[code]
    conn.close()
    _cache_coords[code] = (None, None)
    return None, None


def get_insitu_proche(lon, lat):
    pt = gpd.GeoSeries([Point(lon, lat)], crs="EPSG:4326").to_crs("EPSG:2154")[0]
    dist = gdf_insitu.geometry.distance(pt)
    idx = dist.idxmin()
    return gdf_insitu.loc[idx, "code_sta"], dist[idx] / 1000


def get_insitu_series(code_sta):
    if code_sta not in _cache_ins_series:
        conn = sqlite3.connect(INSITU_DB)
        df = pd.read_sql("SELECT date, h_med_wsh AS wl FROM mesures_insitu "
                          "WHERE code_sta = ? AND date >= ? AND date <= ? ORDER BY date",
                          conn, params=(code_sta, DATE_MIN, DATE_MAX))
        conn.close()
        df["date"] = pd.to_datetime(df["date"])
        df = df.dropna(subset=["wl"]).drop_duplicates(subset=["date"])
        _cache_ins_series[code_sta] = df.set_index("date")["wl"].sort_index() if len(df) >= 5 else None
    return _cache_ins_series[code_sta]


def get_match(station_id):
    if station_id in _cache_match:
        return _cache_match[station_id]
    lon, lat = get_coords(station_id)
    if lon is None:
        _cache_match[station_id] = None
        return None
    code_ins, dist_km = get_insitu_proche(lon, lat)
    if dist_km > DIST_MAX_KM:
        _cache_match[station_id] = None
        return None
    _cache_match[station_id] = get_insitu_series(code_ins)
    return _cache_match[station_id]


def main():
    # ── Offsets réels du modèle (pour tester exactement les mêmes positions) ──
    p_path = RUNS_ROOT / RUN_NAME / "validation" / f"model_epoch{EPOCH:03d}" / "validation_results.p"
    with open(p_path, "rb") as f:
        results = pickle.load(f)
    sample_ds = next(iter(results.values()))[FREQ_KEY]["xr"]
    time_step_offsets = [int(x) for x in sample_ds.coords["time_step"].values]
    print(f"[INFO] offsets testés : {time_step_offsets}")

    # ── Obs (= water_level) déjà disponible dans les résidus produits ──
    df = pd.read_csv(RESIDUALS_FULL_CSV, parse_dates=["date"])
    df["station"] = df["station"].astype(str)
    print(f"[INFO] {df['station'].nunique()} stations dans {RESIDUALS_FULL_CSV.name}")

    results_rows = []
    for station_id, g in df.groupby("station"):
        insitu_series = get_match(station_id)
        if insitu_series is None:
            continue

        obs_series = g.set_index("date")["obs"].sort_index()
        obs_locf = obs_series.ffill()
        anchor_dates = obs_series.index  # les dates réelles de la station

        for offset in time_step_offsets:
            calendar_dates = anchor_dates + pd.to_timedelta(offset, unit="D")
            pred = obs_locf.reindex(calendar_dates, method="ffill").values.astype(float)
            ins = insitu_series.reindex(calendar_dates, method="nearest",
                                          tolerance=pd.Timedelta(days=WINDOW_DAYS)).values.astype(float)

            both = ~np.isnan(pred) & ~np.isnan(ins)
            if both.sum() < MIN_PAIRS:
                continue

            pred_z, ins_z = zscore(pred[both]), zscore(ins[both])
            results_rows.append({
                "station": station_id, "offset": offset,
                "NSE": nse(ins_z, pred_z), "KGE": kge_no_beta(ins_z, pred_z),
            })

    df_res = pd.DataFrame(results_rows)
    if df_res.empty:
        print("[ERREUR] Toujours aucune donnée -- vérifier RESIDUALS_FULL_CSV et le matching insitu.")
        return

    summary = df_res.groupby("offset")[["NSE", "KGE"]].median().sort_index()
    print("\n" + "=" * 70)
    print("BASELINE PERSISTANCE (LOCF sur obs satellite) — VS INSITU")
    print("=" * 70)
    print(summary.to_string(float_format=lambda x: f"{x:.4f}"))
    print("=" * 70)

    delta_nse = summary["NSE"].iloc[-1] - summary["NSE"].iloc[0]
    delta_kge = summary["KGE"].iloc[-1] - summary["KGE"].iloc[0]
    print(f"\n[COMPARAISON] offset {summary.index[0]}j -> {summary.index[-1]}j : "
          f"delta NSE = {delta_nse:+.4f}, delta KGE = {delta_kge:+.4f}")
    print(f"[RAPPEL] delta du vrai modèle (attention) : NSE = -0.0040, KGE = -0.0020")

    summary.to_csv(OUT_DIR / "baseline_persistence_simple.csv")
    print(f"\n[OK] Sauvegardé.")


if __name__ == "__main__":
    main()