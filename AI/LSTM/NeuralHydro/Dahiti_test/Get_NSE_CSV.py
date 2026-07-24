"""
compute_nse_benchmark_csv.py
═══════════════════════════════════════════════════════════════════════════
Pour chaque station HW Next avec correspondance DAHITI < 1.5km,
calcule les 3 NSE vs insitu le plus proche :

  - nse_hw_ins    : NSE HW Next ↔ insitu
  - nse_dahiti_ins: NSE DAHITI  ↔ insitu
  - nse_modele_ins: NSE Modèle(pred) ↔ insitu

Les 3 sont calculés de façon identique :
  nse(zscore(série), zscore(insitu_aligné))

Sorties :
  ./AI/LSTM/NeuralHydro/Dahiti_test/nse_benchmark_27j.csvC
  ./AI/LSTM/NeuralHydro/Dahiti_test/nse_benchmark_10j.csv
═══════════════════════════════════════════════════════════════════════════
"""

import sqlite3
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import Point

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
HWNEXT_DB   = "./data/hydroweb_next.db"
DAHITI_DB   = "./data/dahiti.db"
INSITU_DB   = "./data/insitu_data.db"
INSITU_SHP  = "./data/insitu/shp/station_schapi_alti_ref_2025_river.gpkg"

RESIDUALS_27J = "./data/outlier_detection/residuals_27j_hydroweb_next.csv"
RESIDUALS_10J = "./data/outlier_detection/residuals_10j_hydroweb_next.csv"
STATIONS_27J  = "./data/IA/NeuralHydrology_hydroweb_next/stations_dahiti_27j.txt"
STATIONS_10J  = "./data/IA/NeuralHydrology_hydroweb_next/stations_dahiti_10j.txt"

OUTPUT_DIR    = Path("./AI/LSTM/NeuralHydro/Dahiti_test")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

DATE_MIN, DATE_MAX     = "2016-01-01", "2025-12-31"
DIST_MAX_INSITU_KM     = 50.0
DIST_MAX_DAHITI_KM     = 1.5
WINDOW_27J, WINDOW_10J = 14, 5

# ═══════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════
def zscore(arr):
    mu, sig = np.nanmean(arr), np.nanstd(arr)
    return (arr - mu) / sig if sig > 0 else arr * 0

def nse(obs, sim):
    mask = ~(np.isnan(obs) | np.isnan(sim))
    if mask.sum() < 5: return np.nan
    o, s = obs[mask], sim[mask]
    d = np.sum((o - o.mean()) ** 2)
    return 1 - np.sum((o - s) ** 2) / d if d > 0 else np.nan

def align_series(dates_ref, df_ins, window_days):
    """Aligne l'insitu sur dates_ref avec fenêtre ±window_days."""
    wl = np.full(len(dates_ref), np.nan)
    for i, d in enumerate(pd.to_datetime(dates_ref)):
        diff = (df_ins["date"] - d).abs()
        idx  = diff.idxmin()
        if diff[idx] <= pd.Timedelta(days=window_days):
            wl[i] = df_ins.loc[idx, "wl"]
    return wl

def get_coords(conn, station_code):
    for code in [station_code, station_code.lstrip("0") or station_code]:
        df = pd.read_sql(
            "SELECT reference_longitude AS lon, reference_latitude AS lat "
            "FROM stations WHERE station_code = ?",
            conn, params=(code,)
        )
        if not df.empty:
            return float(df.iloc[0]["lon"]), float(df.iloc[0]["lat"])
    return None, None

def get_alti_series(conn, station_code):
    for code in [station_code, station_code.lstrip("0") or station_code]:
        df = pd.read_sql("""
            SELECT measure_date AS date, orthometric_height AS wl
            FROM measurements
            WHERE station_code = ? AND is_valid = 1
              AND measure_date >= ? AND measure_date <= ?
            ORDER BY date
        """, conn, params=(code, DATE_MIN, DATE_MAX))
        if not df.empty:
            df["date"] = pd.to_datetime(df["date"])
            return df.dropna(subset=["wl"])
    return pd.DataFrame()

def get_insitu_series(code_sta):
    conn = sqlite3.connect(INSITU_DB)
    df = pd.read_sql("""
        SELECT date, h_med_wsh AS wl FROM mesures_insitu
        WHERE code_sta = ? AND date >= ? AND date <= ?
        ORDER BY date
    """, conn, params=(code_sta, DATE_MIN, DATE_MAX))
    conn.close()
    df["date"] = pd.to_datetime(df["date"])
    return df.dropna(subset=["wl"])

def find_dahiti_match(lon, lat, df_d_idx):
    dists = np.sqrt(
        ((df_d_idx["lon"] - lon) * 111 * np.cos(np.radians(lat))) ** 2 +
        ((df_d_idx["lat"] - lat) * 111) ** 2
    )
    idx_min = dists.idxmin()
    dist_km = dists[idx_min]
    if dist_km <= DIST_MAX_DAHITI_KM:
        return df_d_idx.loc[idx_min, "station_code"], dist_km
    return None, float("inf")

# ═══════════════════════════════════════════════════════════════
# CALCUL PAR FRÉQUENCE
# ═══════════════════════════════════════════════════════════════
def compute_nse_csv(stations_file, residuals_csv, freq_label, window):
    print(f"\n{'='*60}")
    print(f"  NSE BENCHMARK {freq_label} — tous vs insitu")
    print(f"{'='*60}")

    stations = [s.strip().zfill(13)
                for s in open(stations_file).read().split() if s.strip()]

    df_res = pd.read_csv(residuals_csv)
    df_res["station"] = df_res["station"].astype(str).str.zfill(13)
    df_res["date"]    = pd.to_datetime(df_res["date"])

    gdf     = gpd.read_file(INSITU_SHP).to_crs("EPSG:2154")
    conn_hw = sqlite3.connect(HWNEXT_DB)
    conn_d  = sqlite3.connect(DAHITI_DB)

    df_d_idx = pd.read_sql("""
        SELECT station_code, reference_longitude AS lon, reference_latitude AS lat
        FROM stations WHERE reference_longitude IS NOT NULL
    """, conn_d)

    rows = []

    for code in stations:
        lon, lat = get_coords(conn_hw, code)
        if lon is None: continue

        # Match DAHITI < 1.5km — skip si absent
        code_d, dist_d = find_dahiti_match(lon, lat, df_d_idx)
        if code_d is None: continue

        # Insitu le plus proche (référence commune pour les 3)
        pt      = gpd.GeoSeries([Point(lon, lat)], crs="EPSG:4326").to_crs("EPSG:2154")[0]
        dists_g = gdf.geometry.distance(pt)
        idx_ins = dists_g.idxmin()
        dist_ins_km = dists_g[idx_ins] / 1000
        code_ins    = gdf.loc[idx_ins, "code_sta"]
        if dist_ins_km > DIST_MAX_INSITU_KM: continue

        df_ins = get_insitu_series(code_ins)
        if df_ins.empty: continue

        # Séries altimétriques
        df_hw = get_alti_series(conn_hw, code)
        df_d  = get_alti_series(conn_d, code_d)
        sub   = df_res[df_res["station"] == code].sort_values("date")
        if sub.empty or df_hw.empty: continue

        # ── NSE 1 : HW Next ↔ insitu ─────────────────────────
        ins_hw     = align_series(df_hw["date"].values, df_ins, window)
        nse_hw_ins = nse(zscore(df_hw["wl"].values), zscore(ins_hw))

        # ── NSE 2 : DAHITI ↔ insitu ──────────────────────────
        nse_d_ins = np.nan
        if not df_d.empty:
            ins_d     = align_series(df_d["date"].values, df_ins, window)
            nse_d_ins = nse(zscore(df_d["wl"].values), zscore(ins_d))

        # ── NSE 3 : Modèle(pred) ↔ insitu ────────────────────
        ins_mod     = align_series(sub["date"].values, df_ins, window)
        nse_mod_ins = nse(zscore(sub["pred"].values), zscore(ins_mod))

        rows.append({
            "station"        : code,
            "freq"           : freq_label,
            "code_dahiti"    : code_d,
            "dist_dahiti_km" : round(dist_d, 2),
            "code_insitu"    : code_ins,
            "dist_insitu_km" : round(dist_ins_km, 1),
            "n_hw"           : len(df_hw),
            "n_dahiti"       : len(df_d),
            "n_modele"       : len(sub),
            "nse_hw_ins"     : round(nse_hw_ins,  3) if not np.isnan(nse_hw_ins)  else np.nan,
            "nse_dahiti_ins" : round(nse_d_ins,   3) if not np.isnan(nse_d_ins)   else np.nan,
            "nse_modele_ins" : round(nse_mod_ins, 3) if not np.isnan(nse_mod_ins) else np.nan,
        })

        print(f"  {code} | "
              f"HW↔ins={rows[-1]['nse_hw_ins']:.3f} | "
              f"DAHITI↔ins={rows[-1]['nse_dahiti_ins']:.3f} | "
              f"Mod↔ins={rows[-1]['nse_modele_ins']:.3f}")

    conn_hw.close()
    conn_d.close()

    df = pd.DataFrame(rows)
    csv_path = OUTPUT_DIR / f"nse_benchmark_{freq_label}.csv"
    df.to_csv(csv_path, index=False)

    print(f"\n  Stations avec DAHITI : {len(df)}")
    for col, label in [
        ("nse_hw_ins",     "HW Next ↔ insitu    "),
        ("nse_dahiti_ins", "DAHITI  ↔ insitu    "),
        ("nse_modele_ins", "Modèle  ↔ insitu    "),
    ]:
        v = df[col].dropna()
        print(f"\n  {label} (n={len(v)})")
        print(f"    NSE médian : {v.median():.3f}")
        print(f"    NSE moyen  : {v.mean():.3f}")
        print(f"    NSE > 0.5  : {(v > 0.5).sum()} ({(v > 0.5).mean():.0%})")
        print(f"    NSE < 0    : {(v < 0).sum()} ({(v < 0).mean():.0%})")

    print(f"\n✅ {csv_path}")
    return df

df_27 = compute_nse_csv(STATIONS_27J, RESIDUALS_27J, "27j", WINDOW_27J)
df_10 = compute_nse_csv(STATIONS_10J, RESIDUALS_10J, "10j", WINDOW_10J)