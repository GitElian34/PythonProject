"""
benchmark_dahiti_final.py
════════════════════════════════════════════════════════════════════════
Benchmark final : DAHITI vs insitu & Modèle AR-LSTM vs insitu — 10j

Sources :
  - DAHITI obs  : CSV résidus (date + obs)
  - Modèle pred : CSV résidus (date + pred)
  - Insitu      : insitu_data.db (h_med_wsh)
  - Coords      : dahiti.db (stations, reference_longitude/latitude)

Sorties :
  - benchmark_dahiti_10j.csv
  - benchmark_dahiti_global.png  (boxplots comparatifs)
════════════════════════════════════════════════════════════════════════
"""

import sqlite3
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import geopandas as gpd
from pathlib import Path
from shapely.geometry import Point

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
CSV_10J    = "./data/outlier_detection/benchmark_DtoD_dahiti27j/residuals_dahiti_27j_80pct.csv"
INSITU_DB  = "./data/insitu_data.db"
DAHITI_DB  = "./data/dahiti.db"
INSITU_SHP = "./data/insitu/shp/station_schapi_alti_ref_2025_river.gpkg"
OUTPUT_DIR = Path("./data/outlier_detection/benchmark_dahiti")

DATE_MIN    = "2016-01-01"
DATE_MAX    = "2025-12-31"
DIST_MAX_KM = 10.0
WINDOW_DAYS = 7

C_DA  = "#2980b9"   # DAHITI
C_MOD = "#c0392b"   # Modèle
C_INS = "#e67e22"   # Insitu

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ═══════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════
def zscore(arr):
    arr = np.asarray(arr, dtype=float)
    mask = ~np.isnan(arr)
    if mask.sum() < 2:
        return arr * np.nan
    mu, sig = arr[mask].mean(), arr[mask].std()
    return (arr - mu) / sig if sig > 0 else arr * 0

def nse(obs, sim):
    mask = ~(np.isnan(obs) | np.isnan(sim))
    if mask.sum() < 5:
        return np.nan
    o, s = obs[mask], sim[mask]
    d = np.sum((o - o.mean()) ** 2)
    return float(1 - np.sum((o - s) ** 2) / d) if d > 0 else np.nan

def align_insitu(dates, df_ins, window_days):
    wl        = np.full(len(dates), np.nan)
    ins_dates = np.array(df_ins["date"].values, dtype="datetime64[D]")
    ins_wl    = df_ins["wl"].values
    for i, d in enumerate(np.array(dates, dtype="datetime64[D]")):
        diff = np.abs((ins_dates - d).astype(float))
        idx  = int(np.argmin(diff))
        if diff[idx] <= window_days:
            wl[i] = ins_wl[idx]
    return wl

def _plot_boxplot(ax, data_dict, rng, title):
    labels = list(data_dict.keys())
    vals   = [data_dict[l][0] for l in labels]
    colors = [data_dict[l][1] for l in labels]

    bp = ax.boxplot(vals, tick_labels=labels, patch_artist=True,
                    medianprops={"color": "black", "linewidth": 2},
                    widths=0.45)
    for box, color in zip(bp["boxes"], colors):
        box.set_facecolor(color)
        box.set_alpha(0.65)

    for j, (v, color) in enumerate(zip(vals, colors), 1):
        jitter = rng.uniform(-0.12, 0.12, len(v))
        ax.scatter(np.full(len(v), j) + jitter, v,
                   alpha=0.35, s=14, color=color, zorder=3)
        med = np.nanmedian(v)
        ax.text(j, med + 0.03, f"{med:.3f}", ha="center",
                fontsize=9, fontweight="bold")

    ax.axhline(0,   color="red",   lw=1, ls="--", alpha=0.5, label="NSE=0")
    ax.axhline(0.5, color="green", lw=1, ls="--", alpha=0.5, label="NSE=0.5")
    ax.set_title(title, fontsize=10, fontweight="bold")
    ax.set_ylabel("NSE")
    ax.grid(True, alpha=0.25, axis="y")
    ax.legend(fontsize=8)

# ═══════════════════════════════════════════════════════════════
# CHARGEMENT INSITU
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

def get_coords_dahiti(conn_da, code):
    for c in [str(code), str(code).zfill(13)]:
        df = pd.read_sql(
            "SELECT reference_longitude AS lon, reference_latitude AS lat "
            "FROM stations WHERE station_code = ?",
            conn_da, params=(c,)
        )
        if not df.empty:
            return float(df.iloc[0]["lon"]), float(df.iloc[0]["lat"])
    return None, None

# ═══════════════════════════════════════════════════════════════
# TRAITEMENT 10j
# ═══════════════════════════════════════════════════════════════
print(f"\n{'='*65}")
print(f"  TRAITEMENT DAHITI 10j")
print(f"{'='*65}")

df = pd.read_csv(CSV_10J)
df["date"]    = pd.to_datetime(df["date"])
df["station"] = df["station"].astype(str)
df = df.dropna(subset=["obs", "pred"])
print(f"  Lignes après dropna : {len(df)} | Stations : {df['station'].nunique()}")

conn_da = sqlite3.connect(DAHITI_DB)
results = []

for code in df["station"].unique():
    sub = df[df["station"] == code].sort_values("date").reset_index(drop=True)
    if len(sub) < 5:
        continue

    lon, lat = get_coords_dahiti(conn_da, code)
    if lon is None:
        continue

    code_ins, dist_km = get_insitu_proche(lon, lat)
    if dist_km > DIST_MAX_KM:
        continue

    df_ins = get_insitu_series(code_ins)
    if df_ins is None:
        continue

    ins     = align_insitu(sub["date"].values, df_ins, WINDOW_DAYS)
    n_pairs = int(np.sum(~np.isnan(ins)))
    if n_pairs < 5:
        continue

    obs_z  = zscore(sub["obs"].values)
    pred_z = zscore(sub["pred"].values)
    ins_z  = zscore(ins)

    nse_da     = nse(obs_z,  ins_z)
    nse_mod    = nse(pred_z, ins_z)
    nse_mod_da = nse(pred_z, obs_z)

    results.append({
        "station"        : code,
        "code_insitu"    : code_ins,
        "dist_insitu_km" : round(dist_km, 1),
        "n_dates"        : len(sub),
        "n_insitu_pairs" : n_pairs,
        "nse_dahiti_ins" : round(nse_da,     3) if not np.isnan(nse_da)     else np.nan,
        "nse_modele_ins" : round(nse_mod,    3) if not np.isnan(nse_mod)    else np.nan,
        "nse_modele_da"  : round(nse_mod_da, 3) if not np.isnan(nse_mod_da) else np.nan,
    })

conn_da.close()

df_out = pd.DataFrame(results)
csv_path = OUTPUT_DIR / "benchmark_dahiti_10j.csv"
df_out.to_csv(csv_path, index=False)

print(f"\n  Stations traitées : {len(df_out)}")
if len(df_out) > 0:
    meilleur = (df_out["nse_modele_ins"] > df_out["nse_dahiti_ins"]).sum()
    print(f"\n  {'':30} {'médiane':>8} {'moyenne':>8} {'> 0':>6} {'> 0.5':>6}")
    print(f"  {'-'*60}")
    for col, label in [
        ("nse_dahiti_ins", "DAHITI obs   vs insitu"),
        ("nse_modele_ins", "Modele pred  vs insitu"),
        ("nse_modele_da",  "Modele pred  vs DAHITI"),
    ]:
        v = df_out[col].dropna()
        print(f"  {label:<30} {v.median():>8.3f} {v.mean():>8.3f} {(v>0).sum():>6} {(v>0.5).sum():>6}")
    print(f"\n  Modele meilleur que DAHITI : {meilleur}/{len(df_out)} ({meilleur/len(df_out):.0%})")
    print(f"  CSV → {csv_path}")

# ═══════════════════════════════════════════════════════════════
# FIGURE — boxplots
# ═══════════════════════════════════════════════════════════════
if len(df_out) > 0:
    rng = np.random.default_rng(42)
    fig, axes = plt.subplots(1, 2, figsize=(12, 6))
    fig.suptitle(
        "Benchmark NSE — DAHITI & Modèle AR-LSTM vs Insitu (10j)\n"
        "(z-scores indépendants, fenêtre alignement ±7j)",
        fontsize=12, fontweight="bold"
    )

    _plot_boxplot(axes[0], {
        "DAHITI\nvs insitu" : (df_out["nse_dahiti_ins"].dropna().values, C_DA),
        "Modèle\nvs insitu" : (df_out["nse_modele_ins"].dropna().values, C_MOD),
    }, rng, title=f"DAHITI & Modèle vs Insitu  (n={len(df_out)})")

    _plot_boxplot(axes[1], {
        "Modèle\nvs DAHITI" : (df_out["nse_modele_da"].dropna().values, C_MOD),
    }, rng, title=f"Modèle vs DAHITI  (n={len(df_out)})")

    plt.tight_layout()
    fig_path = OUTPUT_DIR / "benchmark_dahiti_global.png"
    fig.savefig(fig_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"\n✅ Figure → {fig_path}")