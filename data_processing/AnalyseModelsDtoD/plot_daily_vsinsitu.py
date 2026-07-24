"""
plot_daily_vs_insitu_80pct.py
════════════════════════════════════════════════════════════════════════
Pour chaque station DAHITI 10j (matching insitu via coordonnées DAHITI),
plot la prédiction quotidienne complète du modèle DtoD80 vs l'insitu,
une figure par année.

Source des prédictions :
  ./data_processing/AnalyseModelsDtoD/residuals/residuals_dahiti_10j_80pct.csv

Sorties :
  ./data_processing/AnalyseModelsDtoD/plot/{station}/{station}_{year}.png

Usage :
    python plot_daily_vs_insitu_80pct.py
════════════════════════════════════════════════════════════════════════
"""

import sqlite3
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import geopandas as gpd
from pathlib import Path
from shapely.geometry import Point

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
MASK_PCT     = 80
RESIDUALS_CSV = Path(f"./data_processing/AnalyseModelsDtoD/residuals/residuals_dahiti_10j_{MASK_PCT}pct.csv")

STATIONS_TXT = Path("./data/IA/NeuralHydrologyDahiti10jClean/stations_dahiti_10j.txt")
DAHITI_DB    = "./data/dahiti.db"
INSITU_DB    = "./data/insitu_data.db"
INSITU_SHP   = "./data/insitu/shp/station_schapi_alti_ref_2025_river.gpkg"

OUTPUT_BASE = Path("./data_processing/AnalyseModelsDtoD/plot")
OUTPUT_BASE.mkdir(parents=True, exist_ok=True)

DATE_MIN    = "2016-01-01"
DATE_MAX    = "2025-12-31"
DIST_MAX_KM = 50.0
WINDOW_DAYS_DAILY = 1   # alignement strict ±1j (comparaison quotidienne)

C_PRED = "#c0392b"   # modèle (quotidien)
C_INS  = "#e67e22"   # insitu

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

# ═══════════════════════════════════════════════════════════════
# CHARGEMENT INSITU + COORDS DAHITI (matching uniquement)
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

stations_10j = [s.strip() for s in STATIONS_TXT.read_text().split() if s.strip()]
print(f"{len(stations_10j)} stations DAHITI 10j (matching insitu)\n")

conn_da = sqlite3.connect(DAHITI_DB)
station_to_insitu = {}
for code in stations_10j:
    lon, lat = get_coords_dahiti(conn_da, code)
    if lon is None:
        continue
    code_ins, dist_km = get_insitu_proche(lon, lat)
    if dist_km > DIST_MAX_KM:
        continue
    df_ins = get_insitu_series(code_ins)
    if df_ins is None:
        continue
    station_to_insitu[code] = (code_ins, dist_km, df_ins)
conn_da.close()

print(f"{len(station_to_insitu)} stations avec insitu <{DIST_MAX_KM}km\n")

# ═══════════════════════════════════════════════════════════════
# CHARGEMENT PRÉDICTIONS MODÈLE
# ═══════════════════════════════════════════════════════════════
if not RESIDUALS_CSV.exists():
    print(f"⚠ Fichier introuvable : {RESIDUALS_CSV}")
    exit()

df_model = pd.read_csv(RESIDUALS_CSV)
df_model["date"]    = pd.to_datetime(df_model["date"])
df_model["station"] = df_model["station"].astype(str)
print(f"Résidus chargés : {len(df_model)} lignes, {df_model['station'].nunique()} stations\n")

# ═══════════════════════════════════════════════════════════════
# GÉNÉRATION DES PLOTS
# ═══════════════════════════════════════════════════════════════
n_plots = 0
n_skip  = 0

for code, (code_ins, dist_km, df_ins) in station_to_insitu.items():
    sub = df_model[df_model["station"] == code].sort_values("date").reset_index(drop=True)
    sub = sub.dropna(subset=["pred"])
    if len(sub) < 10:
        n_skip += 1
        continue

    dates     = sub["date"].values
    pred_full = sub["pred"].values

    ins_wl  = align_insitu(dates, df_ins, WINDOW_DAYS_DAILY)
    n_pairs = int(np.sum(~np.isnan(ins_wl)))
    if n_pairs < 10:
        n_skip += 1
        continue

    pred_z = zscore(pred_full)
    ins_z  = zscore(ins_wl)

    out_dir = OUTPUT_BASE / code
    out_dir.mkdir(parents=True, exist_ok=True)

    years = sorted(pd.to_datetime(dates).year.unique())

    for year in years:
        mask_year = pd.to_datetime(dates).year == year
        if mask_year.sum() < 30:   # au moins ~1 mois de données pour que le plot ait un sens
            continue

        dates_y = dates[mask_year]
        pred_y  = pred_z[mask_year]
        ins_y   = ins_z[mask_year]

        nse_y = nse(ins_y, pred_y)
        nse_str = f"{nse_y:.3f}" if not np.isnan(nse_y) else "n/a"

        fig, ax = plt.subplots(figsize=(14, 5))
        ax.plot(dates_y, pred_y, "-", color=C_PRED, lw=1.3,
                label=f"Modèle DtoD{MASK_PCT}% (quotidien)")
        ax.plot(dates_y, ins_y, "o-", color=C_INS, lw=1.3, ms=3,
                label=f"Insitu {code_ins}")
        ax.axhline(0, color="gray", lw=0.7, ls=":")

        ax.set_title(
            f"Station DAHITI {code} — {year}  |  NSE = {nse_str}\n"
            f"Insitu : {code_ins} ({dist_km:.1f} km)",
            fontsize=11, fontweight="bold"
        )
        ax.set_ylabel("WL (z-score)", fontsize=9)
        ax.set_xlabel("Date", fontsize=9)
        ax.legend(fontsize=9, loc="upper right")
        ax.grid(True, alpha=0.3)
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b"))
        ax.xaxis.set_major_locator(mdates.MonthLocator())
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=30, ha="right")

        plt.tight_layout()
        fig_path = out_dir / f"{code}_{year}.png"
        fig.savefig(fig_path, dpi=130, bbox_inches="tight")
        plt.close(fig)
        n_plots += 1

    if n_plots % 50 == 0 and n_plots > 0:
        print(f"  {n_plots} plots générés...")

print(f"\n{'='*55}")
print(f"  Plots générés : {n_plots}")
print(f"  Stations skip : {n_skip}")
print(f"  Dossier       : {OUTPUT_BASE}/")
print(f"{'='*55}")