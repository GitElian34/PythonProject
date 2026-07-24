"""
plot_compare_models_dahiti10j.py
════════════════════════════════════════════════════════════════════════
Pour chaque station DAHITI 10j, génère une figure par année avec
les 5 courbes de prédiction (DtoD 0/20/50/80% + modèle legacy 10j)
comparées à l'obs satellite et à l'insitu le plus proche.

Panels :
  [1] Obs satellite + 4 modèles DtoD
  [2] Obs satellite + modèle legacy 10j
  [3] Obs satellite vs insitu (référence)

Sorties :
  ./AI/LSTM/NeuralHydro/compare_models/{station}/{station}_{year}.png

Usage :
    python plot_compare_models_dahiti10j.py
════════════════════════════════════════════════════════════════════════
"""

import sqlite3
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import geopandas as gpd
import netCDF4 as nc
from pathlib import Path
from shapely.geometry import Point

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
RESIDUALS_DIR  = Path("./data/outlier_detection/benchmark_DtoD_hwnext10j")
RESIDUALS_LEG  = Path("./data/outlier_detection/benchmark_legacy_hwnext10j/residuals_hwnext_10j_legacy.csv")
NC_DIR_DTOD    = Path("./data/IA/NeuralHydrologyDahitiDtoD/time_series")
NC_DIR_LEGACY  = Path("./data/IA/NeuralHydrology_hydroweb_next/10j/time_series")

STATIONS_TXT   = Path("./data/IA/NeuralHydrologyHWNextDtoD/stations_hwnext_10j.txt")

DAHITI_DB  = "./data/hydroweb_next.db"
INSITU_DB  = "./data/insitu_data.db"
INSITU_SHP = "./data/insitu/shp/station_schapi_alti_ref_2025_river.gpkg"

OUTPUT_BASE = Path("./AI/LSTM/NeuralHydro/HydrowebNext/plot/compare_models")
DATE_MIN    = "2016-01-01"
DATE_MAX    = "2025-12-31"
DIST_MAX_KM = 50.0
WINDOW_DAYS = 7

# Modèles DtoD
DTOD_MODELS = {
    0  : {"csv": "residuals_hwnext_10j_0pct.csv",  "color": "#95a5a6", "label": "DtoD 0%"},
    20 : {"csv": "residuals_hwnext_10j_20pct.csv", "color": "#3498db", "label": "DtoD 20%"},
    50 : {"csv": "residuals_hwnext_10j_50pct.csv", "color": "#27ae60", "label": "DtoD 50%"},
    80 : {"csv": "residuals_hwnext_10j_80pct.csv", "color": "#e74c3c", "label": "DtoD 80%"},
}
C_OBS = "#2c3e50"   # obs satellite
C_LEG = "#8e44ad"   # modèle legacy
C_INS = "#e67e22"   # insitu

OUTPUT_BASE.mkdir(parents=True, exist_ok=True)

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

def align_insitu(dates, df_ins, w=7):
    wl        = np.full(len(dates), np.nan)
    ins_dates = np.array(df_ins["date"].values, dtype="datetime64[D]")
    ins_wl    = df_ins["wl"].values
    for i, d in enumerate(np.array(pd.to_datetime(dates), dtype="datetime64[D]")):
        diff = np.abs((ins_dates - d).astype(float))
        idx  = int(np.argmin(diff))
        if diff[idx] <= w:
            wl[i] = ins_wl[idx]
    return wl

def get_dates_vraies_legacy(sub, wl_ok, dates_ok):
    """Match les obs CSV legacy avec les vraies dates .nc par valeur."""
    start = 0
    for j, d in enumerate(dates_ok):
        if d >= sub["date"].iloc[0]:
            start = j
            break
    ptr = start
    dates_vraies = []
    for i in range(len(sub)):
        obs_val = sub["obs"].iloc[i]
        best_j, best_diff = ptr, np.inf
        for j in range(ptr, min(ptr + 5, len(wl_ok))):
            d = abs(wl_ok.iloc[j] - obs_val)
            if d < best_diff:
                best_diff = d
                best_j = j
        if best_diff < 0.001:
            dates_vraies.append(dates_ok.iloc[best_j])
            ptr = best_j + 1
        else:
            dates_vraies.append(pd.NaT)
    return dates_vraies

# ═══════════════════════════════════════════════════════════════
# CHARGEMENT
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

# Charger résidus DtoD
print("Chargement résidus DtoD...")
dtod_data = {}
for mask_pct, info in DTOD_MODELS.items():
    csv_path = RESIDUALS_DIR / info["csv"]
    if not csv_path.exists():
        print(f"  ⚠ {csv_path} introuvable — skip")
        continue
    df = pd.read_csv(csv_path)
    df["date"]    = pd.to_datetime(df["date"])
    df["station"] = df["station"].astype(str)
    df = df.dropna(subset=["obs", "pred"])
    dtod_data[mask_pct] = df
    print(f"  DtoD {mask_pct}% : {len(df)} lignes, {df['station'].nunique()} stations")

# Charger résidus legacy
print("Chargement résidus legacy...")
df_leg = None
if RESIDUALS_LEG.exists():
    df_leg = pd.read_csv(RESIDUALS_LEG)
    df_leg["date"]    = pd.to_datetime(df_leg["date"])
    df_leg["station"] = df_leg["station"].astype(str)
    df_leg = df_leg.dropna(subset=["obs", "pred"])
    print(f"  Legacy : {len(df_leg)} lignes, {df_leg['station'].nunique()} stations")

# Stations
stations = [s.strip() for s in STATIONS_TXT.read_text().split() if s.strip()]
print(f"\n{len(stations)} stations DAHITI 10j à traiter\n")

conn_da = sqlite3.connect(DAHITI_DB)

# ═══════════════════════════════════════════════════════════════
# GÉNÉRATION DES PLOTS
# ═══════════════════════════════════════════════════════════════
n_plots = 0
n_skip  = 0

for code in stations:
    # Coordonnées DAHITI
    for c in [str(code).zfill(13), code]:
        r = pd.read_sql(
            "SELECT reference_longitude AS lon, reference_latitude AS lat FROM stations WHERE station_code=?",
            conn_da, params=(c,)
        )
        if not r.empty:
            break
    if r.empty:
        n_skip += 1
        continue

    lon, lat = float(r.iloc[0]["lon"]), float(r.iloc[0]["lat"])
    code_ins, dist_km = get_insitu_proche(lon, lat)
    if dist_km > DIST_MAX_KM:
        n_skip += 1
        continue

    df_ins = get_insitu_series(code_ins)
    if df_ins is None:
        n_skip += 1
        continue

    # Récupérer obs DtoD (même pour tous les modèles)
    ref_mask = list(dtod_data.keys())[0]
    if ref_mask not in dtod_data:
        n_skip += 1
        continue

    sub_ref = dtod_data[ref_mask]
    sub_ref = sub_ref[sub_ref["station"] == code].sort_values("date").reset_index(drop=True)
    if len(sub_ref) < 5:
        n_skip += 1
        continue

    obs_dates = sub_ref["date"].values
    obs_z     = zscore(sub_ref["obs"].values)
    ins_wl    = align_insitu(obs_dates, df_ins)
    ins_z     = zscore(ins_wl)

    # Legacy — récupérer vraies dates via .nc
    leg_dates_vraies = None
    leg_pred_z       = None
    if df_leg is not None:
        sub_leg = df_leg[df_leg["station"] == code].sort_values("date").reset_index(drop=True)
        if len(sub_leg) >= 5:
            f_leg = list(NC_DIR_LEGACY.glob(f"*{code}*.nc"))
            if f_leg:
                ds = nc.Dataset(f_leg[0])
                dates_nc = pd.to_datetime("2016-01-01") + pd.to_timedelta(
                    ds.variables["date"][:], unit="D"
                )
                wl_nc = ds.variables["water_level"][:]
                ds.close()
                mask_nc  = ~np.isnan(wl_nc)
                dates_ok = pd.Series(dates_nc[mask_nc]).reset_index(drop=True)
                wl_ok    = pd.Series(wl_nc[mask_nc]).reset_index(drop=True)
                dv = get_dates_vraies_legacy(sub_leg, wl_ok, dates_ok)
                sub_leg["date_vraie"] = dv
                sub_ok = sub_leg.dropna(subset=["date_vraie"])
                if len(sub_ok) >= 5:
                    leg_dates_vraies = sub_ok["date_vraie"].values
                    leg_pred_z       = zscore(sub_ok["pred"].values)

    # Années disponibles
    years = sorted(pd.to_datetime(obs_dates).year.unique())
    out_dir = OUTPUT_BASE / code
    out_dir.mkdir(parents=True, exist_ok=True)

    for year in years:
        mask_year = pd.to_datetime(obs_dates).year == year
        if mask_year.sum() < 3:
            continue

        dates_y = obs_dates[mask_year]
        obs_y   = obs_z[mask_year]
        ins_y   = ins_z[mask_year]

        fig, axes = plt.subplots(3, 1, figsize=(14, 12), sharex=True)
        fig.suptitle(
            f"Station DAHITI {code} — {year}\n"
            f"Insitu : {code_ins} ({dist_km:.1f} km)",
            fontsize=11, fontweight="bold"
        )

        # ── Panel 1 : obs + 4 modèles DtoD ───────────────────
        ax1 = axes[0]
        ax1.plot(dates_y, obs_y, "o-", color=C_OBS, lw=2, ms=5,
                 label="Obs satellite", zorder=5)

        for mask_pct, info in DTOD_MODELS.items():
            if mask_pct not in dtod_data:
                continue
            sub_m  = dtod_data[mask_pct]
            sub_m  = sub_m[sub_m["station"] == code].sort_values("date")
            mask_y = pd.to_datetime(sub_m["date"]).dt.year == year
            sub_my = sub_m[mask_y]
            if len(sub_my) < 2:
                continue
            pred_y = zscore(sub_my["pred"].values)
            nse_v  = nse(obs_y[:len(pred_y)], pred_y) if len(pred_y) == len(obs_y) else np.nan
            nse_str = f"{nse_v:.3f}" if not np.isnan(nse_v) else "n/a"
            ax1.plot(sub_my["date"].values, pred_y, "^--",
                     color=info["color"], lw=1.2, ms=4,
                     label=f"{info['label']}  NSE={nse_str}", alpha=0.85)

        ax1.axhline(0, color="gray", lw=0.7, ls=":")
        ax1.set_ylabel("WL (z-score)", fontsize=9)
        ax1.set_title("[1] Obs satellite + modèles DtoD", fontsize=10, fontweight="bold")
        ax1.legend(fontsize=7, loc="upper right", ncol=2)
        ax1.grid(True, alpha=0.3)

        # ── Panel 2 : obs + modèle legacy ────────────────────
        ax2 = axes[1]
        ax2.plot(dates_y, obs_y, "o-", color=C_OBS, lw=2, ms=5,
                 label="Obs satellite", zorder=5)

        if leg_dates_vraies is not None:
            mask_leg_y = pd.to_datetime(leg_dates_vraies).year == year
            leg_d_y = leg_dates_vraies[mask_leg_y]
            leg_p_y = leg_pred_z[mask_leg_y]
            if len(leg_d_y) >= 2:
                nse_leg = nse(obs_y[:len(leg_p_y)], leg_p_y) if len(leg_p_y) == len(obs_y) else np.nan
                nse_str = f"{nse_leg:.3f}" if not np.isnan(nse_leg) else "n/a"
                ax2.plot(leg_d_y, leg_p_y, "s--", color=C_LEG, lw=1.5, ms=4,
                         label=f"Legacy 10j  NSE={nse_str}")

        ax2.axhline(0, color="gray", lw=0.7, ls=":")
        ax2.set_ylabel("WL (z-score)", fontsize=9)
        ax2.set_title("[2] Obs satellite + modèle legacy 10j", fontsize=10, fontweight="bold")
        ax2.legend(fontsize=8, loc="upper right")
        ax2.grid(True, alpha=0.3)

        # ── Panel 3 : obs vs insitu ───────────────────────────
        ax3 = axes[2]
        ax3.plot(dates_y, obs_y, "o-", color=C_OBS, lw=2, ms=5,
                 label="Obs satellite", zorder=5)
        ax3.plot(dates_y, ins_y, "^--", color=C_INS, lw=1.5, ms=4,
                 label=f"Insitu {code_ins}  NSE={nse(obs_y, ins_y):.3f}" if not np.isnan(nse(obs_y, ins_y)) else f"Insitu {code_ins}")

        ax3.axhline(0, color="gray", lw=0.7, ls=":")
        ax3.set_ylabel("WL (z-score)", fontsize=9)
        ax3.set_xlabel("Date", fontsize=9)
        ax3.set_title("[3] Obs satellite vs Insitu (référence)", fontsize=10, fontweight="bold")
        ax3.legend(fontsize=8, loc="upper right")
        ax3.grid(True, alpha=0.3)
        ax3.xaxis.set_major_formatter(mdates.DateFormatter("%b"))
        ax3.xaxis.set_major_locator(mdates.MonthLocator())
        plt.setp(ax3.xaxis.get_majorticklabels(), rotation=30, ha="right")

        plt.tight_layout()
        fig_path = out_dir / f"{code}_{year}.png"
        fig.savefig(fig_path, dpi=130, bbox_inches="tight")
        plt.close(fig)
        n_plots += 1

    if n_plots % 50 == 0 and n_plots > 0:
        print(f"  {n_plots} plots générés...")

conn_da.close()

print(f"\n{'='*55}")
print(f"  Plots générés : {n_plots}")
print(f"  Stations skip : {n_skip}")
print(f"  Dossier       : {OUTPUT_BASE}/")
print(f"{'='*55}")