"""
plot_residuals_hwnext_DtoD.py
════════════════════════════════════════════════════════════════════════
Analyse des résidus zero-shot HW Next 10j pour un modèle DtoD donné.
Pour chaque station, génère 3 panels :
  [1] Modèle (pred) vs Station alti (obs)
  [2] Station alti (obs) vs Insitu
  [3] Modèle (pred) vs Insitu

Sorties :
  ./AI/LSTM/NeuralHydro/HydrowebNext/plots/{MODEL_NAME}/{station}/

Usage :
    Modifier MODEL_NAME et MASK_PCT en bas du script, puis :
    python plot_residuals_hwnext_DtoD.py
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
# PARAMÈTRES — modifier ici pour changer de modèle
# ═══════════════════════════════════════════════════════════════
MODEL_NAME    = "arlstm_DtoD50_1506_145950"
MASK_PCT      = 50   # pas de masquage pour cet ancien modèle                  # ← taux de masquage correspondant

RESIDUALS_DIR = Path("./data/outlier_detection/benchmark_DtoD_hwnext27j")
RESIDUALS_CSV = RESIDUALS_DIR / "residuals_hwnext_27j_50pct.csv"

HW_DB      = "./data/hydroweb_next.db"
INSITU_DB  = "./data/insitu_data.db"
INSITU_SHP = "./data/insitu/shp/station_schapi_alti_ref_2025_river.gpkg"

OUTPUT_BASE   = Path(f"./AI/LSTM/NeuralHydro/HydrowebNext/plot/{MODEL_NAME}")

DATE_MIN    = "2016-01-01"
DATE_MAX    = "2025-12-31"
DIST_MAX_KM = 50.0
WINDOW_DAYS = 7   # fenêtre alignement insitu ±7j pour 10j

# Couleurs
C_OBS = "#2980b9"   # observations satellite
C_PRD = "#c0392b"   # prédictions modèle
C_INS = "#e67e22"   # insitu

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

def align_insitu(dates, df_ins, window_days=7):
    wl        = np.full(len(dates), np.nan)
    ins_dates = np.array(df_ins["date"].values, dtype="datetime64[D]")
    ins_wl    = df_ins["wl"].values
    for i, d in enumerate(np.array(dates, dtype="datetime64[D]")):
        diff = np.abs((ins_dates - d).astype(float))
        idx  = int(np.argmin(diff))
        if diff[idx] <= window_days:
            wl[i] = ins_wl[idx]
    return wl

# ═══════════════════════════════════════════════════════════════
# CHARGEMENT SHAPEFILE INSITU
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

def get_coords_hw(conn_hw, code):
    for c in [code, str(code).zfill(13)]:
        df = pd.read_sql(
            "SELECT reference_longitude AS lon, reference_latitude AS lat "
            "FROM stations WHERE station_code = ?",
            conn_hw, params=(c,)
        )
        if not df.empty:
            return float(df.iloc[0]["lon"]), float(df.iloc[0]["lat"])
    return None, None

# ═══════════════════════════════════════════════════════════════
# CHARGEMENT RÉSIDUS
# ═══════════════════════════════════════════════════════════════
print(f"Chargement résidus : {RESIDUALS_CSV}")
if not RESIDUALS_CSV.exists():
    print(f"⚠ Fichier introuvable : {RESIDUALS_CSV}")
    print("  Lancer d'abord eval_zeroshot_hwnext_27j_DtoD.py")
    exit()

df_res = pd.read_csv(RESIDUALS_CSV)
df_res["date"]    = pd.to_datetime(df_res["date"])
df_res["station"] = df_res["station"].astype(str)

stations = sorted(df_res["station"].unique())
print(f"  {len(df_res)} lignes | {len(stations)} stations\n")

# ═══════════════════════════════════════════════════════════════
# GÉNÉRATION DES PLOTS — STATION PAR STATION
# ═══════════════════════════════════════════════════════════════
conn_hw = sqlite3.connect(HW_DB)
n_plots = 0
n_skip  = 0

for code in stations:
    sub = df_res[df_res["station"] == code].sort_values("date").reset_index(drop=True)
    if len(sub) < 5:
        n_skip += 1
        continue

    # Coordonnées + insitu
    lon, lat = get_coords_hw(conn_hw, code)
    if lon is None:
        print(f"  ⚠ {code} : coordonnées introuvables → skip")
        n_skip += 1
        continue

    code_ins, dist_km = get_insitu_proche(lon, lat)
    if dist_km > DIST_MAX_KM:
        n_skip += 1
        continue

    df_ins = get_insitu_series(code_ins)
    if df_ins is None:
        n_skip += 1
        continue

    # Alignement insitu
    ins_wl  = align_insitu(sub["date"].values, df_ins, WINDOW_DAYS)
    n_pairs = int(np.sum(~np.isnan(ins_wl)))
    if n_pairs < 5:
        n_skip += 1
        continue

    # Z-scores
    obs_z  = zscore(sub["obs"].values)
    pred_z = zscore(sub["pred"].values)
    ins_z  = zscore(ins_wl)

    # NSE
    nse_mod_obs = nse(pred_z, obs_z)
    nse_obs_ins = nse(obs_z,  ins_z)
    nse_mod_ins = nse(pred_z, ins_z)

    # Dossier de sortie
    out_dir = OUTPUT_BASE / code
    out_dir.mkdir(parents=True, exist_ok=True)

    dates = sub["date"].values

    # ── Figure 3 panels ───────────────────────────────────────
    fig, axes = plt.subplots(3, 1, figsize=(14, 12), sharex=True)
    fig.suptitle(
        f"Station HW Next {code}  —  Modèle {MODEL_NAME} ({MASK_PCT}% NaN)\n"
        f"Insitu : {code_ins} ({dist_km:.1f} km)  |  n={len(sub)} dates  |  n_insitu={n_pairs}",
        fontsize=11, fontweight="bold"
    )

    # Panel 1 — Modèle vs Station alti
    ax1 = axes[0]
    ax1.plot(dates, obs_z,  color=C_OBS, lw=1.8, marker="o", ms=4,
             label="Obs satellite (HW Next)")
    ax1.plot(dates, pred_z, color=C_PRD, lw=1.5, marker="^", ms=4, ls="--",
             label=f"Modèle (pred)  NSE={nse_mod_obs:.3f}")
    ax1.axhline(0, color="gray", lw=0.7, ls=":")
    ax1.set_ylabel("WL (z-score)", fontsize=9)
    ax1.set_title("[1] Modèle vs Station alti", fontsize=10, fontweight="bold")
    ax1.legend(fontsize=8, loc="upper right")
    ax1.grid(True, alpha=0.3)

    # Panel 2 — Station alti vs Insitu
    ax2 = axes[1]
    ax2.plot(dates, obs_z,  color=C_OBS, lw=1.8, marker="o", ms=4,
             label="Obs satellite (HW Next)")
    ax2.plot(dates, ins_z,  color=C_INS, lw=1.5, marker="^", ms=4, ls="--",
             label=f"Insitu {code_ins}  NSE={nse_obs_ins:.3f}")
    ax2.axhline(0, color="gray", lw=0.7, ls=":")
    ax2.set_ylabel("WL (z-score)", fontsize=9)
    ax2.set_title("[2] Station alti vs Insitu", fontsize=10, fontweight="bold")
    ax2.legend(fontsize=8, loc="upper right")
    ax2.grid(True, alpha=0.3)

    # Panel 3 — Modèle vs Insitu
    ax3 = axes[2]
    ax3.plot(dates, pred_z, color=C_PRD, lw=1.8, marker="o", ms=4,
             label="Modèle (pred)")
    ax3.plot(dates, ins_z,  color=C_INS, lw=1.5, marker="^", ms=4, ls="--",
             label=f"Insitu {code_ins}  NSE={nse_mod_ins:.3f}")
    ax3.axhline(0, color="gray", lw=0.7, ls=":")
    ax3.set_ylabel("WL (z-score)", fontsize=9)
    ax3.set_xlabel("Date", fontsize=9)
    ax3.set_title("[3] Modèle vs Insitu", fontsize=10, fontweight="bold")
    ax3.legend(fontsize=8, loc="upper right")
    ax3.grid(True, alpha=0.3)
    ax3.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    ax3.xaxis.set_major_locator(mdates.MonthLocator(interval=6))
    plt.setp(ax3.xaxis.get_majorticklabels(), rotation=30, ha="right")

    plt.tight_layout()
    fig_path = out_dir / f"{code}_{MODEL_NAME}.png"
    fig.savefig(fig_path, dpi=130, bbox_inches="tight")
    plt.close(fig)
    n_plots += 1

    if n_plots % 20 == 0:
        print(f"  {n_plots}/{len(stations)} plots générés...")

conn_hw.close()

print(f"\n{'='*55}")
print(f"  Plots générés : {n_plots}")
print(f"  Stations skip : {n_skip}")
print(f"  Dossier       : {OUTPUT_BASE}/")
print(f"{'='*55}")

# ═══════════════════════════════════════════════════════════════
# RÉSUMÉ GLOBAL NSE / KGE
# ═══════════════════════════════════════════════════════════════
from collections import defaultdict
summary = defaultdict(list)

conn_hw2 = sqlite3.connect(HW_DB)
for code in stations:
    sub = df_res[df_res["station"] == code].sort_values("date").reset_index(drop=True)
    if len(sub) < 5: continue
    lon, lat = get_coords_hw(conn_hw2, code)
    if lon is None: continue
    code_ins, dist_km = get_insitu_proche(lon, lat)
    if dist_km > DIST_MAX_KM: continue
    df_ins = get_insitu_series(code_ins)
    if df_ins is None: continue
    ins_wl = align_insitu(sub["date"].values, df_ins, WINDOW_DAYS)
    if np.sum(~np.isnan(ins_wl)) < 5: continue

    obs_z  = zscore(sub["obs"].values)
    pred_z = zscore(sub["pred"].values)
    ins_z  = zscore(ins_wl)

    def kge(o, s):
        mask = ~(np.isnan(o)|np.isnan(s))
        if mask.sum() < 5: return np.nan
        o, s = o[mask], s[mask]
        r = np.corrcoef(o, s)[0,1]
        a = s.std()/o.std() if o.std()>0 else np.nan
        b = s.mean()/o.mean() if o.mean()!=0 else np.nan
        return float(1 - np.sqrt((r-1)**2 + (a-1)**2 + (b-1)**2))

    for key, o, s in [
        ("mod_vs_obs", obs_z,  pred_z),
        ("obs_vs_ins", obs_z,  ins_z),
        ("mod_vs_ins", pred_z, ins_z),
    ]:
        summary[f"{key}_nse"].append(nse(o, s))
        summary[f"{key}_kge"].append(kge(o, s))

conn_hw2.close()

print(f"\n{'='*65}")
print(f"  RÉSUMÉ GLOBAL — {MODEL_NAME} ({MASK_PCT}% NaN)")
print(f"  {len([v for v in summary['mod_vs_obs_nse'] if not np.isnan(v)])} stations avec insitu")
print(f"{'='*65}")
print(f"  {'comparaison':<25} {'NSE med':>9} {'NSE moy':>9} {'KGE med':>9}")
print(f"  {'-'*55}")
for key, label in [
    ("mod_vs_obs", "Modèle vs alti (obs)  "),
    ("obs_vs_ins", "Alti (obs) vs insitu  "),
    ("mod_vs_ins", "Modèle vs insitu      "),
]:
    nse_v = np.array([v for v in summary[f"{key}_nse"] if not np.isnan(v)])
    kge_v = np.array([v for v in summary[f"{key}_kge"] if not np.isnan(v)])
    print(f"  {label:<25} {np.median(nse_v):>9.3f} {np.mean(nse_v):>9.3f} {np.median(kge_v):>9.3f}")


# ═══════════════════════════════════════════════════════════════
# POINT D'ENTRÉE — changer MODEL_NAME / MASK_PCT ici
# ═══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    pass