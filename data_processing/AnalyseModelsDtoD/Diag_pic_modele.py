"""
diagnostic_sauts_v2.py
════════════════════════════════════════════════════════════════════════
Étend diagnostic_sauts_apres_observation.py avec 2 analyses supplémentaires :

  (A) Corrélation, AU NIVEAU DE CHAQUE TRANSITION (pas la moyenne station),
      entre :
        écart = |pred[J] - obs[J]|        (erreur du modèle au moment où
                                            il reçoit la vraie observation)
        saut  = |pred[J+1] - pred[J]|      (amplitude de la correction)
      Hypothèse testée : plus le modèle s'était trompé, plus la correction
      qui suit est violente.

  (B) Corrélation ratio (après_obs / normal) <-> NSE(modèle quotidien, insitu)
      — au lieu de NSE(alti, modèle) comme dans la version précédente.
      L'insitu est sélectionné via la méthode SWORD (connectivité réseau).

Sources :
  - Résidus complets : ./data_processing/AnalyseModelsDtoD/residuals/residuals_dahiti_10j_{mask}pct.csv
  - Insitu / SWORD   : mêmes sources que top5_stations_alti_modele_insitu.py

Sorties (dans OUTPUT_DIR) :
  transitions_gap_vs_jump.csv   (détail niveau transition)
  diagnostic_sauts_par_station_v2.csv  (détail par station, avec NSE modèle-insitu)
  resume_v2.csv
  gap_vs_jump_scatter.png
════════════════════════════════════════════════════════════════════════
"""

import sqlite3
import sys
import numpy as np
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from pathlib import Path
from shapely.geometry import Point
from scipy.stats import pearsonr

# ── Import robuste de Sword_connectivity.py ──────────────────────────────
THIS_DIR = Path(__file__).resolve().parent
SWORD_MODULE_DIR = THIS_DIR.parent / "Sword_and_Insitu"
if not (SWORD_MODULE_DIR / "Sword_connectivity.py").exists():
    raise SystemExit(
        f"⚠ Sword_connectivity.py introuvable dans {SWORD_MODULE_DIR}\n"
        f"  Corrige SWORD_MODULE_DIR avec le bon chemin absolu."
    )
sys.path.insert(0, str(SWORD_MODULE_DIR))
from Sword_connectivity import load_sword_reaches, build_graph, check_connectivity

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
SOURCE = "dahiti"
FREQ   = "10j"
MASKS  = [80, 90, 96]

SAT_DB     = "./data/dahiti.db"
INSITU_DB  = "./data/insitu_data.db"
INSITU_SHP = "./data/insitu/shp/station_schapi_alti_ref_2025_river.gpkg"

RESIDUALS_DIR = Path("./data_processing/AnalyseModelsDtoD/residuals")
OUTPUT_DIR    = Path("./data_processing/AnalyseModelsDtoD/diagnostic_sauts")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

DATE_MIN, DATE_MAX = "2016-01-01", "2025-12-31"
DIST_MAX_KM = 50.0
FACC_MAX_RATIO = 2.0
WINDOW_MODEL_DAILY = 1

MIN_TRANSITIONS = 5
MIN_PAIRS_NSE = 30

COLORS = {80: "#2E7D32", 90: "#1565C0", 96: "#C0392B"}

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

def normalize_code(c):
    s = str(c)
    try:
        return str(int(s))
    except ValueError:
        return s

def get_coords_station(code):
    conn = sqlite3.connect(SAT_DB)
    for c in [str(code), str(code).zfill(13)]:
        df = pd.read_sql(
            "SELECT reference_longitude AS lon, reference_latitude AS lat FROM stations WHERE station_code = ?",
            conn, params=(c,)
        )
        if not df.empty:
            conn.close()
            return float(df.iloc[0]["lon"]), float(df.iloc[0]["lat"])
    conn.close()
    return None, None

# ═══════════════════════════════════════════════════════════════
# 1. SWORD + INSITU — chargés une seule fois
# ═══════════════════════════════════════════════════════════════
print("### Chargement SWORD ###")
gdf_sword, gdf_sword_proj = load_sword_reaches()
G_sword, info_sword = build_graph(gdf_sword)

print("### Chargement insitu ###")
gdf_insitu = gpd.read_file(INSITU_SHP).to_crs("EPSG:4326")
gdf_insitu_proj = gdf_insitu.to_crs("EPSG:2154")
_cache_ins = {}

def get_insitu_candidats(lon, lat, dist_max_km):
    pt = gpd.GeoSeries([Point(lon, lat)], crs="EPSG:4326").to_crs("EPSG:2154")[0]
    dist = gdf_insitu_proj.geometry.distance(pt) / 1000
    candidats = dist[dist <= dist_max_km].sort_values()
    return [(gdf_insitu_proj.loc[idx, "code_sta"], d,
              gdf_insitu.loc[idx, "geometry"].x, gdf_insitu.loc[idx, "geometry"].y)
            for idx, d in candidats.items()]

def get_insitu_sword(lon_a, lat_a):
    for code_ins, dist_km, lon_b, lat_b in get_insitu_candidats(lon_a, lat_a, DIST_MAX_KM):
        res = check_connectivity(lon_a, lat_a, lon_b, lat_b, G_sword, info_sword, gdf_sword_proj,
                                  facc_max_ratio=FACC_MAX_RATIO)
        if res["connected"] and not res["has_confluence"] and res["facc_ok"] is not False:
            return code_ins, dist_km
    return None, None

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

_cache_insitu_for_station = {}
def get_insitu_for_station(code):
    code_n = normalize_code(code)
    if code_n in _cache_insitu_for_station:
        return _cache_insitu_for_station[code_n]
    lon, lat = get_coords_station(code)
    if lon is None:
        _cache_insitu_for_station[code_n] = (None, None)
        return None, None
    code_ins, dist_km = get_insitu_sword(lon, lat)
    _cache_insitu_for_station[code_n] = (code_ins, dist_km)
    return code_ins, dist_km

# ═══════════════════════════════════════════════════════════════
# 2. CALCUL — TRANSITIONS (gap vs jump) + STATIONS (ratio + NSE modèle-insitu)
# ═══════════════════════════════════════════════════════════════
transition_rows = []
station_rows = []

for mask in MASKS:
    path = RESIDUALS_DIR / f"residuals_{SOURCE}_{FREQ}_{mask}pct.csv"
    if not path.exists():
        print(f"⚠ Manquant : {path} -> mask {mask} ignoré")
        continue

    df = pd.read_csv(path)
    df["date"]    = pd.to_datetime(df["date"])
    df["station"] = df["station"].astype(str)
    df = df.dropna(subset=["pred"]).sort_values(["station", "date"])
    print(f"Mask {mask}% : {df['station'].nunique()} stations")

    for code, sub in df.groupby("station"):
        sub = sub.sort_values("date").reset_index(drop=True)
        if len(sub) < 10:
            continue

        dates = sub["date"].values
        pred  = sub["pred"].values
        obs   = sub["obs"].values if "obs" in sub.columns else np.full(len(sub), np.nan)
        has_obs = ~np.isnan(obs)

        diffs = pred[1:] - pred[:-1]
        after_obs_mask = has_obs[:-1]
        normal_mask = ~after_obs_mask

        # ── (A) niveau transition : gap (J) vs jump (J->J+1), pour les après_obs ──
        idx_after = np.where(after_obs_mask)[0]
        for i in idx_after:
            gap_i = abs(pred[i] - obs[i])
            jump_i = abs(diffs[i])
            if not (np.isnan(gap_i) or np.isnan(jump_i)):
                transition_rows.append({
                    "mask": mask, "station": code, "date": dates[i],
                    "gap": gap_i, "jump": jump_i,
                })

        jumps_after = np.abs(diffs[after_obs_mask])
        jumps_normal = np.abs(diffs[normal_mask])
        if len(jumps_after) < MIN_TRANSITIONS or len(jumps_normal) < MIN_TRANSITIONS:
            continue
        mean_after, mean_normal = float(np.mean(jumps_after)), float(np.mean(jumps_normal))
        if mean_normal == 0:
            continue
        ratio = mean_after / mean_normal

        # ── (B) NSE modèle (quotidien) vs insitu, via sélection SWORD ──────────
        code_ins, dist_km = get_insitu_for_station(code)
        nse_modele_insitu = np.nan
        if code_ins is not None:
            df_ins = get_insitu_series(code_ins)
            if df_ins is not None:
                ins_wl = align_insitu(dates, df_ins, WINDOW_MODEL_DAILY)
                if int(np.sum(~np.isnan(ins_wl))) >= MIN_PAIRS_NSE:
                    nse_modele_insitu = nse(zscore(ins_wl), zscore(pred))

        station_rows.append({
            "mask": mask, "station": code, "insitu": code_ins,
            "dist_insitu_km": round(dist_km, 1) if dist_km is not None else np.nan,
            "ratio": round(ratio, 3),
            "nse_modele_insitu": round(nse_modele_insitu, 3) if not np.isnan(nse_modele_insitu) else np.nan,
        })

df_trans = pd.DataFrame(transition_rows)
df_sta   = pd.DataFrame(station_rows)

trans_csv = OUTPUT_DIR / "transitions_gap_vs_jump.csv"
sta_csv   = OUTPUT_DIR / "diagnostic_sauts_par_station_v2.csv"
df_trans.to_csv(trans_csv, index=False)
df_sta.to_csv(sta_csv, index=False)
print(f"\nTransitions -> {trans_csv}  ({len(df_trans)} lignes)")
print(f"Stations    -> {sta_csv}  ({len(df_sta)} lignes)")

# ═══════════════════════════════════════════════════════════════
# 3. RÉSUMÉ — (A) corrélation gap/jump par masque, (B) ratio vs NSE modèle-insitu
# ═══════════════════════════════════════════════════════════════
print(f"\n{'='*90}")
print("  (A) Corrélation ÉCART (erreur au moment de l'observation) <-> SAUT (correction)")
print(f"{'='*90}")

resume_rows = []
for mask in MASKS:
    sub = df_trans[df_trans["mask"] == mask]
    if len(sub) < 10:
        print(f"\n--- DtoD{mask}% : pas assez de transitions ---")
        continue
    r_gj, p_gj = pearsonr(sub["gap"], sub["jump"])
    print(f"\n--- DtoD{mask}% (n={len(sub)} transitions, {sub['station'].nunique()} stations) ---")
    print(f"  Corrélation gap <-> jump : r={r_gj:.3f}  (p={p_gj:.3g})")

    sub_sta = df_sta[df_sta["mask"] == mask].dropna(subset=["ratio", "nse_modele_insitu"])
    if len(sub_sta) >= 5:
        r_rn, p_rn = pearsonr(sub_sta["ratio"], sub_sta["nse_modele_insitu"])
        print(f"  Corrélation ratio <-> NSE(modèle, insitu) : r={r_rn:.3f}  (p={p_rn:.3g})  (n={len(sub_sta)})")
    else:
        r_rn, p_rn = np.nan, np.nan
        print(f"  Corrélation ratio <-> NSE(modèle, insitu) : n/a (seulement {len(sub_sta)} stations avec insitu valide)")

    resume_rows.append({
        "mask": mask, "n_transitions": len(sub), "n_stations_transitions": sub["station"].nunique(),
        "corr_gap_jump": round(r_gj, 3), "p_gap_jump": p_gj,
        "n_stations_insitu": len(sub_sta),
        "corr_ratio_nse_insitu": round(r_rn, 3) if not np.isnan(r_rn) else np.nan,
        "p_ratio_nse_insitu": round(p_rn, 3) if not np.isnan(p_rn) else np.nan,
    })

df_resume = pd.DataFrame(resume_rows)
resume_csv = OUTPUT_DIR / "resume_v2.csv"
df_resume.to_csv(resume_csv, index=False)
print(f"\nRésumé -> {resume_csv}")

# ═══════════════════════════════════════════════════════════════
# 4. FIGURE — scatter gap vs jump, un panel par masque
# ═══════════════════════════════════════════════════════════════
fig, axes = plt.subplots(1, len(MASKS), figsize=(5 * len(MASKS), 5), sharex=True, sharey=True)
if len(MASKS) == 1:
    axes = [axes]

for ax, mask in zip(axes, MASKS):
    sub = df_trans[df_trans["mask"] == mask]
    if sub.empty:
        continue
    ax.scatter(sub["gap"], sub["jump"], alpha=0.25, s=12, color=COLORS.get(mask, "grey"))
    if len(sub) >= 10:
        r, _ = pearsonr(sub["gap"], sub["jump"])
        zfit = np.polyfit(sub["gap"], sub["jump"], 1)
        xx = np.linspace(sub["gap"].min(), sub["gap"].max(), 50)
        ax.plot(xx, np.polyval(zfit, xx), color="black", lw=1.5, ls="--")
        ax.set_title(f"DtoD{mask}%  (r={r:.2f})", fontsize=11, fontweight="bold")
    ax.set_xlabel("Écart |pred[J] - obs[J]|")
    ax.grid(True, alpha=0.3)

axes[0].set_ylabel("Saut |pred[J+1] - pred[J]|")
fig.suptitle("Écart au moment de l'observation vs amplitude de la correction qui suit",
             fontsize=12, fontweight="bold")
plt.tight_layout()
fig_path = OUTPUT_DIR / "gap_vs_jump_scatter.png"
fig.savefig(fig_path, dpi=150, bbox_inches="tight")
plt.close(fig)
print(f"Figure -> {fig_path}")