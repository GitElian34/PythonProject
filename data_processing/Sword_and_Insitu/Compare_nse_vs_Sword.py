"""
comparaison_nse_distance_vs_sword.py
════════════════════════════════════════════════════════════════════════
Compare, pour TOUTES les stations HW Next, le NSE (alti brute vs insitu)
obtenu avec 2 méthodes de sélection de l'insitu :

  Méthode "distance" : l'insitu le plus proche en distance, tout simplement
  Méthode "SWORD"     : parmi les candidats insitu dans un rayon donné
                        (triés par distance croissante), le PREMIER qui
                        est connecté au tronçon de la station alti dans
                        le réseau SWORD, SANS confluence intermédiaire.
                        Si aucun candidat ne passe ce filtre -> pas de
                        candidat SWORD pour cette station (NaN).

SWORD est chargé et le graphe construit UNE SEULE FOIS au début, puis
réutilisés pour toutes les stations (cf. sword_connectivity.py).

Sorties :
  ./data_processing/Sword_and_Insitu/comparaison_nse_distance_vs_sword.csv
════════════════════════════════════════════════════════════════════════
"""

import sqlite3
import sys
import pandas as pd
import numpy as np
import geopandas as gpd
from pathlib import Path
from shapely.geometry import Point

sys.path.insert(0, str(Path(__file__).parent))
from Sword_connectivity import load_sword_reaches, build_graph, check_connectivity

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
HWNEXT_DB  = "./data/hydroweb_next.db"
INSITU_DB  = "./data/insitu_data.db"
INSITU_SHP = "./data/insitu/shp/station_schapi_alti_ref_2025_river.gpkg"

DATE_MIN = "2016-01-01"
DATE_MAX = "2025-12-31"

DIST_MAX_KM = 50.0    # rayon de recherche des candidats insitu (comme benchmark_hwnext_final.py)
WINDOW_DAYS = 14
MIN_PAIRS   = 20

N_STATIONS = None     # None = toutes les stations, ou un entier pour limiter (tests rapides)

OUTPUT_CSV = Path("./data_processing/Sword_and_Insitu/comparaison_nse_distance_vs_sword.csv")
OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)

# ═══════════════════════════════════════════════════════════════
# 1. CHARGEMENT SWORD — UNE SEULE FOIS
# ═══════════════════════════════════════════════════════════════
print("### Chargement SWORD (une seule fois) ###")
gdf_sword, gdf_sword_proj = load_sword_reaches()
G, info = build_graph(gdf_sword)

# ═══════════════════════════════════════════════════════════════
# 2. CHARGEMENT STATIONS HW NEXT
# ═══════════════════════════════════════════════════════════════
print("\n### Chargement des stations HW Next ###")
conn = sqlite3.connect(HWNEXT_DB)
limit_clause = f"LIMIT {N_STATIONS}" if N_STATIONS else ""
df_stations = pd.read_sql(f"""
    SELECT station_code, reference_longitude AS lon, reference_latitude AS lat
    FROM stations
    WHERE reference_longitude IS NOT NULL AND reference_latitude IS NOT NULL
    {limit_clause}
""", conn)
conn.close()
print(f"  {len(df_stations)} stations chargées")

# ═══════════════════════════════════════════════════════════════
# 3. CHARGEMENT INSITU (shapefile -> géométrie + code_sta)
# ═══════════════════════════════════════════════════════════════
print("\n### Chargement des stations insitu (shapefile) ###")
gdf_insitu = gpd.read_file(INSITU_SHP).to_crs("EPSG:4326")
gdf_insitu_proj = gdf_insitu.to_crs("EPSG:2154")
print(f"  {len(gdf_insitu)} stations insitu chargées")

_cache_ins_series = {}

def get_insitu_series(code_sta):
    if code_sta not in _cache_ins_series:
        conn = sqlite3.connect(INSITU_DB)
        df = pd.read_sql("""
            SELECT date, h_med_wsh AS wl FROM mesures_insitu
            WHERE code_sta = ? AND date >= ? AND date <= ?
            ORDER BY date
        """, conn, params=(code_sta, DATE_MIN, DATE_MAX))
        conn.close()
        df["date"] = pd.to_datetime(df["date"])
        df = df.dropna(subset=["wl"])
        _cache_ins_series[code_sta] = df if len(df) >= 5 else None
    return _cache_ins_series[code_sta]

# ═══════════════════════════════════════════════════════════════
# HELPERS — NSE (identiques au reste du pipeline)
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

def get_alti_series(code_norm):
    conn = sqlite3.connect(HWNEXT_DB)
    df = pd.read_sql("""
        SELECT measure_date AS date, orthometric_height AS wl
        FROM measurements
        WHERE station_code = ? AND is_valid = 1
          AND measure_date >= ? AND measure_date <= ?
        ORDER BY measure_date
    """, conn, params=(code_norm, DATE_MIN, DATE_MAX))
    conn.close()
    df["date"] = pd.to_datetime(df["date"])
    return df.dropna(subset=["wl"])

def get_insitu_candidats(lon, lat, dist_max_km):
    pt = gpd.GeoSeries([Point(lon, lat)], crs="EPSG:4326").to_crs("EPSG:2154")[0]
    dist = gdf_insitu_proj.geometry.distance(pt) / 1000
    candidats = dist[dist <= dist_max_km].sort_values()
    return [(gdf_insitu_proj.loc[idx, "code_sta"], d,
              gdf_insitu.loc[idx, "geometry"].x, gdf_insitu.loc[idx, "geometry"].y)
            for idx, d in candidats.items()]

def compute_nse_for_pair(df_alti, lon_ins_code):
    """Calcule le NSE alti vs insitu pour un code_sta insitu donné."""
    df_ins = get_insitu_series(lon_ins_code)
    if df_ins is None:
        return np.nan, 0
    ins_wl = align_insitu(df_alti["date"].values, df_ins, WINDOW_DAYS)
    n_pairs = int(np.sum(~np.isnan(ins_wl)))
    if n_pairs < MIN_PAIRS:
        return np.nan, n_pairs
    obs_z = zscore(df_alti["wl"].values)
    ins_z = zscore(ins_wl)
    return nse(ins_z, obs_z), n_pairs

# ═══════════════════════════════════════════════════════════════
# 4. BOUCLE PRINCIPALE
# ═══════════════════════════════════════════════════════════════
print(f"\n### Comparaison sur {len(df_stations)} stations ###\n")
rows = []

for i, sta in df_stations.iterrows():
    code = sta["station_code"]
    lon_a, lat_a = sta["lon"], sta["lat"]

    df_alti = get_alti_series(code)
    if df_alti.empty:
        continue

    candidats = get_insitu_candidats(lon_a, lat_a, DIST_MAX_KM)
    if not candidats:
        continue

    # ── Méthode "distance" : le plus proche tout court ──────────
    code_nearest, dist_nearest, lon_n, lat_n = candidats[0]
    nse_nearest, n_pairs_nearest = compute_nse_for_pair(df_alti, code_nearest)

    # ── Méthode "SWORD" : premier candidat connecté SANS confluence ──
    code_sword, dist_sword, nse_sword, n_pairs_sword = None, np.nan, np.nan, 0
    for code_ins, dist_km, lon_b, lat_b in candidats:
        res = check_connectivity(lon_a, lat_a, lon_b, lat_b, G, info, gdf_sword_proj)
        if res["connected"] and not res["has_confluence"]:
            code_sword = code_ins
            dist_sword = dist_km
            nse_sword, n_pairs_sword = compute_nse_for_pair(df_alti, code_ins)
            break  # premier valide trouvé (le plus proche parmi les valides)

    rows.append({
        "station": code,
        "n_candidats": len(candidats),

        "insitu_nearest": code_nearest, "dist_nearest_km": round(dist_nearest, 2),
        "n_pairs_nearest": n_pairs_nearest, "nse_nearest": round(nse_nearest, 3) if not np.isnan(nse_nearest) else np.nan,

        "insitu_sword": code_sword, "dist_sword_km": round(dist_sword, 2) if not np.isnan(dist_sword) else np.nan,
        "n_pairs_sword": n_pairs_sword, "nse_sword": round(nse_sword, 3) if not np.isnan(nse_sword) else np.nan,

        "meme_insitu": code_nearest == code_sword,
        "sword_trouve": code_sword is not None,
    })

    if (i + 1) % 20 == 0:
        print(f"  ... {i+1}/{len(df_stations)} stations traitées")

df_res = pd.DataFrame(rows)
df_res.to_csv(OUTPUT_CSV, index=False)

# ═══════════════════════════════════════════════════════════════
# 5. RÉSUMÉ
# ═══════════════════════════════════════════════════════════════
print(f"\n{'='*80}")
print(f"  RÉSUMÉ — {len(df_res)} stations traitées")
print(f"{'='*80}")

n_sword_found = int(df_res["sword_trouve"].sum())
print(f"  Candidat SWORD trouvé (connecté, sans confluence) : {n_sword_found} / {len(df_res)} "
      f"({n_sword_found/len(df_res)*100:.1f}%)")

n_meme = int(df_res["meme_insitu"].sum())
print(f"  Même insitu choisi par les 2 méthodes : {n_meme} / {n_sword_found if n_sword_found else 1} "
      f"(parmi les cas où SWORD a trouvé un candidat)")

both_valid = df_res.dropna(subset=["nse_nearest", "nse_sword"])
print(f"\n  Stations avec NSE valide pour LES DEUX méthodes : {len(both_valid)}")

if len(both_valid) > 0:
    print(f"\n  {'':25} {'médiane':>9} {'moyenne':>9} {'>0':>5} {'>0.5':>6}")
    for col, label in [("nse_nearest", "NSE (plus proche)"), ("nse_sword", "NSE (SWORD)")]:
        v = both_valid[col]
        print(f"  {label:<25} {v.median():>9.3f} {v.mean():>9.3f} {(v>0).sum():>5} {(v>0.5).sum():>6}")

    gain = both_valid["nse_sword"] - both_valid["nse_nearest"]
    print(f"\n  Gain médian NSE (SWORD - plus proche) : {gain.median():+.3f}")
    print(f"  Stations où SWORD améliore le NSE     : {(gain > 0).sum()} / {len(both_valid)} "
          f"({(gain > 0).mean()*100:.1f}%)")
    print(f"  Stations où SWORD degrade le NSE      : {(gain < 0).sum()} / {len(both_valid)} "
          f"({(gain < 0).mean()*100:.1f}%)")
    print(f"  Stations à choix identique (gain=0)   : {(gain == 0).sum()} / {len(both_valid)}")

print(f"\n  CSV détaillé -> {OUTPUT_CSV}")