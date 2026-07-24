"""
compare_nse_daily_vs_10j.py
════════════════════════════════════════════════════════════════════════
Compare deux façons de calculer le NSE du modèle vs insitu :

  [A] NSE "10j"      : modèle évalué uniquement aux dates satellite DAHITI
                        (ce qu'on calcule depuis le début)
  [B] NSE "quotidien" : modèle évalué tous les jours (pred complète,
                        y compris les jours "comblés" entre deux
                        passages satellite), comparé à l'insitu quotidien

Source des prédictions :
  ./data_processing/AnalyseModelsDtoD/residuals/residuals_dahiti_10j_{mask}pct.csv
  (généré par eval_zeroshot_dahiti_10j_DtoD.py, colonnes station/date/obs/pred,
   obs peut être NaN, pred couvre tous les jours après warm-up)

Les stations DAHITI 10j servent UNIQUEMENT à récupérer les coordonnées
pour trouver l'insitu le plus proche. La comparaison finale se fait
modèle (quotidien complet) vs insitu (quotidien), sans aucune référence
aux valeurs DAHITI elles-mêmes (seule la présence/absence d'obs DAHITI
sert à délimiter les dates "10j" vs "quotidien").

Sorties :
  ./data/outlier_detection/compare_daily_vs_10j/compare_nse_{mask}pct.csv
  Affichage comparatif dans le terminal

Usage :
    python compare_nse_daily_vs_10j.py
════════════════════════════════════════════════════════════════════════
"""

import sqlite3
import numpy as np
import pandas as pd
import geopandas as gpd
from pathlib import Path
from shapely.geometry import Point

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
STATIONS_TXT  = Path("./data/IA/NeuralHydrologyDahiti10jClean/stations_dahiti_10j.txt")
DAHITI_DB     = "./data/dahiti.db"
INSITU_DB     = "./data/insitu_data.db"
INSITU_SHP    = "./data/insitu/shp/station_schapi_alti_ref_2025_river.gpkg"

RESIDUALS_DIR = Path("./data_processing/AnalyseModelsDtoD/residuals")
OUTPUT_DIR    = Path("./data/outlier_detection/compare_daily_vs_10j")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

DATE_MIN    = "2016-01-01"
DATE_MAX    = "2025-12-31"
DIST_MAX_KM = 10.0
WINDOW_DAYS_10J = 7    # fenêtre alignement pour la comparaison "10j" (aux dates DAHITI)

MASKS = [80,90]   # masquages à comparer (doivent matcher les CSV dans RESIDUALS_DIR)

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
# CHARGEMENT INSITU + COORDS DAHITI (pour matching uniquement)
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
print(f"{len(stations_10j)} stations DAHITI 10j (pour matching insitu uniquement)\n")

# Pré-calcul : station DAHITI → (code_insitu, dist_km, df_insitu)
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
# TRAITEMENT PAR MODÈLE — lecture depuis les CSV résidus complets
# ═══════════════════════════════════════════════════════════════
all_results = []

for mask in MASKS:
    csv_in = RESIDUALS_DIR / f"residuals_dahiti_10j_{mask}pct.csv"

    print(f"\n{'='*65}")
    print(f"  Masquage {mask}%  —  {csv_in}")
    print(f"{'='*65}")

    if not csv_in.exists():
        print(f"  ⚠ Fichier introuvable → skip")
        continue

    df_model = pd.read_csv(csv_in)
    df_model["date"]    = pd.to_datetime(df_model["date"])
    df_model["station"] = df_model["station"].astype(str)

    rows = []

    for code, (code_ins, dist_km, df_ins) in station_to_insitu.items():
        sub = df_model[df_model["station"] == code].sort_values("date")
        if sub.empty:
            continue

        dates      = sub["date"].values
        obs_dahiti = sub["obs"].values    # NaN sauf aux vraies dates DAHITI
        pred_full  = sub["pred"].values   # quotidien complet (après warm-up)

        # ── [A] NSE "10j" — modèle vs insitu, restreint aux dates DAHITI ──
        mask_dahiti = ~np.isnan(obs_dahiti)
        if mask_dahiti.sum() < 5:
            nse_10j = np.nan
            n_10j   = 0
        else:
            dates_dahiti = dates[mask_dahiti]
            pred_dahiti  = pred_full[mask_dahiti]
            ins_10j      = align_insitu(dates_dahiti, df_ins, WINDOW_DAYS_10J)
            n_10j        = int(np.sum(~np.isnan(ins_10j)))
            nse_10j      = nse(zscore(ins_10j), zscore(pred_dahiti)) if n_10j >= 5 else np.nan

        # ── [B] NSE "quotidien" — modèle (tous les jours) vs insitu (tous les jours) ──
        mask_pred  = ~np.isnan(pred_full)
        dates_pred = dates[mask_pred]
        pred_pred  = pred_full[mask_pred]
        ins_daily  = align_insitu(dates_pred, df_ins, window_days=1)  # fenêtre stricte ±1j
        n_daily    = int(np.sum(~np.isnan(ins_daily)))
        nse_daily  = nse(zscore(ins_daily), zscore(pred_pred)) if n_daily >= 5 else np.nan

        rows.append({
            "station"        : code,
            "code_insitu"    : code_ins,
            "dist_insitu_km" : round(dist_km, 1),
            "mask_pct"       : mask,
            "n_pairs_10j"    : n_10j,
            "nse_10j"        : round(nse_10j, 3)   if not np.isnan(nse_10j)   else np.nan,
            "n_pairs_daily"  : n_daily,
            "nse_daily"      : round(nse_daily, 3) if not np.isnan(nse_daily) else np.nan,
        })

    df_res = pd.DataFrame(rows)
    csv_path = OUTPUT_DIR / f"compare_nse_{mask}pct.csv"
    df_res.to_csv(csv_path, index=False)

    print(f"\n  Stations comparées : {len(df_res)}")
    if len(df_res) > 0:
        v_10j   = df_res["nse_10j"].dropna()
        v_daily = df_res["nse_daily"].dropna()
        print(f"\n  {'':20} {'médiane':>9} {'moyenne':>9} {'> 0':>6} {'> 0.5':>6}")
        print(f"  {'-'*55}")
        print(f"  {'NSE 10j (réf.)':<20} {v_10j.median():>9.3f} {v_10j.mean():>9.3f} "
              f"{(v_10j>0).sum():>6} {(v_10j>0.5).sum():>6}")
        print(f"  {'NSE quotidien':<20} {v_daily.median():>9.3f} {v_daily.mean():>9.3f} "
              f"{(v_daily>0).sum():>6} {(v_daily>0.5).sum():>6}")
        print(f"\n  CSV → {csv_path}")

        all_results.append({
            "mask_pct"        : mask,
            "n"               : len(df_res),
            "nse_10j_med"     : v_10j.median(),
            "nse_10j_mean"    : v_10j.mean(),
            "nse_daily_med"   : v_daily.median(),
            "nse_daily_mean"  : v_daily.mean(),
        })

# ═══════════════════════════════════════════════════════════════
# RÉSUMÉ COMPARATIF FINAL
# ═══════════════════════════════════════════════════════════════
if all_results:
    df_summary = pd.DataFrame(all_results)
    summary_csv = OUTPUT_DIR / "summary_daily_vs_10j.csv"
    df_summary.to_csv(summary_csv, index=False)

    print(f"\n{'='*70}")
    print("  RÉSUMÉ FINAL — NSE quotidien vs NSE 10j (modèle vs insitu)")
    print(f"{'='*70}")
    print(f"  {'masquage':>10} {'n':>5} {'NSE10j med':>11} {'NSEdaily med':>13}"
          f" {'NSE10j moy':>11} {'NSEdaily moy':>13}")
    print(f"  {'-'*68}")
    for _, row in df_summary.iterrows():
        print(f"  {int(row['mask_pct']):>9}% {int(row['n']):>5} "
              f"{row['nse_10j_med']:>11.3f} {row['nse_daily_med']:>13.3f} "
              f"{row['nse_10j_mean']:>11.3f} {row['nse_daily_mean']:>13.3f}")
    print(f"\n  Résumé → {summary_csv}")
else:
    print("\nAucun résultat — vérifier que eval_zeroshot_dahiti_10j_DtoD.py a bien tourné.")