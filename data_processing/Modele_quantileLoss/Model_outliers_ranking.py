"""
compute_outliers_ranking.py
════════════════════════════════════════════════════════════════════════
Pour les stations DAHITI 27j déjà évaluées (via eval_zeroshot_quantile_DtoD.py
+ résidus centralisés dans RESIDUALS_DIR), calcule pour chacun des 3 modèles
quantile (DtoD80/90/96) :

  - le nombre de points ALTIMÉTRIE (obs) tombant hors de l'intervalle
    [Q05, Q95] du modèle,
  - le nombre de points INSITU (le plus proche, connecté SWORD, recalé par
    décalage de médiane) tombant hors de ce même intervalle,

puis classe les 3 modèles séparément sur ces 2 critères, sur l'ensemble des
5 stations testées.

Un point "hors intervalle" = un outlier détecté par la quantile loss.
Plus un modèle en flag, plus son intervalle de confiance est étroit /
sensible (à mettre en regard de la calibration : un modèle bien calibré
devrait flaguer environ 10% des points, cf. intervalle Q05-Q95 nominal).

Sorties :
    ./data_processing/Modele_quantileLoss/outlier_ranking/{SOURCE}_{FREQ}/
        outliers_per_station.csv   (détail station x modèle x source)
        outliers_summary.csv       (total + classement par modèle)

Usage :
    python compute_outliers_ranking.py
    (ajuster SOURCE, FREQ, STATIONS ci-dessous avant de lancer)
════════════════════════════════════════════════════════════════════════
"""

import sqlite3
import sys
import numpy as np
import pandas as pd
import geopandas as gpd
from pathlib import Path
from shapely.geometry import Point

# ── Import robuste de Sword_connectivity.py ──────────────────────────────
THIS_DIR = Path(__file__).resolve().parent
SWORD_MODULE_DIR = THIS_DIR.parent / "Sword_and_Insitu"
if not (SWORD_MODULE_DIR / "Sword_connectivity.py").exists():
    raise SystemExit(
        f"⚠ Sword_connectivity.py introuvable dans {SWORD_MODULE_DIR}\n"
        f"  Corrige SWORD_MODULE_DIR avec le chemin absolu vers Sword_and_Insitu."
    )
sys.path.insert(0, str(SWORD_MODULE_DIR))
from Sword_connectivity import load_sword_reaches, build_graph, check_connectivity

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES GLOBAUX
# ═══════════════════════════════════════════════════════════════
SOURCE = "dahiti"   # "hwnext" ou "dahiti"
FREQ   = "27j"      # "10j" ou "27j"

# Stations à traiter. None = toutes les stations présentes dans les CSV de résidus.
STATIONS = None

MASKS = [80, 90, 96]
QUANTILE_COLS = ["q05", "q25", "q50", "q75", "q95"]

# Résidus complets produits par eval_zeroshot_quantile_DtoD.py (suffixe "_quantile")
RESIDUALS_DIR = Path("./data_processing/AnalyseModelsDtoD/residuals_quantile")

HW_DB      = "./data/hydroweb_next.db"
DAHITI_DB  = "./data/dahiti.db"
INSITU_DB  = "./data/insitu_data.db"
INSITU_SHP = "./data/insitu/shp/station_schapi_alti_ref_2025_river.gpkg"

SAT_DB = HW_DB if SOURCE == "hwnext" else DAHITI_DB

DATE_MIN    = "2016-01-01"
DATE_MAX    = "2025-12-31"
DIST_MAX_KM = 50.0
FACC_MAX_RATIO = 2.0
WINDOW_DAYS = 7 if FREQ == "10j" else 14
MIN_PAIRS   = 10

OUTPUT_DIR = Path(f"./data_processing/Modele_quantileLoss/outlier_ranking/{SOURCE}_{FREQ}")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def normalize_code(c):
    """Convention établie : toujours normaliser les codes station pour matcher DB <-> CSV."""
    return str(int(c))


# ═══════════════════════════════════════════════════════════════
# HELPERS — reconstruction en mètres (std/mean alti depuis la DB)
# ═══════════════════════════════════════════════════════════════
_cache_alti_stats = {}

def get_alti_stats(station_code):
    code_n = normalize_code(station_code)
    if code_n in _cache_alti_stats:
        return _cache_alti_stats[code_n]

    conn = sqlite3.connect(SAT_DB)
    df = None
    for c in [code_n, code_n.zfill(13)]:
        tmp = pd.read_sql(
            """
            SELECT m.orthometric_height AS h
            FROM measurements m
            JOIN stations s ON s.station_code = m.station_code
            WHERE s.station_code = ? AND m.is_valid = 1
            """,
            conn, params=(c,)
        )
        if not tmp.empty:
            df = tmp
            break
    conn.close()

    if df is None or df.empty:
        _cache_alti_stats[code_n] = (None, None)
        return None, None

    mean_alti = float(df["h"].mean())
    std_alti  = float(df["h"].std())
    _cache_alti_stats[code_n] = (mean_alti, std_alti)
    return mean_alti, std_alti


def to_meters(arr_zscore, mean_alti, std_alti):
    return np.asarray(arr_zscore, dtype=float) * std_alti + mean_alti


# ═══════════════════════════════════════════════════════════════
# HELPERS — insitu via connectivité SWORD
# ═══════════════════════════════════════════════════════════════
print("Chargement SWORD...")
gdf_sword, gdf_sword_proj = load_sword_reaches()
G_sword, info_sword = build_graph(gdf_sword)

print("Chargement shapefile insitu...")
gdf_insitu = gpd.read_file(INSITU_SHP).to_crs("EPSG:4326")
gdf_insitu_proj = gdf_insitu.to_crs("EPSG:2154")
_cache_ins = {}
_cache_coords = {}

def get_coords(station_code):
    code_n = normalize_code(station_code)
    if code_n in _cache_coords:
        return _cache_coords[code_n]
    conn = sqlite3.connect(SAT_DB)
    for c in [code_n, code_n.zfill(13)]:
        df = pd.read_sql(
            "SELECT reference_longitude AS lon, reference_latitude AS lat FROM stations WHERE station_code = ?",
            conn, params=(c,)
        )
        if not df.empty:
            conn.close()
            _cache_coords[code_n] = (float(df.iloc[0]["lon"]), float(df.iloc[0]["lat"]))
            return _cache_coords[code_n]
    conn.close()
    _cache_coords[code_n] = (None, None)
    return None, None


def get_insitu_candidats(lon, lat, dist_max_km):
    pt = gpd.GeoSeries([Point(lon, lat)], crs="EPSG:4326").to_crs("EPSG:2154")[0]
    dist = gdf_insitu_proj.geometry.distance(pt) / 1000
    candidats = dist[dist <= dist_max_km].sort_values()
    return [(gdf_insitu_proj.loc[idx, "code_sta"], d) for idx, d in candidats.items()]


def get_insitu_sword(lon_a, lat_a):
    for code_ins, dist_km in get_insitu_candidats(lon_a, lat_a, DIST_MAX_KM):
        idx = gdf_insitu_proj[gdf_insitu_proj["code_sta"] == code_ins].index[0]
        lon_b, lat_b = gdf_insitu.loc[idx, "geometry"].x, gdf_insitu.loc[idx, "geometry"].y
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


def recale_insitu_par_mediane(insitu_wl, alti_metres):
    """Décalage de médiane uniquement (neutralise le datum, préserve l'amplitude).
    Retourne (None, nan, n_pairs) si pas assez de paires."""
    mask = ~(np.isnan(insitu_wl) | np.isnan(alti_metres))
    n_pairs = int(mask.sum())
    if n_pairs < MIN_PAIRS:
        return None, np.nan, n_pairs
    shift = np.nanmedian(alti_metres[mask]) - np.nanmedian(insitu_wl[mask])
    return insitu_wl + shift, shift, n_pairs


def flag_outliers(values, q05, q95):
    """Masque bool : True si value hors [q05, q95] (points non-NaN uniquement)."""
    valid = ~np.isnan(values)
    out = np.zeros(len(values), dtype=bool)
    out[valid] = (values[valid] < q05[valid]) | (values[valid] > q95[valid])
    return out


# ═══════════════════════════════════════════════════════════════
# CHARGEMENT DES RÉSIDUS + LISTE DE STATIONS
# ═══════════════════════════════════════════════════════════════
def load_residuals(mask):
    csv_path = RESIDUALS_DIR / f"residuals_{SOURCE}_{FREQ}_{mask}pct_quantile.csv"
    if not csv_path.exists():
        print(f"⚠ Fichier introuvable : {csv_path}")
        return None
    df = pd.read_csv(csv_path)
    df["station"] = df["station"].astype(str)
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values("date")


dfs_by_mask = {mask: load_residuals(mask) for mask in MASKS}
dfs_by_mask = {m: df for m, df in dfs_by_mask.items() if df is not None}

if not dfs_by_mask:
    raise SystemExit("Aucun fichier de résidus trouvé — vérifier RESIDUALS_DIR / SOURCE / FREQ.")

if STATIONS is None:
    stations_set = set()
    for df in dfs_by_mask.values():
        stations_set |= set(df["station"].unique())
    stations_list = sorted(stations_set)
else:
    stations_list = [str(s) for s in STATIONS]

print(f"Stations à traiter ({SOURCE} {FREQ}) : {stations_list}\n")

# ═══════════════════════════════════════════════════════════════
# CALCUL DES OUTLIERS — station x modèle x source(alti/insitu)
# ═══════════════════════════════════════════════════════════════
rows = []

for station in stations_list:
    print(f"Station {station}")

    mean_alti, std_alti = get_alti_stats(station)
    if mean_alti is None or std_alti is None or std_alti == 0:
        print("  ⚠ Stats alti (mean/std) introuvables ou std=0 — station ignorée")
        continue

    lon, lat = get_coords(station)
    insitu_code, dist_km = None, np.nan
    if lon is not None and lat is not None:
        insitu_code, dist_km = get_insitu_sword(lon, lat)
        if insitu_code is None:
            print(f"  ⚠ Aucun insitu connecté (SWORD, <= {DIST_MAX_KM} km) — insitu ignoré pour cette station")
        else:
            print(f"  Insitu connecté (SWORD) : {insitu_code}  (distance = {dist_km:.2f} km)")
            if get_insitu_series(insitu_code) is None:
                print(f"  ⚠ Série insitu {insitu_code} trop courte — insitu ignoré")
                insitu_code = None
    else:
        print("  ⚠ Coordonnées station introuvables en DB")

    for mask, df_all in dfs_by_mask.items():
        df_station = df_all[df_all["station"] == station].copy()
        if df_station.empty:
            print(f"  ⚠ DtoD{mask} : aucune donnée pour cette station")
            continue

        missing_cols = [c for c in QUANTILE_COLS if c not in df_station.columns]
        if missing_cols:
            print(f"  ⚠ DtoD{mask} : colonnes quantile manquantes {missing_cols} — ignoré")
            continue

        q05_m = to_meters(df_station["q05"].values, mean_alti, std_alti)
        q95_m = to_meters(df_station["q95"].values, mean_alti, std_alti)

        # ── Altimétrie ──
        obs_m = to_meters(df_station["obs"].values, mean_alti, std_alti)
        obs_mask = ~np.isnan(obs_m)
        obs_outlier = flag_outliers(obs_m, q05_m, q95_m)
        n_obs = int(obs_mask.sum())
        n_obs_out = int((obs_outlier & obs_mask).sum())

        rows.append({
            "station": station, "model": f"DtoD{mask}", "mask_pct": mask,
            "source": "alti", "n_total": n_obs, "n_outliers": n_obs_out,
            "pct_outliers": round(100 * n_obs_out / n_obs, 2) if n_obs > 0 else np.nan,
        })

        # ── Insitu ──
        if insitu_code is not None:
            df_ins = get_insitu_series(insitu_code)
            ins_wl = align_insitu(df_station["date"].values, df_ins, WINDOW_DAYS)
            ins_metres, shift, n_pairs = recale_insitu_par_mediane(ins_wl, obs_m)
            if ins_metres is not None:
                ins_mask = ~np.isnan(ins_metres)
                ins_outlier = flag_outliers(ins_metres, q05_m, q95_m)
                n_ins = int(ins_mask.sum())
                n_ins_out = int((ins_outlier & ins_mask).sum())

                rows.append({
                    "station": station, "model": f"DtoD{mask}", "mask_pct": mask,
                    "source": "insitu", "n_total": n_ins, "n_outliers": n_ins_out,
                    "pct_outliers": round(100 * n_ins_out / n_ins, 2) if n_ins > 0 else np.nan,
                })

        print(f"  DtoD{mask} : alti {n_obs_out}/{n_obs} outliers"
              + (f" | insitu {n_ins_out}/{n_ins} outliers" if insitu_code is not None and ins_metres is not None else " | insitu n/a"))

if not rows:
    raise SystemExit("Aucun résultat calculé — vérifier les CSV de résidus et les stations.")

df_detail = pd.DataFrame(rows)
detail_csv = OUTPUT_DIR / "outliers_per_station.csv"
df_detail.to_csv(detail_csv, index=False)
print(f"\nDétail par station -> {detail_csv}")

# ═══════════════════════════════════════════════════════════════
# CLASSEMENT — total outliers par modèle, séparé alti / insitu
# ═══════════════════════════════════════════════════════════════
def build_summary(source_label):
    sub = df_detail[df_detail["source"] == source_label]
    if sub.empty:
        print(f"\n⚠ Aucune donnée pour source='{source_label}'")
        return None

    summary = sub.groupby("model").agg(
        n_stations=("station", "nunique"),
        n_total=("n_total", "sum"),
        n_outliers=("n_outliers", "sum"),
    ).reset_index()
    summary["pct_outliers"] = (100 * summary["n_outliers"] / summary["n_total"]).round(2)
    summary = summary.sort_values("n_outliers", ascending=False).reset_index(drop=True)
    summary["rang"] = range(1, len(summary) + 1)
    return summary


summary_alti = build_summary("alti")
summary_insitu = build_summary("insitu")

print(f"\n{'='*70}")
print(f"  CLASSEMENT — OUTLIERS DÉTECTÉS SUR ALTIMÉTRIE  [{SOURCE.upper()} {FREQ}]")
print(f"{'='*70}")
if summary_alti is not None:
    print(f"  {'rang':>4} {'modèle':>8} {'stations':>9} {'n_total':>9} {'n_outliers':>11} {'%':>7}")
    for _, r in summary_alti.iterrows():
        print(f"  {int(r['rang']):>4} {r['model']:>8} {int(r['n_stations']):>9} "
              f"{int(r['n_total']):>9} {int(r['n_outliers']):>11} {r['pct_outliers']:>6.2f}%")
    print(f"  => Modèle qui flag le plus d'outliers sur alti : {summary_alti.iloc[0]['model']}")

print(f"\n{'='*70}")
print(f"  CLASSEMENT — OUTLIERS DÉTECTÉS SUR INSITU  [{SOURCE.upper()} {FREQ}]")
print(f"{'='*70}")
if summary_insitu is not None:
    print(f"  {'rang':>4} {'modèle':>8} {'stations':>9} {'n_total':>9} {'n_outliers':>11} {'%':>7}")
    for _, r in summary_insitu.iterrows():
        print(f"  {int(r['rang']):>4} {r['model']:>8} {int(r['n_stations']):>9} "
              f"{int(r['n_total']):>9} {int(r['n_outliers']):>11} {r['pct_outliers']:>6.2f}%")
    print(f"  => Modèle qui flag le plus d'outliers sur insitu : {summary_insitu.iloc[0]['model']}")

# ── Sauvegarde résumé combiné ──
frames = []
if summary_alti is not None:
    summary_alti_out = summary_alti.copy()
    summary_alti_out["source"] = "alti"
    frames.append(summary_alti_out)
if summary_insitu is not None:
    summary_insitu_out = summary_insitu.copy()
    summary_insitu_out["source"] = "insitu"
    frames.append(summary_insitu_out)

if frames:
    df_summary = pd.concat(frames, ignore_index=True)
    summary_csv = OUTPUT_DIR / "outliers_summary.csv"
    df_summary.to_csv(summary_csv, index=False)
    print(f"\nRésumé + classement -> {summary_csv}")

print(f"\nNote : un modèle bien calibré devrait flaguer environ 10% des points "
      f"(intervalle nominal Q05-Q95). Un % très supérieur suggère un intervalle "
      f"trop étroit (mauvaise calibration), pas forcément plus d'outliers réels.")
print("\nDone")