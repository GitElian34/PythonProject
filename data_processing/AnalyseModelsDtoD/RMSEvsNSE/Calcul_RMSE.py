"""
compute_nrmse_meters.py
════════════════════════════════════════════════════════════════════════
Calcule un NRMSE réellement indépendant du NSE/KGE, sur les valeurs
reconstruites en MÈTRES (pas en z-score) :

    NRMSE_amplitude (%) = RMSE_metres / (P90 - P10 de l'insitu) x 100
    NRMSE_moyenne   (%) = RMSE_metres / moyenne(insitu)          x 100   (= CV-RMSE)

Contrairement au RMSE en z-score (qui est algébriquement lié au NSE via
NRMSE ≈ √(1-NSE), donc redondant), ces deux normalisations apportent une
vraie information supplémentaire : "l'erreur représente X% de l'amplitude
typique de la station" — indépendant de NSE/KGE.

Reprend les briques déjà écrites dans les scripts précédents :
  - reconstruction en mètres (pred_metres = pred_zscore*std_alti+mean_alti)
  - insitu le plus proche connecté SWORD (DIST_MAX_KM, sans confluence)
  - recalage insitu par décalage de médiane (neutralise le datum)

Calculé pour :
  - Modèle vs Insitu (reconstruit en mètres)
  - Alti brute vs Insitu (baseline)

Usage :
    python compute_nrmse_meters.py
    (ajuster SOURCE, FREQ, MASKS, LOSS_SUFFIX_MAP ci-dessous)
════════════════════════════════════════════════════════════════════════
"""

import sqlite3
import sys
import numpy as np
import pandas as pd
import geopandas as gpd
from pathlib import Path
from shapely.geometry import Point

# ── Import robuste de Sword_connectivity.py (recherche en remontant l'arborescence,
# peu importe la profondeur à laquelle ce script est placé) ──────────────────────
THIS_DIR = Path(__file__).resolve().parent
SWORD_MODULE_DIR = None
for ancestor in [THIS_DIR] + list(THIS_DIR.parents):
    candidate = ancestor / "Sword_and_Insitu"
    if (candidate / "Sword_connectivity.py").exists():
        SWORD_MODULE_DIR = candidate
        break

if SWORD_MODULE_DIR is None:
    raise SystemExit(
        "⚠ Sword_connectivity.py introuvable en remontant depuis "
        f"{THIS_DIR} — vérifie qu'un dossier 'Sword_and_Insitu' existe bien "
        "quelque part au-dessus de ce script, ou corrige SWORD_MODULE_DIR manuellement."
    )
sys.path.insert(0, str(SWORD_MODULE_DIR))
from Sword_connectivity import load_sword_reaches, build_graph, check_connectivity

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
SOURCE = "dahiti"   # "hwnext" ou "dahiti"
FREQ   = "27j"      # "10j" ou "27j"
MASKS  = [80, 90, 96]
LOSS_SUFFIX_MAP = {"NSE": "", "RMSE": "_RMSE"}   # les 6 modèles (classiques, non quantile)

HW_DB      = "./data/hydroweb_next.db"
DAHITI_DB  = "./data/dahiti.db"
INSITU_DB  = "./data/insitu_data.db"
INSITU_SHP = "./data/insitu/shp/station_schapi_alti_ref_2025_river.gpkg"

SAT_DB = HW_DB if SOURCE == "hwnext" else DAHITI_DB
RESIDUALS_BENCH_DIR = Path("./data/outlier_detection")   # dossiers benchmark_DtoD_{source}{freq}[_RMSE]

DATE_MIN, DATE_MAX = "2016-01-01", "2025-12-31"
DIST_MAX_KM = 50.0
FACC_MAX_RATIO = 2.0
WINDOW_DAYS = 7 if FREQ == "10j" else 14
MIN_PAIRS = 10

OUTPUT_DIR = Path(f"./data_processing/AnalyseModelsDtoD/nse_vs_rmse_{SOURCE}_{FREQ}")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def normalize_code(c):
    return str(int(c))


# ═══════════════════════════════════════════════════════════════
# HELPERS — reconstruction en mètres
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
            """SELECT m.orthometric_height AS h FROM measurements m
               JOIN stations s ON s.station_code = m.station_code
               WHERE s.station_code = ? AND m.is_valid = 1""",
            conn, params=(c,)
        )
        if not tmp.empty:
            df = tmp
            break
    conn.close()
    result = (float(df["h"].mean()), float(df["h"].std())) if df is not None and not df.empty else (None, None)
    _cache_alti_stats[code_n] = result
    return result


def to_meters(z, mean_alti, std_alti):
    return np.asarray(z, dtype=float) * std_alti + mean_alti


# ═══════════════════════════════════════════════════════════════
# HELPERS — insitu via connectivité SWORD
# ═══════════════════════════════════════════════════════════════
print("Chargement SWORD...")
gdf_sword, gdf_sword_proj = load_sword_reaches()
G_sword, info_sword = build_graph(gdf_sword)

print("Chargement shapefile insitu...")
gdf_insitu = gpd.read_file(INSITU_SHP).to_crs("EPSG:4326")
gdf_insitu_proj = gdf_insitu.to_crs("EPSG:2154")
_cache_ins, _cache_coords = {}, {}

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


def get_insitu_sword(lon_a, lat_a):
    pt = gpd.GeoSeries([Point(lon_a, lat_a)], crs="EPSG:4326").to_crs("EPSG:2154")[0]
    dist = gdf_insitu_proj.geometry.distance(pt) / 1000
    candidats = dist[dist <= DIST_MAX_KM].sort_values()
    for idx, dist_km in candidats.items():
        code_ins = gdf_insitu_proj.loc[idx, "code_sta"]
        lon_b, lat_b = gdf_insitu.loc[idx, "geometry"].x, gdf_insitu.loc[idx, "geometry"].y
        res = check_connectivity(lon_a, lat_a, lon_b, lat_b, G_sword, info_sword, gdf_sword_proj,
                                  facc_max_ratio=FACC_MAX_RATIO)
        if res["connected"] and not res["has_confluence"] and res["facc_ok"] is not False:
            return code_ins, dist_km
    return None, None


def get_insitu_series(code_sta):
    if code_sta not in _cache_ins:
        conn = sqlite3.connect(INSITU_DB)
        df = pd.read_sql(
            "SELECT date, h_med_wsh AS wl FROM mesures_insitu WHERE code_sta = ? AND date >= ? AND date <= ? ORDER BY date",
            conn, params=(code_sta, DATE_MIN, DATE_MAX)
        )
        conn.close()
        df["date"] = pd.to_datetime(df["date"])
        df = df.dropna(subset=["wl"])
        _cache_ins[code_sta] = df if len(df) >= 5 else None
    return _cache_ins[code_sta]


def align_insitu(dates, df_ins, window_days):
    wl = np.full(len(dates), np.nan)
    id_ = np.array(df_ins["date"].values, dtype="datetime64[D]")
    iv = df_ins["wl"].values
    for i, d in enumerate(np.array(dates, dtype="datetime64[D]")):
        diff = np.abs((id_ - d).astype(float))
        idx = int(np.argmin(diff))
        if diff[idx] <= window_days:
            wl[i] = iv[idx]
    return wl


def rmse(a, b):
    m = ~(np.isnan(a) | np.isnan(b))
    if m.sum() < MIN_PAIRS:
        return np.nan, int(m.sum())
    return float(np.sqrt(np.mean((a[m] - b[m]) ** 2))), int(m.sum())


# ═══════════════════════════════════════════════════════════════
# CONSTRUCTION DES 6 MODÈLES (résidus classiques, comme analyse_residus_...)
# ═══════════════════════════════════════════════════════════════
MODEL_SOURCES = {}
for loss_type, suffix in LOSS_SUFFIX_MAP.items():
    for mask in MASKS:
        label = f"DtoD{mask}_{loss_type}"
        csv_path = RESIDUALS_BENCH_DIR / f"benchmark_DtoD_{SOURCE}{FREQ}{suffix}" / f"residuals_{SOURCE}_{FREQ}_{mask}pct.csv"
        MODEL_SOURCES[label] = {"csv": csv_path, "loss_type": loss_type, "mask_pct": mask}

print(f"\nSOURCE={SOURCE}  FREQ={FREQ}")
for label, info in MODEL_SOURCES.items():
    exist = "✓" if info["csv"].exists() else "✗ manquant"
    print(f"  {label:<14} {info['csv']}  [{exist}]")

# ═══════════════════════════════════════════════════════════════
# CALCUL — pour chaque modèle, chaque station
# ═══════════════════════════════════════════════════════════════
rows = []
baseline_cache = {}   # {station: dict avec RMSE_alti_insitu_m, amplitude, moyenne, insitu_code, dist_km, shift, n}

for model_label, info in MODEL_SOURCES.items():
    csv_path = info["csv"]
    if not csv_path.exists():
        print(f"⚠ {model_label} : fichier introuvable -> ignoré")
        continue

    df = pd.read_csv(csv_path)
    df["station"] = df["station"].astype(str)
    df["date"] = pd.to_datetime(df["date"])
    df = df.dropna(subset=["obs", "pred"])

    print(f"\n{model_label} : {df['station'].nunique()} stations")

    for station, sub in df.groupby("station"):
        sub = sub.sort_values("date")
        mean_alti, std_alti = get_alti_stats(station)
        if mean_alti is None or std_alti is None or std_alti == 0:
            continue

        pred_m = to_meters(sub["pred"].values, mean_alti, std_alti)
        obs_m = to_meters(sub["obs"].values, mean_alti, std_alti)

        # ── Insitu (calculé une seule fois par station, mis en cache) ──
        if station not in baseline_cache:
            lon, lat = get_coords(station)
            insitu_code, dist_km = (None, np.nan)
            if lon is not None:
                insitu_code, dist_km = get_insitu_sword(lon, lat)

            if insitu_code is None:
                baseline_cache[station] = None
            else:
                df_ins = get_insitu_series(insitu_code)
                if df_ins is None:
                    baseline_cache[station] = None
                else:
                    ins_wl_alti_dates = align_insitu(sub["date"].values, df_ins, WINDOW_DAYS)
                    mask_pair = ~(np.isnan(ins_wl_alti_dates) | np.isnan(obs_m))
                    if mask_pair.sum() < MIN_PAIRS:
                        baseline_cache[station] = None
                    else:
                        shift = np.nanmedian(obs_m[mask_pair]) - np.nanmedian(ins_wl_alti_dates[mask_pair])
                        ins_recale = ins_wl_alti_dates + shift
                        rmse_alti, n_alti = rmse(obs_m, ins_recale)
                        amplitude = float(np.nanpercentile(ins_recale, 90) - np.nanpercentile(ins_recale, 10))
                        moyenne = float(np.nanmean(ins_recale))
                        baseline_cache[station] = {
                            "insitu_code": insitu_code, "dist_km": round(dist_km, 1), "shift": shift,
                            "rmse_alti_insitu_m": rmse_alti, "n_alti_insitu": n_alti,
                            "amplitude": amplitude, "moyenne": moyenne,
                        }

        base = baseline_cache.get(station)
        if base is None:
            continue

        # ── Modèle vs Insitu (même recalage/shift que pour alti, cohérent avec le datum) ──
        df_ins = get_insitu_series(base["insitu_code"])
        ins_wl_model_dates = align_insitu(sub["date"].values, df_ins, WINDOW_DAYS)
        ins_recale_model = ins_wl_model_dates + base["shift"]
        rmse_modele, n_modele = rmse(pred_m, ins_recale_model)

        if np.isnan(rmse_modele) or base["amplitude"] <= 0 or base["moyenne"] == 0:
            continue

        rows.append({
            "station": station, "model": model_label,
            "mask_pct": info["mask_pct"], "loss_type": info["loss_type"],
            "insitu_code": base["insitu_code"], "dist_insitu_km": base["dist_km"],
            "n_pairs_modele": n_modele, "n_pairs_alti": base["n_alti_insitu"],
            "amplitude_insitu_m": round(base["amplitude"], 3),
            "moyenne_insitu_m": round(base["moyenne"], 3),
            "RMSE_modele_insitu_m": round(rmse_modele, 4),
            "RMSE_alti_insitu_m": round(base["rmse_alti_insitu_m"], 4),
            "NRMSE_amp_modele_pct": round(100 * rmse_modele / base["amplitude"], 2),
            "NRMSE_amp_alti_pct": round(100 * base["rmse_alti_insitu_m"] / base["amplitude"], 2),
            "NRMSE_mean_modele_pct": round(100 * rmse_modele / abs(base["moyenne"]), 2),
            "NRMSE_mean_alti_pct": round(100 * base["rmse_alti_insitu_m"] / abs(base["moyenne"]), 2),
        })

if not rows:
    raise SystemExit("Aucune ligne calculée — vérifier les chemins de résidus / SOURCE / FREQ.")

df_out = pd.DataFrame(rows)
out_csv = OUTPUT_DIR / "metrics_per_station_meters.csv"
df_out.to_csv(out_csv, index=False)

# ═══════════════════════════════════════════════════════════════
# APERÇU RAPIDE
# ═══════════════════════════════════════════════════════════════
print(f"\n{'='*70}")
print(f"  APERÇU — NRMSE indépendant (mètres), {SOURCE.upper()} {FREQ}")
print(f"{'='*70}")
summary = df_out.groupby("model").agg(
    n=("station", "nunique"),
    NRMSE_amp_modele_med=("NRMSE_amp_modele_pct", "median"),
    NRMSE_amp_alti_med=("NRMSE_amp_alti_pct", "median"),
    NRMSE_mean_modele_med=("NRMSE_mean_modele_pct", "median"),
    NRMSE_mean_alti_med=("NRMSE_mean_alti_pct", "median"),
).reset_index()
print(summary.to_string(index=False))

print(f"\n→ {out_csv}")
print("\nDone")