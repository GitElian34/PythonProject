"""
find_best_insitu_par_nse.py
════════════════════════════════════════════════════════════════════════
Pour chaque station satellite (HW Next ou DAHITI, 10j ou 27j), teste tous
les insitu candidats dans un rayon DIST_MAX_KM et retient celui qui
maximise le NSE entre les données brutes satellite (obs) et l'insitu —
sans aucune contrainte de rivière ou de strahler, uniquement basé sur
l'accord empirique des deux séries.

Compare ensuite ce "meilleur insitu" au "plus proche en distance"
(comportement historique), et calcule le NSE modèle vs insitu et
brut vs insitu dans les deux configurations.

Sorties :
  ./data_processing/AnalyseModelsDtoD/riviere/best_insitu_par_nse_{source}_{freq}_{mask}pct.csv

Usage :
    python find_best_insitu_par_nse.py
    (modifier SOURCE, FREQ_LABEL et MASK_PCT ci-dessous)
════════════════════════════════════════════════════════════════════════
"""

import sqlite3
import numpy as np
import pandas as pd
import geopandas as gpd
from pathlib import Path
from shapely.geometry import Point

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES — à modifier ici à chaque utilisation
# ═══════════════════════════════════════════════════════════════
SOURCE     = "hwnext"   # "hwnext" ou "dahiti"
FREQ_LABEL = "27j"      # "10j" ou "27j"
MASK_PCT   = 80       # taux de masquage du modèle à évaluer

RESIDUALS_CSV = Path(
    f"./data/outlier_detection/benchmark_DtoD_{SOURCE}{FREQ_LABEL}/"
    f"residuals_{SOURCE}_{FREQ_LABEL}_{MASK_PCT}pct.csv"
)

HWNEXT_DB  = "./data/hydroweb_next.db"
DAHITI_DB  = "./data/dahiti.db"
INSITU_DB  = "./data/insitu_data.db"
INSITU_SHP = "./data/insitu/shp/station_schapi_alti_ref_2025_river.gpkg"

SAT_DB = HWNEXT_DB if SOURCE == "hwnext" else DAHITI_DB

OUTPUT_DIR = Path("./data_processing/AnalyseModelsDtoD/riviere")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

DATE_MIN    = "2016-01-01"
DATE_MAX    = "2025-12-31"
DIST_MAX_KM = 10.0
WINDOW_DAYS = 7 if FREQ_LABEL == "10j" else 14
MIN_PAIRS   = 20    # nombre minimum de paires obs/insitu pour considérer un candidat valable

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
# CHARGEMENT
# ═══════════════════════════════════════════════════════════════
print(f"Source : {SOURCE.upper()}  |  Frequence : {FREQ_LABEL}  |  Masquage : {MASK_PCT}%\n")
print("Chargement shapefile insitu...")
gdf = gpd.read_file(INSITU_SHP).to_crs("EPSG:2154")

_cache_ins = {}

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

def get_coords_station(conn_sat, code):
    for c in [str(code), str(code).zfill(13)]:
        df = pd.read_sql(
            "SELECT reference_longitude AS lon, reference_latitude AS lat "
            "FROM stations WHERE station_code = ?",
            conn_sat, params=(c,)
        )
        if not df.empty:
            return float(df.iloc[0]["lon"]), float(df.iloc[0]["lat"])
    return None, None

def get_insitu_candidats(lon, lat, dist_max_km):
    pt   = gpd.GeoSeries([Point(lon, lat)], crs="EPSG:4326").to_crs("EPSG:2154")[0]
    dist = gdf.geometry.distance(pt) / 1000
    candidats = dist[dist <= dist_max_km].sort_values()
    return [(gdf.loc[idx, "code_sta"], d) for idx, d in candidats.items()]

# ═══════════════════════════════════════════════════════════════
# CHARGEMENT RÉSIDUS MODÈLE
# ═══════════════════════════════════════════════════════════════
if not RESIDUALS_CSV.exists():
    print(f"Fichier introuvable : {RESIDUALS_CSV}")
    exit()

df_model = pd.read_csv(RESIDUALS_CSV)
df_model["date"]    = pd.to_datetime(df_model["date"])
df_model["station"] = df_model["station"].astype(str)
df_model = df_model.dropna(subset=["obs", "pred"])
print(f"Résidus chargés : {len(df_model)} lignes, {df_model['station'].nunique()} stations\n")

# ═══════════════════════════════════════════════════════════════
# TRAITEMENT — pour chaque station, tester tous les candidats
# ═══════════════════════════════════════════════════════════════
conn_sat = sqlite3.connect(SAT_DB)
rows = []

for code in df_model["station"].unique():
    sub = df_model[df_model["station"] == code].sort_values("date")
    if len(sub) < 5:
        continue

    lon, lat = get_coords_station(conn_sat, code)
    if lon is None:
        continue

    candidats = get_insitu_candidats(lon, lat, DIST_MAX_KM)
    if not candidats:
        continue

    obs_z  = zscore(sub["obs"].values)
    pred_z = zscore(sub["pred"].values)

    best = None      # meilleur insitu par NSE brut
    best_mod = None  # meilleur insitu par NSE modèle
    closest = candidats[0]

    for code_ins, dist_km in candidats:
        df_ins = get_insitu_series(code_ins)
        if df_ins is None:
            continue
        ins_wl = align_insitu(sub["date"].values, df_ins, WINDOW_DAYS)
        n_pairs = int(np.sum(~np.isnan(ins_wl)))
        if n_pairs < MIN_PAIRS:
            continue
        ins_z = zscore(ins_wl)

        nse_brut_c = nse(ins_z, obs_z)
        if not np.isnan(nse_brut_c):
            if best is None or nse_brut_c > best[2]:
                best = (code_ins, dist_km, nse_brut_c, n_pairs)

        nse_mod_c = nse(ins_z, pred_z)
        if not np.isnan(nse_mod_c):
            if best_mod is None or nse_mod_c > best_mod[2]:
                best_mod = (code_ins, dist_km, nse_mod_c, n_pairs)

    if best is None and best_mod is None:
        continue

    if best is not None:
        code_best, dist_best, nse_brut_best, n_pairs_best = best
        df_ins_best = get_insitu_series(code_best)
        ins_wl_best = align_insitu(sub["date"].values, df_ins_best, WINDOW_DAYS)
        ins_z_best  = zscore(ins_wl_best)
        nse_modele_best = nse(ins_z_best, pred_z)
    else:
        code_best, dist_best, nse_brut_best, n_pairs_best, nse_modele_best = None, np.nan, np.nan, 0, np.nan

    code_close, dist_close = closest
    df_ins_close = get_insitu_series(code_close)
    nse_brut_close = np.nan
    nse_modele_close = np.nan
    n_pairs_close = 0
    if df_ins_close is not None:
        ins_wl_close = align_insitu(sub["date"].values, df_ins_close, WINDOW_DAYS)
        n_pairs_close = int(np.sum(~np.isnan(ins_wl_close)))
        if n_pairs_close >= MIN_PAIRS:
            ins_z_close = zscore(ins_wl_close)
            nse_brut_close   = nse(ins_z_close, obs_z)
            nse_modele_close = nse(ins_z_close, pred_z)

    # ── Catégorie 3 — insitu qui maximise le NSE modèle vs insitu ──
    if best_mod is not None:
        code_bestmod, dist_bestmod, nse_modele_bestmod, n_pairs_bestmod = best_mod
        df_ins_bestmod = get_insitu_series(code_bestmod)
        ins_wl_bestmod = align_insitu(sub["date"].values, df_ins_bestmod, WINDOW_DAYS)
        ins_z_bestmod  = zscore(ins_wl_bestmod)
        nse_brut_bestmod = nse(ins_z_bestmod, obs_z)
    else:
        code_bestmod, dist_bestmod, nse_modele_bestmod, n_pairs_bestmod, nse_brut_bestmod = None, np.nan, np.nan, 0, np.nan

    rows.append({
        "station"               : code,
        "n_candidats_testes"    : len(candidats),

        "insitu_plus_proche"    : code_close,
        "dist_plus_proche_km"   : round(dist_close, 1),
        "n_pairs_plus_proche"   : n_pairs_close,
        "nse_brut_plus_proche"  : round(nse_brut_close, 3)   if not np.isnan(nse_brut_close) else np.nan,
        "nse_modele_plus_proche": round(nse_modele_close, 3) if not np.isnan(nse_modele_close) else np.nan,

        "insitu_meilleur_nse"   : code_best,
        "dist_meilleur_nse_km"  : round(dist_best, 1) if not np.isnan(dist_best) else np.nan,
        "n_pairs_meilleur_nse"  : n_pairs_best,
        "nse_brut_meilleur"     : round(nse_brut_best, 3) if not np.isnan(nse_brut_best) else np.nan,
        "nse_modele_meilleur"   : round(nse_modele_best, 3) if not np.isnan(nse_modele_best) else np.nan,

        "insitu_meilleur_modele"    : code_bestmod,
        "dist_meilleur_modele_km"   : round(dist_bestmod, 1) if not np.isnan(dist_bestmod) else np.nan,
        "n_pairs_meilleur_modele"   : n_pairs_bestmod,
        "nse_modele_meilleur_modele": round(nse_modele_bestmod, 3) if not np.isnan(nse_modele_bestmod) else np.nan,
        "nse_brut_meilleur_modele"  : round(nse_brut_bestmod, 3) if not np.isnan(nse_brut_bestmod) else np.nan,

        "meme_insitu"           : code_best == code_close,
        "meme_insitu_best_bestmod": code_best == code_bestmod,
        "gain_nse_brut"         : round(nse_brut_best - nse_brut_close, 3) if not np.isnan(nse_brut_close) and not np.isnan(nse_brut_best) else np.nan,

        "modele_meilleur_proche"   : (not np.isnan(nse_modele_close) and not np.isnan(nse_brut_close)
                                        and nse_modele_close > nse_brut_close),
        "modele_meilleur_meilleur" : (not np.isnan(nse_modele_best) and not np.isnan(nse_brut_best)
                                        and nse_modele_best > nse_brut_best),
        "modele_meilleur_meilleurmodele": (not np.isnan(nse_modele_bestmod) and not np.isnan(nse_brut_bestmod)
                                        and nse_modele_bestmod > nse_brut_bestmod),
    })

conn_sat.close()

df_out = pd.DataFrame(rows)
out_csv = OUTPUT_DIR / f"best_insitu_par_nse_{SOURCE}_{FREQ_LABEL}_{MASK_PCT}pct.csv"
df_out.to_csv(out_csv, index=False)

# ═══════════════════════════════════════════════════════════════
# RÉSUMÉ
# ═══════════════════════════════════════════════════════════════
print(f"{'='*80}")
print(f"  RÉSUMÉ — {SOURCE.upper()} {FREQ_LABEL} — DtoD{MASK_PCT}%")
print(f"  Meilleur insitu (par NSE brut) vs plus proche en distance")
print(f"{'='*80}")
print(f"  Stations traitées          : {len(df_out)}")

if len(df_out) > 0:
    print(f"  Même insitu dans les 2 cas : {df_out['meme_insitu'].sum()} / {len(df_out)} "
          f"({df_out['meme_insitu'].mean()*100:.0f}%)\n")

    v_brut_close = df_out["nse_brut_plus_proche"].dropna()
    v_brut_best  = df_out["nse_brut_meilleur"].dropna()
    v_brut_bestmod = df_out["nse_brut_meilleur_modele"].dropna()
    v_mod_close  = df_out["nse_modele_plus_proche"].dropna()
    v_mod_best   = df_out["nse_modele_meilleur"].dropna()
    v_mod_bestmod = df_out["nse_modele_meilleur_modele"].dropna()

    print(f"  {'':32} {'médiane':>9} {'moyenne':>9} {'>0':>5} {'>0.5':>6}")
    print(f"  {'Brut vs insitu (proche)':<32} {v_brut_close.median():>9.3f} {v_brut_close.mean():>9.3f} "
          f"{(v_brut_close>0).sum():>5} {(v_brut_close>0.5).sum():>6}")
    print(f"  {'Brut vs insitu (meilleur brut)':<32} {v_brut_best.median():>9.3f} {v_brut_best.mean():>9.3f} "
          f"{(v_brut_best>0).sum():>5} {(v_brut_best>0.5).sum():>6}")
    print(f"  {'Brut vs insitu (meilleur modèle)':<32} {v_brut_bestmod.median():>9.3f} {v_brut_bestmod.mean():>9.3f} "
          f"{(v_brut_bestmod>0).sum():>5} {(v_brut_bestmod>0.5).sum():>6}")
    print()
    print(f"  {'Modèle vs insitu (proche)':<32} {v_mod_close.median():>9.3f} {v_mod_close.mean():>9.3f} "
          f"{(v_mod_close>0).sum():>5} {(v_mod_close>0.5).sum():>6}")
    print(f"  {'Modèle vs insitu (meilleur brut)':<32} {v_mod_best.median():>9.3f} {v_mod_best.mean():>9.3f} "
          f"{(v_mod_best>0).sum():>5} {(v_mod_best>0.5).sum():>6}")
    print(f"  {'Modèle vs insitu (meilleur modèle)':<32} {v_mod_bestmod.median():>9.3f} {v_mod_bestmod.mean():>9.3f} "
          f"{(v_mod_bestmod>0).sum():>5} {(v_mod_bestmod>0.5).sum():>6}")

    print(f"\n  Gain médian NSE brut (meilleur brut - proche) : {df_out['gain_nse_brut'].median():.3f}")
    print(f"  Stations avec gain > 0.2 : {(df_out['gain_nse_brut'] > 0.2).sum()} / {len(df_out)}")
    print(f"  Le 'meilleur insitu brut' == 'meilleur insitu modèle' : "
          f"{df_out['meme_insitu_best_bestmod'].sum()} / {len(df_out)} "
          f"({df_out['meme_insitu_best_bestmod'].mean()*100:.0f}%)")

    n_total_proche = df_out["nse_modele_plus_proche"].notna().sum()
    n_mod_gt_proche = df_out["modele_meilleur_proche"].sum()
    n_total_best    = df_out["nse_modele_meilleur"].notna().sum()
    n_mod_gt_best   = df_out["modele_meilleur_meilleur"].sum()
    n_total_bestmod = df_out["nse_modele_meilleur_modele"].notna().sum()
    n_mod_gt_bestmod = df_out["modele_meilleur_meilleurmodele"].sum()

    print(f"\n  Modèle meilleur que le brut (insitu = plus proche)        : "
          f"{n_mod_gt_proche} / {n_total_proche} ({n_mod_gt_proche/n_total_proche*100:.0f}%)" if n_total_proche else "")
    print(f"  Modèle meilleur que le brut (insitu = meilleur NSE brut)   : "
          f"{n_mod_gt_best} / {n_total_best} ({n_mod_gt_best/n_total_best*100:.0f}%)" if n_total_best else "")
    print(f"  Modèle meilleur que le brut (insitu = meilleur NSE modèle) : "
          f"{n_mod_gt_bestmod} / {n_total_bestmod} ({n_mod_gt_bestmod/n_total_bestmod*100:.0f}%)" if n_total_bestmod else "")

print(f"\n  CSV complet → {out_csv}")