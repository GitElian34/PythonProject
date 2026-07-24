"""
compare_outliers_insitu_vs_model.py
════════════════════════════════════════════════════════════════════════
Base objective de comparaison, indépendante du modèle : pour chaque
station DAHITI 27j (ou autre SOURCE/FREQ), on construit 3 ensembles de
dates flaguées comme outliers :

  1. RÉFÉRENCE  : alti vs insitu (deux sources indépendantes du modèle)
                  -> "l'alti semble suspecte selon une vérité terrain
                     externe, sans que le modèle n'intervienne"
  2. AVANT      : modèle (pred brut) vs alti
  3. APRÈS      : modèle corrigé (pred_corrige, k-fold) vs alti

On mesure ensuite le RECOUVREMENT entre la référence (1) et chacun des
deux ensembles modèle (2, 3) : si la correction rapproche le modèle de
ce qu'une source indépendante identifierait comme problématique, le
recouvrement doit augmenter de (2) vers (3).

RÈGLE DE FLAGGING (identique à plot_outliers_avant_apres.py) :
  point flagué si |résidu| > OUTLIER_THRESHOLD * std(résidu), std
  calculé sur toute la série de la station.
  - résidu référence : obs_z(alti) - obs_z(insitu)   (les deux z-scorées
    indépendamment, pour être comparables malgré des unités différentes)
  - résidu avant      : obs(alti) - pred
  - résidu après       : obs(alti) - pred_corrige

MÉTRIQUES DE RESSEMBLANCE (calculées sur les dates communes aux 3
ensembles, pour comparer sur le même échantillon) :
  - Jaccard  = |référence ∩ modèle| / |référence ∪ modèle|
               (ressemblance globale des 2 ensembles)
  - Rappel   = |référence ∩ modèle| / |référence|
               ("% des outliers identifiés par l'insitu que le modèle
               retrouve aussi" -- la métrique la plus intuitive)

Entrée : le fichier corrigé par apply_quantile_mapping_kfold.py
  ./data_processing/AnalyseModelsDtoD/quantile_mapping_{source}_{freq}/{label}_{source}_{freq}_corrige.csv

Sorties :
  outliers_ressemblance_par_station.csv
  outliers_ressemblance_resume.png
════════════════════════════════════════════════════════════════════════
"""

import sqlite3
import numpy as np
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from pathlib import Path
from shapely.geometry import Point

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
SOURCE = "dahiti"
FREQ   = "27j"
LABEL  = "DtoD80_NSE"

OUTLIER_THRESHOLD = 2.0
MIN_PAIRS = 20   # nb minimum de dates communes (alti+insitu+pred+pred_corrige) pour traiter une station

HW_DB, DAHITI_DB = "./data/hydroweb_next.db", "./data/dahiti.db"
INSITU_DB  = "./data/insitu_data.db"
INSITU_SHP = "./data/insitu/shp/station_schapi_alti_ref_2025_river.gpkg"
SAT_DB     = HW_DB if SOURCE == "hwnext" else DAHITI_DB
DATE_MIN, DATE_MAX = "2016-01-01", "2025-12-31"
DIST_MAX_KM = 10.0
WINDOW_DAYS = 7 if FREQ == "10j" else 14

QM_OUTPUT_DIR = Path(f"./data_processing/AnalyseModelsDtoD/quantile_mapping_{SOURCE}_{FREQ}")
CORRIGE_CSV = QM_OUTPUT_DIR / f"{LABEL}_{SOURCE}_{FREQ}_corrige.csv"

OUTPUT_DIR = Path(f"./data_processing/AnalyseModelsDtoD/outliers_ressemblance_{SOURCE}_{FREQ}")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

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


def flag_outliers(residual, threshold):
    residual = np.asarray(residual, dtype=float)
    std = np.nanstd(residual)
    if std <= 0 or np.isnan(std):
        return np.zeros(len(residual), dtype=bool)
    flags = np.abs(residual) > (threshold * std)
    return np.where(np.isnan(residual), False, flags)


def jaccard_recall(flag_ref, flag_model):
    """Jaccard et rappel entre 2 ensembles booléens de même longueur."""
    inter = np.sum(flag_ref & flag_model)
    union = np.sum(flag_ref | flag_model)
    n_ref = np.sum(flag_ref)
    jaccard = inter / union if union > 0 else np.nan
    recall = inter / n_ref if n_ref > 0 else np.nan
    return jaccard, recall, int(inter), int(n_ref), int(np.sum(flag_model))

print("Chargement shapefile insitu...")
gdf_insitu = gpd.read_file(INSITU_SHP).to_crs("EPSG:2154")
_cache_ins, _cache_coords = {}, {}

def get_coords(code):
    if code in _cache_coords:
        return _cache_coords[code]
    conn = sqlite3.connect(SAT_DB)
    for c in [str(code), str(code).zfill(13)]:
        df = pd.read_sql(
            "SELECT reference_longitude AS lon, reference_latitude AS lat FROM stations WHERE station_code = ?",
            conn, params=(c,))
        if not df.empty:
            conn.close()
            _cache_coords[code] = (float(df.iloc[0]["lon"]), float(df.iloc[0]["lat"]))
            return _cache_coords[code]
    conn.close()
    _cache_coords[code] = (None, None)
    return None, None

def get_insitu_proche(lon, lat):
    pt = gpd.GeoSeries([Point(lon, lat)], crs="EPSG:4326").to_crs("EPSG:2154")[0]
    dist = gdf_insitu.geometry.distance(pt)
    idx = dist.idxmin()
    return gdf_insitu.loc[idx, "code_sta"], dist[idx] / 1000

def get_insitu_series(code_sta):
    if code_sta not in _cache_ins:
        conn = sqlite3.connect(INSITU_DB)
        df = pd.read_sql("""
            SELECT date, h_med_wsh AS wl FROM mesures_insitu
            WHERE code_sta = ? AND date >= ? AND date <= ? ORDER BY date
        """, conn, params=(code_sta, DATE_MIN, DATE_MAX))
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

# ═══════════════════════════════════════════════════════════════
# CHARGEMENT
# ═══════════════════════════════════════════════════════════════
if not CORRIGE_CSV.exists():
    raise SystemExit(f"{CORRIGE_CSV} introuvable -> lancer apply_quantile_mapping_kfold.py d'abord.")

df_res = pd.read_csv(CORRIGE_CSV)
df_res["station"] = df_res["station"].astype(str)
df_res["date"] = pd.to_datetime(df_res["date"])
df_res = df_res.dropna(subset=["obs", "pred"]).sort_values(["station", "date"])

print(f"Fichier corrigé chargé : {len(df_res)} lignes, {df_res['station'].nunique()} stations")

# ═══════════════════════════════════════════════════════════════
# BOUCLE PAR STATION
# ═══════════════════════════════════════════════════════════════
rows = []
all_flag_ref, all_flag_avant, all_flag_apres = [], [], []

for station, sub in df_res.groupby("station"):
    sub = sub.sort_values("date").reset_index(drop=True)

    lon, lat = get_coords(station)
    if lon is None:
        continue
    code_ins, dist_km = get_insitu_proche(lon, lat)
    if dist_km > DIST_MAX_KM:
        continue
    df_ins = get_insitu_series(code_ins)
    if df_ins is None:
        continue

    ins_wl = align_insitu(sub["date"].values, df_ins, WINDOW_DAYS)
    has_ins = ~np.isnan(ins_wl)
    has_corrige = ~sub["pred_corrige"].isna().values
    common = has_ins & has_corrige   # dates utilisables pour les 3 ensembles à la fois

    if common.sum() < MIN_PAIRS:
        continue

    obs_all = sub["obs"].values
    pred_all = sub["pred"].values
    pred_corrige_all = sub["pred_corrige"].values

    # ── Ensemble RÉFÉRENCE : alti vs insitu (z-scorés indépendamment) ──
    obs_z = zscore(obs_all)
    ins_z = zscore(ins_wl)
    resid_ref = obs_z - ins_z
    flag_ref = flag_outliers(resid_ref, OUTLIER_THRESHOLD)

    # ── Ensemble AVANT : modèle brut vs alti ───────────────────────────
    resid_avant = obs_all - pred_all
    flag_avant = flag_outliers(resid_avant, OUTLIER_THRESHOLD)

    # ── Ensemble APRÈS : modèle corrigé vs alti ────────────────────────
    resid_apres = obs_all - pred_corrige_all
    flag_apres = flag_outliers(resid_apres, OUTLIER_THRESHOLD)

    # Restreindre à l'échantillon commun pour comparer équitablement
    flag_ref_c = flag_ref[common]
    flag_avant_c = flag_avant[common]
    flag_apres_c = flag_apres[common]

    jac_avant, rec_avant, inter_a, nref_a, nmod_a = jaccard_recall(flag_ref_c, flag_avant_c)
    jac_apres, rec_apres, inter_p, nref_p, nmod_p = jaccard_recall(flag_ref_c, flag_apres_c)

    all_flag_ref.append(flag_ref_c)
    all_flag_avant.append(flag_avant_c)
    all_flag_apres.append(flag_apres_c)

    rows.append({
        "station": station, "insitu_code": code_ins, "dist_insitu_km": round(dist_km, 1),
        "n_communes": int(common.sum()),
        "n_ref_outliers": nref_a,
        "n_avant_outliers": nmod_a, "n_apres_outliers": nmod_p,
        "jaccard_avant": jac_avant, "jaccard_apres": jac_apres,
        "rappel_avant": rec_avant, "rappel_apres": rec_apres,
    })

if not rows:
    raise SystemExit("Aucune station exploitable (pas assez de dates communes alti+insitu+pred_corrige).")

df_out = pd.DataFrame(rows)
out_csv = OUTPUT_DIR / "outliers_ressemblance_par_station.csv"
df_out.to_csv(out_csv, index=False)
print(f"\nRésultats par station -> {out_csv} ({len(df_out)} stations)")

print(f"\nDistribution des comptages (diagnostic de sparsité) :")
print(df_out[["n_ref_outliers", "n_avant_outliers", "n_apres_outliers"]].describe().to_string())

# ═══════════════════════════════════════════════════════════════
# RÉSUMÉ
# ═══════════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"  RESSEMBLANCE OUTLIERS — {LABEL} [{SOURCE.upper()} {FREQ}]")
print(f"  Référence = alti vs insitu (indépendante du modèle)")
print(f"{'=' * 80}")

for metric_pair, label_metric in [(("jaccard_avant", "jaccard_apres"), "Jaccard (ressemblance globale)"),
                                    (("rappel_avant", "rappel_apres"), "Rappel (% outliers insitu retrouvés)")]:
    col_avant, col_apres = metric_pair
    before = df_out[col_avant].dropna()
    after = df_out[col_apres].dropna()
    gain = (df_out[col_apres] - df_out[col_avant]).dropna()
    pct_better = (gain > 0).mean() * 100 if len(gain) else np.nan
    print(f"\n  {label_metric}")
    print(f"    médiane avant  : {before.median():.3f}")
    print(f"    médiane après  : {after.median():.3f}")
    print(f"    gain médian    : {gain.median():+.3f}")
    print(f"    % stations où après > avant : {pct_better:.1f}%")

# ═══════════════════════════════════════════════════════════════
# MÉTRIQUE AGRÉGÉE GLOBALE (pooling de toutes les stations)
# ═══════════════════════════════════════════════════════════════
# Plus robuste que la médiane des ratios par station : avec un seuil à
# 3 sigma, chaque station ne flague souvent que 0-2 points -> les ratios
# individuels sont très bruités (beaucoup de 0 par manque de données,
# pas forcément par absence de signal réel). En regroupant TOUS les
# points flagués de TOUTES les stations avant de calculer Jaccard/rappel,
# on obtient un seul chiffre basé sur un échantillon beaucoup plus
# grand, donc statistiquement plus fiable.
flag_ref_global = np.concatenate(all_flag_ref)
flag_avant_global = np.concatenate(all_flag_avant)
flag_apres_global = np.concatenate(all_flag_apres)

jac_avant_g, rec_avant_g, inter_ag, nref_ag, nmod_ag = jaccard_recall(flag_ref_global, flag_avant_global)
jac_apres_g, rec_apres_g, inter_pg, nref_pg, nmod_pg = jaccard_recall(flag_ref_global, flag_apres_global)

print(f"\n{'=' * 80}")
print(f"  MÉTRIQUE AGRÉGÉE GLOBALE (toutes stations regroupées, {len(flag_ref_global)} points communs)")
print(f"{'=' * 80}")
print(f"  Nb outliers référence (insitu vs alti) : {nref_ag}")
print(f"  Nb outliers AVANT (modèle vs alti)     : {nmod_ag}")
print(f"  Nb outliers APRÈS (modèle corrigé)     : {nmod_pg}")
print(f"\n  Jaccard global : avant={jac_avant_g:.3f}  ->  après={jac_apres_g:.3f}  "
      f"(delta={jac_apres_g - jac_avant_g:+.3f})")
print(f"  Rappel global  : avant={rec_avant_g:.3f}  ->  après={rec_apres_g:.3f}  "
      f"(delta={rec_apres_g - rec_avant_g:+.3f})")

pd.DataFrame([{
    "n_points_communs": len(flag_ref_global),
    "n_ref_outliers": nref_ag, "n_avant_outliers": nmod_ag, "n_apres_outliers": nmod_pg,
    "jaccard_avant": jac_avant_g, "jaccard_apres": jac_apres_g,
    "rappel_avant": rec_avant_g, "rappel_apres": rec_apres_g,
}]).to_csv(OUTPUT_DIR / "outliers_ressemblance_globale.csv", index=False)
print(f"\n  -> {OUTPUT_DIR / 'outliers_ressemblance_globale.csv'}")

# ═══════════════════════════════════════════════════════════════
# FIGURE
# ═══════════════════════════════════════════════════════════════
fig, axes = plt.subplots(1, 2, figsize=(11, 5))
rng = np.random.default_rng(42)

for ax, (col_avant, col_apres, title) in zip(axes, [
    ("jaccard_avant", "jaccard_apres", "Jaccard vs référence insitu"),
    ("rappel_avant", "rappel_apres", "Rappel vs référence insitu"),
]):
    data = [df_out[col_avant].dropna().values, df_out[col_apres].dropna().values]
    bp = ax.boxplot(data, tick_labels=["Avant", "Après\n(k-fold)"], patch_artist=True,
                     medianprops={"color": "black", "linewidth": 2}, widths=0.5)
    for box, color in zip(bp["boxes"], ["#9E9E9E", "#4CAF50"]):
        box.set_facecolor(color)
        box.set_alpha(0.65)
    for j, vals in enumerate(data, 1):
        jitter = rng.uniform(-0.12, 0.12, len(vals))
        ax.scatter(np.full(len(vals), j) + jitter, vals, alpha=0.3, s=10, color="black", zorder=3)
    ax.set_title(title, fontsize=11, fontweight="bold")
    ax.set_ylim(-0.05, 1.05)
    ax.grid(True, alpha=0.25, axis="y")

fig.suptitle(f"{LABEL} [{SOURCE.upper()} {FREQ}]  —  {len(df_out)} stations, seuil {OUTLIER_THRESHOLD}σ",
             fontsize=12, fontweight="bold")
plt.tight_layout()
fig_path = OUTPUT_DIR / "outliers_ressemblance_resume.png"
fig.savefig(fig_path, dpi=150, bbox_inches="tight")
plt.close(fig)
print(f"\n✅ Figure -> {fig_path}")