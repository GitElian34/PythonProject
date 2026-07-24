"""
quantile_mapping_correction.py
════════════════════════════════════════════════════════════════════════
Post-traitement par quantile mapping (CDF matching) : corrige la
distribution des prédictions d'un modèle pour qu'elle épouse la
distribution des mesures in-situ, station par station.

PRINCIPE :
----------
Pour une station donnée, on regarde le rang/percentile de chaque
prédiction dans SA PROPRE distribution (celle du modèle), puis on
remplace cette valeur par celle qui occupe le même rang/percentile dans
la distribution in-situ. Contrairement à un simple recalage médiane ou
à une normalisation z-score globale (qui ne corrige que la moyenne et
l'écart-type), le quantile mapping corrige la FORME ENTIÈRE de la
distribution (asymétrie, comportement de queue) -> pertinent si le
modèle sous-estime spécifiquement les valeurs extrêmes (pics) plus que
les valeurs moyennes.

RIGUEUR — split calibration/test :
------------------------------------
Le mapping (la correspondance quantile modèle -> quantile in-situ) est
construit UNIQUEMENT sur une période de calibration (les CALIB_FRACTION
premières dates disponibles, par station). Il est ensuite appliqué et
évalué sur la période de test restante, JAMAIS vue pendant la
calibration. Sans ce split, une évaluation en boucle fermée (in-sample)
donnerait des résultats artificiellement parfaits, sans valeur
prédictive réelle.

Entrée : résidus centralisés produits par eval_zeroshot_DtoD.py
  ./data_processing/AnalyseModelsDtoD/residuals/residuals_{LABEL}_{source}_{freq}.csv

Sorties (dans OUTPUT_DIR) :
  quantile_mapping_per_station.csv   (métriques avant/après, par station)
  quantile_mapping_resume.csv        (résumé médian)
  quantile_mapping_comparison.png
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
# PARAMÈTRES GLOBAUX
# ═══════════════════════════════════════════════════════════════
SOURCE = "dahiti"   # "hwnext" ou "dahiti"
FREQ   = "27j"      # "10j" ou "27j"
LABEL  = "DtoD80_NSE"   # le modèle à corriger (doit avoir un fichier résidus existant)

CALIB_FRACTION = 0.7   # 70% des dates les plus anciennes -> calibration, 30% restantes -> test
MIN_PAIRS_CALIB = 20   # nb minimum de paires (pred, insitu) en calibration pour tenter le mapping
MIN_PAIRS_TEST  = 10   # nb minimum de paires en test pour évaluer

RESIDUALS_DIR = Path("./data_processing/AnalyseModelsDtoD/residuals")

HW_DB      = "./data/hydroweb_next.db"
DAHITI_DB  = "./data/dahiti.db"
INSITU_DB  = "./data/insitu_data.db"
INSITU_SHP = "./data/insitu/shp/station_schapi_alti_ref_2025_river.gpkg"
SAT_DB     = HW_DB if SOURCE == "hwnext" else DAHITI_DB

DATE_MIN, DATE_MAX = "2016-01-01", "2025-12-31"
DIST_MAX_KM = 10.0
WINDOW_DAYS = 7 if FREQ == "10j" else 14

OUTPUT_DIR = Path(f"./data_processing/AnalyseModelsDtoD/quantile_mapping_{SOURCE}_{FREQ}")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ═══════════════════════════════════════════════════════════════
# QUANTILE MAPPING
# ═══════════════════════════════════════════════════════════════
def quantile_map(pred_calib, target_calib, pred_new):
    """
    Construit la correspondance quantile-à-quantile entre la distribution
    empirique de pred_calib et celle de target_calib, puis l'applique à
    pred_new. Interpolation linéaire dans les deux sens (rang -> valeur).
    """
    pred_calib = np.asarray(pred_calib, dtype=float)
    target_calib = np.asarray(target_calib, dtype=float)
    pred_calib = np.sort(pred_calib[~np.isnan(pred_calib)])
    target_calib = np.sort(target_calib[~np.isnan(target_calib)])

    n_p, n_t = len(pred_calib), len(target_calib)
    if n_p < 5 or n_t < 5:
        return np.full(len(pred_new), np.nan)

    q_p = (np.arange(n_p) + 0.5) / n_p
    q_t = (np.arange(n_t) + 0.5) / n_t

    ranks = np.interp(pred_new, pred_calib, q_p, left=0.0, right=1.0)
    return np.interp(ranks, q_t, target_calib)


def compute_metrics(obs, pred):
    """NSE / KGE (sans beta, cohérent avec les données z-scorées) / RMSE / R2."""
    obs = np.asarray(obs, dtype=float)
    pred = np.asarray(pred, dtype=float)
    n = len(obs)
    if n < 5:
        return {"NSE": np.nan, "KGE": np.nan, "RMSE": np.nan, "R2": np.nan, "n": n}

    denom = np.sum((obs - obs.mean()) ** 2)
    nse = 1 - np.sum((obs - pred) ** 2) / denom if denom > 0 else np.nan
    rmse = float(np.sqrt(np.mean((obs - pred) ** 2)))

    if obs.std() > 0 and pred.std() > 0:
        r = np.corrcoef(obs, pred)[0, 1]
        r2 = float(r ** 2)
        alpha = pred.std() / obs.std()
        kge = 1 - np.sqrt((r - 1) ** 2 + (alpha - 1) ** 2)
    else:
        r2, kge = np.nan, np.nan

    return {"NSE": float(nse) if not np.isnan(nse) else np.nan,
            "KGE": float(kge) if not np.isnan(kge) else np.nan,
            "RMSE": rmse, "R2": r2, "n": n}

# ═══════════════════════════════════════════════════════════════
# HELPERS — insitu le plus proche (identique à compare_models_vs_alti.py)
# ═══════════════════════════════════════════════════════════════
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
# CHARGEMENT DES RÉSIDUS DU MODÈLE À CORRIGER
# ═══════════════════════════════════════════════════════════════
res_path = RESIDUALS_DIR / f"residuals_{LABEL}_{SOURCE}_{FREQ}.csv"
if not res_path.exists():
    raise SystemExit(f"Résidus introuvables pour le label '{LABEL}' : {res_path}\n"
                      f"Lancer eval_zeroshot_DtoD.py (ou migrate_old_residuals.py) d'abord.")

df_res = pd.read_csv(res_path)
df_res["station"] = df_res["station"].astype(str)
df_res["date"] = pd.to_datetime(df_res["date"])
df_res = df_res.dropna(subset=["obs", "pred"]).sort_values(["station", "date"])

if "label" in df_res.columns and not (df_res["label"] == LABEL).all():
    raise SystemExit(f"⚠⚠ INCOHÉRENCE : {res_path} ne contient pas uniquement le label "
                      f"'{LABEL}' (trouvé : {df_res['label'].unique()}) -> vérifier avant de continuer.")

print(f"Résidus chargés pour '{LABEL}' : {len(df_res)} lignes, {df_res['station'].nunique()} stations")

# ═══════════════════════════════════════════════════════════════
# BOUCLE PAR STATION — split calib/test, mapping, évaluation
# ═══════════════════════════════════════════════════════════════
rows = []

for station, sub in df_res.groupby("station"):
    sub = sub.sort_values("date")

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
    if has_ins.sum() < (MIN_PAIRS_CALIB + MIN_PAIRS_TEST):
        continue

    sub = sub.reset_index(drop=True)
    pred_all = sub["pred"].values
    obs_all  = sub["obs"].values

    # Split chronologique : calibration = 1ers CALIB_FRACTION, test = le reste
    n_total = len(sub)
    split_idx = int(n_total * CALIB_FRACTION)

    calib_mask = np.zeros(n_total, dtype=bool)
    calib_mask[:split_idx] = True
    test_mask = ~calib_mask

    # Ne garder, dans chaque période, que les points où l'insitu est dispo
    calib_ok = calib_mask & has_ins
    test_ok  = test_mask & has_ins

    if calib_ok.sum() < MIN_PAIRS_CALIB or test_ok.sum() < MIN_PAIRS_TEST:
        continue

    # z-score de l'insitu calé sur la période de CALIBRATION uniquement
    # (évite toute fuite d'information de la période de test dans la
    # normalisation elle-même)
    ins_calib_vals = ins_wl[calib_ok]
    mu_ins, sig_ins = np.nanmean(ins_calib_vals), np.nanstd(ins_calib_vals)
    if sig_ins <= 0:
        continue
    ins_z_all = (ins_wl - mu_ins) / sig_ins

    # ── AVANT correction : pred brut vs insitu (test uniquement) ──────
    m_before = compute_metrics(ins_z_all[test_ok], pred_all[test_ok])

    # ── Construction du mapping sur la calibration, application sur le test ──
    pred_mapped_test = quantile_map(
        pred_calib=pred_all[calib_ok],
        target_calib=ins_z_all[calib_ok],
        pred_new=pred_all[test_ok],
    )
    m_after = compute_metrics(ins_z_all[test_ok], pred_mapped_test)

    rows.append({
        "station": station, "insitu_code": code_ins, "dist_insitu_km": round(dist_km, 1),
        "n_calib": int(calib_ok.sum()), "n_test": int(test_ok.sum()),
        "NSE_avant": m_before["NSE"], "NSE_apres": m_after["NSE"],
        "KGE_avant": m_before["KGE"], "KGE_apres": m_after["KGE"],
        "RMSE_avant": m_before["RMSE"], "RMSE_apres": m_after["RMSE"],
        "R2_avant": m_before["R2"], "R2_apres": m_after["R2"],
    })

if not rows:
    raise SystemExit("Aucune station exploitable (pas assez de points calib+test avec insitu proche).")

df_out = pd.DataFrame(rows)
df_out.to_csv(OUTPUT_DIR / "quantile_mapping_per_station.csv", index=False)
print(f"\nRésultats par station -> {OUTPUT_DIR / 'quantile_mapping_per_station.csv'} "
      f"({len(df_out)} stations)")

# ═══════════════════════════════════════════════════════════════
# RÉSUMÉ
# ═══════════════════════════════════════════════════════════════
print(f"\n{'=' * 80}")
print(f"  QUANTILE MAPPING — {LABEL}  [{SOURCE.upper()} {FREQ}]  "
      f"(évalué sur période TEST, hors calibration)")
print(f"{'=' * 80}")
print(f"  {'métrique':<8} {'médiane avant':>14} {'médiane après':>15} {'gain médian':>13} "
      f"{'% stations améliorées':>22}")

resume = {"label": LABEL, "n_stations": len(df_out)}
for metric, higher_is_better in {"NSE": True, "KGE": True, "RMSE": False, "R2": True}.items():
    before = df_out[f"{metric}_avant"].dropna()
    after = df_out[f"{metric}_apres"].dropna()
    gain = (df_out[f"{metric}_apres"] - df_out[f"{metric}_avant"]) if higher_is_better \
        else (df_out[f"{metric}_avant"] - df_out[f"{metric}_apres"])
    gain = gain.dropna()
    pct_better = (gain > 0).mean() * 100 if len(gain) else np.nan

    print(f"  {metric:<8} {before.median():>14.3f} {after.median():>15.3f} "
          f"{gain.median():>13.3f} {pct_better:>21.1f}%")

    resume[f"{metric}_med_avant"] = round(before.median(), 3) if len(before) else np.nan
    resume[f"{metric}_med_apres"] = round(after.median(), 3) if len(after) else np.nan
    resume[f"{metric}_gain_med"] = round(gain.median(), 3) if len(gain) else np.nan
    resume[f"{metric}_pct_ameliore"] = round(pct_better, 1) if not np.isnan(pct_better) else np.nan

pd.DataFrame([resume]).to_csv(OUTPUT_DIR / "quantile_mapping_resume.csv", index=False)
print(f"\nRésumé -> {OUTPUT_DIR / 'quantile_mapping_resume.csv'}")

# ═══════════════════════════════════════════════════════════════
# FIGURE — avant/après par métrique
# ═══════════════════════════════════════════════════════════════
fig, axes = plt.subplots(2, 2, figsize=(11, 8))
fig.suptitle(f"Quantile mapping — {LABEL} [{SOURCE.upper()} {FREQ}]\n"
             f"Avant / après correction, évalué sur période test ({len(df_out)} stations)",
             fontsize=12, fontweight="bold")
rng = np.random.default_rng(42)

for ax, metric in zip(axes.flat, ["NSE", "KGE", "RMSE", "R2"]):
    data = [df_out[f"{metric}_avant"].dropna().values, df_out[f"{metric}_apres"].dropna().values]
    bp = ax.boxplot(data, tick_labels=["Avant", "Après\n(quantile mapping)"],
                     patch_artist=True, medianprops={"color": "black", "linewidth": 2}, widths=0.5)
    for box, color in zip(bp["boxes"], ["#9E9E9E", "#4CAF50"]):
        box.set_facecolor(color)
        box.set_alpha(0.65)
    for j, vals in enumerate(data, 1):
        jitter = rng.uniform(-0.12, 0.12, len(vals))
        ax.scatter(np.full(len(vals), j) + jitter, vals, alpha=0.3, s=10, color="black", zorder=3)
    ax.set_title(metric, fontsize=11, fontweight="bold")
    ax.grid(True, alpha=0.25, axis="y")

plt.tight_layout()
fig.savefig(OUTPUT_DIR / "quantile_mapping_comparison.png", dpi=150, bbox_inches="tight")
plt.close(fig)
print(f"✅ Figure -> {OUTPUT_DIR / 'quantile_mapping_comparison.png'}")