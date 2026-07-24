"""
plot_quantile_mapping_examples.py  (VERSION K-FOLD — stations fixées)
════════════════════════════════════════════════════════════════════════
Trace, pour CHAQUE ANNÉE disponible de CHAQUE station d'une liste fixée,
un PNG séparé avec 2 panels empilés :
  - AVANT : alti (obs, la vraie cible) + prédiction brute du modèle
  - APRÈS : alti (obs) + prédiction corrigée par k-fold quantile mapping

Contrairement à la version précédente :
  - Lit directement le fichier déjà corrigé par apply_quantile_mapping_kfold.py
    (colonne "pred_corrige"), au lieu de recalculer un mapping ici.
  - La cible du mapping est l'ALTI elle-même (obs), pas l'insitu -> voir
    la discussion : calibrer sur l'insitu rendrait la méthode dépendante
    de la présence d'une station in-situ, ce qui va à l'encontre du but
    (généraliser partout, y compris sans in-situ).
  - Comme la correction est déjà en k-fold (chaque point corrigé par une
    table qui ne l'a jamais vu), il n'y a PLUS de distinction
    calibration/test par année à afficher : TOUTES les années montrent
    un score hors-échantillon honnête, pas seulement certaines.
  - L'in-situ, quand disponible, reste affiché sur le graphe comme
    référence visuelle indépendante, mais NE SERT PAS à construire la
    correction ni à calculer les métriques NSE/KGE principales.

Sorties :
  ./data_processing/Quantile_Mapping/plot/{station}/{label}_{year}.png

Prérequis : apply_quantile_mapping_kfold.py doit avoir déjà tourné pour
le LABEL choisi (utilise directement son fichier "*_corrige.csv").
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

# Stations fixées (plus de sélection aléatoire)
STATIONS_FIXEES = ["21929", "24129", "23921", "24130", "18872"]

MIN_POINTS_PER_YEAR = 5   # en dessous, on ne trace pas l'année (courbe trop pauvre pour être lisible)

QM_OUTPUT_DIR = Path(f"./data_processing/AnalyseModelsDtoD/quantile_mapping_{SOURCE}_{FREQ}")
CORRIGE_CSV = QM_OUTPUT_DIR / f"{LABEL}_{SOURCE}_{FREQ}_corrige.csv"

# Insitu : affichage visuel secondaire uniquement, jamais utilisé pour le calcul
AFFICHER_INSITU = True
HW_DB, DAHITI_DB = "./data/hydroweb_next.db", "./data/dahiti.db"
INSITU_DB  = "./data/insitu_data.db"
INSITU_SHP = "./data/insitu/shp/station_schapi_alti_ref_2025_river.gpkg"
SAT_DB     = HW_DB if SOURCE == "hwnext" else DAHITI_DB
DATE_MIN, DATE_MAX = "2016-01-01", "2025-12-31"
DIST_MAX_KM = 10.0
WINDOW_DAYS = 7 if FREQ == "10j" else 14

PLOT_ROOT = Path("./data_processing/Quantile_Mapping/plot")

# ═══════════════════════════════════════════════════════════════
# MÉTRIQUES (vs ALTI = la vraie cible)
# ═══════════════════════════════════════════════════════════════
def compute_nse_kge(obs, pred, min_pairs=5):
    obs = np.asarray(obs, dtype=float)
    pred = np.asarray(pred, dtype=float)
    valid = ~(np.isnan(obs) | np.isnan(pred))
    obs, pred = obs[valid], pred[valid]
    if len(obs) < min_pairs:
        return np.nan, np.nan

    denom = np.sum((obs - obs.mean()) ** 2)
    nse = 1 - np.sum((obs - pred) ** 2) / denom if denom > 0 else np.nan

    if obs.std() > 0 and pred.std() > 0:
        r = np.corrcoef(obs, pred)[0, 1]
        alpha = pred.std() / obs.std()
        kge = 1 - np.sqrt((r - 1) ** 2 + (alpha - 1) ** 2)
    else:
        kge = np.nan

    return (float(nse) if not np.isnan(nse) else np.nan,
            float(kge) if not np.isnan(kge) else np.nan)

# ═══════════════════════════════════════════════════════════════
# HELPERS INSITU (référence visuelle secondaire uniquement)
# ═══════════════════════════════════════════════════════════════
if AFFICHER_INSITU:
    print("Chargement shapefile insitu (référence visuelle)...")
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

    def zscore(arr):
        arr = np.asarray(arr, dtype=float)
        m = ~np.isnan(arr)
        if m.sum() < 2:
            return arr * np.nan
        mu, sig = arr[m].mean(), arr[m].std()
        return (arr - mu) / sig if sig > 0 else arr * 0

# ═══════════════════════════════════════════════════════════════
# CHARGEMENT DU FICHIER DÉJÀ CORRIGÉ (k-fold)
# ═══════════════════════════════════════════════════════════════
if not CORRIGE_CSV.exists():
    raise SystemExit(f"{CORRIGE_CSV} introuvable -> lancer apply_quantile_mapping_kfold.py "
                      f"avec LABEL='{LABEL}' d'abord.")

df_res = pd.read_csv(CORRIGE_CSV)
df_res["station"] = df_res["station"].astype(str)
df_res["date"] = pd.to_datetime(df_res["date"])
df_res = df_res.dropna(subset=["obs", "pred"]).sort_values(["station", "date"])

print(f"Fichier corrigé chargé : {len(df_res)} lignes, {df_res['station'].nunique()} stations")
print(f"Stations fixées demandées : {STATIONS_FIXEES}")

# ═══════════════════════════════════════════════════════════════
# BOUCLE PAR STATION -> UN PNG PAR ANNÉE
# ═══════════════════════════════════════════════════════════════
for station in STATIONS_FIXEES:
    sub = df_res[df_res["station"] == station].sort_values("date").reset_index(drop=True)
    if sub.empty:
        print(f"  ⚠ Station {station} : absente du fichier corrigé -> SKIP")
        continue
    if sub["pred_corrige"].isna().all():
        print(f"  ⚠ Station {station} : aucune ligne corrigée (probablement pas assez "
              f"d'observations pour le k-fold) -> SKIP")
        continue

    pred_all = sub["pred"].values
    pred_corrige_all = sub["pred_corrige"].values
    obs_all = sub["obs"].values
    dates = pd.to_datetime(sub["date"].values)

    # Insitu (visuel uniquement)
    ins_z_all = np.full(len(sub), np.nan)
    code_ins, dist_km = None, None
    if AFFICHER_INSITU:
        lon, lat = get_coords(station)
        if lon is not None:
            code_ins, dist_km = get_insitu_proche(lon, lat)
            df_ins = get_insitu_series(code_ins)
            if df_ins is not None:
                ins_wl = align_insitu(dates.values, df_ins, WINDOW_DAYS)
                ins_z_all = zscore(ins_wl)

    station_dir = PLOT_ROOT / str(station)
    station_dir.mkdir(parents=True, exist_ok=True)

    years = sorted(pd.Series(dates).dt.year.unique())
    n_saved = 0

    for year in years:
        year_mask = pd.Series(dates).dt.year.values == year
        if year_mask.sum() < MIN_POINTS_PER_YEAR:
            continue

        d_y = dates[year_mask]
        obs_y = obs_all[year_mask]
        pred_y = pred_all[year_mask]
        pred_corrige_y = pred_corrige_all[year_mask]
        ins_y = ins_z_all[year_mask]

        # Toutes les corrections sont déjà hors-échantillon (k-fold) ->
        # pas de distinction calibration/test à afficher, contrairement à
        # la version précédente (split chronologique 70/30)
        nse_before, kge_before = compute_nse_kge(obs_y, pred_y)
        nse_after, kge_after = compute_nse_kge(obs_y, pred_corrige_y)

        fig, axes = plt.subplots(2, 1, figsize=(11, 7), sharex=True)
        titre_insitu = f"  |  insitu {code_ins} ({dist_km:.1f} km)" if code_ins is not None else ""
        fig.suptitle(f"Station {station} — {year}{titre_insitu}\n"
                     f"{LABEL} [{SOURCE.upper()} {FREQ}]  —  correction k-fold, hors-échantillon",
                     fontsize=12, fontweight="bold")

        panels = [
            (axes[0], pred_y, "AVANT correction", nse_before, kge_before),
            (axes[1], pred_corrige_y, "APRÈS k-fold quantile mapping", nse_after, kge_after),
        ]
        for ax, pred_to_plot, title, nse_val, kge_val in panels:
            ax.plot(d_y, pred_to_plot, color="#1f77b4", lw=1.4, label="Prédiction modèle", zorder=2)
            ax.scatter(d_y, obs_y, color="#ff7f0e", s=32, zorder=3, label="Alti (obs, cible réelle)")

            if AFFICHER_INSITU and not np.all(np.isnan(ins_y)):
                ax.plot(d_y, ins_y, color="#2ca02c", lw=1.2, alpha=0.6, ls=":",
                       label="Insitu (référence visuelle, z-score)", zorder=1)

            nse_str = f"{nse_val:.3f}" if not np.isnan(nse_val) else "n/a"
            kge_str = f"{kge_val:.3f}" if not np.isnan(kge_val) else "n/a"
            ax.set_title(f"{title}   —   NSE vs alti = {nse_str}   |   KGE vs alti = {kge_str}",
                        fontsize=10, fontweight="bold")
            ax.grid(True, alpha=0.25)
            ax.legend(loc="best", fontsize=8)

        axes[1].tick_params(axis="x", labelrotation=30)
        plt.tight_layout()

        out_path = station_dir / f"{LABEL}_{year}.png"
        fig.savefig(out_path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        n_saved += 1

    print(f"  Station {station} : {n_saved} PNG (un par année) -> {station_dir}/")

print(f"\n✅ Terminé. Figures dans : {PLOT_ROOT}/{{station}}/{{label}}_{{annee}}.png")