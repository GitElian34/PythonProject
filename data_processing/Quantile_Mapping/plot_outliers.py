"""
plot_outliers_avant_apres.py
════════════════════════════════════════════════════════════════════════
Sur les mêmes 5 stations fixées, flague les points où le modèle est
"trop loin" de l'alti (outlier), séparément AVANT et APRÈS correction
k-fold, et trace uniquement les ANNÉES où au moins un outlier apparaît
(avant OU après) -> permet de voir à l'œil si la correction réduit
vraiment les écarts flagués, sans avoir à éplucher toutes les années.

RÈGLE DE FLAGGING (cohérente avec la convention déjà utilisée dans le
projet, ex. pct_outliers_hwnext_DtoD80.py, OUTLIER_THRESHOLD=3.0) :
  un point est flagué si |obs - pred| > OUTLIER_THRESHOLD * std(résidu),
  le std étant calculé sur TOUTE la série disponible de la station
  (pas par année -> échantillon plus robuste pour estimer le std).
  Calculé indépendamment avant (résidu = obs - pred) et après
  (résidu = obs - pred_corrige).

Entrée : le fichier déjà corrigé par apply_quantile_mapping_kfold.py
  (colonnes station, date, obs, pred, pred_corrige)

Sorties :
  ./data_processing/Quantile_Mapping/outlier/{station}/{label}_{year}.png
  (uniquement les années avec >=1 outlier, avant ou après)
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

STATIONS_FIXEES = ["21929", "24129", "23921", "24130", "18872"]

OUTLIER_THRESHOLD = 2.0   # en nombre d'écarts-types du résidu (convention du projet)
MIN_POINTS_PER_YEAR = 5

QM_OUTPUT_DIR = Path(f"./data_processing/AnalyseModelsDtoD/quantile_mapping_{SOURCE}_{FREQ}")
CORRIGE_CSV = QM_OUTPUT_DIR / f"{LABEL}_{SOURCE}_{FREQ}_corrige.csv"

AFFICHER_INSITU = True
HW_DB, DAHITI_DB = "./data/hydroweb_next.db", "./data/dahiti.db"
INSITU_DB  = "./data/insitu_data.db"
INSITU_SHP = "./data/insitu/shp/station_schapi_alti_ref_2025_river.gpkg"
SAT_DB     = HW_DB if SOURCE == "hwnext" else DAHITI_DB
DATE_MIN, DATE_MAX = "2016-01-01", "2025-12-31"
DIST_MAX_KM = 10.0
WINDOW_DAYS = 7 if FREQ == "10j" else 14

OUTLIER_ROOT = Path("./data_processing/Quantile_Mapping/outlier")

# ═══════════════════════════════════════════════════════════════
# HELPERS INSITU (référence visuelle secondaire)
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
# CHARGEMENT
# ═══════════════════════════════════════════════════════════════
if not CORRIGE_CSV.exists():
    raise SystemExit(f"{CORRIGE_CSV} introuvable -> lancer apply_quantile_mapping_kfold.py "
                      f"avec LABEL='{LABEL}' d'abord.")

df_res = pd.read_csv(CORRIGE_CSV)
df_res["station"] = df_res["station"].astype(str)
df_res["date"] = pd.to_datetime(df_res["date"])
df_res = df_res.dropna(subset=["obs", "pred"]).sort_values(["station", "date"])

print(f"Fichier corrigé chargé : {len(df_res)} lignes, {df_res['station'].nunique()} stations")
print(f"Seuil de flagging : |obs - pred| > {OUTLIER_THRESHOLD} x std(résidu station)")

# ═══════════════════════════════════════════════════════════════
# BOUCLE PAR STATION
# ═══════════════════════════════════════════════════════════════
for station in STATIONS_FIXEES:
    sub = df_res[df_res["station"] == station].sort_values("date").reset_index(drop=True)
    if sub.empty:
        print(f"  ⚠ Station {station} : absente du fichier corrigé -> SKIP")
        continue
    if sub["pred_corrige"].isna().all():
        print(f"  ⚠ Station {station} : aucune ligne corrigée -> SKIP")
        continue

    pred_all = sub["pred"].values
    pred_corrige_all = sub["pred_corrige"].values
    obs_all = sub["obs"].values
    dates = pd.to_datetime(sub["date"].values)

    # ── Flagging AVANT / APRÈS, std calculé sur toute la station ──────
    resid_avant = obs_all - pred_all
    resid_apres = obs_all - pred_corrige_all
    std_avant = np.nanstd(resid_avant)
    std_apres = np.nanstd(resid_apres)

    flag_avant = np.abs(resid_avant) > (OUTLIER_THRESHOLD * std_avant) if std_avant > 0 else np.zeros(len(sub), dtype=bool)
    flag_apres = (np.abs(resid_apres) > (OUTLIER_THRESHOLD * std_apres)
                  if std_apres > 0 else np.zeros(len(sub), dtype=bool))
    # np.nan dans resid_apres (si pred_corrige manquant) -> jamais flagué
    flag_apres = np.where(np.isnan(resid_apres), False, flag_apres)

    print(f"  Station {station} : {flag_avant.sum()} outliers avant, "
          f"{flag_apres.sum()} outliers après (sur {len(sub)} points, std_avant={std_avant:.3f}, "
          f"std_apres={std_apres:.3f})")

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

    station_dir = OUTLIER_ROOT / str(station)
    station_dir.mkdir(parents=True, exist_ok=True)

    years = sorted(pd.Series(dates).dt.year.unique())
    n_saved = 0

    for year in years:
        year_mask = pd.Series(dates).dt.year.values == year
        if year_mask.sum() < MIN_POINTS_PER_YEAR:
            continue

        n_out_avant_y = int(flag_avant[year_mask].sum())
        n_out_apres_y = int(flag_apres[year_mask].sum())

        # On ne trace que les années avec au moins un outlier (avant ou après)
        if n_out_avant_y == 0 and n_out_apres_y == 0:
            continue

        d_y = dates[year_mask]
        obs_y = obs_all[year_mask]
        pred_y = pred_all[year_mask]
        pred_corrige_y = pred_corrige_all[year_mask]
        ins_y = ins_z_all[year_mask]
        flag_avant_y = flag_avant[year_mask]
        flag_apres_y = flag_apres[year_mask]
        n_total_y = int(year_mask.sum())

        fig, axes = plt.subplots(2, 1, figsize=(11, 7), sharex=True)
        titre_insitu = f"  |  insitu {code_ins} ({dist_km:.1f} km)" if code_ins is not None else ""
        fig.suptitle(f"Station {station} — {year}{titre_insitu}\n"
                     f"{LABEL} [{SOURCE.upper()} {FREQ}]  —  seuil outlier = {OUTLIER_THRESHOLD}σ",
                     fontsize=12, fontweight="bold")

        panels = [
            (axes[0], pred_y, "AVANT correction", flag_avant_y, n_out_avant_y),
            (axes[1], pred_corrige_y, "APRÈS k-fold quantile mapping", flag_apres_y, n_out_apres_y),
        ]
        for ax, pred_to_plot, title, flags_y, n_out in panels:
            ax.plot(d_y, pred_to_plot, color="#1f77b4", lw=1.4, label="Prédiction modèle", zorder=2)

            ok = ~flags_y
            ax.scatter(d_y[ok], obs_y[ok], color="#ff7f0e", s=32, zorder=3, label="Alti (obs)")
            if flags_y.any():
                ax.scatter(d_y[flags_y], obs_y[flags_y], color="red", marker="x", s=90,
                          linewidths=2.2, zorder=4, label="Alti flaguée outlier")

            if AFFICHER_INSITU and not np.all(np.isnan(ins_y)):
                ax.plot(d_y, ins_y, color="#2ca02c", lw=1.2, alpha=0.6, ls=":",
                       label="Insitu (référence visuelle)", zorder=1)

            ax.set_title(f"{title}   —   {n_out}/{n_total_y} outliers flagués",
                        fontsize=10, fontweight="bold")
            ax.grid(True, alpha=0.25)
            ax.legend(loc="best", fontsize=8)

        axes[1].tick_params(axis="x", labelrotation=30)
        plt.tight_layout()

        out_path = station_dir / f"{LABEL}_{year}.png"
        fig.savefig(out_path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        n_saved += 1

    print(f"    -> {n_saved} PNG sauvegardés (années avec >=1 outlier) -> {station_dir}/")

print(f"\n✅ Terminé. Figures dans : {OUTLIER_ROOT}/{{station}}/{{label}}_{{annee}}.png")