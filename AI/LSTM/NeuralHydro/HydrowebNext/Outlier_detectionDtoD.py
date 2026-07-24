"""
plot_outliers_hwnext_DtoD80.py
═══════════════════════════════════════════════════════════════════════════
Plots station par station, année par année, pour le modèle DtoD80 (le
meilleur retenu sur HW Next, à la fois sur 10j et 27j) :
  [1] Obs satellite vs Prédit modèle — outliers annotés
  [2] Obs satellite vs Station insitu la plus proche (z-score)

Les stations 10j et 27j sont traitées séparément et rangées dans deux
sous-dossiers distincts.

Sources résidus :
  10j : ./data/outlier_detection/benchmark_DtoD_hwnext10j/residuals_hwnext_10j_80pct.csv
  27j : ./data/outlier_detection/benchmark_DtoD_hwnext27j/residuals_hwnext_27j_80pct.csv

Sorties :
  ./AI/LSTM/NeuralHydro/HydrowebNext/plot/Outlier_detection/10J/{station}/outlier_{station}_{year}.png
  ./AI/LSTM/NeuralHydro/HydrowebNext/plot/Outlier_detection/27J/{station}/outlier_{station}_{year}.png

Usage :
  python plot_outliers_hwnext_DtoD80.py
═══════════════════════════════════════════════════════════════════════════
"""

import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import geopandas as gpd
from pathlib import Path
from shapely.geometry import Point

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
MASK_PCT = 80
MODEL_NAME = "arlstm_DtoD80_1506_150002"

SOURCES = {
    "10J": {
        "csv": Path("./data/outlier_detection/benchmark_DtoD_hwnext10j/residuals_hwnext_10j_80pct.csv"),
        "window_days": 7,
    },
    "27J": {
        "csv": Path("./data/outlier_detection/benchmark_DtoD_hwnext27j/residuals_hwnext_27j_80pct.csv"),
        "window_days": 14,
    },
}

HWNEXT_DB  = "./data/hydroweb_next.db"
INSITU_DB  = "./data/insitu_data.db"
INSITU_SHP = "./data/insitu/shp/station_schapi_alti_ref_2025_river.gpkg"

OUTPUT_BASE = Path("./AI/LSTM/NeuralHydro/HydrowebNext/plot/Outlier_detection")

DATE_MIN = "2016-01-01"
DATE_MAX = "2025-12-31"
DIST_MAX_KM       = 50.0
OUTLIER_THRESHOLD = 3.0

# ═══════════════════════════════════════════════════════════════
# CHARGEMENT STATIONS INSITU
# ═══════════════════════════════════════════════════════════════
print("Chargement des stations insitu...")
gdf_insitu      = gpd.read_file(INSITU_SHP)
gdf_insitu_proj = gdf_insitu.to_crs("EPSG:2154")


def get_station_insitu_proche(lon, lat):
    point     = gpd.GeoSeries([Point(lon, lat)], crs="EPSG:4326").to_crs("EPSG:2154")[0]
    distances = gdf_insitu_proj.geometry.distance(point)
    idx       = distances.idxmin()
    dist_km   = distances[idx] / 1000
    code_sta  = gdf_insitu_proj.loc[idx, "code_sta"]
    return code_sta, dist_km


def get_coords_hwnext(station_code):
    conn = sqlite3.connect(HWNEXT_DB)
    for code in [str(station_code), str(station_code).zfill(13)]:
        df = pd.read_sql_query(
            "SELECT reference_longitude, reference_latitude "
            "FROM stations WHERE station_code = ?",
            conn, params=(code,)
        )
        if not df.empty:
            conn.close()
            return float(df.iloc[0]["reference_longitude"]), \
                   float(df.iloc[0]["reference_latitude"])
    conn.close()
    return None, None


def get_insitu_series(code_sta, date_min, date_max):
    conn = sqlite3.connect(INSITU_DB)
    df   = pd.read_sql_query("""
        SELECT date, h_med_wsh
        FROM mesures_insitu
        WHERE code_sta = ?
          AND date >= ?
          AND date <= ?
        ORDER BY date
    """, conn, params=(code_sta, str(date_min)[:10], str(date_max)[:10]))
    conn.close()
    if df.empty:
        return None
    df["date"] = pd.to_datetime(df["date"])
    df = df.dropna(subset=["h_med_wsh"])
    return df.rename(columns={"h_med_wsh": "wl"}) if len(df) >= 5 else None


def zscore(arr):
    mu, sig = np.nanmean(arr), np.nanstd(arr)
    return (arr - mu) / sig if sig > 0 else arr - mu


def calc_nse_kge(obs, sim):
    mask = ~(np.isnan(obs) | np.isnan(sim))
    if mask.sum() < 5:
        return np.nan, np.nan
    o, s  = obs[mask], sim[mask]
    denom = np.sum((o - o.mean()) ** 2)
    nse_v = 1 - np.sum((o - s) ** 2) / denom if denom > 0 else np.nan
    r     = np.corrcoef(o, s)[0, 1]
    alpha = s.std() / o.std() if o.std() > 0 else 0
    beta  = s.mean() / o.mean() if o.mean() != 0 else 0
    kge_v = 1 - np.sqrt((r - 1) ** 2 + (alpha - 1) ** 2 + (beta - 1) ** 2)
    return nse_v, kge_v


def align_insitu_to_satellite(dates_sat, df_insitu, window_days):
    dates_sat_pd = pd.to_datetime(dates_sat)
    insitu_wl    = np.full(len(dates_sat_pd), np.nan)
    for i, d in enumerate(dates_sat_pd):
        diff = (df_insitu["date"] - d).abs()
        idx  = diff.idxmin()
        if diff[idx] <= pd.Timedelta(days=window_days):
            insitu_wl[i] = df_insitu.loc[idx, "wl"]
    return insitu_wl


# ═══════════════════════════════════════════════════════════════
# DÉTECTION D'OUTLIERS — résidu normalisé + pénalité de direction
# ═══════════════════════════════════════════════════════════════
def compute_score(residual_norm, obs_arr, pred_arr, i):
    """
    Score outlier avec pénalité de direction.
    Référentiel j0_ref = dernière VRAIE observation satellite disponible
    avant i (jamais la prédiction du modèle), peu importe le nombre de
    jours d'écart. Si aucune observation antérieure n'existe, retombe sur
    le résidu normalisé brut (pas de pénalité de direction calculable).
    """
    if np.isnan(residual_norm):
        return np.nan

    j0_ref = np.nan
    for j in range(i - 1, -1, -1):
        if not np.isnan(obs_arr[j]):
            j0_ref = obs_arr[j]
            break

    if np.isnan(j0_ref):
        return abs(residual_norm)

    j1_obs  = obs_arr[i]
    j1_pred = pred_arr[i]
    if np.isnan(j1_obs) or np.isnan(j1_pred):
        return abs(residual_norm)

    delta_alti  = j1_obs  - j0_ref
    delta_model = j1_pred - j0_ref
    eps = 1e-8
    cos = (delta_alti * delta_model) / (
        np.sqrt(delta_alti**2 + eps) * np.sqrt(delta_model**2 + eps)
    )
    cos_pen  = (1 - cos) / 2          # 0 si même direction, 1 si direction opposée
    amp      = (abs(delta_alti) + abs(delta_model)) / 2
    penalite = cos_pen * np.tanh(amp)
    return abs(residual_norm) * (1 + penalite)


def detect_outliers(df_station):
    """Calcule residual, residual_norm, score (avec pénalité de direction)
    et is_outlier sur le résidu modèle vs obs satellite, pour une station."""
    df_station = df_station.sort_values("date").reset_index(drop=True).copy()

    df_station["residual"] = df_station["obs"] - df_station["pred"]
    std = df_station["residual"].std()
    df_station["residual_norm"] = df_station["residual"] / std if std > 0 else np.nan

    obs_arr  = df_station["obs"].values
    pred_arr = df_station["pred"].values
    rn_arr   = df_station["residual_norm"].values

    scores = np.full(len(df_station), np.nan)
    for i in range(len(df_station)):
        scores[i] = compute_score(rn_arr[i], obs_arr, pred_arr, i)

    df_station["score"]      = scores
    df_station["is_outlier"] = df_station["score"].abs() > OUTLIER_THRESHOLD
    return df_station


# ═══════════════════════════════════════════════════════════════
# CACHE COORDS + INSITU PAR STATION
# ═══════════════════════════════════════════════════════════════
_cache = {}

def get_insitu_info(sta):
    if str(sta) not in _cache:
        lon, lat = get_coords_hwnext(sta)
        if lon is None:
            _cache[str(sta)] = (None, None, None)
        else:
            code, dist = get_station_insitu_proche(lon, lat)
            if dist > DIST_MAX_KM:
                _cache[str(sta)] = (code, dist, None)
            else:
                df_ins = get_insitu_series(code, DATE_MIN, DATE_MAX)
                _cache[str(sta)] = (code, dist, df_ins)
    return _cache[str(sta)]


# ═══════════════════════════════════════════════════════════════
# TRAITEMENT PAR SOURCE (10J / 27J)
# ═══════════════════════════════════════════════════════════════
for label, cfg in SOURCES.items():
    csv_path    = cfg["csv"]
    window_days = cfg["window_days"]

    print(f"\n{'='*70}")
    print(f"  {label}  -  {csv_path}")
    print(f"{'='*70}")

    if not csv_path.exists():
        print(f"  Fichier introuvable -> skip")
        continue

    df_all = pd.read_csv(csv_path, parse_dates=["date"])
    df_all["station"] = df_all["station"].astype(str)
    df_all = df_all.dropna(subset=["obs", "pred"])
    df_all["year"] = df_all["date"].dt.year

    output_dir = OUTPUT_BASE / label
    stations = sorted(df_all["station"].unique())
    print(f"  {len(stations)} stations\n")

    n_plots = 0

    for sta in stations:
        grp = detect_outliers(df_all[df_all["station"] == sta].sort_values("date"))
        outliers = grp[grp["is_outlier"]]

        if len(outliers) == 0:
            continue

        years_with_outliers = sorted(outliers["year"].unique())
        sta_dir = output_dir / sta
        sta_dir.mkdir(parents=True, exist_ok=True)

        code_insitu, dist_km, df_ins = get_insitu_info(sta)
        has_insitu = df_ins is not None

        for year in years_with_outliers:
            grp_year = grp[grp["year"] == year]
            out_year = outliers[outliers["year"] == year]

            insitu_wl_norm = None
            nse_ins = kge_ins = np.nan
            if has_insitu:
                insitu_wl      = align_insitu_to_satellite(grp_year["date"], df_ins, window_days)
                obs_norm       = zscore(grp_year["obs"].values)
                insitu_wl_norm = zscore(insitu_wl)
                nse_ins, kge_ins = calc_nse_kge(obs_norm, insitu_wl_norm)

            n_rows        = 2 if has_insitu else 1
            height_ratios = [3, 2] if has_insitu else [1]

            fig, axes = plt.subplots(
                n_rows, 1,
                figsize=(12, 4 + 3 * n_rows),
                gridspec_kw={"height_ratios": height_ratios, "hspace": 0.08},
                sharex=True
            )
            ax_wl = axes[0] if has_insitu else axes
            ax_in = axes[1] if has_insitu else None

            # -- Panel modele --
            ax_wl.plot(grp_year["date"], grp_year["obs"], "-o", color="#5B9BD5",
                       markersize=5, linewidth=1, label="Observe", zorder=3)
            ax_wl.plot(grp_year["date"], grp_year["pred"], "-o", color="#E88B8B",
                       markersize=5, linewidth=1, label="Predit", zorder=2)

            for _, row in out_year.iterrows():
                ax_wl.plot([row["date"], row["date"]], [row["obs"], row["pred"]],
                           color="red", linewidth=2, alpha=0.7, zorder=4)
                ax_wl.scatter(row["date"], row["obs"], s=150, facecolors="none",
                              edgecolors="red", linewidths=2, zorder=5)
                ax_wl.annotate(f"{row['score']:+.1f} (rn={row['residual_norm']:+.1f}s)",
                               xy=(row["date"], row["obs"]),
                               xytext=(0, 12 if row["residual"] > 0 else -14),
                               textcoords="offset points",
                               fontsize=8, color="red", fontweight="bold",
                               ha="center",
                               va="bottom" if row["residual"] > 0 else "top")

            n_out = len(out_year)
            ax_wl.set_title(
                f"Station {sta} ({label})  -  {year}  -  "
                f"{n_out} outlier{'s' if n_out > 1 else ''}  -  modele {MODEL_NAME}",
                fontsize=11, fontweight="bold"
            )
            ax_wl.set_ylabel("WL (z-score)")
            ax_wl.legend(loc="upper right", fontsize=8)
            ax_wl.grid(True, alpha=0.3, linestyle="--")
            ax_wl.axhline(0, color="grey", linewidth=0.5, linestyle="--")
            if not has_insitu:
                ax_wl.set_xlabel("Date")
                ax_wl.xaxis.set_major_formatter(mdates.DateFormatter("%b"))
                ax_wl.xaxis.set_major_locator(mdates.MonthLocator())
            else:
                ax_wl.tick_params(axis="x", labelbottom=False)
                ax_wl.spines["bottom"].set_visible(False)

            # -- Panel insitu --
            if has_insitu:
                obs_norm_plot = zscore(grp_year["obs"].values)
                ax_in.plot(grp_year["date"], obs_norm_plot, "-o", color="#5B9BD5",
                           markersize=4, linewidth=1, label="Obs satellite", zorder=3)
                ax_in.plot(grp_year["date"], insitu_wl_norm, "-^", color="darkorange",
                           markersize=4, linewidth=1, alpha=0.85,
                           label=f"Insitu {code_insitu} ({dist_km:.1f} km)  "
                                 f"NSE={nse_ins:.2f} | KGE={kge_ins:.2f}",
                           zorder=2)

                for _, row in out_year.iterrows():
                    ax_in.axvline(row["date"], color="red", lw=1.2,
                                  alpha=0.4, linestyle="--", zorder=1)

                residus_ins = obs_norm_plot - insitu_wl_norm
                std_ins = np.nanstd(residus_ins)
                if std_ins > 0:
                    residu_norm_ins = residus_ins / std_ins
                    for d, rn, ri, obs_v, ins_v in zip(
                        grp_year["date"].values, residu_norm_ins, residus_ins,
                        obs_norm_plot, insitu_wl_norm
                    ):
                        if np.isnan(rn) or np.isnan(ins_v):
                            continue
                        if abs(rn) > OUTLIER_THRESHOLD:
                            ax_in.plot([d, d], [obs_v, ins_v],
                                       color="darkorchid", linewidth=2,
                                       alpha=0.8, zorder=4)
                            ax_in.scatter(d, obs_v, s=150, facecolors="none",
                                          edgecolors="darkorchid", linewidths=2,
                                          zorder=5)
                            ax_in.annotate(f"{rn:+.1f}s",
                                           xy=(d, obs_v),
                                           xytext=(0, 12 if ri > 0 else -14),
                                           textcoords="offset points",
                                           fontsize=9, color="darkorchid",
                                           fontweight="bold", ha="center",
                                           va="bottom" if ri > 0 else "top")

                ax_in.axhline(0, color="grey", linewidth=0.5, linestyle="--")
                ax_in.set_ylabel("WL (z-score)")
                ax_in.set_xlabel("Date")
                ax_in.legend(loc="upper right", fontsize=8)
                ax_in.grid(True, alpha=0.3, linestyle="--")
                ax_in.xaxis.set_major_formatter(mdates.DateFormatter("%b"))
                ax_in.xaxis.set_major_locator(mdates.MonthLocator())

            plt.tight_layout()
            out_path = sta_dir / f"outlier_{sta}_{year}.png"
            fig.savefig(out_path, dpi=150, bbox_inches="tight")
            plt.close(fig)
            n_plots += 1

        insitu_str = f"insitu={code_insitu} ({dist_km:.1f}km)" if has_insitu else "pas d'insitu"
        print(f"  {sta:>15s} | {len(outliers):2d} outliers | "
              f"{len(years_with_outliers)} annees | {insitu_str}")

    print(f"\n  {n_plots} figures dans {output_dir}")

print(f"\n{'='*70}")
print(f"  TERMINE - sorties dans {OUTPUT_BASE}/")
print(f"{'='*70}")