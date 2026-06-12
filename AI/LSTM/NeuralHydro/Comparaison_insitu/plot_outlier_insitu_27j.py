"""
plot_outliers_with_insitu_27j.py
═══════════════════════════════════════════════════════════════════════════
Identique à plot_outliers_with_insitu.py mais pour les stations 27j.
  [1] Obs satellite vs Prédit modèle — outliers annotés
  [2] Obs satellite vs Station insitu la plus proche (z-score)

Usage :
  python plot_outliers_with_insitu_27j.py
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
MODEL    = "arlstm_feat27jHigh_modele2_2205_152119"
CSV_PATH = Path("./data/outlier_detection/residuals_27j_all_stations.csv")
OUTPUT_DIR = Path("./AI/LSTM/NeuralHydro/Comparaison_insitu/station_plot/27J")

HYDRO_DB_PATH  = "./data/hydro_data.db"
INSITU_DB_PATH = "./data/insitu_data.db"
INSITU_SHP     = "./data/insitu/shp/station_schapi_alti_ref_2025_river.gpkg"

DIST_MAX_KM       = 50.0
OUTLIER_THRESHOLD = 3.0

# ═══════════════════════════════════════════════════════════════
# CHARGEMENT STATIONS INSITU
# ═══════════════════════════════════════════════════════════════
print("Chargement des stations insitu...")
gdf_insitu      = gpd.read_file(INSITU_SHP)
gdf_insitu_proj = gdf_insitu.to_crs("EPSG:2154")
gdf_insitu_wgs  = gdf_insitu.to_crs("EPSG:4326")


def get_station_insitu_proche(lon_hydro, lat_hydro):
    point     = gpd.GeoSeries([Point(lon_hydro, lat_hydro)],
                              crs="EPSG:4326").to_crs("EPSG:2154")[0]
    distances = gdf_insitu_proj.geometry.distance(point)
    idx       = distances.idxmin()
    dist_km   = distances[idx] / 1000
    code_sta  = gdf_insitu_proj.loc[idx, 'code_sta']
    return code_sta, dist_km


def get_coords_hydro(station_code):
    conn = sqlite3.connect(HYDRO_DB_PATH)
    for code in [str(station_code), str(station_code).zfill(13)]:
        df = pd.read_sql_query(
            "SELECT reference_longitude, reference_latitude "
            "FROM stations WHERE station_code = ?",
            conn, params=(code,)
        )
        if not df.empty:
            conn.close()
            return float(df.iloc[0]['reference_longitude']), \
                   float(df.iloc[0]['reference_latitude'])
    conn.close()
    return None, None


def get_insitu_series(code_sta, date_min, date_max):
    conn = sqlite3.connect(INSITU_DB_PATH)
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
    df['date'] = pd.to_datetime(df['date'])
    df = df.dropna(subset=['h_med_wsh'])
    return df.rename(columns={'h_med_wsh': 'wl'}) if len(df) >= 5 else None


def zscore(arr):
    mu, sig = np.nanmean(arr), np.nanstd(arr)
    return (arr - mu) / sig if sig > 0 else arr - mu


def calc_nse_kge(obs, sim):
    mask = ~(np.isnan(obs) | np.isnan(sim))
    if mask.sum() < 5:
        return np.nan, np.nan
    o, s  = obs[mask], sim[mask]
    denom = np.sum((o - o.mean()) ** 2)
    nse   = 1 - np.sum((o - s) ** 2) / denom if denom > 0 else np.nan
    r     = np.corrcoef(o, s)[0, 1]
    alpha = s.std() / o.std() if o.std() > 0 else 0
    beta  = s.mean() / o.mean() if o.mean() != 0 else 0
    kge   = 1 - np.sqrt((r - 1)**2 + (alpha - 1)**2 + (beta - 1)**2)
    return nse, kge


def align_insitu_to_satellite(dates_sat, df_insitu, window_days=14):
    """Fenêtre plus large pour le 27j (±14j au lieu de ±5j)."""
    dates_sat_pd = pd.to_datetime(dates_sat)
    insitu_wl    = np.full(len(dates_sat_pd), np.nan)
    for i, d in enumerate(dates_sat_pd):
        diff = (df_insitu['date'] - d).abs()
        idx  = diff.idxmin()
        if diff[idx] <= pd.Timedelta(days=window_days):
            insitu_wl[i] = df_insitu.loc[idx, 'wl']
    return insitu_wl


# ═══════════════════════════════════════════════════════════════
# CACHE COORDS + INSITU PAR STATION
# ═══════════════════════════════════════════════════════════════
_cache = {}

def get_insitu_info(sta):
    if str(sta) not in _cache:
        lon_h, lat_h = get_coords_hydro(sta)
        if lon_h is None:
            _cache[str(sta)] = (None, None, None)
        else:
            code, dist = get_station_insitu_proche(lon_h, lat_h)
            if dist > DIST_MAX_KM:
                _cache[str(sta)] = (code, dist, None)
            else:
                df_ins = get_insitu_series(code, "2016-01-01", "2025-12-31")
                _cache[str(sta)] = (code, dist, df_ins)
    return _cache[str(sta)]


# ═══════════════════════════════════════════════════════════════
# CHARGEMENT RÉSIDUS 27J
# ═══════════════════════════════════════════════════════════════
df = pd.read_csv(CSV_PATH, parse_dates=['date'])
df['station']    = df['station'].astype(str)
df['is_outlier'] = df['is_outlier'].astype(bool)
df['year']       = df['date'].dt.year

stations = sorted(df['station'].unique())
print(f"📊 {len(stations)} stations\n")

n_plots = 0

for sta in stations:
    grp      = df[df['station'] == sta].sort_values('date')
    outliers = grp[grp['is_outlier']]

    if len(outliers) == 0:
        print(f"  {sta:>15s} | 0 outliers → skip")
        continue

    years_with_outliers = sorted(outliers['year'].unique())
    sta_dir = OUTPUT_DIR / sta
    sta_dir.mkdir(parents=True, exist_ok=True)

    code_insitu, dist_km, df_ins = get_insitu_info(sta)
    has_insitu = df_ins is not None

    for year in years_with_outliers:
        grp_year = grp[grp['year'] == year]
        out_year = outliers[outliers['year'] == year]

        insitu_wl_norm = None
        nse_ins = kge_ins = np.nan
        if has_insitu:
            insitu_wl      = align_insitu_to_satellite(grp_year['date'], df_ins)
            obs_norm       = zscore(grp_year['obs'].values)
            insitu_wl_norm = zscore(insitu_wl)
            nse_ins, kge_ins = calc_nse_kge(obs_norm, insitu_wl_norm)

        n_rows        = 2 if has_insitu else 1
        height_ratios = [3, 2] if has_insitu else [1]

        fig, axes = plt.subplots(
            n_rows, 1,
            figsize=(12, 4 + 3 * n_rows),
            gridspec_kw={'height_ratios': height_ratios, 'hspace': 0.08},
            sharex=True
        )
        ax_wl = axes[0] if has_insitu else axes
        ax_in = axes[1] if has_insitu else None

        # ── Panel modèle ─────────────────────────────────────────────────
        ax_wl.plot(grp_year['date'], grp_year['obs'], '-o', color='#5B9BD5',
                   markersize=5, linewidth=1, label='Observé', zorder=3)
        ax_wl.plot(grp_year['date'], grp_year['pred'], '-o', color='#E88B8B',
                   markersize=5, linewidth=1, label='Prédit', zorder=2)

        for _, row in out_year.iterrows():
            ax_wl.plot([row['date'], row['date']], [row['obs'], row['pred']],
                       color='red', linewidth=2, alpha=0.7, zorder=4)
            ax_wl.scatter(row['date'], row['obs'], s=150, facecolors='none',
                          edgecolors='red', linewidths=2, zorder=5)
            ax_wl.annotate(f"{row['residual_norm']:+.1f}σ",
                           xy=(row['date'], row['obs']),
                           xytext=(0, 12 if row['residual'] > 0 else -14),
                           textcoords='offset points',
                           fontsize=9, color='red', fontweight='bold',
                           ha='center',
                           va='bottom' if row['residual'] > 0 else 'top')

        n_out = len(out_year)
        ax_wl.set_title(
            f"Station {sta}  —  {year}  —  "
            f"{n_out} outlier{'s' if n_out > 1 else ''}",
            fontsize=11, fontweight='bold'
        )
        ax_wl.set_ylabel('WL (z-score)')
        ax_wl.legend(loc='upper right', fontsize=8)
        ax_wl.grid(True, alpha=0.3, linestyle='--')
        ax_wl.axhline(0, color='grey', linewidth=0.5, linestyle='--')
        if not has_insitu:
            ax_wl.set_xlabel('Date')
            ax_wl.xaxis.set_major_formatter(mdates.DateFormatter('%b'))
            ax_wl.xaxis.set_major_locator(mdates.MonthLocator())
        else:
            ax_wl.tick_params(axis='x', labelbottom=False)
            ax_wl.spines['bottom'].set_visible(False)

        # ── Panel insitu ─────────────────────────────────────────────────
        if has_insitu:
            obs_norm_plot = zscore(grp_year['obs'].values)
            ax_in.plot(grp_year['date'], obs_norm_plot, '-o', color='#5B9BD5',
                       markersize=4, linewidth=1, label='Obs satellite', zorder=3)
            ax_in.plot(grp_year['date'], insitu_wl_norm, '-^', color='darkorange',
                       markersize=4, linewidth=1, alpha=0.85,
                       label=f"Insitu {code_insitu} ({dist_km:.1f} km)  "
                             f"NSE={nse_ins:.2f} | KGE={kge_ins:.2f}",
                       zorder=2)

            for _, row in out_year.iterrows():
                ax_in.axvline(row['date'], color='red', lw=1.2,
                              alpha=0.4, linestyle='--', zorder=1)

            # Outliers insitu (violet)
            residus_ins = obs_norm_plot - insitu_wl_norm
            std_ins = np.nanstd(residus_ins)
            if std_ins > 0:
                residu_norm_ins = residus_ins / std_ins
                for d, rn, ri, obs_v, ins_v in zip(
                    grp_year['date'].values, residu_norm_ins, residus_ins,
                    obs_norm_plot, insitu_wl_norm
                ):
                    if np.isnan(rn) or np.isnan(ins_v):
                        continue
                    if abs(rn) > OUTLIER_THRESHOLD:
                        ax_in.plot([d, d], [obs_v, ins_v],
                                   color='darkorchid', linewidth=2,
                                   alpha=0.8, zorder=4)
                        ax_in.scatter(d, obs_v, s=150, facecolors='none',
                                      edgecolors='darkorchid', linewidths=2,
                                      zorder=5)
                        ax_in.annotate(f"{rn:+.1f}σ",
                                       xy=(d, obs_v),
                                       xytext=(0, 12 if ri > 0 else -14),
                                       textcoords='offset points',
                                       fontsize=9, color='darkorchid',
                                       fontweight='bold', ha='center',
                                       va='bottom' if ri > 0 else 'top')

            ax_in.axhline(0, color='grey', linewidth=0.5, linestyle='--')
            ax_in.set_ylabel('WL (z-score)')
            ax_in.set_xlabel('Date')
            ax_in.legend(loc='upper right', fontsize=8)
            ax_in.grid(True, alpha=0.3, linestyle='--')
            ax_in.xaxis.set_major_formatter(mdates.DateFormatter('%b'))
            ax_in.xaxis.set_major_locator(mdates.MonthLocator())

        plt.tight_layout()
        out_path = sta_dir / f"outlier_{sta}_{year}.png"
        fig.savefig(out_path, dpi=150, bbox_inches='tight')
        plt.close(fig)
        n_plots += 1

    insitu_str = f"insitu={code_insitu} ({dist_km:.1f}km)" if has_insitu else "pas d'insitu"
    print(f"  {sta:>15s} | {len(outliers):2d} outliers | "
          f"{len(years_with_outliers)} années | {insitu_str}")

print(f"\n✅ {n_plots} figures dans {OUTPUT_DIR}")