"""
plot_outliers_with_insitu_sigma0_27j.py
═══════════════════════════════════════════════════════════════════════════
Pour chaque station 27j, génère un plot par année (TOUTES les années).
Le titre indique si l'année contient des outliers.

Panels :
  [1] Obs satellite vs Prédit modèle — outliers annotés (rouge)
  [2] Obs satellite vs Station insitu la plus proche
  [3] WSH brut vs date coloré par sigma0
═══════════════════════════════════════════════════════════════════════════
"""

import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.colors as mcolors
import matplotlib.cm as cm
import geopandas as gpd
from pathlib import Path
from shapely.geometry import Point

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
CSV_RESIDUALS = Path("./data/outlier_detection/residuals_27j_all_stations.csv")
CSV_SIGMA0    = Path("./data/sigma0/sigma0_all_stations.csv")
OUTPUT_DIR    = Path("./AI/LSTM/NeuralHydro/Comparaison_insitu/station_plot/27J")

HYDRO_DB_PATH  = "./data/hydro_data.db"
INSITU_DB_PATH = "./data/insitu_data.db"
INSITU_SHP     = "./data/insitu/shp/station_schapi_alti_ref_2025_river.gpkg"

DIST_MAX_KM       = 50.0
OUTLIER_THRESHOLD = 3.0
SIGMA0_SEUIL      = 30.0


# ═══════════════════════════════════════════════════════════════
# CHARGEMENT DONNÉES
# ═══════════════════════════════════════════════════════════════
print("Chargement des stations insitu...")
gdf_insitu      = gpd.read_file(INSITU_SHP)
gdf_insitu_proj = gdf_insitu.to_crs("EPSG:2154")

print("Chargement sigma0...")
df_sigma = pd.read_csv(CSV_SIGMA0, parse_dates=['date'])
df_sigma['station_code'] = df_sigma['station_code'].astype(str)
sigma0_min = df_sigma['sigma0'].quantile(0.02)
sigma0_max = df_sigma['sigma0'].quantile(0.98)
norm_s0    = mcolors.Normalize(vmin=sigma0_min, vmax=sigma0_max)
cmap_s0    = cm.RdYlGn

print("Chargement résidus 27j...")
df = pd.read_csv(CSV_RESIDUALS, parse_dates=['date'])
df['station']    = df['station'].astype(str)
df['is_outlier'] = df['is_outlier'].astype(bool)
df['year']       = df['date'].dt.year


# ═══════════════════════════════════════════════════════════════
# FONCTIONS
# ═══════════════════════════════════════════════════════════════
def get_coords_hydro(station_code):
    conn = sqlite3.connect(HYDRO_DB_PATH)
    for code in [str(station_code), str(station_code).zfill(13)]:
        df_q = pd.read_sql_query(
            "SELECT reference_longitude, reference_latitude "
            "FROM stations WHERE station_code = ?",
            conn, params=(code,)
        )
        if not df_q.empty:
            conn.close()
            return float(df_q.iloc[0]['reference_longitude']), \
                   float(df_q.iloc[0]['reference_latitude'])
    conn.close()
    return None, None


def get_station_insitu_proche(lon_hydro, lat_hydro):
    point     = gpd.GeoSeries([Point(lon_hydro, lat_hydro)],
                              crs="EPSG:4326").to_crs("EPSG:2154")[0]
    distances = gdf_insitu_proj.geometry.distance(point)
    idx       = distances.idxmin()
    return gdf_insitu_proj.loc[idx, 'code_sta'], distances[idx] / 1000


def get_insitu_series(code_sta):
    conn = sqlite3.connect(INSITU_DB_PATH)
    df_q = pd.read_sql_query("""
        SELECT date, h_med_wsh FROM mesures_insitu
        WHERE code_sta = ? ORDER BY date
    """, conn, params=(code_sta,))
    conn.close()
    if df_q.empty:
        return None
    df_q['date'] = pd.to_datetime(df_q['date'])
    df_q = df_q.dropna(subset=['h_med_wsh'])
    return df_q.rename(columns={'h_med_wsh': 'wl'}) if len(df_q) >= 5 else None


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


def align_insitu(dates_sat, df_ins, window_days=14):
    wl = np.full(len(dates_sat), np.nan)
    for i, d in enumerate(pd.to_datetime(dates_sat)):
        diff = (df_ins['date'] - d).abs()
        idx  = diff.idxmin()
        if diff[idx] <= pd.Timedelta(days=window_days):
            wl[i] = df_ins.loc[idx, 'wl']
    return wl


# ═══════════════════════════════════════════════════════════════
# CACHE INSITU
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
                _cache[str(sta)] = (code, dist, get_insitu_series(code))
    return _cache[str(sta)]


# ═══════════════════════════════════════════════════════════════
# GÉNÉRATION DES FIGURES — TOUTES LES ANNÉES
# ═══════════════════════════════════════════════════════════════
stations = sorted(df['station'].unique())
print(f"\n📊 {len(stations)} stations\n")
n_plots = 0

for sta in stations:
    grp      = df[df['station'] == sta].sort_values('date')
    outliers = grp[grp['is_outlier']]

    # ── Toutes les années de la station (pas seulement celles avec outliers)
    all_years = sorted(grp['year'].unique())
    years_with_outliers = set(outliers['year'].unique())

    if len(all_years) == 0:
        continue

    sta_dir = OUTPUT_DIR / sta
    sta_dir.mkdir(parents=True, exist_ok=True)

    # Matching insitu (une fois par station)
    code_insitu, dist_km, df_ins = get_insitu_info(sta)
    has_insitu = df_ins is not None

    # Sigma0 de la station
    sig_sta = df_sigma[df_sigma['station_code'] == sta].sort_values('date')
    has_sigma0 = not sig_sta.empty

    n_out_total = len(outliers)
    print(f"  {sta:>15s} | {n_out_total:2d} outliers | "
          f"{len(all_years)} années | "
          f"insitu={'oui' if has_insitu else 'non'} | "
          f"sigma0={'oui' if has_sigma0 else 'non'}")

    for year in all_years:
        grp_year = grp[grp['year'] == year]
        out_year = outliers[outliers['year'] == year]
        has_outliers_this_year = year in years_with_outliers
        n_out_year = len(out_year)

        # Insitu aligné sur l'année
        insitu_wl_norm = None
        nse_ins = kge_ins = np.nan
        if has_insitu:
            insitu_wl      = align_insitu(grp_year['date'], df_ins)
            obs_norm_full  = zscore(grp_year['obs'].values)
            insitu_wl_norm = zscore(insitu_wl)
            nse_ins, kge_ins = calc_nse_kge(obs_norm_full, insitu_wl_norm)

        # Sigma0 de l'année
        sig_year = sig_sta[
            sig_sta['date'].dt.year == year
        ] if has_sigma0 else pd.DataFrame()

        # ── Layout dynamique ─────────────────────────────────────────────
        n_rows = 1 + int(has_insitu) + int(has_sigma0 and not sig_year.empty)
        if n_rows == 1:
            height_ratios = [3]
        elif n_rows == 2:
            height_ratios = [3, 2]
        else:
            height_ratios = [3, 2, 2]

        fig, axes_list = plt.subplots(
            n_rows, 1,
            figsize=(13, 3 + 3 * n_rows),
            gridspec_kw={'height_ratios': height_ratios, 'hspace': 0.08},
            sharex=True
        )
        if n_rows == 1:
            axes_list = [axes_list]

        ax_wl = axes_list[0]
        ax_in = axes_list[1] if has_insitu and n_rows >= 2 else None
        ax_s0 = axes_list[-1] if has_sigma0 and not sig_year.empty and n_rows >= 2 else None
        if ax_in is ax_s0:
            ax_s0 = None

        # ── Titre : indique présence/absence d'outliers ──────────────────
        if has_outliers_this_year:
            title = (f"Station {sta}  —  {year}  —  "
                     f"⚠ {n_out_year} OUTLIER{'S' if n_out_year > 1 else ''} DÉTECTÉ{'S' if n_out_year > 1 else ''}")
            title_color = "red"
        else:
            title = f"Station {sta}  —  {year}  —  ✓ Aucun outlier"
            title_color = "green"

        # ── Panel 1 : modèle ─────────────────────────────────────────────
        ax_wl.plot(grp_year['date'], grp_year['obs'], '-o', color='#5B9BD5',
                   markersize=5, lw=1, label='Observé', zorder=3)
        ax_wl.plot(grp_year['date'], grp_year['pred'], '-o', color='#E88B8B',
                   markersize=5, lw=1, label='Prédit', zorder=2)

        for _, row in out_year.iterrows():
            ax_wl.plot([row['date'], row['date']], [row['obs'], row['pred']],
                       color='red', lw=2, alpha=0.7, zorder=4)
            ax_wl.scatter(row['date'], row['obs'], s=150, facecolors='none',
                          edgecolors='red', lw=2, zorder=5)
            ax_wl.annotate(f"{row['residual_norm']:+.1f}σ",
                           xy=(row['date'], row['obs']),
                           xytext=(0, 12 if row['residual'] > 0 else -14),
                           textcoords='offset points',
                           fontsize=9, color='red', fontweight='bold',
                           ha='center',
                           va='bottom' if row['residual'] > 0 else 'top')

        ax_wl.set_title(title, fontsize=11, fontweight='bold', color=title_color)
        ax_wl.set_ylabel('WL (z-score)')
        ax_wl.legend(loc='upper right', fontsize=8)
        ax_wl.grid(True, alpha=0.3, ls='--')
        ax_wl.axhline(0, color='grey', lw=0.5, ls='--')
        ax_wl.tick_params(axis='x', labelbottom=False)
        ax_wl.spines['bottom'].set_visible(False)

        # ── Panel 2 : insitu ─────────────────────────────────────────────
        if ax_in is not None and insitu_wl_norm is not None:
            obs_norm_plot = zscore(grp_year['obs'].values)
            ax_in.plot(grp_year['date'], obs_norm_plot, '-o', color='#5B9BD5',
                       markersize=4, lw=1, label='Obs satellite', zorder=3)
            ax_in.plot(grp_year['date'], insitu_wl_norm, '-^', color='darkorange',
                       markersize=4, lw=1, alpha=0.85,
                       label=f"Insitu {code_insitu} ({dist_km:.1f} km)  "
                             f"NSE={nse_ins:.2f} | KGE={kge_ins:.2f}",
                       zorder=2)

            for _, row in out_year.iterrows():
                ax_in.axvline(row['date'], color='red', lw=1.2,
                              alpha=0.4, ls='--', zorder=1)

            # Outliers insitu (violet)
            residus_ins = obs_norm_plot - insitu_wl_norm
            std_ins = np.nanstd(residus_ins)
            if std_ins > 0:
                rn_ins = residus_ins / std_ins
                for d, rn, ri, ov, iv in zip(
                    grp_year['date'].values, rn_ins, residus_ins,
                    obs_norm_plot, insitu_wl_norm
                ):
                    if np.isnan(rn) or np.isnan(iv) or abs(rn) <= OUTLIER_THRESHOLD:
                        continue
                    ax_in.plot([d, d], [ov, iv], color='darkorchid', lw=2,
                               alpha=0.8, zorder=4)
                    ax_in.scatter(d, ov, s=150, facecolors='none',
                                  edgecolors='darkorchid', lw=2, zorder=5)
                    ax_in.annotate(f"{rn:+.1f}σ", xy=(d, ov),
                                   xytext=(0, 12 if ri > 0 else -14),
                                   textcoords='offset points', fontsize=9,
                                   color='darkorchid', fontweight='bold',
                                   ha='center',
                                   va='bottom' if ri > 0 else 'top')

            ax_in.axhline(0, color='grey', lw=0.5, ls='--')
            ax_in.set_ylabel('WL (z-score)')
            ax_in.legend(loc='upper right', fontsize=8)
            ax_in.grid(True, alpha=0.3, ls='--')
            if ax_s0 is None:
                ax_in.set_xlabel('Date')
                ax_in.xaxis.set_major_formatter(mdates.DateFormatter('%b'))
                ax_in.xaxis.set_major_locator(mdates.MonthLocator())
            else:
                ax_in.tick_params(axis='x', labelbottom=False)
                ax_in.spines['bottom'].set_visible(False)

        # ── Panel 3 : WSH coloré par sigma0 ──────────────────────────────
        if ax_s0 is not None and not sig_year.empty:
            sc = ax_s0.scatter(
                sig_year['date'], sig_year['WSH'],
                c=sig_year['sigma0'], cmap=cmap_s0, norm=norm_s0,
                s=60, edgecolors='white', lw=0.4, zorder=3
            )
            sus = sig_year[sig_year['sigma0'] < SIGMA0_SEUIL]
            if len(sus) > 0:
                ax_s0.scatter(sus['date'], sus['WSH'], s=120,
                              facecolors='none', edgecolors='red',
                              lw=1.5, zorder=4,
                              label=f"σ0 < {SIGMA0_SEUIL} dB (n={len(sus)})")

            for _, row in out_year.iterrows():
                ax_s0.axvline(row['date'], color='red', lw=1.2,
                              alpha=0.4, ls='--', zorder=1)

            plt.colorbar(sc, ax=ax_s0, label='Sigma0 (dB)')
            ax_s0.set_ylabel('WSH (m)')
            ax_s0.set_xlabel('Date')
            if len(sus) > 0:
                ax_s0.legend(loc='upper right', fontsize=8)
            ax_s0.grid(True, alpha=0.3, ls='--')
            ax_s0.xaxis.set_major_formatter(mdates.DateFormatter('%b'))
            ax_s0.xaxis.set_major_locator(mdates.MonthLocator())

        elif n_rows == 1 or (ax_in is None and ax_s0 is None):
            ax_wl.set_xlabel('Date')
            ax_wl.tick_params(axis='x', labelbottom=True)
            ax_wl.xaxis.set_major_formatter(mdates.DateFormatter('%b'))
            ax_wl.xaxis.set_major_locator(mdates.MonthLocator())

        plt.tight_layout()
        out_path = sta_dir / f"outlier_{sta}_{year}.png"
        fig.savefig(out_path, dpi=150, bbox_inches='tight')
        plt.close(fig)
        n_plots += 1

print(f"\n✅ {n_plots} figures dans {OUTPUT_DIR}")