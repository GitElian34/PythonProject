"""
plot_model_vs_sources_27j.py
═══════════════════════════════════════════════════════════════════════════
Pour chaque station 27j, génère un graphique avec 3 panels :
  [1] Modèle vs Alti HydroWeb (z-score)
  [2] Modèle vs WSH CLS (z-score)
  [3] Modèle vs Insitu la plus proche (z-score)

Produit :
  ./figures_zeroshot_satellite/<MODEL>/Comparaison_sources/<station>.png
═══════════════════════════════════════════════════════════════════════════
"""

import sqlite3
import pickle
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import geopandas as gpd
from pathlib import Path
from shapely.geometry import Point

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
MODEL  = "arlstm_feat27jHigh_modele2_0206_145147"
EPOCH  = 27
PERIOD = "validation"

RUN_DIR        = Path(f"./runs/{MODEL}")
RESULTS_P      = RUN_DIR / PERIOD / f"model_epoch{EPOCH:03d}" / f"{PERIOD}_results.p"
SIGMA0_DIR     = Path("./data/sigma0")
HYDRO_DB_PATH  = "./data/hydro_data.db"
INSITU_DB_PATH = "./data/insitu_data.db"
INSITU_SHP     = "./data/insitu/shp/station_schapi_alti_ref_2025_river.gpkg"
OUT_DIR        = Path(f"./figures_zeroshot_satellite/{MODEL}/Comparaison_sources")
OUT_DIR.mkdir(parents=True, exist_ok=True)

TARGET_VAR  = "water_level"
DIST_MAX_KM = 50.0


# ═══════════════════════════════════════════════════════════════
# FONCTIONS
# ═══════════════════════════════════════════════════════════════
def zscore(arr):
    arr = np.array(arr, dtype=float)
    mu  = np.nanmean(arr)
    sig = np.nanstd(arr)
    return (arr - mu) / sig if sig > 0 else arr * 0.0


def calc_metrics(obs, sim):
    mask = ~(np.isnan(obs) | np.isnan(sim))
    if mask.sum() < 5:
        return np.nan, np.nan, np.nan
    o, s = obs[mask], sim[mask]
    # NSE
    denom = np.sum((o - o.mean())**2)
    nse   = 1 - np.sum((o - s)**2) / denom if denom > 0 else np.nan
    # Pearson r
    r     = np.corrcoef(o, s)[0, 1] if len(o) > 2 else np.nan
    # RMSE
    rmse  = np.sqrt(np.mean((o - s)**2))
    return nse, r, rmse


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


def get_insitu_proche(lon_h, lat_h, gdf_proj, gdf_wgs):
    point     = gpd.GeoSeries([Point(lon_h, lat_h)],
                              crs="EPSG:4326").to_crs("EPSG:2154")[0]
    distances = gdf_proj.geometry.distance(point)
    idx       = distances.idxmin()
    dist_km   = distances[idx] / 1000
    code_sta  = gdf_proj.loc[idx, 'code_sta']
    geom_wgs  = gdf_wgs.loc[idx, 'geometry']
    return code_sta, dist_km, geom_wgs.x, geom_wgs.y


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


def align_series(dates_ref, df_other, col='wl', window_days=14):
    """Aligne df_other sur dates_ref par snap temporel."""
    vals = np.full(len(dates_ref), np.nan)
    for i, d in enumerate(pd.to_datetime(dates_ref)):
        diff = (df_other['date'] - d).abs()
        idx  = diff.idxmin()
        if diff[idx] <= pd.Timedelta(days=window_days):
            vals[i] = df_other.loc[idx, col]
    return vals


def find_sigma0_file(sta):
    """Cherche le fichier sigma0 pour la station (format court ou long)."""
    for candidate in [
        sta,
        sta.lstrip('0') or '0',
        str(int(sta.lstrip('0') or 0)),
    ]:
        p = SIGMA0_DIR / f"{candidate}.csv"
        if p.exists():
            return p
    return None


# ═══════════════════════════════════════════════════════════════
# CHARGEMENT
# ═══════════════════════════════════════════════════════════════
print("Chargement du fichier de résultats modèle...")
with open(RESULTS_P, 'rb') as f:
    raw = pickle.load(f)

print("Chargement stations insitu...")
gdf_insitu      = gpd.read_file(INSITU_SHP)
gdf_insitu_proj = gdf_insitu.to_crs("EPSG:2154")
gdf_insitu_wgs  = gdf_insitu.to_crs("EPSG:4326")

stations = sorted(raw.keys())
print(f"{len(stations)} stations\n")

# ═══════════════════════════════════════════════════════════════
# GÉNÉRATION DES FIGURES
# ═══════════════════════════════════════════════════════════════
n_plots = 0

for sta in stations:
    sta = str(sta)

    # ── Données modèle ───────────────────────────────────────────────────
    try:
        sub      = raw[sta]
        freq     = list(sub.keys())[0]
        ds       = sub[freq]['xr']
        dates    = pd.to_datetime(ds.date.values)
        obs_raw  = ds[f"{TARGET_VAR}_obs"].values.flatten()
        pred_raw = ds[f"{TARGET_VAR}_sim"].values.flatten()
    except Exception as e:
        print(f"  ⚠  {sta} : erreur modèle — {e}")
        continue

    # Masque : uniquement là où obs est disponible
    valid_mask = ~np.isnan(obs_raw)
    if valid_mask.sum() < 5:
        continue

    dates_valid = dates[valid_mask]
    obs_z       = obs_raw[valid_mask]
    pred_z      = pred_raw[valid_mask]

    df_model = pd.DataFrame({'date': dates_valid, 'obs_z': obs_z, 'pred_z': pred_z})

    # ── WSH CLS ────────────────────────────────────────────────────────
    sigma0_path = find_sigma0_file(sta)
    has_wsh = False
    if sigma0_path is not None:
        df_wsh = pd.read_csv(sigma0_path, parse_dates=['date'])
        df_wsh = df_wsh.dropna(subset=['WSH']).sort_values('date')
        if len(df_wsh) >= 5:
            has_wsh = True
            wsh_aligned = align_series(dates_valid, df_wsh, col='WSH', window_days=14)
            wsh_z = zscore(wsh_aligned)

    # ── Insitu ──────────────────────────────────────────────────────────
    has_insitu = False
    lon_h, lat_h = get_coords_hydro(sta)
    if lon_h is not None:
        code_ins, dist_km, _, _ = get_insitu_proche(
            lon_h, lat_h, gdf_insitu_proj, gdf_insitu_wgs
        )
        if dist_km <= DIST_MAX_KM:
            df_ins = get_insitu_series(code_ins)
            if df_ins is not None:
                has_insitu = True
                ins_aligned = align_series(dates_valid, df_ins, col='wl', window_days=14)
                ins_z = zscore(ins_aligned)

    # ── Métriques ───────────────────────────────────────────────────────
    nse_hw, r_hw, rmse_hw = calc_metrics(obs_z, pred_z)
    nse_wsh = r_wsh = rmse_wsh = np.nan
    nse_ins = r_ins = rmse_ins = np.nan
    if has_wsh:
        nse_wsh, r_wsh, rmse_wsh = calc_metrics(wsh_z, pred_z)
    if has_insitu:
        nse_ins, r_ins, rmse_ins = calc_metrics(ins_z, pred_z)

    # ── Figure ──────────────────────────────────────────────────────────
    n_rows = 1 + int(has_wsh) + int(has_insitu)
    fig, axes = plt.subplots(n_rows, 1,
                             figsize=(14, 3.5 * n_rows),
                             sharex=True,
                             constrained_layout=True)
    if n_rows == 1:
        axes = [axes]

    ax_idx = 0

    # Panel 1 : Modèle vs Alti HW
    ax = axes[ax_idx]; ax_idx += 1
    ax.plot(dates_valid, obs_z,  '-o', color='#5B9BD5', markersize=4,
            lw=1, label='Alti HydroWeb', zorder=3)
    ax.plot(dates_valid, pred_z, '-o', color='#E88B8B', markersize=4,
            lw=1, label='Modèle', zorder=2)
    nse_str = f"NSE={nse_hw:.3f}" if not np.isnan(nse_hw) else "NSE=N/A"
    r_str   = f"r={r_hw:.3f}"     if not np.isnan(r_hw)   else "r=N/A"
    ax.set_title(f"Station {sta}  —  Modèle vs Alti HydroWeb  |  {nse_str}  {r_str}",
                 fontsize=10, fontweight='bold')
    ax.set_ylabel('WL (z-score)')
    ax.legend(loc='upper right', fontsize=8)
    ax.grid(True, alpha=0.3, ls='--')
    ax.axhline(0, color='grey', lw=0.5, ls='--')

    # Panel 2 : Modèle vs WSH CLS
    if has_wsh:
        ax = axes[ax_idx]; ax_idx += 1
        ax.plot(dates_valid, wsh_z,  '-o', color='#70AD47', markersize=4,
                lw=1, label='WSH CLS', zorder=3)
        ax.plot(dates_valid, pred_z, '-o', color='#E88B8B', markersize=4,
                lw=1, label='Modèle', zorder=2)
        nse_str = f"NSE={nse_wsh:.3f}" if not np.isnan(nse_wsh) else "NSE=N/A"
        r_str   = f"r={r_wsh:.3f}"     if not np.isnan(r_wsh)   else "r=N/A"
        ax.set_title(f"Modèle vs WSH CLS  |  {nse_str}  {r_str}",
                     fontsize=10, fontweight='bold')
        ax.set_ylabel('WL (z-score)')
        ax.legend(loc='upper right', fontsize=8)
        ax.grid(True, alpha=0.3, ls='--')
        ax.axhline(0, color='grey', lw=0.5, ls='--')

    # Panel 3 : Modèle vs Insitu
    if has_insitu:
        ax = axes[ax_idx]; ax_idx += 1
        ax.plot(dates_valid, ins_z,  '-^', color='darkorange', markersize=4,
                lw=1, label=f'Insitu {code_ins} ({dist_km:.1f} km)', zorder=3)
        ax.plot(dates_valid, pred_z, '-o', color='#E88B8B', markersize=4,
                lw=1, label='Modèle', zorder=2)
        nse_str = f"NSE={nse_ins:.3f}" if not np.isnan(nse_ins) else "NSE=N/A"
        r_str   = f"r={r_ins:.3f}"     if not np.isnan(r_ins)   else "r=N/A"
        ax.set_title(f"Modèle vs Insitu  |  {nse_str}  {r_str}",
                     fontsize=10, fontweight='bold')
        ax.set_ylabel('WL (z-score)')
        ax.legend(loc='upper right', fontsize=8)
        ax.grid(True, alpha=0.3, ls='--')
        ax.axhline(0, color='grey', lw=0.5, ls='--')

    axes[-1].set_xlabel('Date')
    axes[-1].xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    axes[-1].xaxis.set_major_locator(mdates.YearLocator())

    out_path = OUT_DIR / f"{sta}.png"
    fig.savefig(out_path, dpi=150, bbox_inches='tight')
    plt.close(fig)
    n_plots += 1

    wsh_str = f"WSH={'oui' if has_wsh else 'non'}"
    ins_str = f"insitu={'oui' if has_insitu else 'non'}"
    print(f"  {sta:>15s} | NSE_HW={nse_hw:.3f} | {wsh_str} | {ins_str}")

print(f"\n✅ {n_plots} figures dans {OUT_DIR}")