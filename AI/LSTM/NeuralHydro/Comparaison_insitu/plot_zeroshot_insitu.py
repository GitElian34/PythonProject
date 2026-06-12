"""
plot_zeroshot_predictions_with_insitu.py
═══════════════════════════════════════════════════════════════════════════
Identique à plot_zeroshot_predictions.py mais ajoute un panneau insitu :
  [1] Précipitations (barres inversées)
  [2] Obs satellite vs Prédit modèle (NSE/KGE modèle)
  [3] Obs satellite vs Station insitu la plus proche (NSE/KGE insitu)

Le matching insitu = station la plus proche géographiquement (pas de
vérification SWORD). Les deux séries sont normalisées en z-score pour
être comparables. Si aucune donnée insitu n'est disponible, le panneau
[3] est masqué.
═══════════════════════════════════════════════════════════════════════════
"""

import pickle
import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import xarray as xr
import geopandas as gpd
from pathlib import Path
from shapely.geometry import Point

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES — À MODIFIER
# ═══════════════════════════════════════════════════════════════
MODEL       = "/arlstm_feat10jLow_modele2_0605_140952"
RUN_DIR     = Path(f"./runs{MODEL}")
EPOCH       = 18
PERIOD      = "test"
NC_DIR      = Path("./data/IA/NeuralHydrology_satellite_10D/time_series")

RESULTS_P   = RUN_DIR / PERIOD / f"model_epoch{EPOCH:03d}" / f"{PERIOD}_results.p"
METRICS_CSV = RUN_DIR / PERIOD / f"model_epoch{EPOCH:03d}" / f"{PERIOD}_metrics.csv"

OUT_DIR     = Path(f"./figures_zeroshot_insitu{MODEL}")
OUT_DIR.mkdir(parents=True, exist_ok=True)

TARGET_VAR  = "water_level"

# Bases de données
HYDRO_DB_PATH  = "./data/hydro_data.db"
INSITU_DB_PATH = "./data/insitu_data.db"

# Fichier shapefile des stations insitu (pour le matching géographique)
INSITU_SHP = "./data/insitu/shp/station_schapi_alti_ref_2025_river.gpkg"

# Distance max pour le matching insitu (km) — si > seuil, panneau masqué
DIST_MAX_KM = 50.0


# ═══════════════════════════════════════════════════════════════
# CHARGEMENT DES STATIONS INSITU (géométries pour matching)
# ═══════════════════════════════════════════════════════════════
print("Chargement des stations insitu...")
gdf_insitu = gpd.read_file(INSITU_SHP)
gdf_insitu_proj = gdf_insitu.to_crs("EPSG:2154")


def get_station_insitu_proche(lon_hydro, lat_hydro):
    """Retourne (code_sta, dist_km) de la station insitu la plus proche."""
    point = gpd.GeoSeries([Point(lon_hydro, lat_hydro)],
                          crs="EPSG:4326").to_crs("EPSG:2154")[0]
    distances = gdf_insitu_proj.geometry.distance(point)
    idx = distances.idxmin()
    dist_km = distances[idx] / 1000
    code_sta = gdf_insitu_proj.loc[idx, 'code_sta']
    return code_sta, dist_km


def get_coords_hydro(station_code):
    """Récupère lon/lat d'une station satellite depuis hydro_data.db."""
    conn = sqlite3.connect(HYDRO_DB_PATH)
    df = pd.read_sql_query(
        "SELECT reference_longitude, reference_latitude FROM stations WHERE station_code = ?",
        conn, params=(station_code,)
    )
    conn.close()
    if df.empty:
        return None, None
    return float(df.iloc[0]['reference_longitude']), float(df.iloc[0]['reference_latitude'])


def get_insitu_series(code_sta, date_min, date_max):
    """
    Charge la série insitu quotidienne entre date_min et date_max.
    Retourne un DataFrame avec colonnes [date, wl] ou None.
    """
    conn = sqlite3.connect(INSITU_DB_PATH)
    df = pd.read_sql_query("""
        SELECT date, h_med_wsh
        FROM mesures_insitu
        WHERE code_sta = ?
          AND date >= ?
          AND date <= ?
        ORDER BY date
    """, conn, params=(code_sta,
                       str(date_min)[:10],
                       str(date_max)[:10]))
    conn.close()
    if df.empty:
        return None
    df['date'] = pd.to_datetime(df['date'])
    df = df.dropna(subset=['h_med_wsh'])
    if len(df) < 5:
        return None
    return df.rename(columns={'h_med_wsh': 'wl'})


def zscore(arr):
    """Normalise en z-score, ignore les NaN."""
    mu  = np.nanmean(arr)
    sig = np.nanstd(arr)
    if sig == 0:
        return arr - mu
    return (arr - mu) / sig


def calc_nse_kge(obs, sim):
    """NSE et KGE sur valeurs alignées (sans NaN)."""
    mask = ~(np.isnan(obs) | np.isnan(sim))
    if mask.sum() < 5:
        return np.nan, np.nan
    o, s = obs[mask], sim[mask]
    denom = np.sum((o - o.mean()) ** 2)
    nse   = 1 - np.sum((o - s) ** 2) / denom if denom > 0 else np.nan
    r     = np.corrcoef(o, s)[0, 1]
    alpha = s.std() / o.std() if o.std() > 0 else 0
    beta  = s.mean() / o.mean() if o.mean() != 0 else 0
    kge   = 1 - np.sqrt((r - 1)**2 + (alpha - 1)**2 + (beta - 1)**2)
    return nse, kge


def align_insitu_to_satellite(dates_sat, df_insitu, window_days=5):
    """
    Pour chaque date satellite, cherche la mesure insitu la plus proche
    dans une fenêtre de ±window_days jours.
    Retourne un array de même longueur que dates_sat (NaN si pas de match).
    """
    dates_sat_pd = pd.to_datetime(dates_sat)
    insitu_wl    = np.full(len(dates_sat_pd), np.nan)

    for i, d in enumerate(dates_sat_pd):
        diff = (df_insitu['date'] - d).abs()
        idx  = diff.idxmin()
        if diff[idx] <= pd.Timedelta(days=window_days):
            insitu_wl[i] = df_insitu.loc[idx, 'wl']

    return insitu_wl


# ═══════════════════════════════════════════════════════════════
# CHARGEMENT DES RÉSULTATS MODÈLE
# ═══════════════════════════════════════════════════════════════
print("=" * 60)
print("PLOT ZERO-SHOT + INSITU — STATIONS SATELLITE 10J")
print("=" * 60)

if not RESULTS_P.exists():
    print(f"❌ Pas de résultats : {RESULTS_P}")
    exit(1)

print(f"\n📂 Chargement de {RESULTS_P}...")
with open(RESULTS_P, 'rb') as f:
    results = pickle.load(f)

df_metrics = pd.read_csv(METRICS_CSV, header=None, names=["station", "NSE", "KGE"])
df_metrics["NSE"] = pd.to_numeric(df_metrics["NSE"], errors="coerce")
df_metrics["KGE"] = pd.to_numeric(df_metrics["KGE"], errors="coerce")
df_metrics = df_metrics.set_index("station")

print(f"Médiane NSE modèle : {df_metrics['NSE'].median():.3f}")
print(f"Génération des figures...\n")

stations = sorted(results.keys())

for sid in stations:
    try:
        sub   = results[sid]
        freqs = list(sub.keys())
        if not freqs:
            continue
        ds = sub[freqs[0]]['xr']

        obs_var = f"{TARGET_VAR}_obs"
        sim_var = f"{TARGET_VAR}_sim"
        if obs_var not in ds or sim_var not in ds:
            continue

        dates = ds.date.values
        obs   = ds[obs_var].values.flatten()
        sim   = ds[sim_var].values.flatten()

        nse_model = df_metrics.loc[sid, 'NSE'] if sid in df_metrics.index else np.nan
        kge_model = df_metrics.loc[sid, 'KGE'] if sid in df_metrics.index else np.nan

        # ── Précipitations ───────────────────────────────────────────────
        precip       = None
        precip_dates = None
        for nc_name in [f"{sid}.nc", f"{str(sid).zfill(13)}.nc"]:
            nc_path = NC_DIR / nc_name
            if nc_path.exists():
                ds_nc = xr.open_dataset(nc_path)
                if 'precipitation_J0' in ds_nc:
                    precip       = ds_nc['precipitation_J0'].values
                    precip_dates = pd.to_datetime(ds_nc.date.values)
                ds_nc.close()
                break

        # ── Matching insitu ──────────────────────────────────────────────
        insitu_wl_aligned = None
        insitu_wl_norm    = None
        nse_insitu        = np.nan
        kge_insitu        = np.nan
        code_insitu       = None
        dist_insitu_km    = None

        lon_h, lat_h = get_coords_hydro(str(sid))
        if lon_h is not None:
            code_insitu, dist_insitu_km = get_station_insitu_proche(lon_h, lat_h)

            if dist_insitu_km <= DIST_MAX_KM:
                date_min = pd.to_datetime(dates).min()
                date_max = pd.to_datetime(dates).max()
                df_ins   = get_insitu_series(code_insitu, date_min, date_max)

                if df_ins is not None:
                    insitu_wl_aligned = align_insitu_to_satellite(dates, df_ins)
                    # Normalisation z-score des deux séries
                    obs_norm          = zscore(obs)
                    insitu_wl_norm    = zscore(insitu_wl_aligned)
                    nse_insitu, kge_insitu = calc_nse_kge(obs_norm, insitu_wl_norm)

        # ── Figure ───────────────────────────────────────────────────────
        has_insitu = insitu_wl_norm is not None

        if has_insitu:
            height_ratios = [1, 3, 3]
            n_rows = 3
        else:
            height_ratios = [1, 3]
            n_rows = 2

        fig, axes = plt.subplots(
            n_rows, 1, figsize=(14, 4 + 3 * n_rows),
            gridspec_kw={'height_ratios': height_ratios, 'hspace': 0.08},
            sharex=True
        )
        ax_p  = axes[0]
        ax_wl = axes[1]
        ax_in = axes[2] if has_insitu else None

        # Panel précipitations
        if precip is not None:
            ax_p.bar(precip_dates, precip, width=8, color='#4A90D9',
                     alpha=0.7, edgecolor='none')
            ax_p.invert_yaxis()
            ax_p.set_ylabel('Précip\n(mm/j)', fontsize=8)
            ax_p.grid(True, alpha=0.2)
            ax_p.spines['bottom'].set_visible(False)
            ax_p.tick_params(axis='x', labelbottom=False)
            p95 = np.nanpercentile(precip, 95)
            if p95 > 0:
                ax_p.set_ylim(min(p95 * 1.5, np.nanmax(precip)), 0)
        else:
            ax_p.set_visible(False)

        # Panel modèle
        ax_wl.plot(dates, obs, 'o-', color="steelblue", lw=1, ms=3,
                   label=f"Obs satellite (n={(~np.isnan(obs)).sum()})")
        ax_wl.plot(dates, sim, 's-', color="crimson", lw=1, ms=3, alpha=0.7,
                   label=f"Prédit modèle  NSE={nse_model:.3f} | KGE={kge_model:.3f}")
        ax_wl.axhline(0, color="gray", lw=0.5, ls="--")
        ax_wl.set_ylabel("WL (z-score)", fontsize=9)
        ax_wl.legend(fontsize=8, loc='upper right')
        ax_wl.grid(True, alpha=0.3)
        ax_wl.spines['bottom'].set_visible(False)
        ax_wl.tick_params(axis='x', labelbottom=False)

        # Panel insitu
        if has_insitu:
            obs_norm_plot = zscore(obs)
            ax_in.plot(dates, obs_norm_plot, 'o-', color="steelblue",
                       lw=1, ms=3, label="Obs satellite (z-score)")
            ax_in.plot(dates, insitu_wl_norm, '^-', color="darkorange",
                       lw=1, ms=3, alpha=0.8,
                       label=f"Insitu {code_insitu} ({dist_insitu_km:.1f} km)  "
                             f"NSE={nse_insitu:.3f} | KGE={kge_insitu:.3f}")
            ax_in.axhline(0, color="gray", lw=0.5, ls="--")
            ax_in.set_ylabel("WL (z-score)", fontsize=9)
            ax_in.set_xlabel("Date", fontsize=9)
            ax_in.legend(fontsize=8, loc='upper right')
            ax_in.grid(True, alpha=0.3)
            ax_in.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
            ax_in.xaxis.set_major_locator(mdates.YearLocator())
        else:
            ax_wl.set_xlabel("Date", fontsize=9)
            ax_wl.tick_params(axis='x', labelbottom=True)
            ax_wl.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
            ax_wl.xaxis.set_major_locator(mdates.YearLocator())
            reason = f"(pas de station insitu < {DIST_MAX_KM} km)" \
                     if dist_insitu_km and dist_insitu_km > DIST_MAX_KM \
                     else "(coords manquantes)"
            fig.text(0.5, 0.02, f"Pas de données insitu {reason}",
                     ha='center', fontsize=8, color='gray')

        fig.suptitle(f"{sid}  —  NSE modèle = {nse_model:.3f}  |  KGE = {kge_model:.3f}",
                     fontsize=11, fontweight='bold', y=0.99)

        plt.savefig(OUT_DIR / f"{sid}.png", dpi=120, bbox_inches='tight')
        plt.close()

        insitu_info = f"insitu={code_insitu} ({dist_insitu_km:.1f}km) NSE={nse_insitu:.2f}" \
                      if has_insitu else "pas d'insitu"
        print(f"  ✅ {sid} | modèle NSE={nse_model:.2f} | {insitu_info}")

    except Exception as e:
        print(f"  ❌ {sid} : {e}")
        continue

print(f"\n✅ Figures dans : {OUT_DIR}")