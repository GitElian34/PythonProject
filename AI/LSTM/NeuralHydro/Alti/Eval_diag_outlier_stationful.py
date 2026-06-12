"""
zeroshot_eval_outliers_27j.py
═══════════════════════════════════════════════════════════════════════════
Script tout-en-un :
  1. Évalue le modèle sur les stations satellite 27j (zero-shot)
  2. Affiche NSE/KGE médians + distribution
  3. Extrait les résidus et détecte les outliers
  4. Génère les plots 3 panneaux pour TOUTES les années

Panels :
  [1] Obs satellite vs Prédit modèle — outliers annotés (rouge)
  [2] Obs satellite vs Station insitu la plus proche
  [3] WSH brut vs date coloré par sigma0
═══════════════════════════════════════════════════════════════════════════
"""

import pickle
import sqlite3
import numpy as np
import pandas as pd
import xarray as xr
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.colors as mcolors
import matplotlib.cm as cm
import geopandas as gpd
import folium
import torch
from pathlib import Path
from shapely.geometry import Point
from ruamel.yaml import YAML
from neuralhydrology.utils.config import Config
from neuralhydrology.evaluation.evaluate import start_evaluation

torch.set_num_threads(8)

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES — seules ces lignes changent d'un run à l'autre
# ═══════════════════════════════════════════════════════════════
MODEL  = "arlstm_feat10jLow_modele2_0605_140952"
EPOCH  = 7
PERIOD = "validation"

RUN_DIR            = Path(f"./runs/{MODEL}")
STATIONS_FILE      = Path("./data/IA/NeuralHydrology_hydroweb_next/stations_dahiti_10j.txt")
DATA_DIR_SATELLITE = Path("./data/IA/NeuralHydrology_hydroweb_next/10j")

HYDRO_DB_PATH  = "./data/hydroweb_next.db"
INSITU_DB_PATH = "./data/insitu_data.db"
INSITU_SHP     = "./data/insitu/shp/station_schapi_alti_ref_2025_river.gpkg"
CSV_SIGMA0     = Path("./data/sigma0/sigma0_all_stations.csv")

OUT_CSV   = Path("./data/outlier_detection/residuals_10j_hydroweb_next.csv")
OUT_PLOTS = Path(f"./figures_zeroshot_satellite/{MODEL}/HydroWeb_Next_10j")

OUTLIER_THRESHOLD = 2.5
TARGET_VAR        = "water_level"
DIST_MAX_KM       = 50.0
SIGMA0_SEUIL      = 30.0

# Pour tester : limiter à une seule station (None = toutes)
ONLY_STATION      = None

OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
OUT_PLOTS.mkdir(parents=True, exist_ok=True)


# ═══════════════════════════════════════════════════════════════
# ÉTAPE 1 — ÉVALUATION DU MODÈLE
# ═══════════════════════════════════════════════════════════════
print("=" * 60)
print("ÉTAPE 1 — ÉVALUATION ZERO-SHOT")
print("=" * 60)

ryaml = YAML()
ryaml.preserve_quotes = True
config_path = RUN_DIR / "config.yml"
config_eval = RUN_DIR / "config_eval_satellite.yml"

with open(config_path) as f:
    cfg_dict = ryaml.load(f)

cfg_dict["validation_basin_file"] = str(STATIONS_FILE.resolve())
cfg_dict["data_dir"]              = str(DATA_DIR_SATELLITE.resolve())

with open(config_eval, "w") as f:
    ryaml.dump(cfg_dict, f)

print(f"Run      : {MODEL}")
print(f"Epoch    : {EPOCH}")
print(f"Data     : {DATA_DIR_SATELLITE}")
print(f"Stations : {STATIONS_FILE}\n")

cfg = Config(config_eval)
start_evaluation(cfg=cfg, run_dir=RUN_DIR, epoch=EPOCH, period=PERIOD)

# ─── Résumé NSE/KGE ─────────────────────────────────────────────────────────
results_p = RUN_DIR / PERIOD / f"model_epoch{EPOCH:03d}" / f"{PERIOD}_results.p"
with open(results_p, "rb") as f:
    raw = pickle.load(f)

records_metrics = []
for station, data in raw.items():
    try:
        freq_key = list(data.keys())[0]
        nse = float(np.squeeze(data[freq_key]["NSE"]))
        kge = float(np.squeeze(data[freq_key]["KGE"]))
        if not np.isnan(nse):
            records_metrics.append({"station": station, "NSE": nse, "KGE": kge})
    except Exception:
        continue

df_metrics = pd.DataFrame(records_metrics).sort_values("NSE", ascending=False)

print(f"\n{'='*50}")
print(f"RÉSULTATS ZERO-SHOT — {MODEL} epoch {EPOCH}")
print(f"{'='*50}")
print(f"  N stations  : {len(df_metrics)}")
print(f"  NSE médian  : {df_metrics['NSE'].median():.3f}")
print(f"  NSE moyen   : {df_metrics['NSE'].mean():.3f}")
print(f"  KGE médian  : {df_metrics['KGE'].median():.3f}")
print(f"\n  Distribution NSE :")
bins_display = [(-np.inf,0,"< 0"), (0,0.3,"0–0.3"), (0.3,0.5,"0.3–0.5"),
                (0.5,0.7,"0.5–0.7"), (0.7,np.inf,"> 0.7")]
for lo, hi, label in bins_display:
    n = int(((df_metrics["NSE"] > lo) & (df_metrics["NSE"] <= hi)).sum())
    print(f"    {label:<10} : {n:>4}  ({n/len(df_metrics)*100:.1f}%)")
print(f"\n  {'Station':<20} {'NSE':>8} {'KGE':>8}")
print(f"  {'-'*40}")
for _, row in df_metrics.iterrows():
    print(f"  {row['station']:<20} {row['NSE']:>8.3f} {row['KGE']:>8.3f}")


# ═══════════════════════════════════════════════════════════════
# ÉTAPE 2 — EXTRACTION DES RÉSIDUS + NOUVELLE FORMULE
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("ÉTAPE 2 — EXTRACTION RÉSIDUS & DÉTECTION OUTLIERS")
print("=" * 60)

# Variables dynamiques à diagnostiquer (celles présentes dans les .nc)
DIAG_VARS = [
    'precipitation_J0', 'temperature_J0', 'pet_J0',
    'precip_mean_J3', 'pet_mean_J3', 'temp_mean_J3',
    'precip_mean_J27', 'precip_mean_J10', 'temp_mean_J10',
    'clim_mean_20j', 'clim_std_20j',
    'precip_max_J27', 'precip_last7', 'nb_jours_pluie_J27', 'precip_mean_J14',
]

def compute_score(residual_norm, obs, pred, obs_arr, pred_arr, i):
    """
    Score outlier avec pénalité de direction.
    Référentiel commun : J0_ref = obs[i-1] si dispo, sinon pred[i-1]
    """
    if np.isnan(residual_norm):
        return np.nan

    if i == 0:
        return abs(residual_norm)

    # Référentiel commun J0
    j0_obs  = obs_arr[i-1]
    j0_pred = pred_arr[i-1]
    j0_ref  = j0_obs if not np.isnan(j0_obs) else j0_pred
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
    cos_pen  = (1 - cos) / 2
    amp      = (abs(delta_alti) + abs(delta_model)) / 2
    penalite = cos_pen * np.tanh(amp)

    return abs(residual_norm) * (1 + penalite)


rows = []
# Stocker aussi les tableaux obs/pred par station pour le calcul des deltas
raw_by_station = {}

for sid, sub in raw.items():
    try:
        freq = list(sub.keys())[0]
        ds   = sub[freq]['xr']
        obs_var = f"{TARGET_VAR}_obs"
        sim_var = f"{TARGET_VAR}_sim"
        if obs_var not in ds or sim_var not in ds:
            continue

        dates    = pd.to_datetime(ds.date.values)
        obs_arr  = ds[obs_var].values.flatten()
        pred_arr = ds[sim_var].values.flatten()

        raw_by_station[str(sid)] = {
            'dates': dates, 'obs': obs_arr, 'pred': pred_arr
        }

        for i, (d, o, p) in enumerate(zip(dates, obs_arr, pred_arr)):
            rows.append({
                'station':  str(sid),
                'date':     d,
                'obs':      o,
                'pred':     p,
                'residual': o - p if not (np.isnan(o) or np.isnan(p)) else np.nan,
                '_i':       i,
            })
    except Exception as e:
        print(f"  ⚠  {sid} : {e}")

df = pd.DataFrame(rows)
df['date'] = pd.to_datetime(df['date'])

# Normalisation résidu par station
def norm_residuals(grp):
    std = np.nanstd(grp['residual'])
    grp['residual_norm'] = grp['residual'] / std if std > 0 else np.nan
    return grp

df = df.groupby('station', group_keys=False).apply(norm_residuals)

# Calcul du score avec pénalité de direction
scores = []
for _, row in df.iterrows():
    sid = row['station']
    i   = int(row['_i'])
    obs_arr  = raw_by_station[sid]['obs']
    pred_arr = raw_by_station[sid]['pred']
    scores.append(compute_score(row['residual_norm'], row['obs'], row['pred'],
                                obs_arr, pred_arr, i))

df['score']      = scores
df['is_outlier'] = df['score'] > OUTLIER_THRESHOLD
df['year']       = df['date'].dt.year
df = df.drop(columns=['_i'])

df.to_csv(OUT_CSV, index=False)
print(f"✅ {len(df)} lignes → {OUT_CSV}")
print(f"   {df['station'].nunique()} stations")
print(f"   {df['is_outlier'].sum()} outliers détectés ({df['is_outlier'].mean()*100:.1f}%)")

# ─── Filtre station unique pour les tests ───────────────────────────────────
if ONLY_STATION is not None:
    df = df[df['station'] == ONLY_STATION].copy()
    print(f"\n⚠  MODE TEST : limité à la station {ONLY_STATION}")
    print(f"   {df['is_outlier'].sum()} outliers sur cette station")


# ═══════════════════════════════════════════════════════════════
# ÉTAPE 2b — FONCTION DE DIAGNOSTIC VARIABLES DYNAMIQUES
# ═══════════════════════════════════════════════════════════════
def diagnostic_station(sta, df_sta, nc_dir, diag_vars):
    """
    Pour chaque outlier de la station, calcule le z-score de chaque
    variable dynamique à la date outlier par rapport à toute la série.
    Retourne (lignes texte, dataframe structuré).
    """
    nc_path = None
    for candidate in [sta, sta.lstrip('0'), sta.zfill(13)]:
        p = Path(nc_dir) / 'time_series' / f"{candidate}.nc"
        if p.exists():
            nc_path = p
            break
    if nc_path is None:
        return [f"⚠  .nc introuvable pour {sta}\n"], pd.DataFrame()

    ds = xr.open_dataset(nc_path)
    dates_nc = pd.to_datetime(ds.date.values)
    vars_dispo = [v for v in diag_vars if v in ds]

    # Stats globales par variable
    stats = {}
    for var in vars_dispo:
        vals = ds[var].values.flatten()
        vals_clean = vals[~np.isnan(vals)]
        if len(vals_clean) > 0:
            stats[var] = {
                'mean': np.mean(vals_clean),
                'std':  np.std(vals_clean),
                'p10':  np.percentile(vals_clean, 10),
                'p25':  np.percentile(vals_clean, 25),
                'p75':  np.percentile(vals_clean, 75),
                'p90':  np.percentile(vals_clean, 90),
            }

    outliers_sta = df_sta[df_sta['is_outlier']].sort_values('date')
    lines = []
    lines.append(f"{'='*70}\n")
    lines.append(f"DIAGNOSTIC STATION {sta}\n")
    lines.append(f"  {len(outliers_sta)} outlier(s) détecté(s)\n")
    lines.append(f"{'='*70}\n\n")

    csv_rows = []

    for _, row in outliers_sta.iterrows():
        date_out = row['date']
        lines.append(f"{'─'*70}\n")
        lines.append(f"  OUTLIER — {date_out.strftime('%Y-%m-%d')}\n")
        lines.append(f"  Obs      : {row['obs']:+.4f}\n")
        lines.append(f"  Prédit   : {row['pred']:+.4f}\n")
        lines.append(f"  Résidu σ : {row['residual_norm']:+.2f}σ\n")
        lines.append(f"  Score    : {row['score']:+.2f}\n")
        lines.append(f"{'─'*70}\n")

        idx_nc = np.argmin(np.abs(dates_nc - date_out))
        date_nc = dates_nc[idx_nc]
        if abs((date_nc - date_out).days) > 14:
            lines.append(f"  ⚠  Date .nc la plus proche : {date_nc} "
                         f"(écart {abs((date_nc - date_out).days)}j)\n\n")
            continue

        lines.append(f"\n  {'Variable':<25} {'Valeur':>10} {'Moy':>10} "
                     f"{'Std':>10} {'Zscore':>8} {'Pct':>6} {'Flag'}\n")
        lines.append(f"  {'-'*80}\n")

        # Ligne CSV pour cet outlier
        csv_row = {
            'station':       sta,
            'date':          date_out.strftime('%Y-%m-%d'),
            'obs':           row['obs'],
            'pred':          row['pred'],
            'residual_norm': row['residual_norm'],
            'score':         row['score'],
        }

        for var in vars_dispo:
            if var not in stats:
                continue
            val = float(ds[var].values.flatten()[idx_nc])
            s   = stats[var]
            if np.isnan(val):
                lines.append(f"  {var:<25} {'NaN':>10}\n")
                csv_row[f'{var}_val']    = np.nan
                csv_row[f'{var}_zscore'] = np.nan
                csv_row[f'{var}_pct']    = np.nan
                continue

            zscore_val = (val - s['mean']) / s['std'] if s['std'] > 0 else 0.0
            vals_all   = ds[var].values.flatten()
            vals_clean = vals_all[~np.isnan(vals_all)]
            pct        = int(np.mean(vals_clean <= val) * 100)

            flag = ""
            if abs(zscore_val) > 2:   flag = "⚠ EXTREME"
            elif abs(zscore_val) > 1.5: flag = "~ élevé"

            lines.append(f"  {var:<25} {val:>10.3f} {s['mean']:>10.3f} "
                         f"{s['std']:>10.3f} {zscore_val:>+8.2f} {pct:>5}%  {flag}\n")

            csv_row[f'{var}_val']    = round(val, 4)
            csv_row[f'{var}_zscore'] = round(zscore_val, 4)
            csv_row[f'{var}_pct']    = pct

        lines.append("\n")
        csv_rows.append(csv_row)

    ds.close()
    df_csv = pd.DataFrame(csv_rows)
    return lines, df_csv


# ═══════════════════════════════════════════════════════════════
# ÉTAPE 3 — CHARGEMENT DONNÉES AUXILIAIRES
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("ÉTAPE 3 — GÉNÉRATION DES PLOTS")
print("=" * 60)

print("Chargement stations insitu...")
gdf_insitu      = gpd.read_file(INSITU_SHP)
gdf_insitu_proj = gdf_insitu.to_crs("EPSG:2154")

print("Chargement sigma0...")
df_sigma = pd.read_csv(CSV_SIGMA0, parse_dates=['date'])
df_sigma['station_code'] = df_sigma['station_code'].astype(str)
sigma0_min = df_sigma['sigma0'].quantile(0.02)
sigma0_max = df_sigma['sigma0'].quantile(0.98)
norm_s0    = mcolors.Normalize(vmin=sigma0_min, vmax=sigma0_max)
cmap_s0    = cm.RdYlGn


# ═══════════════════════════════════════════════════════════════
# FONCTIONS AUXILIAIRES
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


# ─── Cache insitu ────────────────────────────────────────────────────────────
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
print(f"📊 {len(stations)} stations\n")
n_plots = 0
all_diag_rows = []  # accumule tous les diagnostics pour le global

for sta in stations:
    grp      = df[df['station'] == sta].sort_values('date')
    outliers = grp[grp['is_outlier']]

    all_years           = sorted(grp['year'].unique())
    years_with_outliers = set(outliers['year'].unique())

    if len(all_years) == 0:
        continue

    sta_dir = OUT_PLOTS / sta
    sta_dir.mkdir(parents=True, exist_ok=True)

    # ── Diagnostic dynamique des outliers de la station ──────────────────
    if len(outliers) > 0:
        diag_lines, diag_df = diagnostic_station(sta, grp, DATA_DIR_SATELLITE, DIAG_VARS)
        diag_path = sta_dir / f"diagnostic_{sta}.txt"
        with open(diag_path, 'w') as f:
            f.writelines(diag_lines)
        if not diag_df.empty:
            diag_df.to_csv(sta_dir / f"diagnostic_{sta}.csv", index=False)
            all_diag_rows.append(diag_df)

    code_insitu, dist_km, df_ins = get_insitu_info(sta)
    has_insitu = df_ins is not None

    sig_sta   = df_sigma[df_sigma['station_code'] == sta].sort_values('date')
    has_sigma0 = not sig_sta.empty

    n_out_total = len(outliers)
    print(f"  {sta:>15s} | {n_out_total:2d} outliers | {len(all_years)} années | "
          f"insitu={'oui' if has_insitu else 'non'} | "
          f"sigma0={'oui' if has_sigma0 else 'non'}")

    for year in all_years:
        grp_year = grp[grp['year'] == year]
        out_year = outliers[outliers['year'] == year]
        has_outliers_this_year = year in years_with_outliers
        n_out_year = len(out_year)

        # Insitu aligné
        insitu_wl_norm = None
        nse_ins = kge_ins = np.nan
        if has_insitu:
            insitu_wl      = align_insitu(grp_year['date'], df_ins)
            obs_norm_full  = zscore(grp_year['obs'].values)
            insitu_wl_norm = zscore(insitu_wl)
            nse_ins, kge_ins = calc_nse_kge(obs_norm_full, insitu_wl_norm)

        sig_year = sig_sta[sig_sta['date'].dt.year == year] if has_sigma0 else pd.DataFrame()

        # Layout dynamique
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
            gridspec_kw={'height_ratios': height_ratios},
            sharex=True,
            constrained_layout=True
        )
        if n_rows == 1:
            axes_list = [axes_list]

        ax_wl = axes_list[0]
        ax_in = axes_list[1] if has_insitu and n_rows >= 2 else None
        ax_s0 = axes_list[-1] if has_sigma0 and not sig_year.empty and n_rows >= 2 else None
        if ax_in is ax_s0:
            ax_s0 = None

        # Titre
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
                ax_in.axvline(row['date'], color='red', lw=1.2, alpha=0.4, ls='--', zorder=1)

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
                    ax_in.plot([d, d], [ov, iv], color='darkorchid', lw=2, alpha=0.8, zorder=4)
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
                ax_s0.axvline(row['date'], color='red', lw=1.2, alpha=0.4, ls='--', zorder=1)

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

        out_path = sta_dir / f"outlier_{sta}_{year}.png"
        fig.savefig(out_path, dpi=150, bbox_inches='tight')
        plt.close(fig)
        n_plots += 1

    # ── Carte Folium par station ──────────────────────────────────────────
    import folium

    def couleur_nse_carte(nse):
        if np.isnan(nse): return 'gray'
        if nse >= 0.7:    return 'darkgreen'
        if nse >= 0.5:    return 'green'
        if nse >= 0.0:    return 'orange'
        return 'red'

    def couleur_lien_carte(dist):
        if dist <= 5:  return '#2E7D32'
        if dist <= 15: return '#F9A825'
        return '#C62828'

    lon_h, lat_h = get_coords_hydro(sta)
    if lon_h is not None:
        nse_sta = df_metrics[df_metrics['station'] == sta]['NSE'].values
        nse_val = float(nse_sta[0]) if len(nse_sta) > 0 else float('nan')

        # Récupère lon/lat insitu depuis le shapefile si station proche trouvée
        lon_i = lat_i = None
        if has_insitu and code_insitu is not None:
            mask = gdf_insitu_proj['code_sta'] == code_insitu
            if mask.any():
                geom_wgs = gdf_insitu.to_crs("EPSG:4326").loc[mask.idxmax(), 'geometry']
                lon_i, lat_i = geom_wgs.x, geom_wgs.y

        # Centre la carte
        if lon_i is not None:
            lat_c = (lat_h + lat_i) / 2
            lon_c = (lon_h + lon_i) / 2
        else:
            lat_c, lon_c = lat_h, lon_h

        m = folium.Map(location=[lat_c, lon_c], zoom_start=9, tiles='OpenStreetMap')

        # Marqueur station alti
        folium.CircleMarker(
            location=[lat_h, lon_h],
            radius=9,
            color='white', weight=2,
            fill=True, fill_color=couleur_nse_carte(nse_val), fill_opacity=0.9,
            popup=folium.Popup(
                f"<b>Station alti {sta}</b><br>NSE = {nse_val:.3f}<br>"
                f"Outliers : {n_out_total}<br>"
                f"lon/lat : {lon_h:.4f}, {lat_h:.4f}",
                max_width=250),
            tooltip=f"ALTI {sta} | NSE={nse_val:.3f}",
        ).add_to(m)

        # Marqueur + lien station insitu
        if lon_i is not None:
            folium.CircleMarker(
                location=[lat_i, lon_i],
                radius=8,
                color='white', weight=2,
                fill=True, fill_color='#E65100', fill_opacity=0.9,
                popup=folium.Popup(
                    f"<b>Station insitu {code_insitu}</b><br>"
                    f"Distance : {dist_km:.1f} km<br>"
                    f"lon/lat : {lon_i:.4f}, {lat_i:.4f}",
                    max_width=250),
                tooltip=f"INSITU {code_insitu} ({dist_km:.1f} km)",
            ).add_to(m)

            folium.PolyLine(
                locations=[[lat_h, lon_h], [lat_i, lon_i]],
                color=couleur_lien_carte(dist_km), weight=2.5, opacity=0.8,
                tooltip=f"{dist_km:.1f} km",
            ).add_to(m)

        # Légende
        insitu_label = f"{code_insitu} ({dist_km:.1f} km)" if lon_i is not None else "aucune"
        legende = f"""
        <div style="position:fixed;bottom:20px;left:20px;z-index:1000;
                    background:white;padding:10px 14px;border-radius:6px;
                    box-shadow:0 1px 5px rgba(0,0,0,0.4);font-size:12px;">
          <b>{sta}</b><br>
          <span style="color:{couleur_nse_carte(nse_val)};">●</span> Station alti — NSE={nse_val:.3f}<br>
          <span style="color:#E65100;">●</span> Station insitu — {insitu_label}<br>
          Outliers détectés : {n_out_total}
        </div>
        """
        m.get_root().html.add_child(folium.Element(legende))

        carte_path = sta_dir / f"carte_{sta}.html"
        m.save(str(carte_path))

print(f"\n✅ {n_plots} figures dans {OUT_PLOTS}")
print(f"✅ Cartes HTML générées dans les sous-dossiers par station")


# ═══════════════════════════════════════════════════════════════
# ÉTAPE 4 — DIAGNOSTIC GLOBAL
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("ÉTAPE 4 — DIAGNOSTIC GLOBAL")
print("=" * 60)

if not all_diag_rows:
    print("  Aucun outlier détecté — pas de diagnostic global.")
else:
    df_global = pd.concat(all_diag_rows, ignore_index=True)

    # Sauvegarde CSV global
    global_csv = OUT_CSV.parent / "diagnostic_global_27j.csv"
    df_global.to_csv(global_csv, index=False)
    print(f"✅ {len(df_global)} outliers → {global_csv}")

    # Colonnes zscore uniquement
    zscore_cols = [c for c in df_global.columns if c.endswith('_zscore')]

    print(f"\n{'='*70}")
    print(f"ANALYSE DES Z-SCORES AUX DATES OUTLIERS ({len(df_global)} outliers, {df_global['station'].nunique()} stations)")
    print(f"{'='*70}")
    print(f"\n  {'Variable':<30} {'|Z| moyen':>10} {'|Z| médian':>11} {'% |Z|>2':>9} {'% |Z|>1.5':>10}")
    print(f"  {'-'*75}")

    summary_rows = []
    for col in zscore_cols:
        var = col.replace('_zscore', '')
        z = df_global[col].dropna().abs()
        if len(z) == 0:
            continue
        z_mean   = z.mean()
        z_med    = z.median()
        pct_2    = (z > 2).mean() * 100
        pct_15   = (z > 1.5).mean() * 100
        summary_rows.append({'variable': var, 'z_mean': z_mean, 'z_med': z_med,
                              'pct_gt2': pct_2, 'pct_gt15': pct_15, 'n': len(z)})
        flag = " ⚠" if pct_2 > 20 else ""
        print(f"  {var:<30} {z_mean:>10.3f} {z_med:>11.3f} {pct_2:>8.1f}% {pct_15:>9.1f}%{flag}")

    df_summary = pd.DataFrame(summary_rows).sort_values('z_mean', ascending=False)

    print(f"\n{'='*70}")
    print(f"TOP 5 VARIABLES LES PLUS EXTRÊMES AUX DATES OUTLIERS")
    print(f"{'='*70}")
    for _, row in df_summary.head(5).iterrows():
        print(f"  {row['variable']:<30} |Z| moyen={row['z_mean']:.3f}  "
              f"|Z|>2 dans {row['pct_gt2']:.1f}% des cas")

    print(f"\n{'='*70}")
    print(f"PERCENTILES MOYENS AUX DATES OUTLIERS")
    print(f"{'='*70}")
    pct_cols = [c for c in df_global.columns if c.endswith('_pct')]
    print(f"\n  {'Variable':<30} {'Pct moyen':>10} {'Pct médian':>11} {'% >p90':>8} {'% <p10':>8}")
    print(f"  {'-'*70}")
    for col in pct_cols:
        var = col.replace('_pct', '')
        p = df_global[col].dropna()
        if len(p) == 0:
            continue
        pct_above_90 = (p > 90).mean() * 100
        pct_below_10 = (p < 10).mean() * 100
        flag = " ⚠" if pct_above_90 > 20 or pct_below_10 > 20 else ""
        print(f"  {var:<30} {p.mean():>10.1f} {p.median():>11.1f} "
              f"{pct_above_90:>7.1f}% {pct_below_10:>7.1f}%{flag}")

    # Sauvegarde du résumé
    summary_csv = OUT_CSV.parent / "diagnostic_summary_27j.csv"
    df_summary.to_csv(summary_csv, index=False)
    print(f"\n✅ Résumé → {summary_csv}")