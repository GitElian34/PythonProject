"""
analyse_satellite_quality.py
═══════════════════════════════════════════════════════════════════════════
Analyse détaillée de la qualité du signal altimétrique pour les stations
10j et 27j : uncertainty, satellite/mission, orbit, track, répartition
temporelle, et croisement avec le NSE alti↔insitu.

Sorties (dans OUTPUT_DIR) :
  - satellite_quality_full.csv       : stats par station
  - satellite_quality_summary.csv    : stats agrégées par groupe (10j/27j)
  - satellite_by_mission.csv         : répartition des missions par groupe
  - satellite_quality_report.png     : figure multi-panneaux
═══════════════════════════════════════════════════════════════════════════
"""

import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import geopandas as gpd
from pathlib import Path
from shapely.geometry import Point
from scipy import stats

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
STATIONS_27J   = Path("./AI/LSTM/NeuralHydro_satellite_27D/stations_27j.txt")
STATIONS_10J   = Path("./AI/LSTM/NeuralHydro_satellite_10D/stations_10j.txt")
NSE_CSV        = Path("./data/outlier_detection/nse_alti_insitu_comparison.csv")

HYDRO_DB_PATH  = "./data/hydro_data.db"
INSITU_DB_PATH = "./data/insitu_data.db"
INSITU_SHP     = "./data/insitu/shp/station_schapi_alti_ref_2025_river.gpkg"

DIST_MAX_KM    = 50.0
DATE_MIN       = "2016-01-01"
DATE_MAX       = "2025-12-31"

OUTPUT_DIR     = Path("./data/outlier_detection")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ═══════════════════════════════════════════════════════════════
# CHARGEMENT LISTES DE STATIONS
# ═══════════════════════════════════════════════════════════════
def load_station_list(path: Path) -> list[str]:
    with open(path) as f:
        return [l.strip().zfill(13) for l in f if l.strip()]

stations_27j = load_station_list(STATIONS_27J)
stations_10j = load_station_list(STATIONS_10J)
all_stations = [(s, 10) for s in stations_10j] + [(s, 27) for s in stations_27j]
print(f"Stations 10j : {len(stations_10j)}  |  27j : {len(stations_27j)}")

# ═══════════════════════════════════════════════════════════════
# CHARGEMENT NSE ALTI↔INSITU (déjà calculé)
# ═══════════════════════════════════════════════════════════════
df_nse = None
if NSE_CSV.exists():
    df_nse = pd.read_csv(NSE_CSV)
    df_nse['station'] = df_nse['station'].astype(str).str.zfill(13)
    print(f"NSE CSV chargé : {len(df_nse)} lignes")
else:
    print("⚠️  NSE CSV non trouvé — croisement NSE désactivé")

# ═══════════════════════════════════════════════════════════════
# REQUÊTE PRINCIPALE : stats par station depuis measurements
# ═══════════════════════════════════════════════════════════════
def get_station_stats(station_code: str) -> dict:
    conn = sqlite3.connect(HYDRO_DB_PATH)

    # Essai des deux formats de code
    df = pd.DataFrame()
    for code in [station_code, station_code.lstrip("0") or station_code]:
        df = pd.read_sql_query("""
            SELECT
                measure_date,
                orthometric_height,
                uncertainty,
                satellite,
                orbit_mission,
                track_number,
                cycle_number,
                retracking_algorithm,
                gdr_version,
                is_valid
            FROM measurements
            WHERE station_code = ?
              AND measure_date >= ?
              AND measure_date <= ?
            ORDER BY measure_date
        """, conn, params=(code, DATE_MIN, DATE_MAX))
        if not df.empty:
            break

    # Coords station
    df_coords = pd.DataFrame()
    for code in [station_code, station_code.lstrip("0") or station_code]:
        df_coords = pd.read_sql_query(
            "SELECT reference_longitude, reference_latitude, mean_altitude, "
            "upstream_watershed_km2, width_approx_m "
            "FROM stations WHERE station_code = ?",
            conn, params=(code,)
        )
        if not df_coords.empty:
            break
    conn.close()

    if df.empty:
        return {'station': station_code, 'status': 'no_data'}

    df['measure_date'] = pd.to_datetime(df['measure_date'])

    # ── Métriques uncertainty ────────────────────────────────
    unc = df['uncertainty'].dropna()
    unc_valid = df[df['is_valid'] == 1]['uncertainty'].dropna()

    # ── Répartition satellites/missions ──────────────────────
    sat_counts  = df['satellite'].value_counts().to_dict()
    miss_counts = df['orbit_mission'].value_counts().to_dict()
    algo_counts = df['retracking_algorithm'].value_counts().to_dict()

    # Mission dominante
    dominant_satellite = df['satellite'].mode()[0] if not df['satellite'].isna().all() else None
    dominant_mission   = df['orbit_mission'].mode()[0] if not df['orbit_mission'].isna().all() else None
    dominant_algo      = df['retracking_algorithm'].mode()[0] if not df['retracking_algorithm'].isna().all() else None

    # ── Couverture temporelle ────────────────────────────────
    n_total    = len(df)
    n_valid    = int(df['is_valid'].sum())
    valid_rate = n_valid / n_total if n_total > 0 else np.nan

    date_range_days = (df['measure_date'].max() - df['measure_date'].min()).days
    n_missions = df['orbit_mission'].nunique()
    n_satellites = df['satellite'].nunique()

    # ── Variabilité du signal ────────────────────────────────
    wl_valid = df[df['is_valid'] == 1]['orthometric_height'].dropna()
    wl_std   = float(wl_valid.std()) if len(wl_valid) > 1 else np.nan
    wl_range = float(wl_valid.max() - wl_valid.min()) if len(wl_valid) > 1 else np.nan

    # ── Nombre de tracks distincts ───────────────────────────
    n_tracks = df['track_number'].nunique()

    # ── Attributs station ────────────────────────────────────
    lon = lat = alt = area = width = np.nan
    def safe_float(val):
        """Convertit en float, retourne NaN si vide, None, ou non numérique ('TBD', etc.)."""
        return float(pd.to_numeric(val, errors='coerce'))

    if not df_coords.empty:
        row0  = df_coords.iloc[0]
        lon   = safe_float(row0.get('reference_longitude'))
        lat   = safe_float(row0.get('reference_latitude'))
        alt   = safe_float(row0.get('mean_altitude'))
        area  = safe_float(row0.get('upstream_watershed_km2'))
        width = safe_float(row0.get('width_approx_m'))

    return {
        'station'           : station_code,
        'status'            : 'ok',
        'lon'               : lon,
        'lat'               : lat,
        'mean_altitude_m'   : alt,
        'watershed_km2'     : area,
        'width_m'           : width,
        # Uncertainty
        'unc_median'        : float(unc.median()) if len(unc) > 0 else np.nan,
        'unc_mean'          : float(unc.mean())   if len(unc) > 0 else np.nan,
        'unc_p75'           : float(unc.quantile(0.75)) if len(unc) > 0 else np.nan,
        'unc_p90'           : float(unc.quantile(0.90)) if len(unc) > 0 else np.nan,
        'unc_valid_median'  : float(unc_valid.median()) if len(unc_valid) > 0 else np.nan,
        'n_unc_available'   : int(unc.notna().sum()),
        # Couverture
        'n_total'           : n_total,
        'n_valid'           : n_valid,
        'valid_rate'        : valid_rate,
        'date_range_days'   : date_range_days,
        'date_first'        : str(df['measure_date'].min().date()),
        'date_last'         : str(df['measure_date'].max().date()),
        # Signal
        'wl_std'            : wl_std,
        'wl_range'          : wl_range,
        # Satellites / missions
        'dominant_satellite': dominant_satellite,
        'dominant_mission'  : dominant_mission,
        'dominant_algo'     : dominant_algo,
        'n_satellites'      : n_satellites,
        'n_missions'        : n_missions,
        'n_tracks'          : n_tracks,
        'sat_counts'        : str(sat_counts),
        'mission_counts'    : str(miss_counts),
        'algo_counts'       : str(algo_counts),
    }

# ── Boucle principale ────────────────────────────────────────
print("\nRequêtes en cours...")
rows = []
for sta, period in all_stations:
    r = get_station_stats(sta)
    r['period'] = period
    rows.append(r)
    flag = f"unc={r.get('unc_median', np.nan):.3f}m | valid={r.get('valid_rate', 0):.1%} | {r.get('dominant_satellite','?')}" \
           if r['status'] == 'ok' else r['status']
    print(f"  [{period:2d}j] {sta} | {flag}")

df_full = pd.DataFrame(rows)

# ── Croisement avec NSE alti↔insitu ─────────────────────────
if df_nse is not None:
    df_full = df_full.merge(
        df_nse[['station', 'nse', 'dist_km', 'code_insitu']].rename(
            columns={'nse': 'nse_alti_insitu'}),
        on='station', how='left'
    )

# ── Export CSV complet ───────────────────────────────────────
csv_full = OUTPUT_DIR / "satellite_quality_full.csv"
df_full.to_csv(csv_full, index=False)
print(f"\n✅ CSV complet → {csv_full}")

# ═══════════════════════════════════════════════════════════════
# SYNTHÈSE AGRÉGÉE PAR GROUPE
# ═══════════════════════════════════════════════════════════════
df_ok = df_full[df_full['status'] == 'ok'].copy()

summary_rows = []
for period in [10, 27]:
    sub = df_ok[df_ok['period'] == period]
    row = {'period': period, 'n_stations': len(sub)}
    for col in ['unc_median', 'unc_p75', 'unc_p90', 'valid_rate',
                'n_total', 'wl_std', 'n_missions', 'n_satellites']:
        vals = sub[col].dropna()
        row[f'{col}_median'] = float(vals.median()) if len(vals) > 0 else np.nan
        row[f'{col}_mean']   = float(vals.mean())   if len(vals) > 0 else np.nan
    if 'nse_alti_insitu' in df_ok.columns:
        nse = sub['nse_alti_insitu'].dropna()
        row['nse_alti_insitu_median'] = float(nse.median()) if len(nse) > 0 else np.nan
        row['nse_alti_insitu_mean']   = float(nse.mean())   if len(nse) > 0 else np.nan
        row['nse_gt05']  = int((nse > 0.5).sum())
        row['nse_lt0']   = int((nse < 0).sum())
    summary_rows.append(row)

df_summary = pd.DataFrame(summary_rows)
df_summary.to_csv(OUTPUT_DIR / "satellite_quality_summary.csv", index=False)

# Répartition missions
mission_rows = []
for period in [10, 27]:
    sub = df_ok[df_ok['period'] == period]
    for _, row in sub.iterrows():
        import ast
        try:
            mc = ast.literal_eval(row.get('mission_counts', '{}'))
        except Exception:
            mc = {}
        for mission, count in mc.items():
            mission_rows.append({'period': period, 'mission': mission, 'count': count})

df_missions = pd.DataFrame(mission_rows)
if not df_missions.empty:
    df_missions_agg = df_missions.groupby(['period', 'mission'])['count'].sum().reset_index()
    df_missions_agg.to_csv(OUTPUT_DIR / "satellite_by_mission.csv", index=False)
    print(f"✅ Missions CSV → {OUTPUT_DIR / 'satellite_by_mission.csv'}")

# ═══════════════════════════════════════════════════════════════
# AFFICHAGE CONSOLE
# ═══════════════════════════════════════════════════════════════
print("\n" + "═"*60)
print("SYNTHÈSE QUALITÉ SIGNAL SATELLITE")
print("═"*60)

for period in [10, 27]:
    sub = df_ok[df_ok['period'] == period]
    print(f"\n── Période {period}j  ({len(sub)} stations) ──")
    print(f"  Uncertainty médiane   : {sub['unc_median'].median():.3f} m")
    print(f"  Uncertainty p75       : {sub['unc_p75'].median():.3f} m")
    print(f"  Uncertainty p90       : {sub['unc_p90'].median():.3f} m")
    print(f"  Taux valid médian     : {sub['valid_rate'].median():.1%}")
    print(f"  Nb mesures médian     : {sub['n_total'].median():.0f}")
    print(f"  Std WL médiane        : {sub['wl_std'].median():.3f} m")
    if 'nse_alti_insitu' in sub.columns:
        nse = sub['nse_alti_insitu'].dropna()
        print(f"  NSE alti↔insitu médian: {nse.median():.3f}  (n={len(nse)})")

    # Top satellites
    sat_col = sub['dominant_satellite'].value_counts()
    print(f"  Satellites dominants  : {dict(sat_col.head(5))}")
    miss_col = sub['dominant_mission'].value_counts()
    print(f"  Missions dominantes   : {dict(miss_col.head(5))}")

# Test Mann-Whitney uncertainty 10j vs 27j
unc_10 = df_ok[df_ok['period'] == 10]['unc_median'].dropna()
unc_27 = df_ok[df_ok['period'] == 27]['unc_median'].dropna()
if len(unc_10) > 0 and len(unc_27) > 0:
    u_stat, p_val = stats.mannwhitneyu(unc_10, unc_27, alternative='two-sided')
    print(f"\nMann-Whitney uncertainty 10j vs 27j : U={u_stat:.0f}, p={p_val:.4f}")
    print("  → " + ("Différence significative ✅" if p_val < 0.05 else "Pas significatif ⚠️"))

# ═══════════════════════════════════════════════════════════════
# FIGURE MULTI-PANNEAUX
# ═══════════════════════════════════════════════════════════════
COLORS = {10: '#3A9CC9', 27: '#E88B1A'}
LABELS = {10: '10j', 27: '27j'}

fig = plt.figure(figsize=(18, 14))
gs  = gridspec.GridSpec(3, 3, figure=fig, hspace=0.42, wspace=0.35)

ax1 = fig.add_subplot(gs[0, 0])   # Boxplot uncertainty
ax2 = fig.add_subplot(gs[0, 1])   # Boxplot valid_rate
ax3 = fig.add_subplot(gs[0, 2])   # Boxplot NSE
ax4 = fig.add_subplot(gs[1, 0])   # Scatter uncertainty vs NSE
ax5 = fig.add_subplot(gs[1, 1])   # Scatter valid_rate vs NSE
ax6 = fig.add_subplot(gs[1, 2])   # Scatter n_missions vs NSE
ax7 = fig.add_subplot(gs[2, 0])   # Barplot missions 10j
ax8 = fig.add_subplot(gs[2, 1])   # Barplot missions 27j
ax9 = fig.add_subplot(gs[2, 2])   # Scatter unc vs valid_rate

rng = np.random.default_rng(42)

def boxstrip(ax, data_dict, ylabel, title, ref_lines=None, log=False):
    """Boxplot + strip pour 2 groupes."""
    vals = [data_dict[10].dropna().values, data_dict[27].dropna().values]
    labels = ['10j', '27j']
    bp = ax.boxplot(vals, labels=labels, patch_artist=True,
                    medianprops={'color': 'black', 'linewidth': 2}, widths=0.5)
    for i, (box, color) in enumerate(zip(bp['boxes'], [COLORS[10], COLORS[27]])):
        box.set_facecolor(color)
        box.set_alpha(0.7)
        jitter = rng.uniform(-0.15, 0.15, len(vals[i]))
        ax.scatter(np.full(len(vals[i]), i+1) + jitter, vals[i],
                   alpha=0.5, s=20, color=color, zorder=3)
        med = np.nanmedian(vals[i])
        ax.text(i+1, med, f' {med:.3f}', va='center', fontsize=8,
                fontweight='bold', color='black')
    if ref_lines:
        for val, lbl, col in ref_lines:
            ax.axhline(val, color=col, lw=1, ls='--', alpha=0.6, label=lbl)
        ax.legend(fontsize=7)
    if log:
        ax.set_yscale('log')
    ax.set_ylabel(ylabel, fontsize=9)
    ax.set_title(title, fontsize=10, fontweight='bold')
    ax.grid(True, alpha=0.3, axis='y')

# ── Panel 1 : Uncertainty ────────────────────────────────────
boxstrip(ax1,
         {10: df_ok[df_ok['period']==10]['unc_median'],
          27: df_ok[df_ok['period']==27]['unc_median']},
         'Uncertainty médiane (m)',
         'Uncertainty\nalti↔insitu')

# ── Panel 2 : Valid rate ─────────────────────────────────────
boxstrip(ax2,
         {10: df_ok[df_ok['period']==10]['valid_rate']*100,
          27: df_ok[df_ok['period']==27]['valid_rate']*100},
         'Taux de mesures valides (%)',
         'Taux de validité\ndes mesures')

# ── Panel 3 : NSE ────────────────────────────────────────────
if 'nse_alti_insitu' in df_ok.columns:
    boxstrip(ax3,
             {10: df_ok[df_ok['period']==10]['nse_alti_insitu'],
              27: df_ok[df_ok['period']==27]['nse_alti_insitu']},
             'NSE alti ↔ insitu',
             'NSE alti↔insitu\npar groupe',
             ref_lines=[(0, 'NSE=0', 'red'), (0.5, 'NSE=0.5', 'green')])

# ── Panel 4 : Scatter uncertainty vs NSE ────────────────────
if 'nse_alti_insitu' in df_ok.columns:
    for period in [10, 27]:
        sub = df_ok[(df_ok['period'] == period)].dropna(
            subset=['unc_median', 'nse_alti_insitu'])
        ax4.scatter(sub['unc_median'], sub['nse_alti_insitu'],
                    color=COLORS[period], alpha=0.7, s=40,
                    label=f'{period}j (n={len(sub)})', zorder=3)
        if len(sub) > 3:
            z = np.polyfit(sub['unc_median'], sub['nse_alti_insitu'], 1)
            x_line = np.linspace(sub['unc_median'].min(), sub['unc_median'].max(), 50)
            ax4.plot(x_line, np.polyval(z, x_line),
                     color=COLORS[period], lw=1.5, ls='--', alpha=0.8)
    ax4.axhline(0, color='red', lw=1, ls='--', alpha=0.5)
    ax4.set_xlabel('Uncertainty médiane (m)', fontsize=9)
    ax4.set_ylabel('NSE alti↔insitu', fontsize=9)
    ax4.set_title('Uncertainty vs NSE', fontsize=10, fontweight='bold')
    ax4.legend(fontsize=8)
    ax4.grid(True, alpha=0.3)

# ── Panel 5 : Scatter valid_rate vs NSE ─────────────────────
if 'nse_alti_insitu' in df_ok.columns:
    for period in [10, 27]:
        sub = df_ok[(df_ok['period'] == period)].dropna(
            subset=['valid_rate', 'nse_alti_insitu'])
        ax5.scatter(sub['valid_rate']*100, sub['nse_alti_insitu'],
                    color=COLORS[period], alpha=0.7, s=40,
                    label=f'{period}j', zorder=3)
        if len(sub) > 3:
            z = np.polyfit(sub['valid_rate'], sub['nse_alti_insitu'], 1)
            x_line = np.linspace(sub['valid_rate'].min(), sub['valid_rate'].max(), 50)
            ax5.plot(x_line*100, np.polyval(z, x_line),
                     color=COLORS[period], lw=1.5, ls='--', alpha=0.8)
    ax5.axhline(0, color='red', lw=1, ls='--', alpha=0.5)
    ax5.set_xlabel('Taux de validité (%)', fontsize=9)
    ax5.set_ylabel('NSE alti↔insitu', fontsize=9)
    ax5.set_title('Taux validité vs NSE', fontsize=10, fontweight='bold')
    ax5.legend(fontsize=8)
    ax5.grid(True, alpha=0.3)

# ── Panel 6 : Scatter n_missions vs NSE ─────────────────────
if 'nse_alti_insitu' in df_ok.columns:
    for period in [10, 27]:
        sub = df_ok[(df_ok['period'] == period)].dropna(
            subset=['n_missions', 'nse_alti_insitu'])
        jitter = rng.uniform(-0.1, 0.1, len(sub))
        ax6.scatter(sub['n_missions'] + jitter, sub['nse_alti_insitu'],
                    color=COLORS[period], alpha=0.7, s=40,
                    label=f'{period}j', zorder=3)
    ax6.axhline(0, color='red', lw=1, ls='--', alpha=0.5)
    ax6.set_xlabel('Nombre de missions distinctes', fontsize=9)
    ax6.set_ylabel('NSE alti↔insitu', fontsize=9)
    ax6.set_title('Nb missions vs NSE', fontsize=10, fontweight='bold')
    ax6.legend(fontsize=8)
    ax6.grid(True, alpha=0.3)

# ── Panels 7-8 : Barplot missions ───────────────────────────
for ax_m, period in [(ax7, 10), (ax8, 27)]:
    sub = df_ok[df_ok['period'] == period]
    import ast
    mission_agg: dict[str, int] = {}
    for _, row in sub.iterrows():
        try:
            mc = ast.literal_eval(row.get('mission_counts', '{}'))
        except Exception:
            mc = {}
        for m, c in mc.items():
            mission_agg[m] = mission_agg.get(m, 0) + c

    if mission_agg:
        df_m = pd.Series(mission_agg).sort_values(ascending=False).head(10)
        bars = ax_m.barh(df_m.index[::-1], df_m.values[::-1],
                         color=COLORS[period], alpha=0.8, edgecolor='white')
        for bar, val in zip(bars, df_m.values[::-1]):
            ax_m.text(bar.get_width() + df_m.max()*0.01, bar.get_y() + bar.get_height()/2,
                      str(val), va='center', fontsize=7)
    ax_m.set_title(f'Missions satellite — {period}j\n(total mesures)',
                   fontsize=10, fontweight='bold')
    ax_m.set_xlabel('Nombre de mesures', fontsize=9)
    ax_m.grid(True, alpha=0.3, axis='x')

# ── Panel 9 : Scatter uncertainty vs valid_rate ──────────────
for period in [10, 27]:
    sub = df_ok[df_ok['period'] == period].dropna(
        subset=['unc_median', 'valid_rate'])
    ax9.scatter(sub['unc_median'], sub['valid_rate']*100,
                color=COLORS[period], alpha=0.7, s=40,
                label=f'{period}j (n={len(sub)})', zorder=3)
ax9.set_xlabel('Uncertainty médiane (m)', fontsize=9)
ax9.set_ylabel('Taux de validité (%)', fontsize=9)
ax9.set_title('Uncertainty vs\nTaux validité', fontsize=10, fontweight='bold')
ax9.legend(fontsize=8)
ax9.grid(True, alpha=0.3)

fig.suptitle('Qualité du signal altimétrique — Analyse 10j vs 27j\n'
             '(uncertainty · validité · missions · NSE alti↔insitu)',
             fontsize=13, fontweight='bold', y=1.01)

fig_path = OUTPUT_DIR / "satellite_quality_report.png"
fig.savefig(fig_path, dpi=150, bbox_inches='tight')
plt.close(fig)
print(f"\n✅ Figure → {fig_path}")
print(f"✅ Terminé. Fichiers dans {OUTPUT_DIR}/")