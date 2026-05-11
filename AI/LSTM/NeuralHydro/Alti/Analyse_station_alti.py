#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Analyse complète des stations altimetriques à fréquence ~10j
============================================================
- Détection automatique des stations à mode d'intervalle = 10j
- Profil des trous (gaps) : nombre, durée, distribution
- Couverture temporelle et % de données manquantes
- Dynamique hydrologique : cycle annuel moyen normalisé
- Tableau synthétique CSV
- Figures individuelles par station

Sortie :
  ./Exploring_data/Stations_10j/synthese_stations_10j.csv
  ./Exploring_data/Stations_10j/figures/  (PNG par station)
  ./Exploring_data/Stations_10j/stations_10j.txt
"""

import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from pathlib import Path
from collections import Counter
import warnings
warnings.filterwarnings('ignore')

# ═══════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════
DB_PATH = "./data/hydro_data.db"
OUT_DIR = Path("./Exploring_data/Stations_10j")
FIG_DIR = OUT_DIR / "figures"
OUT_DIR.mkdir(parents=True, exist_ok=True)
FIG_DIR.mkdir(parents=True, exist_ok=True)

# Tolérance pour le mode d'intervalle (jours)
MODE_TARGET = 10
MODE_TOLERANCE = 1  # accepte 9, 10, 11


# ═══════════════════════════════════════════════════════════════
# 1. DÉTECTION DES STATIONS À FRÉQUENCE ~10j
# ═══════════════════════════════════════════════════════════════
def detecter_stations_10j(conn):
    """
    Identifie les stations dont le mode des intervalles entre mesures
    est ~10j (±tolérance). Utilise le mode, pas la moyenne (robuste aux trous).
    """
    print("=" * 70)
    print("1. DÉTECTION DES STATIONS À FRÉQUENCE ~10j")
    print("=" * 70)

    stations = pd.read_sql(
        "SELECT station_code, river_name, basin_name, nb_measurements "
        "FROM stations ORDER BY station_code", conn
    )

    resultats = []
    for _, sta in stations.iterrows():
        code = sta['station_code']
        df = pd.read_sql(
            "SELECT measure_date FROM measurements "
            "WHERE station_code = ? ORDER BY measure_date",
            conn, params=(code,)
        )
        df['measure_date'] = pd.to_datetime(df['measure_date'])
        if len(df) < 5:
            continue

        intervals = df['measure_date'].diff().dt.days.dropna().astype(int)
        if len(intervals) == 0:
            continue

        mode_interval = Counter(intervals.values).most_common(1)[0][0]
        resultats.append({
            'code': code,
            'river': sta['river_name'],
            'basin': sta['basin_name'],
            'nb_mes': len(df),
            'mode_interval': mode_interval,
            'mean_interval': intervals.mean(),
            'median_interval': intervals.median(),
        })

    df_res = pd.DataFrame(resultats)
    mask_10j = df_res['mode_interval'].between(
        MODE_TARGET - MODE_TOLERANCE, MODE_TARGET + MODE_TOLERANCE
    )
    stations_10j = df_res[mask_10j].copy()
    stations_10j = stations_10j.sort_values('code').reset_index(drop=True)

    print(f"\n  Total stations analysées : {len(df_res)}")
    print(f"  Distribution des modes d'intervalle :")
    for mode, count in df_res['mode_interval'].value_counts().sort_index().items():
        marker = " ◄── CIBLE" if MODE_TARGET - MODE_TOLERANCE <= mode <= MODE_TARGET + MODE_TOLERANCE else ""
        print(f"    {mode:3d}j : {count:3d} stations{marker}")

    print(f"\n  → {len(stations_10j)} stations à fréquence ~10j retenues")

    # Sauvegarder la liste
    with open(OUT_DIR / "stations_10j.txt", 'w') as f:
        for code in stations_10j['code']:
            f.write(f"{code}\n")
    print(f"  → Liste sauvegardée dans {OUT_DIR / 'stations_10j.txt'}")

    return stations_10j


# ═══════════════════════════════════════════════════════════════
# 2. ANALYSE DES TROUS (GAPS)
# ═══════════════════════════════════════════════════════════════
def analyser_gaps(conn, stations_10j):
    """
    Pour chaque station : nombre de gaps, durée min/max/moy,
    couverture temporelle, % manquant.
    """
    print("\n" + "=" * 70)
    print("2. ANALYSE DES TROUS (GAPS)")
    print("=" * 70)

    resultats = []
    for _, sta in stations_10j.iterrows():
        code = sta['code']
        df = pd.read_sql(
            "SELECT measure_date, orthometric_height FROM measurements "
            "WHERE station_code = ? ORDER BY measure_date",
            conn, params=(code,)
        )
        df['measure_date'] = pd.to_datetime(df['measure_date'])

        date_min = df['measure_date'].min()
        date_max = df['measure_date'].max()
        duree_totale = (date_max - date_min).days

        intervals = df['measure_date'].diff().dt.days.dropna().values

        # Un gap = intervalle > 1.5 × mode attendu (15j)
        seuil_gap = 15
        gaps = intervals[intervals > seuil_gap]
        nb_gaps = len(gaps)

        # Nombre théorique de mesures à 10j
        nb_theorique = duree_totale / 10
        couverture_pct = (len(df) / nb_theorique * 100) if nb_theorique > 0 else 0

        # Stats water_level
        wl = df['orthometric_height'].dropna()
        wl_mean = wl.mean() if len(wl) > 0 else np.nan
        wl_std = wl.std() if len(wl) > 1 else np.nan
        wl_range = wl.max() - wl.min() if len(wl) > 0 else np.nan

        resultats.append({
            'code': code,
            'river': sta['river'],
            'basin': sta['basin'],
            'date_debut': date_min.strftime('%Y-%m-%d'),
            'date_fin': date_max.strftime('%Y-%m-%d'),
            'duree_jours': duree_totale,
            'nb_mesures': len(df),
            'nb_theorique': int(nb_theorique),
            'couverture_pct': round(couverture_pct, 1),
            'nb_gaps': nb_gaps,
            'gap_max_jours': int(gaps.max()) if nb_gaps > 0 else 0,
            'gap_moy_jours': round(gaps.mean(), 1) if nb_gaps > 0 else 0,
            'gap_total_jours': int(gaps.sum()) if nb_gaps > 0 else 0,
            'wl_mean': round(wl_mean, 2),
            'wl_std': round(wl_std, 3),
            'wl_range': round(wl_range, 3),
            'intervalle_moy': round(intervals.mean(), 1),
            'intervalle_std': round(intervals.std(), 1),
        })

    df_gaps = pd.DataFrame(resultats)

    # Résumé global
    print(f"\n  Couverture temporelle :")
    print(f"    Médiane : {df_gaps['couverture_pct'].median():.1f}%")
    print(f"    Min     : {df_gaps['couverture_pct'].min():.1f}% ({df_gaps.loc[df_gaps['couverture_pct'].idxmin(), 'code']})")
    print(f"    Max     : {df_gaps['couverture_pct'].max():.1f}% ({df_gaps.loc[df_gaps['couverture_pct'].idxmax(), 'code']})")

    print(f"\n  Gaps (intervalles > 15j) :")
    print(f"    Stations sans gap    : {(df_gaps['nb_gaps'] == 0).sum()}")
    print(f"    Stations avec gaps   : {(df_gaps['nb_gaps'] > 0).sum()}")
    print(f"    Gap max global       : {df_gaps['gap_max_jours'].max()}j ({df_gaps.loc[df_gaps['gap_max_jours'].idxmax(), 'code']})")

    print(f"\n  Nombre de mesures :")
    print(f"    Médiane : {df_gaps['nb_mesures'].median():.0f}")
    print(f"    Min     : {df_gaps['nb_mesures'].min()} ({df_gaps.loc[df_gaps['nb_mesures'].idxmin(), 'code']})")
    print(f"    Max     : {df_gaps['nb_mesures'].max()} ({df_gaps.loc[df_gaps['nb_mesures'].idxmax(), 'code']})")

    print(f"\n  Amplitude water_level (range) :")
    print(f"    Médiane : {df_gaps['wl_range'].median():.2f} m")
    print(f"    Min     : {df_gaps['wl_range'].min():.2f} m")
    print(f"    Max     : {df_gaps['wl_range'].max():.2f} m")

    return df_gaps


# ═══════════════════════════════════════════════════════════════
# 3. DYNAMIQUE HYDROLOGIQUE PAR STATION
# ═══════════════════════════════════════════════════════════════
def plot_station_complete(conn, code, river, basin, info_row, fig_dir):
    """
    Figure complète pour une station :
      - Haut : série temporelle brute avec gaps marqués
      - Bas : cycle annuel moyen normalisé (z-score par DOY)
    """
    df = pd.read_sql(
        "SELECT measure_date, orthometric_height FROM measurements "
        "WHERE station_code = ? ORDER BY measure_date",
        conn, params=(code,)
    )
    df['measure_date'] = pd.to_datetime(df['measure_date'])
    df = df.set_index('measure_date').sort_index()
    df = df.rename(columns={'orthometric_height': 'wl'})
    df['wl'] = pd.to_numeric(df['wl'], errors='coerce')

    fig, axes = plt.subplots(2, 1, figsize=(14, 8), gridspec_kw={'height_ratios': [2, 1.2]})
    fig.suptitle(f"{code} — {river} ({basin})", fontsize=14, fontweight='bold')

    # ── Panel 1 : série temporelle ──
    ax1 = axes[0]
    ax1.plot(df.index, df['wl'], 'o-', markersize=2, linewidth=0.8, color='#2196F3', alpha=0.8)

    # Marquer les gaps > 15j
    intervals = df.index.to_series().diff().dt.days
    gap_mask = intervals > 15
    for gap_start, gap_days in zip(df.index[gap_mask], intervals[gap_mask]):
        gap_end = gap_start
        gap_begin = gap_start - pd.Timedelta(days=int(gap_days))
        ax1.axvspan(gap_begin, gap_end, alpha=0.15, color='red', zorder=0)

    ax1.set_ylabel("Hauteur orthométrique (m)")
    ax1.set_title(f"Série temporelle — {info_row['nb_mesures']} mesures, "
                  f"couverture {info_row['couverture_pct']}%, "
                  f"{info_row['nb_gaps']} gaps (max {info_row['gap_max_jours']}j)",
                  fontsize=10)
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    ax1.xaxis.set_major_locator(mdates.YearLocator())
    ax1.grid(True, alpha=0.3)

    # ── Panel 2 : cycle annuel moyen ──
    ax2 = axes[1]
    wl_clean = df['wl'].dropna()
    if len(wl_clean) > 10:
        # Normalisation z-score
        wl_norm = (wl_clean - wl_clean.mean()) / wl_clean.std()
        doy = np.clip(wl_norm.index.dayofyear, 1, 365)
        df_cycle = pd.DataFrame({'doy': doy, 'wl_norm': wl_norm.values})

        cycle_mean = df_cycle.groupby('doy')['wl_norm'].mean()
        cycle_std = df_cycle.groupby('doy')['wl_norm'].std()
        cycle_count = df_cycle.groupby('doy')['wl_norm'].count()

        # Lissage rolling (fenêtre circulaire)
        cycle_ext = pd.concat([cycle_mean.iloc[-15:], cycle_mean, cycle_mean.iloc[:15]])
        smooth = cycle_ext.rolling(7, center=True, min_periods=1).mean()
        smooth = smooth.iloc[15:-15]

        std_ext = pd.concat([cycle_std.iloc[-15:], cycle_std, cycle_std.iloc[:15]])
        smooth_std = std_ext.rolling(7, center=True, min_periods=1).mean()
        smooth_std = smooth_std.iloc[15:-15].fillna(0)

        ax2.fill_between(smooth.index, smooth - smooth_std, smooth + smooth_std,
                         alpha=0.2, color='#2196F3', label='±1 std')
        ax2.plot(smooth.index, smooth.values, color='#1565C0', linewidth=1.5, label='Moyenne')
        ax2.axhline(0, color='gray', linestyle='--', linewidth=0.5)

        # Labels mois
        month_ticks = [1, 32, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335]
        month_labels = ['Jan', 'Fév', 'Mar', 'Avr', 'Mai', 'Jun',
                        'Jul', 'Aoû', 'Sep', 'Oct', 'Nov', 'Déc']
        ax2.set_xticks(month_ticks)
        ax2.set_xticklabels(month_labels)
        ax2.set_ylabel("Water level normalisé (z-score)")
        ax2.set_title("Cycle annuel moyen", fontsize=10)
        ax2.legend(loc='upper right', fontsize=8)
        ax2.grid(True, alpha=0.3)
        ax2.set_xlim(1, 365)
    else:
        ax2.text(0.5, 0.5, "Pas assez de données pour le cycle annuel",
                 transform=ax2.transAxes, ha='center', va='center', fontsize=12, color='gray')

    plt.tight_layout()
    plt.savefig(fig_dir / f"{code}.png", dpi=150, bbox_inches='tight')
    plt.close()


def analyser_dynamiques(conn, df_gaps):
    """Génère les figures pour chaque station."""
    print("\n" + "=" * 70)
    print("3. DYNAMIQUE HYDROLOGIQUE — FIGURES PAR STATION")
    print("=" * 70)

    for i, row in df_gaps.iterrows():
        print(f"  [{i+1:2d}/{len(df_gaps)}] {row['code']} — {row['river']}", end="")
        try:
            plot_station_complete(conn, row['code'], row['river'], row['basin'], row, FIG_DIR)
            print(" ✅")
        except Exception as e:
            print(f" ❌ {e}")

    print(f"\n  → Figures sauvegardées dans {FIG_DIR}/")


# ═══════════════════════════════════════════════════════════════
# 4. FIGURE RÉCAPITULATIVE
# ═══════════════════════════════════════════════════════════════
def plot_recapitulatif(df_gaps):
    """Figure synthétique avec 4 panels."""
    print("\n" + "=" * 70)
    print("4. FIGURE RÉCAPITULATIVE")
    print("=" * 70)

    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle(f"Synthèse des {len(df_gaps)} stations à fréquence ~10j", fontsize=14, fontweight='bold')

    # Panel 1 : distribution du nombre de mesures
    ax = axes[0, 0]
    ax.hist(df_gaps['nb_mesures'], bins=20, color='#2196F3', edgecolor='white', alpha=0.8)
    ax.axvline(df_gaps['nb_mesures'].median(), color='red', linestyle='--', label=f"Médiane = {df_gaps['nb_mesures'].median():.0f}")
    ax.set_xlabel("Nombre de mesures")
    ax.set_ylabel("Nombre de stations")
    ax.set_title("Distribution du nombre de mesures")
    ax.legend()

    # Panel 2 : distribution de la couverture
    ax = axes[0, 1]
    ax.hist(df_gaps['couverture_pct'], bins=20, color='#4CAF50', edgecolor='white', alpha=0.8)
    ax.axvline(df_gaps['couverture_pct'].median(), color='red', linestyle='--', label=f"Médiane = {df_gaps['couverture_pct'].median():.1f}%")
    ax.set_xlabel("Couverture (%)")
    ax.set_ylabel("Nombre de stations")
    ax.set_title("Couverture temporelle (mesures / théorique)")
    ax.legend()

    # Panel 3 : gap max par station
    ax = axes[1, 0]
    df_sorted = df_gaps.sort_values('gap_max_jours', ascending=True)
    colors = ['#F44336' if g > 60 else '#FF9800' if g > 30 else '#4CAF50' for g in df_sorted['gap_max_jours']]
    ax.barh(range(len(df_sorted)), df_sorted['gap_max_jours'], color=colors, edgecolor='white', alpha=0.8)
    ax.set_yticks(range(len(df_sorted)))
    ax.set_yticklabels(df_sorted['code'], fontsize=6)
    ax.set_xlabel("Gap max (jours)")
    ax.set_title("Gap maximal par station")
    ax.axvline(30, color='orange', linestyle='--', linewidth=0.8, alpha=0.5)
    ax.axvline(60, color='red', linestyle='--', linewidth=0.8, alpha=0.5)

    # Panel 4 : amplitude vs couverture
    ax = axes[1, 1]
    scatter = ax.scatter(df_gaps['couverture_pct'], df_gaps['wl_range'],
                         c=df_gaps['nb_mesures'], cmap='viridis', s=50, alpha=0.7, edgecolors='white')
    ax.set_xlabel("Couverture (%)")
    ax.set_ylabel("Amplitude water_level (m)")
    ax.set_title("Amplitude vs Couverture")
    plt.colorbar(scatter, ax=ax, label='Nb mesures')

    plt.tight_layout()
    plt.savefig(OUT_DIR / "recapitulatif_stations_10j.png", dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  → Figure sauvegardée dans {OUT_DIR / 'recapitulatif_stations_10j.png'}")


# ═══════════════════════════════════════════════════════════════
# 5. DISTRIBUTION DES INTERVALLES (toutes stations 10j)
# ═══════════════════════════════════════════════════════════════
def plot_distribution_intervalles(conn, stations_10j):
    """Histogramme global de tous les intervalles entre mesures."""
    print("\n" + "=" * 70)
    print("5. DISTRIBUTION DES INTERVALLES")
    print("=" * 70)

    all_intervals = []
    for code in stations_10j['code']:
        df = pd.read_sql(
            "SELECT measure_date FROM measurements WHERE station_code = ? ORDER BY measure_date",
            conn, params=(code,)
        )
        df['measure_date'] = pd.to_datetime(df['measure_date'])
        intervals = df['measure_date'].diff().dt.days.dropna().astype(int).values
        all_intervals.extend(intervals)

    all_intervals = np.array(all_intervals)

    fig, ax = plt.subplots(figsize=(10, 5))
    bins = np.arange(0, min(all_intervals.max() + 2, 100), 1)
    ax.hist(all_intervals, bins=bins, color='#2196F3', edgecolor='white', alpha=0.8)
    ax.axvline(10, color='red', linestyle='--', linewidth=1.5, label='10j (attendu)')
    ax.set_xlabel("Intervalle entre mesures (jours)")
    ax.set_ylabel("Fréquence")
    ax.set_title(f"Distribution des intervalles — {len(stations_10j)} stations (~{len(all_intervals)} intervalles)")
    ax.legend()
    ax.set_xlim(0, 60)
    ax.grid(True, alpha=0.3)

    # Stats
    print(f"  Total intervalles : {len(all_intervals)}")
    print(f"  Mode : {Counter(all_intervals).most_common(1)[0]}")
    print(f"  Médiane : {np.median(all_intervals):.0f}j")
    print(f"  % dans [9-11]j : {np.mean((all_intervals >= 9) & (all_intervals <= 11)) * 100:.1f}%")
    print(f"  % > 30j : {np.mean(all_intervals > 30) * 100:.1f}%")

    plt.tight_layout()
    plt.savefig(OUT_DIR / "distribution_intervalles_10j.png", dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  → Figure sauvegardée")


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("╔══════════════════════════════════════════════════════════════╗")
    print("║  ANALYSE DES STATIONS ALTIMETRIQUES À FRÉQUENCE ~10j       ║")
    print("╚══════════════════════════════════════════════════════════════╝\n")

    conn = sqlite3.connect(DB_PATH)

    # 1. Détection
    stations_10j = detecter_stations_10j(conn)

    if len(stations_10j) == 0:
        print("\n❌ Aucune station à fréquence ~10j trouvée. Fin.")
        conn.close()
        exit()

    # 2. Analyse des gaps
    df_gaps = analyser_gaps(conn, stations_10j)

    # 3. Figures par station
    analyser_dynamiques(conn, df_gaps)

    # 4. Figure récapitulative
    plot_recapitulatif(df_gaps)

    # 5. Distribution des intervalles
    plot_distribution_intervalles(conn, stations_10j)

    # 6. Sauvegarde CSV synthétique
    csv_path = OUT_DIR / "synthese_stations_10j.csv"
    df_gaps.to_csv(csv_path, index=False, float_format='%.2f')
    print(f"\n  → Tableau synthétique sauvegardé dans {csv_path}")

    # 7. Résumé final
    print("\n" + "=" * 70)
    print("RÉSUMÉ FINAL")
    print("=" * 70)
    print(f"  Stations à ~10j     : {len(df_gaps)}")
    print(f"  Mesures totales     : {df_gaps['nb_mesures'].sum()}")
    print(f"  Couverture médiane  : {df_gaps['couverture_pct'].median():.1f}%")
    print(f"  Gap max global      : {df_gaps['gap_max_jours'].max()}j")
    print(f"  Amplitude médiane   : {df_gaps['wl_range'].median():.2f} m")
    print(f"\n  Fichiers produits :")
    print(f"    {OUT_DIR / 'stations_10j.txt'}")
    print(f"    {csv_path}")
    print(f"    {OUT_DIR / 'recapitulatif_stations_10j.png'}")
    print(f"    {OUT_DIR / 'distribution_intervalles_10j.png'}")
    print(f"    {FIG_DIR}/ ({len(df_gaps)} PNG)")

    conn.close()
    print("\n✅ Analyse terminée.")