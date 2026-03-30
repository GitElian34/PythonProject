import sqlite3
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy import stats

INSITU_DB_PATH   = "./data/insitu_data.db"
OUTPUT_DIR       = "./data/insitu/visualisation/dynamique_par_sauts"
CATEGORIES_SAUTS = ['aucun', '< 10', '10-100', '100-500', '> 500']
COLORS_SAUTS     = ['#4CAF50', '#2196F3', '#FF9800', '#FF5722', '#9C27B0']

import os
os.makedirs(OUTPUT_DIR, exist_ok=True)


# ─────────────────────────────────────────────
# Chargement données
# ─────────────────────────────────────────────
def get_stations_avec_sauts():
    conn = sqlite3.connect(INSITU_DB_PATH)
    df   = pd.read_sql_query("""
        SELECT code_sta, qualite_sauts
        FROM stations_insitu
        WHERE qualite_sauts IS NOT NULL
          AND (dans_lac IS NULL OR dans_lac NOT IN ('dans_lac', 'proche_lac'))
    """, conn)
    conn.close()
    return df


def get_serie_station(station_code):
    conn = sqlite3.connect(INSITU_DB_PATH)
    df   = pd.read_sql_query("""
        SELECT date, h_med_wsh
        FROM mesures_insitu
        WHERE code_sta = ?
          AND h_med_wsh IS NOT NULL
        ORDER BY date
    """, conn, params=(station_code,))
    conn.close()
    df['date'] = pd.to_datetime(df['date'])
    return df


# ─────────────────────────────────────────────
# Calcul métriques dynamiques
# ─────────────────────────────────────────────
def calculer_dynamique(df):
    if len(df) < 30:
        return None

    h = df['h_med_wsh'].values

    # 1. Écart-type — variabilité globale
    std = np.std(h)

    # 2. |Δh| moyen journalier — réactivité
    delta_h = np.mean(np.abs(np.diff(h)))

    # 3. Autocorrélation à lag 7j — mémoire du signal
    autocorr_7 = pd.Series(h).autocorr(lag=7)

    # 4. Coefficient de variation — variabilité relative (robuste aux échelles)
    mean_h = np.mean(np.abs(h))
    cv     = std / mean_h if mean_h > 0 else np.nan

    return {
        'std':        std,
        'delta_h':    delta_h,
        'autocorr_7': autocorr_7,
        'cv':         cv,
        'n_obs':      len(df),
    }


# ─────────────────────────────────────────────
# Visualisation
# ─────────────────────────────────────────────
def plot_dynamique(df_res):
    METRIQUES = [
        ('std',        'Écart-type (m)',              True),
        ('delta_h',    '|Δh| moyen journalier (m)',   True),
        ('autocorr_7', 'Autocorrélation lag 7j',      False),
        ('cv',         'Coefficient de variation',    True),
    ]

    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle(
        f"Dynamique des stations insitu par catégorie de sauts\n"
        f"({len(df_res)} stations rivières)",
        fontsize=13, fontweight='bold'
    )

    for ax, (col, titre, plus_grand_meilleur) in zip(axes.flatten(), METRIQUES):
        moyennes = []
        erreurs  = []
        labels   = []

        for cat in CATEGORIES_SAUTS:
            groupe = df_res[df_res['qualite_sauts'] == cat][col].dropna()
            moyennes.append(groupe.mean() if not groupe.empty else np.nan)
            erreurs.append(groupe.std()   if not groupe.empty else np.nan)
            labels.append(f"{cat}\n(n={len(groupe)})")

        vals_ok  = [v for v in moyennes if not np.isnan(v)]
        if not vals_ok:
            continue

        colors   = list(COLORS_SAUTS)
        best_idx = int(np.argmax(moyennes)) if plus_grand_meilleur else int(np.argmin(moyennes))
        colors[best_idx] = 'seagreen'

        bars = ax.bar(range(len(CATEGORIES_SAUTS)), moyennes,
                      color=colors, alpha=0.85, edgecolor='white', width=0.6,
                      yerr=erreurs, capsize=4, error_kw={'linewidth': 1, 'alpha': 0.6})

        ax.text(best_idx, moyennes[best_idx] + (erreurs[best_idx] or 0) + max(vals_ok) * 0.03,
                '★', ha='center', va='bottom', fontsize=14, color='gold')

        for i, v in enumerate(moyennes):
            if not np.isnan(v):
                ax.text(i, (erreurs[i] or 0) + v + max(vals_ok) * 0.01,
                        f"{v:.3f}", ha='center', va='bottom',
                        fontsize=8, fontweight='bold')

        ax.set_xticks(range(len(CATEGORIES_SAUTS)))
        ax.set_xticklabels(labels, fontsize=9)
        ax.set_title(titre, fontsize=11, fontweight='bold')
        ax.set_ylabel(titre, fontsize=9)
        ax.grid(axis='y', alpha=0.3)

    plt.tight_layout()
    path = os.path.join(OUTPUT_DIR, "dynamique_par_sauts.png")
    plt.savefig(path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  📊 {path}")


# ─────────────────────────────────────────────
# Test statistique Mann-Whitney entre groupes
# ─────────────────────────────────────────────
def print_tests_stats(df_res):
    METRIQUES = ['std', 'delta_h', 'autocorr_7', 'cv']
    ref_cat   = 'aucun'

    print(f"\n{'═'*65}")
    print(f"  Tests Mann-Whitney vs '{ref_cat}' (p < 0.05 = différence significative)")
    print(f"{'═'*65}")

    for col in METRIQUES:
        print(f"\n  {col}")
        ref = df_res[df_res['qualite_sauts'] == ref_cat][col].dropna()
        for cat in CATEGORIES_SAUTS:
            if cat == ref_cat:
                continue
            groupe = df_res[df_res['qualite_sauts'] == cat][col].dropna()
            if len(groupe) < 3 or len(ref) < 3:
                continue
            _, p = stats.mannwhitneyu(ref, groupe, alternative='two-sided')
            sig  = "✅ significatif" if p < 0.05 else "❌ non significatif"
            print(f"    {ref_cat} vs {cat:<10} p={p:.4f}  →  {sig}")


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────
if __name__ == "__main__":
    df_stations = get_stations_avec_sauts()
    print(f"📍 {len(df_stations)} stations rivières chargées")
    print(df_stations['qualite_sauts'].value_counts().to_string())

    resultats = []
    for i, (_, row) in enumerate(df_stations.iterrows()):
        df_serie = get_serie_station(row['code_sta'])
        m        = calculer_dynamique(df_serie)
        if m is None:
            continue
        resultats.append({
            'code_sta':      row['code_sta'],
            'qualite_sauts': row['qualite_sauts'],
            **m
        })

        if (i + 1) % 300 == 0:
            print(f"  [{i+1}/{len(df_stations)}] en cours...")

    df_res = pd.DataFrame(resultats)
    print(f"\n✅ {len(df_res)} stations analysées")

    # Résumé terminal
    print(f"\n{'═'*70}")
    print(f"  {'Catégorie':<12} {'n':>5} {'std':>8} {'|Δh|':>8} {'autocorr7':>10} {'CV':>8}")
    print(f"  {'─'*65}")
    for cat in CATEGORIES_SAUTS:
        g = df_res[df_res['qualite_sauts'] == cat]
        if not g.empty:
            print(f"  {cat:<12} {len(g):>5} "
                  f"{g['std'].mean():>8.4f} "
                  f"{g['delta_h'].mean():>8.4f} "
                  f"{g['autocorr_7'].mean():>10.4f} "
                  f"{g['cv'].mean():>8.4f}")
    print(f"{'═'*70}")

    print_tests_stats(df_res)
    plot_dynamique(df_res)