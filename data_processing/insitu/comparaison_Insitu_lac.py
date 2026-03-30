import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from scipy import stats

# ─────────────────────────────────────────
# 1. Chargement des données
# ─────────────────────────────────────────
def load_all_stations(db_path="./data/insitu_data.db"):
    conn = sqlite3.connect(db_path)
    df_flags = pd.read_sql_query(
        "SELECT code_sta, dans_lac FROM stations_insitu", conn
    )
    query = '''
        SELECT m.code_sta, m.date, m.h_09h_wsh, e.precip_jour
        FROM mesures_insitu m
        LEFT JOIN era5_insitu e ON m.code_sta = e.code_sta AND m.date = e.date
        ORDER BY m.code_sta, m.date
    '''
    df_mesures = pd.read_sql_query(query, conn)
    conn.close()

    df_mesures['date'] = pd.to_datetime(df_mesures['date'])
    df = df_mesures.merge(df_flags, on='code_sta', how='left')
    df['groupe'] = df['dans_lac'].apply(
        lambda x: 'lac/proche_lac' if x in ['dans_lac', 'proche_lac'] else 'rivière'
    )
    return df


# ─────────────────────────────────────────
# 2. Calcul des métriques par station
# ─────────────────────────────────────────
def compute_metrics(df):
    results = []
    for code_sta, grp in df.groupby('code_sta'):
        grp = grp.dropna(subset=['h_09h_wsh']).sort_values('date')
        if len(grp) < 30:
            continue

        h = grp['h_09h_wsh'].values

        std_h    = np.std(h)
        delta_h  = np.mean(np.abs(np.diff(h)))

        df_valid = grp.dropna(subset=['h_09h_wsh', 'precip_jour'])
        corr_precip = df_valid['h_09h_wsh'].corr(df_valid['precip_jour']) if len(df_valid) > 10 else np.nan

        autocorr_7 = pd.Series(h).autocorr(lag=7) if len(h) > 14 else np.nan

        results.append({
            'code_sta':    code_sta,
            'groupe':      grp['groupe'].iloc[0],
            'n_obs':       len(grp),
            'std_h':       std_h,
            'delta_h_moy': delta_h,
            'corr_precip': corr_precip,
            'autocorr_7j': autocorr_7
        })
    return pd.DataFrame(results)


# ─────────────────────────────────────────
# 3. Filtrage des outliers (2.5% de chaque côté)
# ─────────────────────────────────────────
def remove_outliers(metrics_df, cols, q_low=0.025, q_high=0.975):
    """Coupe les 2.5% extrêmes de chaque côté sur les colonnes spécifiées."""
    df_clean = metrics_df.copy()
    for col in cols:
        lo = df_clean[col].quantile(q_low)
        hi = df_clean[col].quantile(q_high)
        df_clean = df_clean[(df_clean[col].isna()) |
                            ((df_clean[col] >= lo) & (df_clean[col] <= hi))]
    n_removed = len(metrics_df) - len(df_clean)
    print(f"Outliers supprimés : {n_removed} stations "
          f"({n_removed/len(metrics_df)*100:.1f}%)")
    return df_clean


# ─────────────────────────────────────────
# 4. Visualisation
# ─────────────────────────────────────────
def plot_comparaison(metrics_df):
    groupes = ['rivière', 'lac/proche_lac']
    colors  = {'rivière': '#2196F3', 'lac/proche_lac': '#FF5722'}
    labels  = {'rivière': 'Rivière', 'lac/proche_lac': 'Lac / proche lac'}

    fig = plt.figure(figsize=(18, 12))
    fig.suptitle("Comparaison stations rivière vs lac/proche_lac\n(outliers 5% retirés)",
                 fontsize=14, fontweight='bold')
    gs = gridspec.GridSpec(2, 4, figure=fig, hspace=0.5, wspace=0.45)

    # ── Panneau A : std_h — violin + strip (lisible avec beaucoup de points) ──
    ax_std = fig.add_subplot(gs[:, 0])
    data_std = [metrics_df[metrics_df['groupe'] == g]['std_h'].dropna().values
                for g in groupes]
    parts = ax_std.violinplot(data_std, positions=[1, 2], showmedians=True,
                               showextrema=True)
    for pc, g in zip(parts['bodies'], groupes):
        pc.set_facecolor(colors[g]); pc.set_alpha(0.6)
    parts['cmedians'].set_color('black'); parts['cmedians'].set_linewidth(2)
    # Scatter jitter
    for i, g in enumerate(groupes):
        vals = metrics_df[metrics_df['groupe'] == g]['std_h'].dropna().values
        jitter = np.random.uniform(-0.08, 0.08, size=len(vals))
        ax_std.scatter(np.full(len(vals), i+1) + jitter, vals,
                       alpha=0.25, s=6, color=colors[g], zorder=3)
    ax_std.set_xticks([1, 2])
    ax_std.set_xticklabels([labels[g] for g in groupes], fontsize=9)
    ax_std.set_ylabel('Écart-type des hauteurs (m)', fontsize=9)
    ax_std.set_title('Variabilité globale', fontsize=10, fontweight='bold')
    ax_std.grid(axis='y', alpha=0.3)

    # ── Panneau B : delta_h — violin + strip ──
    ax_dh = fig.add_subplot(gs[:, 1])
    data_dh = [metrics_df[metrics_df['groupe'] == g]['delta_h_moy'].dropna().values
               for g in groupes]
    parts2 = ax_dh.violinplot(data_dh, positions=[1, 2], showmedians=True,
                               showextrema=True)
    for pc, g in zip(parts2['bodies'], groupes):
        pc.set_facecolor(colors[g]); pc.set_alpha(0.6)
    parts2['cmedians'].set_color('black'); parts2['cmedians'].set_linewidth(2)
    for i, g in enumerate(groupes):
        vals = metrics_df[metrics_df['groupe'] == g]['delta_h_moy'].dropna().values
        jitter = np.random.uniform(-0.08, 0.08, size=len(vals))
        ax_dh.scatter(np.full(len(vals), i+1) + jitter, vals,
                      alpha=0.25, s=6, color=colors[g], zorder=3)
    ax_dh.set_xticks([1, 2])
    ax_dh.set_xticklabels([labels[g] for g in groupes], fontsize=9)
    ax_dh.set_ylabel('|Δh| moyen journalier (m)', fontsize=9)
    ax_dh.set_title('Réactivité', fontsize=10, fontweight='bold')
    ax_dh.grid(axis='y', alpha=0.3)

    # ── Panneaux C & D : corr_precip + autocorr_7j — histogrammes KDE ──
    for j, (col, xlabel, title) in enumerate([
        ('corr_precip', 'Corrélation hauteur / précipitations', 'Lien pluie → hauteur'),
        ('autocorr_7j', 'Autocorrélation lag 7 jours',          'Mémoire du signal'),
    ]):
        ax_top = fig.add_subplot(gs[0, 2+j])
        ax_bot = fig.add_subplot(gs[1, 2+j])

        for g in groupes:
            vals = metrics_df[metrics_df['groupe'] == g][col].dropna()
            # Histogramme
            ax_top.hist(vals, bins=30, alpha=0.55, label=labels[g],
                        color=colors[g], density=True)
            # KDE
            kde_x = np.linspace(vals.min(), vals.max(), 300)
            kde   = stats.gaussian_kde(vals)
            ax_bot.plot(kde_x, kde(kde_x), color=colors[g],
                        label=labels[g], linewidth=2)
            ax_bot.fill_between(kde_x, kde(kde_x), alpha=0.2, color=colors[g])

        for ax in [ax_top, ax_bot]:
            ax.set_xlabel(xlabel, fontsize=8)
            ax.legend(fontsize=7)
            ax.grid(alpha=0.3)
        ax_top.set_ylabel('Densité', fontsize=8)
        ax_top.set_title(title, fontsize=10, fontweight='bold')
        ax_bot.set_ylabel('KDE', fontsize=8)

    plt.savefig("./data/insitu/visualisation/comparaison_stations.png",
                dpi=150, bbox_inches='tight')
    plt.show()
    print("Figure sauvegardée.")


# ─────────────────────────────────────────
# 5. Résumé statistique
# ─────────────────────────────────────────
def print_summary(metrics_df):
    print("\n── Résumé par groupe ──────────────────────────────────")
    summary = metrics_df.groupby('groupe')[
        ['std_h', 'delta_h_moy', 'corr_precip', 'autocorr_7j']
    ].agg(['median', 'mean', 'std']).round(4)
    print(summary.to_string())

    print("\n── Tests Mann-Whitney (p-value) ───────────────────────")
    for col in ['std_h', 'delta_h_moy', 'corr_precip', 'autocorr_7j']:
        a = metrics_df[metrics_df['groupe'] == 'rivière'][col].dropna()
        b = metrics_df[metrics_df['groupe'] == 'lac/proche_lac'][col].dropna()
        if len(a) > 3 and len(b) > 3:
            _, p = stats.mannwhitneyu(a, b, alternative='two-sided')
            sig  = "✅ significatif" if p < 0.05 else "❌ non significatif"
            print(f"  {col:<20} p={p:.4f}  →  {sig}")


# ─────────────────────────────────────────
# Main
# ─────────────────────────────────────────
if __name__ == "__main__":
    print("1")
    df = load_all_stations()
    print("2")
    metrics_df = compute_metrics(df)

    print(f"\n{len(metrics_df)} stations avant filtrage")
    print(metrics_df['groupe'].value_counts().to_string())

    metrics_clean = remove_outliers(
        metrics_df,
        cols=['std_h', 'delta_h_moy', 'corr_precip', 'autocorr_7j']
    )

    print(f"\n{len(metrics_clean)} stations après filtrage")
    print(metrics_clean['groupe'].value_counts().to_string())

    print_summary(metrics_clean)
    plot_comparaison(metrics_clean)