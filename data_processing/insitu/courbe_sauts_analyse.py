import sqlite3
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.preprocessing import MinMaxScaler
import os

INSITU_DB_PATH   = "./data/insitu_data.db"
OUTPUT_DIR       = "./data/insitu/visualisation/cycle_annuel"
CATEGORIES_SAUTS = ['aucun', '< 10', '10-100', '100-500', '> 500']
COLORS_SAUTS     = ['#4CAF50', '#2196F3', '#FF9800', '#FF5722', '#9C27B0']
os.makedirs(OUTPUT_DIR, exist_ok=True)


# ─────────────────────────────────────────────
# Chargement des stations avec leur catégorie
# ─────────────────────────────────────────────
def get_stations_par_categorie():
    conn = sqlite3.connect(INSITU_DB_PATH)
    df   = pd.read_sql_query("""
        SELECT code_sta, qualite_sauts, signal_plat
        FROM stations_insitu
        WHERE qualite_sauts IS NOT NULL
          AND (dans_lac IS NULL OR dans_lac NOT IN ('dans_lac', 'proche_lac'))
    """, conn)
    conn.close()
    # Convertir en label lisible pour les graphes
    df['groupe_plat'] = df['signal_plat'].apply(
        lambda x: 'Signal plat' if x == 1 else 'Signal OK'
    )
    return df


def calculer_cycles(df_merge, col_groupe, categories, min_stations=5):
    """
    Calcule le cycle annuel moyen pour chaque valeur de col_groupe.
    min_stations : nombre minimum de stations pour inclure un groupe.
    """
    cycles = {}
    for cat in categories:
        sous_df = df_merge[df_merge[col_groupe] == cat]
        if sous_df['code_sta'].nunique() < min_stations:
            print(f"  ⚠️  {cat} — trop peu de stations ({sous_df['code_sta'].nunique()}), skip")
            continue

        cycle = sous_df.groupby('jour_annee').agg(
            h_norm_moy      =('h_norm',     'mean'),
            h_norm_std      =('h_norm',     'std'),
            h_anomalie_moy  =('h_anomalie', 'mean'),
            h_anomalie_std  =('h_anomalie', 'std'),
            n_stations      =('code_sta',   'nunique')
        ).reset_index()

        for col_src, col_dst in [
            ('h_norm_moy',    'h_lisse'),
            ('h_norm_std',    'std_lisse'),
            ('h_anomalie_moy','h_anomalie_lisse'),
            ('h_anomalie_std','std_anomalie_lisse'),
        ]:
            cycle[col_dst] = cycle[col_src].rolling(
                window=7, center=True, min_periods=1
            ).mean()

        cycles[cat] = cycle
        print(f"  {cat:<15} → {sous_df['code_sta'].nunique()} stations, "
              f"{len(sous_df):,} mesures")

    return cycles
# ─────────────────────────────────────────────
# Chargement des séries temporelles
# ─────────────────────────────────────────────
def get_series_temporelles(stations):
    conn  = sqlite3.connect(INSITU_DB_PATH)
    codes = stations['code_sta'].tolist()

    df = pd.read_sql_query(f"""
        SELECT code_sta, date, h_med_wsh
        FROM mesures_insitu
        WHERE code_sta IN ({','.join('?' * len(codes))})
          AND h_med_wsh IS NOT NULL
        ORDER BY code_sta, date
    """, conn, params=codes)
    conn.close()

    df['date']       = pd.to_datetime(df['date'])
    df['jour_annee'] = df['date'].dt.dayofyear  # 1 → 365
    return df


# ─────────────────────────────────────────────
# Normalisation et anomalie par station
# ─────────────────────────────────────────────
def normaliser_par_station(df):
    """
    Pour chaque station, calcule deux transformations indépendantes :
    - h_norm     : MinMax [0, 1] — pour comparer les formes
    - h_anomalie : h - moyenne de la station — pour comparer les variations
                   autour du niveau habituel. Les stations dont l'anomalie
                   dépasse ±5m sont exclues car elles correspondent à des
                   données corrompues (capteurs défaillants).
    """
    SEUIL_ANOMALIE = 5.0  # en mètres

    df = df.copy()
    df['h_norm']     = np.nan
    df['h_anomalie'] = np.nan

    # ── Étape 1 : calcul des deux transformations pour chaque station ──
    for code_sta, grp in df.groupby('code_sta'):
        vals = grp['h_med_wsh'].values

        # Anomalie = h - moyenne de la station sur toute sa période
        moy = np.mean(vals)
        df.loc[grp.index, 'h_anomalie'] = vals - moy

        # Normalisation MinMax — skip si signal complètement plat
        if vals.max() != vals.min():
            df.loc[grp.index, 'h_norm'] = MinMaxScaler().fit_transform(
                vals.reshape(-1, 1)
            ).flatten()

    # ── Étape 2 : filtrage des stations aberrantes APRÈS la boucle ──
    # On calcule le max absolu de l'anomalie par station en une seule passe
    anomalies_max = df.groupby('code_sta')['h_anomalie'].apply(lambda x: x.abs().max())
    stations_ok   = anomalies_max[anomalies_max <= SEUIL_ANOMALIE].index
    n_exclues     = df['code_sta'].nunique() - len(stations_ok)

    # On met à NaN les anomalies des stations hors seuil
    df.loc[~df['code_sta'].isin(stations_ok), 'h_anomalie'] = np.nan
    print(f"  Stations exclues (anomalie > ±{SEUIL_ANOMALIE}m) : {n_exclues}")

    return df


# ─────────────────────────────────────────────
# Visualisation
# ─────────────────────────────────────────────
def plot_cycles(cycles, categories, colors, nom_fichier, titre):
    fig, axes = plt.subplots(1, 2, figsize=(18, 7))
    fig.suptitle(titre, fontsize=13, fontweight='bold')

    mois_labels = ['Jan', 'Fév', 'Mar', 'Avr', 'Mai', 'Jun',
                   'Jul', 'Aoû', 'Sep', 'Oct', 'Nov', 'Déc']
    mois_jours  = [1, 32, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335]

    for ax, (col_y, col_std, ylabel, titre_ax) in zip(axes, [
        ('h_lisse',         'std_lisse',         'Hauteur normalisée [0-1]',     'Normalisé (MinMax)'),
        ('h_anomalie_lisse','std_anomalie_lisse', 'Anomalie (h - moyenne) en m', 'Anomalie centrée'),
    ]):
        for cat, color in zip(categories, colors):
            if cat not in cycles:
                continue
            cycle = cycles[cat]
            x     = cycle['jour_annee'].values
            y     = cycle[col_y].values
            std   = cycle[col_std].values
            n_sta = cycle['n_stations'].mean()

            ax.plot(x, y, color=color, linewidth=2,
                    label=f"{cat} (n≈{n_sta:.0f})")
            ax.fill_between(x, y - std * 0.5, y + std * 0.5,
                            color=color, alpha=0.12)

        if 'anomalie' in col_y:
            ax.axhline(0, color='black', linewidth=0.8, linestyle='--', alpha=0.4)

        ax.set_xticks(mois_jours)
        ax.set_xticklabels(mois_labels, fontsize=9)
        ax.set_xlabel("Mois", fontsize=10)
        ax.set_ylabel(ylabel, fontsize=10)
        ax.set_title(titre_ax, fontsize=11, fontweight='bold')
        ax.legend(fontsize=9)
        ax.grid(alpha=0.3)

    plt.tight_layout()
    path = os.path.join(OUTPUT_DIR, nom_fichier)
    plt.savefig(path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  📊 {path}")


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────
if __name__ == "__main__":
    print("📍 Chargement des stations...")
    df_stations = get_stations_par_categorie()
    print(df_stations['qualite_sauts'].value_counts().to_string())
    print(df_stations['groupe_plat'].value_counts().to_string())

    print("\n📂 Chargement des séries temporelles...")
    df_series = get_series_temporelles(df_stations)
    print(f"  {len(df_series):,} mesures chargées pour {df_series['code_sta'].nunique()} stations")

    print("\n⚙️  Normalisation et calcul des anomalies par station...")
    df_series = normaliser_par_station(df_series)

    # Fusion avec les catégories — on a besoin de qualite_sauts ET groupe_plat
    df_merge = df_series.merge(df_stations, on='code_sta', how='left')

    # ── Cycles par catégorie de sauts ──────────────────────────────
    print("\n── Cycles par catégorie de sauts ──")
    cycles_sauts = calculer_cycles(df_merge, 'qualite_sauts', CATEGORIES_SAUTS)

    # ── Cycles signal plat vs OK ───────────────────────────────────
    print("\n── Cycles signal plat vs OK ──")
    CATEGORIES_PLAT = ['Signal OK', 'Signal plat']
    COLORS_PLAT     = ['#2196F3', '#FF5722']
    cycles_plat     = calculer_cycles(df_merge, 'groupe_plat', CATEGORIES_PLAT)

    # ── Visualisations ─────────────────────────────────────────────
    print("\n📊 Génération des graphes...")

    plot_cycles(
        cycles_sauts,
        CATEGORIES_SAUTS,
        COLORS_SAUTS,
        "cycle_annuel_par_sauts.png",
        "Cycle annuel moyen par catégorie de sauts\nNormalisé [0-1] | Anomalie centrée"
    )

    plot_cycles(
        cycles_plat,
        CATEGORIES_PLAT,
        COLORS_PLAT,
        "cycle_annuel_plat_vs_ok.png",
        "Cycle annuel moyen — Signal plat vs Signal OK\nNormalisé [0-1] | Anomalie centrée"
    )

    print("✅ Terminé")