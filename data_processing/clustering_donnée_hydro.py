import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import sqlite3
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from tslearn.clustering import TimeSeriesKMeans, KShape
from tslearn.preprocessing import TimeSeriesScalerMeanVariance
from tslearn.utils import to_time_series_dataset
import warnings
warnings.filterwarnings('ignore')

from data_processing.insitu.db_insitu import get_donnees_station, get_stations_insitu

DB_PATH = "./data/insitu_data.db"


# ─────────────────────────────────────────────
# CONSTRUCTION DU CYCLE ANNUEL MOYEN
# ─────────────────────────────────────────────
def cycle_annuel(df, code_sta):
    """
    Calcule la hauteur moyenne par jour de l'année (1-365)
    Retourne un vecteur de 365 valeurs
    """
    df = df.copy()
    df['day_of_year'] = df['date'].dt.dayofyear

    # Moyenne par jour de l'année
    cycle = df.groupby('day_of_year')['h_09h_wsh'].mean()

    # S'assurer qu'on a bien 365 jours
    cycle = cycle.reindex(range(1, 366))

    # Interpolation des jours manquants
    cycle = cycle.interpolate(method='linear').fillna(method='bfill').fillna(method='ffill')

    if cycle.isna().any():
        return None

    return cycle.values


# ─────────────────────────────────────────────
# CHARGEMENT DE TOUTES LES STATIONS
# ─────────────────────────────────────────────
def charger_cycles(stations):
    print(f"📍 Calcul des cycles annuels pour {len(stations)} stations...")
    cycles = {}

    for code_sta in stations:
        df = get_donnees_station(code_sta)
        if df is None or len(df) < 365:
            print(f"  ⚠️  {code_sta} : pas assez de données, skip")
            continue
        try:
            c = cycle_annuel(df, code_sta)
            if c is not None:
                cycles[code_sta] = c
                print(f"  ✅ {code_sta}")
        except Exception as e:
            print(f"  ⚠️  {code_sta} : {e}")

    print(f"\n📊 {len(cycles)} cycles annuels calculés")
    return cycles


# ─────────────────────────────────────────────
# CLUSTERING FDA
# ─────────────────────────────────────────────
def clustering_fda(cycles, n_clusters=5):
    codes_sta = list(cycles.keys())
    X = np.array([cycles[c] for c in codes_sta])

    # Normalisation mean-variance pour comparer les formes
    scaler = TimeSeriesScalerMeanVariance()
    X_scaled = scaler.fit_transform(X.reshape(len(X), 365, 1))

    # ── TimeSeriesKMeans avec DTW ──
    print(f"\n🔄 TimeSeriesKMeans (DTW) — {n_clusters} clusters...")
    model_dtw = TimeSeriesKMeans(
        n_clusters=n_clusters,
        metric="dtw",
        random_state=42,
        n_jobs=-1
    )
    labels_dtw = model_dtw.fit_predict(X_scaled)

    # ── KShape ──
    print(f"🔄 KShape — {n_clusters} clusters...")
    model_kshape = KShape(
        n_clusters=n_clusters,
        random_state=42
    )
    labels_kshape = model_kshape.fit_predict(X_scaled)

    print(f"\n✅ Clustering terminé")
    for i in range(n_clusters):
        print(f"  DTW   Cluster {i} : {(labels_dtw == i).sum()} stations")
    print()
    for i in range(n_clusters):
        print(f"  KShape Cluster {i} : {(labels_kshape == i).sum()} stations")

    return labels_dtw, labels_kshape, X_scaled, model_dtw, model_kshape, codes_sta


# ─────────────────────────────────────────────
# VISUALISATION DES COURBES PAR CLUSTER
# ─────────────────────────────────────────────
def visualiser_clusters(cycles, labels, codes_sta, model, titre, n_clusters):
    jours = np.arange(1, 366)
    mois_labels = ['Jan', 'Fév', 'Mar', 'Avr', 'Mai', 'Jun',
                   'Jul', 'Aoû', 'Sep', 'Oct', 'Nov', 'Déc']
    mois_ticks  = [1, 32, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335]

    fig, axes = plt.subplots(1, n_clusters, figsize=(4 * n_clusters, 5))
    fig.suptitle(f"FDA — Cycles annuels par cluster ({titre})",
                 fontsize=13, fontweight='bold')

    couleurs = plt.cm.tab10(np.linspace(0, 1, n_clusters))

    for cluster_id, ax in enumerate(axes):
        mask = labels == cluster_id
        stations_cluster = [codes_sta[i] for i, m in enumerate(mask) if m]

        # Courbes individuelles
        for code in stations_cluster:
            ax.plot(jours, cycles[code], color=couleurs[cluster_id],
                    alpha=0.3, linewidth=0.8)

        # Courbe moyenne du cluster
        if len(stations_cluster) > 0:
            moyenne = np.mean([cycles[c] for c in stations_cluster], axis=0)
            ax.plot(jours, moyenne, color='black', linewidth=2,
                    label='Moyenne', linestyle='--')

        ax.set_xticks(mois_ticks)
        ax.set_xticklabels(mois_labels, fontsize=8, rotation=45)
        ax.set_title(f"Cluster {cluster_id}\n({len(stations_cluster)} stations)", fontsize=10)
        ax.set_ylabel("h_09h_wsh (m)")
        ax.legend(fontsize=8)
        ax.grid(True, alpha=0.3)

    plt.tight_layout()
    path = f'./data/IA/Visualisation/fda_clusters_{titre.replace(" ", "_")}.png'
    plt.savefig(path, dpi=150, bbox_inches='tight')
    print(f"📊 Sauvegardé : {path}")
    plt.close()


# ─────────────────────────────────────────────
# VISUALISATION PCA 2D
# ─────────────────────────────────────────────
def visualiser_pca(cycles, labels_dtw, labels_kshape, codes_sta):
    X = np.array([cycles[c] for c in codes_sta])
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    pca = PCA(n_components=2)
    X_2d = pca.fit_transform(X_scaled)
    var_exp = pca.explained_variance_ratio_.sum()

    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    fig.suptitle(f"PCA des cycles annuels (variance expliquée : {var_exp:.1%})",
                 fontsize=13, fontweight='bold')

    for ax, labels, titre in zip(axes,
                                  [labels_dtw, labels_kshape],
                                  ['DTW KMeans', 'KShape']):
        scatter = ax.scatter(X_2d[:, 0], X_2d[:, 1],
                             c=labels, cmap='tab10', s=60, alpha=0.8)
        for i, code in enumerate(codes_sta):
            ax.annotate(code, (X_2d[i, 0], X_2d[i, 1]),
                        fontsize=6, alpha=0.6, ha='center', va='bottom')
        plt.colorbar(scatter, ax=ax, label='Cluster')
        ax.set_title(titre)
        ax.set_xlabel("PC1")
        ax.set_ylabel("PC2")
        ax.grid(True, alpha=0.3)

    plt.tight_layout()
    path = './data/IA/Visualisation/fda_pca.png'
    plt.savefig(path, dpi=150, bbox_inches='tight')
    print(f"📊 Sauvegardé : {path}")
    plt.close()


# ─────────────────────────────────────────────
# CHOIX DU NOMBRE DE CLUSTERS (ELBOW)
# ─────────────────────────────────────────────
def elbow_method(X_scaled, k_max=10):
    print("\n🔄 Calcul de la méthode Elbow...")
    inertias = []
    ks = range(2, k_max + 1)

    for k in ks:
        model = TimeSeriesKMeans(n_clusters=k, metric="dtw",
                                  random_state=42, n_jobs=-1)
        model.fit(X_scaled)
        inertias.append(model.inertia_)
        print(f"  k={k} | inertia={model.inertia_:.2f}")

    fig, ax = plt.subplots(figsize=(8, 4))
    ax.plot(list(ks), inertias, marker='o', color='steelblue', linewidth=2)
    ax.set_xlabel("Nombre de clusters K")
    ax.set_ylabel("Inertie (DTW)")
    ax.set_title("Méthode Elbow — Choix du nombre de clusters")
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    path = './data/IA/Visualisation/fda_elbow.png'
    plt.savefig(path, dpi=150, bbox_inches='tight')
    print(f"📊 Sauvegardé : {path}")
    plt.close()


# ─────────────────────────────────────────────
# RÉSUMÉ PAR CLUSTER
# ─────────────────────────────────────────────
def resume_clusters(cycles, labels, codes_sta, titre):
    print(f"\n{'='*60}")
    print(f"📋 RÉSUMÉ CLUSTERS — {titre}")
    print(f"{'='*60}")

    n_clusters = len(set(labels))
    for cluster_id in range(n_clusters):
        stations = [codes_sta[i] for i, l in enumerate(labels) if l == cluster_id]
        courbes  = np.array([cycles[c] for c in stations])
        moyenne  = courbes.mean(axis=0)

        # Mois du pic et de l'étiage
        mois_labels = ['Jan', 'Fév', 'Mar', 'Avr', 'Mai', 'Jun',
                       'Jul', 'Aoû', 'Sep', 'Oct', 'Nov', 'Déc']
        mois_pic    = mois_labels[int(np.argmax(moyenne) / 30.4)]
        mois_etiage = mois_labels[int(np.argmin(moyenne) / 30.4)]

        print(f"\n  Cluster {cluster_id} ({len(stations)} stations) :")
        print(f"    Pic      : {mois_pic}")
        print(f"    Étiage   : {mois_etiage}")
        print(f"    Amplitude: {moyenne.max() - moyenne.min():.3f} m")
        print(f"    Stations : {stations}")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
if __name__ == "__main__":
    conn = sqlite3.connect(DB_PATH)
    stations = [row[0] for row in get_stations_insitu(conn)]
    conn.close()

    # Chargement des cycles annuels
    cycles = charger_cycles(stations)

    if len(cycles) < 5:
        print("❌ Pas assez de stations valides")
        exit()

    codes_sta = list(cycles.keys())
    X = np.array([cycles[c] for c in codes_sta])

    # Normalisation
    scaler = TimeSeriesScalerMeanVariance()
    X_scaled = scaler.fit_transform(X.reshape(len(X), 365, 1))

    # Méthode Elbow pour choisir K
    elbow_method(X_scaled, k_max=8)

    # Clustering avec K=5 (à ajuster après elbow)
    N_CLUSTERS = 4
    labels_dtw, labels_kshape, X_scaled, model_dtw, model_kshape, codes_sta = \
        clustering_fda(cycles, n_clusters=N_CLUSTERS)

    # Visualisations
    visualiser_clusters(cycles, labels_dtw,    codes_sta, model_dtw,    'DTW',    N_CLUSTERS)
    visualiser_clusters(cycles, labels_kshape, codes_sta, model_kshape, 'KShape', N_CLUSTERS)
    visualiser_pca(cycles, labels_dtw, labels_kshape, codes_sta)

    # Résumés
    resume_clusters(cycles, labels_dtw,    codes_sta, 'DTW KMeans')
    resume_clusters(cycles, labels_kshape, codes_sta, 'KShape')

    print(f"\n✅ Terminé — {len(cycles)} stations analysées")