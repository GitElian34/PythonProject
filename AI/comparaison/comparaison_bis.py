import torch
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.preprocessing import MinMaxScaler
from torch.utils.data import DataLoader
import sqlite3
import random
import os

from data_processing import get_donnees_station
from AI.LSTM.LSTM import LSTMHydro, device, HydroDataset, evaluer

DB_PATH    = "./data/insitu_data.db"
MODELS_DIR = "./data/IA/Models/Flag/"
OUTPUT_DIR = "./data/IA/Visualisation/Flag/"
N_TEST     = 20
BATCH_SIZE = 32
SEED       = 91  # seed différent de l'entraînement pour indépendance
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Correspondance entre chaque modèle et le critère SQL
# qui définit "le même type de station"
MODELES = {
    'sans_sauts': {
        'path':  os.path.join(MODELS_DIR, 'lstm_sans_sauts.pt'),
        'where': "qualite_sauts = 'aucun'",
    },
    'beaucoup_sauts': {
        'path':  os.path.join(MODELS_DIR, 'lstm_beaucoup_sauts.pt'),
        'where': "qualite_sauts = '100-500'",
    },
    'mixte': {
        'path':  os.path.join(MODELS_DIR, 'lstm_mixte.pt'),
        'where': "qualite_sauts IS NOT NULL",
    },
    'signal_plat': {
        'path':  os.path.join(MODELS_DIR, 'lstm_signal_plat.pt'),
        'where': "signal_plat = 1",
    },
    'sans_plat': {
        'path':  os.path.join(MODELS_DIR, 'lstm_sans_plat.pt'),
        'where': "signal_plat = 0 AND qualite_sauts IN ('aucun', '< 10') AND gap_max_jours IS NULL",
    },
    'aleatoire': {
        'path':  os.path.join(MODELS_DIR, 'lstm_aleatoire.pt'),
        'where': "1=1",
    },
}

METRIQUES_KEYS   = ['mae', 'rmse', 'kge', 'ratio_overfit']
METRIQUES_LABELS = ['MAE', 'RMSE', 'KGE', 'Ratio Overfit']
METRIQUES_SENS   = [False, False, True, False]  # True = plus grand = meilleur


# ─────────────────────────────────────────────
# Chargement du modèle depuis le checkpoint
# ─────────────────────────────────────────────
def charger_modele(path):
    checkpoint = torch.load(path, map_location=device, weights_only=False)
    config     = checkpoint['config']
    features   = checkpoint['features']

    # On injecte features et target dans config pour les passer
    # facilement à preparer_donnees sans paramètres supplémentaires
    config['features'] = features
    config['target']   = checkpoint['target']

    model = LSTMHydro(
        input_size=len(features),
        hidden_size=config['hidden_size'],
        num_layers=config['num_layers']
    ).to(device)
    model.load_state_dict(checkpoint['model_state'])

    # On récupère aussi les stations d'entraînement pour les exclure du test
    stations_train = checkpoint.get('stations', [])

    return model, config, stations_train


# ─────────────────────────────────────────────
# Sélection des stations de test
# ─────────────────────────────────────────────
def selectionner_stations_test_aleatoire(stations_train, n=N_TEST, seed=SEED):
    """
    Sélectionne n stations complètement au hasard parmi toutes
    les stations disponibles, en excluant uniquement celles
    qui ont été vues pendant l'entraînement du modèle testé.
    Pas de filtre sur le type (qualite_sauts, signal_plat, etc.)
    """
    random.seed(seed)
    conn = sqlite3.connect(DB_PATH)

    exclusion = ""
    params    = []
    if stations_train:
        exclusion = f"AND code_sta NOT IN ({','.join('?' * len(stations_train))})"
        params    = list(stations_train)

    df = pd.read_sql_query(f"""
        SELECT code_sta FROM stations_insitu
        WHERE (dans_lac IS NULL OR dans_lac NOT IN ('dans_lac', 'proche_lac'))
          {exclusion}
    """, conn, params=params)['code_sta'].tolist()
    conn.close()

    if len(df) < n:
        print(f"  ⚠️  Seulement {len(df)} stations disponibles")

    return random.sample(df, min(n, len(df)))

# ─────────────────────────────────────────────
# Préparation des données d'une station
# ─────────────────────────────────────────────
def preparer_station(station_code, config):
    """
    Charge les données d'une station, normalise avec un scaler
    propre à cette station (indépendant du scaler d'entraînement),
    et construit le DataLoader correspondant.
    """
    features = config['features']
    target   = config['target']

    df = get_donnees_station(station_code)
    if df is None or len(df) < config['fenetre'] + 10:
        return None, None

    manquantes = [f for f in features if f not in df.columns]
    if manquantes:
        print(f"  ⚠️  {station_code} — colonnes manquantes : {manquantes}")
        return None, None

    data        = df[features].copy()
    scaler      = MinMaxScaler()
    data_scaled = scaler.fit_transform(data)

    dataset = HydroDataset(data_scaled, config['fenetre'], features, target)
    loader  = DataLoader(dataset, batch_size=BATCH_SIZE, shuffle=False)

    return loader, len(df)


# ─────────────────────────────────────────────
# Test d'un modèle sur ses 10 stations
# ─────────────────────────────────────────────
def tester_modele(nom, infos_modele):
    print(f"\n{'═'*55}")
    print(f"  Modèle : {nom}")
    print(f"{'═'*55}")

    # Chargement
    model, config, stations_train = charger_modele(infos_modele['path'])

    # Sélection des stations de test du même type
    stations_test = selectionner_stations_test_aleatoire(stations_train)
    print(f"  {len(stations_test)} stations de test : {stations_test}")

    resultats_stations = []

    for station in stations_test:
        loader, n_jours = preparer_station(station, config)
        if loader is None:
            continue

        # On évalue sur toute la série — train_loader=loader car on teste
        # sur la série complète sans séparation train/test ici
        predictions, actuals, _, _, metriques = evaluer(model, loader, loader)

        resultats_stations.append(metriques)
        print(f"  {station} — MAE={metriques['mae']:.4f} | "
              f"KGE={metriques.get('kge', float('nan')):.4f} | "
              f"RMSE={metriques['rmse']:.4f}")

    if not resultats_stations:
        print(f"  ⚠️  Aucune station testée pour {nom}")
        return None

    # Moyenne des métriques sur les 10 stations
    moyennes = {
        k: np.mean([r[k] for r in resultats_stations if k in r and not np.isnan(r[k])])
        for k in METRIQUES_KEYS
    }
    print(f"\n  Moyenne : MAE={moyennes['mae']:.4f} | "
          f"KGE={moyennes.get('kge', float('nan')):.4f} | "
          f"RMSE={moyennes['rmse']:.4f} | "
          f"Overfit={moyennes['ratio_overfit']:.4f}")

    return moyennes


# ─────────────────────────────────────────────
# Visualisation
# ─────────────────────────────────────────────
def plot_resultats(tous_resultats):
    """
    Affiche 4 graphes (un par métrique) avec une barre par modèle.
    Le meilleur modèle pour chaque métrique est mis en vert avec une étoile.
    """
    noms_modeles = list(tous_resultats.keys())
    fig, axes    = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle(
        f"Évaluation des 6 modèles LSTM sur 20 stations aléatoires\n"
        f"(stations hors entraînement, tous types confondus)",
        fontsize=13, fontweight='bold'
    )

    for ax, key, label, meilleur_haut in zip(
        axes.flatten(), METRIQUES_KEYS, METRIQUES_LABELS, METRIQUES_SENS
    ):
        valeurs  = [tous_resultats[nom][key] for nom in noms_modeles]
        couleurs = ['#2196F3'] * len(noms_modeles)

        # Identifier et mettre en vert le meilleur modèle
        vals_ok  = [v for v in valeurs if not np.isnan(v)]
        if not vals_ok:
            continue
        best_idx = int(np.argmax(valeurs)) if meilleur_haut else int(np.argmin(valeurs))
        couleurs[best_idx] = 'seagreen'

        ax.bar(range(len(noms_modeles)), valeurs,
               color=couleurs, alpha=0.85, edgecolor='white', width=0.6)

        # Étoile sur le meilleur
        ax.text(best_idx, valeurs[best_idx] + max(vals_ok) * 0.03,
                '★', ha='center', va='bottom', fontsize=16, color='gold')

        # Valeur sur chaque barre
        for i, v in enumerate(valeurs):
            if not np.isnan(v):
                ax.text(i, v + max(vals_ok) * 0.01, f"{v:.4f}",
                        ha='center', va='bottom', fontsize=8, fontweight='bold')

        ax.set_xticks(range(len(noms_modeles)))
        ax.set_xticklabels(noms_modeles, rotation=20, ha='right', fontsize=9)
        ax.set_title(label, fontsize=11, fontweight='bold')
        ax.set_ylabel(label, fontsize=9)
        ax.grid(axis='y', alpha=0.3)

    plt.tight_layout()
    path = os.path.join(OUTPUT_DIR, "evaluation_6_modeles.png")
    plt.savefig(path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"\n📊 Graphe sauvegardé : {path}")


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────
if __name__ == "__main__":
    tous_resultats = {}

    for nom, infos in MODELES.items():
        if not os.path.exists(infos['path']):
            print(f"⚠️  Modèle {nom} introuvable : {infos['path']}, skip")
            continue

        moyennes = tester_modele(nom, infos)
        if moyennes is not None:
            tous_resultats[nom] = moyennes

    plot_resultats(tous_resultats)