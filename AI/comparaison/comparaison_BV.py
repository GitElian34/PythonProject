import torch
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.preprocessing import MinMaxScaler
from torch.utils.data import DataLoader
import sqlite3
import random
import os

from data_processing import get_donnees_station, get_donnees_station_bv
from AI.LSTM.LSTM import LSTMHydro, device, HydroDataset, evaluer

DB_PATH    = "./data/insitu_data.db"
MODELS_DIR = "./data/IA/Models/BV/"
OUTPUT_DIR = "./data/IA/Visualisation/BV/"
N_TEST     = 10
BATCH_SIZE = 32
SEED       = 91
os.makedirs(OUTPUT_DIR, exist_ok=True)

MODELES = {
    'baseline': {
        'path'   : os.path.join(MODELS_DIR, 'lstm_baseline.pt'),
        'avec_bv': False,
    },
    'avec_bv': {
        'path'   : os.path.join(MODELS_DIR, 'lstm_avec_bv.pt'),
        'avec_bv': True,
    },
}

METRIQUES_KEYS   = ['mae', 'rmse', 'kge', 'ratio_overfit']
METRIQUES_LABELS = ['MAE', 'RMSE', 'KGE', 'Ratio Overfit']
METRIQUES_SENS   = [False, False, True, False]


# ─────────────────────────────────────────────
# Chargement du modèle
# ─────────────────────────────────────────────
def charger_modele(path):
    checkpoint     = torch.load(path, map_location=device, weights_only=False)
    config         = checkpoint['config']
    config['features'] = checkpoint['features']
    config['target']   = checkpoint['target']

    model = LSTMHydro(
        input_size=len(checkpoint['features']),
        hidden_size=config['hidden_size'],
        num_layers=config['num_layers']
    ).to(device)
    model.load_state_dict(checkpoint['model_state'])

    return model, config, checkpoint.get('stations', [])


# ─────────────────────────────────────────────
# Sélection des stations de test
# — uniquement celles qui ont des données BV
# — et qui n'ont pas été vues à l'entraînement
# ─────────────────────────────────────────────
def selectionner_stations_test(stations_train, n=N_TEST, seed=SEED):
    """
    Sélectionne n stations aléatoires parmi celles qui ont
    des données ERA5-BV (présentes dans era5_pluie_bv),
    en excluant les stations d'entraînement.
    """
    random.seed(seed)
    conn = sqlite3.connect(DB_PATH)

    exclusion = ""
    params    = []
    if stations_train:
        exclusion = f"AND s.code_sta NOT IN ({','.join('?' * len(stations_train))})"
        params    = list(stations_train)

    # On force la présence de données BV via la jointure
    df = pd.read_sql_query(f"""
        SELECT DISTINCT s.code_sta
        FROM stations_insitu s
        JOIN era5_pluie_bv p ON s.code_sta = p.code_sta
        WHERE (s.dans_lac IS NULL OR s.dans_lac NOT IN ('dans_lac', 'proche_lac'))
          {exclusion}
    """, conn, params=params)['code_sta'].tolist()
    conn.close()

    if len(df) < n:
        print(f"  ⚠️  Seulement {len(df)} stations disponibles avec données BV")

    return random.sample(df, min(n, len(df)))


# ─────────────────────────────────────────────
# Préparation des données d'une station
# ─────────────────────────────────────────────
def preparer_station(station_code, config, avec_bv=False):
    """
     Charge les données d'une station avec ou sans features BV,
     normalise et construit le DataLoader.
     """

    if avec_bv:
        df = get_donnees_station_bv(station_code)
    else:
        df = get_donnees_station(station_code)

    features = config['features']
    target   = config['target']

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
# Test d'un modèle
# ─────────────────────────────────────────────
def tester_modele(nom, infos_modele, stations_test):
    print(f"\n{'═'*55}")
    print(f"  Modèle : {nom}")
    print(f"{'═'*55}")

    model, config, _ = charger_modele(infos_modele['path'])
    avec_bv          = infos_modele['avec_bv']

    print(f"  {len(stations_test)} stations de test : {stations_test}")

    resultats_stations = []
    for station in stations_test:
        loader, n_jours = preparer_station(station, config, avec_bv=avec_bv)
        if loader is None:
            continue

        predictions, actuals, _, _, metriques = evaluer(model, loader, loader)
        resultats_stations.append(metriques)
        print(f"  {station} — MAE={metriques['mae']:.4f} | "
              f"KGE={metriques.get('kge', float('nan')):.4f} | "
              f"RMSE={metriques['rmse']:.4f}")

    if not resultats_stations:
        print(f"  ⚠️  Aucune station testée pour {nom}")
        return None

    moyennes = {
        k: np.mean([r[k] for r in resultats_stations
                    if k in r and not np.isnan(r[k])])
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
    Compare les 2 modèles (baseline vs avec_bv) sur 4 métriques.
    """
    noms_modeles = list(tous_resultats.keys())
    fig, axes    = plt.subplots(2, 2, figsize=(12, 8))
    fig.suptitle(
        f"Comparaison LSTM baseline vs avec features ERA5-BV\n"
        f"({N_TEST} stations de test avec données BV disponibles)",
        fontsize=13, fontweight='bold'
    )

    for ax, key, label, meilleur_haut in zip(
        axes.flatten(), METRIQUES_KEYS, METRIQUES_LABELS, METRIQUES_SENS
    ):
        valeurs  = [tous_resultats[nom][key] for nom in noms_modeles]
        couleurs = ['#2196F3', '#FF7043']  # bleu=baseline, orange=avec_bv

        vals_ok  = [v for v in valeurs if not np.isnan(v)]
        if not vals_ok:
            continue
        best_idx = int(np.argmax(valeurs)) if meilleur_haut else int(np.argmin(valeurs))
        couleurs[best_idx] = 'seagreen'

        ax.bar(range(len(noms_modeles)), valeurs,
               color=couleurs, alpha=0.85, edgecolor='white', width=0.4)

        ax.text(best_idx, valeurs[best_idx] + max(vals_ok) * 0.03,
                '★', ha='center', va='bottom', fontsize=16, color='gold')

        for i, v in enumerate(valeurs):
            if not np.isnan(v):
                ax.text(i, v + max(vals_ok) * 0.01, f"{v:.4f}",
                        ha='center', va='bottom', fontsize=9, fontweight='bold')

        ax.set_xticks(range(len(noms_modeles)))
        ax.set_xticklabels(noms_modeles, rotation=0, fontsize=10)
        ax.set_title(label, fontsize=11, fontweight='bold')
        ax.set_ylabel(label, fontsize=9)
        ax.grid(axis='y', alpha=0.3)

    plt.tight_layout()
    path = os.path.join(OUTPUT_DIR, "comparaison_baseline_vs_bv.png")
    plt.savefig(path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"\n📊 Graphe sauvegardé : {path}")


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────
if __name__ == "__main__":
    tous_resultats = {}

    # ── Charger les stations d'entraînement des deux modèles ──
    stations_train_all = set()
    for nom, infos in MODELES.items():
        if not os.path.exists(infos['path']):
            print(f"⚠️  Modèle {nom} introuvable : {infos['path']}, skip")
            continue
        _, _, stations_train = charger_modele(infos['path'])
        stations_train_all.update(stations_train)

    # ── Sélectionner les mêmes stations de test pour les deux modèles ──
    stations_test = selectionner_stations_test(list(stations_train_all))
    print(f"\nStations de test communes ({len(stations_test)}) : {stations_test}")

    # ── Tester chaque modèle sur les mêmes stations ──
    for nom, infos in MODELES.items():
        if not os.path.exists(infos['path']):
            continue

        moyennes = tester_modele(nom, infos, stations_test)
        if moyennes is not None:
            tous_resultats[nom] = moyennes

    # ── Visualisation ──
    if tous_resultats:
        plot_resultats(tous_resultats)

        print(f"\n{'═'*55}")
        print(f"  RÉSUMÉ COMPARAISON")
        print(f"{'═'*55}")
        print(f"  {'Modèle':<12} {'MAE':<8} {'RMSE':<8} {'KGE':<8} {'Overfit'}")
        print(f"  {'-'*50}")
        for nom, m in tous_resultats.items():
            print(f"  {nom:<12} {m['mae']:<8.4f} {m['rmse']:<8.4f} "
                  f"{m['kge']:<8.4f} {m['ratio_overfit']:.4f}")