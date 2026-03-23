import torch
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.preprocessing import MinMaxScaler
from torch.utils.data import DataLoader

from data_processing.insitu.db_insitu import get_donnees_station
from AI.LSTM import LSTMHydro, device, HydroDataset, evaluer

FEATURES = ['h_01h_wsh', 'h_09h_wsh', 'h_17h_wsh',
            'precip_jour', 'temp_min_jour', 'temp_max_jour',
            'temp_moy_jour', 'temp_moy_10j', 'precip_moy_10j']
TARGET     = 'h_09h_wsh'
BATCH_SIZE = 32
STATIONS_TEST = [
    'A285011001',  # A
    'B422431001',  # B
    #'D015658001',  # D attention +
    'F221000201',  # F
    'J473401001',  # J
    'K612311010',  # K
    'L562301001',  # L
    'O341000401',  # O
    #'Q218000101',  # Q atention +++
    #'W103000101',  # W  attention
]

MODELES = {
    'Normalisation par station':    './data/IA/Models/lstm_normalisation_par_station.pt',
}

def charger_modele(path):
    checkpoint = torch.load(path, map_location=device, weights_only=False)
    config = checkpoint['config']
    model = LSTMHydro(
        input_size=len(FEATURES),
        hidden_size=config['hidden_size'],
        num_layers=config['num_layers']
    ).to(device)
    model.load_state_dict(checkpoint['model_state'])
    return model, config  # scaler plus nécessaire


def preparer_test(df, fenetre):
    data = df[FEATURES].copy()
    # Scaler dédié à la station de test
    scaler_test = MinMaxScaler()
    data_scaled = scaler_test.fit_transform(data)
    dataset = HydroDataset(data_scaled, fenetre, FEATURES, TARGET)
    loader  = DataLoader(dataset, batch_size=BATCH_SIZE, shuffle=False)
    return loader

def tester_station(station):
    print(f"\n{'='*50}")
    print(f"🔄 Station test : {station}")
    df = get_donnees_station(station)
    if df is None or len(df) == 0:
        print(f"  ❌ Pas de données")
        return None, None

    resultats = {}
    for nom, path in MODELES.items():
        print(f"  📦 {nom}")
        try:
            model, config = charger_modele(path)  # plus de scaler
            loader = preparer_test(df, config['fenetre'])  # plus de scaler
            predictions, actuals, outliers, erreurs, metriques = evaluer(model, loader, loader)
            resultats[nom] = {
                'predictions': predictions,
                'actuals':     actuals,
                'outliers':    outliers,
                'metriques':   metriques,
            }
            print(f"    MAE={metriques['mae']:.4f} | NSE={metriques['nse']:.4f}")
        except Exception as e:
            print(f"    ⚠️  Erreur: {e}")

    return resultats, df

if __name__ == "__main__":
    METRIQUES_LABELS = ['MAE', 'RMSE', 'NSE', 'Ratio Overfit']
    METRIQUES_KEYS = ['mae', 'rmse', 'nse', 'ratio_overfit']
    METRIQUES_SENS = [False, False, True, False]
    tous_resultats = {}
    tous_df = {}
    for station in STATIONS_TEST:
        resultats, df = tester_station(station)
        if resultats:
            tous_resultats[station] = resultats
            tous_df[station] = df

    # ─── Moyenne des métriques sur les 2 stations par modèle ───
    noms_modeles = list(MODELES.keys())

    moyennes = {nom: {k: [] for k in METRIQUES_KEYS} for nom in noms_modeles}
    for station in STATIONS_TEST:
        if station not in tous_resultats:
            continue
        for nom in noms_modeles:
            if nom not in tous_resultats[station]:
                continue
            for k in METRIQUES_KEYS:
                moyennes[nom][k].append(tous_resultats[station][nom]['metriques'][k])

    # Calcul des moyennes finales
    moyennes_finales = {
        nom: {k: np.mean(v) for k, v in moyennes[nom].items()}
        for nom in noms_modeles
    }

    # ─── 4 graphes (1 par métrique) ───
    fig, axes = plt.subplots(2, 2, figsize=(12, 8))
    fig.suptitle("Comparaison des modèles — Moyenne sur 5 stations", fontsize=13, fontweight='bold')

    for ax, k, label, sens in zip(
        axes.flatten(),
        METRIQUES_KEYS,
        METRIQUES_LABELS,
        METRIQUES_SENS
    ):
        valeurs = [moyennes_finales[nom][k] for nom in noms_modeles]
        couleurs = ['steelblue'] * len(noms_modeles)

        # Meilleur modèle
        best_idx = int(np.argmax(valeurs)) if sens else int(np.argmin(valeurs))
        couleurs[best_idx] = 'seagreen'

        bars = ax.bar(range(len(noms_modeles)), valeurs, color=couleurs, alpha=0.8, edgecolor='white')

        # Étoile sur le meilleur
        ax.text(best_idx, valeurs[best_idx] + max(valeurs) * 0.02,
                '★', ha='center', va='bottom', fontsize=16, color='gold')

        ax.set_xticks(range(len(noms_modeles)))
        ax.set_xticklabels(noms_modeles, rotation=20, ha='right', fontsize=8)
        ax.set_title(label, fontsize=11)
        ax.set_ylabel(label)
        ax.grid(True, alpha=0.3, axis='y')

        # Valeur sur chaque barre
        for i, v in enumerate(valeurs):
            ax.text(i, v + max(valeurs) * 0.01, f"{v:.4f}",
                    ha='center', va='bottom', fontsize=8)

    plt.tight_layout()
    path = './data/IA/Visualisation/best_modele_moyenne8_QWout.png'
    plt.savefig(path, dpi=150, bbox_inches='tight')
    print(f"\n📊 Sauvegardé : {path}")
    plt.close()