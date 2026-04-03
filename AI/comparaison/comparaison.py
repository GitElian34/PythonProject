import torch
import numpy as np
import matplotlib.pyplot as plt
from sklearn.preprocessing import MinMaxScaler
from torch.utils.data import DataLoader

from AI.entrainement.entrainement_10stations import selectionner_stations_test
from data_processing.insitu.db_insitu import get_donnees_station
from AI.LSTM import LSTMHydro, device, HydroDataset, evaluer
BATCH_SIZE = 32
DB_PATH    = "./data/insitu_data.db"
# STATIONS_TEST = [
#     'A285011001',  # A
#     'B422431001',  # B
#     #'D015658001',  # D attention +
#     'F221000201',  # F
#     'J473401001',  # J
#     'K612311010',  # K
#     'L562301001',  # L
#     'O341000401',  # O
#     #'Q218000101',  # Q atention +++
#     'W103000101',  # W  attention
# ]
MODELES = {
    'h_01h': './data/IA/Models/lstm_h_01h_10sta.pt',
    'h_09h': './data/IA/Models/lstm_h_09h_10sta.pt',
    'h_17h': './data/IA/Models/lstm_h_17h_10sta.pt',
    'h_med': './data/IA/Models/lstm_h_med_10sta.pt',
}

METRIQUES_KEYS   = ['mae', 'rmse', 'nse', 'kge', 'ratio_overfit']
METRIQUES_LABELS = ['MAE', 'RMSE', 'NSE', 'KGE', 'Ratio Overfit']
METRIQUES_SENS   = [False, False, True, True, False]
 # True = plus grand = meilleur


# ─────────────────────────────────────────────
# Chargement modèle
# ─────────────────────────────────────────────
def charger_modele(path):
    checkpoint = torch.load(path, map_location=device, weights_only=False)
    config     = checkpoint['config']
    features   = checkpoint['features']

    config['features'] = features
    config['target']   = checkpoint['target']

    model = LSTMHydro(
        input_size=len(features),
        hidden_size=config['hidden_size'],
        num_layers=config['num_layers']
    ).to(device)
    model.load_state_dict(checkpoint['model_state'])
    return model, config


# ─────────────────────────────────────────────
# Préparation données test
# ─────────────────────────────────────────────
def preparer_test(df, config):
    features = config['features']
    target   = config['target']

    manquantes = [f for f in features if f not in df.columns]
    if manquantes:
        raise ValueError(f"Colonnes manquantes : {manquantes}")

    data        = df[features].copy()
    scaler      = MinMaxScaler()
    data_scaled = scaler.fit_transform(data)
    dataset     = HydroDataset(data_scaled, config['fenetre'], features, target)
    return DataLoader(dataset, batch_size=BATCH_SIZE, shuffle=False)


# ─────────────────────────────────────────────
# Test d'une station sur tous les modèles
# ─────────────────────────────────────────────
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
            model, config = charger_modele(path)
            loader        = preparer_test(df, config)
            predictions, actuals, outliers, erreurs, metriques = evaluer(
                model, loader, loader
            )
            resultats[nom] = {
                'predictions': predictions,
                'actuals':     actuals,
                'metriques':   metriques,
            }
            print(f"    MAE={metriques['mae']:.4f} | NSE={metriques['nse']:.4f}")
        except Exception as e:
            print(f"    ⚠️  Erreur : {e}")

    return resultats, df


# ─────────────────────────────────────────────
# Visualisation
# ─────────────────────────────────────────────
def plot_comparaison(moyennes_finales):
    noms_modeles = list(MODELES.keys())

    fig, axes = plt.subplots(2, 3, figsize=(15, 8))
    fig.suptitle(f"Comparaison h01 / h09 / h17 / médiane\n"
                 f"Moyenne sur {len(STATIONS_TEST)} stations test",
                 fontsize=13, fontweight='bold')

    for ax, k, label, sens in zip(
        axes.flatten(), METRIQUES_KEYS, METRIQUES_LABELS, METRIQUES_SENS
    ):
        valeurs  = [moyennes_finales[nom][k] for nom in noms_modeles]
        couleurs = ['steelblue'] * len(noms_modeles)

        best_idx = int(np.argmax(valeurs)) if sens else int(np.argmin(valeurs))
        couleurs[best_idx] = 'seagreen'

        ax.bar(range(len(noms_modeles)), valeurs,
               color=couleurs, alpha=0.8, edgecolor='white')
        ax.text(best_idx, valeurs[best_idx] + max(valeurs) * 0.02,
                '★', ha='center', va='bottom', fontsize=16, color='gold')

        ax.set_xticks(range(len(noms_modeles)))
        ax.set_xticklabels(noms_modeles, fontsize=10)
        ax.set_title(label, fontsize=11)
        ax.set_ylabel(label)
        ax.grid(True, alpha=0.3, axis='y')

        for i, v in enumerate(valeurs):
            ax.text(i, v + max(valeurs) * 0.01, f"{v:.4f}",
                    ha='center', va='bottom', fontsize=9)

    plt.tight_layout()
    path = './data/IA/Visualisation/medianeVSh09/10stations/comparaisonNew10stations_h01_h09_h17_hmed.png'
    plt.savefig(path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"\n📊 Sauvegardé : {path}")


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────
if __name__ == "__main__":
    ckpt_ref = torch.load(list(MODELES.values())[0], map_location='cpu', weights_only=False)
    stations_train = ckpt_ref['stations']

    STATIONS_TEST = selectionner_stations_test(DB_PATH, stations_train, seed=42)
    tous_resultats = {}

    for station in STATIONS_TEST:
        resultats, df = tester_station(station)
        if resultats:
            tous_resultats[station] = resultats

    # ─── Moyennes par modèle sur toutes les stations ───
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

    moyennes_finales = {
        nom: {k: np.mean(v) for k, v in moyennes[nom].items()}
        for nom in noms_modeles
    }

    # ─── Résumé terminal ───
    print(f"\n{'═'*55}")
    print(f"  RÉSUMÉ — Moyenne sur {len(STATIONS_TEST)} stations test")
    print(f"{'═'*55}")
    print(f"  {'Modèle':<10} {'MAE':<8} {'RMSE':<8} {'NSE':<8} {'KGE':<8} {'Overfit'}")
    print(f"  {'-' * 55}")
    for nom in sorted(moyennes_finales, key=lambda n: moyennes_finales[n]['kge'], reverse=True):
        m = moyennes_finales[nom]
        print(f"  {nom:<10} {m['mae']:<8.4f} {m['rmse']:<8.4f} "
              f"{m['nse']:<8.4f} {m['kge']:<8.4f} {m['ratio_overfit']:.4f}")

    plot_comparaison(moyennes_finales)