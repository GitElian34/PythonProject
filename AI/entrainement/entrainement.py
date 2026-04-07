import torch
import torch.nn as nn
import numpy as np
import matplotlib.pyplot as plt
from sklearn.preprocessing import MinMaxScaler
from torch.utils.data import DataLoader

from AI.LSTM.LSTM import LSTMHydro, device, HydroDataset, entrainer, evaluer
from data_processing import get_donnees_station

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────
STATION    = 'O323000401'
BATCH_SIZE = 32
EPOCHS     = 50

CONFIG = {
    'fenetre':     30,
    'hidden_size': 64,
    'num_layers':  2,
    'lr':          0.001,
}

EXOGENES_BASE = ['precip_jour', 'temp_min_jour', 'temp_max_jour',
                 'temp_moy_jour', 'temp_moy_10j', 'precip_moy_10j']

COMBINATIONS_TARGET = {
    'h_01h_wsh seul': (['h_01h_wsh'] + EXOGENES_BASE, 'h_01h_wsh'),
    'h_09h_wsh seul': (['h_09h_wsh'] + EXOGENES_BASE, 'h_09h_wsh'),
    'h_17h_wsh seul': (['h_17h_wsh'] + EXOGENES_BASE, 'h_17h_wsh'),
    'h_med_wsh seul': (['h_med_wsh'] + EXOGENES_BASE, 'h_med_wsh'),
}


# ─────────────────────────────────────────────
# ENTRAÎNEMENT — évaluation sur le train
# ─────────────────────────────────────────────
def entrainer_combinaison(df, features, target, config):
    manquantes = [f for f in features if f not in df.columns]
    if manquantes:
        raise ValueError(f"Colonnes manquantes : {manquantes}")

    data = df[features].copy()
    scaler = MinMaxScaler()
    data_scaled = scaler.fit_transform(data)
    split = int(len(data_scaled) * 0.8)

    train_dataset = HydroDataset(data_scaled[:split], config['fenetre'], features, target)
    train_loader  = DataLoader(train_dataset, batch_size=BATCH_SIZE, shuffle=False)

    # Loader séparé pour l'évaluation sur le train (identique mais isolé)
    train_eval_loader = DataLoader(
        HydroDataset(data_scaled[:split], config['fenetre'], features, target),
        batch_size=BATCH_SIZE, shuffle=False
    )

    model = LSTMHydro(
        input_size=len(features),
        hidden_size=config['hidden_size'],
        num_layers=config['num_layers']
    ).to(device)

    optimizer = torch.optim.Adam(model.parameters(), lr=config['lr'])
    criterion = nn.MSELoss()

    for _ in range(EPOCHS):
        entrainer(model, train_loader, optimizer, criterion)

    # Évaluation sur les données d'entraînement
    predictions, actuals, outliers, erreurs, metriques = evaluer(
        model, train_eval_loader, train_loader
    )
    return metriques, predictions, actuals


# ─────────────────────────────────────────────
# VISUALISATION
# ─────────────────────────────────────────────
def plot_heatmap(resultats, station):
    noms      = list(resultats.keys())
    mae_vals  = [resultats[n]['mae']           for n in noms]
    nse_vals  = [resultats[n]['nse']           for n in noms]
    rmse_vals = [resultats[n]['rmse']          for n in noms]
    over_vals = [resultats[n]['ratio_overfit'] for n in noms]

    metriques_labels = ['MAE', 'RMSE', 'NSE', 'Ratio Overfit']
    metriques_sens   = [False, False, True, False]
    data_matrix      = np.array([mae_vals, rmse_vals, nse_vals, over_vals]).T

    data_norm = np.zeros_like(data_matrix)
    for j, sens in enumerate(metriques_sens):
        col = data_matrix[:, j]
        col_min, col_max = col.min(), col.max()
        if col_max != col_min:
            normalized = (col - col_min) / (col_max - col_min)
            data_norm[:, j] = normalized if not sens else 1 - normalized
        else:
            data_norm[:, j] = 0.5

    fig, ax = plt.subplots(figsize=(10, 5))
    fig.suptitle(f"Comparaison h01 / h09 / h17 / médiane — Station {station}",
                 fontsize=13, fontweight='bold')

    ax.imshow(data_norm, aspect='auto', cmap='RdYlGn_r', vmin=0, vmax=1)
    ax.set_xticks(range(len(metriques_labels)))
    ax.set_xticklabels(metriques_labels, fontsize=11)
    ax.set_yticks(range(len(noms)))
    ax.set_yticklabels(noms, fontsize=10)

    for i in range(len(noms)):
        for j in range(len(metriques_labels)):
            ax.text(j, i, f"{data_matrix[i, j]:.4f}",
                    ha='center', va='center', fontsize=9,
                    fontweight='bold', color='black')

    plt.tight_layout()
    path = f'./data/IA/Visualisation/medianeVSh09/target_impact_{station}.png'
    plt.savefig(path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  📊 Heatmap sauvegardée : {path}")


def plot_predictions(resultats_pred, station):
    n    = len(resultats_pred)
    fig, axes = plt.subplots(n, 1, figsize=(14, 4 * n), sharex=False)
    if n == 1:
        axes = [axes]

    fig.suptitle(f"Prédictions vs Réel (train) — Station {station}",
                 fontsize=13, fontweight='bold')

    for ax, (nom, (preds, actuals)) in zip(axes, resultats_pred.items()):
        ax.plot(actuals, color='#2196F3', linewidth=0.8, alpha=0.9, label='Réel')
        ax.plot(preds,   color='#FF5722', linewidth=0.8, alpha=0.9,
                linestyle='--', label='Prédit')
        ax.set_title(nom, fontsize=10, fontweight='bold')
        ax.set_ylabel('Hauteur (normalisée)', fontsize=8)
        ax.legend(fontsize=8)
        ax.grid(alpha=0.3)

    plt.tight_layout()
    path = f'./data/IA/Visualisation/medianeVSh09/predictions_{station}.png'
    plt.savefig(path, dpi=130, bbox_inches='tight')
    plt.close()
    print(f"  📈 Courbes sauvegardées : {path}")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
if __name__ == "__main__":
    print(f"🔄 Chargement des données pour {STATION}...")
    df = get_donnees_station(STATION)
    print(f"  {len(df)} jours disponibles | colonnes : {list(df.columns)}")

    resultats      = {}
    resultats_pred = {}
    total          = len(COMBINATIONS_TARGET)

    for i, (nom, (features, target)) in enumerate(COMBINATIONS_TARGET.items()):
        print(f"\n[{i+1}/{total}] {nom}  (target={target}, {len(features)} features)")
        try:
            metriques, preds, actuals = entrainer_combinaison(df, features, target, CONFIG)
            resultats[nom]      = metriques
            resultats_pred[nom] = (preds, actuals)
            print(f"  → MAE={metriques['mae']:.4f} | NSE={metriques['nse']:.4f} "
                  f"| RMSE={metriques['rmse']:.4f} | Overfit={metriques['ratio_overfit']:.4f}")
        except Exception as e:
            print(f"  ⚠️  Erreur : {e}")

    plot_heatmap(resultats, STATION)
    plot_predictions(resultats_pred, STATION)

    print(f"\n📊 Résumé (trié par NSE décroissant) :")
    print(f"  {'Combinaison':<20} {'MAE':<8} {'NSE':<8} {'RMSE':<8} {'Overfit'}")
    print(f"  {'-'*55}")
    for nom in sorted(resultats, key=lambda n: resultats[n]['nse'], reverse=True):
        m = resultats[nom]
        print(f"  {nom:<20} {m['mae']:<8.4f} {m['nse']:<8.4f} "
              f"{m['rmse']:<8.4f} {m['ratio_overfit']:.4f}")