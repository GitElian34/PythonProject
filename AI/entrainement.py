import torch
import torch.nn as nn
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.preprocessing import MinMaxScaler
from torch.utils.data import DataLoader

from AI.LSTM import LSTMHydro, device, HydroDataset, entrainer, evaluer
from data_processing.insitu.db_insitu import get_donnees_station

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────
STATION  = 'A343021001'
TARGET   = 'h_09h_wsh'
BATCH_SIZE = 32
EPOCHS   = 50

CONFIG = {
    'fenetre':     30,
    'hidden_size': 64,
    'num_layers':  2,
    'lr':          0.001,
}

COMBINATIONS_EXOGENES = {
    'Précip + Temp min/max': ['h_01h_wsh', 'h_09h_wsh', 'h_17h_wsh',
                              'precip_jour', 'temp_min_jour', 'temp_max_jour'],

    'Précip + Temp tout': ['h_01h_wsh', 'h_09h_wsh', 'h_17h_wsh',
                           'precip_jour', 'temp_min_jour', 'temp_max_jour',
                           'temp_moy_jour', 'temp_moy_10j'],

    'Précip jour + Temp moy10j': ['h_01h_wsh', 'h_09h_wsh', 'h_17h_wsh',
                                  'precip_jour', 'temp_moy_10j'],

    'Temp jour + Temp 10j': ['h_01h_wsh', 'h_09h_wsh', 'h_17h_wsh',
                             'temp_moy_jour', 'temp_moy_10j'],

    'Précip + Temp jour + 10j': ['h_01h_wsh', 'h_09h_wsh', 'h_17h_wsh',
                                 'precip_jour', 'temp_moy_jour', 'temp_moy_10j'],
}

# ─────────────────────────────────────────────
# ENTRAÎNEMENT
# ─────────────────────────────────────────────
def entrainer_combinaison(df, features, config):
    data = df[features].copy()
    scaler = MinMaxScaler()
    data_scaled = scaler.fit_transform(data)
    split = int(len(data_scaled) * 0.8)

    train_dataset = HydroDataset(data_scaled[:split], config['fenetre'], features, TARGET)
    test_dataset  = HydroDataset(data_scaled[split:],  config['fenetre'], features, TARGET)
    train_loader  = DataLoader(train_dataset, batch_size=BATCH_SIZE, shuffle=False)
    test_loader   = DataLoader(test_dataset,  batch_size=BATCH_SIZE, shuffle=False)

    model = LSTMHydro(
        input_size=len(features),
        hidden_size=config['hidden_size'],
        num_layers=config['num_layers']
    ).to(device)

    optimizer = torch.optim.Adam(model.parameters(), lr=config['lr'])
    criterion = nn.MSELoss()

    for epoch in range(EPOCHS):
        entrainer(model, train_loader, optimizer, criterion)

    predictions, actuals, outliers, erreurs, metriques = evaluer(model, test_loader, train_loader)
    return metriques


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
if __name__ == "__main__":
    print(f"🔄 Chargement des données pour {STATION}...")
    df = get_donnees_station(STATION)

    resultats = {}
    total = len(COMBINATIONS_EXOGENES)

    for i, (nom, features) in enumerate(COMBINATIONS_EXOGENES.items()):
        print(f"\n[{i+1}/{total}] {nom} ({len(features)} features)")
        try:
            metriques = entrainer_combinaison(df, features, CONFIG)
            resultats[nom] = metriques
            print(f"  → MAE={metriques['mae']:.4f} | NSE={metriques['nse']:.4f} | Overfit={metriques['ratio_overfit']:.4f}")
        except Exception as e:
            print(f"  ⚠️  Erreur: {e}")

    # ─── Graphique heatmap ───
    noms      = list(resultats.keys())
    mae_vals  = [resultats[n]['mae']          for n in noms]
    nse_vals  = [resultats[n]['nse']          for n in noms]
    rmse_vals = [resultats[n]['rmse']         for n in noms]
    over_vals = [resultats[n]['ratio_overfit'] for n in noms]

    metriques_labels = ['MAE', 'RMSE', 'NSE', 'Ratio Overfit']
    metriques_sens   = [False, False, True, False]
    data_matrix = np.array([mae_vals, rmse_vals, nse_vals, over_vals]).T

    # Normalisation colonne par colonne pour la couleur
    data_norm = np.zeros_like(data_matrix)
    for j, sens in enumerate(metriques_sens):
        col = data_matrix[:, j]
        col_min, col_max = col.min(), col.max()
        if col_max != col_min:
            normalized = (col - col_min) / (col_max - col_min)
            data_norm[:, j] = normalized if not sens else 1 - normalized
        else:
            data_norm[:, j] = 0.5

    fig, ax = plt.subplots(figsize=(10, 7))
    fig.suptitle(f"Impact des variables exogènes — Station {STATION}", fontsize=13, fontweight='bold')

    im = ax.imshow(data_norm, aspect='auto', cmap='RdYlGn_r', vmin=0, vmax=1)

    ax.set_xticks(range(len(metriques_labels)))
    ax.set_xticklabels(metriques_labels, fontsize=11)
    ax.set_yticks(range(len(noms)))
    ax.set_yticklabels(noms, fontsize=10)

    # Valeur réelle dans chaque cellule
    for i in range(len(noms)):
        for j in range(len(metriques_labels)):
            ax.text(j, i, f"{data_matrix[i, j]:.4f}",
                    ha='center', va='center',
                    fontsize=9, fontweight='bold', color='black')

    plt.colorbar(im, ax=ax, label='Pire → Meilleur')
    plt.tight_layout()
    path = f'./data/IA/Visualisation/exogenes_impact_avancé_{STATION}.png'
    plt.savefig(path, dpi=150, bbox_inches='tight')
    print(f"\n📊 Sauvegardé : {path}")
    plt.close()

    # ─── Résumé texte ───
    print(f"\n📊 Résumé (trié par NSE) :")
    print(f"  {'Combinaison':<25} {'MAE':<8} {'NSE':<8} {'RMSE':<8} {'Overfit'}")
    print(f"  {'-'*60}")
    for nom in sorted(resultats, key=lambda n: resultats[n]['nse'], reverse=True):
        m = resultats[nom]
        print(f"  {nom:<25} {m['mae']:<8.4f} {m['nse']:<8.4f} {m['rmse']:<8.4f} {m['ratio_overfit']:.4f}")