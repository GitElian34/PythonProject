import torch
import torch.nn as nn
import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from torch.utils.data import DataLoader, ConcatDataset
import sqlite3

from data_processing.insitu.db_insitu import get_donnees_station, get_stations_insitu
from AI.LSTM import LSTMHydro, device, HydroDataset, entrainer, evaluer

DB_PATH = "./data/insitu_data.db"

CONFIG = {
    'fenetre':     30,
    'hidden_size': 64,
    'num_layers':  2,
    'lr':          0.001,
    'exogenes':    True
}

FEATURES = ['h_01h_wsh', 'h_09h_wsh', 'h_17h_wsh',
            'precip_jour', 'temp_min_jour', 'temp_max_jour',
            'temp_moy_jour', 'temp_moy_10j', 'precip_moy_10j']
TARGET     = 'h_09h_wsh'
EPOCHS     = 50
BATCH_SIZE = 32


if __name__ == "__main__":
    conn = sqlite3.connect(DB_PATH)
    stations = [row[0] for row in get_stations_insitu(conn)][:10]
    conn.close()

    print(f"📍 Chargement et normalisation par station...")

    # ─── Normalisation indépendante par station ───
    train_datasets = []
    test_datasets  = []
    scalers        = {}

    for station in stations:
        print(f"  🔄 {station}")
        df = get_donnees_station(station)
        if df is None or len(df) < CONFIG['fenetre'] + 10:
            print(f"    ⚠️  Pas assez de données, skip")
            continue

        # Scaler propre à chaque station
        data = df[FEATURES].copy()
        scaler = MinMaxScaler()
        data_scaled = scaler.fit_transform(data)
        scalers[station] = scaler

        split = int(len(data_scaled) * 0.8)
        train_datasets.append(HydroDataset(data_scaled[:split], CONFIG['fenetre'], FEATURES, TARGET))
        test_datasets.append(HydroDataset(data_scaled[split:],  CONFIG['fenetre'], FEATURES, TARGET))
        print(f"    ✅ {len(data_scaled)} jours normalisés indépendamment")

    # ─── Concaténation des datasets normalisés ───
    train_combined = ConcatDataset(train_datasets)
    test_combined  = ConcatDataset(test_datasets)

    train_loader = DataLoader(train_combined, batch_size=BATCH_SIZE, shuffle=False)  # shuffle=True important
    test_loader  = DataLoader(test_combined,  batch_size=BATCH_SIZE, shuffle=False)

    print(f"\n📊 Dataset combiné : {len(train_combined)} train | {len(test_combined)} test")

    # ─── Modèle ───
    model = LSTMHydro(
        input_size=len(FEATURES),
        hidden_size=CONFIG['hidden_size'],
        num_layers=CONFIG['num_layers']
    ).to(device)

    optimizer = torch.optim.Adam(model.parameters(), lr=CONFIG['lr'])
    criterion = nn.MSELoss()

    # ─── Entraînement ───
    print(f"\n🚀 Entraînement ({EPOCHS} epochs)...")
    for epoch in range(EPOCHS):
        loss = entrainer(model, train_loader, optimizer, criterion)
        if (epoch + 1) % 10 == 0:
            print(f"  Epoch {epoch+1}/{EPOCHS} | Loss: {loss:.4f}")

    # ─── Évaluation globale ───
    predictions, actuals, outliers, erreurs, metriques = evaluer(model, test_loader, train_loader)
    print(f"\n✅ MAE={metriques['mae']:.4f} | NSE={metriques['nse']:.4f} | Overfit={metriques['ratio_overfit']:.4f}")

    # ─── Sauvegarde ───
    torch.save({
        'model_state': model.state_dict(),
        'config':      CONFIG,
        'features':    FEATURES,
        'target':      TARGET,
        'scalers':     scalers,  # un scaler par station
        'stations':    stations,
        'metriques':   metriques
    }, './data/IA/Models/lstm_normalisation_par_station_sfFalse.pt')
    print(f"\n💾 Modèle sauvegardé : lstm_normalisation_par_station.pt")