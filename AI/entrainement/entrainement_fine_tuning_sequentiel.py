import torch
import torch.nn as nn
import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from torch.utils.data import DataLoader
import sqlite3

from data_processing.insitu.db_insitu import get_donnees_station, get_stations_insitu
from AI.LSTM import LSTMHydro, device, HydroDataset, entrainer, evaluer

DB_PATH = "./data/insitu_data.db"

CONFIG = {
    'fenetre':     30,
    'hidden_size': 64,
    'num_layers':  2,
    'lr':          0.001,
    'lr_finetune': 0.0005,
    'exogenes':    True,
    'patience':    5
}

FEATURES = ['h_01h_wsh', 'h_09h_wsh', 'h_17h_wsh',
            'precip_jour', 'temp_min_jour', 'temp_max_jour',
            'temp_moy_jour', 'temp_moy_10j', 'precip_moy_10j']
TARGET     = 'h_09h_wsh'
EPOCHS     = 50
BATCH_SIZE = 32


def preparer_donnees_station(df, fenetre):
    data = df[FEATURES].copy()
    scaler = MinMaxScaler()
    data_scaled = scaler.fit_transform(data)
    split = int(len(data_scaled) * 0.8)
    train_dataset = HydroDataset(data_scaled[:split], fenetre, FEATURES, TARGET)
    test_dataset  = HydroDataset(data_scaled[split:],  fenetre, FEATURES, TARGET)
    train_loader  = DataLoader(train_dataset, batch_size=BATCH_SIZE, shuffle=False)
    test_loader   = DataLoader(test_dataset,  batch_size=BATCH_SIZE, shuffle=False)
    return train_loader, test_loader, scaler


if __name__ == "__main__":
    conn = sqlite3.connect(DB_PATH)
    stations = [row[0] for row in get_stations_insitu(conn)][:10]
    conn.close()

    print(f"📍 {len(stations)} stations — fine-tuning séquentiel")

    model = LSTMHydro(
        input_size=len(FEATURES),
        hidden_size=CONFIG['hidden_size'],
        num_layers=CONFIG['num_layers']
    ).to(device)

    criterion  = nn.MSELoss()
    historique = []
    dernier_scaler = None

    for i, station in enumerate(stations):
        print(f"\n🔄 [{i+1}/{len(stations)}] Station {station}")

        df = get_donnees_station(station)
        if df is None or len(df) < CONFIG['fenetre'] + 10:
            print(f"  ⚠️  Pas assez de données, skip")
            continue

        train_loader, test_loader, scaler = preparer_donnees_station(df, CONFIG['fenetre'])
        dernier_scaler = scaler

        lr = CONFIG['lr'] if i == 0 else CONFIG['lr_finetune']
        optimizer = torch.optim.Adam(model.parameters(), lr=lr)
        print(f"  lr={lr} | {len(train_loader.dataset)} séquences train")

        # ─── Entraînement avec early stopping + sauvegarde meilleur état ───
        best_loss         = float('inf')
        best_model_state  = None
        patience_counter  = 0

        for epoch in range(EPOCHS):
            loss = entrainer(model, train_loader, optimizer, criterion)

            if loss < best_loss:
                best_loss        = loss
                best_model_state = {k: v.clone() for k, v in model.state_dict().items()}
                patience_counter = 0
            else:
                patience_counter += 1

            if (epoch + 1) % 10 == 0:
                print(f"  Epoch {epoch+1}/{EPOCHS} | Loss: {loss:.4f} | Best: {best_loss:.4f}")

            if patience_counter >= CONFIG['patience']:
                print(f"  ⏹️  Early stopping à l'epoch {epoch+1}")
                break

        # Recharge le meilleur état avant évaluation
        model.load_state_dict(best_model_state)

        predictions, actuals, outliers, erreurs, metriques = evaluer(model, test_loader, train_loader)
        print(f"  ✅ MAE={metriques['mae']:.4f} | NSE={metriques['nse']:.4f} | Overfit={metriques['ratio_overfit']:.4f}")

        historique.append({
            'station': station,
            'ordre':   i + 1,
            'mae':     metriques['mae'],
            'nse':     metriques['nse'],
            'rmse':    metriques['rmse'],
            'overfit': metriques['ratio_overfit']
        })

    # ─── Sauvegarde ───
    torch.save({
        'model_state': model.state_dict(),
        'config':      CONFIG,
        'features':    FEATURES,
        'target':      TARGET,
        'scaler':      dernier_scaler,
        'stations':    stations,
        'historique':  historique
    }, './data/IA/Models/lstm_finetune_sequentiel2.pt')
    print(f"\n💾 Modèle sauvegardé : lstm_finetune_sequentiel2.pt")

    print(f"\n📊 Résumé par station :")
    print(f"{'Ordre':<6} {'Station':<15} {'MAE':<8} {'NSE':<8} {'RMSE':<8} {'Overfit'}")
    print("-" * 55)
    for h in historique:
        print(f"{h['ordre']:<6} {h['station']:<15} {h['mae']:<8.4f} {h['nse']:<8.4f} {h['rmse']:<8.4f} {h['overfit']:.4f}")