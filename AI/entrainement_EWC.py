import torch
import torch.nn as nn
import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from torch.utils.data import DataLoader
import sqlite3
from copy import deepcopy

from data_processing.insitu.db_insitu import get_donnees_station, get_stations_insitu
from AI.LSTM import LSTMHydro, device, HydroDataset, entrainer, evaluer

DB_PATH = "./data/insitu_data.db"

CONFIG = {
    'fenetre':     30,
    'hidden_size': 64,
    'num_layers':  2,
    'lr':          0.001,
    'ewc_lambda':  100,  # force de la protection EWC
    'exogenes':    True
}

FEATURES = ['h_01h_wsh', 'h_09h_wsh', 'h_17h_wsh',
            'precip_jour', 'temp_min_jour', 'temp_max_jour',
            'temp_moy_jour', 'temp_moy_10j', 'precip_moy_10j']
TARGET     = 'h_09h_wsh'
EPOCHS     = 50
BATCH_SIZE = 32


# ─────────────────────────────────────────────
# EWC
# ─────────────────────────────────────────────
class EWC:
    def __init__(self, model, train_loader, criterion):
        self.model    = model
        self.params   = {n: p.clone().detach() for n, p in model.named_parameters() if p.requires_grad}
        self.fisher   = self._compute_fisher(train_loader, criterion)

    def _compute_fisher(self, train_loader, criterion):
        fisher = {n: torch.zeros_like(p) for n, p in self.model.named_parameters() if p.requires_grad}
        self.model.eval()

        for X_batch, y_batch in train_loader:
            X_batch, y_batch = X_batch.to(device), y_batch.to(device)
            self.model.zero_grad()
            output = self.model(X_batch)
            loss = criterion(output, y_batch)
            loss.backward()

            for n, p in self.model.named_parameters():
                if p.requires_grad and p.grad is not None:
                    fisher[n] += p.grad.detach() ** 2

        # Moyenne sur tous les batchs
        for n in fisher:
            fisher[n] /= len(train_loader)

        return fisher

    def penalty(self, model):
        loss = torch.tensor(0.0).to(device)
        for n, p in model.named_parameters():
            if p.requires_grad and n in self.fisher:
                loss += (self.fisher[n] * (p - self.params[n]) ** 2).sum()
        return loss


def entrainer_ewc(model, train_loader, optimizer, criterion, ewc_list, ewc_lambda):
    model.train()
    total_loss = 0
    for X_batch, y_batch in train_loader:
        X_batch, y_batch = X_batch.to(device), y_batch.to(device)
        optimizer.zero_grad()

        output = model(X_batch)
        loss = criterion(output, y_batch)

        # Ajout de la pénalité EWC pour chaque station précédente
        for ewc in ewc_list:
            loss += ewc_lambda * ewc.penalty(model)

        loss.backward()
        optimizer.step()
        total_loss += loss.item()
    return total_loss / len(train_loader)


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

    print(f"📍 {len(stations)} stations — EWC")

    model = LSTMHydro(
        input_size=len(FEATURES),
        hidden_size=CONFIG['hidden_size'],
        num_layers=CONFIG['num_layers']
    ).to(device)

    criterion  = nn.MSELoss()
    ewc_list   = []  # liste des EWC des stations précédentes
    historique = []
    scalers = {}

    for i, station in enumerate(stations):
        print(f"\n🔄 [{i+1}/{len(stations)}] Station {station}")

        df = get_donnees_station(station)
        if df is None or len(df) < CONFIG['fenetre'] + 10:
            print(f"  ⚠️  Pas assez de données, skip")
            continue

        train_loader, test_loader, scaler = preparer_donnees_station(df, CONFIG['fenetre'])
        scalers[station] = scaler

        optimizer = torch.optim.Adam(model.parameters(), lr=CONFIG['lr'])

        print(f"  EWC actif sur {len(ewc_list)} station(s) précédente(s)")

        for epoch in range(EPOCHS):
            loss = entrainer_ewc(model, train_loader, optimizer, criterion,
                                 ewc_list, CONFIG['ewc_lambda'])
            if (epoch + 1) % 10 == 0:
                print(f"  Epoch {epoch+1}/{EPOCHS} | Loss: {loss:.4f}")

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

        # Calcul EWC pour cette station → protège ses poids pour les suivantes
        ewc_list.append(EWC(model, train_loader, criterion))
        print(f"  🔒 Poids protégés pour les stations suivantes")

    # ─── Sauvegarde ───
    torch.save({
        'model_state': model.state_dict(),
        'config':      CONFIG,
        'features':    FEATURES,
        'target':      TARGET,
        'scalers':      scalers,
        'stations':    stations,
        'historique':  historique
    }, './data/IA/Models/lstm_ewc100.pt')
    print(f"\n💾 Modèle sauvegardé : lstm_ewc100.pt")

    print(f"\n📊 Résumé par station :")
    print(f"{'Ordre':<6} {'Station':<15} {'MAE':<8} {'NSE':<8} {'RMSE':<8} {'Overfit'}")
    print("-" * 55)
    for h in historique:
        print(f"{h['ordre']:<6} {h['station']:<15} {h['mae']:<8.4f} {h['nse']:<8.4f} {h['rmse']:<8.4f} {h['overfit']:.4f}")