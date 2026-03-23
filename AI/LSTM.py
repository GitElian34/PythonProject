import torch
import torch.nn as nn
import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from torch.utils.data import Dataset, DataLoader

from AI.Visualisation import visualiser_outliers
from data_processing.insitu.db_insitu import get_donnees_station
import os

# Limite à 1 seul GPU
os.environ['CUDA_VISIBLE_DEVICES'] = '0'

device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
print(f"🖥️  Device : {device}")

# Limite la mémoire GPU à 30% du GPU sélectionné
if torch.cuda.is_available():
    torch.cuda.set_per_process_memory_fraction(0.3)
    print(f"  GPU : {torch.cuda.get_device_name(0)}")
    print(f"  Mémoire limitée à 30%")

# Limite les threads CPU
torch.set_num_threads(4)
# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────
STATION = 'A343021001'
FENETRE = 7        # jours passés utilisés pour prédire J+1
BATCH_SIZE = 32
EPOCHS = 30
HIDDEN_SIZE = 64
NUM_LAYERS = 2
LR = 0.001

FEATURES = ['h_01h_wsh', 'h_09h_wsh', 'h_17h_wsh',
            'precip_jour', 'temp_min_jour', 'temp_max_jour',
            'temp_moy_jour', 'temp_moy_10j', 'precip_moy_10j']
TARGET = 'h_09h_wsh'  # valeur à prédire à J+1


# ─────────────────────────────────────────────
# DATASET
# ─────────────────────────────────────────────
class HydroDataset(Dataset):
    def __init__(self, data, fenetre, features, target):
        self.X = []
        self.y = []
        target_idx = features.index(target)
        for i in range(len(data) - fenetre):
            self.X.append(data[i:i+fenetre])
            self.y.append(data[i+fenetre, target_idx])
        self.X = torch.tensor(np.array(self.X), dtype=torch.float32)
        self.y = torch.tensor(np.array(self.y), dtype=torch.float32)

    def __len__(self):
        return len(self.X)          # ← manquait

    def __getitem__(self, idx):
        return self.X[idx], self.y[idx]
# ─────────────────────────────────────────────
# MODÈLE
# ─────────────────────────────────────────────
class LSTMHydro(nn.Module):
    def __init__(self, input_size, hidden_size, num_layers):
        super(LSTMHydro, self).__init__()
        self.lstm = nn.LSTM(input_size, hidden_size, num_layers, batch_first=True, dropout=0.2)
        self.fc = nn.Linear(hidden_size, 1)

    def forward(self, x):
        out, _ = self.lstm(x)
        out = self.fc(out[:, -1, :])  # dernier pas de temps
        return out.squeeze()


# ─────────────────────────────────────────────
# PRÉPARATION DES DONNÉES
# ─────────────────────────────────────────────
def preparer_donnees(df, features, target, fenetre, batch_size=32):
    data = df[features].copy()
    scaler = MinMaxScaler()
    data_scaled = scaler.fit_transform(data)
    split = int(len(data_scaled) * 0.8)

    train_dataset = HydroDataset(data_scaled[:split], fenetre, features, target)
    test_dataset  = HydroDataset(data_scaled[split:],  fenetre, features, target)
    train_loader  = DataLoader(train_dataset, batch_size=batch_size, shuffle=False)
    test_loader   = DataLoader(test_dataset,  batch_size=batch_size, shuffle=False)

    return train_loader, test_loader, scaler, data_scaled, split
# ─────────────────────────────────────────────
# ENTRAÎNEMENT
# ─────────────────────────────────────────────
def entrainer(model, train_loader, optimizer, criterion):
    model.train()
    total_loss = 0
    for X_batch, y_batch in train_loader:
        optimizer.zero_grad()
        y_pred = model(X_batch)
        loss = criterion(y_pred, y_batch)
        loss.backward()
        optimizer.step()
        total_loss += loss.item()
    return total_loss / len(train_loader)


# ─────────────────────────────────────────────
# ÉVALUATION + DÉTECTION OUTLIERS
# ─────────────────────────────────────────────
def evaluer(model, test_loader, train_loader, seuil_sigma=2.5):
    model.eval()
    predictions = []
    actuals = []

    with torch.no_grad():
        for X_batch, y_batch in test_loader:
            X_batch, y_batch = X_batch.to(device), y_batch.to(device)
            y_pred = model(X_batch)
            # Protection contre les tenseurs 0-d (batch de taille 1)
            if y_pred.dim() == 0:
                y_pred = y_pred.unsqueeze(0)
            if y_batch.dim() == 0:
                y_batch = y_batch.unsqueeze(0)
            predictions.extend(y_pred.cpu().numpy())
            actuals.extend(y_batch.cpu().numpy())

    predictions = np.array(predictions)
    actuals = np.array(actuals)
    erreurs = np.abs(predictions - actuals)

    # MAE
    mae = erreurs.mean()

    # RMSE
    rmse = np.sqrt((erreurs**2).mean())

    # R²
    ss_res = np.sum((actuals - predictions)**2)
    ss_tot = np.sum((actuals - actuals.mean())**2)
    r2 = 1 - ss_res / ss_tot if ss_tot > 0 else 0

    # NSE
    nse = 1 - np.sum((actuals - predictions)**2) / np.sum((actuals - actuals.mean())**2) if ss_tot > 0 else 0

    # Train loss pour ratio overfitting
    model.train()
    criterion = nn.MSELoss()
    train_losses = []
    with torch.no_grad():
        for X_batch, y_batch in train_loader:
            y_pred = model(X_batch)
            train_losses.append(criterion(y_pred, y_batch).item())
    train_loss = np.mean(train_losses)

    test_losses = (erreurs**2).mean()
    ratio_overfit = test_losses / train_loss if train_loss > 0 else None

    # Outliers
    mean_err = erreurs.mean()
    std_err = erreurs.std()
    outliers = erreurs > (mean_err + seuil_sigma * std_err)

    print(f"\n📊 Résultats :")
    print(f"  MAE            : {mae:.4f}")
    print(f"  RMSE           : {rmse:.4f}")
    print(f"  R²             : {r2:.4f}")
    print(f"  NSE            : {nse:.4f}")
    print(f"  Ratio overfit  : {ratio_overfit:.4f}")
    print(f"  Outliers       : {outliers.sum()} / {len(outliers)}")

    return predictions, actuals, outliers, erreurs, {
        'mae': mae, 'rmse': rmse, 'r2': r2,
        'nse': nse, 'ratio_overfit': ratio_overfit
    }


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
if __name__ == "__main__":
    print(f"🔄 Chargement des données pour {STATION}...")
    df = get_donnees_station(STATION)
    train_loader, test_loader, scaler, data_scaled, split = preparer_donnees(
        df, FEATURES, TARGET, FENETRE, BATCH_SIZE
    )
    model = LSTMHydro(
        input_size=len(FEATURES),
        hidden_size=HIDDEN_SIZE,
        num_layers=NUM_LAYERS
    )
    optimizer = torch.optim.Adam(model.parameters(), lr=LR)
    criterion = nn.MSELoss()

    print(f"\n🚀 Entraînement ({EPOCHS} epochs)...")
    for epoch in range(EPOCHS):
        loss = entrainer(model, train_loader, optimizer, criterion)
        if (epoch + 1) % 10 == 0:
            print(f"  Epoch {epoch+1}/{EPOCHS} | Loss: {loss:.4f}")

    print(f"\n🔍 Évaluation et détection d'outliers...")
    predictions, actuals, outliers, erreurs = evaluer(model, test_loader)

    visualiser_outliers(predictions, actuals, outliers, erreurs, STATION, df)

    torch.save(model.state_dict(), f'./data/models/lstm_{STATION}.pt')
    print(f"\n✅ Modèle sauvegardé")