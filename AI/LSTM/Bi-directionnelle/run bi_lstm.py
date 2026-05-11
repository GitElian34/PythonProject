#!/usr/bin/env python3
"""
run_bilstm.py — Script principal du Bi-LSTM
Orchestre : sélection stations → dataset → modèle → entraînement → évaluation
"""

import torch
import numpy as np
import random
from config import SEED, DYNAMIC_VARS, STATIC_COLS
from dataset import select_stations, WaterLevelDataset
from model import BiLSTMOutlierDetector
from train import train
from evaluate import evaluate

# ─── Reproductibilité ────────────────────────────────────────────────────────
random.seed(SEED)
np.random.seed(SEED)
torch.manual_seed(SEED)
torch.set_num_threads(10)

device = torch.device("cpu")
print(f"🖥️  Device : {device}")

# ─── 1. Sélection des stations ───────────────────────────────────────────────
print("\n📂 Sélection des stations...")
train_ids, val_ids = select_stations()
print(f"  {len(train_ids)} entraînement | {len(val_ids)} validation")

# ─── 2. Construction des datasets ────────────────────────────────────────────
print("\n📊 Construction des datasets...")
print("  Train :")
train_ds = WaterLevelDataset(train_ids)
print("  Validation :")
val_ds   = WaterLevelDataset(val_ids)

# ─── 3. Initialisation du modèle ─────────────────────────────────────────────
n_dynamic = len(DYNAMIC_VARS)
n_static  = len(STATIC_COLS)

model = BiLSTMOutlierDetector(
    n_dynamic=n_dynamic,
    n_static=n_static
).to(device)

total_params = sum(p.numel() for p in model.parameters())
print(f"\n🧠 Bi-LSTM : {total_params:,} paramètres")
print(f"   hidden_size={model.lstm.hidden_size} | "
      f"bidirectionnel=True | layers={model.lstm.num_layers}")

# ─── 4. Entraînement ─────────────────────────────────────────────────────────
best_nse = train(model, train_ds, val_ds, device)

# ─── 5. Évaluation finale ────────────────────────────────────────────────────
print("\n📊 Évaluation finale...")
nse, kge = evaluate(model, val_ds, device)