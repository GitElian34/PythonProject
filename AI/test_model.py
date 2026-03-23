import torch
import numpy as np
import matplotlib.pyplot as plt
from data_processing.insitu.db_insitu import get_donnees_station
from LSTM import LSTMHydro, HydroDataset, evaluer, visualiser_outliers, FEATURES, FENETRE, HIDDEN_SIZE, NUM_LAYERS
from torch.utils.data import DataLoader
from sklearn.preprocessing import MinMaxScaler

STATION = 'A343021001'
MODEL_PATH = f'./data/IA/Models/lstm_{STATION}.pt'
BATCH_SIZE = 32
STATION_TEST = 'U221502001'


if __name__ == "__main__":
    print(f"🔄 Chargement des données pour {STATION}...")
    df = get_donnees_station(STATION_TEST)

    # Normalisation
    scaler = MinMaxScaler()
    data_scaled = scaler.fit_transform(df[FEATURES].copy())

    # Dataset complet (pas de split train/test)
    dataset = HydroDataset(data_scaled, FENETRE)
    loader = DataLoader(dataset, batch_size=BATCH_SIZE, shuffle=False)

    # Charger le modèle
    model = LSTMHydro(
        input_size=len(FEATURES),
        hidden_size=HIDDEN_SIZE,
        num_layers=NUM_LAYERS
    )
    model.load_state_dict(torch.load(MODEL_PATH))
    model.eval()
    print(f"✅ Modèle chargé : {MODEL_PATH}")

    # Évaluation
    print(f"\n🔍 Détection d'outliers...")
    predictions, actuals, outliers, erreurs = evaluer(model, loader)

    # Visualisation
    visualiser_outliers(predictions, actuals, outliers, erreurs, STATION, df)