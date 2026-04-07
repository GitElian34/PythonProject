import torch
import torch.nn as nn
import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from torch.utils.data import DataLoader, ConcatDataset
import sqlite3
import random
import os

from data_processing import get_donnees_station, get_era5_bv
from AI.LSTM.LSTM import LSTMHydro, device, HydroDataset, entrainer, evaluer

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
DB_PATH    = "./data/insitu_data.db"
OUTPUT_DIR = "./data/IA/Models/BV/"
os.makedirs(OUTPUT_DIR, exist_ok=True)

EPOCHS     = 100
PATIENCE   = 10
BATCH_SIZE = 32
N_STATIONS = 10
SEED       = 42

CONFIG = {
    'fenetre'    : 30,
    'hidden_size': 64,
    'num_layers' : 2,
    'lr'         : 0.001,
}

TARGET = 'h_med_wsh'

# Features baseline
FEATURES_BASE = ['h_med_wsh', 'precip_jour', 'temp_min_jour',
                 'temp_max_jour', 'temp_moy_jour', 'temp_moy_10j',
                 'precip_moy_10j']

# Features avec BV — precip_moy_10j remplacé par les 50 features ERA5-BV
TRANCHES      = ['0-40km', '40-80km', '80-150km', '150-300km', '>300km']
FEATURES_BV   = ['h_med_wsh', 'precip_jour', 'temp_min_jour',
                 'temp_max_jour', 'temp_moy_jour', 'temp_moy_10j'] + \
                [f'J{j}_{t}' for j in range(10) for t in TRANCHES]

# ═══════════════════════════════════════════════════════════════
# FONCTIONS
# ═══════════════════════════════════════════════════════════════

def selectionner_stations_avec_bv(n=N_STATIONS, seed=SEED):
    """Sélectionne n stations qui ont des données ERA5-BV disponibles."""
    random.seed(seed)
    conn = sqlite3.connect(DB_PATH)

    stations = pd.read_sql_query('''
        SELECT DISTINCT p.code_sta
        FROM era5_pluie_bv p
        JOIN mesures_insitu m ON p.mesure_id = m.id
        JOIN era5_insitu e ON m.code_sta = e.code_sta AND m.date = e.date
        WHERE (
            SELECT COUNT(*) FROM era5_transfert t
            WHERE t.code_sta = p.code_sta
        ) >= 100
    ''', conn)['code_sta'].tolist()

    conn.close()
    print(f"Stations avec données BV disponibles : {len(stations)}")

    selected = random.sample(stations, min(n, len(stations)))
    print(f"Stations sélectionnées : {selected}")
    return selected


def construire_datasets(stations, features, target, fenetre, avec_bv=False):
    """
    Construit les datasets train/test pour une liste de stations.
    Normalisation par station ET par groupe de colonnes si avec_bv=True.
    """
    train_datasets = []
    test_datasets  = []
    scalers        = {}
    stations_ok    = []

    TRANCHES_BV = ['0-40km', '40-80km', '80-150km', '150-300km', '>300km']
    COLS_BASE   = ['h_med_wsh', 'precip_jour', 'temp_min_jour',
                   'temp_max_jour', 'temp_moy_jour', 'temp_moy_10j',
                   'precip_moy_10j']

    for station in stations:
        # Charger les données
        df = get_donnees_station(station)
        if df is None or len(df) < fenetre + 10:
            print(f"  ⚠️  {station} — pas assez de données, skip")
            continue

        if avec_bv:
            df_bv = get_era5_bv(station)
            if df_bv is None:
                print(f"  ⚠️  {station} — pas de données BV, skip")
                continue
            df = df.merge(df_bv, on='date', how='inner')
            df = df.drop(columns=['precip_moy_10j'], errors='ignore')

        manquantes = [f for f in features if f not in df.columns]
        if manquantes:
            print(f"  ⚠️  {station} — colonnes manquantes : {manquantes}, skip")
            continue

        data = df[features].copy()
        data_scaled = np.zeros((len(data), len(features)), dtype=np.float32)
        scalers_station = {}

        if not avec_bv:
            # ── Normalisation simple : un seul RobustScaler pour tout ──
            scaler = MinMaxScaler()
            data_scaled = scaler.fit_transform(data)
            scalers_station['all'] = scaler

        else:
            # ── Normalisation par groupe de colonnes ──

            # 1. Colonnes de base
            cols_base_presentes = [c for c in COLS_BASE if c in data.columns]
            if cols_base_presentes:
                idx = [data.columns.get_loc(c) for c in cols_base_presentes]
                scaler_base = MinMaxScaler()
                data_scaled[:, idx] = scaler_base.fit_transform(data.iloc[:, idx])
                scalers_station['base'] = scaler_base

            # 2. Colonnes BV par tranche
            for tranche in TRANCHES_BV:
                cols_tranche = [c for c in data.columns if tranche in c]
                if not cols_tranche:
                    continue
                idx = [data.columns.get_loc(c) for c in cols_tranche]
                scaler_t = MinMaxScaler()
                data_scaled[:, idx] = scaler_t.fit_transform(data.iloc[:, idx])
                scalers_station[tranche] = scaler_t

        scalers[station] = scalers_station

        split = int(len(data_scaled) * 0.8)
        train_datasets.append(HydroDataset(data_scaled[:split], fenetre, features, target))
        test_datasets.append( HydroDataset(data_scaled[split:],  fenetre, features, target))
        stations_ok.append(station)
        print(f"  ✅ {station} — {len(data_scaled)} jours | {len(features)} features")

    if not train_datasets:
        raise ValueError("Aucune station valide.")

    train_loader = DataLoader(ConcatDataset(train_datasets), batch_size=BATCH_SIZE, shuffle=True)
    test_loader  = DataLoader(ConcatDataset(test_datasets),  batch_size=BATCH_SIZE, shuffle=False)

    print(f"  Dataset : {len(ConcatDataset(train_datasets))} train "
          f"| {len(ConcatDataset(test_datasets))} test")

    return train_loader, test_loader, scalers, stations_ok


class EarlyStopping:
    def __init__(self, patience=PATIENCE, min_delta=1e-4):
        self.patience     = patience
        self.min_delta    = min_delta
        self.best_loss    = np.inf
        self.counter      = 0
        self.best_weights = None

    def step(self, val_loss, model):
        if val_loss < self.best_loss - self.min_delta:
            self.best_loss    = val_loss
            self.counter      = 0
            self.best_weights = {k: v.clone() for k, v in model.state_dict().items()}
        else:
            self.counter += 1
        return self.counter >= self.patience

    def restore(self, model):
        if self.best_weights is not None:
            model.load_state_dict(self.best_weights)


def entrainer_modele(nom, features, stations, avec_bv=False):
    """Entraîne un modèle LSTM sur les stations données."""
    print(f"\n{'═'*60}")
    print(f"  Modèle : {nom}")
    print(f"  Features : {len(features)} | avec_bv={avec_bv}")
    print(f"{'═'*60}")

    train_loader, test_loader, scalers, stations_ok = construire_datasets(
        stations, features, TARGET, CONFIG['fenetre'], avec_bv=avec_bv
    )

    model     = LSTMHydro(
        input_size=len(features),
        hidden_size=CONFIG['hidden_size'],
        num_layers=CONFIG['num_layers']
    ).to(device)
    optimizer = torch.optim.Adam(model.parameters(), lr=CONFIG['lr'])
    criterion = nn.MSELoss()
    stopper   = EarlyStopping(patience=PATIENCE)

    print(f"\n🚀 Entraînement (max {EPOCHS} epochs, patience={PATIENCE})...")
    epoch_arret = EPOCHS
    for epoch in range(EPOCHS):
        train_loss = entrainer(model, train_loader, optimizer, criterion)

        if stopper.step(train_loss, model):
            epoch_arret = epoch + 1
            print(f"  ⏹️  Early stopping epoch {epoch_arret} "
                  f"(best loss: {stopper.best_loss:.4f})")
            stopper.restore(model)
            break

        if (epoch + 1) % 10 == 0:
            print(f"  Epoch {epoch+1}/{EPOCHS} | Loss: {train_loss:.4f}")

    predictions, actuals, outliers, erreurs, metriques = evaluer(
        model, test_loader, train_loader
    )
    print(f"\n  ✅ MAE={metriques['mae']:.4f} | NSE={metriques['nse']:.4f} "
          f"| KGE={metriques.get('kge', float('nan')):.4f} "
          f"| Overfit={metriques['ratio_overfit']:.4f}")

    path = os.path.join(OUTPUT_DIR, f"lstm_{nom}.pt")
    torch.save({
        'model_state': model.state_dict(),
        'config'     : CONFIG,
        'features'   : features,
        'target'     : TARGET,
        'scalers'    : scalers,
        'stations'   : stations_ok,
        'metriques'  : metriques,
        'epoch_arret': epoch_arret,
        'avec_bv'    : avec_bv,
    }, path)
    print(f"  💾 Sauvegardé : {path}")

    return metriques, epoch_arret


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    random.seed(SEED)

    # Mêmes stations pour les deux modèles
    stations = selectionner_stations_avec_bv(N_STATIONS, SEED)

    resultats = {}

    # ── Modèle 1 : baseline sans BV ──
    try:
        metriques, epoch_arret = entrainer_modele(
            'baseline', FEATURES_BASE, stations, avec_bv=False
        )
        resultats['baseline'] = {**metriques, 'epoch_arret': epoch_arret}
    except Exception as e:
        print(f"⚠️  Baseline échoué : {e}")

    # ── Modèle 2 : avec features ERA5-BV ──
    try:
        metriques, epoch_arret = entrainer_modele(
            'avec_bv', FEATURES_BV, stations, avec_bv=True
        )
        resultats['avec_bv'] = {**metriques, 'epoch_arret': epoch_arret}
    except Exception as e:
        print(f"⚠️  Avec BV échoué : {e}")

    # ── Résumé comparatif ──
    print(f"\n{'═'*65}")
    print(f"  COMPARAISON — mêmes {N_STATIONS} stations")
    print(f"{'═'*65}")
    print(f"  {'Modèle':<12} {'MAE':<8} {'NSE':<8} {'RMSE':<8} {'Overfit':<10} {'Epochs'}")
    print(f"  {'-'*55}")
    for nom in ['baseline', 'avec_bv']:
        if nom in resultats:
            m = resultats[nom]
            print(f"  {nom:<12} {m['mae']:<8.4f} {m.get('nse', float('nan')):<8.4f} "
                  f"{m['rmse']:<8.4f} {m['ratio_overfit']:<10.4f} {m['epoch_arret']}")
    print(f"{'═'*65}")