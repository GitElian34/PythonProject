import torch
import torch.nn as nn
import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from torch.utils.data import DataLoader, ConcatDataset
import sqlite3
import random
import os

from data_processing.insitu.db_insitu import get_donnees_station
from AI.LSTM import LSTMHydro, device, HydroDataset, entrainer, evaluer

DB_PATH    = "./data/insitu_data.db"
OUTPUT_DIR = "./data/IA/Models/Flag/"
os.makedirs(OUTPUT_DIR, exist_ok=True)

EPOCHS        = 100   # max — early stopping arrêtera avant si nécessaire
PATIENCE      = 5     # epochs sans amélioration avant arrêt
BATCH_SIZE    = 32
N_STATIONS    = 30    # stations par modèle
SEED          = 42

CONFIG = {
    'fenetre':     30,
    'hidden_size': 64,
    'num_layers':  2,
    'lr':          0.001,
}

EXOGENES  = ['precip_jour', 'temp_min_jour', 'temp_max_jour',
             'temp_moy_jour', 'temp_moy_10j', 'precip_moy_10j']
FEATURES  = ['h_med_wsh'] + EXOGENES
TARGET    = 'h_med_wsh'

# ── Définition des 6 expériences ──────────────────────────────────────────────
# Chaque expérience est définie par son nom, son critère de sélection SQL
# et un commentaire expliquant l'intention
EXPERIENCES = {
    'sans_sauts': {
        'description': 'Stations sans aucun saut brutal détecté',
        'where':       "qualite_sauts = 'aucun'",
        'params':      [],
    },
    'beaucoup_sauts': {
        'description': 'Stations avec 100 à 500 sauts brutaux',
        'where':       "qualite_sauts = '100-500'",
        'params':      [],
    },
    'mixte': {
        'description': 'Mix équilibré de toutes les catégories de sauts',
        'where':       "qualite_sauts IS NOT NULL",
        'params':      [],
        'stratifie':   True,  # ← sélection stratifiée par catégorie
    },
    'signal_plat': {
        'description': 'Stations avec un signal plat détecté',
        'where':       "signal_plat = 1",
        'params':      [],
    },
    'sans_plat': {
        'description': 'Stations sans signal plat, sans saut, sans gap',
        'where':       "signal_plat = 0 AND qualite_sauts IN ('aucun', '< 10') AND gap_max_jours IS NULL",
        'params':      [],
    },
    'aleatoire': {
        'description': 'Sélection complètement aléatoire — baseline',
        'where':       "1=1",  # pas de filtre
        'params':      [],
    },
}


# ─────────────────────────────────────────────
# Sélection des stations selon le critère
# ─────────────────────────────────────────────
def selectionner_stations(experience, n=N_STATIONS, seed=SEED):
    """
    Sélectionne n stations selon le critère défini dans l'expérience.
    Pour l'expérience 'mixte', on fait une sélection stratifiée :
    on tire des stations de chaque catégorie de sauts proportionnellement
    pour garantir la diversité plutôt qu'un tirage aléatoire naïf.
    """
    random.seed(seed)
    conn = sqlite3.connect(DB_PATH)

    # Cas spécial : sélection stratifiée pour le modèle mixte
    if experience.get('stratifie'):
        categories = ['aucun', '< 10', '10-100', '100-500', '> 500']
        n_par_cat  = max(1, n // len(categories))
        stations   = []

        for cat in categories:
            df_cat = pd.read_sql_query(f"""
                SELECT code_sta FROM stations_insitu
                WHERE qualite_sauts = ?
                  AND (dans_lac IS NULL OR dans_lac NOT IN ('dans_lac', 'proche_lac'))
            """, conn, params=[cat])['code_sta'].tolist()

            # On tire min(n_par_cat, disponible) stations par catégorie
            stations += random.sample(df_cat, min(n_par_cat, len(df_cat)))

        # Compléter si on n'a pas atteint n stations
        if len(stations) < n:
            df_reste = pd.read_sql_query(f"""
                SELECT code_sta FROM stations_insitu
                WHERE qualite_sauts IS NOT NULL
                  AND (dans_lac IS NULL OR dans_lac NOT IN ('dans_lac', 'proche_lac'))
                  AND code_sta NOT IN ({','.join('?' * len(stations))})
            """, conn, params=stations)['code_sta'].tolist()
            stations += random.sample(df_reste, min(n - len(stations), len(df_reste)))

    else:
        df = pd.read_sql_query(f"""
            SELECT code_sta FROM stations_insitu
            WHERE {experience['where']}
              AND (dans_lac IS NULL OR dans_lac NOT IN ('dans_lac', 'proche_lac'))
        """, conn, params=experience['params'])['code_sta'].tolist()

        if len(df) < n:
            print(f"  ⚠️  Seulement {len(df)} stations disponibles (demandé : {n})")

        stations = random.sample(df, min(n, len(df)))

    conn.close()
    return stations


# ─────────────────────────────────────────────
# Construction des datasets
# ─────────────────────────────────────────────
def construire_datasets(stations, features, target, fenetre):
    """
    Pour chaque station, normalise indépendamment avec MinMaxScaler
    puis crée les fenêtres glissantes. On sépare 80% pour l'entraînement
    et 20% pour le test, dans l'ordre chronologique (pas de shuffle).
    """
    train_datasets = []
    test_datasets  = []
    scalers        = {}
    stations_ok    = []

    for station in stations:
        df = get_donnees_station(station)
        if df is None or len(df) < fenetre + 10:
            print(f"  ⚠️  {station} — pas assez de données, skip")
            continue

        manquantes = [f for f in features if f not in df.columns]
        if manquantes:
            print(f"  ⚠️  {station} — colonnes manquantes : {manquantes}, skip")
            continue

        data        = df[features].copy()
        scaler      = MinMaxScaler()
        data_scaled = scaler.fit_transform(data)
        scalers[station] = scaler

        split = int(len(data_scaled) * 0.8)
        train_datasets.append(HydroDataset(data_scaled[:split], fenetre, features, target))
        test_datasets.append( HydroDataset(data_scaled[split:], fenetre, features, target))
        stations_ok.append(station)
        print(f"  ✅ {station} — {len(data_scaled)} jours")

    if not train_datasets:
        raise ValueError("Aucune station valide trouvée pour cette expérience.")

    train_loader = DataLoader(ConcatDataset(train_datasets), batch_size=BATCH_SIZE, shuffle=True)
    test_loader  = DataLoader(ConcatDataset(test_datasets),  batch_size=BATCH_SIZE, shuffle=False)

    print(f"  Dataset : {len(ConcatDataset(train_datasets))} train "
          f"| {len(ConcatDataset(test_datasets))} test")

    return train_loader, test_loader, scalers, stations_ok


# ─────────────────────────────────────────────
# Early stopping
# ─────────────────────────────────────────────
class EarlyStopping:
    """
    Surveille la loss de validation à chaque epoch.
    Si elle ne s'améliore pas pendant 'patience' epochs consécutives,
    on arrête l'entraînement et on restaure les meilleurs poids.
    """
    def __init__(self, patience=PATIENCE, min_delta=1e-4):
        self.patience    = patience
        self.min_delta   = min_delta
        self.best_loss   = np.inf
        self.counter     = 0
        self.best_weights = None

    def step(self, val_loss, model):
        if val_loss < self.best_loss - self.min_delta:
            # Amélioration — on sauvegarde les poids et on remet le compteur à 0
            self.best_loss    = val_loss
            self.counter      = 0
            self.best_weights = {k: v.clone() for k, v in model.state_dict().items()}
        else:
            self.counter += 1

        return self.counter >= self.patience  # True = on arrête

    def restore(self, model):
        """Restaure les meilleurs poids sauvegardés."""
        if self.best_weights is not None:
            model.load_state_dict(self.best_weights)


# ─────────────────────────────────────────────
# Entraînement d'une expérience
# ─────────────────────────────────────────────
def entrainer_experience(nom, experience, features, target):
    print(f"\n{'═'*60}")
    print(f"  Expérience : {nom}")
    print(f"  {experience['description']}")
    print(f"{'═'*60}")

    # Sélection et construction des données
    stations = selectionner_stations(experience)
    print(f"\n  {len(stations)} stations sélectionnées : {stations}")

    train_loader, test_loader, scalers, stations_ok = construire_datasets(
        stations, features, target, CONFIG['fenetre']
    )

    # Modèle, optimiseur, critère
    model     = LSTMHydro(
        input_size=len(features),
        hidden_size=CONFIG['hidden_size'],
        num_layers=CONFIG['num_layers']
    ).to(device)
    optimizer = torch.optim.Adam(model.parameters(), lr=CONFIG['lr'])
    criterion = nn.MSELoss()
    stopper   = EarlyStopping(patience=PATIENCE)

    # Boucle d'entraînement avec early stopping
    print(f"\n🚀 Entraînement (max {EPOCHS} epochs, patience={PATIENCE})...")
    epoch_arret = EPOCHS
    for epoch in range(EPOCHS):
        train_loss = entrainer(model, train_loader, optimizer, criterion)

        # On utilise la train_loss comme proxy de validation
        # (pas de vrai val set séparé ici — le test_loader sert à l'évaluation finale)
        if stopper.step(train_loss, model):
            epoch_arret = epoch + 1
            print(f"  ⏹️  Early stopping à l'epoch {epoch_arret} "
                  f"(meilleure loss : {stopper.best_loss:.4f})")
            stopper.restore(model)
            break

        if (epoch + 1) % 10 == 0:
            print(f"  Epoch {epoch+1}/{EPOCHS} | Loss: {train_loss:.4f}")

    # Évaluation finale sur le test set
    predictions, actuals, outliers, erreurs, metriques = evaluer(
        model, test_loader, train_loader
    )
    print(f"\n  ✅ MAE={metriques['mae']:.4f} | NSE={metriques['nse']:.4f} "
          f"| KGE={metriques.get('kge', float('nan')):.4f} "
          f"| Overfit={metriques['ratio_overfit']:.4f}")

    # Sauvegarde
    path = os.path.join(OUTPUT_DIR, f"lstm_{nom}.pt")
    torch.save({
        'model_state':  model.state_dict(),
        'config':       CONFIG,
        'features':     features,
        'target':       target,
        'scalers':      scalers,
        'stations':     stations_ok,
        'metriques':    metriques,
        'epoch_arret':  epoch_arret,
        'description':  experience['description'],
    }, path)
    print(f"  💾 Sauvegardé : {path}")

    return metriques, epoch_arret


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────
if __name__ == "__main__":
    random.seed(SEED)
    resultats = {}

    for nom, experience in EXPERIENCES.items():
        try:
            metriques, epoch_arret = entrainer_experience(
                nom, experience, FEATURES, TARGET
            )
            resultats[nom] = {**metriques, 'epoch_arret': epoch_arret}
        except Exception as e:
            print(f"  ⚠️  Expérience {nom} échouée : {e}")

    # ── Résumé final ──────────────────────────────────────────────
    print(f"\n{'═'*65}")
    print(f"  RÉSUMÉ FINAL — 6 modèles")
    print(f"{'═'*65}")
    print(f"  {'Modèle':<18} {'MAE':<8} {'NSE':<8} {'RMSE':<8} {'Overfit':<10} {'Epochs'}")
    print(f"  {'-'*60}")
    for nom in sorted(resultats, key=lambda n: resultats[n].get('nse', 0), reverse=True):
        m = resultats[nom]
        print(f"  {nom:<18} {m['mae']:<8.4f} {m.get('nse', float('nan')):<8.4f} "
              f"{m['rmse']:<8.4f} {m['ratio_overfit']:<10.4f} {m['epoch_arret']}")
    print(f"{'═'*65}")