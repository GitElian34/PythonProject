#!/usr/bin/env python3
"""
dataset.py — Dataset PyTorch pour le Bi-LSTM
Lit les .nc existants et construit des séquences ±WINDOW autour de T
water_level(T) est masqué à 0 dans les inputs
"""

import numpy as np
import pandas as pd
import xarray as xr
import sqlite3
import random
import torch
from torch.utils.data import Dataset
from config import (
    NC_DIR, ATTRS_CSV, DB_PATH,
    WINDOW, DYNAMIC_VARS, STATIC_COLS,
    N_TRAIN, N_VAL, SEED, MIN_DIST_M, MIN_STD
)

# ─── Chargement et normalisation des attributs statiques ─────────────────────
def load_attrs():
    attrs = pd.read_csv(ATTRS_CSV)
    attrs["station_id"] = attrs["station_id"].astype(str)
    attrs = attrs.set_index("station_id")

    vals  = attrs[STATIC_COLS].values.astype(np.float32)
    mean_ = np.nanmean(vals, axis=0)
    std_  = np.nanstd(vals,  axis=0)
    std_[std_ == 0] = 1.0

    return attrs, mean_, std_

ATTRS, ATTRS_MEAN, ATTRS_STD = load_attrs()

def get_static(sid):
    """Retourne le vecteur statique normalisé pour une station."""
    if sid not in ATTRS.index:
        return np.zeros(len(STATIC_COLS), dtype=np.float32)
    row = ATTRS.loc[sid, STATIC_COLS].values.astype(np.float32)
    row = np.nan_to_num(row, nan=0.0)
    return (row - ATTRS_MEAN) / ATTRS_STD


# ─── Sélection des stations ───────────────────────────────────────────────────
def select_stations():
    """
    Sélectionne les stations qualifiées (dist_barrage >= 500m,
    .nc disponible, assez de données valides) et les sépare
    en train/val.
    """
    random.seed(SEED)
    np.random.seed(SEED)

    conn   = sqlite3.connect(DB_PATH)
    df_ok  = pd.read_sql("""
        SELECT code_sta FROM stations_insitu
        WHERE dist_barrage_m >= ? AND lon IS NOT NULL
    """, conn, params=(MIN_DIST_M,))
    conn.close()

    qualified = []
    for sid in df_ok["code_sta"]:
        nc_path = NC_DIR / f"{sid}.nc"
        if not nc_path.exists():
            continue
        try:
            ds    = xr.open_dataset(nc_path)
            wl    = ds["water_level"].values
            ds.close()
            valid = wl[~np.isnan(wl)]
            # Besoin d'assez de points pour construire des séquences ±WINDOW
            if len(valid) >= 2*WINDOW + 100 and np.std(valid) >= MIN_STD:
                qualified.append(sid)
        except Exception:
            continue

    random.shuffle(qualified)
    train_ids = qualified[:N_TRAIN]
    val_ids   = qualified[N_TRAIN:N_TRAIN + N_VAL]
    return train_ids, val_ids


# ─── Dataset ─────────────────────────────────────────────────────────────────
class WaterLevelDataset(Dataset):
    """
    Pour chaque station, génère des échantillons (séquence, statique, cible).

    Séquence de longueur 2*WINDOW+1 centrée sur T :
      positions 0..WINDOW-1  → T-WINDOW .. T-1   (passé)
      position  WINDOW       → T masqué (water_level=0, ERA5 réel)
      positions WINDOW+1..   → T+1 .. T+WINDOW   (futur)

    Cible : water_level(T) réel (normalisé z-score dans le .nc)
    """

    def __init__(self, station_ids):
        self.samples = []
        self._load(station_ids)
        print(f"  Dataset : {len(self.samples)} échantillons "
              f"({len(station_ids)} stations)")

    def _load(self, station_ids):
        for sid in station_ids:
            nc_path = NC_DIR / f"{sid}.nc"
            try:
                ds   = xr.open_dataset(nc_path)
                data = {}
                for v in DYNAMIC_VARS:
                    data[v] = ds[v].values.astype(np.float32) if v in ds \
                              else np.zeros(len(ds["date"]), dtype=np.float32)
                ds.close()
            except Exception:
                continue

            T_total = len(data["water_level"])
            static  = get_static(sid)

            for t in range(WINDOW, T_total - WINDOW):
                wl_t = data["water_level"][t]
                if np.isnan(wl_t):
                    continue

                # Construire la séquence 2*WINDOW+1
                seq = np.zeros((2*WINDOW+1, len(DYNAMIC_VARS)), dtype=np.float32)
                for j, v in enumerate(DYNAMIC_VARS):
                    seq[:WINDOW, j]    = data[v][t-WINDOW:t]     # passé
                    seq[WINDOW+1:, j]  = data[v][t+1:t+WINDOW+1] # futur
                    # Position centrale T
                    seq[WINDOW, j] = 0.0 if v == "water_level" else data[v][t]

                # Ignorer si trop de NaN dans le contexte water_level
                wl_ctx = np.concatenate([
                    data["water_level"][t-WINDOW:t],
                    data["water_level"][t+1:t+WINDOW+1]
                ])
                if np.isnan(wl_ctx).mean() > 0.5:
                    continue

                seq = np.nan_to_num(seq, nan=0.0)
                self.samples.append((seq, static, np.float32(wl_t)))

    def __len__(self):
        return len(self.samples)

    def __getitem__(self, idx):
        seq, static, target = self.samples[idx]
        return (
            torch.tensor(seq,    dtype=torch.float32),
            torch.tensor(static, dtype=torch.float32),
            torch.tensor(target, dtype=torch.float32)
        )