#!/usr/bin/env python3
"""
config.py — Tous les paramètres du Bi-LSTM au même endroit
"""
from pathlib import Path

# ─── Chemins ────────────────────────────────────────────────────────────────
NC_DIR    = Path("./data/IA/NeuralHydrology/time_series")
ATTRS_CSV = Path("./data/IA/NeuralHydrology/attributes/attributes.csv")
DB_PATH   = "./data/insitu_data.db"
MODEL_DIR = Path("./runs/bilstm")

# ─── Stations ────────────────────────────────────────────────────────────────
N_TRAIN    = 160
N_VAL      = 40
SEED       = 42
MIN_DIST_M = 500       # distance minimale au barrage
MIN_STD    = 0.05      # std minimale du water_level (exclut signaux plats)

# ─── Séquences ───────────────────────────────────────────────────────────────
WINDOW  = 180           # ±30 jours autour de T
SEQ_LEN = 2*WINDOW+1   # 61 pas de temps (T inclus mais masqué)

# ─── Variables ───────────────────────────────────────────────────────────────
DYNAMIC_VARS = ["precipitation", "temperature", "pet", "water_level"]
STATIC_COLS  = [
    "aire_km2", "lon", "lat",
    "frac_urban", "frac_agriculture", "frac_forest",
    "frac_semi_natural", "frac_wetland", "frac_water",
    "sg_clay_0_30cm", "sg_sand_0_30cm", "sg_silt_0_30cm",
    "strahler"
]

# ─── Hyperparamètres modèle ──────────────────────────────────────────────────
HIDDEN_SIZE = 128
N_LAYERS    = 2
DROPOUT     = 0.4

# ─── Entraînement ────────────────────────────────────────────────────────────
BATCH_SIZE  = 256
EPOCHS      = 30
LR          = 1e-3
LR_DECAY_EP = 20       # réduction LR x0.1 à cette epoch