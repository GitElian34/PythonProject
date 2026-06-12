"""
prepare_finetune_split.py
─────────────────────────
1. Lit stations_27j.txt (137 stations satellite)
2. Split aléatoire reproductible : 100 fine-tuning / 37 test held-out
3. Écrit finetune_100.txt et test_37.txt
4. Génère la grille de YAMLs (LR × epochs) pour nh_run continue_training

Approche : on utilise "nh_run continue_training" avec checkpoint_path
pour charger le .pt exact voulu, sans toucher au source de NeuralHydrology.
Le YAML override data_dir, basin_files, LR, epochs par rapport au run insitu.

Usage :
    python prepare_finetune_split.py
"""

import random
import os
import copy
import yaml

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION — À MODIFIER
# ═══════════════════════════════════════════════════════════════════════════════

# Fichier listant les stations satellite 27j
STATIONS_FILE = "./AI/LSTM/NeuralHydro_satellite_27D/stations_27j.txt"

# Dossier du run insitu de référence
BASE_RUN_DIR = "./runs/arlstm_feat27jHigh_modele2_1805_111000"

# Epoch exact à charger comme point de départ
BASE_EPOCH = 11

# Dossier de sortie
OUT_DIR = "./finetuning"

# Données satellite
SATELLITE_DATA_DIR = "./data/IA/NeuralHydrology_satellite_27D"

# Split
SEED       = 42
N_FINETUNE = 100

# ═══════════════════════════════════════════════════════════════════════════════
# GRILLE — LR × EPOCHS
# ═══════════════════════════════════════════════════════════════════════════════

LR_VALUES    = [1e-5, 5e-5, 1e-4]
EPOCH_VALUES = [3, 5, 10]

# ═══════════════════════════════════════════════════════════════════════════════
# YAML DE BASE (override par rapport au run insitu)
# ═══════════════════════════════════════════════════════════════════════════════

BASE_YAML = {
    "dataset":  "generic",
    "data_dir": SATELLITE_DATA_DIR,

    "train_start_date":      "01/01/2016",
    "train_end_date":        "31/12/2025",
    "validation_start_date": "01/01/2016",
    "validation_end_date":   "31/12/2025",

    "target_variables":      ["water_level"],
    "lagged_features":       {"water_level": [1]},
    "use_frequencies":       ["27D"],
    "autoregressive_inputs": ["water_level_shift1"],

    "dynamic_inputs": [
        "precipitation_J0", "temperature_J0", "pet_J0",
        "precip_mean_J3",   "pet_mean_J3",    "temp_mean_J3",
        "precip_mean_J27",  "precip_mean_J10", "temp_mean_J10",
        "clim_mean_20j",    "clim_std_20j",
        "precip_max_J27",   "precip_last7",
        "nb_jours_pluie_J27", "precip_mean_J14",
    ],

    "static_attributes": [
        "aire_km2", "lon", "lat",
        "frac_urban", "frac_forest", "frac_agriculture",
        "sg_clay_0_30cm", "sg_sand_0_30cm", "sg_silt_0_30cm",
        "strahler", "elevation_mean", "slope_mean",
    ],

    "model":          "arlstm",
    "head":           "regression",
    "hidden_size":    256,
    "seq_length":     {"27D": 20},
    "predict_last_n": {"27D": 1},

    "batch_size":            32,
    "output_dropout":        0.4,
    "clip_gradient_norm":    1.0,
    "target_noise_std":      0.005,
    "optimizer":             "AdamW",
    "loss":                  "NSE",
    "initial_forget_bias":   3,
    "save_validation_results": True,
    "max_updates_per_epoch": 300,

    "validate_every":           1,
    "validate_n_random_basins": 2000,
    "metrics": ["NSE", "KGE"],

    "device": "cpu",
}


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    basins_dir = os.path.join(OUT_DIR, "basins")
    yamls_dir  = os.path.join(OUT_DIR, "yamls")
    os.makedirs(basins_dir, exist_ok=True)
    os.makedirs(yamls_dir,  exist_ok=True)

    # ── 1. Lecture des stations ──────────────────────────────────────────────
    with open(STATIONS_FILE) as f:
        stations = [l.strip() for l in f if l.strip()]
    print(f"Stations lues : {len(stations)}")

    # ── 2. Split aléatoire reproductible ────────────────────────────────────
    random.seed(SEED)
    shuffled = stations[:]
    random.shuffle(shuffled)
    ft_stations   = shuffled[:N_FINETUNE]
    test_stations = shuffled[N_FINETUNE:]
    print(f"  Fine-tuning  : {len(ft_stations)} stations")
    print(f"  Test held-out: {len(test_stations)} stations")

    # ── 3. Fichiers de basins ────────────────────────────────────────────────
    ft_path   = os.path.join(basins_dir, "finetune_100.txt")
    test_path = os.path.join(basins_dir, "test_37.txt")
    with open(ft_path,   "w") as f:
        f.write("\n".join(ft_stations) + "\n")
    with open(test_path, "w") as f:
        f.write("\n".join(test_stations) + "\n")
    print(f"\nFichiers écrits : {ft_path}  |  {test_path}")

    # ── 4. Grille de YAMLs ───────────────────────────────────────────────────
    print(f"\nGénération de la grille ({len(LR_VALUES)} LR × {len(EPOCH_VALUES)} epochs) :")
    generated = []

    checkpoint_path = f"{BASE_RUN_DIR}/model_epoch{BASE_EPOCH:03d}.pt"

    for lr in LR_VALUES:
        for n_epochs in EPOCH_VALUES:

            lr_str = f"{lr:.0e}".replace("e-0", "e-").replace("e+0", "e")
            name   = f"finetune_lr{lr_str}_ep{n_epochs}"

            cfg = copy.deepcopy(BASE_YAML)
            cfg["experiment_name"]  = name
            cfg["checkpoint_path"]  = checkpoint_path  # ← charge le .pt exact

            cfg["train_basin_file"]      = ft_path
            cfg["validation_basin_file"] = test_path
            cfg["test_basin_file"]       = test_path

            cfg["learning_rate"] = {0: lr}
            cfg["epochs"]        = n_epochs

            yaml_path = os.path.join(yamls_dir, f"{name}.yml")
            with open(yaml_path, "w") as f:
                yaml.dump(cfg, f, default_flow_style=False,
                          sort_keys=False, allow_unicode=True)

            generated.append((name, yaml_path))
            print(f"  {name}")

    # ── 5. Script de lancement ───────────────────────────────────────────────
    launch_path = os.path.join(OUT_DIR, "run_grid.sh")
    with open(launch_path, "w") as f:
        f.write("#!/bin/bash\n")
        f.write("# Fine-tuning via nh_run continue_training + checkpoint_path\n")
        f.write("# Usage : bash finetuning/run_grid.sh\n\n")
        for name, yp in generated:
            f.write(f"echo '>>> {name}'\n")
            f.write(f"python -m neuralhydrology.nh_run continue_training "
                    f"--run-dir {BASE_RUN_DIR} --config-file {yp}\n\n")
    os.chmod(launch_path, 0o755)

    # ── 6. Récapitulatif ─────────────────────────────────────────────────────
    print(f"\n{'═'*65}")
    print("RÉCAPITULATIF")
    print(f"{'═'*65}")
    print(f"  Seed          : {SEED}")
    print(f"  Fine-tuning   : {len(ft_stations)} stations → finetune_100.txt")
    print(f"  Test held-out : {len(test_stations)} stations → test_37.txt")
    print(f"  Checkpoint    : {checkpoint_path}")
    print(f"  Runs          : {len(generated)}")
    print(f"  Lancement     : bash {launch_path}")
    print(f"\n  ⚠  Vérifie que {checkpoint_path} existe bien")


if __name__ == "__main__":
    main()