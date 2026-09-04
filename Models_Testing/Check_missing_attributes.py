"""
check_missing_attributes.py — Diagnostique "Some basins are missing static attributes"

Compare les stations listées dans train_basin_file / validation_basin_file
avec celles présentes dans attributes.csv, et vérifie les NaN sur les
colonnes static_attributes du YAML.

Usage :
    python check_missing_attributes.py --data-dir data/IA/NeuralHydrology_feat27j \
        --train-basins AI/LSTM/NeuralHydro_feat27j_high/train_basins.txt \
        --val-basins AI/LSTM/NeuralHydro_feat27j_high/val_basins.txt
"""

import argparse
import pandas as pd
from pathlib import Path

STATIC_ATTRIBUTES = [
    "aire_km2", "lon", "lat",
    "frac_urban", "frac_forest", "frac_agriculture",
    "sg_clay_0_30cm", "sg_sand_0_30cm", "sg_silt_0_30cm",
    "strahler", "elevation_mean", "slope_mean",
]


def load_basin_list(path):
    with open(path) as f:
        return [line.strip() for line in f if line.strip()]


def main(data_dir, train_basins_path, val_basins_path):
    attr_path = Path(data_dir) / "attributes" / "attributes.csv"
    if not attr_path.exists():
        # certains layouts mettent attributes.csv directement dans data_dir
        attr_path = Path(data_dir) / "attributes.csv"

    print(f"Fichier attributs : {attr_path}")
    if not attr_path.exists():
        print("⚠ INTROUVABLE — vérifie --data-dir")
        return

    attrs = pd.read_csv(attr_path, dtype={"station_id": str})
    attrs = attrs.set_index("station_id")
    print(f"  {len(attrs)} stations dans attributes.csv")
    print(f"  Colonnes : {list(attrs.columns)}\n")

    missing_cols = [c for c in STATIC_ATTRIBUTES if c not in attrs.columns]
    if missing_cols:
        print(f"⚠ COLONNES ENTIÈREMENT ABSENTES du fichier : {missing_cols}\n")

    for label, path in [("TRAIN", train_basins_path), ("VALIDATION", val_basins_path)]:
        if not path:
            continue
        print(f"--- {label} ({path}) ---")
        basins = load_basin_list(path)
        print(f"  {len(basins)} stations dans la liste")

        basins_missing_entirely = [b for b in basins if b not in attrs.index]
        print(f"  Stations ABSENTES d'attributes.csv : {len(basins_missing_entirely)}")
        if basins_missing_entirely:
            print(f"    Exemples : {basins_missing_entirely[:10]}")

        basins_present = [b for b in basins if b in attrs.index]
        if basins_present:
            sub = attrs.loc[basins_present]
            present_cols = [c for c in STATIC_ATTRIBUTES if c in sub.columns]
            nan_counts = sub[present_cols].isnull().sum()
            nan_counts = nan_counts[nan_counts > 0]
            if len(nan_counts):
                print(f"  Colonnes avec des NaN parmi les stations présentes :")
                print(nan_counts.to_string())
                # Stations concernées
                rows_with_nan = sub[sub[present_cols].isnull().any(axis=1)]
                print(f"  -> {len(rows_with_nan)} stations avec au moins 1 NaN")
                print(f"     Exemples : {rows_with_nan.index[:10].tolist()}")
            else:
                print("  Aucun NaN sur les static_attributes pour ces stations")
        print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", required=True)
    parser.add_argument("--train-basins", default=None)
    parser.add_argument("--val-basins", default=None)
    args = parser.parse_args()
    main(args.data_dir, args.train_basins, args.val_basins)