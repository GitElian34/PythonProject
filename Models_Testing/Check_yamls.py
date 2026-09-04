#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
check_nc_dataset.py — Vérifie que les .nc d'un dossier NeuralHydrology
correspondent à ce qu'attend un config YAML donné (dynamic_inputs,
target_variables, static_attributes, frequence, nombre de stations).

Usage :
    python check_nc_dataset.py --data-dir /path/to/27j --config /path/to/model.yml
"""

import argparse
import os
import sys
import glob

import numpy as np
import xarray as xr
import yaml
import pandas as pd


def load_yaml_config(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def check_dataset(data_dir: str, config_path: str, n_sample: int = 5):
    cfg = load_yaml_config(config_path)

    dynamic_inputs = cfg.get("dynamic_inputs", [])
    target_variables = cfg.get("target_variables", [])
    static_attributes = cfg.get("static_attributes", [])
    use_frequencies = cfg.get("use_frequencies", [])
    seq_length = cfg.get("seq_length", {})
    predict_last_n = cfg.get("predict_last_n", {})
    lagged_features = cfg.get("lagged_features", {})
    autoregressive_inputs = cfg.get("autoregressive_inputs", [])
    n_basins_expected = cfg.get("number_of_basins")

    print("=" * 70)
    print("CONFIG YAML ATTENDUE")
    print("=" * 70)
    print(f"dynamic_inputs ({len(dynamic_inputs)}) : {dynamic_inputs}")
    print(f"target_variables : {target_variables}")
    print(f"static_attributes ({len(static_attributes)}) : {static_attributes}")
    print(f"use_frequencies : {use_frequencies}")
    print(f"seq_length : {seq_length}")
    print(f"predict_last_n : {predict_last_n}")
    print(f"lagged_features : {lagged_features}")
    print(f"autoregressive_inputs : {autoregressive_inputs}")
    print(f"number_of_basins (yaml) : {n_basins_expected}")
    print()

    ts_dir = os.path.join(data_dir, "time_series")
    attr_dir = os.path.join(data_dir, "attributes")

    if not os.path.isdir(ts_dir):
        # certains layouts mettent les .nc directement dans data_dir
        ts_dir = data_dir

    nc_files = sorted(glob.glob(os.path.join(ts_dir, "*.nc")))
    print("=" * 70)
    print(f"FICHIERS .nc TROUVÉS dans {ts_dir}")
    print("=" * 70)
    print(f"Nombre de .nc : {len(nc_files)}")
    if n_basins_expected is not None:
        status = "OK" if len(nc_files) == n_basins_expected else "MISMATCH"
        print(f"  vs number_of_basins yaml ({n_basins_expected}) : {status}")
    print()

    if not nc_files:
        print("AUCUN FICHIER .nc TROUVÉ — vérifier le chemin --data-dir")
        return

    # ── Vérification sur un échantillon de fichiers ──────────────────
    print("=" * 70)
    print(f"VÉRIFICATION SUR {min(n_sample, len(nc_files))} FICHIERS ÉCHANTILLON")
    print("=" * 70)

    all_vars_seen = set()
    freq_issues = []
    missing_vars_per_file = {}

    for fpath in nc_files[:n_sample]:
        code = os.path.basename(fpath).replace(".nc", "")
        ds = xr.open_dataset(fpath)
        vars_present = set(ds.data_vars) | set(ds.coords)
        all_vars_seen |= vars_present

        print(f"\n--- {code} ---")
        print(f"  Dimensions : {dict(ds.sizes)}")
        print(f"  Variables  : {sorted(ds.data_vars)}")

        # Date range + frequence
        time_dim = "date" if "date" in ds.coords else ("time" if "time" in ds.coords else None)
        if time_dim:
            dates = pd.to_datetime(ds[time_dim].values)
            if len(dates) > 1:
                diffs = np.diff(dates)
                unique_diffs = pd.unique(diffs)
                print(f"  Plage dates : {dates.min()} -> {dates.max()} ({len(dates)} pas)")
                print(f"  Pas de temps observés : {[str(d) for d in unique_diffs[:5]]}"
                      + (" ..." if len(unique_diffs) > 5 else ""))
                if len(unique_diffs) > 1:
                    freq_issues.append(code)
        else:
            print("  ATTENTION: pas de dimension 'date'/'time' trouvée")

        # Variables manquantes vs attendu
        expected = set(dynamic_inputs) | set(target_variables)
        missing = expected - vars_present
        if missing:
            missing_vars_per_file[code] = missing
            print(f"  MANQUANT : {missing}")
        else:
            print(f"  OK : toutes les dynamic_inputs + target_variables présentes")

        # Vérif eventuelle colonne water_level_shift1 (normalement pas nécessaire)
        if "water_level_shift1" in vars_present:
            print(f"  Note : 'water_level_shift1' déjà présent dans le .nc "
                  f"(normalement généré par NeuralHydrology via lagged_features, pas requis en amont)")

        # NaN check sur target
        for tv in target_variables:
            if tv in ds.data_vars:
                n_nan = int(ds[tv].isnull().sum())
                n_total = ds[tv].size
                print(f"  {tv} : {n_nan}/{n_total} NaN ({100*n_nan/n_total:.1f}%)")

        ds.close()

    # ── Résumé ─────────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("RÉSUMÉ")
    print("=" * 70)

    extra_vars = all_vars_seen - set(dynamic_inputs) - set(target_variables) - {"date", "time"}
    missing_globally = (set(dynamic_inputs) | set(target_variables)) - all_vars_seen

    if missing_globally:
        print(f"⚠️  Variables attendues jamais trouvées : {missing_globally}")
    else:
        print("✅ Toutes les dynamic_inputs + target_variables sont présentes (sur l'échantillon)")

    if extra_vars:
        print(f"ℹ️  Variables en plus (non utilisées par le yaml, pas grave) : {extra_vars}")

    if missing_vars_per_file:
        print(f"⚠️  {len(missing_vars_per_file)}/{n_sample} fichiers ont des variables manquantes")

    if freq_issues:
        print(f"⚠️  Fréquence temporelle irrégulière détectée dans : {freq_issues}")
    else:
        print("✅ Fréquence temporelle régulière sur l'échantillon")

    # ── attributes.csv ────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("VÉRIFICATION attributes.csv")
    print("=" * 70)
    attr_csv = os.path.join(attr_dir, "attributes.csv")
    if os.path.exists(attr_csv):
        attrs = pd.read_csv(attr_csv)
        print(f"Fichier trouvé : {attr_csv}")
        print(f"Colonnes : {list(attrs.columns)}")
        print(f"Nb lignes (stations) : {len(attrs)}")

        missing_attrs = set(static_attributes) - set(attrs.columns)
        if missing_attrs:
            print(f"⚠️  static_attributes manquants dans attributes.csv : {missing_attrs}")
        else:
            print("✅ Tous les static_attributes sont présents dans attributes.csv")

        n_nan_attrs = attrs[list(set(static_attributes) & set(attrs.columns))].isnull().sum()
        if n_nan_attrs.sum() > 0:
            print(f"⚠️  NaN dans les attributs statiques :\n{n_nan_attrs[n_nan_attrs > 0]}")
    else:
        print(f"⚠️  attributes.csv INTROUVABLE à {attr_csv} — vérifier le chemin")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Vérifie un dataset .nc vs un config YAML NeuralHydrology")
    parser.add_argument("--data-dir", type=str, required=True,
                        help="Dossier data_dir du yaml (contenant time_series/ et attributes/)")
    parser.add_argument("--config", type=str, required=True, help="Chemin vers le fichier .yml du modèle")
    parser.add_argument("--n-sample", type=int, default=5, help="Nombre de .nc à inspecter en détail")
    args = parser.parse_args()

    check_dataset(args.data_dir, args.config, n_sample=args.n_sample)