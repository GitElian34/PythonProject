"""
data_loader.py
==============
Chargement des fichiers .nc et construction du DataFrame pandas pour le TFT.

Deux populations de stations :
  - In-situ  : données fiables → train / val
  - Altimétriques (136) : cible de la détection → inférence uniquement
"""

import os
import glob
import logging
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import netCDF4 as nc
import yaml
from tqdm import tqdm

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def load_config(config_path: str) -> dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def load_altimetric_ids(config: dict) -> set:
    """Charge la liste des IDs de stations altimétriques depuis le fichier texte."""
    path = config["data"].get("altimetric_stations_file", None)
    if path is None or not os.path.exists(path):
        logger.warning("Fichier stations altimétriques introuvable — aucune station exclue du train.")
        return set()
    with open(path, "r") as f:
        ids = {line.strip() for line in f if line.strip()}
    logger.info(f"{len(ids)} stations altimétriques chargées depuis {path}")
    return ids


def nc_to_dataframe(nc_path: str, config: dict) -> pd.DataFrame | None:
    """Charge un fichier .nc et retourne un DataFrame. None si invalide."""
    cfg_data = config["data"]
    target = cfg_data["target"]
    dynamic_cols = cfg_data["dynamic_inputs"] + cfg_data["known_inputs"]
    date_origin = datetime.strptime(cfg_data["date_origin"], "%Y-%m-%d")

    try:
        ds = nc.Dataset(nc_path)
    except Exception as e:
        logger.warning(f"Impossible d'ouvrir {nc_path} : {e}")
        return None

    station_id = os.path.splitext(os.path.basename(nc_path))[0]
    dates_days = ds.variables["date"][:]
    n = len(dates_days)

    min_length = config["model"]["encoder_length"] + config["model"]["prediction_length"]
    if n < min_length:
        ds.close()
        return None

    dates = [date_origin + timedelta(days=int(d)) for d in dates_days]
    rows = {"station_id": station_id, "date": dates, "time_idx": list(range(n))}

    if target in ds.variables:
        rows[target] = np.array(ds.variables[target][:], dtype=np.float32)
    else:
        logger.warning(f"{station_id} : variable cible '{target}' absente")
        ds.close()
        return None

    for col in dynamic_cols:
        if col in ds.variables:
            rows[col] = np.array(ds.variables[col][:], dtype=np.float32)
        else:
            rows[col] = np.full(n, np.nan, dtype=np.float32)

    ds.close()
    df = pd.DataFrame(rows)

    # Filtre : trop de NaN sur la cible (> 30%)
    if df[target].isna().mean() > 0.30:
        return None

    # Interpolation linéaire sur les NaN résiduels
    df[dynamic_cols] = df[dynamic_cols].interpolate(method="linear", limit_direction="both")
    df[target] = df[target].interpolate(method="linear", limit_direction="both")

    return df


def load_attributes(attributes_path: str, static_cols: list) -> pd.DataFrame:
    """Charge le CSV des attributs statiques."""
    attrs = pd.read_csv(attributes_path)
    attrs = attrs.rename(columns={attrs.columns[0]: "station_id"})
    missing = [c for c in static_cols if c not in attrs.columns]
    if missing:
        logger.warning(f"Attributs statiques manquants : {missing}")
    available = [c for c in static_cols if c in attrs.columns]
    return attrs[["station_id"] + available].set_index("station_id")


def split_stations(station_ids: list, config: dict) -> tuple[list, list]:
    """
    Split train/val sur les stations in-situ uniquement.
    Retourne (train_ids, val_ids) — pas de test_ids car les altimétriques jouent ce rôle.
    """
    cfg = config["data"]
    rng = np.random.default_rng(cfg["seed"])
    ids = np.array(station_ids)
    rng.shuffle(ids)

    n = len(ids)
    n_train = int(n * cfg["train_ratio"])

    train_ids = ids[:n_train].tolist()
    val_ids   = ids[n_train:].tolist()

    logger.info(f"Split in-situ — train: {len(train_ids)}, val: {len(val_ids)}")
    return train_ids, val_ids


def build_dataframe(config: dict, max_stations: int = None,
                    include_altimetric: bool = False) -> tuple[pd.DataFrame, set]:
    """
    Charge toutes les stations et construit le DataFrame global.

    Args:
        config             : dictionnaire de configuration
        max_stations       : limite le nb de stations in-situ (debug)
        include_altimetric : si True, charge aussi les stations altimétriques

    Returns:
        df          : DataFrame complet
        altimetric_ids : set des IDs altimétriques chargés
    """
    cfg = config["data"]
    ts_dir = cfg["time_series_dir"]
    static_cols = cfg["static_inputs"]

    # Chargement attributs statiques
    logger.info("Chargement des attributs statiques...")
    attrs_df = load_attributes(cfg["attributes_path"], static_cols)
    available_static = [c for c in static_cols if c in attrs_df.columns]

    # Identification des stations altimétriques
    altimetric_ids = load_altimetric_ids(config)

    # Liste des fichiers .nc
    all_nc = sorted(glob.glob(os.path.join(ts_dir, "*.nc")))

    # Séparation in-situ / altimétrique
    insitu_nc = [f for f in all_nc
                 if os.path.splitext(os.path.basename(f))[0] not in altimetric_ids]
    alti_nc   = [f for f in all_nc
                 if os.path.splitext(os.path.basename(f))[0] in altimetric_ids]

    if max_stations:
        insitu_nc = insitu_nc[:max_stations]

    logger.info(f"Stations in-situ  : {len(insitu_nc)}")
    logger.info(f"Stations altimétriques : {len(alti_nc)}")

    # max_stations=0 => on ne charge que les altimétriques (si include_altimetric=True)
    if max_stations == 0:
        files_to_load = alti_nc if include_altimetric else []
    else:
        files_to_load = insitu_nc + (alti_nc if include_altimetric else [])

    # Chargement
    dfs = []
    skipped = 0
    for nc_path in tqdm(files_to_load, desc="Chargement séries temporelles"):
        df = nc_to_dataframe(nc_path, config)
        if df is None:
            skipped += 1
            continue

        station_id = df["station_id"].iloc[0]
        if station_id in attrs_df.index:
            for col in available_static:
                df[col] = attrs_df.loc[station_id, col]
        else:
            for col in available_static:
                df[col] = np.nan

        dfs.append(df)

    logger.info(f"Stations chargées : {len(dfs)} | ignorées : {skipped}")

    if not dfs:
        raise ValueError("Aucune série temporelle valide chargée.")

    full_df = pd.concat(dfs, ignore_index=True)

    # Normalisation z-score des attributs statiques
    for col in available_static:
        mean = full_df[col].mean()
        std  = full_df[col].std()
        if std > 0:
            full_df[col] = (full_df[col] - mean) / std

    logger.info(f"DataFrame final : {len(full_df)} lignes, {len(full_df.columns)} colonnes")
    return full_df, altimetric_ids


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="tft_config.yaml")
    parser.add_argument("--max_stations", type=int, default=None)
    parser.add_argument("--output", default="./outputs/tft/full_dataframe.parquet")
    args = parser.parse_args()

    config = load_config(args.config)
    os.makedirs(os.path.dirname(args.output), exist_ok=True)

    df, alti_ids = build_dataframe(config, max_stations=args.max_stations,
                                   include_altimetric=True)
    df.to_parquet(args.output, index=False)
    logger.info(f"DataFrame sauvegardé : {args.output}")
    print(f"\nIn-situ  : {(~df['station_id'].isin(alti_ids)).sum()} lignes")
    print(f"Altimétriques : {df['station_id'].isin(alti_ids).sum()} lignes")