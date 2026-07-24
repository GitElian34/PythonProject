"""
diagnose_train_usable_data.py
---------------------------------
Pour TOUTES les stations du train_basins.txt, calcule quelle fraction des
dates théoriquement disponibles seraient réellement EXPLOITABLES comme
échantillon d'entraînement, sous l'hypothèse (confirmée pour la validation,
présumée identique pour l'entraînement puisque c'est la même construction
de fenêtre glissante) qu'un seul NaN dans une variable dynamique, n'importe
où dans les seq_length jours précédents une date candidate, rend cette
date inutilisable.

Objectif : quantifier combien de données d'entraînement sont perdues à
cause de l'absence de gestion des NaN (nan_handling_method), sur
l'ensemble du jeu d'entraînement plutôt que sur quelques stations isolées.

Usage :
    Ajuster DATA_DIR, TRAIN_BASIN_FILE, SEQ_LENGTH, DYNAMIC_INPUTS puis :
    python diagnose_train_usable_data.py
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd


# ──────────────────────────────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────────────────────────────

DATA_DIR = Path("./data/IA/NeuralHydroDtoD90")
TRAIN_BASIN_FILE = Path("./AI/LSTM/NeuralHydroDtoD90/train_basins.txt")
SEQ_LENGTH = 365

DYNAMIC_INPUTS = [
    "precipitation_J0", "temperature_J0", "pet_J0",
    "precip_mean_J3", "temp_mean_J3", "pet_mean_J3",
    "precip_mean_J10", "temp_mean_J10", "pet_mean_J10",
    "precip_max_J10", "precip_last7", "nb_jours_pluie_J10",
    "snow_depth_J0", "snowmelt_J0", "snow_depth_mean_J3", "snowmelt_mean_J3",
    "snow_depth_mean_J10", "snowmelt_mean_J10",
    "clim_mean_20j", "clim_std_20j",
]

# Optionnel : limiter le train_start_date / train_end_date si connu, pour ne
# considérer que la période réellement utilisée à l'entraînement (sinon on
# utilise toute la période disponible dans le fichier).
TRAIN_START = None  # ex: "2016-01-01"
TRAIN_END = None    # ex: "2023-12-31"

OUT_CSV = Path("./data_processing/predict_last_n_comparison/train_usable_data_diagnosis.csv")
OUT_CSV.parent.mkdir(parents=True, exist_ok=True)


# ──────────────────────────────────────────────────────────────────────────────
# UTILS
# ──────────────────────────────────────────────────────────────────────────────

def load_basin_list(path: Path) -> list:
    with open(path, "r") as f:
        return [line.strip() for line in f if line.strip()]


def load_station_timeseries(data_dir: Path, station_id: str) -> pd.DataFrame:
    ts_dir = data_dir / "time_series"
    nc_path = ts_dir / f"{station_id}.nc"
    csv_path = ts_dir / f"{station_id}.csv"
    if nc_path.exists():
        import xarray as xr
        return xr.open_dataset(nc_path).to_dataframe()
    elif csv_path.exists():
        return pd.read_csv(csv_path, parse_dates=[0], index_col=0)
    else:
        raise FileNotFoundError(f"Ni {nc_path} ni {csv_path} n'existent.")


def simulate_poisoned_windows(nan_dates_union: set, all_dates: pd.DatetimeIndex, seq_length: int) -> np.ndarray:
    n = len(all_dates)
    is_nan_day = np.array([d in nan_dates_union for d in all_dates])
    cumsum = np.concatenate(([0], np.cumsum(is_nan_day.astype(int))))
    poisoned = np.zeros(n, dtype=bool)
    for t in range(n):
        start = max(0, t - seq_length + 1)
        poisoned[t] = (cumsum[t + 1] - cumsum[start]) > 0
    return poisoned


# ──────────────────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────────────────

def main():
    if not TRAIN_BASIN_FILE.exists():
        sys.exit(f"[ERREUR] Fichier introuvable : {TRAIN_BASIN_FILE}")

    basins = load_basin_list(TRAIN_BASIN_FILE)
    print(f"[INFO] {len(basins)} stations dans {TRAIN_BASIN_FILE}")

    rows = []
    for i, station_id in enumerate(basins):
        try:
            df = load_station_timeseries(DATA_DIR, station_id)
        except Exception as e:
            print(f"[WARNING] {station_id} : impossible de charger ({e})")
            continue

        if TRAIN_START:
            df = df[df.index >= pd.to_datetime(TRAIN_START)]
        if TRAIN_END:
            df = df[df.index <= pd.to_datetime(TRAIN_END)]

        if df.empty:
            continue

        all_dates = df.index
        nan_dates_union = set()
        for var in DYNAMIC_INPUTS:
            if var not in df.columns:
                continue
            nan_mask = df[var].isna().values
            nan_indices = np.where(nan_mask)[0]
            nan_dates_union.update(all_dates[nan_indices])

        pct_raw_nan = 100 * len(nan_dates_union) / len(all_dates)

        poisoned = simulate_poisoned_windows(nan_dates_union, all_dates, SEQ_LENGTH)
        pct_poisoned = 100 * poisoned.mean()
        pct_usable = 100 - pct_poisoned

        rows.append({
            "station": station_id,
            "n_dates": len(all_dates),
            "pct_raw_nan_dynamic": pct_raw_nan,
            "pct_windows_poisoned": pct_poisoned,
            "pct_windows_usable": pct_usable,
        })

        if (i + 1) % 20 == 0:
            print(f"[INFO] {i + 1}/{len(basins)} stations traitées...")

    df_out = pd.DataFrame(rows)
    df_out.to_csv(OUT_CSV, index=False)

    print("\n" + "=" * 90)
    print(f"RÉSUMÉ — {len(df_out)} stations d'entraînement analysées")
    print("=" * 90)
    print(f"% brut de NaN dans les variables dynamiques :")
    print(f"    médiane = {df_out['pct_raw_nan_dynamic'].median():.2f}%   "
          f"moyenne = {df_out['pct_raw_nan_dynamic'].mean():.2f}%")
    print(f"\n% de fenêtres de {SEQ_LENGTH}j EXPLOITABLES pour l'entraînement (estimation) :")
    print(f"    médiane = {df_out['pct_windows_usable'].median():.2f}%   "
          f"moyenne = {df_out['pct_windows_usable'].mean():.2f}%")
    print(f"\nStations avec <50% de données exploitables : "
          f"{(df_out['pct_windows_usable'] < 50).sum()} / {len(df_out)}")
    print(f"Stations avec <10% de données exploitables : "
          f"{(df_out['pct_windows_usable'] < 10).sum()} / {len(df_out)}")
    print(f"Stations avec >90% de données exploitables : "
          f"{(df_out['pct_windows_usable'] > 90).sum()} / {len(df_out)}")

    print("\n" + "=" * 90)
    print("10 STATIONS LES PLUS TOUCHÉES (le moins de données exploitables)")
    print("=" * 90)
    print(
        df_out.sort_values("pct_windows_usable").head(10)
        .to_string(index=False, float_format=lambda x: f"{x:.2f}")
    )

    print(f"\n[OK] Détail complet sauvegardé : {OUT_CSV}")
    print("=" * 90)


if __name__ == "__main__":
    main()