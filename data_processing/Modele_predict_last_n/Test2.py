"""
list_all_gaps_and_poison_windows.py
---------------------------------------
Pour une station donnée, liste TOUS les trous NaN (pas seulement le plus
long) dans chaque variable dynamique, puis simule quelle fraction des
fenêtres glissantes de seq_length jours serait "empoisonnée" (contient
au moins un NaN quelque part dans son historique) -- sous l'hypothèse
qu'un seul NaN dans une variable dynamique (contrairement à l'entrée
autorégressive water_level, qui a un mécanisme de réinjection dédié)
suffit à corrompre toute la fenêtre.

Usage :
    python list_all_gaps_and_poison_windows.py
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd


# ──────────────────────────────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────────────────────────────

DATA_DIR = Path("./data/IA/NeuralHydroDtoD90")
STATION_ID = "J621301002"
SEQ_LENGTH = 365  # tel que lu dans le config.yml

DYNAMIC_INPUTS = [
    "precipitation_J0", "temperature_J0", "pet_J0",
    "precip_mean_J3", "temp_mean_J3", "pet_mean_J3",
    "precip_mean_J10", "temp_mean_J10", "pet_mean_J10",
    "precip_max_J10", "precip_last7", "nb_jours_pluie_J10",
    "snow_depth_J0", "snowmelt_J0", "snow_depth_mean_J3", "snowmelt_mean_J3",
    "snow_depth_mean_J10", "snowmelt_mean_J10",
    "clim_mean_20j", "clim_std_20j",
]


# ──────────────────────────────────────────────────────────────────────────────
# UTILS
# ──────────────────────────────────────────────────────────────────────────────

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


def all_nan_runs(mask: np.ndarray, dates: pd.DatetimeIndex) -> list[tuple]:
    """Retourne TOUS les blocs consécutifs de NaN (pas juste le plus long)."""
    if not mask.any():
        return []
    changes = np.flatnonzero(np.diff(np.concatenate(([0], mask.astype(int), [0]))))
    runs = changes.reshape(-1, 2)
    return [(dates[s].date(), dates[e - 1].date(), e - s) for s, e in runs]


def simulate_poisoned_windows(nan_dates_union: set, all_dates: pd.DatetimeIndex, seq_length: int) -> np.ndarray:
    """
    Pour chaque date d'ancrage possible (jour t), regarde si AU MOINS UN jour
    parmi les seq_length jours précédents (t-seq_length+1 .. t) fait partie
    de nan_dates_union. Retourne un masque booléen (True = fenêtre empoisonnée).
    """
    n = len(all_dates)
    is_nan_day = np.array([d in nan_dates_union for d in all_dates])
    poisoned = np.zeros(n, dtype=bool)

    # Approche par fenêtre glissante cumulative (efficace) :
    # une fenêtre ancrée à t est empoisonnée si is_nan_day.any() sur [t-seq_length+1, t]
    cumsum = np.concatenate(([0], np.cumsum(is_nan_day.astype(int))))
    for t in range(n):
        start = max(0, t - seq_length + 1)
        count_nan_in_window = cumsum[t + 1] - cumsum[start]
        poisoned[t] = count_nan_in_window > 0

    return poisoned


# ──────────────────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────────────────

def main():
    df = load_station_timeseries(DATA_DIR, STATION_ID)
    print(f"[INFO] Station {STATION_ID} — {len(df)} lignes, {df.index.min()} -> {df.index.max()}")

    all_dates = df.index
    nan_dates_union = set()

    print("\n" + "=" * 90)
    print("TOUS LES TROUS PAR VARIABLE (pas seulement le plus long)")
    print("=" * 90)
    total_nan_days_any_var = 0
    for var in DYNAMIC_INPUTS:
        if var not in df.columns:
            continue
        nan_mask = df[var].isna().values
        runs = all_nan_runs(nan_mask, all_dates)
        if not runs:
            continue
        print(f"\n{var} — {len(runs)} trou(s), total {nan_mask.sum()} jours NaN :")
        for start, end, length in runs[:20]:  # limite l'affichage si trop nombreux
            print(f"    {start} -> {end}  ({length}j)")
        if len(runs) > 20:
            print(f"    ... et {len(runs) - 20} autres trous")

        nan_indices = np.where(nan_mask)[0]
        nan_dates_union.update(all_dates[nan_indices])

    print(f"\n[INFO] Nombre total de jours NaN uniques (union de TOUTES les variables météo) : "
          f"{len(nan_dates_union)} / {len(all_dates)} ({100*len(nan_dates_union)/len(all_dates):.2f}%)")

    # Simulation de l'empoisonnement des fenêtres
    print("\n" + "=" * 90)
    print(f"SIMULATION : fraction de fenêtres de {SEQ_LENGTH}j empoisonnées par au moins 1 NaN météo")
    print("=" * 90)
    poisoned = simulate_poisoned_windows(nan_dates_union, all_dates, SEQ_LENGTH)
    pct_poisoned = 100 * poisoned.mean()
    print(f"% de dates d'ancrage dont la fenêtre contient au moins 1 jour NaN météo : {pct_poisoned:.2f}%")
    print(f"(à comparer avec le %NaN observé dans les VRAIES prédictions du modèle pour cette station)")

    if pct_poisoned > 40:
        print("\n[CONCLUSION PROBABLE] Le cumul de plusieurs petits trous météo, combiné à une fenêtre "
              f"de {SEQ_LENGTH} jours, suffit à expliquer une large fraction de prédictions NaN -- "
              "cohérent avec l'hypothèse qu'un NaN non géré dans une variable dynamique (contrairement "
              "à water_level qui a sa propre réinjection) corrompt toute la fenêtre.")
    else:
        print("\n[CONCLUSION] Le cumul des trous météo ne suffit PAS à expliquer le taux de NaN observé "
              "dans les prédictions -> l'hypothèse est probablement fausse, chercher ailleurs.")


if __name__ == "__main__":
    main()