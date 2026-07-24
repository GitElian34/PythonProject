"""
inspect_consecutive_windows.py
--------------------------------
Pour UNE station donnée (aléatoire ou fixée), extrait plusieurs fenêtres
de prédiction CONSÉCUTIVES (dates d'ancrage voisines) du pickle de
validation, et les affiche sous forme de tables alignées sur la date
calendaire réelle plutôt que sur l'indice time_step.

But : vérifier si une même date calendaire, prédite à des positions
time_step différentes selon la fenêtre qui l'ancre, donne une valeur de
prédiction stable (-> tâche essentiellement répétée, chaque position
ayant accès à ses propres données météo du jour) ou une valeur qui se
dégrade/change fortement selon l'éloignement de l'ancre (-> vrai effet
d'horizon de prévision).

Usage :
    python inspect_consecutive_windows.py
"""

import sys
import pickle
import random
from pathlib import Path

import numpy as np
import pandas as pd


# ──────────────────────────────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────────────────────────────

RUNS_ROOT = Path("./runs")
#
# RUN_NAME =  "arlstm_DtoD80_last10_1607_114931"
RUN_NAME = "arlstm_DtoD90_last15_1607_111125"
EPOCH = 14

FREQ_KEY = "1D"
TARGET_VAR_OBS = "water_level_obs"
TARGET_VAR_SIM = "water_level_sim"

# Station à inspecter : laisser None pour tirage aléatoire, sinon mettre l'ID exact
STATION_ID = None

N_CONSECUTIVE_WINDOWS = 15  # nombre de fenêtres d'ancrage consécutives à afficher
RANDOM_SEED = None  # mettre un entier pour reproductibilité, None pour aléatoire à chaque run


# ──────────────────────────────────────────────────────────────────────────────
# CHARGEMENT
# ──────────────────────────────────────────────────────────────────────────────

def load_results(run_dir: Path, epoch: int) -> dict:
    p_path = run_dir / "validation" / f"model_epoch{epoch:03d}" / "validation_results.p"
    if not p_path.exists():
        sys.exit(f"[ERREUR] Fichier introuvable : {p_path}")
    with open(p_path, "rb") as f:
        return pickle.load(f)


def get_station_dataset(results: dict, station_id: str):
    try:
        return results[station_id][FREQ_KEY]["xr"]
    except (KeyError, TypeError) as e:
        sys.exit(f"[ERREUR] Impossible d'extraire le dataset pour {station_id} : {e}")


# ──────────────────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────────────────

def main():
    if RANDOM_SEED is not None:
        random.seed(RANDOM_SEED)

    run_dir = (RUNS_ROOT / RUN_NAME).resolve()
    print(f"[INFO] Run : {RUN_NAME}, epoch {EPOCH}")
    results = load_results(run_dir, EPOCH)

    station_id = STATION_ID or random.choice(list(results.keys()))
    print(f"[INFO] Station sélectionnée : {station_id}")

    ds = get_station_dataset(results, station_id)
    print(f"[INFO] dims dataset : {dict(ds.sizes)}")
    print(f"[INFO] coords disponibles : {list(ds.coords)}")

    n_dates = ds.sizes["date"]
    n_timesteps = ds.sizes["time_step"]

    # Vérifie s'il existe une coordonnée 'time_step' avec un vrai décalage temporel
    time_step_coord = ds.coords["time_step"].values if "time_step" in ds.coords else np.arange(n_timesteps)
    print(f"[INFO] valeurs de la coordonnée time_step : {time_step_coord}")

    dates = pd.to_datetime(ds.coords["date"].values)

    # Choisit un point de départ aléatoire pour la séquence de fenêtres consécutives,
    # en laissant de la marge pour N_CONSECUTIVE_WINDOWS fenêtres + n_timesteps jours de recul
    max_start = n_dates - N_CONSECUTIVE_WINDOWS - 1
    if max_start <= 0:
        sys.exit("[ERREUR] Pas assez de dates pour extraire la séquence demandée.")
    start_idx = random.randint(0, max_start)

    print(f"\n[INFO] {N_CONSECUTIVE_WINDOWS} fenêtres d'ancrage consécutives, "
          f"à partir de l'indice date={start_idx} ({dates[start_idx].date()})\n")

    obs_all = ds[TARGET_VAR_OBS].values  # (date, time_step)
    sim_all = ds[TARGET_VAR_SIM].values

    # Tente de déduire si time_step_coord est déjà un décalage temporel (timedelta)
    # ou juste un indice 0..n-1. Dans le 2e cas on suppose que time_step = n-1 est
    # le jour d'ancrage (le plus récent) et time_step = 0 le plus ancien du bloc.
    is_timedelta = np.issubdtype(np.asarray(time_step_coord).dtype, np.timedelta64)

    for w in range(N_CONSECUTIVE_WINDOWS):
        date_idx = start_idx + w
        anchor_date = dates[date_idx]

        rows = []
        for t in range(n_timesteps):
            if is_timedelta:
                offset_days = pd.Timedelta(time_step_coord[t]).days
                calendar_date = anchor_date + pd.Timedelta(days=offset_days)
            else:
                # hypothèse : t=n-1 est le jour d'ancrage, t=0 est n-1 jours avant
                offset_from_anchor = t - (n_timesteps - 1)
                calendar_date = anchor_date + pd.Timedelta(days=offset_from_anchor)

            rows.append({
                "time_step": t,
                "date_calendaire": calendar_date.date(),
                "obs": obs_all[date_idx, t],
                "sim": sim_all[date_idx, t],
            })

        df_window = pd.DataFrame(rows)
        print(f"--- Fenêtre {w+1}/{N_CONSECUTIVE_WINDOWS} — ancrage (date index {date_idx}) : "
              f"{anchor_date.date()} ---")
        print(df_window.to_string(index=False, float_format=lambda x: f"{x:.3f}"))
        print()

    print("=" * 90)
    print("LECTURE : repère une date calendaire qui apparaît dans PLUSIEURS fenêtres "
          "consécutives ci-dessus (à des time_step différents).")
    print("  - Si sa valeur 'sim' reste quasi identique d'une fenêtre à l'autre "
          "-> la position time_step ne dégrade pas la prédiction : chaque position "
          "a probablement accès à ses propres données météo fraîches (reconstruction "
          "dense), pas un vrai forecast à horizon croissant.")
    print("  - Si sa valeur 'sim' se dégrade nettement quand elle est prédite loin de "
          "l'ancrage (time_step petit) -> effet d'horizon réel.")
    print("=" * 90)


if __name__ == "__main__":
    main()