"""
diagnose_nan_predictions.py
------------------------------
Vérifie l'ampleur du problème "sim = NaN" repéré sur la station A420063002.
Pour chaque bassin du run, calcule :
  - le % de dates où TOUT time_step de sim est NaN (fenêtre entièrement cassée)
  - le % de dates où le nowcast (time_step le plus récent) est NaN
  - le nombre de blocs (séquences continues) de dates avec sim totalement NaN,
    et la longueur du plus long bloc (pour repérer un vrai "trou" de données
    dynamiques vs quelques NaN isolés)

Usage :
    python diagnose_nan_predictions.py
"""

import pickle
import sys
from pathlib import Path

import numpy as np
import pandas as pd


# ──────────────────────────────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────────────────────────────

RUNS_ROOT = Path("./runs")

# --- Run à vérifier : décommenter UN SEUL bloc à la fois ---

# Run predict_last_n > 1 déjà suspect
# RUN_NAME = "arlstm_DtoD90_last15_1607_111125"
# EPOCH = 8

# Run CLASSIQUE predict_last_n=1 de session précédente, pour comparaison
# (mêmes stations, pour savoir si le problème existait déjà avant)
RUN_NAME = "arlstm_DtoD90_last10_maskedmean_1707_154212"
EPOCH = 21

FREQ_KEY = "1D"
TARGET_VAR_SIM = "water_level_sim"

# Si non vide, limite l'analyse à ces stations (les 8 déjà identifiées comme
# suspectes) pour une comparaison directe et rapide entre runs.
STATIONS_FILTER = []

OUT_CSV = Path(f"./data_processing/predict_last_n_comparison/nan_diagnosis_{RUN_NAME}.csv")
OUT_CSV.parent.mkdir(parents=True, exist_ok=True)


# ──────────────────────────────────────────────────────────────────────────────
# UTILS
# ──────────────────────────────────────────────────────────────────────────────

def longest_true_run(mask: np.ndarray) -> int:
    """Longueur du plus long bloc consécutif de True dans un array booléen 1D."""
    if not mask.any():
        return 0
    # découpe aux changements de valeur
    changes = np.flatnonzero(np.diff(np.concatenate(([0], mask.astype(int), [0]))))
    runs = changes.reshape(-1, 2)
    lengths = runs[:, 1] - runs[:, 0]
    return int(lengths.max()) if len(lengths) else 0


def load_results(run_dir: Path, epoch: int) -> dict:
    p_path = run_dir / "validation" / f"model_epoch{epoch:03d}" / "validation_results.p"
    if not p_path.exists():
        sys.exit(f"[ERREUR] Fichier introuvable : {p_path}")
    with open(p_path, "rb") as f:
        return pickle.load(f)


def longest_true_run_bounds(mask: np.ndarray) -> tuple[int, int]:
    """Indices (start, end exclusif) du plus long bloc continu de True."""
    if not mask.any():
        return None, None
    changes = np.flatnonzero(np.diff(np.concatenate(([0], mask.astype(int), [0]))))
    runs = changes.reshape(-1, 2)
    lengths = runs[:, 1] - runs[:, 0]
    best = int(np.argmax(lengths))
    return int(runs[best, 0]), int(runs[best, 1])


# ──────────────────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────────────────

def main():
    run_dir = (RUNS_ROOT / RUN_NAME).resolve()
    print(f"[INFO] Run : {RUN_NAME}, epoch {EPOCH}")
    results = load_results(run_dir, EPOCH)

    rows = []
    for station_id, basin_data in results.items():
        if STATIONS_FILTER and station_id not in STATIONS_FILTER:
            continue
        try:
            ds = basin_data[FREQ_KEY]["xr"]
        except (KeyError, TypeError):
            print(f"[WARNING] Structure inattendue pour {station_id}, ignoré.")
            continue

        if TARGET_VAR_SIM not in ds.data_vars:
            continue

        sim = ds[TARGET_VAR_SIM].values  # (date, time_step)
        n_dates, n_timesteps = sim.shape

        all_nan_per_date = np.isnan(sim).all(axis=1)  # date entièrement cassée
        nowcast_nan = np.isnan(sim[:, -1])  # NaN sur la position la plus récente

        start_idx, end_idx = longest_true_run_bounds(all_nan_per_date)
        dates = pd.to_datetime(ds.coords["date"].values) if "date" in ds.coords else None

        streak_start_date = dates[start_idx].date() if (dates is not None and start_idx is not None) else None
        streak_end_date = dates[end_idx - 1].date() if (dates is not None and end_idx is not None) else None

        rows.append({
            "station": station_id,
            "n_dates": n_dates,
            "pct_dates_fully_nan": 100 * all_nan_per_date.mean(),
            "pct_nowcast_nan": 100 * nowcast_nan.mean(),
            "longest_fully_nan_streak_days": longest_true_run(all_nan_per_date),
            "longest_streak_start_date": streak_start_date,
            "longest_streak_end_date": streak_end_date,
        })

    df = pd.DataFrame(rows).sort_values("pct_dates_fully_nan", ascending=False)
    df.to_csv(OUT_CSV, index=False)

    print("\n" + "=" * 80)
    print("RÉSUMÉ GLOBAL — fraction de dates avec sim entièrement NaN, par station")
    print("=" * 80)
    print(f"Nombre de stations analysées : {len(df)}")
    print(f"Médiane  % dates fully-NaN   : {df['pct_dates_fully_nan'].median():.2f}%")
    print(f"Moyenne  % dates fully-NaN   : {df['pct_dates_fully_nan'].mean():.2f}%")
    print(f"Stations avec 0% NaN         : {(df['pct_dates_fully_nan'] == 0).sum()} / {len(df)}")
    print(f"Stations avec >10% NaN       : {(df['pct_dates_fully_nan'] > 10).sum()} / {len(df)}")
    print(f"Stations avec >50% NaN       : {(df['pct_dates_fully_nan'] > 50).sum()} / {len(df)}")

    print("\nTop 15 stations les plus touchées :")
    print(df.head(15).to_string(index=False, float_format=lambda x: f"{x:.2f}"))

    print(f"\n[OK] Détail complet sauvegardé : {OUT_CSV}")
    print("=" * 80)

    # Distribution des dates de début du plus long trou : une cause commune
    # (ex: un forçage météo qui s'arrête à une date précise) se traduirait par
    # plusieurs stations partageant (quasi) la même date de début.
    df_dates = df.dropna(subset=["longest_streak_start_date"]).copy()
    if not df_dates.empty:
        df_dates["longest_streak_start_date"] = pd.to_datetime(df_dates["longest_streak_start_date"])
        # arrondit au mois pour regrouper les dates proches mais pas strictement identiques
        df_dates["start_month"] = df_dates["longest_streak_start_date"].dt.to_period("M")
        counts = df_dates["start_month"].value_counts().sort_index()

        print("\nDISTRIBUTION DES DATES DE DÉBUT DU PLUS LONG TROU (par mois)")
        print("-" * 80)
        print(counts.to_string())
        print("-" * 80)
        top_month = counts.idxmax()
        print(f"[INFO] Mois le plus fréquent comme début de trou : {top_month} "
              f"({counts.max()} stations / {len(df_dates)})")
        if counts.max() >= 0.3 * len(df_dates):
            print("[ALERTE] Une part importante des stations partage (quasi) la même date "
                  "de début de trou -> cause commune probable (ex: un forçage météo ou une "
                  "source de donnée dynamique qui s'arrête/manque à partir de cette date).")
        else:
            print("[INFO] Pas de date de début dominante -> les trous semblent plus "
                  "spécifiques à chaque station (problème par capteur/bassin plutôt que "
                  "source de donnée partagée).")
        print("=" * 80)


if __name__ == "__main__":
    main()