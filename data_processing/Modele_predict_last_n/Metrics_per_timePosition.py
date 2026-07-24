"""
metrics_per_timestep_position.py
----------------------------------
Pour un run NeuralHydrology entraîné avec predict_last_n > 1, calcule le
NSE et le KGE SÉPARÉMENT pour chaque position `time_step` dans la fenêtre
(0 = jour le plus ancien du bloc, n-1 = dernier jour / "nowcast").

Objectif : vérifier si la performance se dégrade en s'éloignant du dernier
jour, et permettre une comparaison "apples-to-apples" entre modèles
last10/last15 en isolant time_step = n-1 uniquement (équivalent au
sequence-to-one classique).

Usage :
    Choisir UN SEUL modèle ci-dessous (les 3 autres restent en commentaire),
    ajuster BEST_EPOCH si besoin, puis :
    python metrics_per_timestep_position.py
"""

import sys
import pickle
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


# ──────────────────────────────────────────────────────────────────────────────
# CONFIG — décommenter UN SEUL modèle à la fois
# ──────────────────────────────────────────────────────────────────────────────

RUNS_ROOT = Path("./runs")

# --- DtoD80_last10 ---
RUN_NAME = "arlstm_DtoD80_last10_1607_114931"
BEST_EPOCH = 14

# --- DtoD90_last10 ---
# RUN_NAME = "arlstm_DtoD90_last10_1607_162211"
# BEST_EPOCH = 8

# --- DtoD90_last15 ---
# RUN_NAME = "arlstm_DtoD90_last15_1607_111125"
# BEST_EPOCH = 8

# --- DtoD96_last10 ---
# RUN_NAME = "arlstm_DtoD96_last10_1607_130811"
# BEST_EPOCH = 12

TARGET_VAR_OBS = "water_level_obs"
TARGET_VAR_SIM = "water_level_sim"
FREQ_KEY = "1D"

# Si True, calcule aussi la KGE "classique" à 3 composantes (avec beta).
# Rappel projet : le beta classique (ratio de moyennes) explose sur données
# z-scorées (obs.mean() ~ 0). Si les valeurs ici sont déjà en mètres
# (reconstruites), le beta classique est valide ; si elles sont encore
# z-scorées, préférer uniquement KGE_NOBETA.
COMPUTE_KGE_WITH_BETA = True

OUT_DIR = Path("./data_processing/predict_last_n_comparison/per_timestep")
OUT_DIR.mkdir(parents=True, exist_ok=True)


# ──────────────────────────────────────────────────────────────────────────────
# MÉTRIQUES
# ──────────────────────────────────────────────────────────────────────────────

def nse(obs: np.ndarray, sim: np.ndarray) -> float:
    mask = ~np.isnan(obs) & ~np.isnan(sim)
    obs, sim = obs[mask], sim[mask]
    if len(obs) < 2:
        return np.nan
    denom = np.sum((obs - obs.mean()) ** 2)
    if denom == 0:
        return np.nan
    return 1 - np.sum((obs - sim) ** 2) / denom


def kge_no_beta(obs: np.ndarray, sim: np.ndarray) -> float:
    """Convention du projet : KGE à 2 composantes (r, alpha), sans beta,
    car obs.mean() ~ 0 sur données z-scorées fait exploser le beta classique."""
    mask = ~np.isnan(obs) & ~np.isnan(sim)
    obs, sim = obs[mask], sim[mask]
    if len(obs) < 2 or obs.std() == 0 or sim.std() == 0:
        return np.nan
    r = np.corrcoef(obs, sim)[0, 1]
    alpha = sim.std() / obs.std()
    return 1 - np.sqrt((r - 1) ** 2 + (alpha - 1) ** 2)


def kge_with_beta(obs: np.ndarray, sim: np.ndarray) -> float:
    """KGE classique à 3 composantes (r, alpha, beta). À utiliser seulement
    si les données ne sont pas z-scorées (sinon beta explose, cf. conventions projet)."""
    mask = ~np.isnan(obs) & ~np.isnan(sim)
    obs, sim = obs[mask], sim[mask]
    if len(obs) < 2 or obs.std() == 0 or sim.std() == 0 or obs.mean() == 0:
        return np.nan
    r = np.corrcoef(obs, sim)[0, 1]
    alpha = sim.std() / obs.std()
    beta = sim.mean() / obs.mean()
    return 1 - np.sqrt((r - 1) ** 2 + (alpha - 1) ** 2 + (beta - 1) ** 2)


# ──────────────────────────────────────────────────────────────────────────────
# CHARGEMENT
# ──────────────────────────────────────────────────────────────────────────────

def load_results(run_dir: Path, epoch: int) -> dict:
    p_path = run_dir / "validation" / f"model_epoch{epoch:03d}" / "validation_results.p"
    if not p_path.exists():
        sys.exit(f"[ERREUR] Fichier introuvable : {p_path}")
    with open(p_path, "rb") as f:
        return pickle.load(f)


def extract_per_timestep_arrays(results: dict) -> tuple[np.ndarray, np.ndarray, list[str], int]:
    """
    Parcourt tous les bassins et empile les tableaux (date, time_step) obs/sim.
    Retourne obs_all, sim_all de shape (n_basins, date, time_step), + liste basins.
    Les bassins n'ont pas forcément le même nombre de dates -> on les garde
    séparés dans une liste plutôt que de forcer une concat brutale.
    """
    obs_list, sim_list, basins = [], [], []
    n_timesteps = None

    for basin_id, basin_data in results.items():
        try:
            ds = basin_data[FREQ_KEY]["xr"]
        except (KeyError, TypeError):
            print(f"[WARNING] Structure inattendue pour bassin {basin_id}, ignoré.")
            continue

        if TARGET_VAR_OBS not in ds.data_vars or TARGET_VAR_SIM not in ds.data_vars:
            print(f"[WARNING] Variables {TARGET_VAR_OBS}/{TARGET_VAR_SIM} absentes pour {basin_id}, ignoré.")
            continue

        obs = ds[TARGET_VAR_OBS].values  # (date, time_step)
        sim = ds[TARGET_VAR_SIM].values

        if n_timesteps is None:
            n_timesteps = obs.shape[1]
        elif obs.shape[1] != n_timesteps:
            print(f"[WARNING] {basin_id}: time_step={obs.shape[1]} != {n_timesteps} attendu, ignoré.")
            continue

        obs_list.append(obs)
        sim_list.append(sim)
        basins.append(basin_id)

    if not obs_list:
        sys.exit("[ERREUR] Aucun bassin exploitable trouvé.")

    return obs_list, sim_list, basins, n_timesteps


def metrics_per_timestep(obs_list, sim_list, basins, n_timesteps) -> pd.DataFrame:
    """
    Pour chaque position time_step (0..n-1), calcule NSE/KGE :
    - par bassin, puis médiane inter-bassins (cohérent avec validation_metrics.csv)
    """
    rows = []
    for t in range(n_timesteps):
        nse_per_basin, kge_nobeta_per_basin, kge_beta_per_basin = [], [], []
        for obs, sim in zip(obs_list, sim_list):
            o_t = obs[:, t]
            s_t = sim[:, t]
            nse_per_basin.append(nse(o_t, s_t))
            kge_nobeta_per_basin.append(kge_no_beta(o_t, s_t))
            if COMPUTE_KGE_WITH_BETA:
                kge_beta_per_basin.append(kge_with_beta(o_t, s_t))

        row = {
            "time_step": t,
            "NSE_median": np.nanmedian(nse_per_basin),
            "NSE_mean": np.nanmean(nse_per_basin),
            "KGE_nobeta_median": np.nanmedian(kge_nobeta_per_basin),
            "KGE_nobeta_mean": np.nanmean(kge_nobeta_per_basin),
            "n_basins_valid": int(np.sum(~np.isnan(nse_per_basin))),
        }
        if COMPUTE_KGE_WITH_BETA:
            row["KGE_beta_median"] = np.nanmedian(kge_beta_per_basin)
            row["KGE_beta_mean"] = np.nanmean(kge_beta_per_basin)
        rows.append(row)

    return pd.DataFrame(rows)


# ──────────────────────────────────────────────────────────────────────────────
# PLOT
# ──────────────────────────────────────────────────────────────────────────────

def plot_per_timestep(df: pd.DataFrame, run_name: str, epoch: int, out_path: Path) -> None:
    metrics_to_plot = ["NSE_median", "KGE_nobeta_median"]
    if COMPUTE_KGE_WITH_BETA:
        metrics_to_plot.append("KGE_beta_median")

    n = len(metrics_to_plot)
    fig, axes = plt.subplots(1, n, figsize=(6 * n, 4.5), squeeze=False)
    fig.suptitle(
        f"Métrique en fonction de la position dans la fenêtre (time_step)\n"
        f"{run_name} — epoch {epoch}  (0 = plus ancien, {df['time_step'].max()} = dernier jour / nowcast)",
        fontsize=13, fontweight="bold", y=1.05,
    )

    for i, metric in enumerate(metrics_to_plot):
        ax = axes[0][i]
        ax.plot(df["time_step"], df[metric], marker="o", color="#2a78d6", linewidth=2, zorder=3)
        # Met en évidence le dernier point (nowcast, comparable au sequence-to-one)
        last_t = df["time_step"].max()
        last_val = df.loc[df["time_step"] == last_t, metric].values[0]
        ax.scatter([last_t], [last_val], color="#e04c4c", s=80, zorder=4,
                    label=f"nowcast (t={last_t})")
        ax.set_title(metric, fontsize=11, fontweight="bold")
        ax.set_xlabel("time_step (position dans la fenêtre)")
        ax.set_ylabel(metric)
        ax.grid(True, linestyle="--", alpha=0.4, zorder=0)
        ax.spines[["top", "right"]].set_visible(False)
        ax.legend(fontsize=9)

    plt.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"[OK] Figure sauvegardée : {out_path}")


# ──────────────────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────────────────

def main():
    run_dir = (RUNS_ROOT / RUN_NAME).resolve()
    print(f"[INFO] Run : {RUN_NAME}, epoch {BEST_EPOCH}")

    results = load_results(run_dir, BEST_EPOCH)
    obs_list, sim_list, basins, n_timesteps = extract_per_timestep_arrays(results)
    print(f"[INFO] {len(basins)} bassins exploitables, {n_timesteps} positions time_step")

    df = metrics_per_timestep(obs_list, sim_list, basins, n_timesteps)

    print("\n" + "=" * 70)
    print(f"MÉTRIQUES PAR POSITION time_step — {RUN_NAME} (epoch {BEST_EPOCH})")
    print("=" * 70)
    print(df.to_string(index=False, float_format=lambda x: f"{x:.4f}"))
    print("=" * 70)

    last_t = n_timesteps - 1
    nowcast_row = df[df["time_step"] == last_t].iloc[0]
    print(f"\n[COMPARAISON] Valeur au nowcast (time_step={last_t}, équivalent seq-to-one) :")
    print(f"  NSE_median        = {nowcast_row['NSE_median']:.4f}")
    print(f"  KGE_nobeta_median = {nowcast_row['KGE_nobeta_median']:.4f}")
    if COMPUTE_KGE_WITH_BETA:
        print(f"  KGE_beta_median   = {nowcast_row['KGE_beta_median']:.4f}")
    print(f"\n  (À comparer à la valeur agrégée toutes positions confondues du CSV "
          f"validation_metrics.csv, pour voir l'écart introduit par le pooling sur predict_last_n.)")

    csv_out = OUT_DIR / f"{RUN_NAME}_epoch{BEST_EPOCH}_per_timestep.csv"
    df.to_csv(csv_out, index=False)
    print(f"\n[OK] CSV sauvegardé : {csv_out}")

    png_out = OUT_DIR / f"{RUN_NAME}_epoch{BEST_EPOCH}_per_timestep.png"
    plot_per_timestep(df, RUN_NAME, BEST_EPOCH, png_out)


if __name__ == "__main__":
    main()