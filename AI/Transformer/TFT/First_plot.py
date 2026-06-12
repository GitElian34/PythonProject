"""
preview_val.py
==============
Visualisation rapide des predictions TFT sur quelques stations de validation.
A lancer pendant ou apres l'entrainement pour verifier la qualite des predictions.

Usage :
    python preview_val.py --config ./AI/Transformer/TFT/config/tft_config.yaml \
                          --checkpoint ./data/Transformer/outputs/tft/checkpoints/tft-best-epoch=02-val_loss=0.0999.ckpt \
                          --n_stations 6 \
                          --max_stations 300
"""

import os
import sys
import argparse
import logging

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

from pytorch_forecasting import TemporalFusionTransformer

# Ajout du chemin vers les modules du projet
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from data_loader import load_config, build_dataframe, split_stations
from train import build_datasets

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def plot_val_stations(config, checkpoint, n_stations=6, max_stations=300, output_path=None):

    # ------------------------------------------------------------------ #
    # 1. Chargement modele                                                 #
    # ------------------------------------------------------------------ #
    logger.info(f"Chargement du checkpoint : {checkpoint}")
    model = TemporalFusionTransformer.load_from_checkpoint(checkpoint)
    model.eval()

    # ------------------------------------------------------------------ #
    # 2. Reconstruction du val dataset                                     #
    # ------------------------------------------------------------------ #
    logger.info(f"Chargement de {max_stations} stations in-situ...")
    df, alti_ids = build_dataframe(config, max_stations=max_stations,
                                   include_altimetric=False)
    insitu_ids = df["station_id"].unique().tolist()
    train_ids, val_ids = split_stations(insitu_ids, config)
    train_dataset, val_dataset = build_datasets(df, config, train_ids, val_ids, alti_ids)

    logger.info(f"Val dataset : {len(val_dataset)} fenetres sur {len(val_ids)} stations")

    loader = val_dataset.to_dataloader(train=False, batch_size=128, num_workers=0)

    # ------------------------------------------------------------------ #
    # 3. Predictions                                                       #
    # ------------------------------------------------------------------ #
    logger.info("Inference en cours...")
    quantiles = config["model"]["quantiles"]

    preds   = model.predict(loader, mode="quantiles", return_index=True)
    pred_vals = preds.output.numpy()   # (N, 1, Q)
    idx_df    = preds.index

    # mode="prediction" retourne directement les valeurs reelles (N,)
    y_true = model.predict(loader, mode="prediction", return_index=False).numpy().flatten()

    q05_idx = quantiles.index(0.05)
    q25_idx = quantiles.index(0.25)
    q50_idx = quantiles.index(0.5)
    q75_idx = quantiles.index(0.75)
    q95_idx = quantiles.index(0.95)

    results = idx_df.copy()
    results["y_true"] = y_true
    results["q05"]    = pred_vals[:, 0, q05_idx]
    results["q25"]    = pred_vals[:, 0, q25_idx]
    results["q50"]    = pred_vals[:, 0, q50_idx]
    results["q75"]    = pred_vals[:, 0, q75_idx]
    results["q95"]    = pred_vals[:, 0, q95_idx]
    results["outlier"] = (results["y_true"] < results["q05"]) | \
                         (results["y_true"] > results["q95"])

    # Merge dates reelles
    results = results.merge(
        df[["station_id", "time_idx", "date"]],
        on=["station_id", "time_idx"], how="left"
    )
    results["date"] = pd.to_datetime(results["date"])

    # ------------------------------------------------------------------ #
    # 4. Selection des stations a plotter                                  #
    # ------------------------------------------------------------------ #
    # Priorite aux stations qui ont au moins un outlier detecte
    stations_with_outliers = results[results["outlier"]]["station_id"].unique()
    stations_normal        = results[~results["station_id"].isin(stations_with_outliers)]["station_id"].unique()

    rng = np.random.default_rng(42)
    n_outlier = min(n_stations // 2, len(stations_with_outliers))
    n_normal  = n_stations - n_outlier

    selected = []
    if n_outlier > 0:
        selected += rng.choice(stations_with_outliers, size=n_outlier, replace=False).tolist()
    if n_normal > 0 and len(stations_normal) > 0:
        selected += rng.choice(stations_normal,
                               size=min(n_normal, len(stations_normal)),
                               replace=False).tolist()

    logger.info(f"Stations selectionnees : {len(selected)} "
                f"({n_outlier} avec outliers, {len(selected)-n_outlier} normales)")

    # ------------------------------------------------------------------ #
    # 5. Plot                                                              #
    # ------------------------------------------------------------------ #
    ncols = 2
    nrows = (len(selected) + 1) // ncols
    fig, axes = plt.subplots(nrows, ncols, figsize=(16, 4 * nrows))
    axes = axes.flatten() if len(selected) > 1 else [axes]

    for i, sid in enumerate(selected):
        ax = axes[i]
        s = results[results["station_id"] == sid].sort_values("date")

        if s.empty:
            ax.set_visible(False)
            continue

        # Intervalles de confiance
        ax.fill_between(s["date"], s["q05"], s["q95"],
                        alpha=0.15, color="steelblue", label="IC 5%-95%")
        ax.fill_between(s["date"], s["q25"], s["q75"],
                        alpha=0.30, color="steelblue", label="IC 25%-75%")

        # Mediane predite
        ax.plot(s["date"], s["q50"], color="steelblue",
                linewidth=1.5, label="Mediane predite")

        # Valeurs reelles
        normal   = s[~s["outlier"]]
        outliers = s[s["outlier"]]
        ax.scatter(normal["date"],   normal["y_true"],
                   color="black", s=20, zorder=3, label="Valeurs normales")
        ax.scatter(outliers["date"], outliers["y_true"],
                   color="red", s=70, zorder=4, marker="x",
                   linewidths=2, label=f"Outliers ({len(outliers)})")

        # Calcul MAE sur cette station
        mae = np.abs(s["y_true"] - s["q50"]).mean()

        ax.set_title(f"{sid}  |  MAE mediane={mae:.4f}  |  "
                     f"outliers={s['outlier'].sum()}", fontsize=9)
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=6))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, fontsize=7)
        ax.set_ylabel("Hauteur d'eau")
        ax.legend(fontsize=7, loc="upper right")
        ax.grid(True, alpha=0.3)

    # Masquer les axes inutilises
    for j in range(len(selected), len(axes)):
        axes[j].set_visible(False)

    # Stats globales en titre
    n_total   = len(results)
    n_outlier_total = results["outlier"].sum()
    mae_global = np.abs(results["y_true"] - results["q50"]).mean()
    coverage = ((results["y_true"] >= results["q05"]) &
                (results["y_true"] <= results["q95"])).mean()

    fig.suptitle(
        f"Predictions TFT — Val dataset\n"
        f"MAE globale={mae_global:.4f} | "
        f"Coverage IC 90%={100*coverage:.1f}% (theorique=90%) | "
        f"Outliers={n_outlier_total}/{n_total} ({100*n_outlier_total/n_total:.1f}%)",
        fontsize=11, fontweight="bold"
    )

    plt.tight_layout()

    if output_path is None:
        output_path = os.path.join(
            config["paths"]["output_dir"], "val_preview.png"
        )
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close()
    logger.info(f"Graphique sauvegarde : {output_path}")

    # Stats console
    print(f"\n{'='*50}")
    print(f"MAE globale (mediane)   : {mae_global:.4f}")
    print(f"Coverage IC 90%         : {100*coverage:.1f}% (theorique : 90%)")
    print(f"Outliers detectes       : {n_outlier_total}/{n_total} ({100*n_outlier_total/n_total:.1f}%)")
    print(f"Stations avec outliers  : {len(stations_with_outliers)}/{results['station_id'].nunique()}")
    print(f"{'='*50}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config",      required=True)
    parser.add_argument("--checkpoint",  required=True)
    parser.add_argument("--n_stations",  type=int, default=6,
                        help="Nombre de stations a visualiser")
    parser.add_argument("--max_stations", type=int, default=300,
                        help="Nb de stations in-situ a charger pour reconstruire le val")
    parser.add_argument("--output",      default=None,
                        help="Chemin du PNG de sortie (defaut : output_dir/val_preview.png)")
    args = parser.parse_args()

    config = load_config(args.config)
    plot_val_stations(
        config=config,
        checkpoint=args.checkpoint,
        n_stations=args.n_stations,
        max_stations=args.max_stations,
        output_path=args.output,
    )