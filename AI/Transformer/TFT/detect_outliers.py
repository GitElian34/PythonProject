"""
preview_val.py
==============
Visualisation des predictions TFT sur les stations de validation.
Trace la serie complete (toutes les fenetres) par station.

Usage :
    python preview_val.py \
        --config ./AI/Transformer/TFT/config/tft_config.yaml \
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

from pytorch_forecasting import TemporalFusionTransformer, TimeSeriesDataSet
from pytorch_forecasting.data import GroupNormalizer

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from data_loader import load_config, build_dataframe, split_stations

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def build_full_val_dataset(df, val_ids, config, reference_dataset):
    """
    Construit un dataset de prediction sur toutes les fenetres des stations val.
    predict=False + stop_randomization=False => toutes les fenetres disponibles.
    """
    val_df = df[df["station_id"].isin(val_ids)].copy()
    dataset = TimeSeriesDataSet.from_dataset(
        reference_dataset, val_df,
        predict=False,
        stop_randomization=False,
    )
    logger.info(f"Val dataset inference : {len(dataset)} fenetres sur {len(val_ids)} stations")
    return dataset


def build_reference_dataset(df, train_ids, val_ids, config, altimetric_ids):
    """Reconstruit le reference_dataset (vocabulaire global train+val)."""
    cfg_data  = config["data"]
    cfg_model = config["model"]
    target        = cfg_data["target"]
    dynamic_obs   = cfg_data["dynamic_inputs"]
    dynamic_known = cfg_data["known_inputs"]
    static_cols   = cfg_data["static_inputs"]

    train_df = df[df["station_id"].isin(train_ids) &
                  ~df["station_id"].isin(altimetric_ids)].copy()
    val_df   = df[df["station_id"].isin(val_ids) &
                  ~df["station_id"].isin(altimetric_ids)].copy()
    all_df   = pd.concat([train_df, val_df], ignore_index=True)

    reference_dataset = TimeSeriesDataSet(
        all_df,
        time_idx="time_idx",
        target=target,
        group_ids=["station_id"],
        min_encoder_length=cfg_model["encoder_length"] // 2,
        max_encoder_length=cfg_model["encoder_length"],
        min_prediction_length=cfg_model["prediction_length"],
        max_prediction_length=cfg_model["prediction_length"],
        static_reals=static_cols,
        time_varying_known_reals=dynamic_known,
        time_varying_unknown_reals=dynamic_obs + [target],
        target_normalizer=GroupNormalizer(groups=["station_id"], transformation=None),
        allow_missing_timesteps=True,
    )
    return reference_dataset


def run_inference(model, dataset, config):
    """Lance l'inference et retourne un DataFrame avec quantiles + y_true."""
    quantiles = config["model"]["quantiles"]
    loader = dataset.to_dataloader(train=False, batch_size=128, num_workers=0)

    # Quantiles predits
    preds     = model.predict(loader, mode="quantiles", return_index=True)
    pred_vals = preds.output.numpy()   # (N, 1, Q)
    idx_df    = preds.index.copy()

    # Valeurs reelles
    y_true = model.predict(loader, mode="prediction", return_index=False).numpy().flatten()

    idx_df["y_true"] = y_true
    idx_df["q05"]    = pred_vals[:, 0, quantiles.index(0.05)]
    idx_df["q25"]    = pred_vals[:, 0, quantiles.index(0.25)]
    idx_df["q50"]    = pred_vals[:, 0, quantiles.index(0.5)]
    idx_df["q75"]    = pred_vals[:, 0, quantiles.index(0.75)]
    idx_df["q95"]    = pred_vals[:, 0, quantiles.index(0.95)]
    idx_df["outlier"] = (idx_df["y_true"] < idx_df["q05"]) | \
                        (idx_df["y_true"] > idx_df["q95"])

    return idx_df


def plot_stations(results, df, config, n_stations, output_path):
    """Trace les series completes pour n_stations stations."""
    # Merge dates reelles
    results = results.merge(
        df[["station_id", "time_idx", "date"]].drop_duplicates(),
        on=["station_id", "time_idx"], how="left"
    )
    results["date"] = pd.to_datetime(results["date"])

    # Selection des stations
    all_stations = results["station_id"].unique()
    stations_with_outliers = results[results["outlier"]]["station_id"].unique()
    stations_normal = [s for s in all_stations if s not in stations_with_outliers]

    rng = np.random.default_rng(42)
    n_out    = min(n_stations // 2, len(stations_with_outliers))
    n_normal = n_stations - n_out
    selected = []
    if n_out > 0:
        selected += rng.choice(stations_with_outliers, size=n_out, replace=False).tolist()
    if n_normal > 0 and len(stations_normal) > 0:
        selected += rng.choice(stations_normal,
                               size=min(n_normal, len(stations_normal)),
                               replace=False).tolist()

    logger.info(f"Stations : {len(selected)} ({n_out} avec outliers, {n_normal} normales)")

    # Plot
    ncols = 2
    nrows = (len(selected) + 1) // ncols
    fig, axes = plt.subplots(nrows, ncols, figsize=(16, 4 * nrows))
    axes = axes.flatten() if len(selected) > 1 else [axes]

    for i, sid in enumerate(selected):
        ax  = axes[i]
        s   = results[results["station_id"] == sid].sort_values("date")

        if s.empty or s["date"].isna().all():
            ax.set_visible(False)
            continue

        ax.fill_between(s["date"], s["q05"], s["q95"],
                        alpha=0.15, color="steelblue", label="IC 5%-95%")
        ax.fill_between(s["date"], s["q25"], s["q75"],
                        alpha=0.30, color="steelblue", label="IC 25%-75%")
        ax.plot(s["date"], s["q50"],
                color="steelblue", linewidth=1.5, label="Mediane predite")

        normal   = s[~s["outlier"]]
        outliers = s[s["outlier"]]
        ax.scatter(normal["date"],   normal["y_true"],
                   color="black", s=20, zorder=3, label="Reel")
        ax.scatter(outliers["date"], outliers["y_true"],
                   color="red", s=70, zorder=4, marker="x",
                   linewidths=2, label=f"Outliers ({len(outliers)})")

        mae = np.abs(s["y_true"] - s["q50"]).mean()
        ax.set_title(f"{sid}  |  MAE={mae:.4f}  |  outliers={s['outlier'].sum()}  |  n={len(s)}",
                     fontsize=9)
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=6))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, fontsize=7)
        ax.set_ylabel("Hauteur d'eau")
        ax.legend(fontsize=7, loc="upper right")
        ax.grid(True, alpha=0.3)

    for j in range(len(selected), len(axes)):
        axes[j].set_visible(False)

    # Stats globales
    mae_g    = np.abs(results["y_true"] - results["q50"]).mean()
    coverage = ((results["y_true"] >= results["q05"]) &
                (results["y_true"] <= results["q95"])).mean()
    n_out_total = results["outlier"].sum()
    n_total     = len(results)

    fig.suptitle(
        f"Predictions TFT — Val dataset\n"
        f"MAE={mae_g:.4f} | Coverage IC 90%={100*coverage:.1f}% (theorique=90%) | "
        f"Outliers={n_out_total}/{n_total} ({100*n_out_total/n_total:.1f}%)",
        fontsize=11, fontweight="bold"
    )
    plt.tight_layout()
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close()
    logger.info(f"Graphique sauvegarde : {output_path}")

    print(f"\n{'='*50}")
    print(f"Fenetres totales        : {n_total}")
    print(f"MAE globale (mediane)   : {mae_g:.4f}")
    print(f"Coverage IC 90%         : {100*coverage:.1f}%")
    print(f"Outliers detectes       : {n_out_total} ({100*n_out_total/n_total:.1f}%)")
    print(f"Stations avec outliers  : {len(stations_with_outliers)}/{len(all_stations)}")
    print(f"{'='*50}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config",       required=True)
    parser.add_argument("--checkpoint",   required=True)
    parser.add_argument("--n_stations",   type=int, default=6)
    parser.add_argument("--max_stations", type=int, default=300)
    parser.add_argument("--output",       default=None)
    args = parser.parse_args()

    config = load_config(args.config)

    output_path = args.output or os.path.join(
        config["paths"]["output_dir"], "val_preview.png")

    # Chargement modele
    model = TemporalFusionTransformer.load_from_checkpoint(args.checkpoint)
    model.eval()

    # Chargement donnees
    logger.info(f"Chargement de {args.max_stations} stations in-situ...")
    df, alti_ids = build_dataframe(config, max_stations=args.max_stations,
                                   include_altimetric=False)
    insitu_ids = df["station_id"].unique().tolist()
    train_ids, val_ids = split_stations(insitu_ids, config)

    # Reference dataset (vocabulaire global)
    reference_dataset = build_reference_dataset(df, train_ids, val_ids, config, alti_ids)

    # Val dataset avec toutes les fenetres
    val_dataset = build_full_val_dataset(df, val_ids, config, reference_dataset)

    # Inference
    results = run_inference(model, val_dataset, config)

    # Plot
    plot_stations(results, df, config, args.n_stations, output_path)