"""
train.py
========
Entraînement du TFT sur les stations in-situ.
Validation primaire : stations in-situ (pilote early stopping + checkpoint)
Validation secondaire : stations altimétriques (monitoring pur, sans influence sur le training)

Usage :
    python train.py --config tft_config.yaml
    python train.py --config tft_config.yaml --debug
    python train.py --config tft_config.yaml --no_lr_find
"""

import os
import argparse
import logging

import numpy as np
import torch
import pandas as pd
import lightning.pytorch as pl
from lightning.pytorch.callbacks import EarlyStopping, LearningRateMonitor, ModelCheckpoint, Callback
from lightning.pytorch.loggers import TensorBoardLogger

from pytorch_forecasting import TemporalFusionTransformer, TimeSeriesDataSet
from pytorch_forecasting.data import GroupNormalizer
from pytorch_forecasting.metrics import QuantileLoss

from data_loader import load_config, build_dataframe, split_stations

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)
torch.set_num_threads(4)

# ================================================================== #
# Callback de validation secondaire sur les stations altimétriques   #
# ================================================================== #
class AltimetricValidationCallback(Callback):
    """
    A la fin de chaque epoch, calcule la QuantileLoss et la MAE
    sur les stations altimétriques et les logue dans TensorBoard.

    Ce callback est purement informatif — il n'influence ni l'early
    stopping ni le ModelCheckpoint, qui restent pilotés par val_loss
    (in-situ).
    """

    def __init__(self, alti_loader, quantiles, log_every_n_epochs=1):
        super().__init__()
        self.alti_loader        = alti_loader
        self.quantiles          = quantiles
        self.log_every_n_epochs = log_every_n_epochs

    def on_validation_epoch_end(self, trainer, pl_module):
        epoch = trainer.current_epoch
        if epoch % self.log_every_n_epochs != 0:
            return

        pl_module.eval()
        all_preds  = []   # (N, Q)
        all_ytrue  = []   # (N,)

        with torch.no_grad():
            for batch in self.alti_loader:
                x, y = batch
                # Deplace sur le bon device
                x = {k: v.to(pl_module.device) if isinstance(v, torch.Tensor) else v
                     for k, v in x.items()}
                out = pl_module(x)
                # out.prediction : (batch, pred_len, Q)
                pred = out.prediction[:, 0, :]   # (batch, Q)
                ytrue = y[0][:, 0].to(pl_module.device)  # (batch,)
                all_preds.append(pred)
                all_ytrue.append(ytrue)

        preds  = torch.cat(all_preds,  dim=0)   # (N, Q)
        ytrue  = torch.cat(all_ytrue,  dim=0)   # (N,)

        # QuantileLoss altimétrique
        loss_fn   = QuantileLoss(quantiles=self.quantiles)
        alti_loss = loss_fn(preds.unsqueeze(1), ytrue.unsqueeze(1)).item()

        # MAE sur la médiane (q=0.5)
        q50_idx = self.quantiles.index(0.5)
        mae     = torch.abs(ytrue - preds[:, q50_idx]).mean().item()

        # Coverage IC 90% [q05, q95]
        q05_idx  = self.quantiles.index(0.05)
        q95_idx  = self.quantiles.index(0.95)
        coverage = ((ytrue >= preds[:, q05_idx]) &
                    (ytrue <= preds[:, q95_idx])).float().mean().item()

        # Taux d'outliers
        outlier_rate = 1.0 - coverage

        # Log dans TensorBoard
        pl_module.log("alti_val_loss",     alti_loss,    on_epoch=True, prog_bar=True)
        pl_module.log("alti_val_mae",      mae,          on_epoch=True, prog_bar=False)
        pl_module.log("alti_coverage_90",  coverage,     on_epoch=True, prog_bar=False)
        pl_module.log("alti_outlier_rate", outlier_rate, on_epoch=True, prog_bar=False)

        logger.info(
            f"[Alti val] epoch={epoch} | "
            f"loss={alti_loss:.4f} | mae={mae:.4f} | "
            f"coverage90={100*coverage:.1f}% | outliers={100*outlier_rate:.1f}%"
        )

        pl_module.train()


# ================================================================== #
# Construction des datasets                                           #
# ================================================================== #
def build_datasets(df, config, train_ids, val_ids, altimetric_ids):
    """
    Construit train et val depuis les stations in-situ.
    Vocabulaire global (train+val) pour eviter les KeyError sur station_id.
    """
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

    logger.info(f"Lignes train : {len(train_df)} | val : {len(val_df)}")

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

    train_dataset = TimeSeriesDataSet.from_dataset(
        reference_dataset, train_df,
        predict=False, stop_randomization=False,
    )
    val_dataset = TimeSeriesDataSet.from_dataset(
        reference_dataset, val_df,
        predict=False, stop_randomization=False,
    )

    logger.info(f"Train dataset : {len(train_dataset)} fenetres")
    logger.info(f"Val dataset   : {len(val_dataset)} fenetres")

    return train_dataset, val_dataset, reference_dataset


def extend_vocabulary(insitu_all_df, alti_df, config):
    """
    Reconstruit un reference_dataset sur insitu + alti pour que
    toutes les station_id soient dans le vocabulaire de l encodeur.
    """
    cfg_data  = config["data"]
    cfg_model = config["model"]
    target        = cfg_data["target"]
    dynamic_obs   = cfg_data["dynamic_inputs"]
    dynamic_known = cfg_data["known_inputs"]
    static_cols   = cfg_data["static_inputs"]

    full_df = pd.concat([insitu_all_df, alti_df], ignore_index=True)

    return TimeSeriesDataSet(
        full_df,
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


def build_alti_dataset(alti_df, reference_dataset):
    """Dataset altimétriques — toutes les fenetres, inférence uniquement."""
    return TimeSeriesDataSet.from_dataset(
        reference_dataset, alti_df,
        predict=False, stop_randomization=False,
    )


def build_model(train_dataset, config):
    cfg = config["model"]
    model = TemporalFusionTransformer.from_dataset(
        train_dataset,
        learning_rate=config["training"]["learning_rate"],
        hidden_size=cfg["hidden_size"],
        lstm_layers=cfg["lstm_layers"],
        attention_head_size=cfg["attention_heads"],
        dropout=cfg["dropout"],
        hidden_continuous_size=cfg["hidden_continuous_size"],
        loss=QuantileLoss(quantiles=cfg["quantiles"]),
        log_interval=50,
        reduce_on_plateau_patience=3,
    )
    logger.info(f"Modele TFT : {sum(p.numel() for p in model.parameters()):,} parametres")
    return model


# ================================================================== #
# Fonction principale d'entrainement                                  #
# ================================================================== #
def train(config, debug=False, args=None):
    cfg_train = config["training"]
    cfg_paths = config["paths"]

    os.makedirs(cfg_paths["output_dir"],     exist_ok=True)
    os.makedirs(cfg_paths["checkpoint_dir"], exist_ok=True)
    os.makedirs(cfg_paths["logs_dir"],       exist_ok=True)

    # ------------------------------------------------------------------ #
    # 1. Chargement in-situ                                                #
    # -------------0----------------------------------------------------- #
    max_stations = 5000 if debug else None
    logger.info("Chargement des donnees in-situ...")
    df, altimetric_ids = build_dataframe(config, max_stations=max_stations,
                                         include_altimetric=False)
    insitu_ids = df["station_id"].unique().tolist()
    train_ids, val_ids = split_stations(insitu_ids, config)

    pd.Series(train_ids).to_csv(os.path.join(cfg_paths["output_dir"], "train_ids.csv"), index=False)
    pd.Series(val_ids).to_csv(os.path.join(cfg_paths["output_dir"],   "val_ids.csv"),   index=False)

    # ------------------------------------------------------------------ #
    # 2. Chargement altimétriques pour la validation secondaire           #
    # Les altimétriques sont toujours toutes chargées (seulement 137)     #
    # max_stations ne s affecte qu aux in-situ                            #
    # ------------------------------------------------------------------ #
    logger.info("Chargement des stations altimétriques...")
    alti_only_df, _ = build_dataframe(config, max_stations=0,
                                      include_altimetric=True)
    alti_df = alti_only_df[alti_only_df["station_id"].isin(altimetric_ids)].copy()
    logger.info(f"{alti_df['station_id'].nunique()} stations altimétriques chargees "
                f"({len(alti_df)} lignes)")

    # ------------------------------------------------------------------ #
    # 3. Datasets et dataloaders                                           #
    # ------------------------------------------------------------------ #
    train_dataset, val_dataset, reference_dataset = build_datasets(
        df, config, train_ids, val_ids, altimetric_ids)

    # Etend le vocabulaire du reference_dataset pour inclure les altimétriques
    # Sans ca, from_dataset leve un KeyError sur les station_id altimétriques
    logger.info("Extension du vocabulaire pour les stations altimétriques...")
    # all_df = train + val in-situ (reconstruit ici pour l extension)
    insitu_all_df = df[df["station_id"].isin(train_ids + val_ids)].copy()
    full_reference = extend_vocabulary(insitu_all_df, alti_df, config)
    alti_dataset   = build_alti_dataset(alti_df, full_reference)
    logger.info(f"Alti dataset : {len(alti_dataset)} fenetres")

    train_loader = train_dataset.to_dataloader(
        train=True,
        batch_size=cfg_train["batch_size"],
        num_workers=cfg_train["num_workers"],
        pin_memory=True,
        prefetch_factor=2,
        persistent_workers=True,
    )
    val_loader = val_dataset.to_dataloader(
        train=False,
        batch_size=cfg_train["batch_size"] * 2,
        num_workers=cfg_train["num_workers"],
        pin_memory=True,
        prefetch_factor=2,
        persistent_workers=True,
    )
    # Alti loader : pas besoin de workers persistants (peu de données)
    alti_loader = alti_dataset.to_dataloader(
        train=False,
        batch_size=cfg_train["batch_size"] * 2,
        num_workers=0,
        pin_memory=False,
    )

    # ------------------------------------------------------------------ #
    # 4. Modele                                                            #
    # ------------------------------------------------------------------ #
    model = build_model(train_dataset, config)

    # ------------------------------------------------------------------ #
    # 5. Callbacks                                                         #
    # ------------------------------------------------------------------ #
    alti_callback = AltimetricValidationCallback(
        alti_loader=alti_loader,
        quantiles=config["model"]["quantiles"],
        log_every_n_epochs=1,
    )

    callbacks = [
        # Validation primaire in-situ — pilote early stopping et checkpoint
        EarlyStopping(
            monitor="val_loss",
            patience=cfg_train["early_stopping_patience"],
            mode="min",
            verbose=True,
        ),
        LearningRateMonitor(logging_interval="epoch"),
        ModelCheckpoint(
            dirpath=cfg_paths["checkpoint_dir"],
            filename="tft-best-{epoch:02d}-{val_loss:.4f}",
            monitor="val_loss",
            mode="min",
            save_top_k=2,
        ),
        # Validation secondaire altimétriques — monitoring pur
        alti_callback,
    ]

    tb_logger = TensorBoardLogger(
        save_dir=cfg_paths["logs_dir"],
        name="tft_water_level",
    )

    # ------------------------------------------------------------------ #
    # 6. Trainer                                                           #
    # ------------------------------------------------------------------ #
    trainer = pl.Trainer(
        max_epochs=cfg_train["max_epochs"],
        gradient_clip_val=cfg_train["gradient_clip_val"],
        callbacks=callbacks,
        logger=tb_logger,
        enable_progress_bar=True,
        log_every_n_steps=50,
    )

    # ------------------------------------------------------------------ #
    # 7. LR Finder (optionnel)                                             #
    # ------------------------------------------------------------------ #
    no_lr_find = getattr(args, "no_lr_find", False)
    if not no_lr_find:
        from lightning.pytorch.tuner import Tuner
        logger.info("Recherche du learning rate optimal (~2-3 min)...")
        tuner = Tuner(pl.Trainer(
            max_epochs=1,
            gradient_clip_val=cfg_train["gradient_clip_val"],
            enable_progress_bar=False,
            logger=False,
        ))
        lr_finder = tuner.lr_find(
            model,
            train_dataloaders=train_loader,
            val_dataloaders=val_loader,
            min_lr=1e-5,
            max_lr=1e-1,
            num_training=100,
        )
        suggested_lr = lr_finder.suggestion()
        logger.info(f"LR suggere : {suggested_lr:.6f} (config : {cfg_train['learning_rate']})")
        model.hparams.learning_rate = suggested_lr
        fig = lr_finder.plot(suggest=True)
        fig.savefig(os.path.join(cfg_paths["output_dir"], "lr_finder.png"))
        logger.info("Courbe LR finder sauvegardee")

    # ------------------------------------------------------------------ #
    # 8. Entrainement                                                      #
    # ------------------------------------------------------------------ #
    logger.info("Demarrage de l'entrainement...")
    logger.info("Metriques loguees :")
    logger.info("  val_loss         — QuantileLoss in-situ  (pilote early stopping)")
    logger.info("  alti_val_loss    — QuantileLoss altimétriques (monitoring)")
    logger.info("  alti_val_mae     — MAE mediane altimétriques")
    logger.info("  alti_coverage_90 — Coverage IC 90% altimétriques (theorique=0.90)")
    logger.info("  alti_outlier_rate— Taux outliers detectes altimétriques")

    trainer.fit(model, train_dataloaders=train_loader, val_dataloaders=val_loader)

    best_ckpt = callbacks[2].best_model_path
    logger.info(f"Entrainement termine. Meilleur checkpoint : {best_ckpt}")
    return model, trainer


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config",     default="tft_config.yaml")
    parser.add_argument("--debug",      action="store_true")
    parser.add_argument("--no_lr_find", action="store_true")
    args = parser.parse_args()

    config = load_config(args.config)
    os.makedirs(config["paths"]["output_dir"], exist_ok=True)
    train(config, debug=args.debug, args=args)