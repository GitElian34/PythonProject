#!/usr/bin/env python3
"""
train.py — Boucle d'entraînement du Bi-LSTM
"""

import numpy as np
import torch
import torch.nn as nn
from torch.utils.data import DataLoader
from config import (
    MODEL_DIR, BATCH_SIZE, EPOCHS,
    LR, LR_DECAY_EP
)


def nse_loss(pred, obs):
    """
    NSE comme fonction de loss — on minimise 1-NSE.
    Identique à ce que fait NeuralHydrology avec loss: NSE.
    """
    mean_obs = obs.mean()
    ss_res   = ((obs - pred)**2).sum()
    ss_tot   = ((obs - mean_obs)**2).sum()
    return ss_res / (ss_tot + 1e-8)


def compute_metrics(pred_np, obs_np):
    """Calcule NSE et KGE sur des arrays numpy."""
    mask = ~np.isnan(obs_np) & ~np.isnan(pred_np)
    obs, pred = obs_np[mask], pred_np[mask]
    if len(obs) < 10:
        return np.nan, np.nan

    # NSE
    nse = 1 - np.sum((obs - pred)**2) / (np.sum((obs - np.mean(obs))**2) + 1e-8)

    # KGE — décomposé en corrélation, variabilité, biais
    r     = np.corrcoef(obs, pred)[0, 1]
    alpha = np.std(pred)  / (np.std(obs)  + 1e-8)
    beta  = np.mean(pred) / (np.mean(obs) + 1e-8)
    kge   = 1 - np.sqrt((r-1)**2 + (alpha-1)**2 + (beta-1)**2)

    return float(nse), float(kge)


def train(model, train_ds, val_ds, device):
    """
    Entraîne le modèle et retourne le meilleur NSE de validation.
    Sauvegarde le meilleur modèle dans MODEL_DIR/bilstm_best.pt
    """
    MODEL_DIR.mkdir(parents=True, exist_ok=True)

    train_loader = DataLoader(train_ds, batch_size=BATCH_SIZE,
                              shuffle=True,  num_workers=0)
    val_loader   = DataLoader(val_ds,   batch_size=BATCH_SIZE,
                              shuffle=False, num_workers=0)

    optimizer = torch.optim.AdamW(model.parameters(), lr=LR, weight_decay=1e-4)
    scheduler = torch.optim.lr_scheduler.StepLR(
        optimizer, step_size=LR_DECAY_EP, gamma=0.1
    )

    best_nse   = -np.inf
    best_epoch = 0

    print(f"\n🏋️  Entraînement — {EPOCHS} epochs")
    print(f"{'Epoch':>6}  {'Loss train':>11}  {'NSE val':>8}  {'KGE val':>8}")
    print("-" * 42)

    for epoch in range(1, EPOCHS + 1):

        # ── Train ─────────────────────────────────────────────────────────
        model.train()
        losses = []
        for seq_b, stat_b, tgt_b in train_loader:
            seq_b  = seq_b.to(device)
            stat_b = stat_b.to(device)
            tgt_b  = tgt_b.to(device)

            optimizer.zero_grad()
            pred = model(seq_b, stat_b)
            loss = nse_loss(pred, tgt_b)
            loss.backward()
            nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            optimizer.step()
            losses.append(loss.item())

        scheduler.step()
        avg_loss = np.mean(losses)

        # ── Validation tous les 5 epochs ──────────────────────────────────
        if epoch % 5 == 0:
            model.eval()
            all_pred, all_obs = [], []
            with torch.no_grad():
                for seq_b, stat_b, tgt_b in val_loader:
                    seq_b  = seq_b.to(device)
                    stat_b = stat_b.to(device)
                    pred   = model(seq_b, stat_b).cpu().numpy()
                    all_pred.extend(pred.tolist())
                    all_obs.extend(tgt_b.numpy().tolist())

            nse_val, kge_val = compute_metrics(
                np.array(all_pred), np.array(all_obs)
            )
            print(f"{epoch:>6}  {avg_loss:>11.4f}  {nse_val:>8.3f}  {kge_val:>8.3f}")

            if nse_val > best_nse:
                best_nse   = nse_val
                best_epoch = epoch
                torch.save(model.state_dict(), MODEL_DIR / "bilstm_best.pt")
        else:
            print(f"{epoch:>6}  {avg_loss:>11.4f}")

    print(f"\n✅ Meilleur modèle : epoch {best_epoch} | NSE val = {best_nse:.3f}")
    return best_nse