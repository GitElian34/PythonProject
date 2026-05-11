#!/usr/bin/env python3
"""
evaluate.py — Évaluation finale du Bi-LSTM + comparaison avec NeuralHydrology
"""

import numpy as np
import torch
import matplotlib.pyplot as plt
from torch.utils.data import DataLoader
from config import MODEL_DIR, BATCH_SIZE
from train import compute_metrics


def evaluate(model, val_ds, device):
    """
    Évalue le meilleur modèle sauvegardé sur le dataset de validation.
    Retourne NSE et KGE finaux et affiche le tableau comparatif.
    """
    model.load_state_dict(torch.load(MODEL_DIR / "bilstm_best.pt"))
    model.eval()

    val_loader = DataLoader(val_ds, batch_size=BATCH_SIZE,
                            shuffle=False, num_workers=0)

    all_pred, all_obs = [], []
    with torch.no_grad():
        for seq_b, stat_b, tgt_b in val_loader:
            seq_b  = seq_b.to(device)
            stat_b = stat_b.to(device)
            pred   = model(seq_b, stat_b).cpu().numpy()
            all_pred.extend(pred.tolist())
            all_obs.extend(tgt_b.numpy().tolist())

    all_pred = np.array(all_pred)
    all_obs  = np.array(all_obs)
    nse, kge = compute_metrics(all_pred, all_obs)

    # ── Tableau comparatif ────────────────────────────────────────────────
    print(f"\n{'='*50}")
    print(f"  RÉSULTATS FINAUX — Bi-LSTM ±30j")
    print(f"  NSE : {nse:.3f}")
    print(f"  KGE : {kge:.3f}")
    print(f"{'='*50}")

    print(f"""
📋 Comparaison avec NeuralHydrology (même dataset) :

  Modèle              NSE      KGE
  ─────────────────────────────────
  CudaLSTM           ~0.55   ~0.45
  EA-LSTM            ~0.56   ~0.42
  AR-LSTM lag-27     ~0.60   ~0.54
  AR-LSTM lag-1      ~0.88   ~0.89
  Bi-LSTM ±30j        {nse:.3f}    {kge:.3f}  ← nouveau
""")

    # ── Plot résidus ──────────────────────────────────────────────────────
    residus = all_obs - all_pred
    fig, axes = plt.subplots(1, 2, figsize=(12, 4))
    fig.suptitle(f"Bi-LSTM ±30j — NSE={nse:.3f} | KGE={kge:.3f}",
                 fontweight="bold")

    # Scatter obs vs sim
    ax = axes[0]
    ax.scatter(all_obs, all_pred, alpha=0.1, s=3, color="#3b82f6")
    lim = [min(all_obs.min(), all_pred.min()),
           max(all_obs.max(), all_pred.max())]
    ax.plot(lim, lim, "r--", linewidth=1.5, label="obs = sim")
    ax.set_xlabel("Observé (normalisé)")
    ax.set_ylabel("Simulé (normalisé)")
    ax.set_title("Obs vs Sim")
    ax.legend()
    ax.spines[["top", "right"]].set_visible(False)

    # Distribution des résidus
    ax = axes[1]
    ax.hist(residus, bins=80, color="#8b5cf6",
            edgecolor="white", linewidth=0.2, alpha=0.85, density=True)
    ax.axvline(0, color="black", linewidth=1.5, label="résidu = 0")
    ax.axvline(np.mean(residus), color="#f97316", linewidth=2,
               linestyle="--", label=f"biais = {np.mean(residus):.4f}")
    ax.set_xlabel("Résidu (obs - sim)")
    ax.set_ylabel("Densité")
    ax.set_title("Distribution des résidus")
    ax.legend()
    ax.spines[["top", "right"]].set_visible(False)

    plt.tight_layout()
    out = MODEL_DIR / "bilstm_eval.png"
    plt.savefig(out, dpi=150, bbox_inches="tight")
    print(f"✅ Figure sauvegardée : {out}")
    plt.show()

    return nse, kge