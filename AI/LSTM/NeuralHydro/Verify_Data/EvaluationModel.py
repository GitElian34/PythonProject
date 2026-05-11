#!/usr/bin/env python3
"""
eval_multi_epochs.py
Évalue plusieurs epochs d'un run NeuralHydrology et compare NSE/KGE.
"""

from pathlib import Path
import pandas as pd
import torch
import numpy as np
import matplotlib.pyplot as plt
from neuralhydrology.utils.config import Config
from neuralhydrology.evaluation.evaluate import start_evaluation


torch.set_num_threads(8)
# ─── Paramètres ─────────────────────────────────────────────────────────────
RUN_DIR = Path("./runs/arlstm_feat10jLow_modele2_3004_130415")
EPOCHS  = [2, 4, 5]   # epochs à évaluer

# Dates de test — à adapter selon ton run


# ─── Config ──────────────────────────────────────────────────────────────────
cfg = Config(RUN_DIR / "config.yml")

# ─── Évaluation par epoch ────────────────────────────────────────────────────
print(f"Run : {RUN_DIR.name}")
print(f"Epochs à évaluer : {EPOCHS}\n")

results = []

for epoch in EPOCHS:
    # Vérifier si déjà évalué
    test_dir = RUN_DIR / "test"
    existing = list(test_dir.glob(f"model_epoch{epoch:03d}*/test_metrics.csv"))

    if existing:
        print(f"Epoch {epoch:>3} — déjà évalué, lecture du CSV...")
        csv_path = existing[0]
    else:
        print(f"Epoch {epoch:>3} — lancement évaluation...")
        start_evaluation(cfg=cfg, run_dir=RUN_DIR, epoch=epoch, period="test")
        candidates = list(test_dir.glob(f"model_epoch{epoch:03d}*/test_metrics.csv"))
        if not candidates:
            print(f"  ⚠️  Pas de résultats trouvés pour epoch {epoch}")
            continue
        csv_path = candidates[0]

    # Lire les métriques
    df      = pd.read_csv(csv_path)
    nse_col = [c for c in df.columns if 'NSE' in c]
    kge_col = [c for c in df.columns if 'KGE' in c]
    nse_med = df[nse_col[0]].median() if nse_col else np.nan
    kge_med = df[kge_col[0]].median() if kge_col else np.nan
    nse_std = df[nse_col[0]].std()    if nse_col else np.nan
    kge_std = df[kge_col[0]].std()    if kge_col else np.nan

    results.append({
        "epoch"  : epoch,
        "nse_med": nse_med,
        "kge_med": kge_med,
        "nse_std": nse_std,
        "kge_std": kge_std,
    })
    print(f"  NSE médian = {nse_med:.4f}  KGE médian = {kge_med:.4f}")

# ─── Tableau récap ───────────────────────────────────────────────────────────
df_res = pd.DataFrame(results)
best_nse = df_res.loc[df_res["nse_med"].idxmax()]
best_kge = df_res.loc[df_res["kge_med"].idxmax()]

print(f"\n{'='*50}")
print(f"{'Epoch':>6}  {'NSE médian':>11}  {'KGE médian':>11}")
print("-" * 35)
for _, r in df_res.iterrows():
    marker = " ← meilleur NSE" if r["epoch"] == best_nse["epoch"] else ""
    print(f"{int(r['epoch']):>6}  {r['nse_med']:>11.4f}  {r['kge_med']:>11.4f}{marker}")
print(f"\n  Meilleur NSE : epoch {int(best_nse['epoch'])} ({best_nse['nse_med']:.4f})")
print(f"  Meilleur KGE : epoch {int(best_kge['epoch'])} ({best_kge['kge_med']:.4f})")

# ─── Graphiques ──────────────────────────────────────────────────────────────
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4))
fig.suptitle(f"Comparaison epochs — {RUN_DIR.name}", fontweight="bold")

# NSE
ax1.plot(df_res["epoch"], df_res["nse_med"], "o-", color="#3b82f6",
         linewidth=2, markersize=7, label="NSE médian")
ax1.fill_between(df_res["epoch"],
                 df_res["nse_med"] - df_res["nse_std"],
                 df_res["nse_med"] + df_res["nse_std"],
                 alpha=0.15, color="#3b82f6", label="±1 std")
ax1.axvline(best_nse["epoch"], color="#ef4444", linestyle="--",
            linewidth=1, label=f"Meilleur epoch {int(best_nse['epoch'])}")
ax1.set_xlabel("Epoch")
ax1.set_ylabel("NSE médian")
ax1.set_title("NSE")
ax1.legend(fontsize=9)
ax1.grid(axis="y", alpha=0.3)
ax1.spines[["top", "right"]].set_visible(False)

# KGE
ax2.plot(df_res["epoch"], df_res["kge_med"], "o-", color="#10b981",
         linewidth=2, markersize=7, label="KGE médian")
ax2.fill_between(df_res["epoch"],
                 df_res["kge_med"] - df_res["kge_std"],
                 df_res["kge_med"] + df_res["kge_std"],
                 alpha=0.15, color="#10b981", label="±1 std")
ax2.axvline(best_kge["epoch"], color="#ef4444", linestyle="--",
            linewidth=1, label=f"Meilleur epoch {int(best_kge['epoch'])}")
ax2.set_xlabel("Epoch")
ax2.set_ylabel("KGE médian")
ax2.set_title("KGE")
ax2.legend(fontsize=9)
ax2.grid(axis="y", alpha=0.3)
ax2.spines[["top", "right"]].set_visible(False)

plt.tight_layout()
out = f"eval_epochs_{RUN_DIR.name}.png"
plt.savefig(out, dpi=150, bbox_inches="tight")
print(f"\n✅ Graphique sauvegardé : {out}")
plt.show()