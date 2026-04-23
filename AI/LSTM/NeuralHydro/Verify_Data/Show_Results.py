"""
Affiche les métriques de validation par epoch + le config du run.
"""

import pandas as pd
import pickle
import numpy as np
from pathlib import Path

RUN_DIR = Path("./runs/satellite_water_level_test_1704_160204")

# ═══════════════════════════════════════════════════════════════
# MÉTRIQUES DE VALIDATION PAR EPOCH
# ═══════════════════════════════════════════════════════════════
print("=" * 60)
print("MÉTRIQUES DE VALIDATION")
print("=" * 60)
print(f"{'Epoch':>6}  {'NSE médiane':>12}  {'KGE médiane':>12}  {'Loss':>10}")
print("-" * 60)

val_dir = RUN_DIR / "validation"
epochs  = sorted(val_dir.glob("model_epoch*"))

for epoch_dir in epochs:
    epoch_num = int(epoch_dir.name.replace("model_epoch", ""))
    csv_path  = epoch_dir / "validation_metrics.csv"

    if not csv_path.exists():
        continue

    df = pd.read_csv(csv_path)

    # NSE et KGE médiane sur toutes les stations
    nse_col = [c for c in df.columns if 'NSE' in c]
    kge_col = [c for c in df.columns if 'KGE' in c]
    loss_col = [c for c in df.columns if 'loss' in c.lower()]

    nse_med  = df[nse_col[0]].median()  if nse_col  else np.nan
    kge_med  = df[kge_col[0]].median()  if kge_col  else np.nan
    loss_val = df[loss_col[0]].median() if loss_col else np.nan

    print(f"  {epoch_num:>4}  {nse_med:>12.4f}  {kge_med:>12.4f}  {loss_val:>10.5f}")

# Meilleur epoch
best_epoch = None
best_nse   = -np.inf
for epoch_dir in epochs:
    csv_path = epoch_dir / "validation_metrics.csv"
    if not csv_path.exists():
        continue
    df      = pd.read_csv(csv_path)
    nse_col = [c for c in df.columns if 'NSE' in c]
    if not nse_col:
        continue
    nse = df[nse_col[0]].median()
    if nse > best_nse:
        best_nse   = nse
        best_epoch = int(epoch_dir.name.replace("model_epoch", ""))

print(f"\n  → Meilleur epoch : {best_epoch}  (NSE médiane = {best_nse:.4f})")

# ═══════════════════════════════════════════════════════════════
# CONFIG DU RUN
# ═══════════════════════════════════════════════════════════════
print("\n\n" + "=" * 60)
print("CONFIG DU RUN")
print("=" * 60)

config_path = RUN_DIR / "config.yml"
if config_path.exists():
    with open(config_path, "r") as f:
        print(f.read())
else:
    print("⚠️  config.yml non trouvé")