from pathlib import Path
import pandas as pd
import numpy as np

RUN_DIR = Path("./runs/arlstm_feat10jLow_modele2_3004_130415")

# Chercher le dossier test
test_dir = RUN_DIR / "test"
epoch_dirs = sorted(test_dir.glob("model_epoch*"))

for epoch_dir in epoch_dirs:
    csv_path = epoch_dir / "test_metrics.csv"
    if not csv_path.exists():
        continue
    df = pd.read_csv(csv_path)
    nse_col = [c for c in df.columns if 'NSE' in c]
    kge_col = [c for c in df.columns if 'KGE' in c]
    nse_med = df[nse_col[0]].median() if nse_col else np.nan
    kge_med = df[kge_col[0]].median() if kge_col else np.nan
    epoch   = int(epoch_dir.name.replace("model_epoch", ""))
    print(f"Epoch {epoch:>3} — NSE médian = {nse_med:.4f}  KGE médian = {kge_med:.4f}")