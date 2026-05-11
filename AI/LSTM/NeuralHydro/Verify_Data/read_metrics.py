"""
Lit les validation_metrics.csv de chaque epoch et affiche la médiane NSE / KGE.
Usage : python read_metrics.py <chemin_du_run>
Exemple : python read_metrics.py runs/arlstm_feat10jLow_modele2_3004_130415
"""

import sys, os, csv
from pathlib import Path

def main():
    if len(sys.argv) < 2:
        print("Usage : python read_metrics.py <chemin_du_run>")
        sys.exit(1)

    run_dir = Path(sys.argv[1])
    val_dir = run_dir / "validation"

    if not val_dir.exists():
        print(f"Dossier validation/ introuvable dans {run_dir}")
        sys.exit(1)

    # Trouver tous les dossiers model_epochXXX
    epoch_dirs = sorted(val_dir.glob("model_epoch*"))
    if not epoch_dirs:
        print("Aucun dossier model_epoch* trouvé.")
        sys.exit(1)

    print(f"{'Epoch':>8}  {'NSE médian':>12}  {'KGE médian':>12}  {'Stations':>10}")
    print("-" * 50)

    for ep_dir in epoch_dirs:
        csv_path = ep_dir / "validation_metrics.csv"
        if not csv_path.exists():
            continue

        nse_vals, kge_vals = [], []
        with open(csv_path) as f:
            reader = csv.reader(f)
            for row in reader:
                if len(row) < 3:
                    continue
                try:
                    nse_vals.append(float(row[1]))
                    kge_vals.append(float(row[2]))
                except ValueError:
                    continue  # header ou ligne invalide

        if not nse_vals:
            continue

        nse_vals.sort()
        kge_vals.sort()
        n = len(nse_vals)
        med_nse = (nse_vals[n // 2] + nse_vals[(n - 1) // 2]) / 2
        med_kge = (kge_vals[n // 2] + kge_vals[(n - 1) // 2]) / 2

        epoch_num = ep_dir.name.replace("model_epoch", "")
        print(f"{epoch_num:>8}  {med_nse:>12.4f}  {med_kge:>12.4f}  {n:>10}")

if __name__ == "__main__":
    main()