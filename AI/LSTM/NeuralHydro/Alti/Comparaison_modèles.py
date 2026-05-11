"""
compare_models_terminal.py
═══════════════════════════════════════════════════════════════════════════
Compare les NSE/KGE de plusieurs modèles sur les stations satellite.
Met en avant les stations où un modèle fait nettement mieux que les autres.

Usage :
  python compare_models_terminal.py
═══════════════════════════════════════════════════════════════════════════
"""

import pandas as pd
import numpy as np
from pathlib import Path

# ═══════════════════════════════════════════════════════════════
# MODÈLES
# ═══════════════════════════════════════════════════════════════
MODELS = {
    "0505_ep2":  "runs/arlstm_feat10jLow_modele2_0505_121508/test/model_epoch002/test_metrics.csv",
    "0605a_ep3": "runs/arlstm_feat10jLow_modele2_0605_124417/validation/model_epoch003/validation_metrics.csv",
    "0605b_ep3": "runs/arlstm_feat10jLow_modele2_0605_123933/validation/model_epoch003/validation_metrics.csv",
    "0605b_ep9": "runs/arlstm_feat10jLow_modele2_0605_123933/validation/model_epoch009/validation_metrics.csv",
}

SEUIL_DIFF = 0.10  # écart NSE/KGE pour considérer une différence notable

# ═══════════════════════════════════════════════════════════════
# 1. Charger
# ═══════════════════════════════════════════════════════════════
all_metrics = {}
for label, csv_path in MODELS.items():
    p = Path(csv_path)
    if not p.exists():
        print(f"❌ {label} : introuvable")
        continue
    df = pd.read_csv(p, header=None, names=["station", "NSE", "KGE"])
    df["NSE"] = pd.to_numeric(df["NSE"], errors="coerce")
    df["KGE"] = pd.to_numeric(df["KGE"], errors="coerce")
    df["station"] = df["station"].astype(str)
    all_metrics[label] = df.set_index("station")

labels = list(all_metrics.keys())
common = sorted(set.intersection(*[set(df.index) for df in all_metrics.values()]))

# ═══════════════════════════════════════════════════════════════
# 2. Récap global
# ═══════════════════════════════════════════════════════════════
print("=" * 70)
print("RÉCAP GLOBAL")
print("=" * 70)
print(f"{'Modèle':>12s} | {'NSE méd':>8s} | {'NSE moy':>8s} | {'KGE méd':>8s} | {'KGE moy':>8s}")
print("-" * 70)
for l in labels:
    df = all_metrics[l].loc[common]
    print(f"{l:>12s} | {df['NSE'].median():8.3f} | {df['NSE'].mean():8.3f} | "
          f"{df['KGE'].median():8.3f} | {df['KGE'].mean():8.3f}")

# ═══════════════════════════════════════════════════════════════
# 3. Pour chaque station : meilleur modèle et écart
# ═══════════════════════════════════════════════════════════════
print(f"\n{'=' * 70}")
print(f"MEILLEUR MODÈLE PAR STATION (écart ≥ {SEUIL_DIFF})")
print(f"{'=' * 70}")

for metric in ["NSE", "KGE"]:
    print(f"\n--- {metric} ---")
    print(f"{'Station':>15s} | {'Meilleur':>12s} | {'Val':>7s} | {'2e':>12s} | {'Val':>7s} | {'Écart':>6s}")
    print("-" * 70)

    n_notable = 0
    for sta in common:
        vals = {l: all_metrics[l].loc[sta, metric] for l in labels}
        ranked = sorted(vals.items(), key=lambda x: x[1], reverse=True)
        best_label, best_val = ranked[0]
        second_label, second_val = ranked[1]
        ecart = best_val - second_val

        if ecart >= SEUIL_DIFF:
            n_notable += 1
            print(f"{sta:>15s} | {best_label:>12s} | {best_val:+7.3f} | "
                  f"{second_label:>12s} | {second_val:+7.3f} | {ecart:+6.3f} ◄")

    if n_notable == 0:
        print("  Aucune station avec écart notable.")
    else:
        print(f"\n  → {n_notable} stations avec écart {metric} ≥ {SEUIL_DIFF}")

# ═══════════════════════════════════════════════════════════════
# 4. Stations où les nouveaux modèles améliorent le baseline
# ═══════════════════════════════════════════════════════════════
baseline = labels[0]
print(f"\n{'=' * 70}")
print(f"GAINS / PERTES vs BASELINE ({baseline})")
print(f"{'=' * 70}")

for l in labels[1:]:
    diff_nse = all_metrics[l].loc[common, "NSE"] - all_metrics[baseline].loc[common, "NSE"]
    diff_kge = all_metrics[l].loc[common, "KGE"] - all_metrics[baseline].loc[common, "KGE"]

    gains_nse = diff_nse[diff_nse > SEUIL_DIFF].sort_values(ascending=False)
    pertes_nse = diff_nse[diff_nse < -SEUIL_DIFF].sort_values()

    print(f"\n--- {l} vs {baseline} ---")
    print(f"  NSE : {(diff_nse > 0).sum()} améliorées, {(diff_nse < 0).sum()} dégradées, {(diff_nse == 0).sum()} stables")
    print(f"  KGE : {(diff_kge > 0).sum()} améliorées, {(diff_kge < 0).sum()} dégradées")

    if len(gains_nse) > 0:
        print(f"\n  🟢 Gains NSE notables (>{SEUIL_DIFF}) :")
        for sta, delta in gains_nse.items():
            old = all_metrics[baseline].loc[sta, "NSE"]
            new = all_metrics[l].loc[sta, "NSE"]
            print(f"     {sta:>15s} : {old:+.3f} → {new:+.3f}  (Δ={delta:+.3f})")

    if len(pertes_nse) > 0:
        print(f"\n  🔴 Pertes NSE notables (<-{SEUIL_DIFF}) :")
        for sta, delta in pertes_nse.items():
            old = all_metrics[baseline].loc[sta, "NSE"]
            new = all_metrics[l].loc[sta, "NSE"]
            print(f"     {sta:>15s} : {old:+.3f} → {new:+.3f}  (Δ={delta:+.3f})")