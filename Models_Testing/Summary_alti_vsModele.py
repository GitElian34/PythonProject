"""
summary_modele_vs_alti_from_csv.py
═══════════════════════════════════════════════════════════════════
Relit les CSV déjà calculés par eval_metrics_hwnext_sword_insitu.py
(pas de recalcul de connectivité SWORD, juste de l'agrégation pandas
-> quasi instantané) et affiche :
  1. Modèle vs Insitu
  2. Alti vs Insitu (baseline, sans modèle)
  3. Gain médian Modèle - Alti + % stations où le modèle bat l'alti

Usage :
    python summary_modele_vs_alti_from_csv.py
═══════════════════════════════════════════════════════════════════
"""

import numpy as np
import pandas as pd
from pathlib import Path

RESIDUALS_DIR = Path("./Models_Testing/Residus")

FILES = [
    {"freq": "10j", "csv": RESIDUALS_DIR / "metrics_10j_hwnext_sword_insitu.csv"},
    {"freq": "27j", "csv": RESIDUALS_DIR / "metrics_27j_hwnext_sword_insitu.csv"},
]

summaries = []

for f in FILES:
    freq, csv_path = f["freq"], f["csv"]
    if not csv_path.exists():
        print(f"⚠ Fichier introuvable : {csv_path} -> ignoré")
        continue

    df = pd.read_csv(csv_path)
    row = {"freq": freq, "n_stations": len(df)}

    for m in ["NSE", "KGE", "RMSE", "R2"]:
        v = df[m].dropna()
        row[f"{m}_median"] = round(v.median(), 3) if len(v) else np.nan

        col_alti = f"{m}_alti_insitu"
        v_alti = df[col_alti].dropna()
        row[f"{col_alti}_median"] = round(v_alti.median(), 3) if len(v_alti) else np.nan

        merged = df[[m, col_alti]].dropna()
        if len(merged):
            higher_is_better = m != "RMSE"
            if higher_is_better:
                gain = merged[m] - merged[col_alti]
            else:
                gain = merged[col_alti] - merged[m]
            row[f"gain_{m}_median"] = round(gain.median(), 3)
            row[f"pct_modele_meilleur_{m}"] = round((gain > 0).mean() * 100, 1)

    summaries.append(row)

df_summary = pd.DataFrame(summaries)

print(f"\n--- Modèle vs Insitu ---")
print(f"{'':10} {'stations':>10} {'NSE':>8} {'KGE':>8} {'RMSE':>8} {'R2':>8}")
print("-" * 60)
for _, row in df_summary.iterrows():
    print(f"{row['freq']:<10} {row['n_stations']:>10} "
          f"{row.get('NSE_median', np.nan):>8.3f} "
          f"{row.get('KGE_median', np.nan):>8.3f} "
          f"{row.get('RMSE_median', np.nan):>8.3f} "
          f"{row.get('R2_median', np.nan):>8.3f}")

print(f"\n--- Alti vs Insitu (baseline, sans modèle) ---")
print(f"{'':10} {'NSE':>8} {'KGE':>8} {'RMSE':>8} {'R2':>8}")
print("-" * 60)
for _, row in df_summary.iterrows():
    print(f"{row['freq']:<10} "
          f"{row.get('NSE_alti_insitu_median', np.nan):>8.3f} "
          f"{row.get('KGE_alti_insitu_median', np.nan):>8.3f} "
          f"{row.get('RMSE_alti_insitu_median', np.nan):>8.3f} "
          f"{row.get('R2_alti_insitu_median', np.nan):>8.3f}")

print(f"\n--- Gain médian Modèle - Alti (positif = modèle apporte qqch) ---")
print(f"{'':10} {'NSE':>8} {'KGE':>8} {'RMSE':>8} {'R2':>8}   {'% modèle > alti (NSE)':>22}")
print("-" * 60)
for _, row in df_summary.iterrows():
    print(f"{row['freq']:<10} "
          f"{row.get('gain_NSE_median', np.nan):>8.3f} "
          f"{row.get('gain_KGE_median', np.nan):>8.3f} "
          f"{row.get('gain_RMSE_median', np.nan):>8.3f} "
          f"{row.get('gain_R2_median', np.nan):>8.3f}   "
          f"{row.get('pct_modele_meilleur_NSE', np.nan):>20.1f}%")

out_path = RESIDUALS_DIR / "summary_modele_vs_alti.csv"
df_summary.to_csv(out_path, index=False)
print(f"\n✅ Résumé -> {out_path}")