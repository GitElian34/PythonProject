"""
graphs_synthese_4_datasets.py
════════════════════════════════════════════════════════════════════════
Génère 4 figures de synthèse (une par dataset : HWNext 10j/27j, DAHITI
10j/27j), chacune composée de :

  - un bar plot : pour chaque métrique (NSE, KGE, RMSE, R²), % de
    stations où le modèle bat l'alti SUR CETTE métrique seule

  - un camembert : % de stations où le modèle bat l'alti sur AU MOINS
    MIN_METRICS_OK des 4 métriques SIMULTANÉMENT (analyse station par
    station)

IMPORTANT — ce script lit DIRECTEMENT metrics_per_station.csv et
baseline_alti_vs_insitu.csv (produits par
analyse_residus_NSE_vs_RMSE_insitu_generic.py), qui contiennent les 6
modèles (DtoD80/90/96 x NSE/RMSE). Il NE dépend PAS de
resume_gain_modele_vs_alti.csv / gain_modele_vs_alti.csv, qui eux ne
contiennent que le top N sélectionné par
comparaison_gain_modele_vs_alti_generic.py — donc forcer un modèle qui
n'était pas dans ce top N (cf. MODEL_OVERRIDE) fonctionne toujours,
peu importe son rang.

Le "meilleur modèle" automatique = 1ère ligne de ranking_vs_insitu.csv.

Entrées (par dataset, dans nse_vs_rmse_{source}_{freq}/) :
  metrics_per_station.csv       (les 6 modèles, colonnes *_modele_insitu)
  baseline_alti_vs_insitu.csv   (colonnes *_alti_insitu, indépendant du modèle)
  ranking_vs_insitu.csv         (pour identifier le meilleur modèle par défaut)

Sorties :
  ./data_processing/AnalyseModelsDtoD/RMSEvsNSE/Graph/synthese_{source}_{freq}.png
════════════════════════════════════════════════════════════════════════
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
DATASETS = {
    "HWNext 10j" : ("hwnext", "10j"),
    "HWNext 27j" : ("hwnext", "27j"),
    "DAHITI 10j" : ("dahiti", "10j"),
    "DAHITI 27j" : ("dahiti", "27j"),
}

# Surcharge manuelle du modèle retenu pour les datasets où le classement automatique
# était trop serré pour être tranché avec confiance.
MODEL_OVERRIDE = {
    ("hwnext", "10j"): "DtoD80_NSE",   # au lieu de DtoD96_NSE (écart de rang faible)
    ("dahiti", "10j"): "DtoD96_NSE",   # forcé : l'auto donne DtoD90_NSE, mais on veut DtoD96_NSE
}

BASE_DIR   = Path("./data_processing/AnalyseModelsDtoD")
OUTPUT_DIR = BASE_DIR / "RMSEvsNSE/Graph"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

METRIC_DIRECTION = {"NSE": True, "KGE": True, "RMSE": False, "R2": True}  # True = plus haut est meilleur
METRICS = list(METRIC_DIRECTION.keys())

MIN_METRICS_OK = 3   # nb minimal de métriques (sur 4) où le modèle doit gagner, par station

C_MODELE = "#1565C0"
C_ALTI   = "#9E9E9E"
C_WIN    = "#2E7D32"
C_LOSE   = "#C0392B"

# ═══════════════════════════════════════════════════════════════
# GÉNÉRATION DES 4 FIGURES
# ═══════════════════════════════════════════════════════════════
for label, (source, freq) in DATASETS.items():
    dataset_dir   = BASE_DIR / f"nse_vs_rmse_{source}_{freq}"
    metrics_csv   = dataset_dir / "metrics_per_station.csv"
    baseline_csv  = dataset_dir / "baseline_alti_vs_insitu.csv"
    ranking_csv   = dataset_dir / "ranking_vs_insitu.csv"

    if not metrics_csv.exists() or not baseline_csv.exists():
        print(f"⚠ {label} : fichier(s) introuvable(s) dans {dataset_dir} -> ignoré")
        continue

    df_metrics  = pd.read_csv(metrics_csv)
    df_baseline = pd.read_csv(baseline_csv)
    df_metrics["station"]  = df_metrics["station"].astype(str)
    df_baseline["station"] = df_baseline["station"].astype(str)

    available_models = df_metrics["model"].unique().tolist()

    # ── Détermination du modèle à utiliser (override ou meilleur auto) ──
    override_model = MODEL_OVERRIDE.get((source, freq))
    if override_model is not None:
        if override_model not in available_models:
            print(f"⚠ {label} : modèle forcé '{override_model}' absent de metrics_per_station.csv "
                  f"(modèles dispo : {available_models}) -> ignoré pour ce dataset")
            continue
        model_name = override_model
        print(f"  ({label} : modèle forcé manuellement -> {model_name})")
    elif ranking_csv.exists():
        df_rank = pd.read_csv(ranking_csv).sort_values("rang_moyen")
        model_name = df_rank.iloc[0]["model"]
    else:
        print(f"⚠ {label} : ni MODEL_OVERRIDE ni ranking_vs_insitu.csv disponible -> ignoré")
        continue

    # ── Reconstruction des métriques pour CE modèle, depuis les données complètes ──
    df_model = df_metrics[df_metrics["model"] == model_name].copy()
    df_merged = df_model.merge(
        df_baseline[["station", "NSE_alti_insitu", "KGE_alti_insitu", "RMSE_alti_insitu", "R2_alti_insitu"]],
        on="station", how="inner"
    )
    needed_cols = [f"{m}_modele_insitu" for m in METRICS] + [f"{m}_alti_insitu" for m in METRICS]
    df_merged = df_merged.dropna(subset=needed_cols, how="any")

    n_total = len(df_merged)
    if n_total == 0:
        print(f"⚠ {label} : aucune station avec toutes les métriques disponibles pour {model_name} -> ignoré")
        continue

    # Gains station par station (pour le camembert)
    for metric, higher_is_better in METRIC_DIRECTION.items():
        col_mod, col_alti = f"{metric}_modele_insitu", f"{metric}_alti_insitu"
        if higher_is_better:
            df_merged[f"gain_{metric}"] = df_merged[col_mod] - df_merged[col_alti]
        else:
            df_merged[f"gain_{metric}"] = df_merged[col_alti] - df_merged[col_mod]

    # ── Bar plot : médianes + % victoire par métrique seule ──
    med_modele, med_alti, pct_wins, wins_metric = [], [], [], []
    for metric, higher_is_better in METRIC_DIRECTION.items():
        col_mod, col_alti = f"{metric}_modele_insitu", f"{metric}_alti_insitu"
        med_modele.append(df_merged[col_mod].median())
        med_alti.append(df_merged[col_alti].median())
        gain = df_merged[f"gain_{metric}"]
        pct = (gain > 0).mean() * 100
        pct_wins.append(pct)
        wins_metric.append(pct > 50.0)

    # ── Camembert : analyse station par station (>= MIN_METRICS_OK / 4) ──
    gain_cols = [f"gain_{m}" for m in METRICS]
    win_count_per_station = (df_merged[gain_cols] > 0).sum(axis=1)
    n_ok    = int((win_count_per_station >= MIN_METRICS_OK).sum())
    n_not_ok = n_total - n_ok
    pct_ok  = n_ok / n_total * 100

    distrib = win_count_per_station.value_counts().reindex(range(len(METRICS) + 1), fill_value=0)

    # ── Figure ───────────────────────────────────────────────────
    fig, (ax_bar, ax_pie) = plt.subplots(1, 2, figsize=(12, 5.5))
    fig.suptitle(
        f"{label} — modèle : {model_name}  (n={n_total} stations)\n"
        f"Comparaison vs insitu LE PLUS PROCHE en distance (≤ 50 km)",
        fontsize=12.5, fontweight="bold"
    )

    x = np.arange(len(METRICS))
    width = 0.35
    bars_mod  = ax_bar.bar(x - width/2, med_modele, width, label=f"{model_name} vs insitu (médiane)", color=C_MODELE)
    bars_alti = ax_bar.bar(x + width/2, med_alti,   width, label="Alti brute vs insitu (médiane)",    color=C_ALTI)

    for bar, win in zip(bars_mod, wins_metric):
        bar.set_edgecolor(C_WIN if win else C_LOSE)
        bar.set_linewidth(2.5)

    for bars in (bars_mod, bars_alti):
        for bar in bars:
            h = bar.get_height()
            ax_bar.text(bar.get_x() + bar.get_width()/2, h, f"{h:.3f}",
                        ha="center", va="bottom" if h >= 0 else "top", fontsize=8)

    for xi, pct in zip(x, pct_wins):
        ax_bar.text(xi, -0.05 * max(max(med_modele), max(med_alti)), f"{pct:.0f}% stations",
                    ha="center", va="top", fontsize=8, fontweight="bold",
                    color=C_WIN if pct > 50 else C_LOSE)

    ax_bar.set_xticks(x)
    ax_bar.set_xticklabels(METRICS)
    ax_bar.axhline(0, color="black", lw=0.8)
    ax_bar.set_title("Médiane (modèle vs insitu) et (alti vs insitu)\n"
                      "[% = stations où modèle > alti, par métrique seule, vs insitu le plus proche]",
                      fontsize=9)
    ax_bar.legend(fontsize=9)
    ax_bar.grid(True, alpha=0.25, axis="y")

    pie_vals   = [n_ok, n_not_ok] if n_not_ok > 0 else [n_ok]
    pie_colors = [C_WIN, C_LOSE] if n_not_ok > 0 else [C_WIN]
    pie_labels = ([f"≥{MIN_METRICS_OK}/4 métriques\n({n_ok} stations)",
                   f"<{MIN_METRICS_OK}/4 métriques\n({n_not_ok} stations)"]
                  if n_not_ok > 0 else [f"≥{MIN_METRICS_OK}/4 métriques\n({n_ok} stations)"])

    ax_pie.pie(pie_vals, labels=pie_labels, colors=pie_colors, autopct=lambda p: f"{p:.0f}%",
               startangle=90, textprops={"fontsize": 10, "fontweight": "bold"},
               wedgeprops={"edgecolor": "white", "linewidth": 2})

    distrib_str = "  ".join(f"{k}/4:{v}" for k, v in distrib.items())
    ax_pie.set_title(
        f"% stations où le modèle (vs insitu) bat l'alti (vs insitu)\n"
        f"sur ≥{MIN_METRICS_OK}/4 métriques simultanément\n"
        f"(insitu le plus proche, analyse station par station)\nDistribution : {distrib_str}",
        fontsize=8.5, fontweight="bold"
    )

    plt.tight_layout()
    fig_path = OUTPUT_DIR / f"synthese_{source}_{freq}.png"
    fig.savefig(fig_path, dpi=150, bbox_inches="tight")
    plt.close(fig)

    print(f"{label:<12} -> {model_name:<14} {n_ok}/{n_total} stations (>= {MIN_METRICS_OK}/4 métriques) "
          f"= {pct_ok:.1f}%  -> {fig_path}")

print(f"\n✅ Figures dans : {OUTPUT_DIR}/")