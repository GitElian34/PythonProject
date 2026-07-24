"""
comparaison_gain_modele_vs_alti_generic.py  (adapté : sélection par motif)
════════════════════════════════════════════════════════════════════════
Version générique fusionnant les 4 combinaisons (HW Next / DAHITI) x
(10j / 27j) via les variables globales SOURCE et FREQ ci-dessous.

CHANGEMENT PAR RAPPORT À LA VERSION PRÉCÉDENTE :
-------------------------------------------------
Les noms de modèles réels (colonne "model" dans metrics_per_station.csv)
sont les noms bruts de dossiers de run NeuralHydrology, avec horodatage
(ex: "arlstm_DtoD96_periodic_0607_150907"). Ces noms changent à chaque
nouvel entraînement et ne suivent pas forcément la même convention d'un
run à l'autre (anciens runs classiques "DtoD96_NSE" vs nouveaux runs
"arlstm_DtoD96_periodic_...").

Au lieu de forcer une correspondance exacte de string (comme avant avec
MANUAL_TOP_MODELS), on définit maintenant MODEL_SELECTION : un dict
{label_propre: motif_a_chercher}. Le motif est cherché en tant que
SOUS-CHAINE dans les valeurs de la colonne "model" -> peu importe le nom
exact du dossier de run, tant que le motif choisi le désigne sans
ambiguïté. Les labels propres (clés du dict) sont ensuite utilisés
PARTOUT dans les CSV et les graphes, à la place du nom de dossier brut.

Si un motif ne matche aucun modèle, ou en matche plusieurs, un avertissement
explicite est affiché (pas d'échec silencieux).

Pour mémoire, les équivalents NSE classiques déjà entraînés étaient :
  DtoD80 -> arlstm_DtoD80_1506_150002   epoch 12
  DtoD90 -> arlstm_DtoD90_1606_111709   epoch 14
  DtoD96 -> arlstm_DtoD96_1606_164901   epoch 13

Entrées (déjà produites par analyse_residus_NSE_vs_RMSE_insitu_generic.py,
dans le même OUTPUT_DIR encodant SOURCE/FREQ) :
  metrics_per_station.csv
  baseline_alti_vs_insitu.csv
  ranking_vs_insitu.csv   (pour la sélection auto du top N, si MODEL_SELECTION=None)

Sorties (dans le même OUTPUT_DIR) :
  gain_modele_vs_alti.csv
  resume_gain_modele_vs_alti.csv
  gain_modele_vs_alti.png
════════════════════════════════════════════════════════════════════════
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.cm as cm
from pathlib import Path

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES GLOBAUX — à modifier ici à chaque utilisation
# ═══════════════════════════════════════════════════════════════
SOURCE = "dahiti"   # "hwnext" ou "dahiti"
FREQ   = "27j"      # "10j" ou "27j"

N_TOP = 3   # nombre de modèles à comparer si sélection AUTOMATIQUE (MODEL_SELECTION=None)

# ── Sélection MANUELLE par motif (substring) ────────────────────
# Clé   : label propre, utilisé partout dans les sorties (CSV, légendes)
# Valeur: motif cherché comme sous-chaîne dans la colonne "model" brute
#         (le nom du dossier de run NeuralHydrology, horodatage inclus)
# Mettre à None pour repasser en sélection automatique (comportement
# d'origine, basé sur ranking_vs_insitu.csv, noms bruts non retraités).
MODEL_SELECTION = {
    "DtoD80_NSE": "DtoD80_NSE",
    "DtoD90_NSE": "DtoD90_NSE",
    "DtoD96_NSE": "DtoD96_NSE",
    "DtoD80_RMSE": "DtoD80_RMSE",
    "DtoD90_RMSE": "DtoD90_RMSE",
    "DtoD96_RMSE": "DtoD96_RMSE",
    # Ajouter ici les nouveaux modèles (periodic/block) une fois que
    # analyse_residus_NSE_vs_RMSE_insitu_generic.py aura été relancé
    # dessus et leur aura donné une entrée dans metrics_per_station.csv.
    # Exemple attendu, à adapter selon le label réellement généré :
    "DtoD80_periodic": "DtoD80_periodic",
    # "DtoD90_periodic": "DtoD90_periodic",
    # "DtoD96_block":    "DtoD96_block",
}

METRIC_DIRECTION = {"NSE": True, "KGE": True, "RMSE": False, "R2": True}

OUTPUT_DIR = Path(f"./data_processing/AnalyseModelsDtoD/nse_vs_rmse_{SOURCE}_{FREQ}")
METRICS_CSV   = OUTPUT_DIR / "metrics_per_station.csv"
BASELINE_CSV  = OUTPUT_DIR / "baseline_alti_vs_insitu.csv"
RANKING_CSV   = OUTPUT_DIR / "ranking_vs_insitu.csv"


# ═══════════════════════════════════════════════════════════════
# RÉSOLUTION DES LABELS PAR MOTIF (substring matching)
# ═══════════════════════════════════════════════════════════════
def resolve_model_labels(df: pd.DataFrame, selection: dict) -> pd.DataFrame:
    """
    Remplace la colonne "model" (noms bruts de run) par des labels propres,
    en cherchant chaque motif de `selection` comme sous-chaîne dans les
    valeurs uniques de "model". Lignes dont le nom brut ne matche aucun
    motif -> exclues (on ne garde que les modèles explicitement sélectionnés).
    """
    df = df.copy()
    raw_values = df["model"].astype(str).unique()

    label_by_raw = {}
    for label, pattern in selection.items():
        found = [raw for raw in raw_values if pattern in raw]

        if not found:
            print(f"  ⚠ Aucun modèle ne correspond au motif '{pattern}' "
                  f"(label '{label}') dans {METRICS_CSV.name} — ignoré")
            continue
        if len(found) > 1:
            print(f"  ⚠ Motif '{pattern}' (label '{label}') ambigu, "
                  f"{len(found)} correspondances trouvées : {found}")
            print(f"    -> rends le motif plus spécifique si ce n'est pas voulu. "
                  f"Pour l'instant, toutes ces correspondances sont fusionnées sous '{label}'.")

        for raw in found:
            if raw in label_by_raw and label_by_raw[raw] != label:
                print(f"  ⚠ '{raw}' matche à la fois '{label_by_raw[raw]}' et '{label}' "
                      f"— motifs se chevauchant, vérifier MODEL_SELECTION")
            label_by_raw[raw] = label

    df["model_raw"] = df["model"]
    df["model"] = df["model_raw"].map(label_by_raw)
    return df[df["model"].notna()].copy()


# ═══════════════════════════════════════════════════════════════
# SÉLECTION DES TOP MODÈLES
# ═══════════════════════════════════════════════════════════════
df_metrics  = pd.read_csv(METRICS_CSV)
df_baseline = pd.read_csv(BASELINE_CSV)

df_metrics["station"]  = df_metrics["station"].astype(str)
df_baseline["station"] = df_baseline["station"].astype(str)

if MODEL_SELECTION is not None:
    print(f"Sélection MANUELLE par motif : {MODEL_SELECTION}")
    df_metrics = resolve_model_labels(df_metrics, MODEL_SELECTION)
    TOP_MODELS = [lbl for lbl in MODEL_SELECTION if lbl in df_metrics["model"].unique()]
    if not TOP_MODELS:
        raise SystemExit(
            "Aucun des motifs de MODEL_SELECTION n'a trouvé de correspondance "
            f"dans {METRICS_CSV} — vérifier les motifs ou le fichier source."
        )
else:
    if not RANKING_CSV.exists():
        raise SystemExit(
            f"ranking_vs_insitu.csv introuvable ({RANKING_CSV}) -> impossible de sélectionner "
            f"automatiquement le top {N_TOP}. Lancer analyse_residus_NSE_vs_RMSE_insitu_generic.py "
            f"d'abord, ou fixer MODEL_SELECTION."
        )
    df_rank = pd.read_csv(RANKING_CSV).sort_values("rang_moyen")
    TOP_MODELS = df_rank["model"].head(N_TOP).tolist()
    print(f"TOP_MODELS sélectionné automatiquement (top {N_TOP} de {RANKING_CSV}) : {TOP_MODELS}")

# Palette de couleurs dynamique (tab10) pour s'adapter à n'importe quel nombre/nom de modèles
palette = cm.get_cmap("tab10", max(len(TOP_MODELS), 3))
COLORS = {model: palette(i) for i, model in enumerate(TOP_MODELS)}
COLORS["Alti (baseline)"] = "#9E9E9E"

# ═══════════════════════════════════════════════════════════════
# MERGE AVEC LA BASELINE
# ═══════════════════════════════════════════════════════════════
df_top = df_metrics[df_metrics["model"].isin(TOP_MODELS)].copy()
if df_top.empty:
    raise SystemExit(f"Aucune ligne trouvée pour les modèles {TOP_MODELS} dans {METRICS_CSV}")

df_merged = df_top.merge(
    df_baseline[["station", "NSE_alti_insitu", "KGE_alti_insitu", "RMSE_alti_insitu", "R2_alti_insitu"]],
    on="station", how="inner"
)
print(f"Stations avec baseline alti-insitu disponible : {df_merged['station'].nunique()}")

# ═══════════════════════════════════════════════════════════════
# CALCUL DES GAINS (modèle vs insitu) - (alti vs insitu)
# ═══════════════════════════════════════════════════════════════
for metric, higher_is_better in METRIC_DIRECTION.items():
    col_mod = f"{metric}_modele_insitu"
    col_alti = f"{metric}_alti_insitu"
    gain_col = f"gain_{metric}"
    if higher_is_better:
        df_merged[gain_col] = df_merged[col_mod] - df_merged[col_alti]
    else:
        df_merged[gain_col] = df_merged[col_alti] - df_merged[col_mod]

detail_csv = OUTPUT_DIR / "gain_modele_vs_alti.csv"
df_merged.to_csv(detail_csv, index=False)
print(f"Détail station par station -> {detail_csv}")

# ═══════════════════════════════════════════════════════════════
# RÉSUMÉ PAR MODÈLE
# ═══════════════════════════════════════════════════════════════
print(f"\n{'='*100}")
print(f"  RÉSUMÉ [{SOURCE.upper()} {FREQ}] — Modèle vs Insitu  contre  Alti vs Insitu  (baseline)")
print(f"{'='*100}")

resume_rows = []
for model in TOP_MODELS:
    sub = df_merged[df_merged["model"] == model]
    if sub.empty:
        continue
    print(f"\n--- {model} (n={len(sub)} stations) ---")
    print(f"  {'métrique':<8} {'médiane modèle':>15} {'médiane alti':>14} {'gain médian':>12} {'% stations où modèle > alti':>30}")
    row = {"model": model, "n_stations": len(sub)}
    for metric, higher_is_better in METRIC_DIRECTION.items():
        col_mod  = sub[f"{metric}_modele_insitu"].dropna()
        col_alti = sub[f"{metric}_alti_insitu"].dropna()
        gain     = sub[f"gain_{metric}"].dropna()
        pct_better = (gain > 0).mean() * 100 if len(gain) else np.nan

        print(f"  {metric:<8} {col_mod.median():>15.3f} {col_alti.median():>14.3f} "
              f"{gain.median():>12.3f} {pct_better:>29.1f}%")

        row[f"{metric}_med_modele"] = round(col_mod.median(), 3) if len(col_mod) else np.nan
        row[f"{metric}_med_alti"]   = round(col_alti.median(), 3) if len(col_alti) else np.nan
        row[f"{metric}_gain_med"]   = round(gain.median(), 3) if len(gain) else np.nan
        row[f"{metric}_pct_modele_meilleur"] = round(pct_better, 1) if not np.isnan(pct_better) else np.nan

    resume_rows.append(row)

df_resume = pd.DataFrame(resume_rows)
resume_csv = OUTPUT_DIR / "resume_gain_modele_vs_alti.csv"
df_resume.to_csv(resume_csv, index=False)
print(f"\nRésumé -> {resume_csv}")

# ═══════════════════════════════════════════════════════════════
# FIGURE — boxplots comparatifs : modèle vs insitu (xN) + alti vs insitu (baseline)
# ═══════════════════════════════════════════════════════════════
fig, axes = plt.subplots(2, 2, figsize=(13, 9))
fig.suptitle(
    f"[{SOURCE.upper()} {FREQ}] Modèle vs Insitu  comparé à  Alti vs Insitu (baseline)\n"
    f"{len(TOP_MODELS)} modèle(s) sélectionné(s) — insitu le plus proche",
    fontsize=12, fontweight="bold"
)
rng = np.random.default_rng(42)

for ax, metric in zip(axes.flat, METRIC_DIRECTION.keys()):
    data, colors, labels = [], [], []

    baseline_vals = df_merged.drop_duplicates(subset="station")[f"{metric}_alti_insitu"].dropna().values
    data.append(baseline_vals)
    colors.append(COLORS["Alti (baseline)"])
    labels.append("Alti\nvs insitu")

    for model in TOP_MODELS:
        vals = df_merged[df_merged["model"] == model][f"{metric}_modele_insitu"].dropna().values
        data.append(vals)
        colors.append(COLORS[model])
        labels.append(f"{model}\nvs insitu")

    bp = ax.boxplot(data, tick_labels=labels, patch_artist=True,
                     medianprops={"color": "black", "linewidth": 2}, widths=0.5)
    for box, color in zip(bp["boxes"], colors):
        box.set_facecolor(color)
        box.set_alpha(0.65)

    for j, (vals, color) in enumerate(zip(data, colors), 1):
        jitter = rng.uniform(-0.12, 0.12, len(vals))
        ax.scatter(np.full(len(vals), j) + jitter, vals, alpha=0.3, s=10, color=color, zorder=3)
        if len(vals):
            med = np.nanmedian(vals)
            ax.text(j, med, f"{med:.2f}", ha="center", fontsize=8, fontweight="bold",
                    va="bottom" if METRIC_DIRECTION[metric] else "top")

    if metric != "RMSE":
        ax.axhline(0, color="red", lw=1, ls="--", alpha=0.5)
    ax.set_title(metric, fontsize=11, fontweight="bold")
    ax.grid(True, alpha=0.25, axis="y")
    ax.tick_params(axis="x", labelsize=8)

plt.tight_layout()
fig_path = OUTPUT_DIR / "gain_modele_vs_alti.png"
fig.savefig(fig_path, dpi=150, bbox_inches="tight")
plt.close(fig)
print(f"\n✅ Figure -> {fig_path}")