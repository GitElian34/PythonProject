"""
build_dtod_metrics_table.py
════════════════════════════════════════════════════════════════════════
Construit des tableaux récapitulatifs (image PNG, prêts pour le rapport)
des métriques (NSE, KGE, RMSE/NRMSE, R2) pour les 3 modèles DtoD
(80/90/96 = % de masquage temporel appliqué à l'entraînement pour
imiter la disponibilité des stations in situ), regroupés par type de
station (gap médian 10j / 27j) avec un bandeau de section unique par
groupe (pas de répétition ligne par ligne).

Lit DIRECTEMENT les fichiers par-station produits par
compare_other_models_vs_insitu.py :

    Models_Testing/DtoD/residus/metrics_DtoD80_hwnext_10j_sword_insitu.csv
    Models_Testing/DtoD/residus/metrics_DtoD80_hwnext_27j_sword_insitu.csv
    Models_Testing/DtoD/residus/metrics_DtoD90_hwnext_10j_sword_insitu.csv
    Models_Testing/DtoD/residus/metrics_DtoD90_hwnext_27j_sword_insitu.csv
    Models_Testing/DtoD/residus/metrics_DtoD96_hwnext_10j_sword_insitu.csv
    Models_Testing/DtoD/residus/metrics_DtoD96_hwnext_27j_sword_insitu.csv

Colonnes attendues (une ligne par station) :
    station, insitu_code, dist_insitu_km, connectivity_validated, n_pairs,
    NSE, KGE, RMSE, R2, NSE_alti_insitu, KGE_alti_insitu, RMSE_alti_insitu, R2_alti_insitu

⚠️ NRMSE (tableau compact) = colonne RMSE déjà calculée sur séries
z-scorées dans compare_other_models_vs_insitu.py, pas un RMSE/plage sur
données brutes (non disponible depuis ces fichiers) -- la valeur reste
comparable entre stations car le z-score normalise déjà l'échelle.

Sorties (PNG haute résolution) :
    Models_Testing/DtoD/figures/table_dtod_compact.png   -> NSE/KGE/R2/NRMSE (toutes en vert), corps du rapport
    Models_Testing/DtoD/figures/table_dtod_complet.png   -> 4 métriques + gains, annexe
    + les CSV équivalents (pour retraitement / export LaTeX si besoin)
════════════════════════════════════════════════════════════════════════
"""

import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
from pathlib import Path

RESIDUS_DIR = Path("./Models_Testing/DtoD/residus")
OUT_DIR = Path("./Models_Testing/DtoD/figures")

SOURCE = "dahiti"   # <-- changer ici pour basculer hwnext <-> dahiti, doit
                    #     matcher SOURCE dans eval_dtod_quantile.py et
                    #     compare_other_models_vs_insitu.py

MODELS = ["DtoD80", "DtoD90", "DtoD96"]
FREQS = ["10j", "27j"]
FREQ_LABELS = {"10j": "Stations à ~10 jours entre observations",
               "27j": "Stations à ~27 jours entre observations"}
METRICS = ["NSE", "KGE", "RMSE", "R2"]
ORDER = {"DtoD80": 0, "DtoD90": 1, "DtoD96": 2}

# Palette
COLOR_HEADER = "#2C3E50"
COLOR_GROUP = "#5D6D7E"
COLOR_HEADER_TEXT = "white"
COLOR_ROW_A = "#F7F9F9"
COLOR_ROW_B = "#FFFFFF"
COLOR_GAIN_POS = "#1E8449"
COLOR_GAIN_NEG = "#C0392B"
COLOR_GRID = "#D5D8DC"
COLOR_SUBTEXT = "#7F8C8D"
COLOR_ACCENT = "#F4D03F"


# ═══════════════════════════════════════════════════════════════
# CALCUL DES MÉTRIQUES (identique à la logique de compare_other_models_vs_insitu.py)
# ═══════════════════════════════════════════════════════════════
def summarize_one(label: str, freq: str) -> dict | None:
    path = RESIDUS_DIR / f"metrics_{label}_{SOURCE}_{freq}_sword_insitu.csv"
    if not path.exists():
        print(f"⚠ Fichier introuvable : {path} -> ignoré")
        return None
    df = pd.read_csv(path)
    if df.empty:
        print(f"⚠ Fichier vide : {path} -> ignoré")
        return None

    row = {"model": label, "freq": freq, "n_ok": len(df)}
    row["sword_pct"] = round(100 * df["connectivity_validated"].mean(), 1)

    for m in METRICS:
        v, va = df[m].dropna(), df[f"{m}_alti_insitu"].dropna()
        row[f"{m}"] = round(v.median(), 3) if len(v) else float("nan")
        row[f"{m}_base"] = round(va.median(), 3) if len(va) else float("nan")

    for m, higher_is_better in [("NSE", True), ("KGE", True), ("RMSE", False), ("R2", True)]:
        merged = df[[m, f"{m}_alti_insitu"]].dropna()
        if len(merged):
            gain = (merged[m] - merged[f"{m}_alti_insitu"]) if higher_is_better \
                else (merged[f"{m}_alti_insitu"] - merged[m])
            row[f"gain_{m}"] = round(gain.median(), 3)
            row[f"pct_better_{m}"] = round((gain > 0).mean() * 100, 1)
    return row


def load_grouped() -> dict[str, pd.DataFrame]:
    """Retourne {freq: DataFrame(modèles triés 80/90/96)}."""
    grouped = {}
    for freq in FREQS:
        rows = [summarize_one(label, freq) for label in MODELS]
        rows = [r for r in rows if r is not None]
        if rows:
            df = pd.DataFrame(rows)
            df["_ord"] = df["model"].map(ORDER)
            grouped[freq] = df.sort_values("_ord").drop(columns="_ord").reset_index(drop=True)
    if not grouped:
        raise FileNotFoundError(f"Aucun fichier metrics_*_sword_insitu.csv trouvé dans {RESIDUS_DIR}.")
    return grouped


# ═══════════════════════════════════════════════════════════════
# RENDU PNG — bandeau de groupe unique par fréquence, colonnes fusionnées
# ═══════════════════════════════════════════════════════════════
def fmt_float(x):
    return f"{x:.3f}" if pd.notna(x) else "—"


def fmt_pct(x):
    return f"{x:.0f}%" if pd.notna(x) else "—"


def fmt_gain(x):
    if pd.isna(x):
        return "—"
    arrow = "▲" if x > 0 else ("▼" if x < 0 else "▬")
    return f"{arrow} {x:+.3f}"


def render_grouped_png(grouped: dict[str, pd.DataFrame], col_specs: list[tuple],
                        out_path: Path, title: str, footnote: str | None = None) -> None:
    """
    col_specs: liste de (label, key, kind, poids_largeur)
    kind ∈ {"model", "float", "float_accent", "pct", "gain"}
      - "float_accent" : valeur numérique mise en avant en vert gras
        (utilisé pour TOUTES les métriques du modèle -- ce sont les
        chiffres à retenir de ce tableau, la baseline reste en gris)
    Label "TOP|bottom" -> en-tête à deux lignes (mot d'accent en haut).
    Colonne "model" affiche le nom du modèle en gras + le nb de stations
    en petit dessous (fusionne Modèle + % masqué + Stations en une seule
    colonne, puisque le % masqué est déjà dans le nom DtoD80/90/96).
    """
    n_cols = len(col_specs)
    widths = [w for _, _, _, w in col_specs]
    widths = [w / sum(widths) for w in widths]

    row_h, group_h, header_h, title_h = 0.45, 0.45, 0.65, 0.55
    footnote_h = 0.35 if footnote else 0.0
    n_data_rows = sum(len(df) for df in grouped.values())
    n_groups = len(grouped)
    fig_h = title_h + header_h + n_groups * group_h + n_data_rows * row_h + footnote_h + 0.15
    fig_w = 1.25 * n_cols + 1.0

    fig = plt.figure(figsize=(fig_w, fig_h), dpi=200)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_xlim(0, fig_w)
    ax.set_ylim(0, fig_h)
    ax.invert_yaxis()
    ax.axis("off")

    margin = 0.12
    right_margin = 0.32
    avail_w = fig_w - margin - right_margin
    x_edges = [margin]
    for w in widths:
        x_edges.append(x_edges[-1] + w * avail_w)

    # Titre
    ax.text(fig_w / 2, title_h / 2, title, ha="center", va="center",
            fontsize=13, fontweight="bold", color=COLOR_HEADER)

    y = title_h
    separator_segments = [(y, y + header_h)]  # en-tête colonnes seulement pour l'instant

    # En-tête colonnes
    ax.add_patch(Rectangle((margin, y), avail_w, header_h, facecolor=COLOR_HEADER,
                            edgecolor="white", linewidth=1))
    for j, (label, _, _, _) in enumerate(col_specs):
        xc = (x_edges[j] + x_edges[j + 1]) / 2
        if "|" in label:
            top, bottom = label.split("|")
            ax.text(xc, y + header_h / 2 - 0.11, top, ha="center", va="center",
                    fontsize=8, fontweight="bold", color=COLOR_ACCENT,
                    style="italic")
            ax.text(xc, y + header_h / 2 + 0.13, bottom, ha="center", va="center",
                    fontsize=9, fontweight="bold", color=COLOR_HEADER_TEXT)
        else:
            ax.text(xc, y + header_h / 2, label, ha="center", va="center",
                    fontsize=9.5, fontweight="bold", color=COLOR_HEADER_TEXT)
    y += header_h

    for freq, df in grouped.items():
        # Bandeau de groupe (une seule fois pour toutes les lignes de la fréquence)
        n_range = sorted(df["n_ok"].unique())
        n_txt = f"n = {n_range[0]}" if len(n_range) == 1 else f"n = {n_range[0]}–{n_range[-1]}"
        ax.add_patch(Rectangle((margin, y), avail_w, group_h, facecolor=COLOR_GROUP,
                                edgecolor="white", linewidth=1))
        ax.text(margin + 0.15, y + group_h / 2, FREQ_LABELS[freq], ha="left", va="center",
                fontsize=9.5, fontweight="bold", color="white")
        ax.text(x_edges[-1] - 0.15, y + group_h / 2, n_txt, ha="right", va="center",
                fontsize=8.5, color="white", style="italic")
        y += group_h
        row_block_start = y

        for i, r in df.iterrows():
            band = COLOR_ROW_A if i % 2 == 0 else COLOR_ROW_B
            ax.add_patch(Rectangle((margin, y), avail_w, row_h, facecolor=band,
                                    edgecolor=COLOR_GRID, linewidth=0.6))
            for j, (label, key, kind, _) in enumerate(col_specs):
                xc = (x_edges[j] + x_edges[j + 1]) / 2
                if kind == "model":
                    ax.text(xc, y + row_h / 2, r["model"], ha="center", va="center",
                            fontsize=9.5, fontweight="bold", color=COLOR_HEADER)
                elif kind == "gain":
                    val = r.get(key, float("nan"))
                    color = COLOR_GAIN_POS if pd.notna(val) and val > 0 else \
                        (COLOR_GAIN_NEG if pd.notna(val) and val < 0 else COLOR_HEADER)
                    ax.text(xc, y + row_h / 2, fmt_gain(val), ha="center", va="center",
                            fontsize=9, fontweight="bold", color=color)
                elif kind == "float_accent":
                    ax.text(xc, y + row_h / 2, fmt_float(r.get(key, float("nan"))),
                            ha="center", va="center", fontsize=9.5, fontweight="bold",
                            color=COLOR_GAIN_POS)
                elif kind == "pct":
                    ax.text(xc, y + row_h / 2, fmt_pct(r.get(key, float("nan"))),
                            ha="center", va="center", fontsize=9, color=COLOR_HEADER)
                else:  # float
                    ax.text(xc, y + row_h / 2, fmt_float(r.get(key, float("nan"))),
                            ha="center", va="center", fontsize=9, color=COLOR_HEADER)
            y += row_h
        separator_segments.append((row_block_start, y))

    # Cadre extérieur
    ax.add_patch(Rectangle((margin, title_h), avail_w, y - title_h, fill=False,
                            edgecolor=COLOR_HEADER, linewidth=1.2))
    # Séparateurs de colonnes (légers) -- uniquement sur l'en-tête et les
    # lignes de données, jamais à travers les bandeaux de groupe
    for xe in x_edges[1:-1]:
        for y_start, y_end in separator_segments:
            ax.plot([xe, xe], [y_start, y_end], color=COLOR_GRID, linewidth=0.5)

    if footnote:
        ax.text(margin, y + footnote_h / 2 + 0.05, footnote, ha="left", va="center",
                fontsize=7.5, color=COLOR_SUBTEXT, style="italic")

    fig.savefig(out_path, facecolor="white")
    plt.close(fig)
    print(f"✅ {out_path}")


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    grouped = load_grouped()

    # CSV bruts (une ligne par modèle x fréquence), pour retraitement
    pd.concat([df.assign(freq=freq) for freq, df in grouped.items()]).to_csv(
        OUT_DIR / f"table_dtod_recap_{SOURCE}.csv", index=False)

    render_grouped_png(
        grouped,
        col_specs=[
            ("Modèle", "model", "model", 1.5),
            ("BASELINE|NSE (Alti-Insitu)", "NSE_base", "float", 1.4),
            ("NSE", "NSE", "float_accent", 0.9),
            ("BASELINE|KGE (Alti-Insitu)", "KGE_base", "float", 1.4),
            ("KGE", "KGE", "float_accent", 0.9),
            ("BASELINE|R2 (Alti-Insitu)", "R2_base", "float", 1.4),
            ("R2", "R2", "float_accent", 0.9),
            ("BASELINE|NRMSE (Alti-Insitu)", "RMSE_base", "float", 1.4),
            ("NRMSE", "RMSE", "float_accent", 0.9),
        ],
        out_path=OUT_DIR / f"table_dtod_compact_{SOURCE}.png",
        title=f"DtoD — Modèle vs Insitu (médianes) — {SOURCE.upper()}",
        footnote="Baseline = Alti(obs) vs Insitu, indépendante du modèle  •  "
                 "toutes les métriques modèle (vert) sont les chiffres à retenir de ce tableau  •  "
                 "NRMSE = RMSE sur séries z-scorées (pas de RMSE/plage sur données brutes disponible ici)",
    )

    render_grouped_png(
        grouped,
        col_specs=[
            ("Modèle", "model", "model", 1.5),
            ("SWORD", "sword_pct", "pct", 0.8),
            ("NSE", "NSE", "float", 0.8), ("BASELINE|NSE", "NSE_base", "float", 1.0),
            ("ΔNSE", "gain_NSE", "gain", 1.1),
            ("KGE", "KGE", "float", 0.8), ("BASELINE|KGE", "KGE_base", "float", 1.0),
            ("ΔKGE", "gain_KGE", "gain", 1.1),
            ("NRMSE", "RMSE", "float", 0.8), ("BASELINE|NRMSE", "RMSE_base", "float", 1.0),
            ("ΔNRMSE", "gain_RMSE", "gain", 1.1),
            ("R2", "R2", "float", 0.8), ("BASELINE|R2", "R2_base", "float", 1.0),
            ("ΔR2", "gain_R2", "gain", 1.1),
        ],
        out_path=OUT_DIR / f"table_dtod_complet_{SOURCE}.png",
        title=f"DtoD — Récapitulatif complet (NSE / KGE / NRMSE / R²) — {SOURCE.upper()}",
        footnote="Baseline = Alti(obs) vs Insitu, indépendante du modèle  •  "
                 "Δ = médiane des gains station-par-station (≠ différence des médianes affichées)  •  "
                 "NRMSE = RMSE sur séries z-scorées",
    )


if __name__ == "__main__":
    main()