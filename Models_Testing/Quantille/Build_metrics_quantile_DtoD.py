"""
build_quantile_vs_dtod_table.py
════════════════════════════════════════════════════════════════════════
Construit un tableau récapitulatif (image PNG) comparant, pour chaque
% de masquage (80/90/96), le modèle Quantile (au quantile Q50) à son
homologue DtoD classique -- même architecture, même % de masquage,
seule la tête de sortie diffère (Q50 vs nowcast direct).

MÉTHODE : le gain (Δ) est calculé STATION PAR STATION (merge sur
"station" entre les fichiers metrics DtoD{X} et Quantile{X}, donc
uniquement sur les stations communes aux deux évaluations), PUIS on
prend la médiane de ces écarts -- pas la différence des médianes prises
séparément (les deux ne coïncident pas si les distributions ne sont pas
parfaitement corrélées station par station, cf. discussion précédente
sur le calcul du gain modèle vs alti-insitu -- même principe ici).

Lit DIRECTEMENT les fichiers par-station produits par
compare_other_models_vs_insitu.py :

    Models_Testing/DtoD/residus/metrics_DtoD{80,90,96}_{SOURCE}_{freq}_sword_insitu.csv
    Models_Testing/Quantille/residus/metrics_Quantile{80,90,96}_{SOURCE}_{freq}_sword_insitu.csv

Colonnes attendues (une ligne par station, mêmes deux fichiers) :
    station, insitu_code, dist_insitu_km, connectivity_validated, n_pairs,
    NSE, KGE, RMSE, R2, NSE_alti_insitu, KGE_alti_insitu, RMSE_alti_insitu, R2_alti_insitu

⚠️ NRMSE = colonne RMSE déjà calculée sur séries z-scorées (compare_
other_models_vs_insitu.py), pas un RMSE/plage sur données brutes (non
disponible depuis ces fichiers) -- reste comparable entre stations car
le z-score normalise déjà l'échelle.

Sortie (PNG haute résolution) :
    Models_Testing/DtoD/figures/table_quantile_vs_dtod_{SOURCE}.png
    + CSV équivalent
════════════════════════════════════════════════════════════════════════
"""

import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
from pathlib import Path

DTOD_DIR = Path("./Models_Testing/DtoD/residus")
QUANTILE_DIR = Path("./Models_Testing/Quantille/residus")
OUT_DIR = Path("./Models_Testing/DtoD/figures")

SOURCE = "hwnext"   # <-- changer ici pour basculer hwnext <-> dahiti, doit
                    #     matcher SOURCE dans les autres scripts

MASKS = [80, 90, 96]
FREQS = ["10j", "27j"]
FREQ_LABELS = {"10j": "Stations à ~10 jours entre observations",
               "27j": "Stations à ~27 jours entre observations"}
METRICS = ["NSE", "KGE", "R2", "RMSE"]  # RMSE affiché "NRMSE"

# Adoucit le dégradé par une compression en loi de puissance (pas juste
# un étirement linéaire) : un écart à X% du max observé pour cette
# colonne est tassé vers le centre (pâle) proportionnellement à
# GAMMA -- seuls les VRAIS écarts extrêmes (proches du max observé)
# atteignent le rouge/vert saturé. GAMMA=1 = dégradé linéaire d'origine ;
# augmenter pour calmer davantage les écarts modestes.
COLOR_INTENSITY_PADDING = 1.2  # léger coussin au-delà du max observé
COLOR_GAMMA = 2.2              # >1 = compresse les écarts modestes vers le centre

# Palette (identique à build_dtod_metrics_table.py)
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
# CALCUL : Quantile(Q50) vs DtoD, station par station, par % masquage
# ═══════════════════════════════════════════════════════════════
def summarize_one(mask: int, freq: str) -> dict | None:
    dtod_path = DTOD_DIR / f"metrics_DtoD{mask}_{SOURCE}_{freq}_sword_insitu.csv"
    quant_path = QUANTILE_DIR / f"metrics_Quantile{mask}_{SOURCE}_{freq}_sword_insitu.csv"

    if not dtod_path.exists() or not quant_path.exists():
        print(f"⚠ Fichier(s) introuvable(s) pour masquage {mask}% [{freq}] "
              f"-> ignoré ({dtod_path}, {quant_path})")
        return None

    df_dtod = pd.read_csv(dtod_path)
    df_quant = pd.read_csv(quant_path)
    if df_dtod.empty or df_quant.empty:
        print(f"⚠ Fichier vide pour masquage {mask}% [{freq}] -> ignoré")
        return None

    merged = df_dtod[["station"] + METRICS].merge(
        df_quant[["station"] + METRICS], on="station", suffixes=("_dtod", "_quantile"))
    if merged.empty:
        print(f"⚠ Aucune station commune DtoD{mask}/Quantile{mask} [{freq}] -> ignoré")
        return None

    row = {"model": f"{mask}%", "mask": mask, "freq": freq, "n_ok": len(merged)}
    for m in METRICS:
        row[f"{m}_dtod"] = round(merged[f"{m}_dtod"].median(), 3)
        row[f"{m}_quantile"] = round(merged[f"{m}_quantile"].median(), 3)
        gain = merged[f"{m}_quantile"] - merged[f"{m}_dtod"]  # >0 : Quantile Q50 meilleur (avant inversion RMSE)
        row[f"gain_{m}"] = round(gain.median(), 3)
        row[f"pct_quantile_better_{m}"] = round((gain > 0).mean() * 100, 1)
    return row


def load_grouped() -> dict[str, pd.DataFrame]:
    """Retourne {freq: DataFrame(masquages triés 80/90/96)}."""
    grouped = {}
    for freq in FREQS:
        rows = [summarize_one(mask, freq) for mask in MASKS]
        rows = [r for r in rows if r is not None]
        if rows:
            grouped[freq] = pd.DataFrame(rows).sort_values("mask").reset_index(drop=True)
    if not grouped:
        raise FileNotFoundError(
            f"Aucune paire DtoD/Quantile trouvée dans {DTOD_DIR} / {QUANTILE_DIR}.")
    return grouped


# ═══════════════════════════════════════════════════════════════
# RENDU PNG (moteur identique à build_dtod_metrics_table.py)
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


def soft_gain_color(diff: float, vmax: float, cmap, gamma: float, darken: float = 0.78):
    """Position dans la colormap compressée en loi de puissance : un écart
    à fraction f du vmax de sa colonne est tassé vers le centre (f**gamma
    au lieu de f), donc seuls les écarts proches du vrai max de la
    colonne atteignent la couleur pleinement saturée."""
    if vmax <= 0 or pd.isna(diff):
        return None
    frac = max(-1.0, min(1.0, diff / vmax))
    compressed = 0.0 if frac == 0 else (1 if frac > 0 else -1) * (abs(frac) ** gamma)
    pos = 0.5 + compressed * 0.5
    rgba = cmap(pos)
    return tuple(c * darken for c in rgba[:3]) + (rgba[3],)


def render_grouped_png(grouped: dict[str, pd.DataFrame], col_specs: list[tuple],
                        out_path: Path, title: str, footnote: str | None = None) -> None:
    """col_specs: liste de (label, key, kind, poids_largeur[, ref_key, higher_is_better]).
    kind ∈ {"model", "float", "float_accent", "float_gain_colored", "pct", "gain"}
      - "float_gain_colored" : affiche la valeur de `key`, colorée en
        vert/rouge selon le signe de (val - ref_val) -- ou l'inverse si
        higher_is_better=False (6e élément du tuple, défaut True ; utile
        pour NRMSE où une valeur plus basse est meilleure).
    Label "TOP|bottom" -> en-tête à deux lignes (mot d'accent en haut)."""
    n_cols = len(col_specs)
    widths = [spec[3] for spec in col_specs]
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

    gain_cmap = plt.get_cmap("RdYlGn")

    # vmax du dégradé pour "float_gain_colored" -- calculé SÉPARÉMENT pour
    # chaque colonne (pas une échelle unique partagée entre NSE/KGE/R2,
    # sinon les colonnes à faible amplitude naturelle -- typiquement KGE --
    # paraissent artificiellement pâles). La couleur suit l'ÉCART BRUT entre
    # la valeur affichée (Q50) et sa référence affichée (DtoD) -- pas le
    # gain station-par-station précalculé. La compression en loi de
    # puissance (COLOR_GAMMA) est appliquée au moment du rendu, pas ici.
    col_vmax = {}
    for j, spec in enumerate(col_specs):
        if spec[2] == "float_gain_colored" and len(spec) > 4:
            key, ref_key = spec[1], spec[4]
            higher_is_better = spec[5] if len(spec) > 5 else True
            diffs = []
            for df in grouped.values():
                if key in df.columns and ref_key in df.columns:
                    d = (df[key] - df[ref_key]) if higher_is_better else (df[ref_key] - df[key])
                    diffs.extend(d.dropna().tolist())
            vmax_col = max((abs(d) for d in diffs), default=1.0) or 1.0
            col_vmax[j] = vmax_col * COLOR_INTENSITY_PADDING

    ax.text(fig_w / 2, title_h / 2, title, ha="center", va="center",
            fontsize=13, fontweight="bold", color=COLOR_HEADER)

    y = title_h
    separator_segments = [(y, y + header_h)]

    ax.add_patch(Rectangle((margin, y), avail_w, header_h, facecolor=COLOR_HEADER,
                            edgecolor="white", linewidth=1))
    for j, spec in enumerate(col_specs):
        label = spec[0]
        xc = (x_edges[j] + x_edges[j + 1]) / 2
        if "|" in label:
            top, bottom = label.split("|")
            ax.text(xc, y + header_h / 2 - 0.11, top, ha="center", va="center",
                    fontsize=8, fontweight="bold", color=COLOR_ACCENT, style="italic")
            ax.text(xc, y + header_h / 2 + 0.13, bottom, ha="center", va="center",
                    fontsize=9, fontweight="bold", color=COLOR_HEADER_TEXT)
        else:
            ax.text(xc, y + header_h / 2, label, ha="center", va="center",
                    fontsize=9.5, fontweight="bold", color=COLOR_HEADER_TEXT)
    y += header_h

    for freq, df in grouped.items():
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
            for j, spec in enumerate(col_specs):
                label, key, kind, _ = spec[0], spec[1], spec[2], spec[3]
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
                elif kind == "float_gain_colored":
                    ref_key = spec[4] if len(spec) > 4 else None
                    higher_is_better = spec[5] if len(spec) > 5 else True
                    val = r.get(key, float("nan"))
                    ref_val = r.get(ref_key, float("nan")) if ref_key else float("nan")
                    if pd.notna(val) and pd.notna(ref_val):
                        diff = (val - ref_val) if higher_is_better else (ref_val - val)
                    else:
                        diff = float("nan")
                    color = COLOR_HEADER
                    if pd.notna(diff) and j in col_vmax:
                        c = soft_gain_color(diff, col_vmax[j], gain_cmap, COLOR_GAMMA)
                        if c is not None:
                            color = c
                    ax.text(xc, y + row_h / 2, fmt_float(val),
                            ha="center", va="center", fontsize=9.5, fontweight="bold", color=color)
                elif kind == "float_accent":
                    ax.text(xc, y + row_h / 2, fmt_float(r.get(key, float("nan"))),
                            ha="center", va="center", fontsize=9.5, fontweight="bold",
                            color=COLOR_GAIN_POS)
                elif kind == "pct":
                    ax.text(xc, y + row_h / 2, fmt_pct(r.get(key, float("nan"))),
                            ha="center", va="center", fontsize=9, color=COLOR_HEADER)
                else:
                    ax.text(xc, y + row_h / 2, fmt_float(r.get(key, float("nan"))),
                            ha="center", va="center", fontsize=9, color=COLOR_HEADER)
            y += row_h
        separator_segments.append((row_block_start, y))

    ax.add_patch(Rectangle((margin, title_h), avail_w, y - title_h, fill=False,
                            edgecolor=COLOR_HEADER, linewidth=1.2))
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

    pd.concat([df.assign(freq=freq) for freq, df in grouped.items()]).to_csv(
        OUT_DIR / f"table_quantile_vs_dtod_{SOURCE}.csv", index=False)

    render_grouped_png(
        grouped,
        col_specs=[
            ("Masquage", "model", "model", 1.0),
            ("DTOD|NSE", "NSE_dtod", "float", 1.0),
            ("MODÈLE QUANTILE Q50|NSE", "NSE_quantile", "float_gain_colored", 1.0, "NSE_dtod", True),
            ("DTOD|KGE", "KGE_dtod", "float", 1.0),
            ("MODÈLE QUANTILE Q50|KGE", "KGE_quantile", "float_gain_colored", 1.0, "KGE_dtod", True),
            ("DTOD|R2", "R2_dtod", "float", 1.0),
            ("MODÈLE QUANTILE Q50|R2", "R2_quantile", "float_gain_colored", 1.0, "R2_dtod", True),
            ("DTOD|NRMSE", "RMSE_dtod", "float", 1.0),
            ("MODÈLE QUANTILE Q50|NRMSE", "RMSE_quantile", "float_gain_colored", 1.0, "RMSE_dtod", False),
        ],
        out_path=OUT_DIR / f"table_quantile_vs_dtod_{SOURCE}.png",
        title=f"Modèle Quantile (Q50) vs DtoD classique, par % de masquage — {SOURCE.upper()}",
        footnote="Q50 en dégradé selon l'écart BRUT vs DtoD (échelle propre à chaque métrique, adoucie) -- "
                 "vert = Q50 meilleur (NSE/KGE/R2 : plus haut ; NRMSE : plus bas), rouge = DtoD meilleur. "
                 "NRMSE = RMSE sur séries z-scorées",
    )


if __name__ == "__main__":
    main()