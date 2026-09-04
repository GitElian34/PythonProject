"""
build_quantile_outlier_rate_table.py
════════════════════════════════════════════════════════════════════════
Tableau récapitulatif : pour chaque origine de stations (Dahiti 10j,
Dahiti 27j, HW Next 10j, HW Next 27j), le % de points flagués outlier
selon 3 critères de sévérité croissante, calculés sur les 3 modèles
Quantile80/90/96 (résultats "_bands", cf. eval_quantile_bands.py) :

  1. ≥2/3 modèles   : au moins 2 des 3 modèles indépendants flaguent
                       le point (obs hors [Q5,Q95] DE CE MODÈLE).
  2. 3/3 modèles     : les 3 modèles flaguent le même point.
  3. 3/3 + 0.5σ      : les 3 modèles flaguent le même point (3/3 ci-dessus)
                       ET l'écart de dépassement MOYEN sur les 3 modèles
                       (distance entre obs et la borne violée -- Q5 ou
                       Q95 -- de CE modèle, exprimée en unités de
                       σ_résidu de CE modèle) est ≥ 0.5. Donc pas un
                       seuil élargi par modèle, mais une moyenne : un
                       modèle peut dépasser de 0.2σ si un autre dépasse
                       de 0.9σ, tant que la moyenne des 3 atteint 0.5σ.
                       σ_résidu = écart-type de (obs − pred_q50), calculé
                       PAR STATION ET PAR MODÈLE sur toute la période
                       disponible (même logique que dans les scripts
                       précédents de détection d'outliers).

⚠️ HYPOTHÈSES À CONFIRMER (dites-moi si je me trompe) :
  - "2/3 stations" du prompt = "2/3 modèles" (vocabulaire utilisé partout
    ailleurs dans nos échanges pour ce mécanisme de consensus).
  - Le "0.5 de sigma" s'ajoute à Q95 (borne haute) ou se retranche de Q5
    (borne basse), PAS un seuil symétrique autour de Q50.

% = (nb de points flagués) / (nb total de points avec obs+bande valides
sur les 3 modèles) × 100 -- calculé séparément par origine (SOURCE×freq).

Sources (produites par eval_quantile_bands.py, pour hwnext ET dahiti) :
  Models_Testing/Quantille/residus/residuals_Quantile{80,90,96}_{SOURCE}_{freq}_bands.csv

Sortie :
  Models_Testing/Quantille/figures/table_outlier_rates.png
  + CSV équivalent
════════════════════════════════════════════════════════════════════════
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
from matplotlib.colors import TwoSlopeNorm
from pathlib import Path

MODELS = ["Quantile80", "Quantile90", "Quantile96"]
SOURCES = ["hwnext", "dahiti"]
FREQS = ["10j", "27j"]
SIGMA_MARGIN = 0.5  # multiplicateur de sigma pour le critère strict

RESIDUS_DIR = Path("./Models_Testing/Quantille/residus")
OUT_DIR = Path("./Models_Testing/Quantille/figures")

ORIGIN_LABELS = {"hwnext": "HW Next", "dahiti": "Dahiti"}

COLOR_HEADER = "#2C3E50"
COLOR_HEADER_TEXT = "white"
COLOR_ROW_A = "#F7F9F9"
COLOR_ROW_B = "#FFFFFF"
COLOR_GRID = "#D5D8DC"
COLOR_SUBTEXT = "#7F8C8D"


# ═══════════════════════════════════════════════════════════════
# CHARGEMENT + FLAGS (basique + strict) PAR MODÈLE
# ═══════════════════════════════════════════════════════════════
def load_and_flag(model_label: str, source: str, freq: str) -> pd.DataFrame:
    path = RESIDUS_DIR / f"residuals_{model_label}_{source}_{freq}_bands.csv"
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path)
    df["station"] = df["station"].astype(str)
    df["date"] = pd.to_datetime(df["date"])
    df = df.dropna(subset=["obs", "pred_q05", "pred_q50", "pred_q95"]).copy()
    if df.empty:
        return df

    # sigma des résidus, PAR STATION (pas de fuite entre stations)
    df["residual"] = df["obs"] - df["pred_q50"]
    sigma_by_station = df.groupby("station")["residual"].transform(
        lambda s: s.std() if len(s) >= 5 else np.nan)
    df["sigma"] = sigma_by_station

    df["is_outlier_basic"] = (df["obs"] < df["pred_q05"]) | (df["obs"] > df["pred_q95"])

    # Écart de dépassement en unités de sigma (0 si le point n'est pas
    # outlier pour ce modèle -- un seul des deux termes est non-nul)
    excess_raw = (df["pred_q05"] - df["obs"]).clip(lower=0) + (df["obs"] - df["pred_q95"]).clip(lower=0)
    df["excess_sigma"] = excess_raw / df["sigma"]
    return df


def compute_rates_for_origin(source: str, freq: str) -> dict | None:
    flagged = {m: load_and_flag(m, source, freq) for m in MODELS}
    if any(flagged[m].empty for m in MODELS):
        missing = [m for m in MODELS if flagged[m].empty]
        print(f"⚠ {ORIGIN_LABELS[source]} {freq} : données manquantes pour {missing} -> ignoré")
        return None

    base = None
    for m in MODELS:
        sub = flagged[m][["station", "date", "obs", "is_outlier_basic", "excess_sigma"]].rename(
            columns={"is_outlier_basic": f"basic_{m}", "excess_sigma": f"excess_{m}"})
        base = sub if base is None else base.merge(sub, on=["station", "date", "obs"], how="inner")

    if base is None or base.empty:
        print(f"⚠ {ORIGIN_LABELS[source]} {freq} : aucun point commun aux 3 modèles -> ignoré")
        return None

    n_agree_basic = base[[f"basic_{m}" for m in MODELS]].sum(axis=1)
    avg_excess_sigma = base[[f"excess_{m}" for m in MODELS]].mean(axis=1)
    n_total = len(base)

    return {
        "origin": f"{ORIGIN_LABELS[source]} {freq}",
        "source": source, "freq": freq,
        "n_total": n_total,
        "pct_2of3": round(100 * (n_agree_basic >= 2).mean(), 2),
        "pct_3of3": round(100 * (n_agree_basic == 3).mean(), 2),
        "pct_3of3_strict": round(100 * ((n_agree_basic == 3) & (avg_excess_sigma >= SIGMA_MARGIN)).mean(), 2),
    }


# ═══════════════════════════════════════════════════════════════
# RENDU PNG — tableau plat, une ligne par origine
# ═══════════════════════════════════════════════════════════════
def fmt_pct(x):
    return f"{x:.2f}%" if pd.notna(x) else "—"


def render_table(df: pd.DataFrame, out_path: Path) -> None:
    columns = [
        ("Origine des stations", "origin", 1.6),
        ("n total\n(points)", "n_total", 1.0),
        ("% outliers\n≥2/3 modèles", "pct_2of3", 1.2),
        ("% outliers\n3/3 modèles", "pct_3of3", 1.2),
        (f"% outliers\n3/3 + {SIGMA_MARGIN}σ", "pct_3of3_strict", 1.3),
    ]
    n_cols = len(columns)
    widths = [w for _, _, w in columns]
    widths = [w / sum(widths) for w in widths]

    row_h, header_h, title_h, footnote_h = 0.55, 0.7, 0.55, 0.55
    n_rows = len(df)
    fig_h = title_h + header_h + n_rows * row_h + footnote_h + 0.15
    fig_w = 1.9 * n_cols

    fig = plt.figure(figsize=(fig_w, fig_h), dpi=200)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_xlim(0, fig_w)
    ax.set_ylim(0, fig_h)
    ax.invert_yaxis()
    ax.axis("off")

    margin = 0.15
    avail_w = fig_w - 2 * margin
    x_edges = [margin]
    for w in widths:
        x_edges.append(x_edges[-1] + w * avail_w)

    ax.text(fig_w / 2, title_h / 2, "Taux d'outliers par origine de stations (modèles Quantile)",
            ha="center", va="center", fontsize=12.5, fontweight="bold", color=COLOR_HEADER)

    y = title_h
    ax.add_patch(Rectangle((margin, y), avail_w, header_h, facecolor=COLOR_HEADER,
                            edgecolor="white", linewidth=1))
    for j, (label, _, _) in enumerate(columns):
        xc = (x_edges[j] + x_edges[j + 1]) / 2
        lines = label.split("\n")
        if len(lines) == 1:
            ax.text(xc, y + header_h / 2, label, ha="center", va="center",
                    fontsize=9.5, fontweight="bold", color=COLOR_HEADER_TEXT)
        else:
            ax.text(xc, y + header_h / 2 - 0.13, lines[0], ha="center", va="center",
                    fontsize=9, fontweight="bold", color=COLOR_HEADER_TEXT)
            ax.text(xc, y + header_h / 2 + 0.13, lines[1], ha="center", va="center",
                    fontsize=9, fontweight="bold", color=COLOR_HEADER_TEXT)
    y += header_h

    # Échelle de couleur pour les 3 colonnes de %, partagée entre elles
    # (les 3 mesurent la même chose -- sévérité croissante -- donc une
    # échelle commune permet de comparer visuellement les colonnes entre elles)
    pct_cols = ["pct_2of3", "pct_3of3", "pct_3of3_strict"]
    all_pct = pd.concat([df[c] for c in pct_cols]).dropna()
    vmax_pct = float(all_pct.max()) if len(all_pct) else 1.0
    cmap = plt.get_cmap("YlOrRd")

    for i, (_, r) in enumerate(df.iterrows()):
        band = COLOR_ROW_A if i % 2 == 0 else COLOR_ROW_B
        for j, (_, key, _) in enumerate(columns):
            xc = (x_edges[j] + x_edges[j + 1]) / 2
            cell_color = band
            text_color = COLOR_HEADER
            fontweight = "normal"
            if key == "origin":
                text = r["origin"]
                fontweight = "bold"
            elif key == "n_total":
                text = f"{int(r['n_total']):,}".replace(",", " ")
            else:
                val = r[key]
                text = fmt_pct(val)
                fontweight = "bold"
                if pd.notna(val) and vmax_pct > 0:
                    rgba = cmap(min(val / vmax_pct, 1.0) * 0.85)  # *0.85 pour éviter le rouge le plus foncé
                    cell_color = rgba
                    # texte blanc si fond foncé, sombre sinon
                    luminance = 0.299 * rgba[0] + 0.587 * rgba[1] + 0.114 * rgba[2]
                    text_color = "white" if luminance < 0.6 else COLOR_HEADER
            ax.add_patch(Rectangle((x_edges[j], y), x_edges[j + 1] - x_edges[j], row_h,
                                    facecolor=cell_color, edgecolor=COLOR_GRID, linewidth=0.6))
            ax.text(xc, y + row_h / 2, text, ha="center", va="center",
                    fontsize=9.5, fontweight=fontweight, color=text_color)
        y += row_h

    ax.add_patch(Rectangle((margin, title_h), avail_w, y - title_h, fill=False,
                            edgecolor=COLOR_HEADER, linewidth=1.2))
    for xe in x_edges[1:-1]:
        ax.plot([xe, xe], [title_h, y], color=COLOR_GRID, linewidth=0.5)

    footnote = (f"σ = écart-type de (obs−pred_q50), par station/modèle  •  "
                f"3/3+{SIGMA_MARGIN}σ = 3/3 ET dépassement moyen (3 modèles) ≥ {SIGMA_MARGIN}σ")
    ax.text(margin, y + footnote_h / 2 + 0.05, footnote, ha="left", va="center",
            fontsize=7.3, color=COLOR_SUBTEXT, style="italic")

    fig.savefig(out_path, facecolor="white")
    plt.close(fig)
    print(f"✅ {out_path}")


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    rows = []
    for source in SOURCES:
        for freq in FREQS:
            r = compute_rates_for_origin(source, freq)
            if r:
                rows.append(r)

    if not rows:
        raise FileNotFoundError("Aucune origine exploitable -- vérifiez les fichiers _bands.csv.")

    df = pd.DataFrame(rows)
    print("\n" + df.to_string(index=False))

    df.to_csv(OUT_DIR / "table_outlier_rates.csv", index=False)
    render_table(df, OUT_DIR / "table_outlier_rates.png")


if __name__ == "__main__":
    main()