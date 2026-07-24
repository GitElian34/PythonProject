"""
plot_validation_metrics.py
--------------------------
Lit les validation_metrics.csv de chaque époque d'un run NeuralHydrology
et affiche les métriques clés (NSE, KGE, RMSE...) en fonction de l'époque.

Usage :
    Modifier la variable RUN_DIR ci-dessous puis lancer :
    python plot_validation_metrics.py
"""

import sys
import re
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np


# ──────────────────────────────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────────────────────────────

# Dossier du run à analyser (modifier ici selon le modèle à inspecter)
# RUN_DIR = Path("./runs/arlstm_DtoD80_quantile_3006_155128"  )
# RUN_DIR = Path("./runs/arlstm_DtoD90_quantile_3006_154719" )
# RUN_DIR = Path("./runs/arlstm_DtoD96_quantile_3006_155152" )
# RUN_DIR = Path("./runs/arlstm_DtoD96_periodic_0707_084706"  )

RUN_DIR = Path("./runs/arlstm_DtoD90_KGE_test_1507_112424"  )
# Métriques à afficher (si présentes dans le CSV).
# On essaie de détecter automatiquement les colonnes disponibles,
# mais cet ordre fixe la priorité d'affichage.
METRIC_PRIORITY = [
    "NSE",
    "KGE",
    "RMSE",
    "Pearson-r",
    "Alpha-NSE",
    "Beta-NSE",
    "FHV",
    "FLV",
]

# Agrégation inter-stations (le CSV contient une ligne par station)
AGGREGATIONS = {
    "median": np.nanmedian,
    "mean": np.nanmean,
}

COLORS = {
    "median": "#2a78d6",
    "mean":   "#eda100",
}

LINESTYLES = {
    "median": "-",
    "mean":   "--",
}


# ──────────────────────────────────────────────────────────────────────────────
# FONCTIONS
# ──────────────────────────────────────────────────────────────────────────────

def find_epoch_dirs(run_dir: Path) -> list[tuple[int, Path]]:
    """Retourne la liste (numéro_époque, chemin_csv) triée par époque."""
    validation_dir = run_dir / "validation"
    if not validation_dir.exists():
        sys.exit(f"[ERREUR] Dossier 'validation' introuvable dans : {run_dir}")

    results = []
    pattern = re.compile(r"model_epoch(\d+)$")
    for epoch_dir in sorted(validation_dir.iterdir()):
        m = pattern.match(epoch_dir.name)
        if m and epoch_dir.is_dir():
            csv_path = epoch_dir / "validation_metrics.csv"
            if csv_path.exists():
                results.append((int(m.group(1)), csv_path))

    if not results:
        sys.exit("[ERREUR] Aucun fichier validation_metrics.csv trouvé.")

    return sorted(results, key=lambda x: x[0])


def load_all_epochs(epoch_files: list[tuple[int, Path]]) -> pd.DataFrame:
    """Charge tous les CSV et retourne un DataFrame avec colonne 'epoch'."""
    frames = []
    for epoch, csv_path in epoch_files:
        try:
            df = pd.read_csv(csv_path)
            df["epoch"] = epoch
            frames.append(df)
        except Exception as e:
            print(f"[WARNING] Impossible de lire {csv_path} : {e}")
    if not frames:
        sys.exit("[ERREUR] Aucun CSV valide n'a pu être chargé.")
    return pd.concat(frames, ignore_index=True)


def detect_metrics(df: pd.DataFrame) -> list[str]:
    """Détecte les colonnes de métriques disponibles."""
    # Colonnes non-métriques à exclure
    non_metric_cols = {"epoch", "station", "station_id", "basin", "basin_id", "gauge_id"}
    available = [c for c in df.columns if c.lower() not in {x.lower() for x in non_metric_cols}
                 and pd.api.types.is_numeric_dtype(df[c])]

    # Trie selon METRIC_PRIORITY d'abord, puis le reste par ordre alphabétique
    priority_lower = [m.lower() for m in METRIC_PRIORITY]
    ordered = []
    for p in METRIC_PRIORITY:
        match = next((c for c in available if c.lower() == p.lower()), None)
        if match:
            ordered.append(match)
    for c in available:
        if c not in ordered:
            ordered.append(c)
    return ordered


def aggregate_by_epoch(df: pd.DataFrame, metrics: list[str]) -> dict[str, pd.DataFrame]:
    """
    Pour chaque agrégation, retourne un DataFrame indexé par epoch
    avec une colonne par métrique.
    """
    result = {}
    grouped = df.groupby("epoch")
    for agg_name, agg_fn in AGGREGATIONS.items():
        rows = {}
        for epoch, group in grouped:
            rows[epoch] = {m: agg_fn(group[m].dropna().values) for m in metrics}
        result[agg_name] = pd.DataFrame(rows).T.sort_index()
    return result


def plot_metrics(
    agg_data: dict[str, pd.DataFrame],
    metrics: list[str],
    run_name: str,
    out_path: Path,
) -> None:
    """Génère la figure avec un subplot par métrique."""
    n = len(metrics)
    ncols = min(3, n)
    nrows = (n + ncols - 1) // ncols

    fig, axes = plt.subplots(
        nrows, ncols,
        figsize=(6 * ncols, 4 * nrows),
        squeeze=False,
    )
    fig.suptitle(
        f"Métriques de validation par époque\n{run_name}",
        fontsize=14,
        fontweight="bold",
        y=1.01,
    )

    epochs = list(agg_data["median"].index)

    for idx, metric in enumerate(metrics):
        row, col = divmod(idx, ncols)
        ax = axes[row][col]

        for agg_name, df_agg in agg_data.items():
            if metric not in df_agg.columns:
                continue
            values = df_agg[metric].values
            ax.plot(
                epochs,
                values,
                color=COLORS[agg_name],
                linestyle=LINESTYLES[agg_name],
                linewidth=2,
                marker="o",
                markersize=4,
                label=agg_name,
                zorder=3,
            )
            # Annotation de la meilleure valeur
            if metric.upper() in ("RMSE",):
                best_idx = int(np.nanargmin(values))
                best_val = values[best_idx]
                label_txt = "min"
            else:
                best_idx = int(np.nanargmax(values))
                best_val = values[best_idx]
                label_txt = "max"

            ax.annotate(
                f"  {label_txt}={best_val:.3f}\n  (ep {epochs[best_idx]})",
                xy=(epochs[best_idx], best_val),
                fontsize=8,
                color=COLORS[agg_name],
                va="center",
            )

        ax.set_title(metric, fontsize=12, fontweight="bold")
        ax.set_xlabel("Époque", fontsize=10)
        ax.set_ylabel(metric, fontsize=10)
        ax.xaxis.set_major_locator(mticker.MaxNLocator(integer=True))
        ax.grid(True, linestyle="--", alpha=0.4, zorder=0)
        ax.spines[["top", "right"]].set_visible(False)
        ax.legend(fontsize=9, framealpha=0.7)

    # Masque les subplots vides
    for idx in range(n, nrows * ncols):
        row, col = divmod(idx, ncols)
        axes[row][col].set_visible(False)

    plt.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    print(f"[OK] Figure sauvegardée : {out_path}")
    plt.show()


def print_summary_table(agg_data: dict[str, pd.DataFrame], metrics: list[str]) -> None:
    """Affiche un tableau récapitulatif dans le terminal."""
    df_median = agg_data["median"]
    print("\n" + "=" * 60)
    print("RÉSUMÉ — médiane inter-stations par époque")
    print("=" * 60)
    cols = [m for m in metrics if m in df_median.columns]
    header = f"{'Époque':>8}  " + "  ".join(f"{c:>10}" for c in cols)
    print(header)
    print("-" * len(header))
    for epoch, row in df_median[cols].iterrows():
        vals = "  ".join(f"{v:>10.4f}" for v in row.values)
        print(f"{epoch:>8}  {vals}")
    print("=" * 60)

    # Meilleure époque par métrique
    print("\nMEILLEURE ÉPOQUE PAR MÉTRIQUE (médiane)")
    print("-" * 40)
    for m in cols:
        series = df_median[m].dropna()
        if series.empty:
            continue
        if m.upper() in ("RMSE",):
            best_ep = series.idxmin()
            best_val = series.min()
            direction = "min"
        else:
            best_ep = series.idxmax()
            best_val = series.max()
            direction = "max"
        print(f"  {m:<12} → époque {best_ep:>3}  ({direction} = {best_val:.4f})")
    print()


# ──────────────────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────────────────

def main():
    run_dir = RUN_DIR.resolve()
    if not run_dir.exists():
        sys.exit(f"[ERREUR] Dossier introuvable : {run_dir}")

    print(f"[INFO] Run : {run_dir.name}")

    # 1. Trouve les dossiers d'époque
    epoch_files = find_epoch_dirs(run_dir)
    print(f"[INFO] {len(epoch_files)} époque(s) trouvée(s) : "
          f"{[e for e, _ in epoch_files]}")

    # 2. Charge tous les CSV
    df_all = load_all_epochs(epoch_files)
    print(f"[INFO] {len(df_all)} lignes chargées "
          f"({df_all['epoch'].nunique()} époques × ~{len(df_all)//df_all['epoch'].nunique()} stations)")

    # 3. Détecte les métriques disponibles
    metrics = detect_metrics(df_all)
    print(f"[INFO] Métriques détectées : {metrics}")

    # 4. Agrège par époque
    agg_data = aggregate_by_epoch(df_all, metrics)

    # 5. Tableau récap terminal
    print_summary_table(agg_data, metrics)

    # 6. Plot
    out_path = run_dir / "validation_metrics_plot.png"
    plot_metrics(agg_data, metrics, run_dir.name, out_path)


if __name__ == "__main__":
    main()