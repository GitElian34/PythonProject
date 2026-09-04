"""
compare_validation_metrics_lastn.py
------------------------------------
Compare les métriques de validation (NSE, KGE, RMSE...) par époque pour
plusieurs runs NeuralHydrology, et identifie la meilleure époque de chacun.

Conçu pour les 4 runs "predict_last_n" :
    - arlstm_DtoD80_last10_...
    - arlstm_DtoD90_last10_...
    - arlstm_DtoD90_last15_...
    - arlstm_DtoD96_last10_...

Usage :
    Vérifier/adapter RUNS ci-dessous puis :
    python compare_validation_metrics_lastn.py
"""

import sys
import re
import pickle
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np


# ──────────────────────────────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────────────────────────────

RUNS_ROOT = Path("./runs")

# label lisible -> nom exact du dossier de run
RUNS = {
    # "Classique_10j": "arlstm_feat10j_Final_ModeleT_3107_110910",
    # "Classique_27j": "arlstm_feat27jFinalModeleT_3107_111332",
    #
    "DtoD80_quantile": "arlstm_DtoD80_quantile_3006_155128",
    "DtoD90_quantile": "arlstm_DtoD90_quantile_3006_154719",
    # "DtoD96_quantile": "arlstm_DtoD96_quantile_3006_155152",

    # "DtoD80": "arlstm_DtoD80_1506_150002",
    "DtoD80Q_new": "arlstm_DtoD80_quantile_attention_Final_3107_145535",
    "DtoD90Q_new": "arlstm_DtoD90_quantile_attention_Final_0308_083446",

    # "DtoD90": "arlstm_DtoD90_1606_111709",
    # "DtoD96": "arlstm_DtoD96_1606_164901",
}

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

# Métrique utilisée pour désigner "la meilleure époque" d'un modèle.
# "Composite" = rang moyen sur NSE et KGE (rang 1 = meilleure époque sur cette
# métrique). Le score final est le rang moyen -> PLUS BAS = MEILLEUR.
# FHV volontairement exclu pour l'instant.
SELECTION_METRIC = "Composite"
COMPOSITE_RANK_METRICS = ["NSE", "KGE"]

OUT_DIR = Path("./data_processing/predict_last_n_comparison")
OUT_DIR.mkdir(parents=True, exist_ok=True)

COLORS = {
    "median": "#2a78d6",
    "mean": "#eda100",
}
LINESTYLES = {
    "median": "-",
    "mean": "--",
}

# Une couleur distincte par run pour le plot comparatif final
RUN_COLORS = ["#2a78d6", "#e04c4c", "#3fb27f", "#a35ee0", "#eda100", "#40b8c9"]


# ──────────────────────────────────────────────────────────────────────────────
# CHARGEMENT
# ──────────────────────────────────────────────────────────────────────────────

def find_epoch_dirs(run_dir: Path) -> list[tuple[int, Path]]:
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
        sys.exit(f"[ERREUR] Aucun validation_metrics.csv trouvé dans {run_dir}")

    return sorted(results, key=lambda x: x[0])


def load_all_epochs(epoch_files: list[tuple[int, Path]]) -> pd.DataFrame:
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
    non_metric_cols = {"epoch", "station", "station_id", "basin", "basin_id", "gauge_id"}
    available = [
        c for c in df.columns
        if c.lower() not in {x.lower() for x in non_metric_cols}
        and pd.api.types.is_numeric_dtype(df[c])
    ]
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
    result = {}
    grouped = df.groupby("epoch")
    for agg_name, agg_fn in {"median": np.nanmedian, "mean": np.nanmean}.items():
        rows = {}
        for epoch, group in grouped:
            rows[epoch] = {m: agg_fn(group[m].dropna().values) for m in metrics}
        df_agg = pd.DataFrame(rows).T.sort_index()

        # Score composite = rang moyen sur COMPOSITE_RANK_METRICS (rang 1 = meilleur
        # sur cette métrique). Plus le score final est BAS, meilleure est l'époque.
        available_rank_metrics = [m for m in COMPOSITE_RANK_METRICS if m in df_agg.columns]
        if available_rank_metrics:
            ranks = pd.DataFrame({
                m: df_agg[m].rank(ascending=False, method="average")
                for m in available_rank_metrics
            })
            df_agg["Composite"] = ranks.mean(axis=1)

        result[agg_name] = df_agg
    return result


def best_epoch_for_metric(df_median: pd.DataFrame, metric: str) -> tuple[int, float]:
    series = df_median[metric].dropna()
    if series.empty:
        return None, None
    if metric.upper() == "RMSE" or metric == "Composite":
        return int(series.idxmin()), float(series.min())
    return int(series.idxmax()), float(series.max())


# ──────────────────────────────────────────────────────────────────────────────
# INSPECTION OPTIONNELLE : structure réelle de validation_results.p
# ──────────────────────────────────────────────────────────────────────────────

def inspect_results_pickle(run_dir: Path, epoch: int) -> None:
    """
    Ouvre validation_results.p pour une époque donnée et affiche les
    dimensions réelles stockées (utile pour comprendre concrètement
    comment predict_last_n > 1 est géré dans les résultats sauvegardés,
    plutôt que de le supposer).
    """
    p_path = run_dir / "validation" / f"model_epoch{epoch:03d}" / "validation_results.p"
    if not p_path.exists():
        print(f"[INFO] Pas de validation_results.p pour {run_dir.name} epoch {epoch}")
        return
    try:
        with open(p_path, "rb") as f:
            results = pickle.load(f)
    except Exception as e:
        print(f"[WARNING] Impossible de charger {p_path} : {e}")
        return

    # results est un dict {basin_id: {freq: xarray.Dataset (ou dict imbriqué)}}
    first_basin = next(iter(results))
    print(f"\n[INSPECTION] {run_dir.name} — bassin exemple : {first_basin}")
    basin_data = results[first_basin]

    def describe(obj, indent="    "):
        """Affiche récursivement la structure d'un objet (dict/xarray/array)."""
        try:
            import xarray as xr
            if isinstance(obj, (xr.Dataset, xr.DataArray)):
                print(f"{indent}[xarray] dims: {dict(obj.dims)}")
                if isinstance(obj, xr.Dataset):
                    for var in obj.data_vars:
                        print(f"{indent}  var '{var}': shape={obj[var].shape}, dims={obj[var].dims}")
                return
        except ImportError:
            pass

        if isinstance(obj, dict):
            for k, v in obj.items():
                print(f"{indent}clé '{k}' -> {type(v).__name__}")
                describe(v, indent + "  ")
        elif isinstance(obj, np.ndarray):
            print(f"{indent}[ndarray] shape={obj.shape}, dtype={obj.dtype}")
        elif hasattr(obj, "shape"):
            print(f"{indent}[{type(obj).__name__}] shape={obj.shape}")
        else:
            print(f"{indent}{type(obj).__name__} (pas de shape/dims exploitable)")

    describe(basin_data)
    print(f"  --> Si une variable/array a une dimension de taille {run_dir.name.split('last')[-1][:2] if 'last' in run_dir.name else '?'} "
          f"(ou proche de predict_last_n), c'est la preuve qu'il y a bien plusieurs pas de temps prédits par fenêtre "
          f"stockés séparément ; sinon, la série est déjà réduite à une seule valeur par date.")


# ──────────────────────────────────────────────────────────────────────────────
# PLOTS
# ──────────────────────────────────────────────────────────────────────────────

def plot_single_run(agg_data, metrics, run_label, run_name, out_path):
    n = len(metrics)
    ncols = min(3, n)
    nrows = (n + ncols - 1) // ncols
    fig, axes = plt.subplots(nrows, ncols, figsize=(6 * ncols, 4 * nrows), squeeze=False)
    fig.suptitle(f"Métriques de validation par époque\n{run_label} ({run_name})",
                 fontsize=14, fontweight="bold", y=1.01)

    epochs = list(agg_data["median"].index)

    for idx, metric in enumerate(metrics):
        row, col = divmod(idx, ncols)
        ax = axes[row][col]
        for agg_name, df_agg in agg_data.items():
            if metric not in df_agg.columns:
                continue
            values = df_agg[metric].values
            ax.plot(epochs, values, color=COLORS[agg_name], linestyle=LINESTYLES[agg_name],
                     linewidth=2, marker="o", markersize=4, label=agg_name, zorder=3)
            if metric.upper() == "RMSE":
                best_idx = int(np.nanargmin(values))
                label_txt = "min"
            else:
                best_idx = int(np.nanargmax(values))
                label_txt = "max"
            best_val = values[best_idx]
            ax.annotate(f"  {label_txt}={best_val:.3f}\n  (ep {epochs[best_idx]})",
                        xy=(epochs[best_idx], best_val), fontsize=8,
                        color=COLORS[agg_name], va="center")

        ax.set_title(metric, fontsize=12, fontweight="bold")
        ax.set_xlabel("Époque", fontsize=10)
        ax.set_ylabel(metric, fontsize=10)
        ax.xaxis.set_major_locator(mticker.MaxNLocator(integer=True))
        ax.grid(True, linestyle="--", alpha=0.4, zorder=0)
        ax.spines[["top", "right"]].set_visible(False)
        ax.legend(fontsize=9, framealpha=0.7)

    for idx in range(n, nrows * ncols):
        row, col = divmod(idx, ncols)
        axes[row][col].set_visible(False)

    plt.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"[OK] Figure sauvegardée : {out_path}")


def plot_comparison(all_models: dict, metrics: list[str], out_path: Path) -> None:
    """Un subplot par métrique, une courbe par modèle (médiane inter-stations)."""
    n = len(metrics)
    ncols = min(2, n)
    nrows = (n + ncols - 1) // ncols
    fig, axes = plt.subplots(nrows, ncols, figsize=(8 * ncols, 5 * nrows), squeeze=False)
    fig.suptitle("Comparaison des 4 modèles predict_last_n — médiane inter-stations",
                 fontsize=15, fontweight="bold", y=1.02)

    for idx, metric in enumerate(metrics):
        row, col = divmod(idx, ncols)
        ax = axes[row][col]
        for i, (label, data) in enumerate(all_models.items()):
            df_median = data["agg"]["median"]
            if metric not in df_median.columns:
                continue
            color = RUN_COLORS[i % len(RUN_COLORS)]
            ax.plot(df_median.index, df_median[metric].values, color=color,
                     linewidth=2, marker="o", markersize=4, label=label, zorder=3)
        ax.set_title(metric, fontsize=13, fontweight="bold")
        ax.set_xlabel("Époque", fontsize=10)
        ax.set_ylabel(metric, fontsize=10)
        ax.xaxis.set_major_locator(mticker.MaxNLocator(integer=True))
        ax.grid(True, linestyle="--", alpha=0.4, zorder=0)
        ax.spines[["top", "right"]].set_visible(False)
        ax.legend(fontsize=9, framealpha=0.8)

    for idx in range(n, nrows * ncols):
        row, col = divmod(idx, ncols)
        axes[row][col].set_visible(False)

    plt.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"[OK] Figure comparative sauvegardée : {out_path}")


# ──────────────────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────────────────

def main():
    all_models = {}

    for label, run_name in RUNS.items():
        run_dir = (RUNS_ROOT / run_name).resolve()
        if not run_dir.exists():
            print(f"[SKIP] Dossier introuvable pour {label} : {run_dir}")
            continue

        print(f"\n[INFO] === {label} ({run_name}) ===")
        epoch_files = find_epoch_dirs(run_dir)
        print(f"[INFO] {len(epoch_files)} époque(s) : {[e for e, _ in epoch_files]}")

        df_all = load_all_epochs(epoch_files)
        metrics = detect_metrics(df_all)
        agg_data = aggregate_by_epoch(df_all, metrics)

        best_ep, best_val = best_epoch_for_metric(agg_data["median"], SELECTION_METRIC)
        print(f"[INFO] Meilleure époque ({SELECTION_METRIC} médiane) : {best_ep} ({best_val:.4f})")

        # Top 3 des meilleures époques (classées par score Composite croissant = meilleur)
        df_median = agg_data["median"]
        if "Composite" in df_median.columns:
            top3 = df_median.sort_values("Composite").head(3)
            print(f"       Top 3 époques (critère : {SELECTION_METRIC}, plus bas = mieux) :")
            for rank, (epoch, row) in enumerate(top3.iterrows(), start=1):
                detail = "  ".join(f"{m}={row[m]:.4f}" for m in metrics if m in row)
                print(f"         #{rank}  epoch {epoch:>3}  Composite={row['Composite']:.4f}  |  {detail}")
        elif best_ep is not None:
            row = agg_data["median"].loc[best_ep]
            print("       Détail à cette époque :")
            for m in metrics:
                if m in row:
                    print(f"         {m:<12} = {row[m]:.4f}")

        all_models[label] = {
            "run_dir": run_dir,
            "df_all": df_all,
            "metrics": metrics,
            "agg": agg_data,
            "best_epoch": best_ep,
            "best_value": best_val,
        }

        # Plot individuel
        out_path = OUT_DIR / f"{label}_validation_metrics.png"
        plot_single_run(agg_data, metrics, label, run_name, out_path)

        # Inspection optionnelle de la structure réelle des résultats sauvegardés
        # (utile pour vérifier concrètement le traitement de predict_last_n)
        if best_ep is not None:
            inspect_results_pickle(run_dir, best_ep)

    if not all_models:
        sys.exit("[ERREUR] Aucun modèle n'a pu être chargé.")

    # Métriques communes à tous les modèles chargés, dans l'ordre de priorité
    common_metrics = [
        m for m in METRIC_PRIORITY
        if all(m in data["metrics"] for data in all_models.values())
    ]
    print(f"\n[INFO] Métriques communes utilisées pour la comparaison : {common_metrics}")

    # Tableau récapitulatif final — top 3 par modèle
    print("\n" + "=" * 70)
    print(f"RÉCAPITULATIF — top 3 époques par modèle (critère : {SELECTION_METRIC})")
    print("=" * 70)
    header = (f"{'Modèle':<40}{'Rang':>5}{'Époque':>8}" + "".join(f"{m:>12}" for m in common_metrics)
              + f"{'Composite':>12}")
    print(header)
    print("-" * len(header))
    for label, data in all_models.items():
        df_median = data["agg"]["median"]
        if "Composite" not in df_median.columns:
            continue
        top3 = df_median.sort_values("Composite").head(3)
        for rank, (epoch, row) in enumerate(top3.iterrows(), start=1):
            vals = "".join(f"{row[m]:>12.4f}" for m in common_metrics)
            model_label = label if rank == 1 else ""
            print(f"{model_label:<40}{rank:>5}{epoch:>8}{vals}{row['Composite']:>12.4f}")
        print("-" * len(header))
    print("=" * 70)

    # Plot comparatif
    if common_metrics:
        plot_comparison(all_models, common_metrics, OUT_DIR / "comparison_4_models.png")

    print(f"\n[OK] Tous les résultats sont dans : {OUT_DIR.resolve()}")


if __name__ == "__main__":
    main()