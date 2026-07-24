"""
correlate_sparsity_vs_nan.py
--------------------------------
Vérifie si le taux de NaN dans les prédictions (sim) est corrélé à la
sparsité des observations réelles (water_level) par station -- ce qui
confirmerait que le problème est propre aux stations peu couvertes par
le satellite, pas un bug général.

Combine :
  - nan_diagnosis_{RUN_NAME}.csv (déjà généré par diagnose_nan_predictions.py,
    sur TOUTES les stations, pas juste les 8 filtrées)
  - le % de NaN sur la colonne water_level lue directement dans les fichiers bruts

Vérifie aussi spécifiquement les 5 stations habituellement utilisées pour les
plots (21929, 24129, 23921, 24130, 18872), pour voir si elles sont mieux
couvertes que la moyenne (ce qui expliquerait qu'aucun NaN n'y ait jamais
été repéré visuellement).

Usage :
    python correlate_sparsity_vs_nan.py
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd


# ──────────────────────────────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────────────────────────────

DATA_DIR = Path("./data/IA/NeuralHydroDtoD90")  # reprendre la valeur qui a fonctionné
NAN_DIAGNOSIS_CSV = Path("./data_processing/predict_last_n_comparison/nan_diagnosis_arlstm_DtoD90_1606_111709.csv")

# ATTENTION : ce CSV doit avoir été généré SANS filtrer STATIONS_FILTER
# (mettre STATIONS_FILTER = [] dans diagnose_nan_predictions.py puis relancer
# sur le run classique si ce n'est pas déjà fait)

FIXED_PLOT_STATIONS = ["21929", "24129", "23921", "24130", "18872"]

OUT_CSV = Path("./data_processing/predict_last_n_comparison/sparsity_vs_nan_correlation.csv")


# ──────────────────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────────────────

def load_station_timeseries(data_dir: Path, station_id: str) -> pd.DataFrame:
    ts_dir = data_dir / "time_series"
    nc_path = ts_dir / f"{station_id}.nc"
    csv_path = ts_dir / f"{station_id}.csv"
    if nc_path.exists():
        import xarray as xr
        return xr.open_dataset(nc_path).to_dataframe()
    elif csv_path.exists():
        return pd.read_csv(csv_path, parse_dates=[0], index_col=0)
    else:
        raise FileNotFoundError(f"Ni {nc_path} ni {csv_path} n'existent.")


def main():
    if not NAN_DIAGNOSIS_CSV.exists():
        sys.exit(f"[ERREUR] {NAN_DIAGNOSIS_CSV} introuvable. "
                  f"Relancer diagnose_nan_predictions.py avec STATIONS_FILTER = [] d'abord.")

    df_nan = pd.read_csv(NAN_DIAGNOSIS_CSV)
    print(f"[INFO] {len(df_nan)} stations chargées depuis {NAN_DIAGNOSIS_CSV.name}")

    ts_dir = DATA_DIR / "time_series"
    rows = []
    for station_id in df_nan["station"]:
        try:
            df = load_station_timeseries(DATA_DIR, str(station_id))
        except Exception as e:
            print(f"[WARNING] {station_id} : {e}")
            continue
        if "water_level" not in df.columns:
            print(f"[WARNING] {station_id} : pas de colonne water_level")
            continue
        pct_nan_wl = 100 * df["water_level"].isna().mean()
        rows.append({"station": station_id, "pct_nan_water_level": pct_nan_wl})

    df_wl = pd.DataFrame(rows)
    df_merged = df_nan.merge(df_wl, on="station", how="inner")
    df_merged.to_csv(OUT_CSV, index=False)

    print(f"\n[INFO] {len(df_merged)} stations avec les deux infos disponibles")

    corr = df_merged["pct_nan_water_level"].corr(df_merged["pct_dates_fully_nan"])
    print(f"\n[RÉSULTAT] Corrélation (Pearson) entre %NaN(water_level) et %NaN(sim) : {corr:.3f}")

    print("\n" + "=" * 80)
    print("STATIONS TRIÉES PAR %NaN(water_level) CROISSANT (les mieux couvertes d'abord)")
    print("=" * 80)
    print(
        df_merged.sort_values("pct_nan_water_level")
        [["station", "pct_nan_water_level", "pct_dates_fully_nan"]]
        .to_string(index=False, float_format=lambda x: f"{x:.2f}")
    )

    print("\n" + "=" * 80)
    print("VÉRIFICATION DES 5 STATIONS HABITUELLEMENT UTILISÉES POUR LES PLOTS")
    print("=" * 80)
    fixed = df_merged[df_merged["station"].astype(str).isin(FIXED_PLOT_STATIONS)]
    if fixed.empty:
        print("[INFO] Aucune des 5 stations fixées n'est dans cet ensemble "
              "(elles ne font peut-être pas partie des stations de VALIDATION, "
              "mais d'un autre split -- à vérifier).")
    else:
        print(fixed[["station", "pct_nan_water_level", "pct_dates_fully_nan"]]
              .to_string(index=False, float_format=lambda x: f"{x:.2f}"))
        print(f"\nMédiane %NaN(water_level) -- ces 5 stations   : {fixed['pct_nan_water_level'].median():.2f}%")
        print(f"Médiane %NaN(water_level) -- toutes les stations : {df_merged['pct_nan_water_level'].median():.2f}%")
    print("=" * 80)

    print(f"\n[OK] Détail sauvegardé : {OUT_CSV}")


if __name__ == "__main__":
    main()