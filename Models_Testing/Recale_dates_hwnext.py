"""
recale_dates_residuals_hwnext.py
═══════════════════════════════════════════════════════════════════
Même logique que recale_dates_residuals.py (DAHITI), appliquée aux
résidus HW Next 10j et 27j.

Pour chaque station :
  - décalage = première date .nc avec water_level non-NaN
             - première date obs non-NaN dans le CSV résidus
  - applique ce décalage fixe à toutes les dates de la station

Usage :
    python recale_dates_residuals_hwnext.py
═══════════════════════════════════════════════════════════════════
"""

import pandas as pd
import numpy as np
import netCDF4 as nc
from pathlib import Path

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
RESIDUALS_DIR = Path("./Models_Testing/Residus")
DATE_DEB      = "2016-01-01"   # doit matcher DATE_DEB de create_dataset_DtoD.py

RUNS = [
    {
        "freq": "10j",
        "csv_in":  RESIDUALS_DIR / "residuals_10j_hwnext.csv",
        "nc_dir":  "./data/IA/NeuralHydrology_hydroweb_next/10j/time_series",
        "csv_out": RESIDUALS_DIR / "residuals_10j_hwnext_recale.csv",
    },
    {
        "freq": "27j",
        "csv_in":  RESIDUALS_DIR / "residuals_27j_hwnext.csv",
        "nc_dir":  "./data/IA/NeuralHydrology_hydroweb_next/27j/time_series",
        "csv_out": RESIDUALS_DIR / "residuals_27j_hwnext_recale.csv",
    },
]


def recale_one_freq(csv_in, nc_dir, csv_out, freq_label):
    print(f"\n{'=' * 60}")
    print(f"  RECALAGE {freq_label.upper()}")
    print(f"{'=' * 60}")

    if not Path(csv_in).exists():
        print(f"⚠ Fichier introuvable : {csv_in} -> ignoré")
        return None

    df_res = pd.read_csv(csv_in)
    df_res["date"] = pd.to_datetime(df_res["date"])
    df_res["station"] = df_res["station"].astype(str)

    stations = df_res["station"].unique()
    print(f"Stations dans le CSV : {len(stations)}")

    results = []
    n_ok, n_skip = 0, 0

    for code in stations:
        sub = df_res[df_res["station"] == code].sort_values("date").copy()

        sub_obs = sub.dropna(subset=["obs"])
        if len(sub_obs) == 0:
            n_skip += 1
            continue
        premiere_obs_csv = sub_obs["date"].iloc[0]

        nc_files = list(Path(nc_dir).glob(f"*{code}*.nc"))
        if not nc_files:
            n_skip += 1
            continue

        ds = nc.Dataset(nc_files[0])
        dates_nc = pd.to_datetime(DATE_DEB) + pd.to_timedelta(
            ds.variables["date"][:], unit="D"
        )
        wl_nc = ds.variables["water_level"][:]
        ds.close()

        mask_nc = ~np.isnan(wl_nc)
        if mask_nc.sum() == 0:
            n_skip += 1
            continue

        premiere_nc = dates_nc[mask_nc][0]
        decalage_j = int((premiere_nc - premiere_obs_csv).days)

        sub["date_orig"] = sub["date"]
        sub["date_recalee"] = sub["date"] + pd.Timedelta(days=decalage_j)
        sub["decalage_j"] = decalage_j

        results.append(sub)
        n_ok += 1

    if not results:
        print("⚠ Aucune station traitée avec succès")
        return None

    df_out = pd.concat(results, ignore_index=True)

    cols = ["station", "date_orig", "date_recalee", "decalage_j",
            "obs", "pred", "residual", "residual_norm", "score", "is_outlier", "year"]
    cols_present = [c for c in cols if c in df_out.columns]
    df_out = df_out[cols_present]

    df_out.to_csv(csv_out, index=False)

    print(f"  Stations traitées : {n_ok} | skippées : {n_skip}")
    print(f"  Lignes exportées  : {len(df_out)}")
    print(f"  CSV -> {csv_out}")

    dec = df_out.drop_duplicates("station")["decalage_j"]
    print(f"\n  Décalages appliqués :")
    print(f"    médiane = {dec.median():.0f}j | min = {dec.min()}j | max = {dec.max()}j")
    print(f"    valeurs uniques : {sorted(dec.unique().tolist())}")

    return df_out


for run in RUNS:
    recale_one_freq(run["csv_in"], run["nc_dir"], run["csv_out"], run["freq"])

print("\n✅ Terminé.")