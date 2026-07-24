"""
recale_dates_residuals.py
═══════════════════════════════════════════════════════════════════
Pour chaque station du CSV résidus DAHITI :
  - Calcule le décalage = première date .nc - première date obs CSV
  - Applique ce décalage à toutes les dates du CSV
  - Exporte un CSV avec : station, date_orig, date_recalee, obs, pred

Usage :
    python recale_dates_residuals.py
═══════════════════════════════════════════════════════════════════
"""

import pandas as pd
import numpy as np
import netCDF4 as nc
from pathlib import Path

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
CSV     = "./data/outlier_detection/residuals_10j_dahiti_clean.csv"
NC_DIR  = "./data/IA/NeuralHydrologyDahiti10jClean/10j/time_series"
OUTPUT  = "./AI/LSTM/NeuralHydro/Dahiti_test/Nexdtaset_dates_10j.csv"
# ═══════════════════════════════════════════════════════════════
# CHARGEMENT CSV
# ═══════════════════════════════════════════════════════════════
df_res = pd.read_csv(CSV)
df_res["date"]    = pd.to_datetime(df_res["date"])
df_res["station"] = df_res["station"].astype(str)

stations = df_res["station"].unique()
print(f"Stations dans le CSV : {len(stations)}")

# ═══════════════════════════════════════════════════════════════
# CALCUL DÉCALAGE ET RECALAGE PAR STATION
# ═══════════════════════════════════════════════════════════════
results = []
n_ok, n_skip = 0, 0

for code in stations:
    sub = df_res[df_res["station"] == code].sort_values("date").copy()

    # Première date avec obs non-NaN dans le CSV
    sub_obs = sub.dropna(subset=["obs"])
    if len(sub_obs) == 0:
        print(f"  ⚠ {code} : aucune obs dans le CSV, skip")
        n_skip += 1
        continue

    premiere_obs_csv = sub_obs["date"].iloc[0]

    # Première date du .nc
    nc_files = list(Path(NC_DIR).glob(f"*{code}*.nc"))
    if not nc_files:
        print(f"  ⚠ {code} : .nc introuvable, skip")
        n_skip += 1
        continue

    ds = nc.Dataset(nc_files[0])
    dates_nc = pd.to_datetime("2016-01-01") + pd.to_timedelta(
        ds.variables["date"][:], unit="D"
    )
    wl_nc = ds.variables["water_level"][:]
    ds.close()

    mask_nc = ~np.isnan(wl_nc)
    if mask_nc.sum() == 0:
        print(f"  ⚠ {code} : .nc sans valeurs non-NaN, skip")
        n_skip += 1
        continue

    premiere_nc = dates_nc[mask_nc][0]

    # Décalage fixe
    decalage_j = int((premiere_nc - premiere_obs_csv).days)

    # Appliquer le décalage à toutes les lignes de cette station
    sub = sub.copy()
    sub["date_orig"]   = sub["date"]
    sub["date_recalee"] = sub["date"] + pd.Timedelta(days=decalage_j)
    sub["decalage_j"]  = decalage_j

    results.append(sub)
    n_ok += 1

# ═══════════════════════════════════════════════════════════════
# EXPORT
# ═══════════════════════════════════════════════════════════════
df_out = pd.concat(results, ignore_index=True)

# Réordonner les colonnes
cols = ["station", "date_orig", "date_recalee", "decalage_j",
        "obs", "pred", "residual", "residual_norm", "score", "is_outlier", "year"]
cols_present = [c for c in cols if c in df_out.columns]
df_out = df_out[cols_present]

df_out.to_csv(OUTPUT, index=False)

print(f"\n{'='*55}")
print(f"  Stations traitées : {n_ok}")
print(f"  Stations skippées : {n_skip}")
print(f"  Lignes exportées  : {len(df_out)}")
print(f"  CSV → {OUTPUT}")

# ═══════════════════════════════════════════════════════════════
# RÉSUMÉ DES DÉCALAGES<
# ═══════════════════════════════════════════════════════════════
dec = df_out.drop_duplicates("station")["decalage_j"]
print(f"\n  Décalages appliqués :")
print(f"    médiane = {dec.median():.0f}j")
print(f"    min     = {dec.min()}j")
print(f"    max     = {dec.max()}j")
print(f"    valeurs uniques : {sorted(dec.unique().tolist())}")