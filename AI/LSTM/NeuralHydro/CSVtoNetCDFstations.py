"""
Conversion CSV → NetCDF pour NeuralHydrology
Utilise scipy pour écrire — compatible avec xarray/NeuralHydrology
"""

import pandas as pd
import numpy as np
import xarray as xr
import os

TIME_SERIES_DIR = "./data/IA/NeuralHydrology/time_series/"

csv_files = [f for f in os.listdir(TIME_SERIES_DIR) if f.endswith(".csv")]
print(f"{len(csv_files)} fichiers CSV à convertir\n")

for fname in csv_files:
    basin_id = fname.replace(".csv", "")
    csv_path = os.path.join(TIME_SERIES_DIR, fname)
    nc_path  = os.path.join(TIME_SERIES_DIR, f"{basin_id}.nc")

    # Supprimer l'ancien .nc si existant
    if os.path.exists(nc_path):
        os.remove(nc_path)

    # Charger le CSV
    df = pd.read_csv(csv_path, parse_dates=["date"])
    df = df.set_index("date").sort_index()

    # Construire le Dataset xarray variable par variable
    data_vars = {}
    for col in df.columns:
        values = df[col].values.astype(np.float32)
        data_vars[col] = xr.Variable("date", values)

    ds = xr.Dataset(
        data_vars,
        coords={"date": df.index.values}
    )

    # Écrire avec le moteur scipy (format NetCDF3 — le plus compatible)
    ds.to_netcdf(nc_path, engine="scipy", format="NETCDF3_CLASSIC")

    print(f"  ✅ {basin_id}.nc ({len(df)} jours)")

print(f"\n✅ Terminé — {len(csv_files)} fichiers convertis")