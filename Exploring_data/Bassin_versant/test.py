import xarray as xr
import pandas as pd
import numpy as np
import random
from pathlib import Path

NC_DIR = Path("./data/IA/NeuralHydrology/time_series")
LAGS   = [1, 5, 10, 27]
N      = 500
SEED   = 42

random.seed(SEED)
all_nc = list(NC_DIR.glob("*.nc"))
selected = random.sample(all_nc, min(N, len(all_nc)))
print(f"{len(selected)} stations sélectionnées\n")

results = {lag: [] for lag in LAGS}

for nc_path in selected:
    try:
        ds = xr.open_dataset(nc_path)
        wl = pd.Series(ds["water_level"].values)
        ds.close()

        valid = wl.dropna()
        if len(valid) < 100:
            continue

        for lag in LAGS:
            ac = wl.autocorr(lag=lag)
            if not np.isnan(ac):
                results[lag].append(ac)

    except Exception:
        continue

print(f"{'Lag':>6}  {'Moyenne':>10}  {'Médiane':>10}  {'Std':>8}  {'N':>6}")
print(f"  {'-'*46}")
for lag in LAGS:
    vals = results[lag]
    print(f"  lag-{lag:<3}  {np.mean(vals):>10.3f}  {np.median(vals):>10.3f}  "
          f"{np.std(vals):>8.3f}  {len(vals):>6}")