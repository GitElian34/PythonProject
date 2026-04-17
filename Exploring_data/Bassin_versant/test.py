import xarray as xr
import numpy as np

ds = xr.open_dataset('./data/ERA5/usable_data_LAND_France/2016/07/data_0.nc')
pev = ds['pev']

# Même pixel, toutes les heures du 15 juillet
lat_idx, lon_idx = 60, 80
series = pev.isel(latitude=lat_idx, longitude=lon_idx).values

print("Heures 0-47 (juillet) :")
for i, v in enumerate(series[14*24:14*24+48]):
    print(f"  h{i:02d} : {v*1000:.4f} mm")