"""
Inspection rapide d'un fichier .nc satellite (ou insitu).
Usage : python inspect_nc.py chemin/vers/fichier.nc
        python inspect_nc.py chemin/vers/dossier/   (inspecte le premier .nc trouvé)
"""
import sys
import os
import glob
import xarray as xr
import numpy as np

def inspect(path):
    ds = xr.open_dataset(path)
    print(f"{'='*60}")
    print(f"FICHIER : {os.path.basename(path)}")
    print(f"{'='*60}")

    # Dimensions
    print(f"\n--- Dimensions ---")
    for dim, size in ds.dims.items():
        print(f"  {dim:20s} : {size}")

    # Coordonnées
    print(f"\n--- Coordonnées ---")
    for coord in ds.coords:
        c = ds.coords[coord]
        if np.issubdtype(c.dtype, np.datetime64):
            print(f"  {coord:20s} : {c.values[0]}  →  {c.values[-1]}  ({len(c)} pas)")
        else:
            print(f"  {coord:20s} : {c.dtype}, {c.values[:5]}...")

    # Variables
    print(f"\n--- Variables ({len(ds.data_vars)}) ---")
    for var in ds.data_vars:
        v = ds[var]
        vals = v.values.flatten()
        n_nan = np.isnan(vals).sum() if np.issubdtype(v.dtype, np.floating) else 0
        pct_nan = 100 * n_nan / len(vals) if len(vals) > 0 else 0
        print(f"  {var:25s} | shape {str(v.shape):15s} | dtype {str(v.dtype):10s} | "
              f"NaN {n_nan:6d} ({pct_nan:5.1f}%) | "
              f"min={np.nanmin(vals):10.4f}  max={np.nanmax(vals):10.4f}  "
              f"mean={np.nanmean(vals):10.4f}")

    # Attributs globaux
    if ds.attrs:
        print(f"\n--- Attributs globaux ---")
        for k, v in ds.attrs.items():
            print(f"  {k}: {v}")

    ds.close()
    print()

# --- Main ---
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python inspect_nc.py <fichier.nc ou dossier/>")
        sys.exit(1)

    target = sys.argv[1]

    if os.path.isdir(target):
        ncs = sorted(glob.glob(os.path.join(target, "*.nc")))
        if not ncs:
            print(f"Aucun .nc trouvé dans {target}")
            sys.exit(1)
        print(f"Trouvé {len(ncs)} fichiers .nc dans {target}")
        print(f"Inspection du premier : {ncs[0]}\n")
        inspect(ncs[0])
        if len(ncs) > 1:
            print(f"Inspection du dernier : {ncs[-1]}\n")
            inspect(ncs[-1])
    else:
        inspect(target)