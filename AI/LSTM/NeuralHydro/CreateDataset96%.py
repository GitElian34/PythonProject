"""
create_dataset_DtoD96.py
════════════════════════════════════════════════════════════════════════
Crée le dataset NeuralHydroDtoD96 en copiant les .nc de NeuralHydroDtoD0
et en masquant 96% des valeurs non-NaN du water_level.

Simule un passage satellite ~27j (1 mesure tous les ~27j ≈ 4% des jours).

Ne modifie PAS le dataset source.

Usage :
    python create_dataset_DtoD96.py
════════════════════════════════════════════════════════════════════════
"""

import numpy as np
import xarray as xr
import shutil
from pathlib import Path

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
SRC_DIR  = Path("./data/IA/NeuralHydroDtoD0")
DST_DIR  = Path("./data/IA/NeuralHydroDtoD96")
NAN_RATE = 0.96
SEED     = 42

SRC_TS  = SRC_DIR / "time_series"
DST_TS  = DST_DIR / "time_series"
SRC_ATT = SRC_DIR / "attributes"
DST_ATT = DST_DIR / "attributes"

# ═══════════════════════════════════════════════════════════════
# INIT
# ═══════════════════════════════════════════════════════════════
DST_TS.mkdir(parents=True, exist_ok=True)
DST_ATT.mkdir(parents=True, exist_ok=True)

# Copie des attributes et basin files (identiques au DtoD0)
print("Copie attributes...")
for f in SRC_ATT.glob("*"):
    shutil.copy2(f, DST_ATT / f.name)
    print(f"  {f.name}")

# Copie des train/val basins
SRC_BASINS = Path("./AI/LSTM/NeuralHydroDtoD0")
DST_BASINS = Path("./AI/LSTM/NeuralHydroDtoD96")
DST_BASINS.mkdir(parents=True, exist_ok=True)
for f in SRC_BASINS.glob("*.txt"):
    shutil.copy2(f, DST_BASINS / f.name)
    print(f"  basins : {f.name}")

# ═══════════════════════════════════════════════════════════════
# MASQUAGE 96%
# ═══════════════════════════════════════════════════════════════
nc_files = sorted(SRC_TS.glob("*.nc"))
print(f"\n{len(nc_files)} fichiers .nc à traiter\n")

rng = np.random.default_rng(SEED)
n_ok = 0
n_skip = 0

for i, src_path in enumerate(nc_files):
    dst_path = DST_TS / src_path.name

    if dst_path.exists():
        n_ok += 1
        continue

    try:
        ds = xr.open_dataset(src_path, engine="scipy")
        ds_new = ds.copy(deep=True)

        if "water_level" in ds_new:
            wl = ds_new["water_level"].values.copy().astype(float)
            valid_idx = np.where(~np.isnan(wl))[0]
            n_mask = int(len(valid_idx) * NAN_RATE)

            if n_mask > 0:
                mask_idx = rng.choice(valid_idx, size=n_mask, replace=False)
                wl[mask_idx] = np.nan
                ds_new["water_level"].values[:] = wl

        ds_new.attrs["nan_rate"] = NAN_RATE
        ds.close()

        ds_new.to_netcdf(dst_path, engine="scipy", format="NETCDF3_CLASSIC")
        ds_new.close()
        n_ok += 1

    except Exception as e:
        print(f"  ⚠ {src_path.name} : {e}")
        n_skip += 1

    if (i + 1) % 200 == 0:
        print(f"  {i+1}/{len(nc_files)} traités...")

print(f"\n{'='*55}")
print(f"  Dataset     : {DST_DIR}")
print(f"  Masquage    : {int(NAN_RATE*100)}% des valeurs non-NaN")
print(f"  .nc générés : {n_ok}")
print(f"  .nc skippés : {n_skip}")
print(f"{'='*55}")
print("""
Config NeuralHydrology à créer : config_DtoD96.yml
  experiment_name: arlstm_DtoD96
  data_dir: ./data/IA/NeuralHydroDtoD96
  train_basin_file: ./AI/LSTM/NeuralHydroDtoD96/train_basins.txt
  validation_basin_file: ./AI/LSTM/NeuralHydroDtoD96/val_basins.txt
  (reste identique aux autres configs DtoD)
""")