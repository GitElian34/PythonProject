"""
shift_dates_feat10j.py
═══════════════════════════════════════════════════════════════════════════
Décale les dates des fichiers .nc du dataset feat10j pour que tous les
décalages _d0 à _d9 commencent au même point (2016-01-01).

Avant :
    _d0 : 2016-01-01 → 2025-12-29
    _d1 : 2016-01-02 → 2025-12-30
    _d4 : 2016-01-05 → 2025-12-23   ← non aligné !

Après :
    _d0 : 2016-01-01 → 2025-12-29
    _d1 : 2016-01-01 → 2025-12-29
    _d4 : 2016-01-01 → 2025-12-23

Le shift est de -d jours pour le décalage _d.
═══════════════════════════════════════════════════════════════════════════
"""

import os
import re
import xarray as xr
import pandas as pd
from pathlib import Path

# ─── Paramètres ─────────────────────────────────────────────────────────────
NC_DIR = Path('./data/IA/NeuralHydrology_feat10j/time_series/')

# ═══════════════════════════════════════════════════════════════
# Parcours des fichiers
# ═══════════════════════════════════════════════════════════════
files = sorted(NC_DIR.glob('*.nc'))
print(f"Fichiers à traiter : {len(files)}\n")

ok = 0
already_shifted = 0
errors = []

for i, fpath in enumerate(files):
    # Extraire le décalage depuis le nom
    m = re.search(r'_d(\d+)\.nc$', fpath.name)
    if not m:
        errors.append((fpath.name, "format inattendu"))
        continue
    decalage = int(m.group(1))

    if decalage == 0:
        # _d0 n'a rien à shifter
        ok += 1
        continue

    try:
        ds = xr.open_dataset(fpath)

        # Vérifier si déjà shifté (la première date doit être 2016-01-01 + décalage)
        first_date = pd.Timestamp(ds.date.values[0])
        expected_unshifted = pd.Timestamp(f'2016-01-{1 + decalage:02d}')

        if first_date == pd.Timestamp('2016-01-01'):
            ds.close()
            already_shifted += 1
            continue

        # Shift de -decalage jours
        new_dates = pd.to_datetime(ds.date.values) - pd.Timedelta(days=decalage)
        ds_shifted = ds.assign_coords(date=new_dates.values)

        ds.close()

        # Sauvegarder par-dessus
        ds_shifted.to_netcdf(fpath, engine='scipy', format='NETCDF3_CLASSIC')
        ok += 1

    except Exception as e:
        errors.append((fpath.name, str(e)[:80]))

    if (i + 1) % 1000 == 0:
        print(f"  {i+1}/{len(files)} fichiers traités")

# ═══════════════════════════════════════════════════════════════
# Stats finales
# ═══════════════════════════════════════════════════════════════
print(f"\n{'='*55}")
print(f"RÉSULTATS")
print(f"{'='*55}")
print(f"  Fichiers shiftés        : {ok}")
print(f"  Fichiers déjà alignés   : {already_shifted}")
print(f"  Erreurs                 : {len(errors)}")

if errors:
    print(f"\nPremières erreurs :")
    for name, msg in errors[:10]:
        print(f"  {name} : {msg}")

# Vérification finale
print(f"\n{'='*55}")
print(f"VÉRIFICATION (un fichier par décalage)")
print(f"{'='*55}")
sample_files = []
for d in range(10):
    matches = list(NC_DIR.glob(f'*_d{d}.nc'))
    if matches:
        sample_files.append(matches[0])

for f in sample_files:
    try:
        ds = xr.open_dataset(f)
        print(f"  {f.name:<30} : {ds.date.values[0]} → {ds.date.values[-1]}")
        ds.close()
    except Exception:
        pass

print("\n✅ Terminé !")