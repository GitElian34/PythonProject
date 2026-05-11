"""
resample_satellite_to_10D.py
═══════════════════════════════════════════════════════════════════════════
Rééchantillonne les .nc satellite sur une grille régulière 10D pour que
NeuralHydrology puisse calculer les lagged_features (water_level_shift1).

Méthode :
  1. Trouver la première et la dernière date de la station
  2. Créer une grille 10D régulière entre les deux
  3. "Snap" chaque mesure à la date de grille la plus proche (tolérance 5j)
  4. Les dates de grille sans mesure → NaN partout

⚠️ Sauvegarde par-dessus les .nc existants. Backup recommandé avant.
═══════════════════════════════════════════════════════════════════════════
"""

import xarray as xr
import pandas as pd
import numpy as np
import shutil
from pathlib import Path

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
NC_DIR    = Path("./data/IA/NeuralHydrology_satellite/time_series")
BAK_DIR   = Path("./data/IA/NeuralHydrology_satellite/time_series_BACKUP")
TOLERANCE_DAYS = 5  # tolérance pour snap une mesure à la grille
ANCHOR_DATE    = "2016-01-01"  # date d'origine de la grille (pour aligner toutes les stations)

# Liste des stations à traiter (les 33 stations 10j)
STATIONS_FILE = Path("./AI/LSTM/NeuralHydro_satellite/stations_10j.txt")

# ═══════════════════════════════════════════════════════════════
# Backup avant modification
# ═══════════════════════════════════════════════════════════════
if not BAK_DIR.exists():
    print(f"📦 Backup en cours : {NC_DIR} → {BAK_DIR}")
    shutil.copytree(NC_DIR, BAK_DIR)
    print(f"✅ Backup terminé\n")
else:
    print(f"✅ Backup déjà présent : {BAK_DIR}\n")

# ═══════════════════════════════════════════════════════════════
# Liste des stations
# ═══════════════════════════════════════════════════════════════
with open(STATIONS_FILE) as f:
    stations = [l.strip() for l in f if l.strip()]
print(f"Stations à rééchantillonner : {len(stations)}\n")

# ═══════════════════════════════════════════════════════════════
# Rééchantillonnage station par station
# ═══════════════════════════════════════════════════════════════
ok, ko = 0, 0
stats = []

for sid in stations:
    nc_path = NC_DIR / f"{sid}.nc"
    if not nc_path.exists():
        print(f"  ⚠️  {sid} : .nc absent")
        ko += 1
        continue

    try:
        ds = xr.open_dataset(nc_path)
        dates_orig = pd.to_datetime(ds.date.values)
        n_orig = len(dates_orig)

        # Charger toutes les variables en DataFrame
        df_orig = ds.to_dataframe()
        ds.close()

        # ── Créer la grille 10D régulière ────────────────────────────────────
        # Aligner sur ANCHOR_DATE pour que toutes les stations partagent la même grille
        anchor = pd.Timestamp(ANCHOR_DATE)
        first_date = dates_orig.min()
        last_date  = dates_orig.max()

        # Premier point de grille >= first_date
        n_steps_before = int(np.ceil((first_date - anchor).days / 10))
        grid_start = anchor + pd.Timedelta(days=n_steps_before * 10)

        # Dernier point de grille <= last_date
        n_steps_after = int(np.floor((last_date - anchor).days / 10))
        grid_end = anchor + pd.Timedelta(days=n_steps_after * 10)

        grid = pd.date_range(grid_start, grid_end, freq='10D')

        # ── Snap chaque mesure originale à la date de grille la plus proche ──
        df_resampled = pd.DataFrame(index=grid, columns=df_orig.columns, dtype=float)
        df_resampled.index.name = 'date'

        for orig_date, row in df_orig.iterrows():
            # Trouver la date de grille la plus proche
            diffs = abs(grid - orig_date).total_seconds() / 86400  # en jours
            idx_min = np.argmin(diffs)
            if diffs[idx_min] <= TOLERANCE_DAYS:
                df_resampled.iloc[idx_min] = row.values

        # ── Sauvegarder le nouveau .nc ───────────────────────────────────────
        ds_new = xr.Dataset(
            {col: xr.Variable("date", df_resampled[col].values.astype(np.float32))
             for col in df_resampled.columns},
            coords={"date": df_resampled.index.values}
        )
        ds_new.to_netcdf(nc_path, engine="scipy", format="NETCDF3_CLASSIC")

        n_grid = len(grid)
        n_filled = df_resampled.dropna(how='all').shape[0]
        stats.append({
            'station': sid,
            'n_orig': n_orig,
            'n_grid': n_grid,
            'n_filled': n_filled,
            'pct_filled': round(100 * n_filled / n_grid, 1),
        })
        ok += 1

    except Exception as e:
        print(f"  ❌ {sid} : {e}")
        ko += 1

# ═══════════════════════════════════════════════════════════════
# Résumé
# ═══════════════════════════════════════════════════════════════
print(f"\n{'='*60}")
print(f"RÉSULTATS")
print(f"{'='*60}")
print(f"  ✅ OK : {ok}")
print(f"  ❌ KO : {ko}")

if stats:
    df_stats = pd.DataFrame(stats)
    print(f"\nTaux de remplissage de la grille 10D :")
    print(df_stats[['station', 'n_orig', 'n_grid', 'n_filled', 'pct_filled']].to_string(index=False))

# ═══════════════════════════════════════════════════════════════
# Vérification finale : la grille est-elle vraiment régulière ?
# ═══════════════════════════════════════════════════════════════
if stats:
    print(f"\n{'='*60}")
    print(f"VÉRIFICATION (1 station échantillon)")
    print(f"{'='*60}")
    sample_sid = stats[0]['station']
    ds = xr.open_dataset(NC_DIR / f"{sample_sid}.nc")
    dates = pd.to_datetime(ds.date.values)
    diffs = np.diff(dates).astype('timedelta64[D]').astype(int)
    unique_diffs = sorted(set(diffs))
    print(f"  {sample_sid}")
    print(f"  Nb dates  : {len(dates)}")
    print(f"  Période   : {dates[0].date()} → {dates[-1].date()}")
    print(f"  Diffs uniques : {unique_diffs}")
    if unique_diffs == [10]:
        print(f"  ✅ Grille parfaitement régulière 10D !")
    else:
        print(f"  ⚠️  Grille pas tout à fait régulière")
    ds.close()

print(f"\n📁 Pour restaurer en cas de problème :")
print(f"   rm -rf {NC_DIR} && cp -r {BAK_DIR} {NC_DIR}")