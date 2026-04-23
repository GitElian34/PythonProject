"""
Évaluation comparative de 3 runs sur 200 stations aléatoires
- Métriques : médiane, moyenne brute, moyenne tronquée (±10)
- Sortie    : top 10 meilleures et 10 pires stations par run
- Attributs : lus dynamiquement depuis le config de chaque run
"""

import torch
import pickle
import numpy as np
import xarray as xr
import sqlite3
import pandas as pd
import random
from pathlib import Path
from ruamel.yaml import YAML

torch.set_num_threads(8)

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
RUN_DIRS = {
    "run1": Path("./runs/satellite_water_level_test_1704_121301"),
    "run2": Path("./runs/satellite_water_level_test_1704_083907"),
    "run3": Path("./runs/satellite_water_level_test_1704_083949"),
}

OUTPUT_DIR = Path("./data/IA/NeuralHydrology/")
BASINS_DIR = Path("./AI/LSTM/NeuralHydro/")
DB_PATH    = "./data/insitu_data.db"
EPOCHS     = [15]
N_STATIONS = 200
TRIM       = 10
N_TOP      = 10
SEED       = 42

TEST_BASIN_FILE = BASINS_DIR / "test_200_stations.txt"
TEST_DATA_DIR   = OUTPUT_DIR.parent / "NeuralHydrology_test200"
MIN_VALID_DAYS  = 300
MIN_STD         = 0.05

# ═══════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════
def trimmed_stats(values, n_trim):
    arr = sorted(values)
    trimmed = arr[n_trim:-n_trim] if len(arr) > 2 * n_trim else arr
    return {
        'median'      : np.median(values),
        'mean_trimmed': np.mean(trimmed) if trimmed else np.nan,
        'mean_raw'    : np.mean(values),
        'n_total'     : len(values),
        'n_trimmed'   : len(trimmed),
    }

def get_static_attrs(run_dir):
    """Lit les static_attributes depuis le config.yml du run."""
    ryaml = YAML()
    with open(run_dir / "config.yml", "r") as f:
        cfg = ryaml.load(f)
    return list(cfg.get("static_attributes", []))

# ═══════════════════════════════════════════════════════════════
# ÉTAPE 1 — Sélection des 200 stations (partagées entre tous les runs)
# ═══════════════════════════════════════════════════════════════
print("Étape 1 : Sélection des 200 stations...")

with open(BASINS_DIR / "train_basins.txt") as f:
    train_stations = set(f.read().splitlines())
with open(BASINS_DIR / "val_basins.txt") as f:
    val_stations = set(f.read().splitlines())

used_stations = train_stations | val_stations
nc_files      = list((OUTPUT_DIR / "time_series").glob("*.nc"))
available     = {f.stem for f in nc_files}
candidates    = list(available - used_stations)

print(f"  Filtrage qualité sur {len(candidates)} candidats (2024-2025)...")

qualified = []
for station_id in candidates:
    nc_path = OUTPUT_DIR / "time_series" / f"{station_id}.nc"
    try:
        ds      = xr.open_dataset(nc_path)
        ds_test = ds.sel(date=slice("2024-01-01", "2025-12-31"))
        wl      = ds_test["water_level"].values
        ds.close()
        valid = wl[~np.isnan(wl)]
        if len(valid) >= MIN_VALID_DAYS and np.std(valid) >= MIN_STD:
            qualified.append(station_id)
    except Exception:
        continue

print(f"  {len(qualified)} stations qualifiées")
random.seed(SEED)
selected = random.sample(qualified, min(N_STATIONS, len(qualified)))

with open(TEST_BASIN_FILE, 'w') as f:
    f.write('\n'.join(selected))
print(f"  ✅ {len(selected)} stations → {TEST_BASIN_FILE}")

# ═══════════════════════════════════════════════════════════════
# ÉTAPE 2 — Data dir partagé
# ═══════════════════════════════════════════════════════════════
print("\nÉtape 2 : Création du data_dir de test...")

TEST_DATA_DIR.mkdir(exist_ok=True)

ts_link = TEST_DATA_DIR / "time_series"
if not ts_link.exists():
    ts_link.symlink_to((OUTPUT_DIR / "time_series").resolve())

attrs_dir = TEST_DATA_DIR / "attributes"
attrs_dir.mkdir(exist_ok=True)

# Requête large — toutes les colonnes potentielles
conn = sqlite3.connect(DB_PATH)
placeholders = ','.join(['?' for _ in selected])
attrs_full = pd.read_sql(f'''
    SELECT b.code_sta AS station_id,
           b.aire_km2,
           s.lon,
           s.lat,
           c.frac_urban,
           c.frac_agriculture,
           c.frac_forest,
           c.frac_semi_natural,
           c.frac_wetland,
           c.frac_water,
           c.sg_clay_0_30cm,
           c.sg_sand_0_30cm,
           c.sg_silt_0_30cm
    FROM bv_data b
    JOIN bv_corine c       ON b.code_sta = c.code_sta
    JOIN stations_insitu s ON b.code_sta = s.code_sta
    WHERE b.code_sta IN ({placeholders})
''', conn, params=selected)
conn.close()

# Les attributs sont filtrés par run à l'étape 3 selon le config
print(f"  ✅ {len(attrs_full)} attributs chargés (colonnes brutes : {list(attrs_full.columns)})")

# ═══════════════════════════════════════════════════════════════
# ÉTAPE 3 — Évaluation par run
# ═══════════════════════════════════════════════════════════════
from neuralhydrology.utils.config import Config
from neuralhydrology.evaluation.evaluate import start_evaluation

all_results = {}
all_summary = {}

for run_name, run_dir in RUN_DIRS.items():
    print(f"\n{'═'*60}")
    print(f"RUN : {run_name}  ({run_dir.name})")
    print(f"{'═'*60}")

    # Lire les static_attributes attendus par CE run
    static_attrs = get_static_attrs(run_dir)
    cols_needed  = ["station_id"] + static_attrs
    cols_ok      = [c for c in cols_needed if c in attrs_full.columns]
    cols_missing = [c for c in cols_needed if c not in attrs_full.columns]

    if cols_missing:
        print(f"  ⚠️  Attributs manquants dans la DB : {cols_missing}")

    attrs_run = attrs_full[cols_ok]
    attrs_run.to_csv(attrs_dir / "attributes.csv", index=False)
    print(f"  ✅ attributes.csv — colonnes : {cols_ok}")

    # Config spécifique à ce run
    test_config = run_dir / "config_test200.yml"
    ryaml = YAML()
    ryaml.preserve_quotes = True

    with open(run_dir / "config.yml", "r") as f:
        cfg_dict = ryaml.load(f)

    cfg_dict["test_basin_file"] = str(TEST_BASIN_FILE.resolve())
    cfg_dict["test_start_date"] = "01/01/2024"
    cfg_dict["test_end_date"]   = "31/12/2025"
    cfg_dict["data_dir"]        = str(TEST_DATA_DIR.resolve())
    cfg_dict["run_dir"]         = str(run_dir.resolve())

    for key in ["train_basin_file", "validation_basin_file"]:
        cfg_dict.pop(key, None)

    with open(test_config, "w") as f:
        ryaml.dump(cfg_dict, f)

    all_results[run_name] = {}
    all_summary[run_name] = {}

    for epoch in EPOCHS:
        print(f"\n  Epoch {epoch}...")

        cfg = Config(test_config)
        start_evaluation(cfg=cfg, run_dir=run_dir, epoch=epoch, period="test")

        candidates_p = list((run_dir / "test").glob(f"*epoch{epoch:03d}*/*.p"))
        if not candidates_p:
            print(f"  ⚠️  Résultats non trouvés pour epoch {epoch}")
            continue

        with open(sorted(candidates_p)[-1], "rb") as f:
            results = pickle.load(f)

        station_nse, station_kge = {}, {}
        for station, data in results.items():
            try:
                nse = data['1D']['NSE']
                kge = data['1D']['KGE']
                if not np.isnan(nse): station_nse[station] = nse
                if not np.isnan(kge): station_kge[station] = kge
            except Exception:
                continue

        nse_stats = trimmed_stats(list(station_nse.values()), TRIM)
        kge_stats = trimmed_stats(list(station_kge.values()), TRIM)

        all_results[run_name][epoch] = station_nse
        all_summary[run_name][epoch] = {'nse': nse_stats, 'kge': kge_stats}

        print(f"  ✅ Epoch {epoch:2d} — "
              f"NSE médiane={nse_stats['median']:.3f} | "
              f"NSE moy.trim={nse_stats['mean_trimmed']:.3f} | "
              f"NSE moy.brute={nse_stats['mean_raw']:.3f} | "
              f"n={nse_stats['n_total']} (→{nse_stats['n_trimmed']} après trim±{TRIM})")

# ═══════════════════════════════════════════════════════════════
# RÉSUMÉ COMPARATIF
# ═══════════════════════════════════════════════════════════════
print("\n\n" + "=" * 90)
print(f"RÉSUMÉ COMPARATIF — {len(selected)} stations — moyenne tronquée (±{TRIM})")
print("=" * 90)
print(f"{'Run':<8} {'Epoch':>6} {'NSE médiane':>12} {'NSE moy.trim':>13} {'NSE moy.brute':>14} "
      f"{'KGE médiane':>12} {'KGE moy.trim':>13} {'N':>6}")
print("-" * 90)
for run_name in RUN_DIRS:
    for epoch, res in all_summary.get(run_name, {}).items():
        print(f"{run_name:<8} {epoch:>6} "
              f"{res['nse']['median']:>12.3f} "
              f"{res['nse']['mean_trimmed']:>13.3f} "
              f"{res['nse']['mean_raw']:>14.3f} "
              f"{res['kge']['median']:>12.3f} "
              f"{res['kge']['mean_trimmed']:>13.3f} "
              f"{res['nse']['n_total']:>6}")

# ═══════════════════════════════════════════════════════════════
# TOP 10 / FLOP 10 par run
# ═══════════════════════════════════════════════════════════════
for run_name in RUN_DIRS:
    if run_name not in all_summary or not all_summary[run_name]:
        continue

    best_epoch  = max(all_summary[run_name], key=lambda e: all_summary[run_name][e]['nse']['median'])
    station_nse = all_results[run_name][best_epoch]
    sorted_st   = sorted(station_nse.items(), key=lambda x: x[1])
    top10       = sorted_st[-N_TOP:][::-1]
    flop10      = sorted_st[:N_TOP]

    print(f"\n{'='*65}")
    print(f"{run_name.upper()}  ({RUN_DIRS[run_name].name})  — epoch {best_epoch}")
    print(f"{'='*65}")
    print(f"  TOP {N_TOP} :")
    print(f"  {'Rang':>4}  {'Station':<15}  {'NSE':>8}")
    print(f"  {'-'*4}  {'-'*15}  {'-'*8}")
    for i, (s, nse) in enumerate(top10, 1):
        print(f"  {i:>4}  {s:<15}  {nse:>8.3f}")

    print(f"\n  FLOP {N_TOP} :")
    print(f"  {'Rang':>4}  {'Station':<15}  {'NSE':>8}")
    print(f"  {'-'*4}  {'-'*15}  {'-'*8}")
    for i, (s, nse) in enumerate(flop10, 1):
        print(f"  {i:>4}  {s:<15}  {nse:>8.3f}")