"""
Évaluation sur 100 stations à au moins 500m d'un barrage
pour les epochs 10, 40 et 50 du run 2004_160725.
"""

import pickle
import random
import sqlite3
import numpy as np
import pandas as pd
import xarray as xr
import matplotlib.pyplot as plt
from pathlib import Path
from ruamel.yaml import YAML
from neuralhydrology.utils.config import Config
from neuralhydrology.evaluation.evaluate import start_evaluation

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
RUN_DIR    = Path("./runs/satellite_water_level_test_2104_151622")  # AR-LSTM
OUTPUT_DIR = Path("./data/IA/NeuralHydrology/")
BASINS_DIR = Path("./AI/LSTM/NeuralHydro/")
DB_PATH    = "./data/insitu_data.db"
EPOCHS     = [20, 25]
N          = 200
SEED       = 42
MIN_DIST_M = 500
MIN_VALID_DAYS = 300
MIN_STD        = 0.05

TEST_BASIN_FILE = BASINS_DIR / "test_200_arlstm.txt"
TEST_DATA_DIR   = OUTPUT_DIR.parent / "NeuralHydrology_test_arlstm"

# ═══════════════════════════════════════════════════════════════
# SÉLECTION — 100 stations hors train/val + dist_barrage >= 500m
# ═══════════════════════════════════════════════════════════════
print(f"Sélection des {N} stations (dist_barrage >= {MIN_DIST_M}m)...")

with open(BASINS_DIR / "train_basins.txt") as f:
    used = set(f.read().splitlines())
with open(BASINS_DIR / "val_basins.txt") as f:
    used |= set(f.read().splitlines())

# Stations avec dist_barrage >= 500m depuis la BDD
conn = sqlite3.connect(DB_PATH)
df_dist = pd.read_sql(
    "SELECT code_sta FROM stations_insitu "
    "WHERE dist_barrage_m >= ? AND dist_barrage_m IS NOT NULL "
    "AND lon IS NOT NULL",
    conn, params=(MIN_DIST_M,)
)
conn.close()

eligibles = set(df_dist['code_sta'].tolist()) - used

# Filtrer sur les .nc disponibles + qualité 2024-2025
qualified = []
for sid in eligibles:
    nc_path = OUTPUT_DIR / "time_series" / f"{sid}.nc"
    if not nc_path.exists():
        continue
    try:
        ds    = xr.open_dataset(nc_path)
        wl    = ds.sel(date=slice("2024-01-01", "2025-12-31"))["water_level"].values
        ds.close()
        valid = wl[~np.isnan(wl)]
        if len(valid) >= MIN_VALID_DAYS and np.std(valid) >= MIN_STD:
            qualified.append(sid)
    except Exception:
        continue

print(f"  {len(qualified)} stations qualifiées → ", end="")
random.seed(SEED)
selected = random.sample(qualified, min(N, len(qualified)))
print(f"{len(selected)} sélectionnées")

with open(TEST_BASIN_FILE, 'w') as f:
    f.write('\n'.join(selected))

# ═══════════════════════════════════════════════════════════════
# DATA DIR
# ═══════════════════════════════════════════════════════════════
TEST_DATA_DIR.mkdir(exist_ok=True)
ts_link = TEST_DATA_DIR / "time_series"
if not ts_link.exists():
    ts_link.symlink_to((OUTPUT_DIR / "time_series").resolve())

attrs_dir = TEST_DATA_DIR / "attributes"
attrs_dir.mkdir(exist_ok=True)

ryaml = YAML()
with open(RUN_DIR / "config.yml", "r") as f:
    cfg_run = ryaml.load(f)
static_attrs = list(cfg_run.get("static_attributes", []))

conn = sqlite3.connect(DB_PATH)
ph   = ','.join(['?' for _ in selected])
attrs_full = pd.read_sql(f'''
    SELECT b.code_sta AS station_id, b.aire_km2,
           s.lon, s.lat,
           c.frac_urban, c.frac_agriculture, c.frac_forest,
           c.frac_semi_natural, c.frac_wetland, c.frac_water,
           c.sg_clay_0_30cm, c.sg_sand_0_30cm, c.sg_silt_0_30cm
    FROM bv_data b
    JOIN bv_corine c       ON b.code_sta = c.code_sta
    JOIN stations_insitu s ON b.code_sta = s.code_sta
    WHERE b.code_sta IN ({ph})
''', conn, params=selected)
conn.close()

cols = ["station_id"] + [c for c in static_attrs if c in attrs_full.columns]
attrs_full[cols].to_csv(attrs_dir / "attributes.csv", index=False)
print(f"  ✅ attributes.csv — {len(attrs_full)} stations")

# ═══════════════════════════════════════════════════════════════
# ÉVALUATION PAR EPOCH
# ═══════════════════════════════════════════════════════════════
resultats = {}
bins = [
    (-np.inf, 0,    "< 0",       "#d32f2f"),
    (0,   0.3,      "0.0–0.3",   "#f57c00"),
    (0.3, 0.5,      "0.3–0.5",   "#fbc02d"),
    (0.5, 0.7,      "0.5–0.7",   "#388e3c"),
    (0.7, np.inf,   "> 0.7",     "#1565c0"),
]

for epoch in EPOCHS:
    print(f"\n{'═'*55}")
    print(f"Évaluation epoch {epoch}...")

    test_config = RUN_DIR / f"config_eval_500m_epoch{epoch:03d}.yml"
    ryaml2 = YAML()
    ryaml2.preserve_quotes = True
    with open(RUN_DIR / "config.yml", "r") as f:
        cfg_dict = ryaml2.load(f)

    cfg_dict["test_basin_file"] = str(TEST_BASIN_FILE.resolve())
    cfg_dict["test_start_date"] = "01/01/2024"
    cfg_dict["test_end_date"]   = "31/12/2025"
    cfg_dict["data_dir"]        = str(TEST_DATA_DIR.resolve())
    cfg_dict["run_dir"]         = str(RUN_DIR.resolve())
    for key in ["train_basin_file", "validation_basin_file"]:
        cfg_dict.pop(key, None)

    with open(test_config, "w") as f:
        ryaml2.dump(cfg_dict, f)

    cfg = Config(test_config)
    start_evaluation(cfg=cfg, run_dir=RUN_DIR, epoch=epoch, period="test")

    candidates_p = list((RUN_DIR / "test").glob(f"*epoch{epoch:03d}*/*.p"))
    with open(sorted(candidates_p)[-1], "rb") as f:
        raw = pickle.load(f)

    scores = {}
    for station, data in raw.items():
        try:
            nse = data['1D']['NSE']
            kge = data['1D']['KGE']
            if not np.isnan(nse) and not np.isnan(kge):
                scores[station] = {'NSE': nse, 'KGE': kge}
        except Exception:
            continue

    nse_arr = np.array([v['NSE'] for v in scores.values()])
    kge_arr = np.array([v['KGE'] for v in scores.values()])

    trim    = 10
    nse_s   = sorted(nse_arr)
    nse_trim= nse_s[trim:-trim] if len(nse_s) > 2*trim else nse_s

    resultats[epoch] = {
        'scores'        : scores,
        'NSE_median'    : float(np.median(nse_arr)),
        'NSE_mean_trim' : float(np.mean(nse_trim)),
        'NSE_mean_raw'  : float(np.mean(nse_arr)),
        'KGE_median'    : float(np.median(kge_arr)),
        'n'             : len(scores),
    }

    print(f"  NSE médiane={resultats[epoch]['NSE_median']:.3f}  "
          f"NSE moy.trim={resultats[epoch]['NSE_mean_trim']:.3f}  "
          f"KGE médiane={resultats[epoch]['KGE_median']:.3f}  "
          f"n={resultats[epoch]['n']}")

    # Distribution
    n_total = len(nse_arr)
    print(f"  Distribution :")
    for lo, hi, label, _ in bins:
        n_cat = int(((nse_arr > lo) & (nse_arr <= hi)).sum())
        print(f"    {label:<10} : {n_cat:>4}  ({n_cat/n_total*100:.1f}%)")

# ═══════════════════════════════════════════════════════════════
# RÉSUMÉ COMPARATIF
# ═══════════════════════════════════════════════════════════════
print(f"\n\n{'='*65}")
print(f"RÉSUMÉ — {N} stations à +{MIN_DIST_M}m d'un barrage")
print(f"{'='*65}")
print(f"  {'Epoch':>6}  {'NSE méd.':>9}  {'NSE trim':>9}  {'NSE brut':>9}  {'KGE méd.':>9}  {'N':>5}")
print(f"  {'-'*55}")
for epoch, r in resultats.items():
    print(f"  {epoch:>6}  {r['NSE_median']:>9.3f}  {r['NSE_mean_trim']:>9.3f}  "
          f"{r['NSE_mean_raw']:>9.3f}  {r['KGE_median']:>9.3f}  {r['n']:>5}")

# ═══════════════════════════════════════════════════════════════
# PLOT COMPARATIF
# ═══════════════════════════════════════════════════════════════
fig, axes = plt.subplots(1, len(EPOCHS), figsize=(5 * len(EPOCHS), 5),
                         constrained_layout=True)
fig.suptitle(f"Distribution NSE — {N} stations (dist_barrage >= {MIN_DIST_M}m)", fontsize=12)

for ax, epoch in zip(axes, EPOCHS):
    r       = resultats[epoch]
    nse_arr = np.array([v['NSE'] for v in r['scores'].values()])
    n_total = len(nse_arr)
    counts  = [int(((nse_arr > lo) & (nse_arr <= hi)).sum()) for lo, hi, _, _ in bins]
    labels  = [lb for _, _, lb, _ in bins]
    colors  = [c  for _, _, _, c  in bins]

    bars = ax.bar(labels, counts, color=colors, edgecolor='white')
    ax.set_title(f"Epoch {epoch}\nNSE méd.={r['NSE_median']:.3f}  KGE méd.={r['KGE_median']:.3f}",
                 fontsize=9)
    ax.set_ylabel("N stations")
    ax.grid(axis='y', alpha=0.3)
    ax.tick_params(axis='x', labelsize=7)
    for bar, count in zip(bars, counts):
        if count > 0:
            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.2,
                    f"{count}\n({count/n_total*100:.0f}%)",
                    ha='center', va='bottom', fontsize=7, fontweight='bold')

out_path = Path("./nse_distribution_500m.png")
fig.savefig(out_path, dpi=150, bbox_inches='tight')
plt.close()
print(f"\n✅ Plot → {out_path}")