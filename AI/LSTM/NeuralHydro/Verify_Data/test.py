"""
Évaluation du modèle sur 4 groupes de stations selon leur distance au barrage ROE.
Seuils : < 200 m | 200 m – 1 km | 1 km – 5 km | 5 km+
50 stations par groupe, résultats NSE/KGE médiane et moyenne.
"""

import pickle
import sqlite3
import numpy as np
import pandas as pd
import xarray as xr
import random
from pathlib import Path
from ruamel.yaml import YAML
from neuralhydrology.utils.config import Config
from neuralhydrology.evaluation.evaluate import start_evaluation

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
RUN_DIR    = Path("./runs/satellite_water_level_test_1704_153436")
OUTPUT_DIR = Path("./data/IA/NeuralHydrology/")
BASINS_DIR = Path("./AI/LSTM/NeuralHydro/")
DB_PATH    = "./data/insitu_data.db"
EPOCH      = 45
N_PAR_SEUIL = 150
SEED        = 42

SEUILS = [
    ("moins_100m",   0,      100,    "0-100 m"),
    ("100m_500",   100,      500,    "100-500 m"),
    ("200m_1km",     500,    1000,   "500 m – 1 km"),
    ("1km_5km",      1000,   5000,   "1 km – 5 km"),
    ("5km_plus",     5000,   1e9,    "> 5 km"),
]

# ═══════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════
def get_static_attrs(run_dir, db_path, stations):
    """Construit le attributes.csv pour un groupe de stations."""
    ryaml = YAML()
    with open(run_dir / "config.yml", "r") as f:
        cfg = ryaml.load(f)
    static_attrs = list(cfg.get("static_attributes", []))

    conn = sqlite3.connect(db_path)
    ph   = ','.join(['?' for _ in stations])
    attrs = pd.read_sql(f'''
        SELECT b.code_sta AS station_id, b.aire_km2,
               s.lon, s.lat,
               c.frac_urban, c.frac_agriculture, c.frac_forest,
               c.frac_semi_natural, c.frac_wetland, c.frac_water,
               c.sg_clay_0_30cm, c.sg_sand_0_30cm, c.sg_silt_0_30cm
        FROM bv_data b
        JOIN bv_corine c       ON b.code_sta = c.code_sta
        JOIN stations_insitu s ON b.code_sta = s.code_sta
        WHERE b.code_sta IN ({ph})
    ''', conn, params=stations)
    conn.close()

    cols = ["station_id"] + [c for c in static_attrs if c in attrs.columns]
    return attrs[cols]


def evaluer_groupe(run_dir, output_dir, basins_dir, db_path, epoch,
                   stations, label_court):
    """Lance l'évaluation NeuralHydrology sur un groupe de stations."""
    test_data_dir   = output_dir.parent / f"NeuralHydrology_eval_{label_court}"
    test_basin_file = basins_dir / f"test_{label_court}.txt"

    test_data_dir.mkdir(exist_ok=True)

    ts_link = test_data_dir / "time_series"
    if not ts_link.exists():
        ts_link.symlink_to((output_dir / "time_series").resolve())

    attrs_dir = test_data_dir / "attributes"
    attrs_dir.mkdir(exist_ok=True)

    attrs = get_static_attrs(run_dir, db_path, stations)
    attrs.to_csv(attrs_dir / "attributes.csv", index=False)

    with open(test_basin_file, 'w') as f:
        f.write('\n'.join(stations))

    # Config
    test_config = run_dir / f"config_eval_{label_court}.yml"
    ryaml = YAML()
    ryaml.preserve_quotes = True
    with open(run_dir / "config.yml", "r") as f:
        cfg_dict = ryaml.load(f)

    cfg_dict["test_basin_file"] = str(test_basin_file.resolve())
    cfg_dict["test_start_date"] = "01/01/2024"
    cfg_dict["test_end_date"]   = "31/12/2025"
    cfg_dict["data_dir"]        = str(test_data_dir.resolve())
    cfg_dict["run_dir"]         = str(run_dir.resolve())
    for key in ["train_basin_file", "validation_basin_file"]:
        cfg_dict.pop(key, None)

    with open(test_config, "w") as f:
        ryaml.dump(cfg_dict, f)

    cfg = Config(test_config)
    start_evaluation(cfg=cfg, run_dir=run_dir, epoch=epoch, period="test")

    candidates = list((run_dir / "test").glob(f"*epoch{epoch:03d}*/*.p"))
    with open(sorted(candidates)[-1], "rb") as f:
        results = pickle.load(f)

    nse_list, kge_list = [], []
    for station, data in results.items():
        try:
            nse = data['1D']['NSE']
            kge = data['1D']['KGE']
            if not np.isnan(nse): nse_list.append(nse)
            if not np.isnan(kge): kge_list.append(kge)
        except Exception:
            continue

    return {
        'NSE_median': float(np.median(nse_list)) if nse_list else np.nan,
        'NSE_mean'  : float(np.mean(nse_list))   if nse_list else np.nan,
        'KGE_median': float(np.median(kge_list)) if kge_list else np.nan,
        'KGE_mean'  : float(np.mean(kge_list))   if kge_list else np.nan,
        'n'         : len(nse_list),
    }

# ═══════════════════════════════════════════════════════════════
# SÉLECTION DES STATIONS PAR SEUIL
# ═══════════════════════════════════════════════════════════════
print("Chargement des distances depuis la BDD...")
conn = sqlite3.connect(DB_PATH)
df_dist = pd.read_sql(
    "SELECT code_sta, dist_barrage_m FROM stations_insitu "
    "WHERE dist_barrage_m IS NOT NULL AND lon IS NOT NULL",
    conn
)
conn.close()
print(f"  {len(df_dist)} stations avec distance barrage")

# Stations avec .nc disponibles
nc_files  = {f.stem for f in (OUTPUT_DIR / "time_series").glob("*.nc")}
df_dist   = df_dist[df_dist['code_sta'].isin(nc_files)]

# Exclure train/val
# with open(BASINS_DIR / "train_basins.txt") as f:
#     used = set(f.read().splitlines())
# with open(BASINS_DIR / "val_basins.txt") as f:
#     used |= set(f.read().splitlines())
# df_dist = df_dist[~df_dist['code_sta'].isin(used)]

print(f"  {len(df_dist)} stations candidates (hors train/val, avec .nc)")

random.seed(SEED)

groupes = {}
for label_court, lo, hi, label_long in SEUILS:
    mask      = (df_dist['dist_barrage_m'] >= lo) & (df_dist['dist_barrage_m'] < hi)
    candidats = df_dist[mask]['code_sta'].tolist()
    selected  = random.sample(candidats, min(N_PAR_SEUIL, len(candidats)))
    groupes[label_court] = {
        'stations'   : selected,
        'label_long' : label_long,
        'n_candidats': len(candidats),
    }
    print(f"  {label_long:<18} : {len(candidats)} candidats → {len(selected)} sélectionnées")

# ═══════════════════════════════════════════════════════════════
# ÉVALUATION PAR SEUIL
# ═══════════════════════════════════════════════════════════════
resultats = {}

for label_court, info in groupes.items():
    stations = info['stations']
    if not stations:
        print(f"\n⚠️  {info['label_long']} — pas assez de stations")
        continue

    print(f"\n{'═'*55}")
    print(f"Évaluation : {info['label_long']}  ({len(stations)} stations)")
    print(f"{'═'*55}")

    res = evaluer_groupe(
        run_dir    = RUN_DIR,
        output_dir = OUTPUT_DIR,
        basins_dir = BASINS_DIR,
        db_path    = DB_PATH,
        epoch      = EPOCH,
        stations   = stations,
        label_court= label_court,
    )
    resultats[label_court] = {**res, **info}
    print(f"  NSE médiane={res['NSE_median']:.3f}  NSE moy={res['NSE_mean']:.3f}  "
          f"KGE médiane={res['KGE_median']:.3f}  KGE moy={res['KGE_mean']:.3f}  n={res['n']}")

# ═══════════════════════════════════════════════════════════════
# RÉSUMÉ COMPARATIF
# ═══════════════════════════════════════════════════════════════
print("\n\n" + "="*80)
print(f"RÉSUMÉ — Impact de la proximité aux barrages sur les performances du modèle")
print(f"Epoch {EPOCH} | {N_PAR_SEUIL} stations par groupe")
print("="*80)
print(f"  {'Seuil':<18}  {'N cand.':>7}  {'N eval':>6}  "
      f"{'NSE méd.':>9}  {'NSE moy.':>9}  {'KGE méd.':>9}  {'KGE moy.':>9}")
print(f"  {'-'*75}")

for label_court, _, _, label_long in SEUILS:
    if label_court not in resultats:
        continue
    r = resultats[label_court]
    print(f"  {label_long:<18}  {r['n_candidats']:>7}  {r['n']:>6}  "
          f"{r['NSE_median']:>9.3f}  {r['NSE_mean']:>9.3f}  "
          f"{r['KGE_median']:>9.3f}  {r['KGE_mean']:>9.3f}")

print(f"\n  Référence (200 stations sans filtre) : NSE médiane ≈ 0.42")