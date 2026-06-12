"""
iterative_outlier_correction.py
═══════════════════════════════════════════════════════════════════════════
Correction itérative des outliers sur les stations satellite 10D :

  Iter 0 : évaluation normale (zeroshot)
  Iter k : remplace water_level[t] par pred[t] pour chaque outlier détecté
            à l'itération k-1, puis réévalue
  Max 3 itérations.

Les .nc originaux sont sauvegardés dans un dossier backup avant toute
modification. À la fin, les .nc sont restaurés.

Produit :
  - résultats iter 0/1/2/3 dans ./data/outlier_correction/
  - comparaison NSE avant/après par station
═══════════════════════════════════════════════════════════════════════════
"""

import pickle
import shutil
import sqlite3
import numpy as np
import pandas as pd
import xarray as xr
import torch
from pathlib import Path
from ruamel.yaml import YAML
from neuralhydrology.utils.config import Config
from neuralhydrology.evaluation.evaluate import start_evaluation

torch.set_num_threads(10)

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
RUN_DIR        = Path("./runs/arlstm_feat10jLow_modele2_0605_140952")
EPOCH          = 7
SATELLITE_DIR  = Path("./data/IA/NeuralHydrology_satellite_10D")
NC_DIR         = SATELLITE_DIR / "time_series"
STATIONS_FILE  = Path("./AI/LSTM/NeuralHydro_satellite_10D/stations_10j.txt")

OUT_DIR        = Path("./data/outlier_correction")
BACKUP_DIR     = OUT_DIR / "nc_backup"
OUT_DIR.mkdir(parents=True, exist_ok=True)
BACKUP_DIR.mkdir(parents=True, exist_ok=True)

OUTLIER_THRESHOLD = 3.0
MAX_ITER          = 3

# Période de test
TEST_START = "01/01/2016"
TEST_END   = "31/12/2025"


# ═══════════════════════════════════════════════════════════════
# FONCTIONS UTILITAIRES
# ═══════════════════════════════════════════════════════════════

def load_stations(stations_file):
    with open(stations_file) as f:
        return [l.strip() for l in f if l.strip()]


def build_eval_config(run_dir, epoch, satellite_dir, stations_file, iter_id):
    """Génère un config YAML temporaire pour l'évaluation satellite."""
    ryaml = YAML()
    ryaml.preserve_quotes = True
    with open(run_dir / "config.yml") as f:
        cfg = ryaml.load(f)

    cfg["test_basin_file"]  = str(stations_file.resolve())
    cfg["test_start_date"]  = TEST_START
    cfg["test_end_date"]    = TEST_END
    cfg["data_dir"]         = str(satellite_dir.resolve())
    cfg["run_dir"]          = str(run_dir.resolve())
    # Supprime les clés qui ne concernent pas le test
    for key in ["train_basin_file", "validation_basin_file"]:
        cfg.pop(key, None)

    config_path = run_dir / f"config_eval_satellite_iter{iter_id}.yml"
    with open(config_path, "w") as f:
        ryaml.dump(cfg, f)
    return config_path


def run_evaluation(run_dir, epoch, config_path, iter_id):
    """Lance start_evaluation et retourne le chemin du .p de résultats."""
    # Archive le dossier test existant pour forcer NeuralHydrology à en créer un nouveau
    test_dir  = run_dir / "test"
    epoch_dir = test_dir / f"model_epoch{epoch:03d}"
    if epoch_dir.exists():
        archived = test_dir / f"model_epoch{epoch:03d}_iter{iter_id}_archived"
        shutil.move(str(epoch_dir), str(archived))
        print(f"  Ancien dossier test archivé → {archived.name}")

    cfg = Config(config_path)
    start_evaluation(cfg=cfg, run_dir=run_dir, epoch=epoch, period="test")

    # Cherche le .p généré
    candidates = sorted(epoch_dir.glob("*.p")) if epoch_dir.exists() else []
    if not candidates:
        candidates = sorted(test_dir.glob(f"model_epoch{epoch:03d}/*.p"))
    if not candidates:
        raise FileNotFoundError(f"Pas de .p trouvé pour epoch {epoch} iter {iter_id}")
    return candidates[-1]


def extract_obs_pred(results_p):
    """
    Charge le .p et retourne un dict :
      { station : { 'dates': [...], 'obs': [...], 'pred': [...] } }
    """
    with open(results_p, "rb") as f:
        raw = pickle.load(f)

    out = {}
    for sid, sub in raw.items():
        try:
            freq   = list(sub.keys())[0]
            ds     = sub[freq]['xr']
            dates  = pd.to_datetime(ds.date.values)
            obs    = ds['water_level_obs'].values.flatten()
            pred   = ds['water_level_sim'].values.flatten()
            out[str(sid)] = {'dates': dates, 'obs': obs, 'pred': pred}
        except Exception as e:
            print(f"  ⚠  {sid} : {e}")
    return out


def detect_outliers(obs, pred, threshold=OUTLIER_THRESHOLD):
    """
    Retourne un masque booléen des outliers.
    Résidu normalisé par std des résidus de la station.
    """
    residus = obs - pred
    std     = np.nanstd(residus)
    if std == 0:
        return np.zeros(len(obs), dtype=bool)
    residu_norm = np.abs(residus) / std
    return residu_norm > threshold


def compute_nse(obs, pred):
    mask = ~(np.isnan(obs) | np.isnan(pred))
    if mask.sum() < 5:
        return np.nan
    o, s  = obs[mask], pred[mask]
    denom = np.sum((o - o.mean()) ** 2)
    return 1 - np.sum((o - s) ** 2) / denom if denom > 0 else np.nan


def backup_nc(nc_dir, backup_dir, stations):
    """Copie les .nc originaux dans backup_dir (une seule fois)."""
    for sid in stations:
        for nc_name in [f"{sid}.nc", f"{str(sid).zfill(13)}.nc"]:
            src = nc_dir / nc_name
            if src.exists():
                dst = backup_dir / nc_name
                if not dst.exists():
                    shutil.copy2(src, dst)
                break


def restore_nc(nc_dir, backup_dir):
    """Restaure les .nc originaux depuis backup_dir."""
    for nc_path in backup_dir.glob("*.nc"):
        shutil.copy2(nc_path, nc_dir / nc_path.name)
    print("  ✅ .nc originaux restaurés")


def apply_corrections(nc_dir, station_data, outlier_masks):
    """
    Pour chaque station, remplace water_level[dates_outlier] par pred[dates_outlier].
    Modifie les .nc directement.
    Retourne le nombre total de valeurs corrigées.
    """
    n_corrected = 0
    for sid, data in station_data.items():
        mask = outlier_masks.get(sid)
        if mask is None or mask.sum() == 0:
            continue

        # Cherche le .nc
        nc_path = None
        for nc_name in [f"{sid}.nc", f"{str(sid).zfill(13)}.nc"]:
            p = nc_dir / nc_name
            if p.exists():
                nc_path = p
                break
        if nc_path is None:
            print(f"  ⚠  {sid} : .nc introuvable")
            continue

        # Charge, modifie, sauvegarde
        ds = xr.open_dataset(nc_path).load()  # load() pour pouvoir fermer et réécrire
        ds.close()

        dates_outlier = data['dates'][mask]
        pred_outlier  = data['pred'][mask]

        # Convertit les dates en format compatible xarray
        dates_outlier_np = np.array(dates_outlier, dtype='datetime64[ns]')

        wl = ds['water_level'].values.copy()
        dates_nc = pd.to_datetime(ds.date.values)

        n_replaced = 0
        for d, pv in zip(dates_outlier, pred_outlier):
            idx = np.where(dates_nc == d)[0]
            if len(idx) > 0 and not np.isnan(pv):
                wl[idx[0]] = pv
                n_replaced += 1

        if n_replaced > 0:
            # Reconstruit le dataset avec les valeurs modifiées
            ds_new = ds.copy()
            ds_new['water_level'] = xr.DataArray(
                wl,
                coords=ds['water_level'].coords,
                dims=ds['water_level'].dims,
                attrs=ds['water_level'].attrs,
            )
            # Écrit dans un fichier temporaire puis remplace
            tmp_path = nc_path.with_suffix('.tmp.nc')
            ds_new.to_netcdf(tmp_path)
            ds_new.close()
            tmp_path.replace(nc_path)
            n_corrected += n_replaced

    return n_corrected


# ═══════════════════════════════════════════════════════════════
# PIPELINE ITÉRATIF
# ═══════════════════════════════════════════════════════════════
print("=" * 65)
print("CORRECTION ITÉRATIVE DES OUTLIERS — STATIONS SATELLITE 10D")
print("=" * 65)

stations = load_stations(STATIONS_FILE)
print(f"\n{len(stations)} stations satellite")
print(f"Run : {RUN_DIR.name}  |  Epoch : {EPOCH}")
print(f"Seuil outlier : {OUTLIER_THRESHOLD}σ  |  Max itérations : {MAX_ITER}")

# Backup des .nc originaux
print(f"\nBackup des .nc originaux → {BACKUP_DIR}")
backup_nc(NC_DIR, BACKUP_DIR, stations)

# Stockage des résultats par itération
all_nse = {}   # { iter_id : { sid : nse } }

try:
    for iter_id in range(MAX_ITER + 1):  # iter 0 = baseline, puis 1/2/3
        print(f"\n{'─'*65}")
        print(f"ITÉRATION {iter_id}{' (baseline)' if iter_id == 0 else ''}")
        print(f"{'─'*65}")

        # ── Évaluation ───────────────────────────────────────────────────
        config_path = build_eval_config(
            RUN_DIR, EPOCH, SATELLITE_DIR, STATIONS_FILE, iter_id
        )
        print(f"Lancement de l'évaluation...")
        results_p = run_evaluation(RUN_DIR, EPOCH, config_path, iter_id)
        print(f"Résultats : {results_p}")

        station_data = extract_obs_pred(results_p)

        # Sauvegarde du .p de cette itération
        out_p = OUT_DIR / f"results_iter{iter_id}.p"
        shutil.copy2(results_p, out_p)

        # ── Métriques ────────────────────────────────────────────────────
        nse_iter = {}
        outlier_masks = {}
        n_outliers_total = 0

        for sid, data in station_data.items():
            # Détecte les outliers sur cette itération
            mask_out = detect_outliers(data['obs'], data['pred'])
            # NSE calculé uniquement sur les points non-outliers
            mask_clean = ~mask_out
            nse = compute_nse(data['obs'][mask_clean], data['pred'][mask_clean])
            nse_iter[sid] = nse
            if iter_id < MAX_ITER:
                outlier_masks[sid] = mask_out
                n_outliers_total += mask_out.sum()

        all_nse[iter_id] = nse_iter

        nse_vals = [v for v in nse_iter.values() if not np.isnan(v)]
        print(f"\n  Stations évaluées : {len(nse_vals)}")
        print(f"  NSE médian        : {np.median(nse_vals):.4f}")
        print(f"  NSE > 0.5         : {sum(v > 0.5 for v in nse_vals)}/{len(nse_vals)}")
        print(f"  NSE < 0           : {sum(v < 0   for v in nse_vals)}/{len(nse_vals)}")
        if iter_id < MAX_ITER:
            print(f"  Outliers détectés : {n_outliers_total}")

        if iter_id == MAX_ITER:
            break

        if n_outliers_total == 0:
            print(f"\n  ✅ Plus d'outliers détectés — arrêt à l'itération {iter_id}")
            break

        # ── Correction des .nc ───────────────────────────────────────────
        print(f"\n  Application des corrections sur les .nc...")
        n_corr = apply_corrections(NC_DIR, station_data, outlier_masks)
        print(f"  {n_corr} valeurs remplacées dans les .nc")

finally:
    # ── Restauration des .nc originaux ───────────────────────────────────
    print(f"\n{'─'*65}")
    print("Restauration des .nc originaux...")
    restore_nc(NC_DIR, BACKUP_DIR)

# ═══════════════════════════════════════════════════════════════
# COMPARAISON FINALE
# ═══════════════════════════════════════════════════════════════
print(f"\n{'═'*65}")
print("COMPARAISON NSE PAR ITÉRATION")
print(f"{'═'*65}")

iters_done = sorted(all_nse.keys())
all_sids   = sorted(set(sid for d in all_nse.values() for sid in d))

rows = []
for sid in all_sids:
    row = {'station': sid}
    for it in iters_done:
        row[f'NSE_iter{it}'] = all_nse[it].get(sid, np.nan)
    rows.append(row)

df_comp = pd.DataFrame(rows)

# Delta total
if len(iters_done) >= 2:
    df_comp['delta_NSE'] = (
        df_comp[f'NSE_iter{iters_done[-1]}'] - df_comp['NSE_iter0']
    )

print(f"\n{'Station':<20}", end="")
for it in iters_done:
    print(f"{'Iter '+str(it):>12}", end="")
if 'delta_NSE' in df_comp.columns:
    print(f"{'Δ NSE':>10}", end="")
print()
print("─" * (20 + 12 * len(iters_done) + 10))

for _, row in df_comp.iterrows():
    print(f"{str(row['station']):<20}", end="")
    for it in iters_done:
        v = row[f'NSE_iter{it}']
        print(f"{v:>12.3f}" if not np.isnan(v) else f"{'nan':>12}", end="")
    if 'delta_NSE' in df_comp.columns:
        d = row['delta_NSE']
        print(f"{d:>+10.3f}" if not np.isnan(d) else f"{'nan':>10}", end="")
    print()

print("─" * (20 + 12 * len(iters_done) + 10))
for it in iters_done:
    vals = df_comp[f'NSE_iter{it}'].dropna()
    print(f"{'MÉDIANE':<20}" if it == iters_done[0] else f"{'':<20}", end="")
    print(f"{vals.median():>12.3f}", end="")
print()

# Sauvegarde CSV
csv_path = OUT_DIR / "comparaison_nse_iterations.csv"
df_comp.to_csv(csv_path, index=False)
print(f"\n✅ Résultats sauvegardés : {csv_path}")
print(f"✅ .p par itération dans : {OUT_DIR}")