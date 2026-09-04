"""
recollage_dates_1a1.py
═══════════════════════════════════════════════════════════════════
Recolle les résultats d'évaluation NeuralHydrology (obs/pred, indexés
sur la grille théorique interne) aux VRAIES dates du .nc source, en
appariant 1-à-1 dans l'ordre chronologique (validé : même nombre
d'observations valides des deux côtés, ordre préservé).

Principe :
  1. Charge results.p -> extrait obs/pred dans l'ordre (peu importe
     la date "date" qu'il donne, on l'ignore).
  2. Charge le .nc source -> extrait les VRAIES dates où water_level
     est non-NaN, dans l'ordre chronologique.
  3. Apparie ligne i de obs/pred <-> i-ème vraie date du .nc.
  4. Vérifie que les valeurs concordent (sécurité) avant de valider
     l'appariement pour cette station.

Usage :
    python recollage_dates_1a1.py --model arlstm_feat27jFinalModeleT_3107_111332 \
        --epoch 25 --nc-dir ./data/IA/NeuralHydrology_hydroweb_next/27j/time_series \
        --out ./Models_Testing/Classic/residus/residuals_27j_hwnext_true_dates.csv
═══════════════════════════════════════════════════════════════════
"""

import argparse
import pickle
import numpy as np
import pandas as pd
import netCDF4 as ncdf
from pathlib import Path

DATE_DEB = "2016-01-01"
TARGET_VAR = "water_level"
TOLERANCE_VALUE = 1e-3  # tolérance pour vérifier obs == vraie valeur .nc


def get_true_dates_values(nc_path):
    """Vraies dates + valeurs où water_level est non-NaN, dans l'ordre chronologique."""
    ds = ncdf.Dataset(nc_path)
    dates = pd.to_datetime(DATE_DEB) + pd.to_timedelta(ds.variables["date"][:], unit="D")
    wl = np.array(ds.variables["water_level"][:])
    ds.close()
    mask = ~np.isnan(wl)
    return dates[mask], wl[mask]


def main(model, epoch, nc_dir, out_csv):
    results_p = Path(f"./runs/{model}/validation/model_epoch{epoch:03d}/validation_results.p")
    with open(results_p, "rb") as f:
        raw = pickle.load(f)
    print(f"{len(raw)} stations chargées depuis {results_p}")

    nc_dir = Path(nc_dir)
    rows = []
    n_ok, n_mismatch_count, n_mismatch_value, n_no_nc = 0, 0, 0, 0

    for sid, sub in raw.items():
        sid_str = str(sid)
        try:
            freq_key = list(sub.keys())[0]
            ds = sub[freq_key]["xr"]
            obs_var, sim_var = f"{TARGET_VAR}_obs", f"{TARGET_VAR}_sim"
            if obs_var not in ds or sim_var not in ds:
                continue

            obs_arr = ds[obs_var].values.flatten()
            pred_arr = ds[sim_var].values.flatten()
            mask_valid = ~np.isnan(obs_arr)
            obs_valid = obs_arr[mask_valid]
            pred_valid = pred_arr[mask_valid]

            nc_files = list(nc_dir.glob(f"*{sid_str}*.nc"))
            if not nc_files:
                n_no_nc += 1
                continue

            true_dates, true_values = get_true_dates_values(nc_files[0])

            if len(true_dates) != len(obs_valid):
                n_mismatch_count += 1
                print(f"  ⚠ {sid_str} : nb obs différent (.nc={len(true_dates)} "
                      f"vs results.p={len(obs_valid)}) -> SKIP")
                continue

            # Vérification de sécurité : les valeurs doivent concorder
            # (confirme que l'ordre chronologique est bien préservé)
            if not np.allclose(obs_valid, true_values, atol=TOLERANCE_VALUE, equal_nan=True):
                n_mismatch_value += 1
                diff = np.abs(obs_valid - true_values)
                print(f"  ⚠ {sid_str} : valeurs ne concordent pas (max diff={np.nanmax(diff):.4f}) -> SKIP")
                continue

            n_ok += 1
            for d, o, p in zip(true_dates, obs_valid, pred_valid):
                rows.append({
                    "station": sid_str,
                    "date": d,
                    "obs": float(o),
                    "pred": float(p) if not np.isnan(p) else np.nan,
                })

        except Exception as e:
            print(f"  ⚠ {sid_str} : erreur {e}")

    print(f"\nStations OK (recollées avec succès)     : {n_ok}")
    print(f"Stations skip (nb obs différent)         : {n_mismatch_count}")
    print(f"Stations skip (valeurs ne concordent pas): {n_mismatch_value}")
    print(f"Stations skip (.nc introuvable)          : {n_no_nc}")

    df = pd.DataFrame(rows)
    df.to_csv(out_csv, index=False)
    print(f"\n✅ CSV -> {out_csv} ({len(df)} lignes, {df['station'].nunique()} stations)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", required=True)
    parser.add_argument("--epoch", type=int, required=True)
    parser.add_argument("--nc-dir", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    main(args.model, args.epoch, args.nc_dir, args.out)