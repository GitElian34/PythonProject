"""
permutation_importance_dynamic.py
═══════════════════════════════════════════════════════════════════════════
Permutation importance des variables DYNAMIQUES sur un modèle NeuralHydrology.

Approche SAFE :
  1. Copie les .nc des 200 stations test dans un dossier temporaire
  2. Crée un config.yml temporaire pointant vers ce dossier
  3. Pour chaque variable :
     a. Permute dans les copies (jamais les originaux)
     b. Évalue via le config temporaire
     c. Restaure les copies depuis les originaux
  4. Supprime le dossier temporaire à la fin

Les originaux et le config du run ne sont JAMAIS modifiés.
═══════════════════════════════════════════════════════════════════════════
"""

import os
import shutil
import subprocess
import pandas as pd
import numpy as np
import xarray as xr
from pathlib import Path
import time
import yaml
import torch

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES — À ADAPTER
# ═══════════════════════════════════════════════════════════════
RUN_DIR         = Path("./runs/arlstm_feat10jLow_modele2_0405_151732")
EPOCH           = 2
ORIGINAL_DIR    = Path("./data/IA/NeuralHydrology_feat10j/")
BASINS_DIR      = Path("./AI/LSTM/NeuralHydro_feat10j_low/")
TEMP_DIR        = Path("./data/IA/_perm_temp/")
OUT_CSV         = Path("./permutation_importance_dynamic_results.csv")
torch.set_num_threads(5)
N_STATIONS_TEST = 500
SEED            = 42

DYNAMIC_VARS = [
    "clim_mean",
    "clim_std",
    "doy_sin",
    "doy_cos"
]


# ═══════════════════════════════════════════════════════════════
# Fonctions utilitaires
# ═══════════════════════════════════════════════════════════════

def selectionner_stations_test(basins_dir, n_stations, seed):
    """
    Sélectionne N stations réelles au hasard depuis val_basins.txt,
    retourne la liste des station_id avec décalages (_d0..._d9).
    """
    val_path = basins_dir / "val_basins.txt"
    with open(val_path) as f:
        all_ids = [l.strip() for l in f if l.strip()]

    bases = list({sid.rsplit("_d", 1)[0] for sid in all_ids})
    rng = np.random.default_rng(seed)
    rng.shuffle(bases)
    bases = bases[:n_stations]

    test_ids = [sid for sid in all_ids if sid.rsplit("_d", 1)[0] in set(bases)]
    print(f"  {len(bases)} stations réelles → {len(test_ids)} IDs avec décalages")
    return test_ids, bases


def preparer_dossier_temp(original_dir, temp_dir, test_ids):
    """
    Crée le dossier temporaire avec :
      - time_series/ contenant uniquement les .nc des stations test
      - attributes/ copié tel quel
    """
    if temp_dir.exists():
        shutil.rmtree(temp_dir)

    ts_temp = temp_dir / "time_series"
    ts_temp.mkdir(parents=True)

    ts_orig = original_dir / "time_series"
    n_copied = 0
    for sid in test_ids:
        src = ts_orig / f"{sid}.nc"
        if src.exists():
            shutil.copy2(src, ts_temp / f"{sid}.nc")
            n_copied += 1

    attrs_orig = original_dir / "attributes"
    attrs_temp = temp_dir / "attributes"
    shutil.copytree(attrs_orig, attrs_temp)

    print(f"  {n_copied} .nc copiés dans {ts_temp}")
    return ts_temp


def creer_config_temp(run_dir, temp_dir, test_ids):
    """
    Copie le config.yml du run dans le dossier temp et modifie :
      - data_dir → dossier temp
      - test_basin_file → fichier basins des 200 stations
      - validation_basin_file → idem
    Retourne le chemin du config temporaire.
    """
    # Lire le config original
    config_path = run_dir / "config.yml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    # Écrire le fichier basins test dans le dossier temp
    basins_file = temp_dir / "test_basins.txt"
    with open(basins_file, "w") as f:
        f.write("\n".join(test_ids))

    # Modifier le config
    config["data_dir"] = str(temp_dir.resolve())
    config["test_basin_file"] = str(basins_file.resolve())
    config["validation_basin_file"] = str(basins_file.resolve())
    config["number_of_basins"] = len(test_ids)

    # Sauvegarder dans le dossier temp
    temp_config = temp_dir / "config_perm.yml"
    with open(temp_config, "w") as f:
        yaml.dump(config, f, default_flow_style=False)

    print(f"  Config temporaire : {temp_config}")
    print(f"  data_dir → {temp_dir.resolve()}")
    print(f"  basins → {len(test_ids)} IDs")
    return temp_config


def evaluer_modele(run_dir, epoch):
    """
    Lance nh-run evaluate avec le run_dir (qui contient le config modifié
    ou le config original selon le cas).
    Retourne le NSE médian par station réelle.
    """
    cmd = ["nh-run", "evaluate",
           "--run-dir", str(run_dir),
           "--epoch", str(epoch)]

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        stderr_lines = result.stderr.split('\n')
        important = [l for l in stderr_lines
                     if any(k in l for k in ['Error', 'error', 'Traceback', 'raise'])
                     and 'tensorflow' not in l.lower()
                     and 'oneDNN' not in l]
        if important:
            for line in important[-10:]:
                print(f"     {line}")
        else:
            # Afficher les dernières lignes si rien de filtré
            for line in stderr_lines[-10:]:
                if line.strip():
                    print(f"     {line}")
        return None

    # Chercher le CSV de métriques
    for subdir in ["test", "validation"]:
        metrics_dir = run_dir / subdir / f"model_epoch{epoch:03d}"
        if not metrics_dir.exists():
            continue
        for fname in ["test_metrics.csv", "validation_metrics.csv"]:
            p = metrics_dir / fname
            if p.exists():
                df = pd.read_csv(p, header=None, names=["station_d", "NSE", "KGE"])
                df["NSE"] = pd.to_numeric(df["NSE"], errors="coerce")
                df = df.dropna(subset=["NSE"])
                df["station"] = df["station_d"].str.replace(r"_d\d+$", "", regex=True)
                return df.groupby("station")["NSE"].mean().median()

    print("     ❌ Fichier métriques introuvable")
    return None


def neutraliser_variable_nc(ts_dir, test_ids, var_name):
    """
    Remplace toutes les valeurs d'une variable par sa moyenne (par station).
    Détruit toute la variabilité temporelle tout en gardant une valeur
    réaliste en magnitude.
    Travaille UNIQUEMENT dans le dossier temporaire.
    """
    for sid in test_ids:
        nc_path = ts_dir / f"{sid}.nc"
        if not nc_path.exists():
            continue

        ds = xr.open_dataset(nc_path, engine="scipy")
        if var_name not in ds:
            ds.close()
            continue

        vals = ds[var_name].values.copy()
        mean_val = np.nanmean(vals)
        # Remplacer les valeurs valides par la moyenne, garder les NaN
        vals[~np.isnan(vals)] = mean_val

        ds[var_name].values = vals
        ds.to_netcdf(nc_path, engine="scipy", format="NETCDF3_CLASSIC")
        ds.close()


def restaurer_nc_depuis_originaux(original_dir, ts_temp, test_ids):
    """Recopie les .nc originaux dans le dossier temp."""
    ts_orig = original_dir / "time_series"
    for sid in test_ids:
        src = ts_orig / f"{sid}.nc"
        if src.exists():
            shutil.copy2(src, ts_temp / f"{sid}.nc")


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("=" * 60)
    print("IMPORTANCE DES VARIABLES DYNAMIQUES (neutralisation par moyenne)")
    print(f"Run : {RUN_DIR}")
    print(f"Epoch : {EPOCH}")
    print("=" * 60)

    # --- 1. Sélection des stations test ---
    print(f"\n1. Sélection de {N_STATIONS_TEST} stations test...")
    test_ids, test_bases = selectionner_stations_test(
        BASINS_DIR, N_STATIONS_TEST, SEED
    )

    # --- 2. Préparer le dossier temporaire ---
    print(f"\n2. Préparation du dossier temporaire {TEMP_DIR}...")
    ts_temp = preparer_dossier_temp(ORIGINAL_DIR, TEMP_DIR, test_ids)

    # --- 3. Créer le config temporaire ---
    print(f"\n3. Création du config temporaire...")
    temp_config = creer_config_temp(RUN_DIR, TEMP_DIR, test_ids)

    # Copier le modèle entraîné dans le dossier temp pour que nh-run le trouve
    # nh-run evaluate cherche train_data/ dans le run_dir
    # → On va plutôt injecter le config temp dans le run_dir existant
    # Stratégie : remplacer temporairement le config.yml du run
    config_orig = RUN_DIR / "config.yml"
    config_backup = RUN_DIR / "config_BACKUP.yml"
    shutil.copy2(config_orig, config_backup)
    shutil.copy2(temp_config, config_orig)
    print(f"  config.yml du run remplacé (backup → config_BACKUP.yml)")

    try:
        # --- 4. Baseline ---
        print(f"\n4. Évaluation baseline...")
        t0 = time.time()
        baseline_nse = evaluer_modele(RUN_DIR, EPOCH)
        if baseline_nse is None:
            print("\n❌ Impossible d'obtenir le baseline.")
            raise RuntimeError("Baseline failed")
        dt = time.time() - t0
        print(f"  ✅ NSE baseline médian : {baseline_nse:.4f}  ({dt:.0f}s)")
        print(f"  Estimation totale : ~{dt * (len(DYNAMIC_VARS) + 1) / 60:.0f} min")

        # --- 5. Permutation de chaque variable ---
        results = []

        for i, var in enumerate(DYNAMIC_VARS):
            print(f"\n[{i+1}/{len(DYNAMIC_VARS)}] Permutation de '{var}'...")
            t1 = time.time()

            # Neutraliser (remplacer par la moyenne) dans le dossier temp
            neutraliser_variable_nc(ts_temp, test_ids, var)

            # Évaluer
            nse_perm = evaluer_modele(RUN_DIR, EPOCH)

            # Restaurer les .nc depuis les originaux
            restaurer_nc_depuis_originaux(ORIGINAL_DIR, ts_temp, test_ids)

            dt = time.time() - t1

            if nse_perm is None:
                print(f"   ⚠️  Évaluation échouée ({dt:.0f}s)")
                continue

            delta = baseline_nse - nse_perm
            pct = (delta / abs(baseline_nse)) * 100 if baseline_nse != 0 else 0
            print(f"   NSE = {nse_perm:.4f}  |  ΔNSE = {delta:+.4f}  |  "
                  f"{pct:+.1f}%  ({dt:.0f}s)")

            results.append({
                "variable": var,
                "nse_baseline": baseline_nse,
                "nse_permuted": nse_perm,
                "delta_nse": delta,
                "importance_pct": pct,
            })

    finally:
        # --- 6. Restauration GARANTIE ---
        print(f"\n6. Restauration...")
        shutil.copy2(config_backup, config_orig)
        config_backup.unlink()
        print(f"  ✅ config.yml restauré")

        shutil.rmtree(TEMP_DIR, ignore_errors=True)
        print(f"  ✅ Dossier temporaire supprimé")

    # --- 7. Résultats ---
    df_res = pd.DataFrame(results).sort_values("delta_nse", ascending=False)
    df_res.to_csv(OUT_CSV, index=False)

    print("\n" + "=" * 60)
    print("CLASSEMENT PAR IMPORTANCE (chute de NSE)")
    print("=" * 60)
    print(f"\n  {'Variable':<25} {'NSE perm':>10} {'ΔNSE':>9} {'%':>7}")
    print(f"  {'-'*55}")
    for _, row in df_res.iterrows():
        bar = "█" * max(1, int(abs(row["importance_pct"]) * 2))
        sign = "" if row["importance_pct"] >= 0 else "(neg!)"
        print(f"  {row['variable']:<25} {row['nse_permuted']:>10.4f} "
              f"{row['delta_nse']:>+9.4f} {row['importance_pct']:>+6.1f}% {bar} {sign}")

    print(f"\n✅ Résultats sauvegardés : {OUT_CSV}")
    print(f"""
📝 Interprétation :
   - ΔNSE grand positif → variable TRÈS UTILE (le modèle s'en sert)
   - ΔNSE proche de 0   → variable INUTILE (le modèle l'ignore)
   - ΔNSE négatif       → variable NUISIBLE (bruit)

📝 Groupes à comparer :
   - precipitation_J0 vs precip_mean_J3 vs precip_mean_J10
   - temperature_J0 vs temp_mean_J3 vs temp_mean_J10
   - snow_depth vs snowmelt → la neige aide-t-elle ?
""")