"""
extract_residuals.py
═══════════════════════════════════════════════════════════════════════════
Extrait les prédictions vs observations depuis test_results.p
et calcule les résidus normalisés pour la détection d'outliers.

Produit :
  - Un CSV par station : station_code, date, obs, pred, residual, residual_norm
  - Un CSV récapitulatif : toutes les stations concaténées
  - Un résumé des outliers détectés

Usage :
  python extract_residuals.py
═══════════════════════════════════════════════════════════════════════════
"""

import pickle
import pandas as pd
import numpy as np
from pathlib import Path

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
MODEL = "/arlstm_feat10jLow_modele2_0505_121508"
RUN_DIR   = Path(f"./runs{MODEL}")
EPOCH     = 2
PERIOD    = "test"
TARGET    = "water_level_obs"   # nom dans le xarray (souvent water_level_obs)
PRED_KEY  = "water_level_sim"   # nom de la prédiction (souvent water_level_sim)

# Seuil pour la détection d'outliers (en nombre d'écarts-types)
OUTLIER_THRESHOLD = 3.0

# Dossier de sortie
OUT_DIR = Path("./data/outlier_detection")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ═══════════════════════════════════════════════════════════════
# 1. Charger test_results.p
# ═══════════════════════════════════════════════════════════════
results_path = RUN_DIR / PERIOD / f"model_epoch{EPOCH:03d}" / f"{PERIOD}_results.p"

if not results_path.exists():
    print(f"❌ Fichier introuvable : {results_path}")
    print(f"   Vérifie le RUN_DIR, EPOCH et PERIOD.")
    print(f"   Contenu du dossier :")
    parent = results_path.parent
    if parent.exists():
        for f in sorted(parent.iterdir()):
            print(f"     {f.name}")
    exit(1)

print(f"📂 Chargement : {results_path}")
with open(results_path, "rb") as f:
    results = pickle.load(f)

# ═══════════════════════════════════════════════════════════════
# 2. Explorer la structure (au cas où les clés diffèrent)
# ═══════════════════════════════════════════════════════════════
print(f"\n📋 Structure du fichier results :")
print(f"   Type : {type(results)}")

if isinstance(results, dict):
    print(f"   Nombre de stations : {len(results)}")
    first_key = list(results.keys())[0]
    print(f"   Première clé : {first_key}")
    first_val = results[first_key]
    print(f"   Type valeur : {type(first_val)}")

    # Si c'est un xarray Dataset
    if hasattr(first_val, 'data_vars'):
        print(f"   Variables disponibles : {list(first_val.data_vars)}")
        print(f"   Coordonnées : {list(first_val.coords)}")
        print(f"   Dimensions : {dict(first_val.dims)}")
    # Si c'est un DataFrame
    elif hasattr(first_val, 'columns'):
        print(f"   Colonnes : {list(first_val.columns)}")
    # Si c'est un dict imbriqué
    elif isinstance(first_val, dict):
        print(f"   Sous-clés : {list(first_val.keys())}")
else:
    print(f"   ⚠️ Type inattendu, affichage brut des premières clés")
    print(f"   {str(results)[:500]}")
    exit(1)

# ═══════════════════════════════════════════════════════════════
# 3. Extraction obs/pred par station
# ═══════════════════════════════════════════════════════════════
all_rows = []

for station_id, data in results.items():
    # --- Déplier les dicts imbriqués ---
    # Structure NeuralHydrology : station → {'10D': {'xr': xarray, 'NSE': ..., 'KGE': ...}}
    if isinstance(data, dict):
        freq_key = list(data.keys())[0]  # '10D'
        data = data[freq_key]
    if isinstance(data, dict) and 'xr' in data:
        data = data['xr']

    # --- Adapter selon le type de data ---
    # Cas xarray Dataset (le plus courant dans NeuralHydrology)
    if hasattr(data, 'data_vars'):
        var_names = list(data.data_vars)
        # Trouver les noms obs et sim
        obs_name = None
        sim_name = None
        for v in var_names:
            if 'obs' in v.lower():
                obs_name = v
            elif 'sim' in v.lower():
                sim_name = v
        if obs_name is None or sim_name is None:
            # Fallback : prendre les 2 premières variables
            print(f"   ⚠️ {station_id}: noms auto-détectés = {var_names}")
            if len(var_names) >= 2:
                obs_name, sim_name = var_names[0], var_names[1]
            else:
                print(f"   ❌ Pas assez de variables pour {station_id}, skip")
                continue

        obs = data[obs_name].values.flatten()
        sim = data[sim_name].values.flatten()

        # Récupérer les dates
        if 'date' in data.coords:
            dates = pd.to_datetime(data.coords['date'].values)
        elif 'time' in data.coords:
            dates = pd.to_datetime(data.coords['time'].values)
        else:
            # Prendre la première dim temporelle
            time_dim = [d for d in data.dims if d not in ['time_step']]
            if time_dim:
                dates = pd.to_datetime(data.coords[time_dim[0]].values)
            else:
                dates = pd.RangeIndex(len(obs))

    # Cas DataFrame
    elif hasattr(data, 'columns'):
        cols = list(data.columns)
        obs_col = [c for c in cols if 'obs' in c.lower()]
        sim_col = [c for c in cols if 'sim' in c.lower() or 'pred' in c.lower()]
        obs = data[obs_col[0]].values if obs_col else data.iloc[:, 0].values
        sim = data[sim_col[0]].values if sim_col else data.iloc[:, 1].values
        dates = data.index

    else:
        print(f"   ❌ Type non géré pour {station_id}: {type(data)}")
        continue

    # --- Construire le DataFrame de la station ---
    df_sta = pd.DataFrame({
        'station': station_id,
        'date': dates[:len(obs)],
        'obs': obs,
        'pred': sim,
    })

    # Supprimer les lignes où obs est NaN (pas de mesure satellite)
    df_sta = df_sta.dropna(subset=['obs'])

    if len(df_sta) == 0:
        print(f"   ⚠️ {station_id}: aucune observation valide, skip")
        continue

    # Résidu
    df_sta['residual'] = df_sta['obs'] - df_sta['pred']

    # Résidu normalisé (par la std des résidus de la station)
    std_res = df_sta['residual'].std()
    if std_res > 0:
        df_sta['residual_norm'] = df_sta['residual'] / std_res
    else:
        df_sta['residual_norm'] = 0.0

    # Flag outlier
    df_sta['is_outlier'] = df_sta['residual_norm'].abs() > OUTLIER_THRESHOLD

    all_rows.append(df_sta)

# ═══════════════════════════════════════════════════════════════
# 4. Concaténer et sauvegarder
# ═══════════════════════════════════════════════════════════════
if not all_rows:
    print("\n❌ Aucune donnée extraite !")
    exit(1)

df_all = pd.concat(all_rows, ignore_index=True)

# CSV global
out_csv = OUT_DIR / "residuals_all_stations.csv"
df_all.to_csv(out_csv, index=False, float_format='%.4f')
print(f"\n✅ CSV global sauvegardé : {out_csv}")
print(f"   {len(df_all)} points, {df_all['station'].nunique()} stations")

# ═══════════════════════════════════════════════════════════════
# 5. Résumé par station
# ═══════════════════════════════════════════════════════════════
print(f"\n{'='*80}")
print(f"{'Station':>15s} | {'N obs':>6s} | {'Std res':>8s} | {'Outliers':>8s} | {'% out':>6s} | {'Max |res_n|':>10s}")
print(f"{'-'*80}")

summary_rows = []
for station_id, grp in df_all.groupby('station'):
    n_obs = len(grp)
    std_res = grp['residual'].std()
    n_outliers = grp['is_outlier'].sum()
    pct_outliers = 100 * n_outliers / n_obs if n_obs > 0 else 0
    max_res_norm = grp['residual_norm'].abs().max()

    print(f"{str(station_id):>15s} | {n_obs:6d} | {std_res:8.3f} | {n_outliers:8d} | {pct_outliers:5.1f}% | {max_res_norm:10.2f}")

    summary_rows.append({
        'station': station_id,
        'n_obs': n_obs,
        'std_residual': round(std_res, 4),
        'n_outliers': n_outliers,
        'pct_outliers': round(pct_outliers, 1),
        'max_residual_norm': round(max_res_norm, 2),
    })

df_summary = pd.DataFrame(summary_rows)
summary_csv = OUT_DIR / "summary_outliers.csv"
df_summary.to_csv(summary_csv, index=False)
print(f"\n✅ Résumé sauvegardé : {summary_csv}")

# ═══════════════════════════════════════════════════════════════
# 6. Stats globales
# ═══════════════════════════════════════════════════════════════
n_total = len(df_all)
n_outliers_total = df_all['is_outlier'].sum()
print(f"\n{'='*60}")
print(f"STATS GLOBALES (seuil = {OUTLIER_THRESHOLD}σ)")
print(f"{'='*60}")
print(f"  Points totaux     : {n_total}")
print(f"  Outliers détectés : {n_outliers_total} ({100*n_outliers_total/n_total:.1f}%)")
print(f"  Résidu moyen      : {df_all['residual'].mean():.4f}")
print(f"  Résidu std global : {df_all['residual'].std():.4f}")
print(f"\n💡 Pour visualiser : python plot_outliers.py")