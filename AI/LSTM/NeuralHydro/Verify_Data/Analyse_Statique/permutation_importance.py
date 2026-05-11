"""
permutation_importance_attrs.py
═══════════════════════════════════════════════════════════════════════════
Permutation importance des attributs statiques sur un modèle NeuralHydrology
déjà entraîné.

⚠️ Le backup est placé EN DEHORS du dossier attributes/ car NeuralHydrology
   lit tous les CSV du dossier (sinon conflit de colonnes dupliquées).

Pour chaque attribut :
  1. Permute aléatoirement ses valeurs entre stations dans attributes.csv
  2. Lance nh-run evaluate
  3. Mesure la chute de NSE médian
  4. Restaure attributes.csv depuis le backup
═══════════════════════════════════════════════════════════════════════════
"""

import os
import shutil
import subprocess
import pandas as pd
import numpy as np
from pathlib import Path

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
RUN_DIR    = Path("./runs/arlstm_feat10j_modele2_2704_112827")
EPOCH      = 5
ATTRS_PATH = Path("./data/IA/NeuralHydrology_feat10j/attributes/attributes.csv")
# ⚠️ Backup HORS du dossier attributes/ pour éviter le conflit
ATTRS_BAK  = Path("./attributes_BACKUP.csv")
OUT_CSV    = Path("./permutation_importance_results.csv")

ATTRIBUTES_TO_TEST = [
    "aire_km2",
    "lon",
    "lat",
    "frac_urban",
    "frac_forest",
    "frac_agriculture",
    "sg_clay_0_30cm",
    "sg_sand_0_30cm",
    "sg_silt_0_30cm",
    "strahler",
    "elevation_mean",
    "slope_mean",
]

SEED = 42

# ═══════════════════════════════════════════════════════════════
# Fonctions utilitaires
# ═══════════════════════════════════════════════════════════════
def evaluer_modele():
    """Lance nh-run evaluate et retourne le NSE médian par station réelle."""
    cmd = ["nh-run", "evaluate",
           "--run-dir", str(RUN_DIR),
           "--epoch", str(EPOCH)]
    print(f"   → {' '.join(cmd)}")

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        # Afficher la vraie erreur (filtrer les warnings TF)
        stderr_lines = result.stderr.split('\n')
        # Garder les lignes pertinentes (Traceback, Error, etc.)
        important = [l for l in stderr_lines
                     if any(k in l for k in ['Error', 'error', 'Traceback', 'raise'])]
        if important:
            print(f"❌ Erreur évaluation :")
            for line in important[-10:]:
                print(f"     {line}")
        else:
            print(f"❌ Erreur (returncode={result.returncode}) :")
            print(result.stderr[-500:])
        return None

    # Localiser le CSV
    metrics_paths = [
        RUN_DIR / "test" / f"model_epoch{EPOCH:03d}" / "test_metrics.csv",
        RUN_DIR / "validation" / f"model_epoch{EPOCH:03d}" / "validation_metrics.csv",
    ]
    metrics_path = None
    for p in metrics_paths:
        if p.exists():
            metrics_path = p
            break

    if metrics_path is None:
        print(f"❌ Pas de fichier métriques trouvé. Cherché :")
        for p in metrics_paths:
            print(f"     {p}")
        return None

    df = pd.read_csv(metrics_path, header=None, names=["station_d", "NSE", "KGE"])
    df["NSE"] = pd.to_numeric(df["NSE"], errors="coerce")
    df = df.dropna(subset=["NSE"])

    df["station"] = df["station_d"].str.replace(r"_d\d+$", "", regex=True)
    nse_par_station = df.groupby("station")["NSE"].mean()

    return nse_par_station.median()


def permuter_attribut(attrs_df, col_name, seed):
    """Permute les valeurs entre stations RÉELLES."""
    df = attrs_df.copy()
    df["station_base"] = df["station_id"].str.replace(r"_d\d+$", "", regex=True)

    valeurs_uniques = df.drop_duplicates("station_base")[["station_base", col_name]]

    rng = np.random.default_rng(seed)
    valeurs_perm = rng.permutation(valeurs_uniques[col_name].values)
    mapping = dict(zip(valeurs_uniques["station_base"].values, valeurs_perm))

    df[col_name] = df["station_base"].map(mapping)
    df = df.drop(columns=["station_base"])
    return df


# ═══════════════════════════════════════════════════════════════
# 0. Nettoyage : supprimer un éventuel backup mal placé
# ═══════════════════════════════════════════════════════════════
old_bak = ATTRS_PATH.parent / "attributes_BACKUP.csv"
if old_bak.exists():
    print(f"⚠️  Suppression du backup mal placé : {old_bak}")
    old_bak.unlink()

# ═══════════════════════════════════════════════════════════════
# 1. Backup et baseline
# ═══════════════════════════════════════════════════════════════
print("="*60)
print("PERMUTATION IMPORTANCE")
print("="*60)

if not ATTRS_BAK.exists():
    shutil.copy(ATTRS_PATH, ATTRS_BAK)
    print(f"✅ Backup créé : {ATTRS_BAK}")
else:
    print(f"✅ Backup déjà présent : {ATTRS_BAK}")

print(f"\n📊 Évaluation baseline (sans permutation)...")
baseline_nse = evaluer_modele()
if baseline_nse is None:
    print("\n❌ Impossible d'obtenir le baseline.")
    print("   Restaure manuellement avec : cp attributes_BACKUP.csv "
          f"{ATTRS_PATH}")
    exit(1)
print(f"✅ NSE baseline médian : {baseline_nse:.4f}\n")

# ═══════════════════════════════════════════════════════════════
# 2. Test de chaque attribut
# ═══════════════════════════════════════════════════════════════
results = []
attrs_original = pd.read_csv(ATTRS_BAK)

for i, attr in enumerate(ATTRIBUTES_TO_TEST):
    print(f"\n[{i+1}/{len(ATTRIBUTES_TO_TEST)}] Permutation de '{attr}'...")

    if attr not in attrs_original.columns:
        print(f"   ⚠️  Colonne absente, on saute")
        continue

    attrs_perm = permuter_attribut(attrs_original, attr, seed=SEED)
    attrs_perm.to_csv(ATTRS_PATH, index=False)

    nse_perm = evaluer_modele()
    if nse_perm is None:
        print(f"   ⚠️  Évaluation échouée")
        continue

    delta = baseline_nse - nse_perm
    pct = (delta / baseline_nse) * 100 if baseline_nse > 0 else 0
    print(f"   NSE permuté = {nse_perm:.4f}  |  ΔNSE = {delta:+.4f}  |  {pct:+.1f}%")

    results.append({
        "attribut": attr,
        "nse_baseline": baseline_nse,
        "nse_permuted": nse_perm,
        "delta_nse": delta,
        "importance_pct": pct,
    })

# ═══════════════════════════════════════════════════════════════
# 3. Restauration et sauvegarde
# ═══════════════════════════════════════════════════════════════
shutil.copy(ATTRS_BAK, ATTRS_PATH)
print(f"\n✅ attributes.csv restauré depuis le backup")

df_res = pd.DataFrame(results).sort_values("delta_nse", ascending=False)
df_res.to_csv(OUT_CSV, index=False)

# ═══════════════════════════════════════════════════════════════
# 4. Affichage classement
# ═══════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("CLASSEMENT PAR IMPORTANCE (chute de NSE)")
print("="*60)
print(f"\n  {'Attribut':<22} {'NSE perm':>10} {'ΔNSE':>9} {'%':>7}")
print(f"  {'-'*52}")
for _, row in df_res.iterrows():
    bar = "█" * max(1, int(abs(row["importance_pct"]) * 2))
    sign = "" if row["importance_pct"] >= 0 else "(neg!)"
    print(f"  {row['attribut']:<22} {row['nse_permuted']:>10.4f} "
          f"{row['delta_nse']:>+9.4f} {row['importance_pct']:>+6.1f}% {bar} {sign}")

print(f"\n✅ Résultats sauvegardés : {OUT_CSV}")
print(f"""
📝 Interprétation :
   - ΔNSE grand positif → attribut TRÈS UTILE
   - ΔNSE proche de 0   → attribut INUTILE
   - ΔNSE négatif       → attribut NUISIBLE
""")