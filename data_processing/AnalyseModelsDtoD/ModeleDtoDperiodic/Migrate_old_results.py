"""
migrate_old_residuals.py
════════════════════════════════════════════════════════════════════════
Copie des fichiers résidus déjà produits par l'ANCIEN pipeline
(eval_zeroshot_generic_DtoD.py) vers la nouvelle convention de nommage
par label utilisée par eval_zeroshot_DtoD.py / compare_models_vs_alti.py.

Utile pour éviter de relancer une évaluation zero-shot (~40 min/modèle)
pour des modèles dont le fichier résidus n'a PAS été écrasé entre temps.

⚠ Vérifie TOUJOURS le contenu du fichier source avant de le migrer (cf.
commande de vérification ci-dessous) -> ne migre QUE des fichiers dont
tu es sûr qu'ils correspondent encore au bon modèle.

Chaque entrée de MIGRATIONS spécifie maintenant sa PROPRE source
(dahiti/hwnext) et fréquence (10j/27j), donc une seule liste peut
mélanger toutes les combinaisons source x fréquence x modèle en une
seule exécution -- plus besoin de relancer le script par combinaison.

Usage :
    python migrate_old_residuals.py
    (ajuster MIGRATIONS ci-dessous avant de lancer)
════════════════════════════════════════════════════════════════════════
"""

import pandas as pd
from pathlib import Path

RESIDUALS_DIR = Path("./data_processing/AnalyseModelsDtoD/residuals")
RESIDUALS_DIR.mkdir(parents=True, exist_ok=True)

# Chaque entrée : (source, freq, label_cible, chemin_ancien_fichier, run_name_attendu)
#   source : "dahiti" ou "hwnext"
#   freq   : "10j" ou "27j"
# run_name_attendu sert de vérification : si le fichier source ne contient
# pas ce run_name dans sa colonne "model", la migration est refusée.
MIGRATIONS = [
    ("dahiti", "10j", "DtoD80_NSE",
     Path("./data/outlier_detection/benchmark_DtoD_dahiti10j/residuals_dahiti_10j_80pct.csv"),
     "arlstm_DtoD80_periodic_0607_151150"),
    ("dahiti", "10j", "DtoD90_NSE",
     Path("./data/outlier_detection/benchmark_DtoD_dahiti10j/residuals_dahiti_10j_90pct.csv"),
     "arlstm_DtoD90_1606_111709"),
    ("dahiti", "10j", "DtoD96_NSE",
     Path("./data/outlier_detection/benchmark_DtoD_dahiti10j/residuals_dahiti_10j_96pct.csv"),
     "arlstm_DtoD96_1606_164901"),

    ("dahiti", "27j", "DtoD80_NSE",
     Path("./data/outlier_detection/benchmark_DtoD_dahiti27j/residuals_dahiti_27j_80pct.csv"),
     "arlstm_DtoD80_periodic_0607_151150"),
    ("dahiti", "27j", "DtoD90_NSE",
     Path("./data/outlier_detection/benchmark_DtoD_dahiti27j/residuals_dahiti_27j_90pct.csv"),
     "arlstm_DtoD90_1606_111709"),
    ("dahiti", "27j", "DtoD96_NSE",
     Path("./data/outlier_detection/benchmark_DtoD_dahiti27j/residuals_dahiti_27j_96pct.csv"),
     "arlstm_DtoD96_1606_164901"),

    ("hwnext", "10j", "DtoD80_NSE",
     Path("./data/outlier_detection/benchmark_DtoD_hwnext10j/residuals_hwnext_10j_80pct.csv"),
     "arlstm_DtoD80_1506_150002"),
    ("hwnext", "10j", "DtoD90_NSE",
     Path("./data/outlier_detection/benchmark_DtoD_hwnext10j/residuals_hwnext_10j_90pct.csv"),
     "arlstm_DtoD90_1606_111709"),
    ("hwnext", "10j", "DtoD96_NSE",
     Path("./data/outlier_detection/benchmark_DtoD_hwnext10j/residuals_hwnext_10j_96pct.csv"),
     "arlstm_DtoD96_1606_164901"),
    # Ajouter ici hwnext 27j si/quand les résidus complets existent pour
    # cette combinaison (rappel : HWNext n'a historiquement que les résidus
    # filtrés, pas les résidus complets, cf. résumé de session).
]

print(f"{'=' * 70}")
print("  Migration résidus -> nouvelle convention  [toutes sources/fréquences]")
print(f"{'=' * 70}\n")

n_ok, n_skip, n_refuse = 0, 0, 0

for source, freq, label, old_path, expected_run_name in MIGRATIONS:
    tag = f"[{source.upper()} {freq}] {label}"
    new_path = RESIDUALS_DIR / f"residuals_{label}_{source}_{freq}.csv"

    if not old_path.exists():
        print(f"⚠ {tag} : fichier source introuvable ({old_path}) -> skip")
        n_skip += 1
        continue

    df = pd.read_csv(old_path)
    if "model" in df.columns:
        found_models = df["model"].unique()
        if len(found_models) != 1 or found_models[0] != expected_run_name:
            print(f"⚠⚠ {tag} : contenu inattendu dans {old_path}")
            print(f"    attendu : '{expected_run_name}'")
            print(f"    trouvé  : {found_models}")
            print(f"    -> migration REFUSÉE, ce fichier a probablement été écrasé "
                  f"par un autre run. Relancer eval_zeroshot_DtoD.py pour ce label.")
            n_refuse += 1
            continue
    else:
        # Fichier de format "mono-run" (un seul modèle par construction,
        # ex: sortie brute d'un ancien script d'évaluation) -> pas de colonne
        # model, donc rien à vérifier contre expected_run_name. On migre en
        # le signalant clairement, sans la garantie de correspondance exacte
        # qu'apporte la vérification par nom de run.
        print(f"ℹ {tag} : pas de colonne 'model' -> fichier mono-run, "
              f"migration SANS vérification du run_name (à confirmer manuellement)")

    # Garde seulement les colonnes nécessaires en aval, + label/source/freq propres
    keep_cols = [c for c in ["station", "date", "obs", "pred"] if c in df.columns]
    df_out = df[keep_cols].copy()
    df_out["label"] = label
    df_out["source"] = source
    df_out["freq"] = freq
    df_out.to_csv(new_path, index=False)

    print(f"✅ {tag} : {old_path.name}  ({len(df_out)} lignes, "
          f"{df_out['station'].nunique()} stations)  -> {new_path}")
    n_ok += 1

print(f"\n{'=' * 70}")
print(f"Terminé. {n_ok} migré(s), {n_skip} introuvable(s), {n_refuse} refusé(s).")
print("Relancer compare_models_vs_alti.py -- les labels migrés")
print("seront lus directement, sans ré-évaluation.")
print(f"{'=' * 70}")