"""
apply_quantile_mapping_kfold.py  (VERSION SIMPLIFIÉE)
════════════════════════════════════════════════════════════════════════
Corrige la colonne "pred" via quantile mapping, avec validation croisée
à K blocs (K-fold) pour éliminer le biais de circularité : chaque ligne
est corrigée par une table construite SANS elle (ni son bloc).

Toutes les lignes du fichier d'entrée ont "obs" présente (fichier
résidus filtré standard) -> pas de distinction trous/non-trous ici, le
k-fold s'applique uniformément à toutes les lignes.

"obs" n'est jamais modifiée. Seule "pred" est corrigée, dans une
nouvelle colonne "pred_corrige".

Entrée : le fichier résidus centralisé habituel (obs toujours valide)
  ./data_processing/AnalyseModelsDtoD/residuals/residuals_{LABEL}_{source}_{freq}.csv

Sorties (dans OUTPUT_DIR) :
  {label}_{source}_{freq}_corrige.csv   (toutes les lignes + pred_corrige)
  kfold_resume_par_station.csv          (NSE/KGE avant/après par station,
                                          toutes les corrections étant
                                          hors-échantillon)
════════════════════════════════════════════════════════════════════════
"""

import numpy as np
import pandas as pd
from pathlib import Path

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
SOURCE = "dahiti"
FREQ   = "27j"
LABEL  = "DtoD80_NSE"

K_FOLDS = 5
MIN_PAIRS_PER_FOLD = 8
MIN_TOTAL_OBS = K_FOLDS * MIN_PAIRS_PER_FOLD   # station ignorée si moins d'obs que ça
RANDOM_SEED = 42

RESIDUALS_DIR = Path("./data_processing/AnalyseModelsDtoD/residuals")
INPUT_CSV = RESIDUALS_DIR / f"residuals_{LABEL}_{SOURCE}_{FREQ}.csv"

OUTPUT_DIR = Path(f"./data_processing/AnalyseModelsDtoD/quantile_mapping_{SOURCE}_{FREQ}")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ═══════════════════════════════════════════════════════════════
# QUANTILE MAPPING
# ═══════════════════════════════════════════════════════════════
def quantile_map(pred_calib, target_calib, pred_new):
    pred_calib = np.sort(np.asarray(pred_calib, dtype=float))
    target_calib = np.sort(np.asarray(target_calib, dtype=float))
    n_p, n_t = len(pred_calib), len(target_calib)
    if n_p < 5 or n_t < 5:
        return np.full(len(pred_new), np.nan)
    q_p = (np.arange(n_p) + 0.5) / n_p
    q_t = (np.arange(n_t) + 0.5) / n_t
    ranks = np.interp(pred_new, pred_calib, q_p, left=0.0, right=1.0)
    return np.interp(ranks, q_t, target_calib)


def compute_metrics(obs, pred):
    obs = np.asarray(obs, dtype=float)
    pred = np.asarray(pred, dtype=float)
    n = len(obs)
    if n < 5:
        return {"NSE": np.nan, "KGE": np.nan, "n": n}
    denom = np.sum((obs - obs.mean()) ** 2)
    nse = 1 - np.sum((obs - pred) ** 2) / denom if denom > 0 else np.nan
    if obs.std() > 0 and pred.std() > 0:
        r = np.corrcoef(obs, pred)[0, 1]
        alpha = pred.std() / obs.std()
        kge = 1 - np.sqrt((r - 1) ** 2 + (alpha - 1) ** 2)
    else:
        kge = np.nan
    return {"NSE": float(nse) if not np.isnan(nse) else np.nan,
            "KGE": float(kge) if not np.isnan(kge) else np.nan, "n": n}

# ═══════════════════════════════════════════════════════════════
# CHARGEMENT
# ═══════════════════════════════════════════════════════════════
if not INPUT_CSV.exists():
    raise SystemExit(f"Résidus introuvables : {INPUT_CSV}")

df = pd.read_csv(INPUT_CSV)
df["station"] = df["station"].astype(str)
df["date"] = pd.to_datetime(df["date"])
df = df.dropna(subset=["obs", "pred"]).sort_values(["station", "date"]).reset_index(drop=True)

if "label" in df.columns and not (df["label"] == LABEL).all():
    raise SystemExit(f"⚠⚠ INCOHÉRENCE : {INPUT_CSV} ne contient pas uniquement '{LABEL}' "
                      f"(trouvé : {df['label'].unique()})")

print(f"Résidus chargés pour '{LABEL}' : {len(df)} lignes, {df['station'].nunique()} stations")

rng = np.random.default_rng(RANDOM_SEED)
df["pred_corrige"] = np.nan
resume_rows = []

# ═══════════════════════════════════════════════════════════════
# BOUCLE PAR STATION — K-FOLD SUR TOUTES LES LIGNES
# ═══════════════════════════════════════════════════════════════
for station, idx_station in df.groupby("station").groups.items():
    idx_station = np.array(idx_station)
    obs_vals = df.loc[idx_station, "obs"].values
    pred_vals = df.loc[idx_station, "pred"].values
    n = len(idx_station)

    if n < MIN_TOTAL_OBS:
        continue

    folds = rng.integers(0, K_FOLDS, size=n)
    pred_corrected = np.full(n, np.nan)

    for k in range(K_FOLDS):
        train = folds != k
        test = folds == k
        if train.sum() < 5 or test.sum() == 0:
            continue
        pred_corrected[test] = quantile_map(
            pred_calib=pred_vals[train], target_calib=obs_vals[train],
            pred_new=pred_vals[test],
        )

    df.loc[idx_station, "pred_corrige"] = pred_corrected

    m_before = compute_metrics(obs_vals, pred_vals)
    m_after = compute_metrics(obs_vals, pred_corrected)
    resume_rows.append({
        "station": station, "n": n,
        "NSE_avant": m_before["NSE"], "NSE_apres": m_after["NSE"],
        "KGE_avant": m_before["KGE"], "KGE_apres": m_after["KGE"],
    })

# ═══════════════════════════════════════════════════════════════
# SORTIES
# ═══════════════════════════════════════════════════════════════
out_csv = OUTPUT_DIR / f"{LABEL}_{SOURCE}_{FREQ}_corrige.csv"
df.to_csv(out_csv, index=False)
n_corrige = df["pred_corrige"].notna().sum()
print(f"\nSérie corrigée -> {out_csv}")
print(f"  {n_corrige}/{len(df)} lignes corrigées "
      f"({df['station'].nunique() - len(resume_rows)} stations ignorées, "
      f"pas assez d'observations pour {K_FOLDS}-fold)")

df_resume = pd.DataFrame(resume_rows)
resume_csv = OUTPUT_DIR / "kfold_resume_par_station.csv"
df_resume.to_csv(resume_csv, index=False)

if not df_resume.empty:
    print(f"\n{'=' * 70}")
    print(f"  K-FOLD ({K_FOLDS} blocs) — {LABEL} [{SOURCE.upper()} {FREQ}]")
    print(f"  Corrections toutes hors-échantillon (jamais construites sur le point corrigé)")
    print(f"{'=' * 70}")
    for metric in ["NSE", "KGE"]:
        before = df_resume[f"{metric}_avant"].dropna()
        after = df_resume[f"{metric}_apres"].dropna()
        gain = (df_resume[f"{metric}_apres"] - df_resume[f"{metric}_avant"]).dropna()
        pct_better = (gain > 0).mean() * 100 if len(gain) else np.nan
        print(f"  {metric} : médiane avant={before.median():.3f}  après={after.median():.3f}  "
              f"gain médian={gain.median():.3f}  ({pct_better:.1f}% stations améliorées)")
    print(f"\nRésumé par station -> {resume_csv}")
else:
    print(f"\n⚠ Aucune station n'a atteint le minimum d'observations requis "
          f"({MIN_TOTAL_OBS}) pour un k-fold à {K_FOLDS} blocs.")