"""
compare_models_vs_alti.py
════════════════════════════════════════════════════════════════════════
Fusionne en UN SEUL script ce qui était avant 2 scripts séparés
(analyse_residus_NSE_vs_RMSE_insitu_generic.py +
comparaison_gain_modele_vs_alti_generic.py), pour éliminer le problème
de cache intermédiaire périmé (metrics_per_station.csv pouvait rester
en mémoire d'un ancien calcul même après que les résidus source aient
changé). Ici, tout est TOUJOURS recalculé à partir des résidus actuels
sur disque -> jamais de résultat périmé silencieux.

Lit directement les fichiers centralisés produits par eval_zeroshot_DtoD.py :
  ./data_processing/AnalyseModelsDtoD/residuals/residuals_{label}_{source}_{freq}.csv

Chaque LABEL (clé de MODELS_TO_COMPARE ci-dessous) doit correspondre
exactement à un label déjà évalué par eval_zeroshot_DtoD.py pour ce
SOURCE/FREQ. Pas de matching approximatif par motif : le label est la
même chaîne partout dans la chaîne, donc pas d'ambiguïté possible.

Calcule, par station, pour chaque modèle sélectionné :
  1. Modèle  vs Alti    (obs/pred directement, KGE avec beta -> même référentiel)
  2. Modèle  vs Insitu  (insitu le plus proche en distance, <= DIST_MAX_KM,
                         KGE SANS beta -> évite l'explosion sur données z-scorées)
  3. Alti    vs Insitu  (baseline indépendante du modèle)

Sorties (dans OUTPUT_DIR, qui encode SOURCE/FREQ) :
  metrics_per_station.csv
  baseline_alti_vs_insitu.csv
  ranking_vs_insitu.csv
  gain_modele_vs_alti.csv
  resume_gain_modele_vs_alti.csv
  gain_modele_vs_alti.png
════════════════════════════════════════════════════════════════════════
"""

import sqlite3
import numpy as np
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib.cm as cm
from pathlib import Path
from shapely.geometry import Point

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES GLOBAUX — à modifier ici à chaque utilisation
# ═══════════════════════════════════════════════════════════════
SOURCE = "dahiti"   # "hwnext" ou "dahiti"
FREQ   = "10j"      # "10j" ou "27j"

# Labels à comparer : DOIVENT correspondre exactement aux clés utilisées
# dans MODELS de eval_zeroshot_DtoD.py pour ce SOURCE/FREQ.
MODELS_TO_COMPARE = [
    "DtoD80_NSE", "DtoD90_NSE", "DtoD96_NSE",
    "DtoD80_periodic", "DtoD90_periodic", "DtoD96_block",
]

RESIDUALS_DIR = Path("./data_processing/AnalyseModelsDtoD/residuals")

HW_DB      = "./data/hydroweb_next.db"
DAHITI_DB  = "./data/dahiti.db"
INSITU_DB  = "./data/insitu_data.db"
INSITU_SHP = "./data/insitu/shp/station_schapi_alti_ref_2025_river.gpkg"
SAT_DB     = HW_DB if SOURCE == "hwnext" else DAHITI_DB

DATE_MIN, DATE_MAX = "2016-01-01", "2025-12-31"
DIST_MAX_KM = 10.0
WINDOW_DAYS = 7 if FREQ == "10j" else 14
MIN_PAIRS   = 10

OUTPUT_DIR = Path(f"./data_processing/AnalyseModelsDtoD/comparaison_{SOURCE}_{FREQ}")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

METRIC_DIRECTION = {"NSE": True, "KGE": True, "RMSE": False, "R2": True}

# ═══════════════════════════════════════════════════════════════
# HELPERS — métriques
# ═══════════════════════════════════════════════════════════════
def compute_metrics(obs, pred, kge_with_bias):
    """kge_with_bias=False -> KGE sans terme beta (recommandé sur données
    z-scorées par station, où obs.mean() ~ 0 fait exploser le beta)."""
    n = len(obs)
    if n < MIN_PAIRS:
        return {"NSE": np.nan, "KGE": np.nan, "RMSE": np.nan, "R2": np.nan, "n": n}

    obs = np.asarray(obs, dtype=float)
    pred = np.asarray(pred, dtype=float)

    denom_nse = np.sum((obs - obs.mean()) ** 2)
    nse = 1 - np.sum((obs - pred) ** 2) / denom_nse if denom_nse > 0 else np.nan
    rmse = float(np.sqrt(np.mean((obs - pred) ** 2)))

    if obs.std() > 0 and pred.std() > 0:
        r = np.corrcoef(obs, pred)[0, 1]
        r2 = float(r ** 2)
    else:
        r, r2 = np.nan, np.nan

    if obs.std() > 0 and not np.isnan(r):
        alpha = pred.std() / obs.std()
        if kge_with_bias and obs.mean() != 0:
            beta = pred.mean() / obs.mean()
            kge = 1 - np.sqrt((r - 1) ** 2 + (alpha - 1) ** 2 + (beta - 1) ** 2)
        else:
            kge = 1 - np.sqrt((r - 1) ** 2 + (alpha - 1) ** 2)
    else:
        kge = np.nan

    return {"NSE": float(nse) if not np.isnan(nse) else np.nan,
            "KGE": float(kge) if not np.isnan(kge) else np.nan,
            "RMSE": rmse, "R2": r2, "n": n}


def zscore(arr):
    arr = np.asarray(arr, dtype=float)
    m = ~np.isnan(arr)
    if m.sum() < 2:
        return arr * np.nan
    mu, sig = arr[m].mean(), arr[m].std()
    return (arr - mu) / sig if sig > 0 else arr * 0

# ═══════════════════════════════════════════════════════════════
# HELPERS — insitu le plus proche
# ═══════════════════════════════════════════════════════════════
print("Chargement shapefile insitu...")
gdf_insitu = gpd.read_file(INSITU_SHP).to_crs("EPSG:2154")
_cache_ins, _cache_coords = {}, {}

def get_coords(code):
    if code in _cache_coords:
        return _cache_coords[code]
    conn = sqlite3.connect(SAT_DB)
    for c in [str(code), str(code).zfill(13)]:
        df = pd.read_sql(
            "SELECT reference_longitude AS lon, reference_latitude AS lat FROM stations WHERE station_code = ?",
            conn, params=(c,))
        if not df.empty:
            conn.close()
            _cache_coords[code] = (float(df.iloc[0]["lon"]), float(df.iloc[0]["lat"]))
            return _cache_coords[code]
    conn.close()
    _cache_coords[code] = (None, None)
    return None, None

def get_insitu_proche(lon, lat):
    pt = gpd.GeoSeries([Point(lon, lat)], crs="EPSG:4326").to_crs("EPSG:2154")[0]
    dist = gdf_insitu.geometry.distance(pt)
    idx = dist.idxmin()
    return gdf_insitu.loc[idx, "code_sta"], dist[idx] / 1000

def get_insitu_series(code_sta):
    if code_sta not in _cache_ins:
        conn = sqlite3.connect(INSITU_DB)
        df = pd.read_sql("""
            SELECT date, h_med_wsh AS wl FROM mesures_insitu
            WHERE code_sta = ? AND date >= ? AND date <= ? ORDER BY date
        """, conn, params=(code_sta, DATE_MIN, DATE_MAX))
        conn.close()
        df["date"] = pd.to_datetime(df["date"])
        df = df.dropna(subset=["wl"])
        _cache_ins[code_sta] = df if len(df) >= 5 else None
    return _cache_ins[code_sta]

def align_insitu(dates, df_ins, window_days):
    wl = np.full(len(dates), np.nan)
    id_ = np.array(df_ins["date"].values, dtype="datetime64[D]")
    iv = df_ins["wl"].values
    for i, d in enumerate(np.array(dates, dtype="datetime64[D]")):
        diff = np.abs((id_ - d).astype(float))
        idx = int(np.argmin(diff))
        if diff[idx] <= window_days:
            wl[i] = iv[idx]
    return wl

# ═══════════════════════════════════════════════════════════════
# CHARGEMENT DES RÉSIDUS PAR LABEL (toujours frais, jamais de cache)
# ═══════════════════════════════════════════════════════════════
all_rows, baseline_rows, missing = [], [], []

for label in MODELS_TO_COMPARE:
    csv_path = RESIDUALS_DIR / f"residuals_{label}_{SOURCE}_{FREQ}.csv"
    if not csv_path.exists():
        print(f"⚠ {label} : fichier introuvable ({csv_path}) -> ignoré. "
              f"Lancer eval_zeroshot_DtoD.py avec ce label pour ce SOURCE/FREQ.")
        missing.append(label)
        continue

    df = pd.read_csv(csv_path)
    df["station"] = df["station"].astype(str)
    df = df.dropna(subset=["obs", "pred"])
    df["date"] = pd.to_datetime(df["date"])

    # Sécurité : vérifie que le fichier appartient bien au label attendu
    # (protège contre une future collision similaire à celle déjà vécue)
    if "label" in df.columns and not (df["label"] == label).all():
        found = df["label"].unique()
        print(f"⚠⚠ INCOHÉRENCE : {csv_path} est censé être '{label}' mais "
              f"contient le(s) label(s) {found} -> ignoré par sécurité.")
        continue

    for station, sub in df.groupby("station"):
        sub = sub.sort_values("date")

        m_alti = compute_metrics(sub["obs"].values, sub["pred"].values, kge_with_bias=True)
        row = {"model": label, "station": station,
               "NSE_modele_alti": m_alti["NSE"], "KGE_modele_alti": m_alti["KGE"],
               "RMSE_modele_alti": m_alti["RMSE"], "R2_modele_alti": m_alti["R2"],
               "n_alti": m_alti["n"]}

        lon, lat = get_coords(station)
        if lon is not None:
            code_ins, dist_km = get_insitu_proche(lon, lat)
            row["insitu_code"] = code_ins
            row["dist_insitu_km"] = round(dist_km, 1)

            if dist_km <= DIST_MAX_KM:
                df_ins = get_insitu_series(code_ins)
                if df_ins is not None:
                    ins_wl = align_insitu(sub["date"].values, df_ins, WINDOW_DAYS)
                    n_pairs = int(np.sum(~np.isnan(ins_wl)))

                    if n_pairs >= MIN_PAIRS:
                        obs_z, pred_z, ins_z = (zscore(sub["obs"].values),
                                                 zscore(sub["pred"].values), zscore(ins_wl))

                        mask_c = ~(np.isnan(pred_z) | np.isnan(ins_z))
                        m_mod_ins = compute_metrics(ins_z[mask_c], pred_z[mask_c], kge_with_bias=False)
                        row.update({"NSE_modele_insitu": m_mod_ins["NSE"],
                                    "KGE_modele_insitu": m_mod_ins["KGE"],
                                    "RMSE_modele_insitu": m_mod_ins["RMSE"],
                                    "R2_modele_insitu": m_mod_ins["R2"],
                                    "n_insitu": n_pairs})

                        mask_c2 = ~(np.isnan(obs_z) | np.isnan(ins_z))
                        m_alti_ins = compute_metrics(ins_z[mask_c2], obs_z[mask_c2], kge_with_bias=False)
                        baseline_rows.append({"station": station, "insitu_code": code_ins,
                                               "dist_insitu_km": round(dist_km, 1),
                                               "n_pairs": int(mask_c2.sum()),
                                               "NSE_alti_insitu": m_alti_ins["NSE"],
                                               "KGE_alti_insitu": m_alti_ins["KGE"],
                                               "RMSE_alti_insitu": m_alti_ins["RMSE"],
                                               "R2_alti_insitu": m_alti_ins["R2"]})
        all_rows.append(row)

if not all_rows:
    raise SystemExit("Aucune métrique calculée — vérifier MODELS_TO_COMPARE et "
                      "que eval_zeroshot_DtoD.py a bien tourné pour ces labels.")

df_metrics = pd.DataFrame(all_rows)
df_metrics.to_csv(OUTPUT_DIR / "metrics_per_station.csv", index=False)
print(f"\nMétriques par station -> {OUTPUT_DIR / 'metrics_per_station.csv'} ({len(df_metrics)} lignes)")

df_baseline = pd.DataFrame(baseline_rows).drop_duplicates(subset=["station"])
df_baseline.to_csv(OUTPUT_DIR / "baseline_alti_vs_insitu.csv", index=False)
print(f"Baseline alti vs insitu -> {OUTPUT_DIR / 'baseline_alti_vs_insitu.csv'} ({len(df_baseline)} stations)")
if len(df_baseline):
    print(f"  NSE médian alti vs insitu : {df_baseline['NSE_alti_insitu'].median():.3f}")
if missing:
    print(f"\n⚠ Labels manquants (ignorés) : {missing}")

# ═══════════════════════════════════════════════════════════════
# CLASSEMENT (rang moyen sur les 4 métriques, stations communes)
# ═══════════════════════════════════════════════════════════════
def build_ranking(df_metrics, suffix, output_name):
    cols = {m: f"{m}{suffix}" for m in METRIC_DIRECTION}
    if not all(c in df_metrics.columns for c in cols.values()):
        print(f"\n⚠ Colonnes manquantes pour '{output_name}' -> ignoré")
        return None

    df_sub = df_metrics.dropna(subset=list(cols.values()), how="all")
    stations_par_modele = df_sub.groupby("model")["station"].apply(set)
    if len(stations_par_modele) < 2:
        print(f"\n⚠ Pas assez de modèles pour '{output_name}'")
        return None
    stations_communes = set.intersection(*stations_par_modele.tolist())
    df_common = df_sub[df_sub["station"].isin(stations_communes)]

    rank_rows = []
    for station in stations_communes:
        sub = df_common[df_common["station"] == station]
        for metric, higher_is_better in METRIC_DIRECTION.items():
            vals = sub.set_index("model")[cols[metric]].dropna()
            if len(vals) < 2:
                continue
            ranks = vals.rank(ascending=not higher_is_better, method="average")
            for model, rank in ranks.items():
                rank_rows.append({"station": station, "metric": metric, "model": model, "rank": rank})

    df_ranks = pd.DataFrame(rank_rows)
    if df_ranks.empty:
        print(f"\n⚠ Pas assez de données pour '{output_name}'")
        return None

    classement = (df_ranks.groupby("model")["rank"].mean().sort_values()
                  .reset_index().rename(columns={"rank": "rang_moyen"}))
    classement["position"] = range(1, len(classement) + 1)
    classement.to_csv(OUTPUT_DIR / output_name, index=False)

    print(f"\n{'=' * 80}")
    print(f"  CLASSEMENT — {output_name}  [{SOURCE.upper()} {FREQ}]  "
          f"({len(stations_communes)} stations communes)")
    print(f"{'=' * 80}")
    for _, row in classement.iterrows():
        print(f"  #{int(row['position'])}  {row['model']:<20} rang moyen = {row['rang_moyen']:.3f}")
    print(f"  -> {OUTPUT_DIR / output_name}")
    return classement

build_ranking(df_metrics, "_modele_insitu", "ranking_vs_insitu.csv")

# ═══════════════════════════════════════════════════════════════
# GAIN MODÈLE VS ALTI (par rapport à la baseline alti vs insitu)
# ═══════════════════════════════════════════════════════════════
df_merged = df_metrics.merge(
    df_baseline[["station", "NSE_alti_insitu", "KGE_alti_insitu", "RMSE_alti_insitu", "R2_alti_insitu"]],
    on="station", how="inner")
print(f"\nStations avec baseline alti-insitu disponible : {df_merged['station'].nunique()}")

for metric, higher_is_better in METRIC_DIRECTION.items():
    col_mod, col_alti = f"{metric}_modele_insitu", f"{metric}_alti_insitu"
    if higher_is_better:
        df_merged[f"gain_{metric}"] = df_merged[col_mod] - df_merged[col_alti]
    else:
        df_merged[f"gain_{metric}"] = df_merged[col_alti] - df_merged[col_mod]

df_merged.to_csv(OUTPUT_DIR / "gain_modele_vs_alti.csv", index=False)
print(f"Détail station par station -> {OUTPUT_DIR / 'gain_modele_vs_alti.csv'}")

print(f"\n{'=' * 100}")
print(f"  RÉSUMÉ [{SOURCE.upper()} {FREQ}] — Modèle vs Insitu  contre  Alti vs Insitu  (baseline)")
print(f"{'=' * 100}")

resume_rows = []
for model in MODELS_TO_COMPARE:
    sub = df_merged[df_merged["model"] == model]
    if sub.empty:
        continue
    print(f"\n--- {model} (n={len(sub)} stations) ---")
    print(f"  {'métrique':<8} {'médiane modèle':>15} {'médiane alti':>14} {'gain médian':>12} {'% modèle > alti':>16}")
    row = {"model": model, "n_stations": len(sub)}
    for metric, higher_is_better in METRIC_DIRECTION.items():
        col_mod = sub[f"{metric}_modele_insitu"].dropna()
        col_alti = sub[f"{metric}_alti_insitu"].dropna()
        gain = sub[f"gain_{metric}"].dropna()
        pct_better = (gain > 0).mean() * 100 if len(gain) else np.nan
        print(f"  {metric:<8} {col_mod.median():>15.3f} {col_alti.median():>14.3f} "
              f"{gain.median():>12.3f} {pct_better:>15.1f}%")
        row[f"{metric}_gain_med"] = round(gain.median(), 3) if len(gain) else np.nan
        row[f"{metric}_pct_meilleur"] = round(pct_better, 1) if not np.isnan(pct_better) else np.nan
    resume_rows.append(row)

pd.DataFrame(resume_rows).to_csv(OUTPUT_DIR / "resume_gain_modele_vs_alti.csv", index=False)
print(f"\nRésumé -> {OUTPUT_DIR / 'resume_gain_modele_vs_alti.csv'}")

# ═══════════════════════════════════════════════════════════════
# FIGURE
# ═══════════════════════════════════════════════════════════════
palette = cm.get_cmap("tab10", max(len(MODELS_TO_COMPARE), 3))
COLORS = {m: palette(i) for i, m in enumerate(MODELS_TO_COMPARE)}
COLORS["Alti (baseline)"] = "#9E9E9E"

fig, axes = plt.subplots(2, 2, figsize=(13, 9))
fig.suptitle(f"[{SOURCE.upper()} {FREQ}] Modèle vs Insitu comparé à Alti vs Insitu (baseline)",
             fontsize=12, fontweight="bold")
rng = np.random.default_rng(42)

for ax, metric in zip(axes.flat, METRIC_DIRECTION.keys()):
    data, colors, labels = [df_merged.drop_duplicates("station")[f"{metric}_alti_insitu"].dropna().values], \
                            [COLORS["Alti (baseline)"]], ["Alti\nvs insitu"]
    for model in MODELS_TO_COMPARE:
        vals = df_merged[df_merged["model"] == model][f"{metric}_modele_insitu"].dropna().values
        data.append(vals)
        colors.append(COLORS[model])
        labels.append(f"{model}\nvs insitu")

    bp = ax.boxplot(data, tick_labels=labels, patch_artist=True,
                     medianprops={"color": "black", "linewidth": 2}, widths=0.5)
    for box, color in zip(bp["boxes"], colors):
        box.set_facecolor(color)
        box.set_alpha(0.65)
    for j, (vals, color) in enumerate(zip(data, colors), 1):
        jitter = rng.uniform(-0.12, 0.12, len(vals))
        ax.scatter(np.full(len(vals), j) + jitter, vals, alpha=0.3, s=10, color=color, zorder=3)
        if len(vals):
            ax.text(j, np.nanmedian(vals), f"{np.nanmedian(vals):.2f}", ha="center",
                    fontsize=8, fontweight="bold")
    if metric != "RMSE":
        ax.axhline(0, color="red", lw=1, ls="--", alpha=0.5)
    ax.set_title(metric, fontsize=11, fontweight="bold")
    ax.grid(True, alpha=0.25, axis="y")
    ax.tick_params(axis="x", labelsize=7)

plt.tight_layout()
fig.savefig(OUTPUT_DIR / "gain_modele_vs_alti.png", dpi=150, bbox_inches="tight")
plt.close(fig)
print(f"\n✅ Figure -> {OUTPUT_DIR / 'gain_modele_vs_alti.png'}")