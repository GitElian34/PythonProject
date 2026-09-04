"""
plot_stations_outliers_consensus.py
════════════════════════════════════════════════════════════════════════
Contrairement à plot_stations_outliers.py (1 seul modèle, top N par NSE),
ce script :

  - Regarde les 3 modèles DtoD80/90/96 EN MÊME TEMPS, sur TOUTES les
    stations disponibles (pas seulement les meilleures par NSE).
  - Un outlier est "confirmé" quand DtoD80 ET DtoD96 le détectent TOUS
    LES DEUX INDÉPENDAMMENT à la même date (même critère que
    plot_stations_outliers.py : résidu obs-pred, sigma par station ET
    PAR MODÈLE, seuil OUTLIER_SIGMA). DtoD90 est calculé et affiché dans
    l'annotation à titre informatif, mais n'entre pas dans la décision
    de confirmation.
  - Sélectionne 10 stations AU HASARD (seed fixe pour reproductibilité)
    parmi celles qui ont au moins un outlier confirmé -- par fréquence
    (10 pour 10j, 10 pour 27j), indépendamment du NSE.
  - Ne trace que les années où cette station a au moins un outlier confirmé.
  - La courbe DtoD96 est TOUJOURS celle affichée sur les plots (plus de
    logique de sélection dynamique du modèle affiché).

Les outliers sont détectés en interne sur les 3 modèles (consensus), mais
UN SEUL modèle est affiché par graphique -- celui qui vote outlier le
plus souvent cette année-là parmi les modèles en accord (pas les 3
courbes en même temps, trop chargé visuellement). L'annotation garde
le compte "n/3 modèles" pour indiquer la force du consensus même si
une seule courbe est tracée.

Obs + le modèle affiché sont z-scorés sur une échelle PARTAGÉE (mu/sigma
commun, calculé sur les valeurs aux dates d'observation) -- indispensable
pour que l'écart visuel reste proportionnel au résidu réel (cf.
discussion précédente sur le paradoxe visuel). Seul l'insitu garde son
propre z-score (référentiel différent).

Supporte HW Next et DAHITI via SOURCE.

Sources (par modèle, pour chaque freq) :
  Models_Testing/DtoD/residus/residuals_DtoD{80,90,96}_{SOURCE}_{freq}.csv

Sorties :
  Models_Testing/DtoD/plots/outliers_consensus/{SOURCE}/{freq}/{station}/{year}.png
════════════════════════════════════════════════════════════════════════
"""

import random
import sqlite3
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

SOURCE = "hwnext"   # <-- "hwnext" ou "dahiti"
MODELS = ["DtoD80", "DtoD90", "DtoD96"]
FREQS = ["10j", "27j"]
OUTLIER_SIGMA = 2.5
REQUIRED_AGREE_MODELS = ["DtoD80", "DtoD96"]  # outlier confirmé ssi CES modèles sont TOUS deux outlier (DtoD90 ignoré pour la confirmation, juste informatif)
DISPLAY_MODEL = "DtoD96"   # courbe toujours affichée sur les plots
N_RANDOM_STATIONS = 10     # par fréquence
RANDOM_SEED = 42           # fixe -> reproductible ; changer pour un autre tirage

WINDOW_DAYS = {"10j": 5, "27j": 14}  # tolérance de recalage insitu (affichage seulement)

RESIDUS_DIR = Path("./Models_Testing/DtoD/residus")
PLOT_DIR = Path("./Models_Testing/DtoD/plots/outliers_consensus")
INSITU_DB = "./data/insitu_data.db"
DATE_MIN, DATE_MAX = "2016-01-01", "2025-12-31"

C_OBS = "#1B4F72"
C_INS = "#229954"
C_MODEL = {"DtoD80": "#E74C3C", "DtoD90": "#C0392B", "DtoD96": "#78281F"}  # clair -> foncé
C_OUTLIER = "#8E44AD"  # violet, distinct de la famille rouge des modèles
C_TEXT = "#2C3E50"

plt.rcParams.update({
    "font.family": "DejaVu Sans",
    "axes.edgecolor": "#BFC9CA",
    "axes.labelcolor": C_TEXT,
    "text.color": C_TEXT,
    "xtick.color": C_TEXT,
    "ytick.color": C_TEXT,
})


# ═══════════════════════════════════════════════════════════════
# INSITU : série brute quotidienne
# ═══════════════════════════════════════════════════════════════
_cache_ins = {}


def get_insitu_series(code_sta):
    if code_sta not in _cache_ins:
        conn = sqlite3.connect(INSITU_DB)
        df = pd.read_sql("""
            SELECT date, h_med_wsh AS wl FROM mesures_insitu
            WHERE code_sta = ? AND date >= ? AND date <= ? ORDER BY date
        """, conn, params=(code_sta, DATE_MIN, DATE_MAX))
        conn.close()
        df["date"] = pd.to_datetime(df["date"])
        _cache_ins[code_sta] = df.dropna(subset=["wl"])
    return _cache_ins[code_sta]


def zscore_params(values):
    v = np.asarray(values, dtype=float)
    v = v[~np.isnan(v)]
    if len(v) < 2:
        return 0.0, 1.0
    mu, sigma = v.mean(), v.std()
    return mu, (sigma if sigma > 0 else 1.0)


def align_insitu_to_dates(dates, df_ins, window_days):
    out = np.full(len(dates), np.nan)
    if df_ins.empty:
        return out
    id_ = np.array(df_ins["date"].values, dtype="datetime64[D]")
    iv = df_ins["wl"].values
    for i, d in enumerate(np.array(dates, dtype="datetime64[D]")):
        diff = np.abs((id_ - d).astype(float))
        idx = int(np.argmin(diff))
        if diff[idx] <= window_days:
            out[i] = iv[idx]
    return out


# ═══════════════════════════════════════════════════════════════
# DÉTECTION D'OUTLIERS PAR MODÈLE (même critère que plot_stations_outliers.py)
# ═══════════════════════════════════════════════════════════════
def flag_outliers_one_station(grp: pd.DataFrame) -> pd.DataFrame:
    """grp : lignes obs+pred non-NaN d'UNE station, UN modèle."""
    df = grp.sort_values("date").copy()
    residual = df["obs"] - df["pred"]
    sigma = float(residual.std()) if len(residual) >= 5 else float("nan")
    if not sigma or np.isnan(sigma) or sigma == 0:
        df["residual_norm"], df["is_outlier"] = np.nan, False
    else:
        df["residual_norm"] = residual / sigma
        df["is_outlier"] = df["residual_norm"].abs() >= OUTLIER_SIGMA
    return df


def load_and_flag(model_label: str, freq: str) -> pd.DataFrame:
    path = RESIDUS_DIR / f"residuals_{model_label}_{SOURCE}_{freq}.csv"
    if not path.exists():
        print(f"⚠ Fichier introuvable : {path} -> {model_label} ignoré pour {freq}")
        return pd.DataFrame()
    df = pd.read_csv(path)
    df["station"] = df["station"].astype(str)
    df["date"] = pd.to_datetime(df["date"])
    df_obs = df.dropna(subset=["obs", "pred"])
    if df_obs.empty:
        return pd.DataFrame()
    results = []
    for station, grp in df_obs.groupby("station"):
        flagged = flag_outliers_one_station(grp)
        flagged["station"] = station
        results.append(flagged)
    flagged_all = pd.concat(results, ignore_index=True) if results else pd.DataFrame()
    if not flagged_all.empty:
        flagged_all["model"] = model_label
    return flagged_all


# ═══════════════════════════════════════════════════════════════
# CONSENSUS : au moins MIN_MODELS_AGREE modèles flaguent la même (station, date)
# ═══════════════════════════════════════════════════════════════
def build_consensus(freq: str) -> tuple[pd.DataFrame, dict]:
    """Retourne (table consensus station/date/n_agree, dict {model: df daily complet})."""
    flagged_by_model = {m: load_and_flag(m, freq) for m in MODELS}
    daily_by_model = {}
    for m in MODELS:
        p = RESIDUS_DIR / f"residuals_{m}_{SOURCE}_{freq}.csv"
        if p.exists():
            d = pd.read_csv(p)
            d["station"] = d["station"].astype(str)
            d["date"] = pd.to_datetime(d["date"])
            daily_by_model[m] = d

    valid_models = [m for m in MODELS if not flagged_by_model[m].empty]
    if not all(m in valid_models for m in REQUIRED_AGREE_MODELS):
        print(f"  ⚠ Un des modèles requis ({REQUIRED_AGREE_MODELS}) est indisponible pour {freq} "
              f"-> consensus impossible")
        return pd.DataFrame(), daily_by_model

    # Table station/date/obs (commune) + is_outlier_{model}
    base = None
    for m in valid_models:
        sub = flagged_by_model[m][["station", "date", "obs", "is_outlier"]].rename(
            columns={"is_outlier": f"is_outlier_{m}"})
        base = sub if base is None else base.merge(sub, on=["station", "date", "obs"], how="inner")

    if base is None or base.empty:
        return pd.DataFrame(), daily_by_model

    flag_cols = [f"is_outlier_{m}" for m in valid_models]
    base["n_agree"] = base[flag_cols].sum(axis=1)
    base["n_models_checked"] = len(valid_models)
    return base, daily_by_model


# ═══════════════════════════════════════════════════════════════
# PLOT PAR STATION / ANNÉE
# ═══════════════════════════════════════════════════════════════
def plot_station_year(freq, station, chosen_model, model_daily, obs_ref, confirmed_station,
                       df_ins, year, out_path):
    year_dates_confirmed = confirmed_station[confirmed_station["date"].dt.year == year]
    if year_dates_confirmed.empty:
        return False

    ins_year = df_ins[df_ins["date"].dt.year == year] if len(df_ins) else pd.DataFrame()
    obs_year = obs_ref[obs_ref["date"].dt.year == year]
    pred_year = model_daily[model_daily["date"].dt.year == year].dropna(subset=["pred_z"])

    fig, ax = plt.subplots(figsize=(11, 4.6))

    if len(ins_year):
        ax.plot(ins_year["date"], ins_year["wl_z"], "-", color=C_INS,
                linewidth=1.1, alpha=0.5, label="Insitu (quotidien, z-score)", zorder=1)

    if len(pred_year):
        ax.plot(pred_year["date"], pred_year["pred_z"], "-", color=C_MODEL[chosen_model],
                linewidth=1.4, alpha=0.9, label=f"{chosen_model} (quotidien, z-score)", zorder=2)

    ax.plot(obs_year["date"], obs_year["obs_z"], "o", color=C_OBS, markersize=6,
            markeredgecolor="white", markeredgewidth=0.6,
            label="Altimétrie (obs, z-score)", zorder=3)

    pred_lookup = model_daily.set_index("date")["pred_z"]
    for _, orow in year_dates_confirmed.iterrows():
        d = orow["date"]
        obs_row = obs_year[obs_year["date"] == d]
        if obs_row.empty:
            continue
        obs_zv = obs_row["obs_z"].iloc[0]
        if d in pred_lookup.index:
            ax.plot([d, d], [obs_zv, pred_lookup[d]], ":", color=C_OUTLIER,
                    linewidth=1.4, alpha=0.85, zorder=3.5)
        ax.scatter([d], [obs_zv], s=190, facecolors="none", edgecolors=C_OUTLIER,
                    linewidth=2.2, zorder=4)
        ax.annotate(f"{int(orow['n_agree'])}/{int(orow['n_models_checked'])} modèles",
                    xy=(d, obs_zv), xytext=(0, 14), textcoords="offset points",
                    ha="center", fontsize=8.5, fontweight="bold", color=C_OUTLIER)

    ax.plot([], [], "o", color=C_OUTLIER, markerfacecolor="none", markeredgewidth=2.2,
            markersize=9, label=f"Outlier confirmé ({' + '.join(REQUIRED_AGREE_MODELS)} d'accord)")

    ax.set_title(f"{station}  ·  {freq}  ·  {year}", fontsize=12, fontweight="bold", loc="left")
    ax.text(0.99, 1.03, f"{len(year_dates_confirmed)} outlier(s) confirmé(s) cette année",
            transform=ax.transAxes, ha="right", va="bottom",
            fontsize=9.5, style="italic", color=C_OUTLIER, fontweight="bold")
    ax.text(0.01, -0.20,
            f"Modèle affiché : {chosen_model}  ·  outlier confirmé si {' + '.join(REQUIRED_AGREE_MODELS)} "
            f"sont tous deux outlier  ·  station au hasard (seed={RANDOM_SEED})  ·  "
            f"obs/modèle échelle partagée, insitu z-scoré indépendamment",
            transform=ax.transAxes, ha="left", va="top", fontsize=7.5, color="#7F8C8D")

    ax.set_ylabel("Niveau d'eau — z-score")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(True, alpha=0.25, linestyle="--")
    ax.legend(frameon=False, fontsize=8.5, loc="upper left", bbox_to_anchor=(0, -0.08),
              ncol=4, handletextpad=0.5, columnspacing=1.2)

    fig.tight_layout(rect=[0, 0.06, 1, 1])
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=140, facecolor="white")
    plt.close(fig)
    return True


# ═══════════════════════════════════════════════════════════════
# BOUCLE PRINCIPALE
# ═══════════════════════════════════════════════════════════════
def main():
    rng = random.Random(RANDOM_SEED)

    for freq in FREQS:
        print(f"\n{'=' * 60}\n  CONSENSUS OUTLIERS — {freq.upper()} ({SOURCE})\n{'=' * 60}")

        consensus, daily_by_model = build_consensus(freq)
        if consensus.empty:
            print(f"  ⚠ Aucune donnée exploitable pour {freq}")
            continue

        confirmed = consensus[
            consensus[[f"is_outlier_{m}" for m in REQUIRED_AGREE_MODELS]].all(axis=1)
        ]
        candidate_stations = sorted(confirmed["station"].unique())
        print(f"  Stations avec {' ET '.join(REQUIRED_AGREE_MODELS)} d'accord sur au moins 1 outlier : "
              f"{len(candidate_stations)}")

        if not candidate_stations:
            print(f"  ⚠ Aucune station candidate pour {freq}")
            continue

        n_pick = min(N_RANDOM_STATIONS, len(candidate_stations))
        selected = sorted(rng.sample(candidate_stations, n_pick))
        print(f"  {n_pick} station(s) sélectionnée(s) au hasard : {selected}")

        for station in selected:
            station_obs = consensus[consensus["station"] == station].sort_values("date")
            confirmed_station = confirmed[confirmed["station"] == station]
            years = sorted(confirmed_station["date"].dt.year.unique())
            print(f"  {station} : {len(years)} année(s) avec consensus {years}")

            insitu_code = None
            for m in MODELS:
                met_path = RESIDUS_DIR / f"metrics_{m}_{SOURCE}_{freq}_sword_insitu.csv"
                if met_path.exists():
                    dmet = pd.read_csv(met_path)
                    dmet["station"] = dmet["station"].astype(str)
                    row = dmet[dmet["station"] == station]
                    if not row.empty:
                        insitu_code = row["insitu_code"].iloc[0]
                        break

            n_plotted = 0
            for year in years:
                chosen_model = DISPLAY_MODEL

                if chosen_model not in daily_by_model:
                    continue
                d_model = daily_by_model[chosen_model]
                d_model_station = d_model[d_model["station"] == station].copy()

                # Z-score PARTAGÉ obs + SEULEMENT le modèle choisi (aux dates
                # d'observation de la station, sur toute sa période)
                pooled = list(station_obs["obs"].dropna().values)
                mask_dates = d_model_station["date"].isin(station_obs["date"])
                pooled.extend(d_model_station.loc[mask_dates, "pred"].dropna().values)
                model_mu, model_sigma = zscore_params(np.array(pooled))

                station_obs_z = station_obs.assign(obs_z=(station_obs["obs"] - model_mu) / model_sigma)
                d_model_station["pred_z"] = (d_model_station["pred"] - model_mu) / model_sigma

                df_ins = get_insitu_series(insitu_code).copy() if insitu_code else pd.DataFrame()
                if len(df_ins):
                    aligned_ins = align_insitu_to_dates(station_obs["date"].values, df_ins, WINDOW_DAYS[freq])
                    ins_mu, ins_sigma = zscore_params(aligned_ins)
                    df_ins["wl_z"] = (df_ins["wl"] - ins_mu) / ins_sigma

                out_path = PLOT_DIR / SOURCE / freq / station / f"{year}.png"
                if plot_station_year(freq, station, chosen_model, d_model_station, station_obs_z,
                                      confirmed_station, df_ins, year, out_path):
                    n_plotted += 1
            print(f"    -> {n_plotted} graphique(s) -> {PLOT_DIR / SOURCE / freq / station}/")

    print("\n✅ Terminé.")


if __name__ == "__main__":
    main()