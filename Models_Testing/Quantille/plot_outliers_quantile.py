"""
plot_stations_outliers_quantile_bands.py
════════════════════════════════════════════════════════════════════════
Variante Quantile de plot_stations_outliers_consensus.py :

  - Regarde les 3 modèles Quantile80/90/96 EN MÊME TEMPS, sur TOUTES
    les stations disponibles.
  - DÉFINITION DE L'OUTLIER (différente de la version DtoD -- ici on a
    la distribution complète, pas juste un point) : un point est outlier
    POUR UN MODÈLE DONNÉ si l'observation altimétrique tombe EN DEHORS
    de l'intervalle [Q5, Q95] prédit par ce modèle -- pas de sigma, pas
    de seuil arbitraire, directement la couverture de l'intervalle prédit.
  - Un outlier est "confirmé" quand Quantile96 le détecte ET qu'AU MOINS
    UN des deux autres (Quantile80 ou Quantile90) le détecte aussi
    (REQUIRED_MODEL obligatoire + MIN_OTHER_AGREE parmi OTHER_MODELS).
  - Sélectionne 10 stations AU HASARD (seed fixe) parmi celles qui ont
    au moins un outlier confirmé -- par fréquence, indépendamment du NSE.
  - Ne trace que les années avec au moins un outlier confirmé.
  - Quantile96 est TOUJOURS le modèle affiché, avec sa bande complète
    en dégradé de bleu : ligne bleu foncé = Q50, remplissage bleu moyen
    = [Q25,Q75], remplissage bleu pâle = [Q5,Q25] et [Q75,Q95].

Obs + toute la bande du modèle affiché sont z-scorés sur une échelle
PARTAGÉE (mu/sigma commun, calculé sur obs+Q50 aux dates d'observation,
même transform appliqué à Q5/Q25/Q75/Q95) -- la largeur de bande reste
proportionnelle à l'incertitude réelle du modèle. Insitu garde son
propre z-score (référentiel différent).

Source (fichiers "_bands", produits par eval_quantile_bands.py) :
  Models_Testing/Quantille/residus/residuals_Quantile{80,90,96}_{SOURCE}_{freq}_bands.csv
  colonnes : station, date, obs, pred_q05, pred_q25, pred_q50, pred_q75, pred_q95

Sorties :
  Models_Testing/Quantille/plots/outliers_bands/{SOURCE}/{freq}/{station}/{year}.png
════════════════════════════════════════════════════════════════════════
"""

import random
import sqlite3
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

SOURCE = "hwnext"   # <-- "hwnext" ou "dahiti"
MODELS = ["Quantile80", "Quantile90", "Quantile96"]
REQUIRED_MODEL = "Quantile96"                       # obligatoire pour confirmer
OTHER_MODELS = ["Quantile80", "Quantile90"]          # au moins MIN_OTHER_AGREE parmi eux
MIN_OTHER_AGREE = 1
DISPLAY_MODEL = "Quantile96"                         # bande toujours affichée
FREQS = ["10j", "27j"]
N_RANDOM_STATIONS = 10
RANDOM_SEED = 42

WINDOW_DAYS = {"10j": 5, "27j": 14}  # tolérance de recalage insitu (affichage seulement)

RESIDUS_DIR = Path("./Models_Testing/Quantille/residus")
PLOT_DIR = Path("./Models_Testing/Quantille/plots/outliers_bands")
INSITU_DB = "./data/insitu_data.db"
DATE_MIN, DATE_MAX = "2016-01-01", "2025-12-31"

C_OBS = "#1B4F72"
C_INS = "#229954"
C_Q50 = "#0B3D91"       # bleu foncé
C_BAND_MID = "#5DADE2"  # bleu moyen (Q25-Q75)
C_BAND_OUT = "#AED6F1"  # bleu pâle (Q5-Q25, Q75-Q95)
C_OUTLIER = "#8E44AD"   # violet
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
# OUTLIER = obs hors [Q5, Q95] du modèle, PAR MODÈLE
# ═══════════════════════════════════════════════════════════════
def load_and_flag(model_label: str, freq: str) -> pd.DataFrame:
    path = RESIDUS_DIR / f"residuals_{model_label}_{SOURCE}_{freq}_bands.csv"
    if not path.exists():
        print(f"⚠ Fichier introuvable : {path} -> {model_label} ignoré pour {freq} "
              f"(lancez eval_quantile_bands.py d'abord)")
        return pd.DataFrame()
    df = pd.read_csv(path)
    df["station"] = df["station"].astype(str)
    df["date"] = pd.to_datetime(df["date"])
    df_obs = df.dropna(subset=["obs", "pred_q05", "pred_q95"])
    if df_obs.empty:
        return pd.DataFrame()
    df_obs = df_obs.copy()
    df_obs["is_outlier"] = (df_obs["obs"] < df_obs["pred_q05"]) | (df_obs["obs"] > df_obs["pred_q95"])
    return df_obs


def build_consensus(freq: str) -> tuple[pd.DataFrame, dict]:
    flagged_by_model = {m: load_and_flag(m, freq) for m in MODELS}
    daily_by_model = {}
    for m in MODELS:
        p = RESIDUS_DIR / f"residuals_{m}_{SOURCE}_{freq}_bands.csv"
        if p.exists():
            d = pd.read_csv(p)
            d["station"] = d["station"].astype(str)
            d["date"] = pd.to_datetime(d["date"])
            daily_by_model[m] = d

    required_ok = not flagged_by_model[REQUIRED_MODEL].empty
    valid_others = [m for m in OTHER_MODELS if not flagged_by_model[m].empty]
    if not required_ok or len(valid_others) < MIN_OTHER_AGREE:
        print(f"  ⚠ Modèle requis ({REQUIRED_MODEL}) ou modèles secondaires insuffisants "
              f"pour {freq} -> consensus impossible")
        return pd.DataFrame(), daily_by_model

    valid_models = [REQUIRED_MODEL] + valid_others
    base = None
    for m in valid_models:
        sub = flagged_by_model[m][["station", "date", "obs", "is_outlier"]].rename(
            columns={"is_outlier": f"is_outlier_{m}"})
        base = sub if base is None else base.merge(sub, on=["station", "date", "obs"], how="inner")

    if base is None or base.empty:
        return pd.DataFrame(), daily_by_model

    base["n_agree"] = base[[f"is_outlier_{m}" for m in valid_models]].sum(axis=1)
    base["n_models_checked"] = len(valid_models)
    return base, daily_by_model


# ═══════════════════════════════════════════════════════════════
# PLOT PAR STATION / ANNÉE
# ═══════════════════════════════════════════════════════════════
def plot_station_year(freq, station, model_daily, obs_ref, confirmed_station, df_ins, year, out_path):
    year_dates_confirmed = confirmed_station[confirmed_station["date"].dt.year == year]
    if year_dates_confirmed.empty:
        return False

    ins_year = df_ins[df_ins["date"].dt.year == year] if len(df_ins) else pd.DataFrame()
    obs_year = obs_ref[obs_ref["date"].dt.year == year]
    band_year = model_daily[model_daily["date"].dt.year == year].dropna(subset=["q50_z"]).sort_values("date")

    fig, ax = plt.subplots(figsize=(11, 4.8))

    if len(band_year):
        ax.fill_between(band_year["date"], band_year["q05_z"], band_year["q25_z"],
                         color=C_BAND_OUT, alpha=0.55, linewidth=0, zorder=1, label="Q5–Q25 / Q75–Q95")
        ax.fill_between(band_year["date"], band_year["q75_z"], band_year["q95_z"],
                         color=C_BAND_OUT, alpha=0.55, linewidth=0, zorder=1)
        ax.fill_between(band_year["date"], band_year["q25_z"], band_year["q75_z"],
                         color=C_BAND_MID, alpha=0.55, linewidth=0, zorder=2, label="Q25–Q75")
        ax.plot(band_year["date"], band_year["q50_z"], "-", color=C_Q50,
                linewidth=1.4, alpha=0.95, label=f"{DISPLAY_MODEL} (Q50)", zorder=3)

    if len(ins_year):
        ax.plot(ins_year["date"], ins_year["wl_z"], "-", color=C_INS,
                linewidth=1.1, alpha=0.6, label="Insitu (quotidien, z-score)", zorder=2.5)

    ax.plot(obs_year["date"], obs_year["obs_z"], "o", color=C_OBS, markersize=6,
            markeredgecolor="white", markeredgewidth=0.6,
            label="Altimétrie (obs, z-score)", zorder=4)

    band_lookup = model_daily.set_index("date")
    for _, orow in year_dates_confirmed.iterrows():
        d = orow["date"]
        obs_row = obs_year[obs_year["date"] == d]
        if obs_row.empty or d not in band_lookup.index:
            continue
        obs_zv = obs_row["obs_z"].iloc[0]
        q05v, q95v = band_lookup.loc[d, "q05_z"], band_lookup.loc[d, "q95_z"]
        boundary = q05v if obs_zv < q05v else q95v  # borne violée
        ax.plot([d, d], [obs_zv, boundary], ":", color=C_OUTLIER, linewidth=1.4, alpha=0.85, zorder=4.5)
        ax.scatter([d], [obs_zv], s=190, facecolors="none", edgecolors=C_OUTLIER,
                    linewidth=2.2, zorder=5)
        ax.annotate(f"{int(orow['n_agree'])}/{int(orow['n_models_checked'])} modèles",
                    xy=(d, obs_zv), xytext=(0, 14), textcoords="offset points",
                    ha="center", fontsize=8.5, fontweight="bold", color=C_OUTLIER)

    ax.plot([], [], "o", color=C_OUTLIER, markerfacecolor="none", markeredgewidth=2.2,
            markersize=9, label=f"Outlier confirmé (hors Q5-Q95, {REQUIRED_MODEL}+{MIN_OTHER_AGREE})")

    ax.set_title(f"{station}  ·  {freq}  ·  {year}", fontsize=12, fontweight="bold", loc="left")
    ax.text(0.99, 1.03, f"{len(year_dates_confirmed)} outlier(s) confirmé(s) cette année",
            transform=ax.transAxes, ha="right", va="bottom",
            fontsize=9.5, style="italic", color=C_OUTLIER, fontweight="bold")
    ax.text(0.01, -0.20,
            f"Bande affichée : {DISPLAY_MODEL}  ·  outlier = obs hors [Q5,Q95] du modèle  ·  "
            f"confirmé si {REQUIRED_MODEL} + ≥{MIN_OTHER_AGREE}/{len(OTHER_MODELS)} autres  ·  "
            f"station au hasard (seed={RANDOM_SEED})",
            transform=ax.transAxes, ha="left", va="top", fontsize=7.5, color="#7F8C8D")

    ax.set_ylabel("Niveau d'eau — z-score")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(True, alpha=0.25, linestyle="--")
    ax.legend(frameon=False, fontsize=8, loc="upper left", bbox_to_anchor=(0, -0.08),
              ncol=3, handletextpad=0.5, columnspacing=1.1)

    fig.tight_layout(rect=[0, 0.07, 1, 1])
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
        print(f"\n{'=' * 60}\n  OUTLIERS BANDES QUANTILE — {freq.upper()} ({SOURCE})\n{'=' * 60}")

        consensus, daily_by_model = build_consensus(freq)
        if consensus.empty:
            print(f"  ⚠ Aucune donnée exploitable pour {freq}")
            continue

        other_flag_sum = consensus[[f"is_outlier_{m}" for m in OTHER_MODELS
                                     if f"is_outlier_{m}" in consensus.columns]].sum(axis=1)
        confirmed = consensus[consensus[f"is_outlier_{REQUIRED_MODEL}"] & (other_flag_sum >= MIN_OTHER_AGREE)]
        candidate_stations = sorted(confirmed["station"].unique())
        print(f"  Stations avec {REQUIRED_MODEL} + ≥{MIN_OTHER_AGREE} autre(s) d'accord "
              f"sur au moins 1 outlier : {len(candidate_stations)}")

        if not candidate_stations:
            print(f"  ⚠ Aucune station candidate pour {freq}")
            continue

        n_pick = min(N_RANDOM_STATIONS, len(candidate_stations))
        selected = sorted(rng.sample(candidate_stations, n_pick))
        print(f"  {n_pick} station(s) sélectionnée(s) au hasard : {selected}")

        if DISPLAY_MODEL not in daily_by_model:
            print(f"  ⚠ {DISPLAY_MODEL} indisponible -> impossible d'afficher")
            continue

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

            d_model = daily_by_model[DISPLAY_MODEL]
            d_model_station = d_model[d_model["station"] == station].copy()

            # Z-score PARTAGÉ : mu/sigma calculé sur obs + Q50 aux dates
            # d'observation, PUIS appliqué identiquement à Q5/Q25/Q50/Q75/Q95
            # -> la largeur de bande reste proportionnelle à l'incertitude réelle.
            mask_dates = d_model_station["date"].isin(station_obs["date"])
            pooled = list(station_obs["obs"].dropna().values)
            pooled.extend(d_model_station.loc[mask_dates, "pred_q50"].dropna().values)
            model_mu, model_sigma = zscore_params(np.array(pooled))

            station_obs_z = station_obs.assign(obs_z=(station_obs["obs"] - model_mu) / model_sigma)
            for q in ["q05", "q25", "q50", "q75", "q95"]:
                d_model_station[f"{q}_z"] = (d_model_station[f"pred_{q}"] - model_mu) / model_sigma

            df_ins = get_insitu_series(insitu_code).copy() if insitu_code else pd.DataFrame()
            if len(df_ins):
                aligned_ins = align_insitu_to_dates(station_obs["date"].values, df_ins, WINDOW_DAYS[freq])
                ins_mu, ins_sigma = zscore_params(aligned_ins)
                df_ins["wl_z"] = (df_ins["wl"] - ins_mu) / ins_sigma

            n_plotted = 0
            for year in years:
                out_path = PLOT_DIR / SOURCE / freq / station / f"{year}.png"
                if plot_station_year(freq, station, d_model_station, station_obs_z,
                                      confirmed_station, df_ins, year, out_path):
                    n_plotted += 1
            print(f"    -> {n_plotted} graphique(s) -> {PLOT_DIR / SOURCE / freq / station}/")

    print("\n✅ Terminé.")


if __name__ == "__main__":
    main()