"""
plot_stations_targeted.py
════════════════════════════════════════════════════════════════════════
Variante ciblée de plot_stations_outliers_consensus.py :

  - PAS de sélection aléatoire -- stations FIXÉES : TARGET_STATIONS
    ci-dessous.
  - Trace TOUTES LES ANNÉES disponibles pour ces stations (pas
    seulement celles avec un outlier confirmé) -- si une année n'a
    aucun outlier, elle est quand même tracée, juste sans marqueur.
  - Un outlier reste "confirmé" quand DtoD80 ET DtoD96 le détectent
    TOUS LES DEUX INDÉPENDAMMENT à la même date. S'il y en a sur une
    année donnée, ils sont visibles (cercle violet + connecteur +
    annotation "n/3 modèles").
  - DtoD96 reste le modèle affiché en courbe.
  - Bande ±OUTLIER_SIGMA×σ0 affichée autour de la courbe du modèle
    affiché -- σ0 = écart-type des résidus (obs-pred) de CE modèle sur
    la station.
  - NOUVEAU : encart comparant les 4 métriques (NSE/KGE/RMSE/R²) du
    MEILLEUR des 3 modèles DtoD (par NSE vs insitu) à la baseline
    "Alti vs Insitu" (indépendante du modèle), pour CETTE année.
    Fenêtre de calcul : uniquement les vraies dates d'observation
    altimétrique de l'année en cours ; si l'année n'a pas assez de
    points (< MIN_PAIRS), bascule automatiquement sur TOUTE la période
    disponible pour la station (indiqué dans l'encart).

Sources (par modèle, pour chaque freq) :
  Models_Testing/DtoD/residus/residuals_DtoD{80,90,96}_{SOURCE}_{freq}.csv
  Models_Testing/DtoD/residus/metrics_DtoD{80,90,96}_{SOURCE}_{freq}_sword_insitu.csv

Sorties (dossier séparé des autres scripts de plot) :
  Models_Testing/DtoD/plots/targeted_stations/{SOURCE}/{freq}/{station}/{year}.png
════════════════════════════════════════════════════════════════════════
"""

import sqlite3
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

SOURCE = "dahiti"   # <-- "hwnext" ou "dahiti"
TARGET_STATIONS = ["13391"]   # <-- stations fixées, pas de tirage aléatoire
MODELS = ["DtoD80", "DtoD90", "DtoD96"]
FREQS = ["10j", "27j"]
OUTLIER_SIGMA = 2
REQUIRED_AGREE_MODELS = ["DtoD80", "DtoD96"]  # outlier confirmé ssi CES modèles sont TOUS deux outlier
DISPLAY_MODEL = "DtoD96"   # courbe toujours affichée sur les plots
MIN_PAIRS = 10             # nb minimal de points obs pour un calcul de métriques fiable

WINDOW_DAYS = {"10j": 5, "27j": 14}  # tolérance de recalage insitu

RESIDUS_DIR = Path("./Models_Testing/DtoD/residus")
PLOT_DIR = Path("./Models_Testing/DtoD/plots/targeted_stations")  # dossier séparé
INSITU_DB = "./data/insitu_data.db"
DATE_MIN, DATE_MAX = "2016-01-01", "2025-12-31"

C_OBS = "#1B4F72"
C_INS = "#229954"
C_MODEL = {"DtoD80": "#E74C3C", "DtoD90": "#C0392B", "DtoD96": "#78281F"}
C_OUTLIER = "#8E44AD"
C_SIGMA_BAND = "#78281F"
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


def zscore(arr):
    """z-score indépendant (sa propre moyenne/écart-type) -- pour comparer
    à l'insitu, référentiel différent (cf. discussions précédentes)."""
    arr = np.asarray(arr, dtype=float)
    m = ~np.isnan(arr)
    if m.sum() < 2:
        return arr * np.nan
    mu, sig = arr[m].mean(), arr[m].std()
    return (arr - mu) / sig if sig > 0 else arr * 0


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
# MÉTRIQUES (NSE/KGE sans beta/RMSE/R2, identique à
# compare_other_models_vs_insitu.py -- pour rester cohérent avec le
# reste du pipeline)
# ═══════════════════════════════════════════════════════════════
def compute_metrics(obs, pred) -> dict:
    obs, pred = np.asarray(obs, dtype=float), np.asarray(pred, dtype=float)
    n = len(obs)
    if n < MIN_PAIRS:
        return {"NSE": np.nan, "KGE": np.nan, "RMSE": np.nan, "R2": np.nan, "n": n}
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
        kge = 1 - np.sqrt((r - 1) ** 2 + (alpha - 1) ** 2)
    else:
        kge = np.nan
    return {"NSE": float(nse) if pd.notna(nse) else np.nan,
            "KGE": float(kge) if pd.notna(kge) else np.nan,
            "RMSE": rmse, "R2": r2, "n": n}


def fmt_metric(x, digits=2):
    return f"{x:.{digits}f}" if pd.notna(x) else "—"


# ═══════════════════════════════════════════════════════════════
# DÉTECTION D'OUTLIERS PAR MODÈLE (identique à plot_stations_outliers_consensus.py)
# ═══════════════════════════════════════════════════════════════
def flag_outliers_one_station(grp: pd.DataFrame) -> pd.DataFrame:
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
        if station not in TARGET_STATIONS:
            continue
        flagged = flag_outliers_one_station(grp)
        flagged["station"] = station
        results.append(flagged)
    flagged_all = pd.concat(results, ignore_index=True) if results else pd.DataFrame()
    if not flagged_all.empty:
        flagged_all["model"] = model_label
    return flagged_all


def compute_sigma0(model_daily: pd.DataFrame) -> float:
    """σ0 = écart-type des résidus (obs-pred) de la station, pour le
    modèle affiché -- même calcul que flag_outliers_one_station, mais
    isolé pour pouvoir dessiner la bande de tolérance sur le plot."""
    grp = model_daily.dropna(subset=["obs", "pred"])
    if len(grp) < 5:
        return float("nan")
    residual = grp["obs"] - grp["pred"]
    sigma = float(residual.std())
    return sigma if sigma > 0 else float("nan")


# ═══════════════════════════════════════════════════════════════
# CONSENSUS (identique à plot_stations_outliers_consensus.py, restreint
# à TARGET_STATIONS)
# ═══════════════════════════════════════════════════════════════
def build_consensus(freq: str) -> tuple[pd.DataFrame, dict]:
    flagged_by_model = {m: load_and_flag(m, freq) for m in MODELS}
    daily_by_model = {}
    for m in MODELS:
        p = RESIDUS_DIR / f"residuals_{m}_{SOURCE}_{freq}.csv"
        if p.exists():
            d = pd.read_csv(p)
            d["station"] = d["station"].astype(str)
            d["date"] = pd.to_datetime(d["date"])
            d = d[d["station"].isin(TARGET_STATIONS)]
            daily_by_model[m] = d

    valid_models = [m for m in MODELS if not flagged_by_model[m].empty]
    if not all(m in valid_models for m in REQUIRED_AGREE_MODELS):
        print(f"  ⚠ Un des modèles requis ({REQUIRED_AGREE_MODELS}) est indisponible pour {freq} "
              f"-> consensus impossible sur ce point (les plots seront quand même tracés, sans marqueur)")
        return pd.DataFrame(), daily_by_model

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
# COMPARAISON "MEILLEUR MODÈLE" vs "ALTI-INSITU BASELINE" POUR CETTE ANNÉE
# ═══════════════════════════════════════════════════════════════
def compute_year_comparison(station: str, year: int, daily_by_model: dict,
                             df_ins: pd.DataFrame, window_days: int):
    """
    Retourne (best_model, best_metrics, baseline_metrics, window_label)
    ou (None, None, None, None) si rien d'exploitable.

    Fenêtre : uniquement les vraies dates d'observation altimétrique de
    l'ANNÉE en cours ; si aucun modèle n'atteint MIN_PAIRS points cette
    année-là, bascule sur TOUTE la période disponible de la station.
    Dans les deux cas, seules les dates avec obs réelle comptent (c'est
    de toute façon indispensable pour calculer NSE/KGE/RMSE/R²).
    """
    def gather(use_year: bool) -> dict:
        out = {}
        for m in MODELS:
            if m not in daily_by_model:
                continue
            dfm = daily_by_model[m]
            sub = dfm[dfm["station"] == station].dropna(subset=["obs", "pred"]).sort_values("date")
            if use_year:
                sub = sub[sub["date"].dt.year == year]
            if len(sub) < MIN_PAIRS:
                continue
            ins_wl = align_insitu_to_dates(sub["date"].values, df_ins, window_days)
            out[m] = {"obs": sub["obs"].values, "pred": sub["pred"].values, "ins": ins_wl}
        return out

    data_year = gather(use_year=True)
    if data_year:
        window_label, data = f"année {year}", data_year
    else:
        data_all = gather(use_year=False)
        if not data_all:
            return None, None, None, None
        window_label, data = "toutes années (échantillon insuffisant cette année)", data_all

    # Modèle vs Insitu, pour chaque modèle dispo -> on garde le meilleur NSE
    model_metrics = {}
    for m, vals in data.items():
        pred_z, ins_z = zscore(vals["pred"]), zscore(vals["ins"])
        mask = ~(np.isnan(pred_z) | np.isnan(ins_z))
        model_metrics[m] = compute_metrics(ins_z[mask], pred_z[mask])

    valid = {m: mm for m, mm in model_metrics.items() if pd.notna(mm["NSE"])}
    if not valid:
        return None, None, None, window_label
    best_model = max(valid, key=lambda m: valid[m]["NSE"])
    best_metrics = valid[best_model]

    # Baseline Alti vs Insitu, sur les MÊMES dates que le meilleur modèle
    # (comparaison équitable, mêmes points)
    vals = data[best_model]
    obs_z, ins_z = zscore(vals["obs"]), zscore(vals["ins"])
    mask = ~(np.isnan(obs_z) | np.isnan(ins_z))
    baseline_metrics = compute_metrics(ins_z[mask], obs_z[mask])

    return best_model, best_metrics, baseline_metrics, window_label


# ═══════════════════════════════════════════════════════════════
# PLOT PAR STATION / ANNÉE -- toutes années, marqueurs seulement si outlier
# ═══════════════════════════════════════════════════════════════
def plot_station_year(freq, station, chosen_model, model_daily, obs_ref, year_dates_confirmed,
                       df_ins, year, sigma0_z, comparison, out_path):
    ins_year = df_ins[df_ins["date"].dt.year == year] if len(df_ins) else pd.DataFrame()
    obs_year = obs_ref[obs_ref["date"].dt.year == year]
    pred_year = model_daily[model_daily["date"].dt.year == year].dropna(subset=["pred_z"])

    if obs_year.empty and pred_year.empty:
        return False

    fig, ax = plt.subplots(figsize=(13, 4.6))

    # Bande ±OUTLIER_SIGMA×σ0 autour de la courbe du modèle affiché
    if len(pred_year) and pd.notna(sigma0_z):
        band_half = OUTLIER_SIGMA * sigma0_z
        ax.fill_between(pred_year["date"], pred_year["pred_z"] - band_half,
                         pred_year["pred_z"] + band_half, color=C_SIGMA_BAND, alpha=0.12,
                         linewidth=0, zorder=0.5,
                         label=f"Intervalle ±{OUTLIER_SIGMA}σ0 ({chosen_model})")

    if len(ins_year):
        ax.plot(ins_year["date"], ins_year["wl_z"], "-", color=C_INS,
                linewidth=1.1, alpha=0.5, label="Insitu (quotidien, z-score)", zorder=1)

    if len(pred_year):
        ax.plot(pred_year["date"], pred_year["pred_z"], "-", color=C_MODEL[chosen_model],
                linewidth=1.4, alpha=0.9, label=f"{chosen_model} (quotidien, z-score)", zorder=2)

    ax.plot(obs_year["date"], obs_year["obs_z"], "o", color=C_OBS, markersize=6,
            markeredgecolor="white", markeredgewidth=0.6,
            label="Altimétrie (obs, z-score)", zorder=3)

    n_outliers_year = 0
    if year_dates_confirmed is not None and len(year_dates_confirmed):
        pred_lookup = model_daily.set_index("date")["pred_z"]
        n_outliers_year = len(year_dates_confirmed)
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
    status_txt = (f"{n_outliers_year} outlier(s) confirmé(s) cette année" if n_outliers_year
                  else "Aucun outlier confirmé cette année")
    status_color = C_OUTLIER if n_outliers_year else "#7F8C8D"
    ax.text(0.99, 1.03, status_txt, transform=ax.transAxes, ha="right", va="bottom",
            fontsize=9.5, style="italic", color=status_color, fontweight="bold")

    # Encart comparaison meilleur modèle vs baseline Alti-Insitu
    best_model, best_metrics, baseline_metrics, window_label = comparison
    if best_model is not None:
        lines = [
            f"Meilleur modèle ({window_label}) : {best_model}",
            "",
            f"{'':>4}{best_model:>10}  Alti-Insitu",
            f"{'NSE':<4}{fmt_metric(best_metrics['NSE']):>10}  {fmt_metric(baseline_metrics['NSE']):>10}",
            f"{'KGE':<4}{fmt_metric(best_metrics['KGE']):>10}  {fmt_metric(baseline_metrics['KGE']):>10}",
            f"{'RMSE':<4}{fmt_metric(best_metrics['RMSE']):>10}  {fmt_metric(baseline_metrics['RMSE']):>10}",
            f"{'R²':<4}{fmt_metric(best_metrics['R2']):>10}  {fmt_metric(baseline_metrics['R2']):>10}",
        ]
        fig.text(0.965, 0.90, "\n".join(lines), transform=fig.transFigure, va="top", ha="right",
                 fontsize=9.5, family="monospace", color=C_TEXT, linespacing=1.6,
                 bbox=dict(boxstyle="round,pad=0.5", facecolor="white",
                           edgecolor=C_MODEL.get(best_model, C_TEXT), alpha=0.95, linewidth=1.4))
    sigma_txt = f"σ0 = {sigma0_z:.3f} (z-score)" if pd.notna(sigma0_z) else "σ0 indisponible"
    ax.text(0.01, -0.24,
            f"Modèle affiché : {chosen_model}  ·  outlier confirmé si {' + '.join(REQUIRED_AGREE_MODELS)} "
            f"sont tous deux outlier  ·  {sigma_txt}  ·  station ciblée (toutes années tracées)  ·  "
            f"obs/modèle échelle partagée, insitu z-scoré indépendamment",
            transform=ax.transAxes, ha="left", va="top", fontsize=7.5, color="#7F8C8D")

    ax.set_ylabel("Niveau d'eau — z-score")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(True, alpha=0.25, linestyle="--")
    ax.legend(frameon=False, fontsize=8, loc="upper left", bbox_to_anchor=(0, -0.08),
              ncol=5, handletextpad=0.4, columnspacing=0.9)

    fig.tight_layout(rect=[0, 0.11, 0.76, 1])
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=140, facecolor="white")
    plt.close(fig)
    return True


# ═══════════════════════════════════════════════════════════════
# BOUCLE PRINCIPALE
# ═══════════════════════════════════════════════════════════════
def main():
    for freq in FREQS:
        print(f"\n{'=' * 60}\n  STATIONS CIBLÉES — {freq.upper()} ({SOURCE})\n{'=' * 60}")

        consensus, daily_by_model = build_consensus(freq)

        if DISPLAY_MODEL not in daily_by_model:
            print(f"  ⚠ {DISPLAY_MODEL} indisponible pour {freq} -> ignoré")
            continue

        for station in TARGET_STATIONS:
            d_model = daily_by_model[DISPLAY_MODEL]
            d_model_station = d_model[d_model["station"] == station].copy()
            if d_model_station.empty:
                print(f"  {station} : absente de {DISPLAY_MODEL} [{freq}] -> ignorée pour cette fréquence")
                continue

            station_obs = d_model_station.dropna(subset=["obs"]).sort_values("date")[
                ["station", "date", "obs"]]
            if station_obs.empty:
                print(f"  {station} : aucune observation [{freq}] -> ignorée")
                continue

            confirmed_station = pd.DataFrame()
            if not consensus.empty:
                confirmed = consensus[
                    consensus[[f"is_outlier_{m}" for m in REQUIRED_AGREE_MODELS]].all(axis=1)
                ]
                confirmed_station = confirmed[confirmed["station"] == station]

            sigma0_raw = compute_sigma0(d_model_station)

            pooled = list(station_obs["obs"].dropna().values)
            mask_dates = d_model_station["date"].isin(station_obs["date"])
            pooled.extend(d_model_station.loc[mask_dates, "pred"].dropna().values)
            model_mu, model_sigma = zscore_params(np.array(pooled))

            sigma0_z = sigma0_raw / model_sigma if pd.notna(sigma0_raw) else float("nan")

            station_obs_z = station_obs.assign(obs_z=(station_obs["obs"] - model_mu) / model_sigma)
            d_model_station["pred_z"] = (d_model_station["pred"] - model_mu) / model_sigma

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

            df_ins = get_insitu_series(insitu_code).copy() if insitu_code else pd.DataFrame()
            if len(df_ins):
                aligned_ins = align_insitu_to_dates(station_obs["date"].values, df_ins, WINDOW_DAYS[freq])
                ins_mu, ins_sigma = zscore_params(aligned_ins)
                df_ins["wl_z"] = (df_ins["wl"] - ins_mu) / ins_sigma

            years = sorted(set(station_obs["date"].dt.year) | set(d_model_station["date"].dt.year))
            n_plotted, n_with_outliers = 0, 0
            for year in years:
                year_confirmed = (confirmed_station[confirmed_station["date"].dt.year == year]
                                   if len(confirmed_station) else pd.DataFrame())

                comparison = compute_year_comparison(station, year, daily_by_model, df_ins,
                                                      WINDOW_DAYS[freq])

                out_path = PLOT_DIR / SOURCE / freq / station / f"{year}.png"
                if plot_station_year(freq, station, DISPLAY_MODEL, d_model_station, station_obs_z,
                                      year_confirmed, df_ins, year, sigma0_z, comparison, out_path):
                    n_plotted += 1
                    if len(year_confirmed):
                        n_with_outliers += 1
            print(f"  {station} : {n_plotted} graphique(s) tracé(s) ({n_with_outliers} avec outlier "
                  f"confirmé) -> {PLOT_DIR / SOURCE / freq / station}/  "
                  f"(σ0 = {sigma0_raw:.4f} unités brutes)")

    print("\n✅ Terminé.")


if __name__ == "__main__":
    main()