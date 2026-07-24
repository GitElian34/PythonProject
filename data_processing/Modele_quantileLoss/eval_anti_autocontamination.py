"""
iterative_outlier_correction.py
════════════════════════════════════════════════════════════════════════
Teste si les "sauts" du modèle AR-LSTM vers chaque point altimétrique
reçu en entrée (même quand ce point est un outlier) viennent d'une
contamination du signal AR. Boucle, par modèle :

  1. Évalue le modèle sur la station (zero-shot).
  2. Flague les points obs hors de [Q05, Q95].
  3. Corrige ces points dans le .nc de travail (jamais l'original) —
     masquage NaN par défaut (CORRECTION_MODE), pour éviter d'apprendre
     au modèle "réalité = ma médiane exacte" (qui ferait s'effondrer
     son incertitude en boucle fermée).
  4. Réévalue, reflague, recorrige, sur N_CYCLES itérations.

Tous les plots comparent systématiquement à l'altimétrie D'ORIGINE
(cycle 0, jamais modifiée), alignée par date à chaque cycle pour éviter
tout décalage entre deux évaluations.

Sorties :
    {OUTPUT_ROOT}/{model}/cycle_XX.png              → 1 plot par cycle
    {OUTPUT_ROOT}/{model}/log_cycles.csv             → outliers + largeur d'intervalle
    {ANTI_ROOT}/{model}/{year}_cycles_X-Y.png        → comparaison par année et groupe de cycles

Usage :
    python iterative_outlier_correction.py
════════════════════════════════════════════════════════════════════════
"""

import shutil
import pickle
import sqlite3
import numpy as np
import pandas as pd
import netCDF4 as nc
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from pathlib import Path
from ruamel.yaml import YAML
from neuralhydrology.utils.config import Config
from neuralhydrology.evaluation.evaluate import start_evaluation

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
STATION = "0000000008681"
N_CYCLES = 5
CORRECTION_MODE = "mask_nan"     # "mask_nan" (recommandé) ou "q50_replace"
FORCE_FRESH_COPY = True          # repart d'une copie propre du .nc à chaque lancement
CYCLE_GROUPS = [[0, 1, 2], [3, 4, 5]]   # groupes de cycles comparés par année

MODELS = {
    "arlstm_DtoD80_quantile_3006_155128": {"epoch": 9,  "mask": 80},
    "arlstm_DtoD90_quantile_3006_154719": {"epoch": 10, "mask": 90},
}
QUANTILES = ["q05", "q25", "q50", "q75", "q95"]
TARGET_VAR = "water_level"

RUNS_DIR = Path("./runs")
DATA_DIR = Path("./data/IA/NeuralHydrologyHWNextDtoD")
NC_DIR = DATA_DIR / "time_series"
SAT_DB = "./data/hydroweb_next.db"

OUTPUT_ROOT = Path(f"./data_processing/Modele_quantileLoss/outlier_correction/{STATION}_{CORRECTION_MODE}")
ANTI_ROOT = Path(f"./data_processing/Modele_quantileLoss/antiautocontamination/{STATION}_{CORRECTION_MODE}")
OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
ANTI_ROOT.mkdir(parents=True, exist_ok=True)


# ═══════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════
def get_alti_stats(station_code):
    """mean/std de orthometric_height (mètres), pour affichage uniquement."""
    conn = sqlite3.connect(SAT_DB)
    df = pd.read_sql(
        """SELECT m.orthometric_height AS h FROM measurements m
           JOIN stations s ON s.station_code = m.station_code
           WHERE s.station_code = ? AND m.is_valid = 1""",
        conn, params=(station_code.zfill(13),)
    )
    conn.close()
    return (float(df["h"].mean()), float(df["h"].std())) if not df.empty else (None, None)


def to_meters(z, mean_alti, std_alti):
    return np.asarray(z, dtype=float) * std_alti + mean_alti


def align_to_dates(dates, values, ref_dates):
    """Réaligne `values` sur `ref_dates` par vraie correspondance de date (pas par
    position) — indispensable car NeuralHydrology ne garantit pas le même
    ordre/longueur de dates d'un cycle à l'autre."""
    s = pd.Series(np.asarray(values, dtype=float), index=pd.DatetimeIndex(dates).normalize())
    s = s[~s.index.duplicated(keep="first")]
    return s.reindex(pd.DatetimeIndex(ref_dates).normalize()).values


def flag_outliers(values, q05, q95):
    out = np.zeros(len(values), dtype=bool)
    valid = ~np.isnan(values)
    out[valid] = (values[valid] < q05[valid]) | (values[valid] > q95[valid])
    return out


def setup_work_nc(model_label):
    """Copie de travail du .nc de la station (symlinks pour le reste du data_dir).
    L'original n'est jamais ouvert en écriture."""
    work_dir = OUTPUT_ROOT / model_label / "data_dir"
    ts_dir = work_dir / "time_series"
    ts_dir.mkdir(parents=True, exist_ok=True)

    for item in DATA_DIR.iterdir():
        if item.name != "time_series" and not (work_dir / item.name).exists():
            (work_dir / item.name).symlink_to(item.resolve())

    src = NC_DIR / f"{STATION}.nc"
    dst = ts_dir / f"{STATION}.nc"
    if FORCE_FRESH_COPY and dst.exists():
        dst.unlink()
    if not dst.exists():
        shutil.copy2(src, dst)

    for f in NC_DIR.glob("*.nc"):
        if f.stem != STATION and not (ts_dir / f.name).exists():
            (ts_dir / f.name).symlink_to(f.resolve())

    stations_txt = OUTPUT_ROOT / model_label / "station_only.txt"
    stations_txt.write_text(STATION)
    return work_dir, dst, stations_txt


def update_nc(nc_path, updates):
    """updates : {Timestamp -> nouvelle valeur z-score, ou np.nan pour masquer}."""
    ds = nc.Dataset(nc_path, mode="r+")
    dates = pd.to_datetime("2016-01-01") + pd.to_timedelta(ds.variables["date"][:], unit="D")
    wl = ds.variables[TARGET_VAR]
    wl.set_auto_mask(False)
    idx_by_date = {d.normalize(): i for i, d in enumerate(pd.DatetimeIndex(dates))}
    n = 0
    for d, val in updates.items():
        idx = idx_by_date.get(pd.Timestamp(d).normalize())
        if idx is not None:
            wl[idx] = val
            n += 1
    ds.close()
    return n


def run_evaluation(run_dir, epoch, work_dir, stations_txt):
    ryaml = YAML()
    ryaml.preserve_quotes = True
    with open(run_dir / "config.yml") as f:
        cfg_dict = ryaml.load(f)
    cfg_dict["validation_basin_file"] = str(stations_txt.resolve())
    cfg_dict["data_dir"] = str(work_dir.resolve())
    config_eval = run_dir / f"config_eval_outlier_correction_{STATION}.yml"
    with open(config_eval, "w") as f:
        ryaml.dump(cfg_dict, f)

    start_evaluation(cfg=Config(config_eval), run_dir=run_dir, epoch=epoch, period="validation")

    results_path = run_dir / "validation" / f"model_epoch{epoch:03d}" / "validation_results.p"
    with open(results_path, "rb") as f:
        raw = pickle.load(f)
    data = raw.get(STATION) or raw.get(str(int(STATION)))
    if data is None:
        raise RuntimeError(f"Station {STATION} absente des résultats.")
    ds = data[list(data.keys())[0]]["xr"]

    dates = pd.to_datetime(ds.date.values)
    obs = ds[f"{TARGET_VAR}_obs"].values.flatten()
    pred = ds[f"{TARGET_VAR}_sim"].values.flatten()
    quantiles = {q: (ds[f"{TARGET_VAR}_sim_{q}"].values.flatten()
                      if f"{TARGET_VAR}_sim_{q}" in ds else None) for q in QUANTILES}
    return dates, obs, pred, quantiles


# ═══════════════════════════════════════════════════════════════
# PLOT — un seul helper de dessin, réutilisé pour tous les cas
# ═══════════════════════════════════════════════════════════════
def draw_panel(ax, dates, obs_m, q_m, outlier_mask, title):
    ax.fill_between(dates, q_m["q05"], q_m["q95"], color="#378ADD", alpha=0.15, label="Q05–Q95")
    ax.fill_between(dates, q_m["q25"], q_m["q75"], color="#378ADD", alpha=0.30, label="Q25–Q75")
    ax.plot(dates, q_m["q50"], color="#185FA5", linewidth=1.6, label="Modèle Q50")

    obs_mask = ~np.isnan(obs_m)
    ax.scatter(dates[obs_mask & ~outlier_mask], obs_m[obs_mask & ~outlier_mask],
               color="#D85A30", s=18, label="Altimétrie d'origine", zorder=4)
    n_out, n_total = int((outlier_mask & obs_mask).sum()), int(obs_mask.sum())
    ax.scatter(dates[outlier_mask], obs_m[outlier_mask], color="#E24B4A", s=45, marker="x",
               linewidths=2, label=f"Hors [Q05,Q95] (n={n_out})", zorder=5)

    pct = f"{100*n_out/n_total:.1f}%" if n_total else "n/a"
    ax.set_title(f"{title} — {n_out}/{n_total} hors intervalle ({pct})", fontsize=10.5,
                fontweight="bold", loc="left")
    ax.set_ylabel("Hauteur d'eau (m)")
    ax.grid(True, linestyle="--", alpha=0.4)
    ax.legend(fontsize=8, loc="upper right")
    return n_out, n_total


def plot_single_cycle(model_label, mask, cycle, dates, obs_m, q_m, outlier_mask, out_dir):
    fig, ax = plt.subplots(figsize=(13, 5))
    draw_panel(ax, dates, obs_m, q_m, outlier_mask, f"Station {STATION} — DtoD{mask} — Cycle {cycle}")
    ax.set_xlabel("Date")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    fig.autofmt_xdate()
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"cycle_{cycle:02d}.png"
    fig.tight_layout()
    fig.savefig(out_path, dpi=140, bbox_inches="tight")
    plt.close(fig)
    return out_path


def plot_year_comparison(model_label, mask, results, mean_alti, std_alti, original_obs_z, ref_dates):
    dates_ref = pd.to_datetime(ref_dates)
    obs_m_full = to_meters(original_obs_z, mean_alti, std_alti)
    out_dir = ANTI_ROOT / model_label
    out_dir.mkdir(parents=True, exist_ok=True)

    for year in sorted(dates_ref.year.unique()):
        yr = dates_ref.year.values == year
        for group in CYCLE_GROUPS:
            cycles = [c for c in group if c in results]
            if not cycles:
                continue
            fig, axes = plt.subplots(len(cycles), 1, figsize=(12, 4 * len(cycles)), sharex=True, squeeze=False)
            fig.suptitle(f"Station {STATION} — DtoD{mask} — cycles {cycles[0]}-{cycles[-1]} — {year}\n"
                        f"(alti d'origine, non modifiée)", fontsize=13, fontweight="bold")

            for ax, cycle in zip(axes[:, 0], cycles):
                q_m = {q: to_meters(results[cycle][q][yr], mean_alti, std_alti) for q in QUANTILES}
                outlier = flag_outliers(obs_m_full[yr], q_m["q05"], q_m["q95"]) & ~np.isnan(obs_m_full[yr])
                label = "Cycle 0 (baseline)" if cycle == 0 else f"Cycle {cycle}"
                draw_panel(ax, dates_ref[yr].values, obs_m_full[yr], q_m, outlier, label)

            axes[-1, 0].set_xlabel("Date")
            axes[-1, 0].xaxis.set_major_formatter(mdates.DateFormatter("%b"))
            axes[-1, 0].xaxis.set_major_locator(mdates.MonthLocator())
            out_path = out_dir / f"{year}_cycles_{cycles[0]}-{cycles[-1]}.png"
            fig.tight_layout(rect=[0, 0, 1, 0.95])
            fig.savefig(out_path, dpi=140, bbox_inches="tight")
            plt.close(fig)
            print(f"    → {out_path}")


# ═══════════════════════════════════════════════════════════════
# BOUCLE PRINCIPALE
# ═══════════════════════════════════════════════════════════════
mean_alti, std_alti = get_alti_stats(STATION)
if mean_alti is None:
    raise SystemExit(f"Stats alti introuvables pour la station {STATION}.")
print(f"Station {STATION} : mean_alti={mean_alti:.3f}  std_alti={std_alti:.3f}  mode={CORRECTION_MODE}")

log_rows = []

for model_label, info in MODELS.items():
    epoch, mask = info["epoch"], info["mask"]
    run_dir = RUNS_DIR / model_label
    if not run_dir.exists():
        print(f"⚠ Run introuvable : {run_dir} — ignoré")
        continue
    print(f"\n{'='*60}\n  DtoD{mask} ({model_label}, epoch {epoch})\n{'='*60}")

    work_dir, station_nc, stations_txt = setup_work_nc(model_label)
    model_out_dir = OUTPUT_ROOT / model_label

    results = {}          # {cycle: {q05:.., q25:.., ... q95:..}}, alignés sur ref_dates
    ref_dates = None
    original_obs_z = None

    for cycle in range(N_CYCLES + 1):
        dates, obs_z, pred_z, q_arrs = run_evaluation(run_dir, epoch, work_dir, stations_txt)

        if cycle == 0:
            ref_dates = dates
            original_obs_z = obs_z.copy()

        q_aligned = {q: align_to_dates(dates, q_arrs[q], ref_dates) for q in QUANTILES}
        pred_aligned = align_to_dates(dates, pred_z, ref_dates)
        results[cycle] = q_aligned

        # Outliers sur l'alti D'ORIGINE (fixe) vs bandes du cycle courant (affichage/diagnostic)
        outlier_plot = flag_outliers(original_obs_z, q_aligned["q05"], q_aligned["q95"]) & ~np.isnan(original_obs_z)
        n_out, n_total = int(outlier_plot.sum()), int((~np.isnan(original_obs_z)).sum())
        sharpness = float(np.nanmean(q_aligned["q95"] - q_aligned["q05"]))
        print(f"  Cycle {cycle} : {n_out}/{n_total} outliers ({100*n_out/n_total:.1f}%)  "
              f"| largeur Q05-Q95 = {sharpness:.4f}")

        obs_m = to_meters(original_obs_z, mean_alti, std_alti)
        q_m = {q: to_meters(q_aligned[q], mean_alti, std_alti) for q in QUANTILES}
        out_path = plot_single_cycle(model_label, mask, cycle, ref_dates, obs_m, q_m, outlier_plot, model_out_dir)
        print(f"    Plot → {out_path}")

        log_rows.append({"model": model_label, "mask_pct": mask, "cycle": cycle,
                         "correction_mode": CORRECTION_MODE, "n_outliers": n_out,
                         "n_total": n_total, "pct_outliers": round(100*n_out/n_total, 2),
                         "sharpness_q05_q95": round(sharpness, 4)})

        # Correction pour le cycle suivant, basée sur l'état COURANT du .nc (pas l'origine)
        if cycle < N_CYCLES:
            obs_mask_live = ~np.isnan(obs_z)
            outlier_live = flag_outliers(obs_z, q_arrs["q05"], q_arrs["q95"]) & obs_mask_live
            if outlier_live.sum() > 0:
                if CORRECTION_MODE == "mask_nan":
                    updates = {pd.Timestamp(d): np.nan for d, f in zip(dates, outlier_live) if f}
                    action = "masqué(s) (NaN)"
                else:
                    updates = {pd.Timestamp(d): float(p) for d, p, f in zip(dates, pred_z, outlier_live) if f}
                    action = "remplacé(s) par Q50"
                n_upd = update_nc(station_nc, updates)
                print(f"    → {n_upd} point(s) {action} pour le cycle {cycle + 1}")

    print(f"\n  Comparaisons par année -> {ANTI_ROOT / model_label}/")
    plot_year_comparison(model_label, mask, results, mean_alti, std_alti, original_obs_z, ref_dates)

# ═══════════════════════════════════════════════════════════════
# RÉSUMÉ
# ═══════════════════════════════════════════════════════════════
df_log = pd.DataFrame(log_rows)
log_csv = OUTPUT_ROOT / "log_cycles.csv"
df_log.to_csv(log_csv, index=False)

print(f"\n{'='*60}\n  RÉSUMÉ\n{'='*60}")
for model_label in MODELS:
    sub = df_log[df_log["model"] == model_label].sort_values("cycle")
    if sub.empty:
        continue
    print(f"\n  {model_label} :")
    for _, r in sub.iterrows():
        print(f"    cycle {int(r['cycle'])}: {int(r['n_outliers'])}/{int(r['n_total'])} "
              f"({r['pct_outliers']:.1f}%)  | largeur = {r['sharpness_q05_q95']:.4f}")

    sv = sub["sharpness_q05_q95"].values
    if len(sv) >= 2 and sv[0] > 0 and 100 * (1 - sv[-1] / sv[0]) > 30:
        print(f"    ⚠ Largeur d'intervalle réduite de {100*(1 - sv[-1]/sv[0]):.0f}% "
              f"entre cycle 0 et {int(sub['cycle'].max())} — signature d'un collapse de variance.")

print(f"\nLog complet -> {log_csv}")
print(f"Original jamais modifié : {NC_DIR / f'{STATION}.nc'}")
print("Done")