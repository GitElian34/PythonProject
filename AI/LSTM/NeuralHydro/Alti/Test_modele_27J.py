"""
zeroshot_eval_27j.py
═══════════════════════════════════════════════════════════════════════════
Ré-évalue le modèle 27j en zero-shot sur les stations satellite
(avec les .nc regénérés), puis extrait les résidus et génère les plots.

Étapes :
  1. Backup du config.yml
  2. Modifier le config pour pointer vers le dataset satellite 27D
  3. Lancer nh-run evaluate
  4. Restaurer le config original
  5. Extraire les résidus + détecter outliers
  6. Plots par année (toutes les années, titre adapté si outlier)

Usage :
    python zeroshot_eval_27j.py                # évalue + plot
    python zeroshot_eval_27j.py --skip_eval    # skip l'éval, plot seulement
═══════════════════════════════════════════════════════════════════════════
"""

import argparse
import shutil
import subprocess
import pickle
import yaml
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from pathlib import Path

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
RUN_DIR        = Path("./runs/arlstm_feat27jHigh_modele2_2205_152119")
EPOCH          = 5
PERIOD         = "validation"
SAT_DATA_DIR   = "data/IA/NeuralHydrology_satellite_27D"
SAT_BASIN_FILE = "AI/LSTM/NeuralHydro_satellite_27D/stations_27j.txt"
TARGET_VAR     = "water_level"

MODEL_NAME     = RUN_DIR.name
CONFIG_PATH    = RUN_DIR / "config.yml"
CONFIG_BACKUP  = RUN_DIR / "config_BACKUP.yml"
RESULTS_P      = RUN_DIR / PERIOD / f"model_epoch{EPOCH:03d}" / f"{PERIOD}_results.p"

OUT_CSV        = Path("./data/outlier_detection/residuals_27j_all_stations.csv")
PLOT_DIR       = Path(f"./figures_zeroshot_satellite/{MODEL_NAME}/Outlier_27j")
OUT_CSV.parent.mkdir(parents=True, exist_ok=True)

OUTLIER_THRESHOLD = 3.0


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip_eval", action="store_true",
                        help="Skip l'évaluation, utilise le results.p existant")
    return parser.parse_args()


# ═══════════════════════════════════════════════════════════════
# PARTIE 1 — ÉVALUATION ZERO-SHOT
# ═══════════════════════════════════════════════════════════════

def run_evaluation():
    print("=" * 60)
    print("ZERO-SHOT ÉVALUATION SUR STATIONS SATELLITE 27D")
    print("=" * 60)

    # 1. Backup
    if not CONFIG_BACKUP.exists():
        shutil.copy(CONFIG_PATH, CONFIG_BACKUP)
        print(f"✅ Config sauvegardé : {CONFIG_BACKUP}")
    else:
        print(f"⚠️  Backup existe déjà : {CONFIG_BACKUP}")

    # 2. Modifier le config
    with open(CONFIG_PATH) as f:
        cfg = yaml.safe_load(f)

    orig_data_dir    = cfg.get('data_dir')
    orig_val_file    = cfg.get('validation_basin_file')
    orig_val_start   = cfg.get('validation_start_date')
    orig_val_end     = cfg.get('validation_end_date')

    cfg['data_dir']                = SAT_DATA_DIR
    cfg['validation_basin_file']   = SAT_BASIN_FILE
    cfg['validation_start_date']   = '01/01/2016'
    cfg['validation_end_date']     = '31/12/2025'

    with open(CONFIG_PATH, 'w') as f:
        yaml.dump(cfg, f)

    print(f"\n📝 Config modifié :")
    print(f"   data_dir              : {orig_data_dir} → {SAT_DATA_DIR}")
    print(f"   validation_basin_file : {orig_val_file} → {SAT_BASIN_FILE}")

    # 3. Lancer l'évaluation
    print(f"\n🚀 Lancement nh-run evaluate (epoch {EPOCH}, period {PERIOD})...")

    cmd = ["nh-run", "evaluate",
           "--run-dir", str(RUN_DIR),
           "--epoch", str(EPOCH),
           "--period", PERIOD]

    eval_ok = False
    try:
        subprocess.run(cmd, check=True)
        eval_ok = True
        print("\n✅ Évaluation terminée")
    except subprocess.CalledProcessError as e:
        print(f"\n❌ Erreur évaluation : {e}")
    finally:
        # 4. Restaurer le config (toujours)
        shutil.copy(CONFIG_BACKUP, CONFIG_PATH)
        print(f"✅ Config original restauré")

    if not eval_ok:
        print("Arrêt : l'évaluation a échoué.")
        exit(1)


# ═══════════════════════════════════════════════════════════════
# PARTIE 2 — EXTRACTION RÉSIDUS
# ═══════════════════════════════════════════════════════════════

def extract_residuals():
    print(f"\n{'='*60}")
    print("EXTRACTION RÉSIDUS")
    print(f"{'='*60}")

    if not RESULTS_P.exists():
        print(f"❌ Pas de résultats : {RESULTS_P}")
        exit(1)

    print(f"Chargement de {RESULTS_P}...")
    with open(RESULTS_P, 'rb') as f:
        results = pickle.load(f)

    rows = []
    for sid, sub in results.items():
        try:
            freq = list(sub.keys())[0]
            ds   = sub[freq]['xr']
            obs_var = f"{TARGET_VAR}_obs"
            sim_var = f"{TARGET_VAR}_sim"
            if obs_var not in ds or sim_var not in ds:
                continue

            dates = pd.to_datetime(ds.date.values)
            obs   = ds[obs_var].values.flatten()
            pred  = ds[sim_var].values.flatten()

            for d, o, p in zip(dates, obs, pred):
                rows.append({
                    'station':  str(sid),
                    'date':     d,
                    'obs':      o,
                    'pred':     p,
                    'residual': o - p if not (np.isnan(o) or np.isnan(p)) else np.nan,
                })
        except Exception as e:
            print(f"  ⚠  {sid} : {e}")

    df = pd.DataFrame(rows)
    df['date'] = pd.to_datetime(df['date'])

    # Normalisation du résidu par station
    def norm_residuals(grp):
        std = np.nanstd(grp['residual'])
        grp['residual_norm'] = grp['residual'] / std if std > 0 else np.nan
        return grp

    df = df.groupby('station', group_keys=False).apply(norm_residuals)
    df['is_outlier'] = df['residual_norm'].abs() > OUTLIER_THRESHOLD
    df['year']       = df['date'].dt.year

    df.to_csv(OUT_CSV, index=False)
    print(f"\n✅ {len(df)} lignes → {OUT_CSV}")
    print(f"   {df['station'].nunique()} stations")
    print(f"   {df['is_outlier'].sum()} outliers détectés "
          f"({df['is_outlier'].mean()*100:.1f}%)")

    return df, results


# ═══════════════════════════════════════════════════════════════
# PARTIE 3 — PLOTS PAR ANNÉE (TOUTES LES ANNÉES)
# ═══════════════════════════════════════════════════════════════

def generate_plots(df, results):
    print(f"\n{'='*60}")
    print("GÉNÉRATION DES PLOTS")
    print(f"{'='*60}")

    # ── Métriques par station ────────────────────────────────
    metrics_path = RUN_DIR / PERIOD / f"model_epoch{EPOCH:03d}" / f"{PERIOD}_metrics.csv"
    df_metrics = None
    if metrics_path.exists():
        df_metrics = pd.read_csv(metrics_path, header=None, names=["station", "NSE", "KGE"])
        df_metrics["NSE"] = pd.to_numeric(df_metrics["NSE"], errors="coerce")
        df_metrics["KGE"] = pd.to_numeric(df_metrics["KGE"], errors="coerce")
        df_metrics = df_metrics.set_index("station")

        print(f"\nMédiane NSE : {df_metrics['NSE'].median():.3f}")
        print(f"Médiane KGE : {df_metrics['KGE'].median():.3f}")
        print(f"Stations NSE > 0.5 : {(df_metrics['NSE'] > 0.5).sum()}")

    stations = sorted(df['station'].unique())
    print(f"\n📊 {len(stations)} stations\n")
    n_plots = 0

    for sta in stations:
        grp      = df[df['station'] == sta].sort_values('date')
        outliers = grp[grp['is_outlier']]
        all_years = sorted(grp['year'].unique())
        years_with_outliers = set(outliers['year'].unique())

        if len(all_years) == 0:
            continue

        sta_dir = PLOT_DIR / sta
        sta_dir.mkdir(parents=True, exist_ok=True)

        # NSE de la station
        nse = np.nan
        if df_metrics is not None and sta in df_metrics.index:
            nse = df_metrics.loc[sta, 'NSE']

        for year in all_years:
            grp_year = grp[grp['year'] == year]
            out_year = outliers[outliers['year'] == year]
            has_outliers = year in years_with_outliers
            n_out = len(out_year)

            fig, ax = plt.subplots(figsize=(12, 4))

            ax.plot(grp_year['date'], grp_year['obs'], '-o', color='#5B9BD5',
                    markersize=5, lw=1, label='Observé', zorder=3)
            ax.plot(grp_year['date'], grp_year['pred'], '-o', color='#E88B8B',
                    markersize=5, lw=1, label='Prédit', zorder=2)

            for _, row in out_year.iterrows():
                ax.plot([row['date'], row['date']], [row['obs'], row['pred']],
                        color='red', lw=2, alpha=0.7, zorder=4)
                ax.scatter(row['date'], row['obs'], s=150, facecolors='none',
                           edgecolors='red', lw=2, zorder=5)
                ax.annotate(f"{row['residual_norm']:+.1f}σ",
                            xy=(row['date'], row['obs']),
                            xytext=(0, 12 if row['residual'] > 0 else -14),
                            textcoords='offset points',
                            fontsize=9, color='red', fontweight='bold',
                            ha='center',
                            va='bottom' if row['residual'] > 0 else 'top')

            if has_outliers:
                title = (f"Station {sta}  —  {year}  —  NSE={nse:.3f}  —  "
                         f"⚠ {n_out} OUTLIER{'S' if n_out > 1 else ''}")
                title_color = "red"
            else:
                title = f"Station {sta}  —  {year}  —  NSE={nse:.3f}  —  ✓ Aucun outlier"
                title_color = "green"

            ax.set_title(title, fontsize=11, fontweight='bold', color=title_color)
            ax.set_ylabel('Water level (z-score)')
            ax.set_xlabel('Date')
            ax.legend(loc='upper right', fontsize=8)
            ax.grid(True, alpha=0.3, ls='--')
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%b'))
            ax.xaxis.set_major_locator(mdates.MonthLocator())
            ax.axhline(0, color='grey', lw=0.5, ls='--')

            plt.tight_layout()
            fig.savefig(sta_dir / f"outlier_{sta}_{year}.png", dpi=150, bbox_inches='tight')
            plt.close(fig)
            n_plots += 1

        n_out_total = len(outliers)
        print(f"  {sta:>15s} | {n_out_total:2d} outliers | "
              f"{len(all_years)} années | NSE={nse:.3f}")

    # ── Histogramme métriques ────────────────────────────────
    if df_metrics is not None:
        fig, axes = plt.subplots(1, 2, figsize=(13, 4.5))

        axes[0].hist(df_metrics["NSE"].dropna(), bins=20, color="steelblue", edgecolor="white")
        axes[0].axvline(df_metrics["NSE"].median(), color="red", lw=2, ls="--",
                        label=f"Médiane = {df_metrics['NSE'].median():.2f}")
        axes[0].axvline(0, color="gray", lw=1, ls=":")
        axes[0].set_xlabel("NSE"); axes[0].set_ylabel("Nb stations")
        axes[0].set_title(f"Distribution NSE — Zero-shot satellite 27j (n={len(df_metrics)})")
        axes[0].legend(); axes[0].grid(True, alpha=0.3)

        axes[1].hist(df_metrics["KGE"].dropna(), bins=20, color="forestgreen", edgecolor="white")
        axes[1].axvline(df_metrics["KGE"].median(), color="red", lw=2, ls="--",
                        label=f"Médiane = {df_metrics['KGE'].median():.2f}")
        axes[1].axvline(0, color="gray", lw=1, ls=":")
        axes[1].set_xlabel("KGE"); axes[1].set_ylabel("Nb stations")
        axes[1].set_title("Distribution KGE")
        axes[1].legend(); axes[1].grid(True, alpha=0.3)

        plt.tight_layout()
        plt.savefig(PLOT_DIR / "_distribution_metrics.png", dpi=120)
        plt.close()

    print(f"\n✅ {n_plots} figures dans {PLOT_DIR}")


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    args = parse_args()

    if not args.skip_eval:
        run_evaluation()

    df, results = extract_residuals()
    generate_plots(df, results)