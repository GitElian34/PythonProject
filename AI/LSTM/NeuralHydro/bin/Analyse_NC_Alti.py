#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
diagnostic_nc_satellite.py
═══════════════════════════════════════════════════════════════════════════
Diagnostic complet des fichiers .nc satellite et attributes.csv

Vérifie :
  1. Cohérence des fichiers : tous les .nc ont les mêmes variables ?
  2. Variables dynamiques : NaN, valeurs aberrantes, stats par variable
  3. Variables statiques (attributes.csv) : NaN, distributions, cohérence
  4. Cohérence croisée : stations dans .nc vs attributes.csv
  5. Séries temporelles : durée, nb mesures, régularité

Sortie :
  - Rapport console complet
  - ./Exploring_data/Diagnostic_NC_satellite/diagnostic_resume.csv
  - ./Exploring_data/Diagnostic_NC_satellite/diagnostic_dynamiques.png
  - ./Exploring_data/Diagnostic_NC_satellite/diagnostic_statiques.png
═══════════════════════════════════════════════════════════════════════════
"""

import os
import numpy as np
import pandas as pd
import xarray as xr
import matplotlib.pyplot as plt
from pathlib import Path
from collections import Counter
import warnings
warnings.filterwarnings('ignore')

# ─── Config ─────────────────────────────────────────────────────────────────
NC_DIR   = "./data/IA/NeuralHydrology_satellite/time_series/"
ATTR_CSV = "./data/IA/NeuralHydrology_satellite/attributes/attributes.csv"
OUT_DIR  = Path("./Exploring_data/Diagnostic_NC_satellite")
OUT_DIR.mkdir(parents=True, exist_ok=True)


# ═══════════════════════════════════════════════════════════════
# 1. INVENTAIRE DES FICHIERS .NC
# ═══════════════════════════════════════════════════════════════
def inventaire_nc():
    print("=" * 70)
    print("1. INVENTAIRE DES FICHIERS .NC")
    print("=" * 70)

    nc_files = sorted([f for f in os.listdir(NC_DIR) if f.endswith('.nc')])
    station_ids = [f.replace('.nc', '') for f in nc_files]

    print(f"\n  Nombre de fichiers .nc : {len(nc_files)}")

    if len(nc_files) == 0:
        print("  ❌ Aucun fichier .nc trouvé !")
        return [], []

    # Vérifier que tous les .nc ont les mêmes variables
    vars_par_fichier = {}
    for f in nc_files:
        ds = xr.open_dataset(os.path.join(NC_DIR, f))
        vars_par_fichier[f] = set(ds.data_vars)
        ds.close()

    all_var_sets = list(vars_par_fichier.values())
    vars_reference = all_var_sets[0]

    fichiers_differents = []
    for f, vs in vars_par_fichier.items():
        if vs != vars_reference:
            fichiers_differents.append((f, vs))

    if not fichiers_differents:
        print(f"  ✅ Tous les .nc ont les mêmes {len(vars_reference)} variables :")
        for v in sorted(vars_reference):
            print(f"      - {v}")
    else:
        print(f"  ⚠️  {len(fichiers_differents)} fichiers avec des variables différentes !")
        # Variables de référence (les plus communes)
        all_vars_flat = [v for vs in all_var_sets for v in vs]
        var_counts = Counter(all_vars_flat)
        print(f"  Variables et leur fréquence :")
        for v, c in var_counts.most_common():
            marker = " ◄ MANQUANTE DANS CERTAINS" if c < len(nc_files) else ""
            print(f"      {v} : {c}/{len(nc_files)}{marker}")

        print(f"\n  Fichiers avec variables manquantes :")
        for f, vs in fichiers_differents[:10]:
            missing = vars_reference - vs
            extra = vs - vars_reference
            if missing:
                print(f"    {f} : manque {missing}")
            if extra:
                print(f"    {f} : extra {extra}")

    return nc_files, station_ids


# ═══════════════════════════════════════════════════════════════
# 2. DIAGNOSTIC DES VARIABLES DYNAMIQUES
# ═══════════════════════════════════════════════════════════════
def diagnostic_dynamiques(nc_files):
    print("\n" + "=" * 70)
    print("2. DIAGNOSTIC DES VARIABLES DYNAMIQUES")
    print("=" * 70)

    # Collecter les stats par variable et par station
    all_stats = []

    for f in nc_files:
        station_id = f.replace('.nc', '')
        ds = xr.open_dataset(os.path.join(NC_DIR, f))

        row = {'station_id': station_id}
        row['nb_dates'] = len(ds.date)
        row['date_min'] = pd.Timestamp(ds.date.values[0]).strftime('%Y-%m-%d')
        row['date_max'] = pd.Timestamp(ds.date.values[-1]).strftime('%Y-%m-%d')

        # Intervalles entre mesures
        dates = pd.to_datetime(ds.date.values)
        if len(dates) > 1:
            intervals = np.diff(dates).astype('timedelta64[D]').astype(int)
            row['interval_mean'] = intervals.mean()
            row['interval_std'] = intervals.std()
            row['interval_min'] = intervals.min()
            row['interval_max'] = intervals.max()
        else:
            row['interval_mean'] = np.nan
            row['interval_std'] = np.nan
            row['interval_min'] = np.nan
            row['interval_max'] = np.nan

        for var in ds.data_vars:
            vals = ds[var].values
            n_total = len(vals)
            n_nan = np.isnan(vals).sum()
            n_valid = n_total - n_nan

            row[f'{var}_n'] = n_total
            row[f'{var}_nan'] = n_nan
            row[f'{var}_nan_pct'] = round(n_nan / n_total * 100, 1) if n_total > 0 else 0

            if n_valid > 0:
                valid = vals[~np.isnan(vals)]
                row[f'{var}_mean'] = valid.mean()
                row[f'{var}_std'] = valid.std()
                row[f'{var}_min'] = valid.min()
                row[f'{var}_max'] = valid.max()
                row[f'{var}_p01'] = np.percentile(valid, 1)
                row[f'{var}_p99'] = np.percentile(valid, 99)
            else:
                for s in ['mean', 'std', 'min', 'max', 'p01', 'p99']:
                    row[f'{var}_{s}'] = np.nan

        ds.close()
        all_stats.append(row)

    df_stats = pd.DataFrame(all_stats)

    # ── Résumé global par variable ──────────────────────────────────────────
    ds_ref = xr.open_dataset(os.path.join(NC_DIR, nc_files[0]))
    variables = list(ds_ref.data_vars)
    ds_ref.close()

    print(f"\n  {'Variable':<22} {'NaN moy%':>9} {'NaN max%':>9} "
          f"{'Mean':>10} {'Std':>10} {'Min':>10} {'Max':>10}")
    print("  " + "─" * 82)

    suspect_stations = {}

    for var in variables:
        nan_pcts = df_stats[f'{var}_nan_pct']
        means = df_stats[f'{var}_mean']
        stds = df_stats[f'{var}_std']
        mins = df_stats[f'{var}_min']
        maxs = df_stats[f'{var}_max']

        flag = ""
        # Détecter les stations avec 100% NaN
        full_nan = df_stats[nan_pcts >= 100]['station_id'].tolist()
        if full_nan:
            flag = f" ⚠️ {len(full_nan)} stations 100% NaN"
            suspect_stations[var] = full_nan

        # Détecter les valeurs aberrantes (>5 std de la moyenne globale)
        global_mean = means.mean()
        global_std = means.std()
        if global_std > 0:
            outlier_mask = (means - global_mean).abs() > 5 * global_std
            outliers = df_stats[outlier_mask]['station_id'].tolist()
            if outliers:
                flag += f" | {len(outliers)} outliers"

        print(f"  {var:<22} {nan_pcts.mean():>8.1f}% {nan_pcts.max():>8.1f}% "
              f"{means.median():>10.3f} {stds.median():>10.3f} "
              f"{mins.min():>10.3f} {maxs.max():>10.3f}{flag}")

    # ── Détail des stations suspectes ───────────────────────────────────────
    if suspect_stations:
        print(f"\n  ⚠️  Stations avec variables 100% NaN :")
        for var, stations in suspect_stations.items():
            print(f"    {var} : {', '.join(stations[:10])}")
            if len(stations) > 10:
                print(f"      ... et {len(stations) - 10} autres")

    # ── Distribution temporelle ─────────────────────────────────────────────
    print(f"\n  Séries temporelles :")
    print(f"    Nb mesures  : médiane={df_stats['nb_dates'].median():.0f}, "
          f"min={df_stats['nb_dates'].min()}, max={df_stats['nb_dates'].max()}")
    print(f"    Date début  : {df_stats['date_min'].min()} → {df_stats['date_min'].max()}")
    print(f"    Date fin    : {df_stats['date_max'].min()} → {df_stats['date_max'].max()}")
    print(f"    Intervalle  : médiane={df_stats['interval_mean'].median():.1f}j, "
          f"max moyen={df_stats['interval_mean'].max():.1f}j")

    return df_stats, variables


# ═══════════════════════════════════════════════════════════════
# 3. DIAGNOSTIC DES ATTRIBUTS STATIQUES
# ═══════════════════════════════════════════════════════════════
def diagnostic_statiques(station_ids):
    print("\n" + "=" * 70)
    print("3. DIAGNOSTIC DES ATTRIBUTS STATIQUES (attributes.csv)")
    print("=" * 70)

    if not os.path.exists(ATTR_CSV):
        print(f"  ❌ Fichier {ATTR_CSV} non trouvé !")
        return None

    attrs = pd.read_csv(ATTR_CSV, dtype={'station_id': str})
    print(f"\n  Shape : {attrs.shape}")
    print(f"  Colonnes : {list(attrs.columns)}")

    # ── NaN par colonne ─────────────────────────────────────────────────────
    print(f"\n  {'Attribut':<22} {'NaN':>5} {'NaN%':>6} "
          f"{'Mean':>10} {'Std':>10} {'Min':>10} {'Max':>10}")
    print("  " + "─" * 72)

    for col in attrs.columns:
        if col == 'station_id':
            continue
        n_nan = attrs[col].isna().sum()
        pct_nan = n_nan / len(attrs) * 100

        if attrs[col].dtype in [np.float64, np.int64, float, int]:
            valid = attrs[col].dropna()
            print(f"  {col:<22} {n_nan:>5} {pct_nan:>5.1f}% "
                  f"{valid.mean():>10.3f} {valid.std():>10.3f} "
                  f"{valid.min():>10.3f} {valid.max():>10.3f}")
        else:
            print(f"  {col:<22} {n_nan:>5} {pct_nan:>5.1f}%  (non numérique)")

    # ── Cohérence des fractions CORINE ──────────────────────────────────────
    frac_cols = [c for c in attrs.columns if c.startswith('frac_')]
    if frac_cols:
        frac_sum = attrs[frac_cols].sum(axis=1)
        print(f"\n  Somme fractions CORINE :")
        print(f"    Médiane : {frac_sum.median():.4f}")
        print(f"    Min     : {frac_sum.min():.4f}")
        print(f"    Max     : {frac_sum.max():.4f}")
        bad_sum = ((frac_sum < 0.95) | (frac_sum > 1.05)).sum()
        if bad_sum > 0:
            print(f"    ⚠️  {bad_sum} stations avec somme hors [0.95, 1.05]")
        else:
            print(f"    ✅ Toutes les sommes dans [0.95, 1.05]")

    # ── Cohérence des textures sol ──────────────────────────────────────────
    soil_cols = [c for c in attrs.columns if c.startswith('sg_')]
    if soil_cols:
        soil_sum = attrs[soil_cols].sum(axis=1)
        print(f"\n  Somme textures sol (clay+sand+silt) :")
        print(f"    Médiane : {soil_sum.median():.1f}")
        print(f"    Min     : {soil_sum.min():.1f}")
        print(f"    Max     : {soil_sum.max():.1f}")

    # ── Cohérence croisée .nc vs attributes ─────────────────────────────────
    print(f"\n  Cohérence croisée :")
    nc_set = set(station_ids)
    attr_set = set(attrs['station_id'].astype(str))

    in_nc_not_attr = nc_set - attr_set
    in_attr_not_nc = attr_set - nc_set

    if not in_nc_not_attr and not in_attr_not_nc:
        print(f"    ✅ Parfaite correspondance ({len(nc_set)} stations)")
    else:
        if in_nc_not_attr:
            print(f"    ⚠️  {len(in_nc_not_attr)} stations dans .nc mais PAS dans attributes.csv :")
            for s in sorted(in_nc_not_attr)[:10]:
                print(f"      {s}")
        if in_attr_not_nc:
            print(f"    ⚠️  {len(in_attr_not_nc)} stations dans attributes.csv mais PAS de .nc :")
            for s in sorted(in_attr_not_nc)[:10]:
                print(f"      {s}")

    return attrs


# ═══════════════════════════════════════════════════════════════
# 4. FIGURES DE DIAGNOSTIC
# ═══════════════════════════════════════════════════════════════
def plot_diagnostic_dynamiques(df_stats, variables):
    print("\n" + "=" * 70)
    print("4. FIGURES DE DIAGNOSTIC — DYNAMIQUES")
    print("=" * 70)

    # Exclure water_level pour les plots de NaN (c'est la cible)
    vars_input = [v for v in variables if v != 'water_level']

    n_vars = len(vars_input)
    n_cols = 3
    n_rows = (n_vars + n_cols - 1) // n_cols

    fig, axes = plt.subplots(n_rows, n_cols, figsize=(16, 4 * n_rows))
    axes = axes.flatten()
    fig.suptitle("Distribution des variables dynamiques (toutes stations)", fontsize=14, fontweight='bold')

    for idx, var in enumerate(vars_input):
        ax = axes[idx]
        col_mean = f'{var}_mean'
        if col_mean in df_stats.columns:
            vals = df_stats[col_mean].dropna()
            ax.hist(vals, bins=30, color='#2196F3', edgecolor='white', alpha=0.8)
            ax.set_title(var, fontsize=10)
            ax.set_ylabel("Nb stations")
            # Marquer la médiane
            med = vals.median()
            ax.axvline(med, color='red', linestyle='--', linewidth=1,
                       label=f'méd={med:.3f}')
            ax.legend(fontsize=7)

    # Masquer les axes vides
    for idx in range(n_vars, len(axes)):
        axes[idx].set_visible(False)

    plt.tight_layout()
    plt.savefig(OUT_DIR / "diagnostic_dynamiques.png", dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  → {OUT_DIR / 'diagnostic_dynamiques.png'}")

    # ── Figure NaN par variable et par station ──────────────────────────────
    fig, ax = plt.subplots(figsize=(14, 6))
    nan_data = []
    for var in variables:
        col = f'{var}_nan_pct'
        if col in df_stats.columns:
            nan_data.append(df_stats[col].values)

    bp = ax.boxplot(nan_data, labels=variables, vert=True, patch_artist=True)
    for patch in bp['boxes']:
        patch.set_facecolor('#FF9800')
        patch.set_alpha(0.6)
    ax.set_ylabel("% NaN")
    ax.set_title("Taux de NaN par variable (distribution sur les stations)")
    ax.tick_params(axis='x', rotation=45)
    ax.grid(True, alpha=0.3, axis='y')

    plt.tight_layout()
    plt.savefig(OUT_DIR / "diagnostic_nan_par_variable.png", dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  → {OUT_DIR / 'diagnostic_nan_par_variable.png'}")


def plot_diagnostic_statiques(attrs):
    if attrs is None:
        return

    print("\n" + "=" * 70)
    print("5. FIGURES DE DIAGNOSTIC — STATIQUES")
    print("=" * 70)

    num_cols = [c for c in attrs.columns
                if c != 'station_id' and attrs[c].dtype in [np.float64, np.int64, float, int]]

    n_vars = len(num_cols)
    n_cols_fig = 3
    n_rows = (n_vars + n_cols_fig - 1) // n_cols_fig

    fig, axes = plt.subplots(n_rows, n_cols_fig, figsize=(16, 4 * n_rows))
    axes = axes.flatten()
    fig.suptitle("Distribution des attributs statiques (toutes stations)", fontsize=14, fontweight='bold')

    for idx, col in enumerate(num_cols):
        ax = axes[idx]
        vals = attrs[col].dropna()
        ax.hist(vals, bins=30, color='#4CAF50', edgecolor='white', alpha=0.8)
        ax.set_title(col, fontsize=10)
        n_nan = attrs[col].isna().sum()
        if n_nan > 0:
            ax.set_title(f"{col} ({n_nan} NaN)", fontsize=10, color='red')
        med = vals.median()
        ax.axvline(med, color='red', linestyle='--', linewidth=1,
                   label=f'méd={med:.2f}')
        ax.legend(fontsize=7)

    for idx in range(n_vars, len(axes)):
        axes[idx].set_visible(False)

    plt.tight_layout()
    plt.savefig(OUT_DIR / "diagnostic_statiques.png", dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  → {OUT_DIR / 'diagnostic_statiques.png'}")


# ═══════════════════════════════════════════════════════════════
# 6. EXPORT CSV RÉSUMÉ
# ═══════════════════════════════════════════════════════════════
def export_resume(df_stats):
    # Colonnes essentielles pour le résumé
    cols_resume = ['station_id', 'nb_dates', 'date_min', 'date_max',
                   'interval_mean', 'interval_max']

    # Ajouter NaN% par variable
    ds_ref = xr.open_dataset(os.path.join(NC_DIR, os.listdir(NC_DIR)[0]))
    for var in ds_ref.data_vars:
        cols_resume.append(f'{var}_nan_pct')
    ds_ref.close()

    cols_present = [c for c in cols_resume if c in df_stats.columns]
    df_resume = df_stats[cols_present].copy()
    csv_path = OUT_DIR / "diagnostic_resume.csv"
    df_resume.to_csv(csv_path, index=False, float_format='%.2f')
    print(f"\n  → Résumé exporté dans {csv_path}")


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("╔══════════════════════════════════════════════════════════════╗")
    print("║  DIAGNOSTIC DES .NC SATELLITE + ATTRIBUTES                  ║")
    print("╚══════════════════════════════════════════════════════════════╝\n")

    # 1. Inventaire
    nc_files, station_ids = inventaire_nc()
    if not nc_files:
        exit()

    # 2. Dynamiques
    df_stats, variables = diagnostic_dynamiques(nc_files)

    # 3. Statiques
    attrs = diagnostic_statiques(station_ids)

    # 4. Figures dynamiques
    plot_diagnostic_dynamiques(df_stats, variables)

    # 5. Figures statiques
    plot_diagnostic_statiques(attrs)

    # 6. Export résumé
    export_resume(df_stats)

    print("\n" + "=" * 70)
    print("✅ Diagnostic terminé.")
    print("=" * 70)