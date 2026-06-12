"""
generate_sigma0_plots_27j.py
═══════════════════════════════════════════════════════════════════════════
Génère un plot WSH vs date coloré par sigma0 par station satellite 27j.
Sauvegardé dans le même dossier que les cartes et outliers :
  ./figures_zeroshot_satellite/<MODEL>/Outlier_27j/<station>/sigma0_<station>.png
═══════════════════════════════════════════════════════════════════════════
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.cm as cm
import matplotlib.colors as mcolors
from pathlib import Path

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
MODEL  = "arlstm_feat27jHigh_modele2_0206_145147"

SIGMA0_CSV = "./data/sigma0/sigma0_all_stations.csv"
OUT_PLOTS  = Path(f"./figures_zeroshot_satellite/{MODEL}/Outlier_27j")

SIGMA0_SEUIL = 30.0

# ═══════════════════════════════════════════════════════════════
# CHARGEMENT
# ═══════════════════════════════════════════════════════════════
print("Chargement sigma0...")
df = pd.read_csv(SIGMA0_CSV, parse_dates=['date'])
df = df.dropna(subset=['WSH', 'sigma0'])
df['station_code'] = df['station_code'].astype(str)
print(f"{len(df)} mesures  |  {df['station_code'].nunique()} stations")

stations = sorted(df['station_code'].unique())

# Colormap commune sur toutes les stations
sigma0_min = df['sigma0'].quantile(0.02)
sigma0_max = df['sigma0'].quantile(0.98)
norm       = mcolors.Normalize(vmin=sigma0_min, vmax=sigma0_max)
cmap       = cm.RdYlGn

# ═══════════════════════════════════════════════════════════════
# FIGURE PAR STATION
# ═══════════════════════════════════════════════════════════════
print("Génération des figures...\n")
n_plots = 0

for sid in stations:
    sub = df[df['station_code'] == sid].sort_values('date')
    if sub.empty:
        continue

    sta_dir = OUT_PLOTS / sid
    sta_dir.mkdir(parents=True, exist_ok=True)

    fig, axes = plt.subplots(2, 1, figsize=(14, 7),
                             gridspec_kw={'height_ratios': [3, 1], 'hspace': 0.08},
                             sharex=True)
    ax_wsh = axes[0]
    ax_s0  = axes[1]

    # ── Panel 1 : WSH coloré par sigma0 ──────────────────────────────────
    sc = ax_wsh.scatter(
        sub['date'], sub['WSH'],
        c=sub['sigma0'], cmap=cmap, norm=norm,
        s=60, edgecolors='white', linewidths=0.4, zorder=3
    )

    suspects = sub[sub['sigma0'] < SIGMA0_SEUIL]
    if len(suspects) > 0:
        ax_wsh.scatter(
            suspects['date'], suspects['WSH'],
            s=120, facecolors='none', edgecolors='red',
            linewidths=1.5, zorder=4,
            label=f"σ0 < {SIGMA0_SEUIL} dB (n={len(suspects)})"
        )

    wsh_med = sub['WSH'].median()
    wsh_std = sub['WSH'].std()
    ax_wsh.axhline(wsh_med, color='gray', lw=1, ls='--', alpha=0.6,
                   label=f"Médiane = {wsh_med:.1f} m")
    ax_wsh.axhspan(wsh_med - 3*wsh_std, wsh_med + 3*wsh_std,
                   alpha=0.05, color='blue', label='±3σ')

    plt.colorbar(sc, ax=ax_wsh, label='Sigma0 (dB)')
    ax_wsh.set_ylabel('WSH (m)', fontsize=10)
    ax_wsh.legend(fontsize=8, loc='upper right')
    ax_wsh.grid(True, alpha=0.25)
    ax_wsh.tick_params(axis='x', labelbottom=False)
    ax_wsh.spines['bottom'].set_visible(False)
    ax_wsh.set_title(
        f"Station {sid}  —  σ0 médian = {sub['sigma0'].median():.1f} dB  "
        f"|  {len(sub)} passages",
        fontsize=11, fontweight='bold'
    )

    # ── Panel 2 : sigma0 vs date ──────────────────────────────────────────
    ax_s0.scatter(sub['date'], sub['sigma0'],
                  c=sub['sigma0'], cmap=cmap, norm=norm,
                  s=30, edgecolors='none', zorder=3)
    ax_s0.axhline(SIGMA0_SEUIL, color='red', lw=1, ls='--', alpha=0.7,
                  label=f"Seuil {SIGMA0_SEUIL} dB")
    ax_s0.axhline(sub['sigma0'].median(), color='gray', lw=1, ls='--', alpha=0.5)
    ax_s0.set_ylabel('Sigma0 (dB)', fontsize=9)
    ax_s0.set_xlabel('Date', fontsize=9)
    ax_s0.legend(fontsize=8, loc='upper right')
    ax_s0.grid(True, alpha=0.25)
    ax_s0.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    ax_s0.xaxis.set_major_locator(mdates.YearLocator())

    plt.tight_layout()
    out_path = sta_dir / f"sigma0_{sid}.png"
    plt.savefig(out_path, dpi=130, bbox_inches='tight')
    plt.close()
    n_plots += 1

    pct_sus = 100 * len(suspects) / len(sub) if len(sub) > 0 else 0
    print(f"  {sid:>15s} | {len(sub):3d} passages | "
          f"σ0 médian={sub['sigma0'].median():.1f} dB | "
          f"suspects={len(suspects)} ({pct_sus:.0f}%)")

print(f"\n✅ {n_plots} figures dans {OUT_PLOTS}")