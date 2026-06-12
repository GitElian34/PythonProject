"""
plot_iterative_correction.py
═══════════════════════════════════════════════════════════════════════════
Pour chaque station, affiche les 4 courbes de prédiction (iter 0→3)
superposées à la série observée (en gras).

Produit : ./figures_iterative_correction/<station>.png
═══════════════════════════════════════════════════════════════════════════
"""

import pickle
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from pathlib import Path

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
CORRECTION_DIR = Path("./data/outlier_correction")
OUT_DIR        = Path("./figures_iterative_correction")
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUTLIER_THRESHOLD = 2.5

# Couleurs et styles par itération — on affiche seulement iter 0 et iter 3
ITER_STYLES = {
    0: {'color': '#B0BEC5', 'lw': 2.0, 'ls': '-', 'alpha': 0.9, 'label': 'Iter 0 (baseline)'},
    3: {'color': '#EF5350', 'lw': 2.0, 'ls': '-', 'alpha': 0.9, 'label': 'Iter 3 (corrigé)'},
}
ITERS_TO_PLOT = [0, 3]
OBS_STYLE = {'color': '#1A237E', 'lw': 2.5, 'ms': 5, 'zorder': 5}


# ═══════════════════════════════════════════════════════════════
# CHARGEMENT
# ═══════════════════════════════════════════════════════════════
def load_iter(iter_id):
    p = CORRECTION_DIR / f"results_iter{iter_id}.p"
    if not p.exists():
        return None
    with open(p, 'rb') as f:
        raw = pickle.load(f)
    out = {}
    for sid, sub in raw.items():
        try:
            freq = list(sub.keys())[0]
            ds   = sub[freq]['xr']
            dates = pd.to_datetime(ds.date.values)
            obs   = ds['water_level_obs'].values.flatten()
            pred  = ds['water_level_sim'].values.flatten()
            out[str(sid)] = {'dates': dates, 'obs': obs, 'pred': pred}
        except Exception:
            continue
    return out


def compute_nse(obs, pred):
    mask = ~(np.isnan(obs) | np.isnan(pred))
    if mask.sum() < 5:
        return np.nan
    o, s  = obs[mask], pred[mask]
    denom = np.sum((o - o.mean()) ** 2)
    return 1 - np.sum((o - s) ** 2) / denom if denom > 0 else np.nan


def detect_outliers(obs, pred, threshold=OUTLIER_THRESHOLD):
    residus = obs - pred
    std = np.nanstd(residus)
    if std == 0:
        return np.zeros(len(obs), dtype=bool)
    return np.abs(residus) / std > threshold


# ═══════════════════════════════════════════════════════════════
# CHARGEMENT DE TOUTES LES ITÉRATIONS
# ═══════════════════════════════════════════════════════════════
print("Chargement des résultats par itération...")
iters = {}
for i in range(4):
    data = load_iter(i)
    if data is not None:
        iters[i] = data
        print(f"  Iter {i} : {len(data)} stations")

if not iters:
    print("❌ Aucun fichier results_iterX.p trouvé dans", CORRECTION_DIR)
    exit(1)

stations = sorted(iters[0].keys())
print(f"\nGénération des figures pour {len(stations)} stations...")

# ═══════════════════════════════════════════════════════════════
# FIGURE PAR STATION
# ═══════════════════════════════════════════════════════════════
for sid in stations:
    try:
        # Données iter 0 pour l'obs (commune à toutes les iters)
        dates = iters[0][sid]['dates']
        obs   = iters[0][sid]['obs']

        # Outliers iter 0 — détectés une fois, exclus de tous les calculs NSE
        outliers_0 = detect_outliers(obs, iters[0][sid]['pred'])
        mask_non_outliers = ~outliers_0

        # NSE calculé uniquement sur les points non-outliers (même masque pour toutes les iters)
        nse_vals = {i: compute_nse(obs[mask_non_outliers], iters[i][sid]['pred'][mask_non_outliers])
                    for i in iters if sid in iters[i]}

        fig, ax = plt.subplots(figsize=(15, 5))

        # ── Série observée (en gros, en premier plan) ─────────────────
        ax.plot(dates, obs, 'o-',
                color=OBS_STYLE['color'],
                lw=OBS_STYLE['lw'],
                ms=OBS_STYLE['ms'],
                zorder=OBS_STYLE['zorder'],
                label=f"Observé")

        # ── Prédictions iter 0 et iter 3 seulement ──────────────────────
        for i in ITERS_TO_PLOT:
            if i not in iters or sid not in iters[i]:
                continue
            pred = iters[i][sid]['pred']
            st   = ITER_STYLES[i]
            nse  = nse_vals.get(i, np.nan)
            ax.plot(dates, pred,
                    color=st['color'],
                    lw=st['lw'],
                    ls=st['ls'],
                    alpha=st['alpha'],
                    zorder=3 + i,
                    label=f"{st['label']}  NSE={nse:.3f}")

        # ── Outliers iter 0 marqués ───────────────────────────────────
        if outliers_0.sum() > 0:
            ax.scatter(dates[outliers_0], obs[outliers_0],
                       s=120, facecolors='none', edgecolors='red',
                       linewidths=1.5, zorder=6,
                       label=f"Outliers iter 0 (n={outliers_0.sum()})")

        # ── Delta NSE ─────────────────────────────────────────────────
        nse_0    = nse_vals.get(0, np.nan)
        nse_last = nse_vals.get(max(iters.keys()), np.nan)
        delta    = nse_last - nse_0 if not (np.isnan(nse_0) or np.isnan(nse_last)) else np.nan
        delta_str = f"  Δ NSE = {delta:+.3f}" if not np.isnan(delta) else ""

        ax.set_title(
            f"Station {sid}  —  Correction itérative outliers{delta_str}",
            fontsize=11, fontweight='bold'
        )
        ax.set_ylabel("Water level (z-score)", fontsize=9)
        ax.set_xlabel("Date", fontsize=9)
        ax.axhline(0, color='gray', lw=0.5, ls='--', alpha=0.5)
        ax.grid(True, alpha=0.25)
        ax.legend(fontsize=8, loc='upper right',
                  framealpha=0.9, ncol=2)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
        ax.xaxis.set_major_locator(mdates.YearLocator())

        plt.tight_layout()
        plt.savefig(OUT_DIR / f"{sid}.png", dpi=130, bbox_inches='tight')
        plt.close()
        print(f"  ✅ {sid}  NSE: {nse_0:.3f} → {nse_last:.3f}  ({delta:+.3f})")

    except Exception as e:
        print(f"  ❌ {sid} : {e}")
        continue

# ═══════════════════════════════════════════════════════════════
# FIGURE RÉCAP : NSE par station et par itération
# ═══════════════════════════════════════════════════════════════
df_comp = pd.read_csv(CORRECTION_DIR / "comparaison_nse_iterations.csv")
df_comp = df_comp.sort_values('NSE_iter0')

fig, ax = plt.subplots(figsize=(16, 6))
x = np.arange(len(df_comp))
w = 0.2

colors_bar = ['#B0BEC5', '#42A5F5', '#FFA726', '#EF5350']
iter_cols  = [c for c in df_comp.columns if c.startswith('NSE_iter')]

for j, (col, color) in enumerate(zip(iter_cols, colors_bar)):
    ax.bar(x + j * w, df_comp[col], width=w, color=color,
           alpha=0.85, label=col.replace('NSE_', 'Iter ').replace('iter', ''))

ax.set_xticks(x + w * 1.5)
ax.set_xticklabels(
    [str(s)[-7:] for s in df_comp['station']],
    rotation=45, ha='right', fontsize=7
)
ax.axhline(0.5, color='gray', lw=1, ls='--', alpha=0.5, label='NSE=0.5')
ax.set_ylabel("NSE", fontsize=10)
ax.set_title("NSE par station et par itération de correction", fontsize=12, fontweight='bold')
ax.legend(fontsize=9)
ax.grid(axis='y', alpha=0.3)

plt.tight_layout()
plt.savefig(OUT_DIR / "_recap_nse_iterations.png", dpi=130, bbox_inches='tight')
plt.close()
print(f"\n✅ Récap NSE : {OUT_DIR}/_recap_nse_iterations.png")
print(f"✅ {len(stations)} figures dans : {OUT_DIR}")