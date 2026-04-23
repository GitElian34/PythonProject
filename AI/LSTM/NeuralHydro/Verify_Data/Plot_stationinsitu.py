"""
Plots obs vs sim pour les 3 meilleures et 3 pires stations (epoch 15)
- OBS  : lues depuis ./data/IA/NeuralHydrology/time_series/{station}.nc
- SIM  : lues depuis le fichier .p du run (résultats NeuralHydrology)
"""

import torch
import pickle
import numpy as np
import xarray as xr
import matplotlib.pyplot as plt
from pathlib import Path

torch.set_num_threads(8)

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
RUN_DIR    = Path("./runs/satellite_water_level_test_1604_114015")
NC_DIR     = Path("./data/IA/NeuralHydrology/time_series")
PLOT_EPOCH = 15
TEST_START = "2024-01-01"
TEST_END   = "2025-12-31"

WORST_STATIONS = [
    ("Y921000203", -21.731),
    ("Y046600501", -10.143),
    ("U141541001",  -1.203),
]
BEST_STATIONS = [
    ("J341303001",  0.791),
    ("O710151001",  0.771),
    ("O301101001",  0.764),
]

# ═══════════════════════════════════════════════════════════════
# CHARGEMENT RÉSULTATS (pour NSE/KGE et sim)
# ═══════════════════════════════════════════════════════════════
candidates = list((RUN_DIR / "test").glob(f"*epoch{PLOT_EPOCH:03d}*/*.p"))
if not candidates:
    raise FileNotFoundError(f"Aucun résultat .p trouvé pour epoch {PLOT_EPOCH}")

with open(sorted(candidates)[-1], "rb") as f:
    results = pickle.load(f)

print(f"✅ Résultats epoch {PLOT_EPOCH} chargés — {len(results)} stations")

# Debug structure xr sur la première station disponible
first = list(results.keys())[0]
xr_ex = results[first]['1D']['xr']
print(f"\nStructure xr (station {first}) :")
print(f"  type   : {type(xr_ex)}")
print(f"  dims   : {dict(xr_ex.dims)}")
print(f"  coords : {list(xr_ex.coords)}")
if hasattr(xr_ex, 'data_vars'):
    print(f"  data_vars : {list(xr_ex.data_vars)}")
else:
    for c in xr_ex.coords:
        try: print(f"  coord '{c}' values : {xr_ex.coords[c].values[:5]}")
        except: pass

# ═══════════════════════════════════════════════════════════════
# HELPER — extraction sim depuis le pickle
# ═══════════════════════════════════════════════════════════════
def extract_sim(xr_data):
    """Extrait sim et times depuis xarray NeuralHydrology."""
    if hasattr(xr_data, 'data_vars'):
        sim_key = next((k for k in xr_data.data_vars if 'sim' in k), list(xr_data.data_vars)[0])
        sim     = xr_data[sim_key].values.flatten()
        times   = xr_data.coords[list(xr_data.dims)[0]].values
        return sim, times

    if 'variable' in xr_data.dims:
        var_names = xr_data.coords['variable'].values
        sim_vars  = [v for v in var_names if 'sim' in str(v)]
        key       = sim_vars[0] if sim_vars else var_names[0]
        sim       = xr_data.sel(variable=key).values.flatten()
        dim0      = [d for d in xr_data.dims if d != 'variable'][0]
        times     = xr_data.coords[dim0].values
        return sim, times

    # Fallback : aplatir
    vals  = xr_data.values.flatten()
    times = xr_data.coords[list(xr_data.dims)[0]].values
    return vals, times

# ═══════════════════════════════════════════════════════════════
# PLOT
# ═══════════════════════════════════════════════════════════════
fig, axes = plt.subplots(6, 1, figsize=(14, 18), constrained_layout=True)
fig.suptitle(
    f"Obs vs Sim — Epoch {PLOT_EPOCH} | {TEST_START} → {TEST_END}",
    fontsize=13, fontweight='bold'
)

plot_groups = [
    ("3 MEILLEURES", BEST_STATIONS,  "steelblue"),
    ("3 PIRES",      WORST_STATIONS, "crimson"),
]

ax_idx = 0
for group_label, stations_list, color in plot_groups:
    for rank, (station, nse_ref) in enumerate(stations_list):
        ax = axes[ax_idx]

        # ── OBS depuis le .nc ──────────────────────────────────
        nc_path = NC_DIR / f"{station}.nc"
        if not nc_path.exists():
            ax.set_title(f"{station} — fichier .nc introuvable : {nc_path}")
            ax_idx += 1
            continue

        try:
            ds      = xr.open_dataset(nc_path)
            ds_test = ds.sel(date=slice(TEST_START, TEST_END))
            obs     = ds_test["water_level"].values.flatten()
            t_obs   = ds_test.coords["date"].values
            ds.close()
        except Exception as e:
            ax.set_title(f"{station} — erreur lecture .nc : {e}")
            ax_idx += 1
            continue

        # ── SIM + métriques depuis le pickle ──────────────────
        nse, kge = np.nan, np.nan
        sim, t_sim = None, None

        if station in results:
            try:
                nse     = results[station]['1D']['NSE']
                kge     = results[station]['1D']['KGE']
                xr_data = results[station]['1D']['xr']
                sim, t_sim = extract_sim(xr_data)
            except Exception as e:
                print(f"  ⚠️  {station} — erreur extraction sim : {e}")

        # ── Alignement temporel obs/sim ────────────────────────
        import pandas as pd
        df_obs = pd.Series(obs, index=pd.to_datetime(t_obs), name='obs')

        if sim is not None and t_sim is not None:
            df_sim = pd.Series(sim, index=pd.to_datetime(t_sim), name='sim')
            df     = pd.concat([df_obs, df_sim], axis=1).dropna()
            t_plot   = df.index
            obs_plot = df['obs'].values
            sim_plot = df['sim'].values
        else:
            t_plot   = df_obs.dropna().index
            obs_plot = df_obs.dropna().values
            sim_plot = np.full_like(obs_plot, np.nan)

        if len(obs_plot) == 0:
            ax.set_title(f"{station} — aucune donnée sur la période test")
            ax_idx += 1
            continue

        obs_std = np.std(obs_plot)
        sim_std = np.nanstd(sim_plot)
        bias    = np.nanmean(sim_plot - obs_plot)

        ax.plot(t_plot, obs_plot, color='black', lw=1.5, label='Observé',  zorder=3)
        if not np.all(np.isnan(sim_plot)):
            ax.plot(t_plot, sim_plot, color=color, lw=1.5, label='Simulé', alpha=0.85, zorder=2)
            ax.fill_between(t_plot, obs_plot, sim_plot, alpha=0.12, color=color)

        ax.set_title(
            f"[{group_label} #{rank+1}]  {station}   "
            f"NSE={nse:.3f}  KGE={kge:.3f}  "
            f"| obs_std={obs_std:.3f}  sim_std={sim_std:.3f}  biais={bias:+.3f}",
            fontsize=9, loc='left'
        )
        ax.set_ylabel("water_level (norm.)", fontsize=8)
        ax.legend(fontsize=8, loc='upper right')
        ax.grid(True, alpha=0.3)

        ax_idx += 1

out_path = Path("./plots_best_worst.png")
fig.savefig(out_path, dpi=150, bbox_inches='tight')
plt.close()
print(f"\n✅ Plot sauvegardé → {out_path}")