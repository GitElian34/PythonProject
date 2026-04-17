"""
Visualisation des prédictions NeuralHydrology vs observations
Structure : results[station]['1D']['xr'] → xarray Dataset
"""

import pickle
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

RESULT_FILE = "./runs/satellite_water_level_test_1504_112759/validation/model_epoch030/validation_results.p"

with open(RESULT_FILE, "rb") as f:
    results = pickle.load(f)

stations = list(results.keys())
print(f"Stations : {stations}")

fig, axes = plt.subplots(len(stations), 1, figsize=(14, 5 * len(stations)))
if len(stations) == 1:
    axes = [axes]

for ax, station in zip(axes, stations):
    xr_data = results[station]['1D']['xr']
    nse     = results[station]['1D']['NSE']
    kge     = results[station]['1D']['KGE']

    print(f"\n{station}")
    print(f"  Variables xr : {list(xr_data.data_vars)}")

    # Cherche obs et sim
    obs_var = [v for v in xr_data.data_vars if 'obs' in v][0]
    sim_var = [v for v in xr_data.data_vars if 'sim' in v][0]

    obs   = xr_data[obs_var].values.flatten()
    sim   = xr_data[sim_var].values.flatten()
    dates = pd.to_datetime(xr_data.coords['date'].values)

    valid = ~np.isnan(obs) & ~np.isnan(sim)
    bias  = np.mean(sim[valid]) - np.mean(obs[valid]) if valid.sum() > 0 else np.nan

    print(f"  NSE={nse:.3f}  KGE={kge:.3f}  Biais={bias:.3f}")
    print(f"  Obs : min={np.nanmin(obs):.3f}  max={np.nanmax(obs):.3f}  NaN={np.isnan(obs).sum()}/{len(obs)}")
    print(f"  Sim : min={np.nanmin(sim):.3f}  max={np.nanmax(sim):.3f}  NaN={np.isnan(sim).sum()}/{len(sim)}")

    ax.plot(dates, obs, label='Observé', color='steelblue', lw=1.5)
    ax.plot(dates, sim, label='Prédit',  color='tomato',   lw=1.5, ls='--')
    ax.set_title(f"{station}  |  NSE={nse:.3f}  KGE={kge:.3f}  Biais={bias:.3f}  "
                 f"| n valides={valid.sum()}/{len(valid)}",
                 fontsize=11, fontweight='bold')
    ax.set_ylabel("Niveau d'eau (normalisé)")
    ax.legend()
    ax.grid(alpha=0.3)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)

plt.tight_layout()
plt.savefig("validation_results.png", dpi=150, bbox_inches='tight')
print("\n✅ Figure sauvegardée : validation_results.png")