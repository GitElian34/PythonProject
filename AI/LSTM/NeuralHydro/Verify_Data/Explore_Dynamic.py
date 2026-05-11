import xarray as xr
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import glob
import os
import random

# ── Config ───────────────────────────────────────────────────────────────────
NC_DIR = "./data/IA/NeuralHydrology_feat10j/time_series/"
ATTR_PATH = "./data/IA/NeuralHydrology_feat10j/attributes/attributes.csv"
N_SAMPLE = 1000
SEED = 42

# Seuil amplitude max acceptable par ordre de Strahler
AMPLITUDE_SEUILS = {1: 3, 2: 5, 3: 7, 4: 10, 5: 15, 6: 25, 7: 40}

# ── Chargement attributs statiques ───────────────────────────────────────────
attrs = pd.read_csv(ATTR_PATH, index_col=0)

# ── Chargement de N_SAMPLE fichiers aléatoires ───────────────────────────────
all_files = glob.glob(os.path.join(NC_DIR, "*.nc"))
random.seed(SEED)
sampled = random.sample(all_files, min(N_SAMPLE, len(all_files)))
print(f"Fichiers disponibles : {len(all_files)} | Analysés : {len(sampled)}")

DYNAMIC_VARS = [
    "precipitation_J0", "temperature_J0", "pet_J0",
    "precip_mean_J3", "temp_mean_J3", "pet_mean_J3",
    "precip_mean_J10", "temp_mean_J10", "pet_mean_J10",
    "water_level"
]

records = []
problematic = []

for fpath in sampled:
    station = os.path.basename(fpath).replace(".nc", "")
    try:
        ds = xr.open_dataset(fpath)
        rec = {"station": station, "n_points": len(ds["date"])}

        for var in DYNAMIC_VARS:
            if var not in ds:
                rec[f"{var}_missing"] = True
                continue
            vals = ds[var].values.astype(float)
            nan_count = np.isnan(vals).sum()
            rec[f"{var}_nan"] = nan_count
            rec[f"{var}_mean"] = np.nanmean(vals)
            rec[f"{var}_std"] = np.nanstd(vals)
            rec[f"{var}_min"] = np.nanmin(vals)
            rec[f"{var}_max"] = np.nanmax(vals)
            rec[f"{var}_pct_nan"] = nan_count / len(vals) * 100

        wl = ds["water_level"].values.astype(float)
        flags = []

        strahler = attrs.loc[station, "strahler"] if station in attrs.index else None
        seuil_amp = AMPLITUDE_SEUILS.get(int(strahler), 20) if strahler is not None else 20
        rec["strahler"] = int(strahler) if strahler is not None else -1

        if np.isnan(wl).all():
            flags.append("water_level 100% NaN")
        else:
            if np.nanstd(wl) < 0.01:
                flags.append(f"série plate (std={np.nanstd(wl):.4f})")
            amplitude = np.nanmax(wl) - np.nanmin(wl)
            if amplitude > seuil_amp:
                s_str = str(int(strahler)) if strahler is not None else "?"
                flags.append(f"amplitude suspecte ({np.nanmin(wl):.1f}→{np.nanmax(wl):.1f}m | S{s_str}, seuil={seuil_amp}m)")
            if np.isnan(wl).sum() / len(wl) > 0.5:
                flags.append(">50% NaN water_level")

        prec = ds["precipitation_J0"].values.astype(float)
        if not np.isnan(prec).all() and np.nanmin(prec) < -0.1:
            flags.append(f"précip négative (min={np.nanmin(prec):.2f})")

        rec["flags"] = " | ".join(flags) if flags else ""
        if flags:
            problematic.append({"station": station, "strahler": rec["strahler"], "flags": " | ".join(flags)})

        records.append(rec)
        ds.close()

    except Exception as e:
        problematic.append({"station": station, "strahler": -1, "flags": f"ERREUR: {e}"})

df = pd.DataFrame(records)

print("\n" + "=" * 60)
print("RÉSUMÉ GLOBAL")
print("=" * 60)
print(f"  Fichiers analysés        : {len(df)}")
print(f"  Fichiers avec problèmes  : {len(problematic)}")
print(f"  Points médian/station    : {df['n_points'].median():.0f}")

print("\n── NaN par variable (% médian sur les stations) ──")
for var in DYNAMIC_VARS:
    col = f"{var}_pct_nan"
    if col in df.columns:
        med = df[col].median()
        max_ = df[col].max()
        n_high = (df[col] > 10).sum()
        print(f"  {var:<25} médiane={med:5.1f}%  max={max_:5.1f}%  stations>10%: {n_high}")

print("\n── Statistiques water_level ──")
print(f"  Médiane des moyennes     : {df['water_level_mean'].median():.3f} m")
print(f"  Médiane des std          : {df['water_level_std'].median():.3f} m")
print(f"  Max amplitude observée   : {(df['water_level_max'] - df['water_level_min']).max():.2f} m")

print("\n── Stations problématiques (Strahler-aware) ──")
if problematic:
    prob_df = pd.DataFrame(problematic).sort_values("strahler")
    for _, row in prob_df.iterrows():
        print(f"  S{row['strahler']}  {row['station']:<30} → {row['flags']}")
    print(f"\n  Total : {len(problematic)} stations")
else:
    print("  Aucune ✅")

# ── Figure ───────────────────────────────────────────────────────────────────
fig, axes = plt.subplots(2, 3, figsize=(16, 10))
fig.suptitle(f"Diagnostic .nc feat10j — {len(df)} stations | seuils Strahler-aware", fontsize=13)

axes[0, 0].hist(df["water_level_mean"].dropna(), bins=40, color="steelblue", edgecolor="white")
axes[0, 0].set_title("Distribution moyennes water_level")
axes[0, 0].set_xlabel("Niveau moyen (m)")

axes[0, 1].hist(df["water_level_std"].dropna(), bins=40, color="darkorange", edgecolor="white")
axes[0, 1].set_title("Distribution std water_level")
axes[0, 1].set_xlabel("Std (m)")

amplitude = df["water_level_max"] - df["water_level_min"]
axes[0, 2].hist(amplitude.dropna(), bins=40, color="green", edgecolor="white")
axes[0, 2].set_title("Amplitude water_level (max - min)")
axes[0, 2].set_xlabel("Amplitude (m)")

axes[1, 0].hist(df["water_level_pct_nan"].dropna(), bins=40, color="red", edgecolor="white")
axes[1, 0].set_title("% NaN water_level par station")
axes[1, 0].set_xlabel("% NaN")

axes[1, 1].hist(df["precipitation_J0_mean"].dropna(), bins=40, color="purple", edgecolor="white")
axes[1, 1].set_title("Précipitations moyennes J0")
axes[1, 1].set_xlabel("Précip moyenne (mm)")

if "strahler" in df.columns:
    axes[1, 2].scatter(df["strahler"], amplitude, alpha=0.3, s=8, color="brown")
    for s, seuil in AMPLITUDE_SEUILS.items():
        axes[1, 2].plot(s, seuil, "r*", markersize=12)
    axes[1, 2].set_title("Amplitude vs Strahler (★ = seuil)")
    axes[1, 2].set_xlabel("Strahler")
    axes[1, 2].set_ylabel("Amplitude (m)")

plt.tight_layout()
out_fig = "./data/IA/NeuralHydrology_feat10j/diagnostic_nc.png"
plt.savefig(out_fig, dpi=150)
print(f"\nFigure sauvegardée : {out_fig}")