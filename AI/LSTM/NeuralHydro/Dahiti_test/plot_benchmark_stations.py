"""
plot_benchmark_dahiti.py
════════════════════════════════════════════════════════════════════════
Pour chaque station DAHITI (27j et 10j séparément) :
  - Panel [1] DAHITI vs Insitu
  - Panel [2] Modèle (pred) vs Insitu
  - Panel [3] Modèle vs DAHITI

Fixes appliqués :
  - dropna(subset=['obs','pred']) dès le départ → grille propre
  - Décalage temporel corrigé par station (médiane des décalages
    entre grille NeuralHydrology et vraies dates DAHITI BDD)
  - NSE calculé sur z-scores indépendants
════════════════════════════════════════════════════════════════════════
"""

import sqlite3
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import geopandas as gpd
from pathlib import Path
from shapely.geometry import Point

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
DAHITI_DB  = "./data/dahiti.db"
INSITU_DB  = "./data/insitu_data.db"
INSITU_SHP = "./data/insitu/shp/station_schapi_alti_ref_2025_river.gpkg"

RESIDUALS_27J = "./data/outlier_detection/residuals_27j_dahiti_clean.csv"
RESIDUALS_10J = "./data/outlier_detection/residuals_10j_dahiti.csv"
STATIONS_27J  = "./data/IA/NeuralHydrologyDahiti27jClean/stations_dahiti_27j.txt"
STATIONS_10J  = "./data/IA/NeuralHydrologyDahitiFull/stations_dahiti_10j.txt"

OUTPUT_DIR         = Path("./data/outlier_detection/plots_dahiti_v2")
DATE_MIN, DATE_MAX = "2016-01-01", "2025-12-31"
DIST_MAX_INSITU_KM = 50.0

# Couleurs
C_DA  = "#27ae60"
C_MOD = "#c0392b"
C_INS = "#e67e22"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ═══════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════
def zscore(arr):
    arr = np.asarray(arr, dtype=float)
    mask = ~np.isnan(arr)
    if mask.sum() < 2:
        return arr * np.nan
    mu, sig = arr[mask].mean(), arr[mask].std()
    return (arr - mu) / sig if sig > 0 else arr * 0

def nse(obs, sim):
    mask = ~(np.isnan(obs) | np.isnan(sim))
    if mask.sum() < 5:
        return np.nan
    o, s = obs[mask], sim[mask]
    d = np.sum((o - o.mean()) ** 2)
    return float(1 - np.sum((o - s) ** 2) / d) if d > 0 else np.nan

def align_insitu(dates_alti, df_ins, window_days):
    """Aligne l'insitu sur les dates altimétriques (fenêtre ±window_days)."""
    wl = np.full(len(dates_alti), np.nan)
    ins_dates = df_ins["date"].values
    ins_wl    = df_ins["wl"].values
    for i, d in enumerate(pd.to_datetime(dates_alti)):
        diff = np.abs((pd.to_datetime(ins_dates) - d).total_seconds())
        idx  = diff.argmin()
        if diff[idx] <= window_days * 86400:
            wl[i] = ins_wl[idx]
    return wl

def get_decalage_median(conn_da, code, df_res_station):
    """
    Calcule le décalage temporel médian entre la grille NeuralHydrology
    (dates du CSV résidus) et les vraies dates DAHITI BDD.
    Retourne un Timedelta.
    """
    sub_ok = df_res_station.dropna(subset=["obs"]).sort_values("date")
    if len(sub_ok) == 0:
        return pd.Timedelta(0)

    for c in [str(code).zfill(13), code]:
        df_bdd = pd.read_sql(
            "SELECT measure_date AS date FROM measurements "
            "WHERE station_code = ? AND is_valid = 1 ORDER BY date",
            conn_da, params=(c,))
        if df_bdd.empty:
            continue
        df_bdd["date"] = pd.to_datetime(df_bdd["date"])
        df_bdd = df_bdd.groupby("date").first().reset_index()

        decalages = []
        for _, row in sub_ok.iterrows():
            diff = (df_bdd["date"] - row["date"]).abs()
            idx  = diff.idxmin()
            decalages.append((df_bdd.loc[idx, "date"] - row["date"]).days)

        mediane = int(np.median(decalages))
        return pd.Timedelta(days=mediane)

    return pd.Timedelta(0)

# ═══════════════════════════════════════════════════════════════
# CHARGEMENT INSITU
# ═══════════════════════════════════════════════════════════════
print("Chargement shapefile insitu...")
gdf        = gpd.read_file(INSITU_SHP).to_crs("EPSG:2154")
_cache_ins = {}

def get_insitu_proche(lon, lat):
    pt   = gpd.GeoSeries([Point(lon, lat)], crs="EPSG:4326").to_crs("EPSG:2154")[0]
    dist = gdf.geometry.distance(pt)
    idx  = dist.idxmin()
    return gdf.loc[idx, "code_sta"], dist[idx] / 1000

def get_insitu_series(code_sta):
    if code_sta not in _cache_ins:
        conn = sqlite3.connect(INSITU_DB)
        df = pd.read_sql("""
            SELECT date, h_med_wsh AS wl FROM mesures_insitu
            WHERE code_sta = ? AND date >= ? AND date <= ?
            ORDER BY date
        """, conn, params=(code_sta, DATE_MIN, DATE_MAX))
        conn.close()
        df["date"] = pd.to_datetime(df["date"])
        df = df.dropna(subset=["wl"])
        _cache_ins[code_sta] = df if len(df) >= 5 else None
    return _cache_ins[code_sta]

def get_coords(conn, code):
    for c in [str(code).zfill(13), code, code.lstrip("0") or code]:
        df = pd.read_sql(
            "SELECT reference_longitude AS lon, reference_latitude AS lat "
            "FROM stations WHERE station_code = ?",
            conn, params=(c,)
        )
        if not df.empty:
            return float(df.iloc[0]["lon"]), float(df.iloc[0]["lat"])
    return None, None

# ═══════════════════════════════════════════════════════════════
# FONCTION PRINCIPALE PAR FRÉQUENCE
# ═══════════════════════════════════════════════════════════════
def run_freq(stations_file, residuals_csv, freq_label, window_insitu):
    print(f"\n{'='*65}")
    print(f"  TRAITEMENT {freq_label}")
    print(f"{'='*65}")

    stations = [s.strip() for s in open(stations_file).read().split() if s.strip()]
    print(f"  {len(stations)} stations")

    df_res = pd.read_csv(residuals_csv)
    df_res["station"] = df_res["station"].astype(str)
    df_res["date"]    = pd.to_datetime(df_res["date"])
    df_res = df_res.dropna(subset=["obs", "pred"])
    print(f"  Lignes résidus après dropna : {len(df_res)}")

    conn_da = sqlite3.connect(DAHITI_DB)
    results = []
    out_freq = OUTPUT_DIR / freq_label
    out_freq.mkdir(parents=True, exist_ok=True)
    n_plotted = 0

    for i, code in enumerate(stations):
        lon, lat = get_coords(conn_da, code)
        if lon is None:
            continue

        code_ins, dist_ins_km = get_insitu_proche(lon, lat)
        if dist_ins_km > DIST_MAX_INSITU_KM:
            continue
        df_ins = get_insitu_series(code_ins)
        if df_ins is None:
            continue

        sub = df_res[df_res["station"] == code].sort_values("date")
        if len(sub) < 5:
            sub = df_res[df_res["station"] == str(int(code))].sort_values("date")
        if len(sub) < 5:
            continue

        # ── Décalage médian station par station ───────────────
        decalage = get_decalage_median(conn_da, code, sub)
        dates_corrigees = pd.to_datetime(sub["date"].values) + decalage

        # ── Insitu aligné sur les dates corrigées ─────────────
        ins_aligned = align_insitu(dates_corrigees.values, df_ins, window_insitu)

        n_pairs = int(np.sum(~np.isnan(ins_aligned)))
        if n_pairs < 5:
            continue

        obs_z      = zscore(sub["obs"].values)
        pred_z     = zscore(sub["pred"].values)
        ins_z      = zscore(ins_aligned)

        nse_da     = nse(obs_z,  ins_z)
        nse_mod    = nse(pred_z, ins_z)
        nse_mod_da = nse(pred_z, obs_z)

        results.append({
            "station"        : code,
            "freq"           : freq_label,
            "code_insitu"    : code_ins,
            "dist_insitu_km" : round(dist_ins_km, 1),
            "n_dates"        : len(sub),
            "n_insitu_pairs" : n_pairs,
            "decalage_j"     : decalage.days,
            "nse_dahiti_ins" : round(nse_da,     3) if not np.isnan(nse_da)     else np.nan,
            "nse_modele_ins" : round(nse_mod,    3) if not np.isnan(nse_mod)    else np.nan,
            "nse_modele_da"  : round(nse_mod_da, 3) if not np.isnan(nse_mod_da) else np.nan,
        })

        # ── Plot ──────────────────────────────────────────────
        fig, axes = plt.subplots(3, 1, figsize=(14, 11), sharex=True)
        fig.suptitle(
            f"Station DAHITI {code} — {freq_label}\n"
            f"Insitu={code_ins} ({dist_ins_km:.1f}km) | "
            f"n={len(sub)} dates | décalage corrigé={decalage.days}j",
            fontsize=11, fontweight="bold"
        )

        dates_plot = dates_corrigees

        # Panel 1 — DAHITI vs Insitu
        ax1 = axes[0]
        ax1.plot(dates_plot, obs_z,  color=C_DA,  lw=1.8, marker="o",
                 ms=4, label="DAHITI (obs)")
        ax1.plot(dates_plot, ins_z,  color=C_INS, lw=1.5, marker="^",
                 ms=4, ls="--", label=f"Insitu {code_ins}  NSE={nse_da:.3f}")
        ax1.axhline(0, color="gray", lw=0.7, ls=":")
        ax1.set_ylabel("WL (z-score)", fontsize=9)
        ax1.set_title("[1] DAHITI vs Insitu", fontsize=10, fontweight="bold")
        ax1.legend(fontsize=8, loc="upper right")
        ax1.grid(True, alpha=0.3)

        # Panel 2 — Modèle vs Insitu
        ax2 = axes[1]
        ax2.plot(dates_plot, pred_z, color=C_MOD, lw=1.8, marker="o",
                 ms=4, label="Modèle (pred)")
        ax2.plot(dates_plot, ins_z,  color=C_INS, lw=1.5, marker="^",
                 ms=4, ls="--", label=f"Insitu {code_ins}  NSE={nse_mod:.3f}")
        ax2.axhline(0, color="gray", lw=0.7, ls=":")
        ax2.set_ylabel("WL (z-score)", fontsize=9)
        ax2.set_title("[2] Modèle vs Insitu", fontsize=10, fontweight="bold")
        ax2.legend(fontsize=8, loc="upper right")
        ax2.grid(True, alpha=0.3)

        # Panel 3 — Modèle vs DAHITI
        ax3 = axes[2]
        ax3.plot(dates_plot, pred_z, color=C_MOD, lw=1.8, marker="o",
                 ms=4, label="Modèle (pred)")
        ax3.plot(dates_plot, obs_z,  color=C_DA,  lw=1.5, marker="^",
                 ms=4, ls="--", label=f"DAHITI (obs)  NSE={nse_mod_da:.3f}")
        ax3.axhline(0, color="gray", lw=0.7, ls=":")
        ax3.set_ylabel("WL (z-score)", fontsize=9)
        ax3.set_xlabel("Date", fontsize=9)
        ax3.set_title("[3] Modèle vs DAHITI", fontsize=10, fontweight="bold")
        ax3.legend(fontsize=8, loc="upper right")
        ax3.grid(True, alpha=0.3)

        plt.tight_layout()
        out_path = out_freq / f"{code}_{freq_label}.png"
        fig.savefig(out_path, dpi=130, bbox_inches="tight")
        plt.close(fig)
        n_plotted += 1

        if (i + 1) % 30 == 0:
            print(f"  {i+1}/{len(stations)} traitées...")

    conn_da.close()

    df_out = pd.DataFrame(results)
    csv_path = OUTPUT_DIR / f"benchmark_dahiti_{freq_label}.csv"
    df_out.to_csv(csv_path, index=False)

    print(f"\n  Stations traitées : {len(df_out)}")
    print(f"  Plots générés     : {n_plotted}  → {out_freq}/")
    print(f"  CSV exporté       : {csv_path}")
    if len(df_out) == 0:
        print("  ⚠ Aucune station traitée")
    else:
        # Distribution des décalages
        dec = df_out["decalage_j"].dropna()
        print(f"\n  Décalages appliqués : médiane={dec.median():.0f}j | "
              f"min={dec.min():.0f}j | max={dec.max():.0f}j")
        for col, label in [
            ("nse_dahiti_ins", "DAHITI ↔ insitu        "),
            ("nse_modele_ins", "Modèle(DAHITI) ↔ insitu"),
            ("nse_modele_da",  "Modèle ↔ DAHITI        "),
        ]:
            v = df_out[col].dropna()
            print(f"\n    {label} (n={len(v)})")
            print(f"      NSE médian : {v.median():.3f}")
            print(f"      NSE moyen  : {v.mean():.3f}")
            print(f"      NSE > 0.5  : {(v > 0.5).sum()} ({(v > 0.5).mean():.0%})")
            print(f"      NSE < 0    : {(v < 0).sum()} ({(v < 0).mean():.0%})")

    return df_out


# ═══════════════════════════════════════════════════════════════
# LANCEMENT
# ═══════════════════════════════════════════════════════════════
df_27 = run_freq(STATIONS_27J, RESIDUALS_27J, "27j", window_insitu=14)
df_10 = run_freq(STATIONS_10J, RESIDUALS_10J, "10j", window_insitu=5)

# ═══════════════════════════════════════════════════════════════
# FIGURE GLOBALE — boxplots comparatifs
# ═══════════════════════════════════════════════════════════════
rng = np.random.default_rng(42)

fig, axes = plt.subplots(1, 2, figsize=(12, 6))
fig.suptitle(
    "Benchmark NSE vs Insitu — DAHITI & Modèle AR-LSTM\n"
    "(décalage temporel corrigé par station)",
    fontsize=12, fontweight="bold"
)

C_DA  = "#27ae60"
C_MOD = "#c0392b"

for ax, (df, freq) in zip(axes, [(df_27, "27j"), (df_10, "10j")]):
    if len(df) == 0:
        continue
    data = {
        "DAHITI" : (df["nse_dahiti_ins"].dropna().values, C_DA),
        "Modèle" : (df["nse_modele_ins"].dropna().values, C_MOD),
    }
    labels = list(data.keys())
    vals   = [data[l][0] for l in labels]
    colors = [data[l][1] for l in labels]

    bp = ax.boxplot(vals, tick_labels=labels, patch_artist=True,
                    medianprops={"color": "black", "linewidth": 2},
                    widths=0.45)
    for box, color in zip(bp["boxes"], colors):
        box.set_facecolor(color)
        box.set_alpha(0.65)

    for j, (v, color) in enumerate(zip(vals, colors), 1):
        jitter = rng.uniform(-0.12, 0.12, len(v))
        ax.scatter(np.full(len(v), j) + jitter, v,
                   alpha=0.35, s=14, color=color, zorder=3)
        med = np.nanmedian(v)
        ax.text(j, med + 0.03, f"{med:.3f}", ha="center",
                fontsize=9, fontweight="bold")

    ax.axhline(0,   color="red",   lw=1, ls="--", alpha=0.5, label="NSE=0")
    ax.axhline(0.5, color="green", lw=1, ls="--", alpha=0.5, label="NSE=0.5")
    ax.set_title(f"{freq}  (n={len(df)} stations)", fontsize=11, fontweight="bold")
    ax.set_ylabel("NSE ↔ insitu")
    ax.grid(True, alpha=0.25, axis="y")
    ax.legend(fontsize=8)

plt.tight_layout()
fig_path = OUTPUT_DIR / "benchmark_global_dahiti.png"
fig.savefig(fig_path, dpi=150, bbox_inches="tight")
plt.close(fig)
print(f"\n✅ Figure globale → {fig_path}")