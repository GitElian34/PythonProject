"""
benchmark_nse_comparison.py
═══════════════════════════════════════════════════════════════════════════
Compare station par station pour 27j et 10j :

Groupe 1 — Toutes stations HW Next :
  - NSE HW Next ↔ insitu
  - NSE Modèle(HW Next) ↔ insitu

Groupe 2 — Stations avec correspondance DAHITI < 1.5km :
  - NSE HW Next ↔ insitu
  - NSE DAHITI ↔ insitu
  - NSE Modèle(HW Next) ↔ insitu

Matching DAHITI : géographique (distance euclidienne < 1.5km)
NSE modèle↔insitu : pred vs insitu normalisé sur la base de obs (même mu/std)

Sorties :
  - benchmark_nse_27j.csv
  - benchmark_nse_10j.csv
  - benchmark_nse_comparison.png
═══════════════════════════════════════════════════════════════════════════
"""

import sqlite3
import numpy as np
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from pathlib import Path
from shapely.geometry import Point

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
HWNEXT_DB   = "./data/hydroweb_next.db"
DAHITI_DB   = "./data/dahiti.db"
INSITU_DB   = "./data/insitu_data.db"
INSITU_SHP  = "./data/insitu/shp/station_schapi_alti_ref_2025_river.gpkg"

RESIDUALS_27J = "./data/outlier_detection/residuals_27j_hydroweb_next.csv"
RESIDUALS_10J = "./data/outlier_detection/residuals_10j_hydroweb_next.csv"
STATIONS_27J  = "./data/IA/NeuralHydrology_hydroweb_next/stations_dahiti_27j.txt"
STATIONS_10J  = "./data/IA/NeuralHydrology_hydroweb_next/stations_dahiti_10j.txt"

OUTPUT_DIR    = Path("./data/outlier_detection")
DATE_MIN, DATE_MAX = "2016-01-01", "2025-12-31"
DIST_MAX_INSITU_KM = 50.0   # distance max station alti ↔ insitu
DIST_MAX_DAHITI_KM = 1.5    # distance max HW Next ↔ DAHITI pour le matching

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

# ═══════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════
def zscore(arr):
    mu, sig = np.nanmean(arr), np.nanstd(arr)
    return (arr - mu) / sig if sig > 0 else arr * 0

def nse(obs, sim):
    mask = ~(np.isnan(obs) | np.isnan(sim))
    if mask.sum() < 5: return np.nan
    o, s = obs[mask], sim[mask]
    d = np.sum((o - o.mean()) ** 2)
    return 1 - np.sum((o - s) ** 2) / d if d > 0 else np.nan

def align_series(dates_alti, df_ins, window_days):
    """Aligne l'insitu sur les dates altimétriques (fenêtre ±window_days)."""
    wl = np.full(len(dates_alti), np.nan)
    for i, d in enumerate(pd.to_datetime(dates_alti)):
        diff = (df_ins["date"] - d).abs()
        idx  = diff.idxmin()
        if diff[idx] <= pd.Timedelta(days=window_days):
            wl[i] = df_ins.loc[idx, "wl"]
    return wl

def get_coords(conn, station_code):
    for code in [station_code, station_code.lstrip("0") or station_code]:
        df = pd.read_sql(
            "SELECT reference_longitude AS lon, reference_latitude AS lat "
            "FROM stations WHERE station_code = ?",
            conn, params=(code,)
        )
        if not df.empty:
            return float(df.iloc[0]["lon"]), float(df.iloc[0]["lat"])
    return None, None

def get_alti_series(conn, station_code):
    for code in [station_code, station_code.lstrip("0") or station_code]:
        df = pd.read_sql("""
            SELECT measure_date AS date, orthometric_height AS wl
            FROM measurements
            WHERE station_code = ? AND is_valid = 1
              AND measure_date >= ? AND measure_date <= ?
            ORDER BY date
        """, conn, params=(code, DATE_MIN, DATE_MAX))
        if not df.empty:
            df["date"] = pd.to_datetime(df["date"])
            df = df.dropna(subset=["wl"])
            return df if len(df) >= 5 else None
    return None

# ═══════════════════════════════════════════════════════════════
# MATCHING GÉOGRAPHIQUE HW Next ↔ DAHITI
# ═══════════════════════════════════════════════════════════════
def build_dahiti_index(conn_d):
    """Charge toutes les stations DAHITI avec leurs coords."""
    return pd.read_sql("""
        SELECT station_code, reference_longitude AS lon, reference_latitude AS lat
        FROM stations
        WHERE reference_longitude IS NOT NULL AND reference_latitude IS NOT NULL
    """, conn_d)

def find_dahiti_match(lon_hw, lat_hw, df_dahiti_idx, max_km=DIST_MAX_DAHITI_KM):
    """
    Trouve la station DAHITI la plus proche d'une station HW Next.
    Retourne (station_code_dahiti, dist_km) ou (None, inf).
    Distance approx. en degrés → km (1° ≈ 111km).
    """
    dists = np.sqrt(
        ((df_dahiti_idx["lon"] - lon_hw) * 111 * np.cos(np.radians(lat_hw))) ** 2 +
        ((df_dahiti_idx["lat"] - lat_hw) * 111) ** 2
    )
    idx_min  = dists.idxmin()
    dist_km  = dists[idx_min]
    if dist_km <= max_km:
        return df_dahiti_idx.loc[idx_min, "station_code"], dist_km
    return None, float("inf")

# ═══════════════════════════════════════════════════════════════
# FONCTION PRINCIPALE PAR FRÉQUENCE
# ═══════════════════════════════════════════════════════════════
def compute_benchmark(stations_file, residuals_csv, freq_label, window_insitu):
    print(f"\n{'='*65}")
    print(f"  BENCHMARK {freq_label}")
    print(f"{'='*65}")

    stations = [s.strip().zfill(13)
                for s in open(stations_file).read().split() if s.strip()]
    print(f"  {len(stations)} stations HW Next")

    df_res = pd.read_csv(residuals_csv)
    df_res["station"] = df_res["station"].astype(str).str.zfill(13)

    conn_hw = sqlite3.connect(HWNEXT_DB)
    conn_d  = sqlite3.connect(DAHITI_DB)

    # Index DAHITI pour le matching géographique
    df_dahiti_idx = build_dahiti_index(conn_d)
    print(f"  {len(df_dahiti_idx)} stations DAHITI disponibles pour matching")

    results = []

    for i, code in enumerate(stations):
        lon, lat = get_coords(conn_hw, code)
        if lon is None:
            continue

        # ── Insitu le plus proche ────────────────────────────
        code_ins, dist_ins_km = get_insitu_proche(lon, lat)
        if dist_ins_km > DIST_MAX_INSITU_KM:
            continue
        df_ins = get_insitu_series(code_ins)
        if df_ins is None:
            continue

        # ── NSE HW Next ↔ insitu ─────────────────────────────
        df_hw  = get_alti_series(conn_hw, code)
        nse_hw = np.nan
        if df_hw is not None:
            ins_hw = align_series(df_hw["date"].values, df_ins, window_insitu)
            nse_hw = nse(zscore(df_hw["wl"].values), zscore(ins_hw))

        # ── NSE modèle(HW Next) ↔ insitu ────────────────────
        # pred est déjà normalisé par NeuralHydrology (z-score sur obs)
        # On normalise l'insitu sur la même base que obs (mu/std de obs)
        sub     = df_res[df_res["station"] == code]
        nse_mod = np.nan
        if len(sub) >= 5:
            ins_mod  = align_series(sub["date"].values, df_ins, window_insitu)
            n_pairs  = int(np.sum(~np.isnan(ins_mod)))
            if n_pairs >= 5:
                nse_mod = nse(zscore(sub["pred"].values), zscore(ins_mod))

        # ── Matching DAHITI géographique (< 1.5km) ───────────
        code_d, dist_d_km = find_dahiti_match(lon, lat, df_dahiti_idx)
        nse_d = np.nan
        if code_d is not None:
            df_d = get_alti_series(conn_d, code_d)
            if df_d is not None:
                ins_d = align_series(df_d["date"].values, df_ins, window_insitu)
                nse_d = nse(zscore(df_d["wl"].values), zscore(ins_d))

        results.append({
            "station"        : code,
            "freq"           : freq_label,
            "code_insitu"    : code_ins,
            "dist_insitu_km" : round(dist_ins_km, 1),
            "nse_hwnext_ins" : round(nse_hw,  3) if not np.isnan(nse_hw)  else np.nan,
            "nse_modele_ins" : round(nse_mod, 3) if not np.isnan(nse_mod) else np.nan,
            "code_dahiti"    : code_d,
            "dist_dahiti_km" : round(dist_d_km, 2) if code_d else np.nan,
            "nse_dahiti_ins" : round(nse_d,  3) if not np.isnan(nse_d)   else np.nan,
        })

        if (i + 1) % 30 == 0:
            print(f"  {i+1}/{len(stations)} traitées...")

    conn_hw.close()
    conn_d.close()

    df = pd.DataFrame(results)
    df_all    = df                                        # toutes stations HW Next
    df_dahiti = df.dropna(subset=["nse_dahiti_ins"])      # stations avec match DAHITI

    # ── Synthèse console ──────────────────────────────────────
    print(f"\n  {'─'*60}")
    print(f"  GROUPE 1 — Toutes stations HW Next (n={len(df_all)})")
    print(f"  {'─'*60}")
    for col, label in [
        ("nse_hwnext_ins", "HW Next ↔ insitu      "),
        ("nse_modele_ins", "Modèle(HWNext) ↔ insitu"),
    ]:
        v = df_all[col].dropna()
        print(f"\n    {label} (n={len(v)})")
        print(f"      NSE médian : {v.median():.3f}")
        print(f"      NSE moyen  : {v.mean():.3f}")
        print(f"      NSE > 0.5  : {(v > 0.5).sum()} ({(v > 0.5).mean():.0%})")
        print(f"      NSE < 0    : {(v < 0).sum()} ({(v < 0).mean():.0%})")

    print(f"\n  {'─'*60}")
    print(f"  GROUPE 2 — Stations avec DAHITI < {DIST_MAX_DAHITI_KM}km (n={len(df_dahiti)})")
    print(f"  {'─'*60}")
    for col, label in [
        ("nse_hwnext_ins", "HW Next ↔ insitu      "),
        ("nse_dahiti_ins", "DAHITI ↔ insitu       "),
        ("nse_modele_ins", "Modèle(HWNext) ↔ insitu"),
    ]:
        v = df_dahiti[col].dropna()
        print(f"\n    {label} (n={len(v)})")
        print(f"      NSE médian : {v.median():.3f}")
        print(f"      NSE moyen  : {v.mean():.3f}")
        print(f"      NSE > 0.5  : {(v > 0.5).sum()} ({(v > 0.5).mean():.0%})")
        print(f"      NSE < 0    : {(v < 0).sum()} ({(v < 0).mean():.0%})")

    return df

# ═══════════════════════════════════════════════════════════════
# LANCEMENT
# ═══════════════════════════════════════════════════════════════
df_27 = compute_benchmark(STATIONS_27J, RESIDUALS_27J, "27j", window_insitu=14)
df_10 = compute_benchmark(STATIONS_10J, RESIDUALS_10J, "10j", window_insitu=5)

df_27.to_csv(OUTPUT_DIR / "benchmark_nse_27j.csv", index=False)
df_10.to_csv(OUTPUT_DIR / "benchmark_nse_10j.csv", index=False)
print(f"\n✅ CSVs exportés dans {OUTPUT_DIR}")

# ═══════════════════════════════════════════════════════════════
# FIGURE
# ═══════════════════════════════════════════════════════════════
rng = np.random.default_rng(42)

C_HW  = "#3498db"   # HW Next
C_D   = "#2ecc71"   # DAHITI
C_MOD = "#e74c3c"   # Modèle

fig = plt.figure(figsize=(18, 12))
gs  = gridspec.GridSpec(2, 3, figure=fig, hspace=0.45, wspace=0.35)

def boxstrip(ax, data_dict, title):
    """
    data_dict : OrderedDict {label: (values, color)}
    """
    labels = list(data_dict.keys())
    vals   = [data_dict[l][0] for l in labels]
    colors = [data_dict[l][1] for l in labels]
    bp = ax.boxplot(vals, tick_labels=labels, patch_artist=True,
                    medianprops={"color": "black", "linewidth": 2}, widths=0.5)
    for box, color in zip(bp["boxes"], colors):
        box.set_facecolor(color); box.set_alpha(0.7)
    for i, (data, color) in enumerate(zip(vals, colors), 1):
        jitter = rng.uniform(-0.15, 0.15, len(data))
        ax.scatter(np.full(len(data), i) + jitter, data,
                   alpha=0.4, s=18, color=color, zorder=3)
        med = np.nanmedian(data)
        ax.text(i, med + 0.04, f"{med:.3f}", ha="center",
                fontsize=8, fontweight="bold")
    ax.axhline(0,   color="red",   lw=1, ls="--", alpha=0.5)
    ax.axhline(0.5, color="green", lw=1, ls="--", alpha=0.5)
    ax.grid(True, alpha=0.3, axis="y")
    ax.set_title(title, fontsize=10, fontweight="bold")
    ax.set_ylabel("NSE ↔ insitu")

# Préparer les données par groupe
for col_idx, (df, freq) in enumerate([(df_27, "27j"), (df_10, "10j")]):
    df_all    = df
    df_dahiti = df.dropna(subset=["nse_dahiti_ins"])

    # Panel haut : Groupe 1 (toutes stations)
    ax1 = fig.add_subplot(gs[0, col_idx])
    boxstrip(ax1, {
        f"HW Next\n(n={df_all['nse_hwnext_ins'].dropna().__len__()})":
            (df_all["nse_hwnext_ins"].dropna().values, C_HW),
        f"Modèle\n(n={df_all['nse_modele_ins'].dropna().__len__()})":
            (df_all["nse_modele_ins"].dropna().values, C_MOD),
    }, f"Groupe 1 — Toutes stations HW Next {freq}\n(n={len(df_all)})")

    # Panel bas : Groupe 2 (stations avec DAHITI)
    ax2 = fig.add_subplot(gs[1, col_idx])
    boxstrip(ax2, {
        f"HW Next\n(n={df_dahiti['nse_hwnext_ins'].dropna().__len__()})":
            (df_dahiti["nse_hwnext_ins"].dropna().values, C_HW),
        f"DAHITI\n(n={df_dahiti['nse_dahiti_ins'].dropna().__len__()})":
            (df_dahiti["nse_dahiti_ins"].dropna().values, C_D),
        f"Modèle\n(n={df_dahiti['nse_modele_ins'].dropna().__len__()})":
            (df_dahiti["nse_modele_ins"].dropna().values, C_MOD),
    }, f"Groupe 2 — Stations avec DAHITI <{DIST_MAX_DAHITI_KM}km {freq}\n(n={len(df_dahiti)})")

# Panel scatter HW Next vs DAHITI (27j + 10j superposés)
ax3 = fig.add_subplot(gs[:, 2])
for df, freq, color, marker in [
    (df_27.dropna(subset=["nse_hwnext_ins","nse_dahiti_ins"]), "27j", "#9b59b6", "o"),
    (df_10.dropna(subset=["nse_hwnext_ins","nse_dahiti_ins"]), "10j", "#e67e22", "^"),
]:
    ax3.scatter(df["nse_hwnext_ins"], df["nse_dahiti_ins"],
                alpha=0.6, s=40, color=color, marker=marker,
                edgecolors="white", label=f"{freq} (n={len(df)})", zorder=3)

all_v = pd.concat([
    df_27[["nse_hwnext_ins","nse_dahiti_ins"]],
    df_10[["nse_hwnext_ins","nse_dahiti_ins"]]
]).dropna()
if not all_v.empty:
    lims = [all_v.min().min() - 0.05, all_v.max().max() + 0.05]
    ax3.plot(lims, lims, "k--", lw=1, alpha=0.5, label="y=x (égalité)")
    ax3.set_xlim(lims); ax3.set_ylim(lims)
ax3.axhline(0, color="red",   lw=1, ls="--", alpha=0.4)
ax3.axhline(0.5, color="green", lw=1, ls="--", alpha=0.4)
ax3.axvline(0, color="red",   lw=1, ls="--", alpha=0.4)
ax3.axvline(0.5, color="green", lw=1, ls="--", alpha=0.4)
ax3.set_xlabel("NSE HW Next ↔ insitu", fontsize=9)
ax3.set_ylabel("NSE DAHITI ↔ insitu", fontsize=9)
ax3.set_title(f"HW Next vs DAHITI\n(au-dessus diag. = DAHITI meilleur)",
              fontsize=10, fontweight="bold")
ax3.legend(fontsize=8); ax3.grid(True, alpha=0.3)

fig.suptitle(
    "Benchmark NSE vs insitu — HW Next / DAHITI / Modèle AR-LSTM\n"
    f"Groupe 1 = toutes stations HW Next  |  "
    f"Groupe 2 = stations avec DAHITI < {DIST_MAX_DAHITI_KM}km",
    fontsize=12, fontweight="bold"
)
fig.savefig(OUTPUT_DIR / "benchmark_nse_comparison.png", dpi=150, bbox_inches="tight")
plt.close(fig)
print(f"✅ Figure → {OUTPUT_DIR}/benchmark_nse_comparison.png")