"""
plot_benchmark_stations.py
═══════════════════════════════════════════════════════════════════════════
Pour chaque station avec correspondance DAHITI < 1.5km,
génère un plot 3 panels par année — tous comparés à l'insitu le plus proche :

  [1] HW Next  vs insitu
  [2] DAHITI   vs insitu
  [3] Modèle   vs insitu

NSE affiché dans chaque titre de panel.

Usage :
    python plot_benchmark_stations.py
    python plot_benchmark_stations.py --freq 27j
    python plot_benchmark_stations.py --only_station 0000000005209
═══════════════════════════════════════════════════════════════════════════
"""

import argparse
import sqlite3
from pathlib import Path

import geopandas as gpd
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
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

OUTPUT_DIR    = Path("./figures_benchmark")
DATE_MIN, DATE_MAX     = "2016-01-01", "2025-12-31"
DIST_MAX_INSITU_KM     = 50.0
DIST_MAX_DAHITI_KM     = 1.5
WINDOW_27J, WINDOW_10J = 14, 5

# Couleurs
C_HW  = "#3498db"
C_D   = "#2ecc71"
C_MOD = "#e74c3c"
C_INS = "darkorange"

# ═══════════════════════════════════════════════════════════════
# HELPERS (identiques au CSV)
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

def align_series(dates_ref, df_ins, window_days):
    wl = np.full(len(dates_ref), np.nan)
    for i, d in enumerate(pd.to_datetime(dates_ref)):
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
            return df.dropna(subset=["wl"])
    return pd.DataFrame()

def get_insitu_series(code_sta):
    conn = sqlite3.connect(INSITU_DB)
    df = pd.read_sql("""
        SELECT date, h_med_wsh AS wl FROM mesures_insitu
        WHERE code_sta = ? AND date >= ? AND date <= ?
        ORDER BY date
    """, conn, params=(code_sta, DATE_MIN, DATE_MAX))
    conn.close()
    df["date"] = pd.to_datetime(df["date"])
    return df.dropna(subset=["wl"])

def find_dahiti_match(lon, lat, df_d_idx):
    dists = np.sqrt(
        ((df_d_idx["lon"] - lon) * 111 * np.cos(np.radians(lat))) ** 2 +
        ((df_d_idx["lat"] - lat) * 111) ** 2
    )
    idx_min = dists.idxmin()
    dist_km = dists[idx_min]
    if dist_km <= DIST_MAX_DAHITI_KM:
        return df_d_idx.loc[idx_min, "station_code"], dist_km
    return None, float("inf")

def plot_panel(ax, dates, serie_z, ins_z, label_serie, color_serie,
               code_ins, dist_ins_km, window_label, is_last=False):
    """Trace une série vs insitu sur un panel."""
    nse_val = nse(serie_z, ins_z)
    nse_str = f"NSE={nse_val:.3f}" if not np.isnan(nse_val) else "NSE=N/A"

    ax.plot(dates, serie_z, "-o", color=color_serie,
            markersize=4, lw=1, label=label_serie, zorder=3)
    ax.plot(dates, ins_z, "-^", color=C_INS,
            markersize=4, lw=1, alpha=0.85,
            label=f"Insitu {code_ins} ({dist_ins_km:.1f}km)  {nse_str}",
            zorder=2)
    ax.axhline(0, color="grey", lw=0.5, ls="--")
    ax.set_ylabel("WL (z-score)")
    ax.legend(loc="upper right", fontsize=8)
    ax.grid(True, alpha=0.3, ls="--")
    if is_last:
        ax.set_xlabel("Date")
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b"))
        ax.xaxis.set_major_locator(mdates.MonthLocator())
    else:
        ax.tick_params(axis="x", labelbottom=False)
        ax.spines["bottom"].set_visible(False)

# ═══════════════════════════════════════════════════════════════
# GÉNÉRATION DES PLOTS
# ═══════════════════════════════════════════════════════════════
def generate_plots_for_freq(stations_file, residuals_csv, freq_label,
                            window, only_station=None):
    print(f"\n{'='*60}")
    print(f"  PLOTS {freq_label}")
    print(f"{'='*60}")

    out_dir = OUTPUT_DIR / freq_label
    out_dir.mkdir(parents=True, exist_ok=True)

    stations = [s.strip().zfill(13)
                for s in open(stations_file).read().split() if s.strip()]

    df_res = pd.read_csv(residuals_csv)
    df_res["station"] = df_res["station"].astype(str).str.zfill(13)
    df_res["date"]    = pd.to_datetime(df_res["date"])
    df_res["year"]    = df_res["date"].dt.year

    gdf     = gpd.read_file(INSITU_SHP).to_crs("EPSG:2154")
    conn_hw = sqlite3.connect(HWNEXT_DB)
    conn_d  = sqlite3.connect(DAHITI_DB)

    df_d_idx = pd.read_sql("""
        SELECT station_code, reference_longitude AS lon, reference_latitude AS lat
        FROM stations WHERE reference_longitude IS NOT NULL
    """, conn_d)

    n_plots = 0

    for code in stations:
        if only_station and code != only_station:
            continue

        lon, lat = get_coords(conn_hw, code)
        if lon is None: continue

        code_d, dist_d = find_dahiti_match(lon, lat, df_d_idx)
        if code_d is None: continue

        pt      = gpd.GeoSeries([Point(lon, lat)], crs="EPSG:4326").to_crs("EPSG:2154")[0]
        dists_g = gdf.geometry.distance(pt)
        idx_ins = dists_g.idxmin()
        dist_ins_km = dists_g[idx_ins] / 1000
        code_ins    = gdf.loc[idx_ins, "code_sta"]
        if dist_ins_km > DIST_MAX_INSITU_KM: continue

        df_ins = get_insitu_series(code_ins)
        if df_ins.empty: continue

        df_hw = get_alti_series(conn_hw, code)
        df_d  = get_alti_series(conn_d, code_d)
        sub   = df_res[df_res["station"] == code].sort_values("date")
        if sub.empty or df_hw.empty: continue

        sta_dir = out_dir / code
        sta_dir.mkdir(exist_ok=True)

        years = sorted(sub["year"].unique())
        print(f"  {code} | DAHITI={code_d} ({dist_d:.2f}km) | "
              f"insitu={code_ins} ({dist_ins_km:.1f}km) | "
              f"{len(years)} années")

        for year in years:
            hw_y  = df_hw[df_hw["date"].dt.year == year].sort_values("date")
            d_y   = df_d[df_d["date"].dt.year == year].sort_values("date") \
                    if not df_d.empty else pd.DataFrame()
            sub_y = sub[sub["year"] == year].sort_values("date")
            ins_y = df_ins[df_ins["date"].dt.year == year]

            if hw_y.empty and sub_y.empty: continue

            fig, axes = plt.subplots(3, 1, figsize=(13, 11),
                                     gridspec_kw={"height_ratios": [3, 3, 3],
                                                  "hspace": 0.08},
                                     sharex=True)
            ax1, ax2, ax3 = axes

            fig.suptitle(f"Station {code}  —  {freq_label}  —  {year}\n"
                         f"DAHITI={code_d} ({dist_d:.2f}km)  |  "
                         f"Insitu={code_ins} ({dist_ins_km:.1f}km)",
                         fontsize=11, fontweight="bold")

            # ── Panel 1 : HW Next vs insitu ───────────────────
            if not hw_y.empty:
                ins_hw_y = align_series(hw_y["date"].values, ins_y.rename(
                    columns={"wl": "wl"}), window) if not ins_y.empty else np.full(len(hw_y), np.nan)
                ax1.set_title("[1] HW Next vs Insitu", fontsize=10, fontweight="bold")
                plot_panel(ax1, hw_y["date"], zscore(hw_y["wl"].values),
                           zscore(ins_hw_y), "HW Next", C_HW,
                           code_ins, dist_ins_km, window)
            else:
                ax1.text(0.5, 0.5, "HW Next non disponible",
                         ha="center", va="center", transform=ax1.transAxes)

            # ── Panel 2 : DAHITI vs insitu ────────────────────
            if not d_y.empty:
                ins_d_y = align_series(d_y["date"].values, ins_y.rename(
                    columns={"wl": "wl"}), window) if not ins_y.empty else np.full(len(d_y), np.nan)
                ax2.set_title("[2] DAHITI vs Insitu", fontsize=10, fontweight="bold")
                plot_panel(ax2, d_y["date"], zscore(d_y["wl"].values),
                           zscore(ins_d_y), f"DAHITI {code_d}", C_D,
                           code_ins, dist_ins_km, window)
            else:
                ax2.text(0.5, 0.5, "DAHITI non disponible",
                         ha="center", va="center", transform=ax2.transAxes)
                ax2.set_title("[2] DAHITI vs Insitu", fontsize=10, fontweight="bold")

            # ── Panel 3 : Modèle vs insitu ────────────────────
            if not sub_y.empty:
                ins_mod_y = align_series(sub_y["date"].values, ins_y.rename(
                    columns={"wl": "wl"}), window) if not ins_y.empty else np.full(len(sub_y), np.nan)
                ax3.set_title("[3] Modèle vs Insitu", fontsize=10, fontweight="bold")
                plot_panel(ax3, sub_y["date"], zscore(sub_y["pred"].values),
                           zscore(ins_mod_y), "Modèle (pred)", C_MOD,
                           code_ins, dist_ins_km, window, is_last=True)
            else:
                ax3.text(0.5, 0.5, "Modèle non disponible",
                         ha="center", va="center", transform=ax3.transAxes)
                ax3.set_title("[3] Modèle vs Insitu", fontsize=10, fontweight="bold")

            plt.tight_layout()
            fig.savefig(sta_dir / f"benchmark_{code}_{year}.png",
                        dpi=150, bbox_inches="tight")
            plt.close(fig)
            n_plots += 1

    conn_hw.close()
    conn_d.close()
    print(f"\n✅ {n_plots} figures → {out_dir}")


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--freq", type=str, default="all",
                        choices=["27j", "10j", "all"])
    parser.add_argument("--only_station", type=str, default=None)
    args = parser.parse_args()

    only = args.only_station.zfill(13) if args.only_station else None
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if args.freq in ("27j", "all"):
        generate_plots_for_freq(STATIONS_27J, RESIDUALS_27J, "27j",
                                WINDOW_27J, only)
    if args.freq in ("10j", "all"):
        generate_plots_for_freq(STATIONS_10J, RESIDUALS_10J, "10j",
                                WINDOW_10J, only)