"""
generalized_top5_zscore_vs_metres.py
════════════════════════════════════════════════════════════════════════
Généralisation du test station-unique (13412) validé précédemment :
  - sélection des N_TOP meilleures stations, SÉPARÉMENT pour 10j et
    27j (même méthode de sélection que top5_stations_alti_modele_insitu.py :
    score = min(NSE alti-insitu, meilleur NSE modèle-insitu), en z-score) ;
  - pour chaque station sélectionnée, pour CHAQUE année disponible, un plot
    à 2 panels :
      panel 1 (haut)  : z-score, chaque série normalisée indépendamment
                        (comme dans tous les plots précédents) ;
      panel 2 (bas)   : mètres, avec la formule de reconstruction VALIDÉE
                        (pred_z × std_alti + mean_alti) et l'insitu recalée
                        visuellement sur le datum alti (décalage de médiane,
                        affiché dans le titre, n'affecte aucun calcul).
  - NSE et KGE affichés dans les titres des deux panels (calculés en
    z-score, seul espace où ils sont comparables entre séries de std
    différents -- cf. discussion sur NSE(zscore) = 2*corr - 1).

Usage :
    python generalized_top5_zscore_vs_metres.py
    (ajuster SOURCE ci-dessous : "hwnext" ou "dahiti")
════════════════════════════════════════════════════════════════════════
"""

import sqlite3
import sys
import numpy as np
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from pathlib import Path
from shapely.geometry import Point

# ── Import robuste de Sword_connectivity.py (remonte l'arborescence) ──
THIS_DIR = Path(__file__).resolve().parent
SWORD_MODULE_DIR = None
for ancestor in [THIS_DIR] + list(THIS_DIR.parents):
    candidate = ancestor / "Sword_and_Insitu"
    if (candidate / "Sword_connectivity.py").exists():
        SWORD_MODULE_DIR = candidate
        break
if SWORD_MODULE_DIR is None:
    raise SystemExit(
        f"⚠ Sword_connectivity.py introuvable en remontant depuis {THIS_DIR} — "
        f"vérifie qu'un dossier 'Sword_and_Insitu' existe au-dessus de ce script."
    )
sys.path.insert(0, str(SWORD_MODULE_DIR))
from Sword_connectivity import load_sword_reaches, build_graph, check_connectivity

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
SOURCE = "hwnext"   # "hwnext" ou "dahiti" — pilote la DB alti utilisée ci-dessous
MASKS = [80, 90, 96]
FREQS = ["10j", "27j"]
N_TOP = 5

HW_DB     = "./data/hydroweb_next.db"
DAHITI_DB = "./data/dahiti.db"
SAT_DB = HW_DB if SOURCE == "hwnext" else DAHITI_DB   # <- corrigé : dépendait de SOURCE avant, était figé sur dahiti.db

INSITU_DB = "./data/insitu_data.db"
INSITU_SHP = "./data/insitu/shp/station_schapi_alti_ref_2025_river.gpkg"

RESIDUALS_DIR = Path("./data_processing/AnalyseModelsDtoD/residuals")
OUTPUT_BASE = Path("./data_processing/AnalyseModelsDtoD/plot/meter")
OUTPUT_BASE.mkdir(parents=True, exist_ok=True)

DATE_MIN, DATE_MAX = "2016-01-01", "2025-12-31"
DIST_MAX_KM = 50.0
FACC_MAX_RATIO = 2.0
WINDOW_ALTI_INSITU = {"10j": 7, "27j": 14}
MIN_PAIRS_SELECTION = 30
MIN_DAYS_PER_YEAR = 30

print(f"SOURCE={SOURCE}  ->  SAT_DB={SAT_DB}")


# ═══════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════
def normalize_code(c):
    s = str(c)
    try:
        return str(int(s))
    except ValueError:
        return s


def zscore(arr):
    a = np.asarray(arr, dtype=float)
    m = ~np.isnan(a)
    if m.sum() < 2:
        return a * np.nan
    mu, sig = a[m].mean(), a[m].std()
    return (a - mu) / sig if sig > 0 else a * 0


def nse(obs, sim):
    m = ~(np.isnan(obs) | np.isnan(sim))
    if m.sum() < 5:
        return np.nan
    o, s = obs[m], sim[m]
    d = np.sum((o - o.mean()) ** 2)
    return float(1 - np.sum((o - s) ** 2) / d) if d > 0 else np.nan


def kge(obs, sim, with_bias=False):
    m = ~(np.isnan(obs) | np.isnan(sim))
    if m.sum() < 5:
        return np.nan
    o, s = obs[m], sim[m]
    if o.std() == 0 or s.std() == 0:
        return np.nan
    r = np.corrcoef(o, s)[0, 1]
    alpha = s.std() / o.std()
    if with_bias and o.mean() != 0:
        beta = s.mean() / o.mean()
        return float(1 - np.sqrt((r - 1) ** 2 + (alpha - 1) ** 2 + (beta - 1) ** 2))
    return float(1 - np.sqrt((r - 1) ** 2 + (alpha - 1) ** 2))


def align_insitu(dates, df_ins, window_days):
    wl = np.full(len(dates), np.nan)
    id_ = np.array(df_ins["date"].values, dtype="datetime64[D]")
    iv = df_ins["wl"].values
    for i, d in enumerate(np.array(dates, dtype="datetime64[D]")):
        diff = np.abs((id_ - d).astype(float))
        idx = int(np.argmin(diff))
        if diff[idx] <= window_days:
            wl[i] = iv[idx]
    return wl


def get_alti_series(code_norm):
    conn = sqlite3.connect(SAT_DB)
    df = pd.read_sql("""
        SELECT measure_date AS date, orthometric_height AS wl
        FROM measurements
        WHERE station_code = ? AND is_valid = 1
          AND measure_date >= ? AND measure_date <= ?
        ORDER BY measure_date
    """, conn, params=(code_norm, DATE_MIN, DATE_MAX))
    conn.close()
    df["date"] = pd.to_datetime(df["date"])
    return df.dropna(subset=["wl"])


# ═══════════════════════════════════════════════════════════════
# 1. SWORD + INSITU (chargés une seule fois)
# ═══════════════════════════════════════════════════════════════
print("### Chargement SWORD (une seule fois) ###")
gdf_sword, gdf_sword_proj = load_sword_reaches()
G_sword, info_sword = build_graph(gdf_sword)

print("### Chargement insitu (shapefile) ###")
gdf_insitu = gpd.read_file(INSITU_SHP).to_crs("EPSG:4326")
gdf_insitu_proj = gdf_insitu.to_crs("EPSG:2154")
_cache_ins = {}


def get_insitu_candidats(lon, lat, dist_max_km):
    pt = gpd.GeoSeries([Point(lon, lat)], crs="EPSG:4326").to_crs("EPSG:2154")[0]
    dist = gdf_insitu_proj.geometry.distance(pt) / 1000
    candidats = dist[dist <= dist_max_km].sort_values()
    return [(gdf_insitu_proj.loc[idx, "code_sta"], d) for idx, d in candidats.items()]


def get_insitu_sword(lon_a, lat_a):
    for code_ins, dist_km in get_insitu_candidats(lon_a, lat_a, DIST_MAX_KM):
        idx = gdf_insitu_proj[gdf_insitu_proj["code_sta"] == code_ins].index[0]
        lon_b, lat_b = gdf_insitu.loc[idx, "geometry"].x, gdf_insitu.loc[idx, "geometry"].y
        res = check_connectivity(lon_a, lat_a, lon_b, lat_b, G_sword, info_sword, gdf_sword_proj,
                                  facc_max_ratio=FACC_MAX_RATIO)
        if res["connected"] and not res["has_confluence"] and res["facc_ok"] is not False:
            return code_ins, dist_km
    return None, None


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
# 2. RÉSIDUS COMPLETS — 2 FREQ x 3 MASQUES
# ═══════════════════════════════════════════════════════════════
print("### Chargement des résidus complets (10j/27j x 80/90/96) ###")
FULL_RESIDUALS = {}
stations_par_freq = {"10j": set(), "27j": set()}

for freq in FREQS:
    for mask in MASKS:
        path = RESIDUALS_DIR / f"residuals_{SOURCE}_{freq}_{mask}pct.csv"
        if not path.exists():
            print(f"  ⚠ Manquant : {path}")
            continue
        df = pd.read_csv(path)
        df["date"] = pd.to_datetime(df["date"])
        df["station"] = df["station"].astype(str)
        df = df.dropna(subset=["pred"])
        FULL_RESIDUALS[(freq, mask)] = df
        stations_par_freq[freq] |= set(normalize_code(c) for c in df["station"].unique())
        print(f"  ({freq}, {mask}%) : {len(df)} lignes, {df['station'].nunique()} stations")

# ═══════════════════════════════════════════════════════════════
# 3. STATIONS
# ═══════════════════════════════════════════════════════════════
print("\n### Chargement des stations ###")
conn = sqlite3.connect(SAT_DB)
df_stations = pd.read_sql("""
    SELECT station_code, reference_longitude AS lon, reference_latitude AS lat
    FROM stations
    WHERE reference_longitude IS NOT NULL AND reference_latitude IS NOT NULL
""", conn)
conn.close()
print(f"  {len(df_stations)} stations\n")

# ═══════════════════════════════════════════════════════════════
# 4. SÉLECTION DES TOP N PAR FRÉQUENCE (score en z-score)
# ═══════════════════════════════════════════════════════════════
def selectionner_top_n(freq):
    print(f"\n### Sélection top {N_TOP} pour {freq} ###")
    rows = []
    for _, sta in df_stations.iterrows():
        code = sta["station_code"]
        code_n = normalize_code(code)
        if code_n not in stations_par_freq[freq]:
            continue
        lon_a, lat_a = sta["lon"], sta["lat"]

        code_ins, dist_km = get_insitu_sword(lon_a, lat_a)
        if code_ins is None:
            continue
        df_ins = get_insitu_series(code_ins)
        if df_ins is None:
            continue
        df_alti = get_alti_series(code)
        if len(df_alti) < MIN_PAIRS_SELECTION:
            continue

        ins_wl_a = align_insitu(df_alti["date"].values, df_ins, WINDOW_ALTI_INSITU[freq])
        if int(np.sum(~np.isnan(ins_wl_a))) < MIN_PAIRS_SELECTION:
            continue
        nse_alti_ins = nse(zscore(ins_wl_a), zscore(df_alti["wl"].values))
        if np.isnan(nse_alti_ins):
            continue

        best_mask, best_nse_model = None, -np.inf
        for mask in MASKS:
            df_m = FULL_RESIDUALS.get((freq, mask))
            if df_m is None:
                continue
            sub = df_m[df_m["station"] == code_n].sort_values("date")
            if len(sub) < MIN_PAIRS_SELECTION:
                continue
            ins_wl_m = align_insitu(sub["date"].values, df_ins, 1)
            if int(np.sum(~np.isnan(ins_wl_m))) < MIN_PAIRS_SELECTION:
                continue
            nse_m = nse(zscore(ins_wl_m), zscore(sub["pred"].values))
            if not np.isnan(nse_m) and nse_m > best_nse_model:
                best_nse_model, best_mask = nse_m, mask

        if best_mask is None:
            continue

        rows.append({
            "station": code, "freq": freq, "insitu": code_ins, "dist_km": round(dist_km, 1),
            "nse_alti_insitu": round(nse_alti_ins, 3),
            "best_mask": best_mask, "nse_modele_insitu": round(best_nse_model, 3),
            "score_min": round(min(nse_alti_ins, best_nse_model), 3),
        })

    if not rows:
        print(f"  ⚠ Aucune station valide pour {freq}")
        return pd.DataFrame()
    df_scores = pd.DataFrame(rows).sort_values("score_min", ascending=False).reset_index(drop=True)
    print(f"  {len(df_scores)} stations évaluées -> top {N_TOP} retenues")
    return df_scores.head(N_TOP)


top_par_freq = {freq: selectionner_top_n(freq) for freq in FREQS}

for freq, df_top in top_par_freq.items():
    if not df_top.empty:
        out_csv = OUTPUT_BASE / f"top{N_TOP}_{freq}.csv"
        df_top.to_csv(out_csv, index=False)
        print(f"\nTOP {N_TOP} {freq} -> {out_csv}")
        print(df_top.to_string(index=False))

# ═══════════════════════════════════════════════════════════════
# 5. PLOTS — pour chaque station sélectionnée, chaque année
# ═══════════════════════════════════════════════════════════════
def plot_station(code, freq, code_ins, mask, dist_km):
    code_n = normalize_code(code)
    df_alti = get_alti_series(code)
    df_ins = get_insitu_series(code_ins)
    sub = FULL_RESIDUALS[(freq, mask)]
    sub = sub[sub["station"] == code_n].sort_values("date").reset_index(drop=True)

    mean_alti = df_alti["wl"].values.mean()
    std_alti = df_alti["wl"].values.std()

    # reconstruction validée
    sub["pred_metres"] = sub["pred"].values * std_alti + mean_alti
    has_obs = sub["obs"].notna().values if "obs" in sub.columns else np.zeros(len(sub), dtype=bool)
    sub["obs_metres"] = np.where(has_obs, sub["obs"].values * std_alti + mean_alti, np.nan)

    # recalage visuel de l'insitu (décalage de médiane, datum)
    med_alti = float(np.median(df_alti["wl"].values))
    med_ins = float(np.median(df_ins["wl"].values))
    offset_insitu = med_alti - med_ins
    df_ins_recale = df_ins.copy()
    df_ins_recale["wl_recale"] = df_ins_recale["wl"] + offset_insitu

    # séries z-score (alignées comme dans le script de sélection / le script
    # station-unique : insitu alignée aux dates alti pour le panel alti-insitu,
    # insitu alignée aux dates modèle pour le panel modèle-insitu)
    ins_wl_a_full = align_insitu(df_alti["date"].values, df_ins, WINDOW_ALTI_INSITU[freq])
    ins_wl_m_full = align_insitu(sub["date"].values, df_ins, 1)
    alti_z = zscore(df_alti["wl"].values)
    ins_a_z = zscore(ins_wl_a_full)
    ins_m_z = zscore(ins_wl_m_full)
    pred_z = sub["pred"].values  # déjà z-scoré en amont, ne PAS re-zscorer

    out_dir = OUTPUT_BASE / freq / code
    out_dir.mkdir(parents=True, exist_ok=True)

    years_alti = pd.to_datetime(df_alti["date"]).dt.year
    years_model = pd.to_datetime(sub["date"]).dt.year
    all_years = sorted(set(years_alti.unique()) | set(years_model.unique()))

    n_figs = 0
    for year in all_years:
        m_a = (years_alti == year).values
        m_m = (years_model == year).values
        m_ins_full = (pd.to_datetime(df_ins["date"]).dt.year == year).values
        if m_a.sum() < MIN_DAYS_PER_YEAR and m_m.sum() < MIN_DAYS_PER_YEAR:
            continue

        nse_a_y = nse(ins_a_z[m_a], alti_z[m_a]) if m_a.sum() >= 5 else np.nan
        kge_a_y = kge(ins_a_z[m_a], alti_z[m_a]) if m_a.sum() >= 5 else np.nan
        nse_m_y = nse(ins_m_z[m_m], pred_z[m_m]) if m_m.sum() >= 5 else np.nan
        kge_m_y = kge(ins_m_z[m_m], pred_z[m_m]) if m_m.sum() >= 5 else np.nan

        fig, (ax_z, ax_m) = plt.subplots(2, 1, figsize=(13, 9), sharex=True)

        # -- panel z-score --
        ax_z.plot(df_alti["date"][m_a], alti_z[m_a], "o-", color="#5B9BD5", ms=4, lw=1, label="Alti (z-score)")
        ax_z.plot(sub["date"][m_m], pred_z[m_m], "-", color="#c0392b", lw=1.2, label=f"Modèle DtoD{mask}% (z-score)")
        ax_z.plot(df_ins["date"][m_ins_full], zscore(df_ins["wl"].values)[m_ins_full], "-",
                  color="#e67e22", lw=0.8, label=f"Insitu {code_ins} (z-score)")
        ax_z.set_title(f"Alti vs Insitu — {year}", fontsize=10, fontweight="bold")
        ax_z.set_ylabel("WL (z-score)")
        ax_z.axhline(0, color="grey", lw=0.6, ls="--")
        ax_z.legend(fontsize=8, loc="upper right")
        ax_z.grid(True, alpha=0.3)

        # -- panel mètres (insitu recalée) --
        ax_m.plot(df_alti["date"][m_a], df_alti["wl"][m_a], "o-", color="#5B9BD5", ms=4, lw=1,
                  label="Alti (brute, mètres)")
        ax_m.plot(sub["date"][m_m], sub["pred_metres"][m_m], "-", color="#c0392b", lw=1.2,
                  label=f"Modèle DtoD{mask}% reconstruit (mètres)")
        obs_mask_year = has_obs & m_m
        ax_m.scatter(sub["date"][obs_mask_year], sub["obs_metres"][obs_mask_year], color="#2c3e50", s=24, zorder=3,
                     label="obs reconstruite (jours satellite)")
        ax_m.plot(df_ins["date"][m_ins_full], df_ins_recale["wl_recale"][m_ins_full], "-",
                  color="#e67e22", lw=0.8, label=f"Insitu {code_ins} (recalée {offset_insitu:+.2f} m)")
        ax_m.set_title(f"Modèle vs Insitu (quotidien) — décalage insitu {offset_insitu:+.2f} m",
                       fontsize=10, fontweight="bold")
        ax_m.set_ylabel("WL (m)")
        ax_m.set_xlabel("Date")
        ax_m.legend(fontsize=8, loc="upper right")
        ax_m.grid(True, alpha=0.3)
        ax_m.xaxis.set_major_formatter(mdates.DateFormatter("%b"))
        ax_m.xaxis.set_major_locator(mdates.MonthLocator())
        plt.setp(ax_m.xaxis.get_majorticklabels(), rotation=30, ha="right")

        def fmt_metrique(label, n, k):
            extra = []
            if not np.isnan(n): extra.append(f"NSE={n:.3f}")
            if not np.isnan(k): extra.append(f"KGE={k:.3f}")
            return f"{label} : " + (", ".join(extra) if extra else "n/a")

        ligne_metriques = (
            fmt_metrique("Alti vs Insitu", nse_a_y, kge_a_y) + "   |   " +
            fmt_metrique("Modèle vs Insitu", nse_m_y, kge_m_y) + "   (calculé en z-score)"
        )

        fig.suptitle(
            f"{SOURCE.upper()} {code} ({freq})  —  Insitu SWORD {code_ins} ({dist_km} km)  —  DtoD{mask}%  —  {year}\n"
            f"{ligne_metriques}",
            fontsize=12, fontweight="bold"
        )
        plt.tight_layout(rect=[0, 0, 1, 0.94])
        fig.savefig(out_dir / f"{code}_{year}.png", dpi=130, bbox_inches="tight")
        plt.close(fig)
        n_figs += 1

    print(f"  Station {code} ({freq}, insitu {code_ins}, {dist_km} km, DtoD{mask}%) : {n_figs} figures -> {out_dir}")


for freq, df_top in top_par_freq.items():
    if df_top.empty:
        continue
    print(f"\n### Génération des figures — {freq} ###")
    for _, row in df_top.iterrows():
        plot_station(row["station"], row["freq"], row["insitu"], row["best_mask"], row["dist_km"])

print(f"\nTerminé. Figures dans : {OUTPUT_BASE}/")