"""
plot_quantile_alti_insitu.py
════════════════════════════════════════════════════════════════════════
Pour chaque station et chaque modèle quantile (DtoD80/90/96), trace sur
un même graphique :
  - l'altimétrie brute (obs, reconstruite en mètres)
  - l'insitu le plus proche (recalée par décalage de médiane)
  - les 5 quantiles du modèle [Q05, Q25, Q50, Q75, Q95] (reconstruits en
    mètres avec la même formule que Q50), affichés en bandes de
    confiance (Q05-Q95 clair, Q25-Q75 plus foncé) + ligne Q50.

Source des données : les résidus complets produits par
eval_zeroshot_quantile_DtoD.py, centralisés dans RESIDUALS_DIR
(colonnes : station, date, obs, pred, q05, q25, q50, q75, q95).

Formule de reconstruction en mètres (validée sur station 13412, session
précédente) :
    valeur_metres = valeur_zscore × std_alti_station + mean_alti_station

Recalage insitu : décalage de médiane uniquement, calculé sur la période
commune avec l'alti (neutralise le datum, préserve l'amplitude).

Sorties :
    ./data_processing/Modele_quantileLoss/plot/{SOURCE}/{FREQ_LABEL}/{year}/{station}/{model_label}.png
    un fichier par (année, station, modèle) — 2 panels empilés dans chaque
    fichier (modèle vs alti en haut, modèle vs insitu en bas). Les points
    obs/insitu hors de l'intervalle [Q05, Q95] du modèle sont flagués en
    rouge (marqueur "x").

Usage :
    python plot_quantile_alti_insitu.py
    (ajuster SOURCE, FREQ, STATIONS ci-dessous avant de lancer)
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

# ── Import robuste de Sword_connectivity.py ──────────────────────────────
THIS_DIR = Path(__file__).resolve().parent
SWORD_MODULE_DIR = THIS_DIR.parent / "Sword_and_Insitu"
if not (SWORD_MODULE_DIR / "Sword_connectivity.py").exists():
    raise SystemExit(
        f"⚠ Sword_connectivity.py introuvable dans {SWORD_MODULE_DIR}\n"
        f"  Corrige SWORD_MODULE_DIR avec le chemin absolu vers Sword_and_Insitu."
    )
sys.path.insert(0, str(SWORD_MODULE_DIR))
from Sword_connectivity import load_sword_reaches, build_graph, check_connectivity

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES GLOBAUX — à modifier ici à chaque utilisation
# ═══════════════════════════════════════════════════════════════
SOURCE     = "dahiti"   # "hwnext" ou "dahiti"
FREQ       = "27j"      # "10j" ou "27j" — utilisé pour retrouver le CSV source
FREQ_LABEL = "27J"      # utilisé uniquement dans le chemin de sortie (respecte la casse demandée)

# Stations à plotter. None = toutes les stations présentes dans les CSV de résidus.
STATIONS = None

MASKS = [80, 90, 96]
QUANTILE_COLS = ["q05", "q25", "q50", "q75", "q95"]

# Résidus complets produits par eval_zeroshot_quantile_DtoD.py (suffixe "_quantile")
RESIDUALS_DIR = Path("./data_processing/AnalyseModelsDtoD/residuals_quantile")

HW_DB      = "./data/hydroweb_next.db"
DAHITI_DB  = "./data/dahiti.db"
INSITU_DB  = "./data/insitu_data.db"
INSITU_SHP = "./data/insitu/shp/station_schapi_alti_ref_2025_river.gpkg"

SAT_DB = HW_DB if SOURCE == "hwnext" else DAHITI_DB

DATE_MIN    = "2016-01-01"
DATE_MAX    = "2025-12-31"
DIST_MAX_KM = 50.0
FACC_MAX_RATIO = 2.0
WINDOW_DAYS = 7 if FREQ == "10j" else 14   # tolérance d'alignement temporel insitu
MIN_PAIRS   = 10

OUTPUT_ROOT = Path(f"./data_processing/Modele_quantileLoss/plot/{SOURCE}/{FREQ_LABEL}")
OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)


def normalize_code(c):
    """Convention établie : toujours normaliser les codes station pour matcher DB <-> CSV."""
    return str(int(c))


# ═══════════════════════════════════════════════════════════════
# HELPERS — reconstruction en mètres (std/mean alti depuis la DB)
# ═══════════════════════════════════════════════════════════════
_cache_alti_stats = {}

def get_alti_stats(station_code):
    """Retourne (mean, std) de orthometric_height (is_valid) pour une station, depuis SAT_DB."""
    code_n = normalize_code(station_code)
    if code_n in _cache_alti_stats:
        return _cache_alti_stats[code_n]

    conn = sqlite3.connect(SAT_DB)
    df = None
    for c in [code_n, code_n.zfill(13)]:
        tmp = pd.read_sql(
            """
            SELECT m.orthometric_height AS h
            FROM measurements m
            JOIN stations s ON s.station_code = m.station_code
            WHERE s.station_code = ? AND m.is_valid = 1
            """,
            conn, params=(c,)
        )
        if not tmp.empty:
            df = tmp
            break
    conn.close()

    if df is None or df.empty:
        _cache_alti_stats[code_n] = (None, None)
        return None, None

    mean_alti = float(df["h"].mean())
    std_alti  = float(df["h"].std())
    _cache_alti_stats[code_n] = (mean_alti, std_alti)
    return mean_alti, std_alti


def to_meters(arr_zscore, mean_alti, std_alti):
    return np.asarray(arr_zscore, dtype=float) * std_alti + mean_alti


# ═══════════════════════════════════════════════════════════════
# HELPERS — insitu via connectivité SWORD (comme generalized_top5_zscore_vs_metres.py)
# ═══════════════════════════════════════════════════════════════
print("Chargement SWORD...")
gdf_sword, gdf_sword_proj = load_sword_reaches()
G_sword, info_sword = build_graph(gdf_sword)

print("Chargement shapefile insitu...")
gdf_insitu = gpd.read_file(INSITU_SHP).to_crs("EPSG:4326")
gdf_insitu_proj = gdf_insitu.to_crs("EPSG:2154")
_cache_ins = {}
_cache_coords = {}

def get_coords(station_code):
    code_n = normalize_code(station_code)
    if code_n in _cache_coords:
        return _cache_coords[code_n]
    conn = sqlite3.connect(SAT_DB)
    for c in [code_n, code_n.zfill(13)]:
        df = pd.read_sql(
            "SELECT reference_longitude AS lon, reference_latitude AS lat FROM stations WHERE station_code = ?",
            conn, params=(c,)
        )
        if not df.empty:
            conn.close()
            _cache_coords[code_n] = (float(df.iloc[0]["lon"]), float(df.iloc[0]["lat"]))
            return _cache_coords[code_n]
    conn.close()
    _cache_coords[code_n] = (None, None)
    return None, None


def get_insitu_candidats(lon, lat, dist_max_km):
    pt = gpd.GeoSeries([Point(lon, lat)], crs="EPSG:4326").to_crs("EPSG:2154")[0]
    dist = gdf_insitu_proj.geometry.distance(pt) / 1000
    candidats = dist[dist <= dist_max_km].sort_values()
    return [(gdf_insitu_proj.loc[idx, "code_sta"], d) for idx, d in candidats.items()]


def get_insitu_sword(lon_a, lat_a):
    """Parcourt les candidats insitu par distance croissante (<=DIST_MAX_KM) et
    retourne le premier connecté au réseau SWORD (même bief, sans confluence,
    facc comparable) — pas simplement le plus proche à vol d'oiseau."""
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


def align_insitu(dates, df_ins, window_days):
    wl  = np.full(len(dates), np.nan)
    id_ = np.array(df_ins["date"].values, dtype="datetime64[D]")
    iv  = df_ins["wl"].values
    for i, d in enumerate(np.array(dates, dtype="datetime64[D]")):
        diff = np.abs((id_ - d).astype(float))
        idx  = int(np.argmin(diff))
        if diff[idx] <= window_days:
            wl[i] = iv[idx]
    return wl


def recale_insitu_par_mediane(insitu_wl, alti_metres):
    """Décalage de médiane uniquement (neutralise le datum, préserve l'amplitude).
    Retourne (None, nan) si pas assez de paires — évite de plotter une série
    non recalée qui sortirait du cadre (datum non aligné)."""
    mask = ~(np.isnan(insitu_wl) | np.isnan(alti_metres))
    n_pairs = int(mask.sum())
    if n_pairs < MIN_PAIRS:
        return None, np.nan, n_pairs
    shift = np.nanmedian(alti_metres[mask]) - np.nanmedian(insitu_wl[mask])
    return insitu_wl + shift, shift, n_pairs


# ═══════════════════════════════════════════════════════════════
# CHARGEMENT DES RÉSIDUS + LISTE DE STATIONS
# ═══════════════════════════════════════════════════════════════
def load_residuals(mask):
    csv_path = RESIDUALS_DIR / f"residuals_{SOURCE}_{FREQ}_{mask}pct_quantile.csv"
    if not csv_path.exists():
        print(f"⚠ Fichier introuvable : {csv_path}")
        return None
    df = pd.read_csv(csv_path)
    df["station"] = df["station"].astype(str)
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values("date")


dfs_by_mask = {mask: load_residuals(mask) for mask in MASKS}
dfs_by_mask = {m: df for m, df in dfs_by_mask.items() if df is not None}

if not dfs_by_mask:
    raise SystemExit("Aucun fichier de résidus trouvé — vérifier RESIDUALS_DIR / SOURCE / FREQ.")

if STATIONS is None:
    stations_set = set()
    for df in dfs_by_mask.values():
        stations_set |= set(df["station"].unique())
    stations_list = sorted(stations_set)
else:
    stations_list = [str(s) for s in STATIONS]

print(f"Stations à traiter : {stations_list}\n")

# ═══════════════════════════════════════════════════════════════
# PLOT — un fichier PAR ANNÉE (2 panels empilés), rangé dans un dossier
# par année : OUTPUT_ROOT/{year}/{station}/DtoD{mask}_quantile.png
# Les points obs/insitu hors de l'intervalle [Q05, Q95] sont flagués.
# ═══════════════════════════════════════════════════════════════
OUTLIER_COLOR = "#E24B4A"
OUTLIER_MARKER = "x"

def _plot_quantile_bands(ax, dates, q_m):
    """Bandes de confiance + médiane du modèle."""
    ax.fill_between(dates, q_m["q05"], q_m["q95"], color="#378ADD", alpha=0.15,
                     label="Intervalle Q05–Q95", zorder=1)
    ax.fill_between(dates, q_m["q25"], q_m["q75"], color="#378ADD", alpha=0.30,
                     label="Intervalle Q25–Q75", zorder=2)
    ax.plot(dates, q_m["q50"], color="#185FA5", linewidth=1.8, label="Modèle Q50", zorder=3)


def _flag_outliers(values, q05, q95):
    """Retourne un masque bool : True si value hors [q05, q95] (points non-NaN uniquement)."""
    valid = ~np.isnan(values)
    out = np.zeros(len(values), dtype=bool)
    out[valid] = (values[valid] < q05[valid]) | (values[valid] > q95[valid])
    return out


def plot_station_model(station, mask, df_station, mean_alti, std_alti,
                        insitu_metres_full, dist_km, insitu_code, shift):
    df_station = df_station.copy()
    df_station["year"] = df_station["date"].dt.year
    years = sorted(df_station["year"].unique())

    # Reconstruction en mètres sur toute la série (avant découpe par année)
    obs_m_full = to_meters(df_station["obs"].values, mean_alti, std_alti)
    q_m_full = {q: to_meters(df_station[q].values, mean_alti, std_alti) for q in QUANTILE_COLS}

    has_insitu = insitu_metres_full is not None

    n_saved = 0
    for year in years:
        yr_mask = df_station["year"].values == year
        dates = df_station["date"].values[yr_mask]
        q_m = {q: q_m_full[q][yr_mask] for q in QUANTILE_COLS}

        fig, (ax_alti, ax_ins) = plt.subplots(2, 1, figsize=(12, 9), sharex=True)
        fig.suptitle(f"Station {station} — modèle DtoD{mask} (quantile loss) — {year}",
                     fontsize=13, fontweight="bold")

        # ── Panel du haut : modèle vs altimétrie ──
        _plot_quantile_bands(ax_alti, dates, q_m)
        obs_m = obs_m_full[yr_mask]
        obs_mask = ~np.isnan(obs_m)
        obs_outlier = _flag_outliers(obs_m, q_m["q05"], q_m["q95"])
        n_obs = int(obs_mask.sum())
        n_obs_out = int((obs_outlier & obs_mask).sum())

        ax_alti.scatter(dates[obs_mask & ~obs_outlier], obs_m[obs_mask & ~obs_outlier],
                        color="#D85A30", s=18, label="Altimétrie (obs)", zorder=4)
        ax_alti.scatter(dates[obs_outlier], obs_m[obs_outlier],
                        color=OUTLIER_COLOR, s=45, marker=OUTLIER_MARKER, linewidths=2,
                        label=f"Altimétrie hors [Q05,Q95] (n={n_obs_out})", zorder=5)

        ax_alti.set_title(
            f"Modèle vs Altimétrie — {n_obs_out}/{n_obs} points hors intervalle",
            fontsize=11, fontweight="bold", loc="left"
        )
        ax_alti.legend(fontsize=8, loc="upper right")

        # ── Panel du bas : modèle vs insitu ──
        _plot_quantile_bands(ax_ins, dates, q_m)
        if has_insitu:
            insitu_metres = insitu_metres_full[yr_mask]
            ins_mask = ~np.isnan(insitu_metres)
            ins_outlier = _flag_outliers(insitu_metres, q_m["q05"], q_m["q95"])
            n_ins = int(ins_mask.sum())
            n_ins_out = int((ins_outlier & ins_mask).sum())

            ax_ins.plot(dates[ins_mask], insitu_metres[ins_mask], color="#3B6D11", linewidth=1.3,
                       linestyle="--", label=f"Insitu {insitu_code} (recalée, {dist_km:.1f} km)", zorder=3)
            ax_ins.scatter(dates[ins_outlier], insitu_metres[ins_outlier],
                          color=OUTLIER_COLOR, s=45, marker=OUTLIER_MARKER, linewidths=2,
                          label=f"Insitu hors [Q05,Q95] (n={n_ins_out})", zorder=5)

            ax_ins.set_title(
                f"Modèle vs Insitu — {n_ins_out}/{n_ins} points hors intervalle",
                fontsize=11, fontweight="bold", loc="left"
            )
        else:
            ax_ins.set_title("Modèle vs Insitu (indisponible)", fontsize=11,
                             fontweight="bold", loc="left", color="#888780")
        ax_ins.legend(fontsize=8, loc="upper right")

        for ax in (ax_alti, ax_ins):
            ax.set_ylabel("Hauteur d'eau (m)")
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%b"))
            ax.xaxis.set_major_locator(mdates.MonthLocator())
            ax.grid(True, linestyle="--", alpha=0.4)
        ax_ins.set_xlabel("Date")

        out_dir = OUTPUT_ROOT / station / str(year)
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"DtoD{mask}_quantile.png"
        fig.tight_layout(rect=[0, 0, 1, 0.96])
        fig.savefig(out_path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        n_saved += 1
        print(f"    → {out_path}")

    print(f"    ({n_saved} fichier(s) généré(s) pour {n_saved} année(s))")


for station in stations_list:
    print(f"Station {station}")

    mean_alti, std_alti = get_alti_stats(station)
    if mean_alti is None or std_alti is None or std_alti == 0:
        print("  ⚠ Stats alti (mean/std) introuvables ou std=0 — station ignorée")
        continue

    lon, lat = get_coords(station)
    if lon is None or lat is None:
        print("  ⚠ Coordonnées station introuvables en DB — pas d'insitu associé")
        insitu_code, dist_km = None, np.nan
    else:
        insitu_code, dist_km = get_insitu_sword(lon, lat)
        if insitu_code is None:
            print(f"  ⚠ Aucun insitu connecté (SWORD, <= {DIST_MAX_KM} km, sans confluence, "
                  f"facc_max_ratio<= {FACC_MAX_RATIO}) — non tracé sur les plots")
            dist_km = np.nan
        else:
            print(f"  Insitu connecté (SWORD) : {insitu_code}  (distance = {dist_km:.2f} km)")
            df_ins_check = get_insitu_series(insitu_code)
            if df_ins_check is None:
                print(f"  ⚠ Série insitu {insitu_code} vide ou trop courte (<5 points) — non tracée")
                insitu_code = None

    for mask, df_all in dfs_by_mask.items():
        df_station = df_all[df_all["station"] == station].copy()
        if df_station.empty:
            print(f"  ⚠ DtoD{mask} : aucune donnée pour cette station")
            continue

        missing_cols = [c for c in QUANTILE_COLS if c not in df_station.columns]
        if missing_cols:
            print(f"  ⚠ DtoD{mask} : colonnes quantile manquantes {missing_cols} — plot ignoré")
            continue

        # Alignement insitu sur les dates de CE modèle (les dates peuvent différer selon la station,
        # mais sont identiques entre modèles pour une même station puisque zero-shot sur le même jeu)
        ins_metres_model = None
        if insitu_code is not None:
            obs_metres_tmp = to_meters(df_station["obs"].values, mean_alti, std_alti)
            df_ins = get_insitu_series(insitu_code)
            ins_wl = align_insitu(df_station["date"].values, df_ins, WINDOW_DAYS)
            ins_metres_model, shift, n_pairs = recale_insitu_par_mediane(ins_wl, obs_metres_tmp)
            if ins_metres_model is None:
                print(f"  ⚠ DtoD{mask} : seulement {n_pairs} paire(s) alti/insitu commune(s) "
                      f"(minimum requis = {MIN_PAIRS}) — insitu non tracée pour ce modèle")
            else:
                print(f"  DtoD{mask} : insitu recalée sur {n_pairs} paires, décalage médiane = {shift:+.3f} m")

        plot_station_model(
            station, mask, df_station, mean_alti, std_alti,
            ins_metres_model, dist_km, insitu_code, shift if insitu_code else np.nan,
        )

print(f"\nDone. Plots dans : {OUTPUT_ROOT}/")