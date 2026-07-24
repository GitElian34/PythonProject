"""
plot_consensus_outliers.py
════════════════════════════════════════════════════════════════════════
Parcourt les 3 modèles quantile (DtoD80/90/96) sur les stations DAHITI 27j
déjà évaluées, et détecte les dates où au moins MIN_MODELS_AGREE des 3
modèles flaguent la MÊME valeur altimétrique (obs) comme outlier (hors de
leur intervalle [Q05, Q95] respectif, éventuellement élargi — voir
OUTLIER_MARGIN_ZSCORE ci-dessous).

Pour chaque (station, année) contenant au moins une date de "consensus"
(>= MIN_MODELS_AGREE/3 modèles d'accord), génère les 3 plots (un par
modèle, 2 panels alti/insitu comme d'habitude) et les range dans un
dossier dédié :

    ./data_processing/Modele_quantileLoss/outlier/{SOURCE}_{FREQ}/{station}/{year}/
        DtoD80_quantile.png
        DtoD90_quantile.png
        DtoD96_quantile.png

Les dates de consensus sont mises en évidence par une ligne verticale
grise pointillée sur les 2 panels (alti et insitu), en plus du flag
individuel de chaque modèle (croix rouge sur son propre panel).

Les stations/années SANS consensus ne génèrent aucun fichier — seul le
sous-ensemble "intéressant" (désaccord/outlier partagé) est produit.

MARGE DE TOLÉRANCE (OUTLIER_MARGIN_ZSCORE) :
Un point n'est flagué que s'il dépasse Q05/Q95 d'AU MOINS cette marge,
exprimée en z-score (même espace que les quantiles bruts, avant
conversion en mètres) — PAS en mètres absolus, qui n'aurait pas de sens
comparable d'une station à l'autre. Une fois reconverti en mètres, cette
marge devient automatiquement `marge x std_alti`, donc proportionnelle à
la variabilité propre de chaque station. Défaut = 0.0 (comportement
identique à avant, aucune tolérance). Augmenter cette valeur (ex: 0.1,
0.2, 0.3) réduit le nombre d'outliers détectés en élargissant l'intervalle
effectif [Q05 - marge, Q95 + marge].

Usage :
    python plot_consensus_outliers.py
    (ajuster SOURCE, FREQ, STATIONS, MIN_MODELS_AGREE, OUTLIER_MARGIN_ZSCORE)
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
# PARAMÈTRES GLOBAUX
# ═══════════════════════════════════════════════════════════════
SOURCE = "hwnext"   # "hwnext" ou "dahiti"
FREQ   = "27j"      # "10j" ou "27j"

# Stations à traiter. None = toutes les stations présentes dans les CSV de résidus.
STATIONS = None

MASKS = [80, 90, 96]
QUANTILE_COLS = ["q05", "q25", "q50", "q75", "q95"]

# Nombre minimum de modèles devant flaguer la MÊME date alti comme outlier
MIN_MODELS_AGREE = 3

# Marge de tolérance en z-score (voir explication détaillée en tête de fichier).
# 0.0 = comportement standard (aucune tolérance). Augmenter réduit le nombre d'outliers.
OUTLIER_MARGIN_ZSCORE = 0.2

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
WINDOW_DAYS = 7 if FREQ == "10j" else 14
MIN_PAIRS   = 10

OUTPUT_ROOT = Path(f"./data_processing/Modele_quantileLoss/outlier/{SOURCE}_{FREQ}")
OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

print(f"OUTLIER_MARGIN_ZSCORE = {OUTLIER_MARGIN_ZSCORE}  "
      f"(0.0 = aucune tolérance, intervalle [Q05,Q95] brut)")


def normalize_code(c):
    return str(int(c))


# ═══════════════════════════════════════════════════════════════
# HELPERS — reconstruction en mètres
# ═══════════════════════════════════════════════════════════════
_cache_alti_stats = {}

def get_alti_stats(station_code):
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


def widen_bounds_zscore(q05_z, q95_z, margin):
    """Élargit l'intervalle [Q05, Q95] d'une marge en z-score (même espace que
    les quantiles bruts, avant conversion en mètres). margin=0 -> pas de changement."""
    return np.asarray(q05_z, dtype=float) - margin, np.asarray(q95_z, dtype=float) + margin


# ═══════════════════════════════════════════════════════════════
# HELPERS — insitu via connectivité SWORD
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
    mask = ~(np.isnan(insitu_wl) | np.isnan(alti_metres))
    n_pairs = int(mask.sum())
    if n_pairs < MIN_PAIRS:
        return None, np.nan, n_pairs
    shift = np.nanmedian(alti_metres[mask]) - np.nanmedian(insitu_wl[mask])
    return insitu_wl + shift, shift, n_pairs


def flag_outliers(values, q05, q95):
    valid = ~np.isnan(values)
    out = np.zeros(len(values), dtype=bool)
    out[valid] = (values[valid] < q05[valid]) | (values[valid] > q95[valid])
    return out


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
    return df.sort_values("date").reset_index(drop=True)


dfs_by_mask = {mask: load_residuals(mask) for mask in MASKS}
dfs_by_mask = {m: df for m, df in dfs_by_mask.items() if df is not None}

if len(dfs_by_mask) < MIN_MODELS_AGREE:
    raise SystemExit(f"Pas assez de modèles chargés ({len(dfs_by_mask)}) pour un consensus "
                      f"à {MIN_MODELS_AGREE} — vérifier RESIDUALS_DIR / SOURCE / FREQ.")

if STATIONS is None:
    stations_set = set()
    for df in dfs_by_mask.values():
        stations_set |= set(df["station"].unique())
    stations_list = sorted(stations_set)
else:
    stations_list = [str(s) for s in STATIONS]

print(f"Stations à traiter ({SOURCE} {FREQ}) : {stations_list}\n")

# ═══════════════════════════════════════════════════════════════
# PLOT — mêmes 2 panels que d'habitude, + highlight des dates de consensus
# ═══════════════════════════════════════════════════════════════
OUTLIER_COLOR = "#E24B4A"
OUTLIER_MARKER = "x"
CONSENSUS_LINE_COLOR = "#5F5E5A"

def _plot_quantile_bands(ax, dates, q_m):
    label_q05_q95 = "Intervalle Q05–Q95"
    if OUTLIER_MARGIN_ZSCORE:
        label_q05_q95 += f" ± marge {OUTLIER_MARGIN_ZSCORE}"
    ax.fill_between(dates, q_m["q05"], q_m["q95"], color="#378ADD", alpha=0.15,
                     label=label_q05_q95, zorder=1)
    ax.fill_between(dates, q_m["q25"], q_m["q75"], color="#378ADD", alpha=0.30,
                     label="Intervalle Q25–Q75", zorder=2)
    ax.plot(dates, q_m["q50"], color="#185FA5", linewidth=1.8, label="Modèle Q50", zorder=3)


def plot_station_model_year(station, mask, year, df_station, mean_alti, std_alti,
                             insitu_metres_full, dist_km, insitu_code,
                             consensus_dates_set):
    yr_mask = df_station["date"].dt.year.values == year
    dates = df_station["date"].values[yr_mask]

    # q05/q95 élargis de la marge de tolérance (en z-score) AVANT conversion en mètres —
    # le plot affiche donc directement le seuil effectif utilisé pour flaguer.
    q05_z, q95_z = widen_bounds_zscore(
        df_station["q05"].values[yr_mask], df_station["q95"].values[yr_mask], OUTLIER_MARGIN_ZSCORE
    )
    q_m = {
        "q05": to_meters(q05_z, mean_alti, std_alti),
        "q25": to_meters(df_station["q25"].values[yr_mask], mean_alti, std_alti),
        "q50": to_meters(df_station["q50"].values[yr_mask], mean_alti, std_alti),
        "q75": to_meters(df_station["q75"].values[yr_mask], mean_alti, std_alti),
        "q95": to_meters(q95_z, mean_alti, std_alti),
    }
    obs_m = to_meters(df_station["obs"].values[yr_mask], mean_alti, std_alti)

    fig, (ax_alti, ax_ins) = plt.subplots(2, 1, figsize=(12, 9), sharex=True)
    suptitle = (f"Station {station} — modèle DtoD{mask} (quantile loss) — {year}\n"
                f"Dates avec consensus outlier (≥{MIN_MODELS_AGREE}/3 modèles) : lignes grises")
    if OUTLIER_MARGIN_ZSCORE:
        suptitle += f"  |  marge de tolérance = {OUTLIER_MARGIN_ZSCORE} (z-score)"
    fig.suptitle(suptitle, fontsize=12, fontweight="bold")

    # ── Panel du haut : modèle vs altimétrie ──
    _plot_quantile_bands(ax_alti, dates, q_m)
    obs_mask = ~np.isnan(obs_m)
    obs_outlier = flag_outliers(obs_m, q_m["q05"], q_m["q95"])
    n_obs = int(obs_mask.sum())
    n_obs_out = int((obs_outlier & obs_mask).sum())

    ax_alti.scatter(dates[obs_mask & ~obs_outlier], obs_m[obs_mask & ~obs_outlier],
                    color="#D85A30", s=18, label="Altimétrie (obs)", zorder=4)
    ax_alti.scatter(dates[obs_outlier], obs_m[obs_outlier],
                    color=OUTLIER_COLOR, s=45, marker=OUTLIER_MARKER, linewidths=2,
                    label=f"Altimétrie hors [Q05,Q95] (n={n_obs_out})", zorder=5)
    ax_alti.set_title(f"Modèle vs Altimétrie — {n_obs_out}/{n_obs} points hors intervalle",
                      fontsize=11, fontweight="bold", loc="left")

    # ── Panel du bas : modèle vs insitu ──
    _plot_quantile_bands(ax_ins, dates, q_m)
    has_insitu = insitu_metres_full is not None
    if has_insitu:
        insitu_metres = insitu_metres_full[yr_mask]
        ins_mask = ~np.isnan(insitu_metres)
        ins_outlier = flag_outliers(insitu_metres, q_m["q05"], q_m["q95"])
        n_ins = int(ins_mask.sum())
        n_ins_out = int((ins_outlier & ins_mask).sum())

        ax_ins.plot(dates[ins_mask], insitu_metres[ins_mask], color="#3B6D11", linewidth=1.3,
                   linestyle="--", label=f"Insitu {insitu_code} (recalée, {dist_km:.1f} km)", zorder=3)
        ax_ins.scatter(dates[ins_outlier], insitu_metres[ins_outlier],
                      color=OUTLIER_COLOR, s=45, marker=OUTLIER_MARKER, linewidths=2,
                      label=f"Insitu hors [Q05,Q95] (n={n_ins_out})", zorder=5)
        ax_ins.set_title(f"Modèle vs Insitu — {n_ins_out}/{n_ins} points hors intervalle",
                         fontsize=11, fontweight="bold", loc="left")
    else:
        ax_ins.set_title("Modèle vs Insitu (indisponible)", fontsize=11,
                         fontweight="bold", loc="left", color="#888780")

    # ── Highlight des dates de consensus (>=N modèles d'accord) sur les 2 panels ──
    dates_dt = pd.to_datetime(dates)
    for d in dates_dt:
        if d.normalize() in consensus_dates_set:
            ax_alti.axvline(d, color=CONSENSUS_LINE_COLOR, linestyle=":", linewidth=1.0, zorder=0)
            ax_ins.axvline(d, color=CONSENSUS_LINE_COLOR, linestyle=":", linewidth=1.0, zorder=0)

    for ax in (ax_alti, ax_ins):
        ax.set_ylabel("Hauteur d'eau (m)")
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b"))
        ax.xaxis.set_major_locator(mdates.MonthLocator())
        ax.grid(True, linestyle="--", alpha=0.4)
        ax.legend(fontsize=8, loc="upper right")
    ax_ins.set_xlabel("Date")

    out_dir = OUTPUT_ROOT / station / str(year)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"DtoD{mask}_quantile.png"
    fig.tight_layout(rect=[0, 0, 1, 0.94])
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return out_path


# ═══════════════════════════════════════════════════════════════
# BOUCLE PRINCIPALE — détection du consensus puis génération ciblée
# ═══════════════════════════════════════════════════════════════
n_station_years_generated = 0

for station in stations_list:
    print(f"Station {station}")

    mean_alti, std_alti = get_alti_stats(station)
    if mean_alti is None or std_alti is None or std_alti == 0:
        print("  ⚠ Stats alti (mean/std) introuvables ou std=0 — station ignorée")
        continue

    # ── Récupère df_station par modèle, vérifie cohérence des dates/obs ──
    dfs_station = {}
    for mask, df_all in dfs_by_mask.items():
        df_station = df_all[df_all["station"] == station].copy()
        if df_station.empty:
            print(f"  ⚠ DtoD{mask} : aucune donnée pour cette station — modèle exclu du consensus")
            continue
        missing_cols = [c for c in QUANTILE_COLS if c not in df_station.columns]
        if missing_cols:
            print(f"  ⚠ DtoD{mask} : colonnes quantile manquantes — modèle exclu du consensus")
            continue
        dfs_station[mask] = df_station.reset_index(drop=True)

    if len(dfs_station) < MIN_MODELS_AGREE:
        print(f"  ⚠ Moins de {MIN_MODELS_AGREE} modèles disponibles pour cette station — ignorée")
        continue

    # ── Construit un DataFrame commun indexé par date, avec le flag outlier de chaque modèle ──
    # (bornes Q05/Q95 élargies de la marge de tolérance, en z-score, avant conversion en mètres)
    merged = None
    for mask, df_station in dfs_station.items():
        obs_m = to_meters(df_station["obs"].values, mean_alti, std_alti)
        q05_z, q95_z = widen_bounds_zscore(df_station["q05"].values, df_station["q95"].values, OUTLIER_MARGIN_ZSCORE)
        q05_m = to_meters(q05_z, mean_alti, std_alti)
        q95_m = to_meters(q95_z, mean_alti, std_alti)
        flag = flag_outliers(obs_m, q05_m, q95_m) & ~np.isnan(obs_m)

        tmp = pd.DataFrame({
            "date": df_station["date"].values,
            f"flag_{mask}": flag,
        })
        merged = tmp if merged is None else merged.merge(tmp, on="date", how="outer")

    flag_cols = [c for c in merged.columns if c.startswith("flag_")]
    merged[flag_cols] = merged[flag_cols].fillna(False)
    merged["n_agree"] = merged[flag_cols].sum(axis=1)
    consensus_rows = merged[merged["n_agree"] >= MIN_MODELS_AGREE]

    if consensus_rows.empty:
        print(f"  Aucune date avec ≥{MIN_MODELS_AGREE}/3 modèles d'accord — station ignorée")
        continue

    consensus_dates_set = set(pd.to_datetime(consensus_rows["date"]).dt.normalize())
    years_with_consensus = sorted(pd.to_datetime(consensus_rows["date"]).dt.year.unique())
    print(f"  {len(consensus_rows)} date(s) de consensus (≥{MIN_MODELS_AGREE}/3) sur les années : "
          f"{list(years_with_consensus)}")

    # ── Insitu (une fois par station, réutilisé pour tous les modèles/années) ──
    lon, lat = get_coords(station)
    insitu_code, dist_km = None, np.nan
    if lon is not None and lat is not None:
        insitu_code, dist_km = get_insitu_sword(lon, lat)
        if insitu_code is None:
            print(f"  ⚠ Aucun insitu connecté (SWORD, <= {DIST_MAX_KM} km)")
        elif get_insitu_series(insitu_code) is None:
            print(f"  ⚠ Série insitu {insitu_code} trop courte — ignorée")
            insitu_code = None
        else:
            print(f"  Insitu connecté (SWORD) : {insitu_code}  (distance = {dist_km:.2f} km)")

    # ── Génère les plots UNIQUEMENT pour les années avec consensus, pour les 3 modèles ──
    for year in years_with_consensus:
        for mask, df_station in dfs_station.items():
            obs_m_full = to_meters(df_station["obs"].values, mean_alti, std_alti)

            ins_metres_full = None
            if insitu_code is not None:
                df_ins = get_insitu_series(insitu_code)
                ins_wl = align_insitu(df_station["date"].values, df_ins, WINDOW_DAYS)
                ins_metres_full, shift, n_pairs = recale_insitu_par_mediane(ins_wl, obs_m_full)

            out_path = plot_station_model_year(
                station, mask, year, df_station, mean_alti, std_alti,
                ins_metres_full, dist_km, insitu_code, consensus_dates_set,
            )
            print(f"    → {out_path}")
        n_station_years_generated += 1

print(f"\n{'='*60}")
print(f"  {n_station_years_generated} combinaison(s) (station, année) avec consensus "
      f"(≥{MIN_MODELS_AGREE}/3 modèles) générée(s)")
print(f"  Marge de tolérance appliquée : {OUTLIER_MARGIN_ZSCORE} (z-score)")
print(f"  Sorties dans : {OUTPUT_ROOT}/")
print("Done")