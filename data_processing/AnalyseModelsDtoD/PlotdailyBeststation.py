"""
top5_stations_alti_modele_insitu.py
════════════════════════════════════════════════════════════════════════
Sélectionne les 5 stations HW Next où :
  (1) l'alti brute est très similaire à l'insitu (NSE alti vs insitu)
  (2) le MEILLEUR modèle parmi DtoD80/90/96 (loss NSE) est lui aussi très
      similaire à l'insitu (NSE modèle quotidien vs insitu)

Changements vs version précédente :
  - Insitu sélectionné via la méthode SWORD (connectivité réseau, sans
    confluence, ratio facc <= 2.0) — plus seulement "le plus proche".
  - Fenêtre d'alignement alti<->insitu adaptée automatiquement (10j -> 7j
    de tolérance, 27j -> 14j), détectée en regardant dans quel(s)
    fichier(s) de résidus complets (10j ou 27j) la station apparaît.
  - Pour la comparaison modèle<->insitu, les 3 masques DtoD80/90/96 sont
    testés et le MEILLEUR est retenu par station (pas un masque fixe).

Classement final : min(NSE alti-insitu, NSE meilleur-modèle-insitu).

Sources :
  - Alti brute : table measurements de hydroweb_next.db
  - Modèle quotidien (résidus complets, boucle fermée) :
      ./data_processing/AnalyseModelsDtoD/residuals/residuals_hwnext_{freq}_{mask}pct.csv
      pour freq in {10j, 27j}, mask in {80, 90, 96}
  - Insitu : shapefile + insitu_data.db
  - Connectivité : SWORD (Sword_connectivity.py)

Usage :
    python top5_stations_alti_modele_insitu.py
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

# ── Import robuste de Sword_connectivity.py, peu importe d'où le script est lancé ──
# Hypothèse : ce script est dans data_processing/<un_dossier>/ et Sword_connectivity.py
# est dans data_processing/Sword_and_Insitu/ (frères, même parent "data_processing").
# Si ton arborescence est différente, modifie SWORD_MODULE_DIR directement avec le
# chemin absolu (ex: SWORD_MODULE_DIR = Path("/chemin/absolu/vers/Sword_and_Insitu")).
THIS_DIR = Path(__file__).resolve().parent
SWORD_MODULE_DIR = THIS_DIR.parent / "Sword_and_Insitu"   # data_processing/Sword_and_Insitu

if not (SWORD_MODULE_DIR / "Sword_connectivity.py").exists():
    raise SystemExit(
        f"⚠ Sword_connectivity.py introuvable dans {SWORD_MODULE_DIR}\n"
        f"  Corrige la variable SWORD_MODULE_DIR ci-dessus avec le bon chemin absolu."
    )

sys.path.insert(0, str(SWORD_MODULE_DIR))
from Sword_connectivity import load_sword_reaches, build_graph, check_connectivity

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
SOURCE = "dahiti"
MASKS  = [80, 90, 96]          # modèles NSE à comparer (pas les versions RMSE)
FREQS  = ["27j"]               # 27j pas encore disponible pour DAHITI

SAT_DB     = "./data/dahiti.db"
INSITU_DB  = "./data/insitu_data.db"
INSITU_SHP = "./data/insitu/shp/station_schapi_alti_ref_2025_river.gpkg"

RESIDUALS_DIR = Path("./data_processing/AnalyseModelsDtoD/residuals")
OUTPUT_BASE   = Path("./data_processing/AnalyseModelsDtoD/plot_top5_alti_modele_insitu/27J")
OUTPUT_BASE.mkdir(parents=True, exist_ok=True)

DATE_MIN = "2016-01-01"
DATE_MAX = "2025-12-31"
DIST_MAX_KM = 50.0          # rayon de recherche des candidats insitu (pour SWORD)
FACC_MAX_RATIO = 2.0

WINDOW_ALTI_INSITU = {"10j": 7, "27j": 14}
WINDOW_MODEL_DAILY = 1       # alignement strict pour la comparaison quotidienne

MIN_PAIRS_SELECTION = 30
MIN_DAYS_PER_YEAR   = 30
N_TOP = 5

C_ALTI, C_PRED, C_INS = "#5B9BD5", "#c0392b", "#e67e22"

# ═══════════════════════════════════════════════════════════════
# HELPERS — métriques
# ═══════════════════════════════════════════════════════════════
def zscore(arr):
    arr = np.asarray(arr, dtype=float)
    m = ~np.isnan(arr)
    if m.sum() < 2:
        return arr * np.nan
    mu, sig = arr[m].mean(), arr[m].std()
    return (arr - mu) / sig if sig > 0 else arr * 0

def nse(obs, sim):
    m = ~(np.isnan(obs) | np.isnan(sim))
    if m.sum() < 5:
        return np.nan
    o, s = obs[m], sim[m]
    d = np.sum((o - o.mean()) ** 2)
    return float(1 - np.sum((o - s) ** 2) / d) if d > 0 else np.nan

def kge(obs, sim, with_bias=False):
    """KGE — sans biais par défaut (comparaisons alti/modèle vs insitu, référentiels
    altimétriques différents, cf. discussion sur le terme beta non interprétable)."""
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
    wl  = np.full(len(dates), np.nan)
    id_ = np.array(df_ins["date"].values, dtype="datetime64[D]")
    iv  = df_ins["wl"].values
    for i, d in enumerate(np.array(dates, dtype="datetime64[D]")):
        diff = np.abs((id_ - d).astype(float))
        idx  = int(np.argmin(diff))
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

def get_coords_station(code):
    conn = sqlite3.connect(SAT_DB)
    for c in [str(code), str(code).zfill(13)]:
        df = pd.read_sql(
            "SELECT reference_longitude AS lon, reference_latitude AS lat FROM stations WHERE station_code = ?",
            conn, params=(c,)
        )
        if not df.empty:
            conn.close()
            return float(df.iloc[0]["lon"]), float(df.iloc[0]["lat"])
    conn.close()
    return None, None

def normalize_code(c):
    """Les codes station de la table `stations` sont zéro-paddés (ex: 0000000006325),
    alors que les CSV de résidus (issus des listes basin NeuralHydrology) utilisent le
    format court (ex: 6325). On normalise en supprimant les zéros de tête pour pouvoir
    comparer/matcher les deux sources entre elles."""
    s = str(c)
    try:
        return str(int(s))
    except ValueError:
        return s

# ═══════════════════════════════════════════════════════════════
# 1. CHARGEMENT SWORD — UNE SEULE FOIS
# ═══════════════════════════════════════════════════════════════
print("### Chargement SWORD (une seule fois) ###")
gdf_sword, gdf_sword_proj = load_sword_reaches()
G_sword, info_sword = build_graph(gdf_sword)

# ═══════════════════════════════════════════════════════════════
# 2. INSITU — shapefile (géométrie) + accès aux mesures
# ═══════════════════════════════════════════════════════════════
print("\n### Chargement insitu ###")
gdf_insitu = gpd.read_file(INSITU_SHP).to_crs("EPSG:4326")
gdf_insitu_proj = gdf_insitu.to_crs("EPSG:2154")
_cache_ins = {}

def get_insitu_candidats(lon, lat, dist_max_km):
    pt = gpd.GeoSeries([Point(lon, lat)], crs="EPSG:4326").to_crs("EPSG:2154")[0]
    dist = gdf_insitu_proj.geometry.distance(pt) / 1000
    candidats = dist[dist <= dist_max_km].sort_values()
    return [(gdf_insitu_proj.loc[idx, "code_sta"], d,
              gdf_insitu.loc[idx, "geometry"].x, gdf_insitu.loc[idx, "geometry"].y)
            for idx, d in candidats.items()]

def get_insitu_sword(lon_a, lat_a):
    """Premier candidat (par distance croissante) connecté sans confluence et facc ok."""
    for code_ins, dist_km, lon_b, lat_b in get_insitu_candidats(lon_a, lat_a, DIST_MAX_KM):
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
# 3. CHARGEMENT DES RÉSIDUS COMPLETS — 2 FREQ x 3 MASQUES
# ═══════════════════════════════════════════════════════════════
print("\n### Chargement des résidus complets (10j/27j x 80/90/96) ###")
FULL_RESIDUALS = {}   # (freq, mask) -> DataFrame
stations_par_freq = {"10j": set(), "27j": set()}

for freq in FREQS:
    for mask in MASKS:
        path = RESIDUALS_DIR / f"residuals_{SOURCE}_{freq}_{mask}pct.csv"
        if not path.exists():
            print(f"  ⚠ Manquant : {path}")
            continue
        df = pd.read_csv(path)
        df["date"]    = pd.to_datetime(df["date"])
        df["station"] = df["station"].astype(str)
        df = df.dropna(subset=["pred"])
        FULL_RESIDUALS[(freq, mask)] = df
        stations_par_freq[freq] |= set(normalize_code(c) for c in df["station"].unique())
        print(f"  ({freq}, {mask}%) : {len(df)} lignes, {df['station'].nunique()} stations")

ambiguës = stations_par_freq["10j"] & stations_par_freq["27j"]
if ambiguës:
    print(f"\n  ⚠ {len(ambiguës)} station(s) présentes dans 10j ET 27j (on privilégiera 10j) : "
          f"{list(ambiguës)[:5]}{'...' if len(ambiguës) > 5 else ''}")

def get_freq_station(code):
    code_n = normalize_code(code)
    if code_n in stations_par_freq["10j"]:
        return "10j"
    if code_n in stations_par_freq["27j"]:
        return "27j"
    return None

# ═══════════════════════════════════════════════════════════════
# 4. STATIONS HW NEXT
# ═══════════════════════════════════════════════════════════════
print("\n### Chargement des stations DAHITI ###")
conn = sqlite3.connect(SAT_DB)
df_stations = pd.read_sql("""
    SELECT station_code, reference_longitude AS lon, reference_latitude AS lat
    FROM stations
    WHERE reference_longitude IS NOT NULL AND reference_latitude IS NOT NULL
""", conn)
conn.close()
print(f"  {len(df_stations)} stations")

# ═══════════════════════════════════════════════════════════════
# 5. CALCUL DES SCORES PAR STATION
# ═══════════════════════════════════════════════════════════════
print("\n### Calcul des scores (alti-insitu, meilleur modèle-insitu) ###")
rows = []
diag = {
    "total": len(df_stations), "pas_de_freq": 0, "pas_insitu_sword": 0, "insitu_series_vide": 0,
    "alti_trop_court": 0, "alti_pas_assez_pairs": 0, "nse_alti_nan": 0, "aucun_masque_valide": 0,
    "ok": 0,
}

for i, sta in df_stations.iterrows():
    code = sta["station_code"]
    lon_a, lat_a = sta["lon"], sta["lat"]

    freq = get_freq_station(code)
    if freq is None:
        diag["pas_de_freq"] += 1
        continue

    code_ins, dist_km = get_insitu_sword(lon_a, lat_a)
    if code_ins is None:
        diag["pas_insitu_sword"] += 1
        continue

    df_ins = get_insitu_series(code_ins)
    if df_ins is None:
        diag["insitu_series_vide"] += 1
        continue

    df_alti = get_alti_series(code)
    if len(df_alti) < MIN_PAIRS_SELECTION:
        diag["alti_trop_court"] += 1
        continue
    ins_wl_a = align_insitu(df_alti["date"].values, df_ins, WINDOW_ALTI_INSITU[freq])
    if int(np.sum(~np.isnan(ins_wl_a))) < MIN_PAIRS_SELECTION:
        diag["alti_pas_assez_pairs"] += 1
        continue
    nse_alti_ins = nse(zscore(ins_wl_a), zscore(df_alti["wl"].values))
    if np.isnan(nse_alti_ins):
        diag["nse_alti_nan"] += 1
        continue

    code_n = normalize_code(code)
    best_mask, best_nse_model = None, -np.inf
    for mask in MASKS:
        df_m = FULL_RESIDUALS.get((freq, mask))
        if df_m is None:
            continue
        sub = df_m[df_m["station"] == code_n].sort_values("date")
        if len(sub) < MIN_PAIRS_SELECTION:
            continue
        ins_wl_m = align_insitu(sub["date"].values, df_ins, WINDOW_MODEL_DAILY)
        if int(np.sum(~np.isnan(ins_wl_m))) < MIN_PAIRS_SELECTION:
            continue
        nse_m = nse(zscore(ins_wl_m), zscore(sub["pred"].values))
        if not np.isnan(nse_m) and nse_m > best_nse_model:
            best_nse_model, best_mask = nse_m, mask

    if best_mask is None:
        diag["aucun_masque_valide"] += 1
        continue

    diag["ok"] += 1
    rows.append({
        "station": code, "freq": freq, "insitu": code_ins, "dist_km": round(dist_km, 1),
        "nse_alti_insitu": round(nse_alti_ins, 3),
        "best_mask": best_mask, "nse_modele_insitu": round(best_nse_model, 3),
        "score_min": round(min(nse_alti_ins, best_nse_model), 3),
    })

print("\n--- Diagnostic du filtrage ---")
for k, v in diag.items():
    print(f"  {k:<22} : {v}")

if not rows:
    raise SystemExit(
        "\n⚠ Aucune station n'a passé tous les filtres -> voir le diagnostic ci-dessus "
        "pour savoir à quelle étape ça bloque (ex: si 'pas_insitu_sword' est élevé, le "
        "rayon DIST_MAX_KM ou le seuil FACC_MAX_RATIO sont peut-être trop stricts)."
    )

df_scores = pd.DataFrame(rows).sort_values("score_min", ascending=False).reset_index(drop=True)
scores_csv = OUTPUT_BASE / "scores_toutes_stations.csv"
df_scores.to_csv(scores_csv, index=False)

print(f"\n{len(df_scores)} stations évaluées avec succès -> {scores_csv}")
print(f"\nTOP {N_TOP} (classées par min(NSE alti-insitu, NSE meilleur-modèle-insitu)) :\n")
top5 = df_scores.head(N_TOP)
print(top5.to_string(index=False))

# ═══════════════════════════════════════════════════════════════
# 6. PLOTS ANNÉE PAR ANNÉE — TOP 5
# ═══════════════════════════════════════════════════════════════
print(f"\nGénération des figures pour les {len(top5)} meilleures stations...\n")

for _, row in top5.iterrows():
    code, code_ins, freq, mask = row["station"], row["insitu"], row["freq"], row["best_mask"]
    code_n = normalize_code(code)
    df_ins = get_insitu_series(code_ins)
    df_alti = get_alti_series(code)
    sub_model = FULL_RESIDUALS[(freq, mask)]
    sub_model = sub_model[sub_model["station"] == code_n].sort_values("date").reset_index(drop=True)

    ins_wl_a_full = align_insitu(df_alti["date"].values, df_ins, WINDOW_ALTI_INSITU[freq])
    ins_wl_m_full = align_insitu(sub_model["date"].values, df_ins, WINDOW_MODEL_DAILY)

    alti_z, ins_a_z = zscore(df_alti["wl"].values), zscore(ins_wl_a_full)
    pred_z, ins_m_z = zscore(sub_model["pred"].values), zscore(ins_wl_m_full)

    # ── Courbe combinée : obs alti réelle quand disponible, modèle sinon ──────
    # (même référentiel physique alti/modèle -> combinaison directe en unités brutes,
    # pas besoin de re-zscorer séparément avant de combiner)
    has_obs = sub_model["obs"].notna().values if "obs" in sub_model.columns else np.zeros(len(sub_model), dtype=bool)
    combined_raw = np.where(has_obs, sub_model["obs"].values, sub_model["pred"].values)
    combined_z = zscore(combined_raw)

    out_dir = OUTPUT_BASE / code
    out_dir.mkdir(parents=True, exist_ok=True)

    years_alti  = pd.to_datetime(df_alti["date"]).dt.year
    years_model = pd.to_datetime(sub_model["date"]).dt.year
    all_years = sorted(set(years_alti.unique()) | set(years_model.unique()))

    n_figs = 0
    for year in all_years:
        m_a = (years_alti == year).values
        m_m = (years_model == year).values
        if m_a.sum() < MIN_DAYS_PER_YEAR and m_m.sum() < MIN_DAYS_PER_YEAR:
            continue

        nse_a_y = nse(ins_a_z[m_a], alti_z[m_a]) if m_a.sum() >= 5 else np.nan
        kge_a_y = kge(ins_a_z[m_a], alti_z[m_a]) if m_a.sum() >= 5 else np.nan
        nse_m_y = nse(ins_m_z[m_m], pred_z[m_m]) if m_m.sum() >= 5 else np.nan
        kge_m_y = kge(ins_m_z[m_m], pred_z[m_m]) if m_m.sum() >= 5 else np.nan
        nse_c_y = nse(ins_m_z[m_m], combined_z[m_m]) if m_m.sum() >= 5 else np.nan
        kge_c_y = kge(ins_m_z[m_m], combined_z[m_m]) if m_m.sum() >= 5 else np.nan

        def titre(base, n, k):
            extra = []
            if not np.isnan(n): extra.append(f"NSE = {n:.3f}")
            if not np.isnan(k): extra.append(f"KGE = {k:.3f}")
            return base + ("  |  " + "  |  ".join(extra) if extra else "")

        fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(13, 11), sharex=True)

        # Panel 1 — Alti vs Insitu
        ax1.plot(df_alti["date"].values[m_a], alti_z[m_a], "o-", color=C_ALTI, ms=4, lw=1, label="Alti (obs)")
        ax1.plot(df_alti["date"].values[m_a], ins_a_z[m_a], "^-", color=C_INS, ms=4, lw=1, label=f"Insitu {code_ins}")
        ax1.set_title(titre(f"Alti vs Insitu — {year}", nse_a_y, kge_a_y), fontsize=10, fontweight="bold")
        ax1.set_ylabel("WL (z-score)"); ax1.legend(fontsize=8, loc="upper right")
        ax1.grid(True, alpha=0.3); ax1.axhline(0, color="grey", lw=0.6, ls="--")

        # Panel 2 — Modèle (quotidien) vs Insitu
        ax2.plot(sub_model["date"].values[m_m], pred_z[m_m], "-", color=C_PRED, lw=1.2, label=f"Modèle DtoD{mask}% (quotidien)")
        ax2.plot(sub_model["date"].values[m_m], ins_m_z[m_m], "-", color=C_INS, lw=0.8, label=f"Insitu {code_ins}")
        ax2.set_title(titre(f"Modèle (quotidien) vs Insitu — {year}", nse_m_y, kge_m_y), fontsize=10, fontweight="bold")
        ax2.set_ylabel("WL (z-score)")
        ax2.legend(fontsize=8, loc="upper right"); ax2.grid(True, alpha=0.3)
        ax2.axhline(0, color="grey", lw=0.6, ls="--")

        # Panel 3 — Combiné (alti aux dates d'obs, modèle le reste du temps) vs Insitu
        ax3.plot(sub_model["date"].values[m_m], combined_z[m_m], "-", color=C_PRED, lw=1.0,
                 label=f"Modèle (jours sans obs satellite)", zorder=2)
        mask_obs_year = has_obs & m_m
        ax3.scatter(sub_model["date"].values[mask_obs_year], combined_z[mask_obs_year],
                    color=C_ALTI, s=22, zorder=3, label="Alti (jours avec obs satellite)")
        ax3.plot(sub_model["date"].values[m_m], ins_m_z[m_m], "-", color=C_INS, lw=0.8, label=f"Insitu {code_ins}", zorder=1)
        ax3.set_title(titre(f"Combiné (Alti+Modèle) vs Insitu — {year}", nse_c_y, kge_c_y), fontsize=10, fontweight="bold")
        ax3.set_ylabel("WL (z-score)"); ax3.set_xlabel("Date")
        ax3.legend(fontsize=8, loc="upper right"); ax3.grid(True, alpha=0.3)
        ax3.axhline(0, color="grey", lw=0.6, ls="--")
        ax3.xaxis.set_major_formatter(mdates.DateFormatter("%b"))
        ax3.xaxis.set_major_locator(mdates.MonthLocator())
        plt.setp(ax3.xaxis.get_majorticklabels(), rotation=30, ha="right")

        fig.suptitle(f"Station DAHITI {code} ({freq})  —  Insitu SWORD {code_ins} ({row['dist_km']} km)  —  Modèle DtoD{mask}%",
                     fontsize=12, fontweight="bold")
        plt.tight_layout()
        fig_path = out_dir / f"{code}_{year}.png"
        fig.savefig(fig_path, dpi=130, bbox_inches="tight")
        plt.close(fig)
        n_figs += 1

    print(f"  Station {code} ({freq}, insitu SWORD {code_ins}, {row['dist_km']} km, modèle DtoD{mask}%) "
          f": {n_figs} figures -> {out_dir}")

print(f"\nTerminé. Figures dans : {OUTPUT_BASE}/")