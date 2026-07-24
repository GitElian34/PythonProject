"""
metrics_per_timestep_vs_insitu.py
------------------------------------
Pour un modèle donné (predict_last_n > 1), calcule le NSE/KGE
SÉPARÉMENT pour chaque position time_step de la fenêtre (J+0 le plus
ancien, dernier = nowcast), en comparant chaque fois la prédiction à
la vraie valeur IN SITU la plus proche -- pas à l'observation
satellite -- pour vérifier si la performance se dégrade réellement
selon la position dans la fenêtre, avec une vérité terrain
indépendante.

Usage :
    Ajuster RUN_NAME/EPOCH/SOURCE ci-dessous puis :
    python metrics_per_timestep_vs_insitu.py
"""

import pickle
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point


# ──────────────────────────────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────────────────────────────

RUNS_ROOT = Path("./runs")
RUN_NAME = "arlstm_DtoD90_last10_attention_1707_154232"
EPOCH = 28

FREQ_KEY = "1D"
TARGET_VAR_SIM = "water_level_sim"

SOURCE = "dahiti"  # confirmé fonctionnel pour ces modèles (87 stations matchées dans le script précédent)
HW_DB = "./data/hydroweb_next.db"
DAHITI_DB = "./data/dahiti.db"
INSITU_DB = "./data/insitu_data.db"
INSITU_SHP = "./data/insitu/shp/station_schapi_alti_ref_2025_river.gpkg"

SAT_DB = HW_DB if SOURCE == "hwnext" else DAHITI_DB

DATE_MIN = "2016-01-01"
DATE_MAX = "2025-12-31"
DIST_MAX_KM = 10.0
WINDOW_DAYS = 14  # tolérance d'alignement temporel insitu
MIN_PAIRS_PER_STATION_POSITION = 10

OUT_DIR = Path("./data_processing/Modele_predict_last_n/per_timestep_vs_insitu")
OUT_DIR.mkdir(parents=True, exist_ok=True)


# ──────────────────────────────────────────────────────────────────────────────
# MÉTRIQUES (convention du projet : KGE sans beta, référentiels différents)
# ──────────────────────────────────────────────────────────────────────────────

def nse(obs, sim):
    mask = ~np.isnan(obs) & ~np.isnan(sim)
    obs, sim = obs[mask], sim[mask]
    if len(obs) < 2:
        return np.nan
    denom = np.sum((obs - obs.mean()) ** 2)
    if denom == 0:
        return np.nan
    return 1 - np.sum((obs - sim) ** 2) / denom


def kge_no_beta(obs, sim):
    mask = ~np.isnan(obs) & ~np.isnan(sim)
    obs, sim = obs[mask], sim[mask]
    if len(obs) < 2 or obs.std() == 0 or sim.std() == 0:
        return np.nan
    r = np.corrcoef(obs, sim)[0, 1]
    alpha = sim.std() / obs.std()
    return 1 - np.sqrt((r - 1) ** 2 + (alpha - 1) ** 2)


def zscore(arr):
    arr = np.asarray(arr, dtype=float)
    m = ~np.isnan(arr)
    if m.sum() < 2:
        return arr * np.nan
    mu, sig = arr[m].mean(), arr[m].std()
    return (arr - mu) / sig if sig > 0 else arr * 0


# ──────────────────────────────────────────────────────────────────────────────
# ACCÈS INSITU / COORDS (insitu le plus proche, cf. conventions du projet)
# ──────────────────────────────────────────────────────────────────────────────

print("Chargement shapefile insitu...")
gdf_insitu = gpd.read_file(INSITU_SHP).to_crs("EPSG:2154")
_cache_coords = {}
_cache_insitu_series = {}
_cache_station_insitu_match = {}


def get_coords(code):
    if code in _cache_coords:
        return _cache_coords[code]
    conn = sqlite3.connect(SAT_DB)
    for c in [str(code), str(code).zfill(13)]:
        df = pd.read_sql(
            "SELECT reference_longitude AS lon, reference_latitude AS lat FROM stations WHERE station_code = ?",
            conn, params=(c,)
        )
        if not df.empty:
            conn.close()
            _cache_coords[code] = (float(df.iloc[0]["lon"]), float(df.iloc[0]["lat"]))
            return _cache_coords[code]
    conn.close()
    _cache_coords[code] = (None, None)
    return None, None


def get_insitu_proche(lon, lat):
    pt = gpd.GeoSeries([Point(lon, lat)], crs="EPSG:4326").to_crs("EPSG:2154")[0]
    dist = gdf_insitu.geometry.distance(pt)
    idx = dist.idxmin()
    return gdf_insitu.loc[idx, "code_sta"], dist[idx] / 1000


def get_insitu_series(code_sta):
    if code_sta not in _cache_insitu_series:
        conn = sqlite3.connect(INSITU_DB)
        df = pd.read_sql("""
            SELECT date, h_med_wsh AS wl FROM mesures_insitu
            WHERE code_sta = ? AND date >= ? AND date <= ?
            ORDER BY date
        """, conn, params=(code_sta, DATE_MIN, DATE_MAX))
        conn.close()
        df["date"] = pd.to_datetime(df["date"])
        df = df.dropna(subset=["wl"]).drop_duplicates(subset=["date"])
        _cache_insitu_series[code_sta] = df.set_index("date")["wl"].sort_index() if len(df) >= 5 else None
    return _cache_insitu_series[code_sta]


def get_station_insitu_match(station_id: str):
    """Retourne (code_insitu, dist_km, série_insitu) pour une station, avec cache."""
    if station_id in _cache_station_insitu_match:
        return _cache_station_insitu_match[station_id]

    lon, lat = get_coords(station_id)
    if lon is None:
        _cache_station_insitu_match[station_id] = (None, None, None)
        return _cache_station_insitu_match[station_id]

    code_ins, dist_km = get_insitu_proche(lon, lat)
    if dist_km > DIST_MAX_KM:
        _cache_station_insitu_match[station_id] = (code_ins, dist_km, None)
        return _cache_station_insitu_match[station_id]

    series = get_insitu_series(code_ins)
    _cache_station_insitu_match[station_id] = (code_ins, dist_km, series)
    return _cache_station_insitu_match[station_id]


def align_insitu_fast(dates: pd.DatetimeIndex, insitu_series: pd.Series, window_days: int) -> np.ndarray:
    """Aligne rapidement (nearest, avec tolérance) via pandas reindex."""
    if insitu_series is None or len(insitu_series) == 0:
        return np.full(len(dates), np.nan)
    aligned = insitu_series.reindex(dates, method="nearest", tolerance=pd.Timedelta(days=window_days))
    return aligned.values.astype(float)


# ──────────────────────────────────────────────────────────────────────────────
# CHARGEMENT DES RÉSULTATS
# ──────────────────────────────────────────────────────────────────────────────

def load_results(run_name: str, epoch: int) -> dict:
    p_path = RUNS_ROOT / run_name / "validation" / f"model_epoch{epoch:03d}" / "validation_results.p"
    print(f"[INFO] Chargement : {p_path}")
    with open(p_path, "rb") as f:
        return pickle.load(f)


# ──────────────────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────────────────

def main():
    results = load_results(RUN_NAME, EPOCH)
    stations = list(results.keys())
    print(f"[INFO] {len(stations)} stations dans le run")

    # Détermine n_timesteps + offsets à partir de la première station valide
    n_timesteps = None
    time_step_offsets = None
    for station_id in stations:
        try:
            ds = results[station_id][FREQ_KEY]["xr"]
            n_timesteps = ds.sizes["time_step"]
            time_step_offsets = ds.coords["time_step"].values  # ex: [-9..0] ou [0..9]
            break
        except (KeyError, TypeError):
            continue
    if n_timesteps is None:
        raise RuntimeError("Impossible de déterminer time_step depuis les résultats.")
    print(f"[INFO] {n_timesteps} positions time_step, offsets = {time_step_offsets}")

    # Structure d'accumulation : par position, une liste de (nse, kge) par station
    per_position_station_metrics = {t: {"nse": [], "kge": [], "n_pairs": []} for t in range(n_timesteps)}

    # ── DIAGNOSTIC : pourquoi le matching insitu échoue-t-il ? ──────────
    print("\n" + "=" * 90)
    print("DIAGNOSTIC — matching insitu sur les 10 premières stations")
    print("=" * 90)
    for station_id in stations[:10]:
        lon, lat = get_coords(str(station_id))
        if lon is None:
            print(f"  {station_id:<20} -> AUCUNE coordonnée trouvée dans {SAT_DB} (station_code absent de la table 'stations' ?)")
            continue
        code_ins, dist_km = get_insitu_proche(lon, lat)
        flag = "OK" if dist_km <= DIST_MAX_KM else f"TROP LOIN (> {DIST_MAX_KM}km)"
        print(f"  {station_id:<20} -> lon={lon:.4f} lat={lat:.4f}  |  insitu le plus proche : "
              f"{code_ins} à {dist_km:.1f}km  [{flag}]")
    print("=" * 90 + "\n")

    n_stations_with_insitu = 0
    for i, station_id in enumerate(stations):
        try:
            ds = results[station_id][FREQ_KEY]["xr"]
        except (KeyError, TypeError):
            continue
        if TARGET_VAR_SIM not in ds.data_vars:
            continue

        code_ins, dist_km, insitu_series = get_station_insitu_match(str(station_id))
        if insitu_series is None:
            continue
        n_stations_with_insitu += 1

        anchor_dates = pd.to_datetime(ds.coords["date"].values)
        sim_all = ds[TARGET_VAR_SIM].values  # (date, time_step)

        for t in range(n_timesteps):
            offset_days = int(time_step_offsets[t]) if not isinstance(time_step_offsets[t], np.timedelta64) \
                else pd.Timedelta(time_step_offsets[t]).days
            calendar_dates = anchor_dates + pd.to_timedelta(offset_days, unit="D")

            sim_t = sim_all[:, t]
            insitu_t = align_insitu_fast(calendar_dates, insitu_series, WINDOW_DAYS)

            both_valid = ~np.isnan(sim_t) & ~np.isnan(insitu_t)
            n_pairs = int(both_valid.sum())
            if n_pairs < MIN_PAIRS_PER_STATION_POSITION:
                continue

            sim_z = zscore(sim_t[both_valid])
            ins_z = zscore(insitu_t[both_valid])

            per_position_station_metrics[t]["nse"].append(nse(ins_z, sim_z))
            per_position_station_metrics[t]["kge"].append(kge_no_beta(ins_z, sim_z))
            per_position_station_metrics[t]["n_pairs"].append(n_pairs)

        if (i + 1) % 50 == 0:
            print(f"[INFO] {i + 1}/{len(stations)} stations traitées...")

    print(f"\n[INFO] {n_stations_with_insitu} stations avec un insitu à moins de {DIST_MAX_KM} km")

    # ── Résumé par position ────────────────────────────────────────────
    rows = []
    for t in range(n_timesteps):
        offset_days = int(time_step_offsets[t]) if not isinstance(time_step_offsets[t], np.timedelta64) \
            else pd.Timedelta(time_step_offsets[t]).days
        nse_vals = np.array(per_position_station_metrics[t]["nse"])
        kge_vals = np.array(per_position_station_metrics[t]["kge"])
        n_pairs_vals = per_position_station_metrics[t]["n_pairs"]
        rows.append({
            "time_step": t,
            "offset_jours": offset_days,
            "n_stations": len(nse_vals),
            "NSE_median": np.nanmedian(nse_vals) if len(nse_vals) else np.nan,
            "KGE_median": np.nanmedian(kge_vals) if len(kge_vals) else np.nan,
            "n_pairs_median": np.median(n_pairs_vals) if n_pairs_vals else np.nan,
        })

    df_summary = pd.DataFrame(rows)
    out_csv = OUT_DIR / f"{RUN_NAME}_epoch{EPOCH}_per_timestep_vs_insitu.csv"
    df_summary.to_csv(out_csv, index=False)

    print("\n" + "=" * 90)
    print(f"NSE/KGE PAR POSITION DANS LA FENÊTRE, VS INSITU LE PLUS PROCHE")
    print(f"Run : {RUN_NAME} (epoch {EPOCH})")
    print("=" * 90)
    print(df_summary.to_string(index=False, float_format=lambda x: f"{x:.4f}"))
    print("=" * 90)

    last_t = n_timesteps - 1
    first_row = df_summary.iloc[0]
    last_row = df_summary.iloc[last_t]
    print(f"\n[COMPARAISON] Position la plus ancienne (offset {first_row['offset_jours']:.0f}j) "
          f"vs nowcast (offset {last_row['offset_jours']:.0f}j) :")
    print(f"  NSE : {first_row['NSE_median']:.4f}  ->  {last_row['NSE_median']:.4f}  "
          f"(delta = {last_row['NSE_median'] - first_row['NSE_median']:+.4f})")
    print(f"  KGE : {first_row['KGE_median']:.4f}  ->  {last_row['KGE_median']:.4f}  "
          f"(delta = {last_row['KGE_median'] - first_row['KGE_median']:+.4f})")
    print(f"\n[LECTURE] Si les valeurs restent stables (delta proche de 0) même face à l'insitu, "
          f"ça confirme -- avec une vérité terrain indépendante cette fois -- que la position dans "
          f"la fenêtre n'a pas d'effet de dégradation façon horizon de prévision.")

    print(f"\n[OK] Détail sauvegardé : {out_csv}")


if __name__ == "__main__":
    main()