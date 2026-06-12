"""
1_collect_dahiti.py
═══════════════════════════════════════════════════════════════════════════
Collecte toutes les stations DAHITI en France métropolitaine,
télécharge les séries et calcule le NSE alti↔insitu pour chacune.

Sorties :
  - dahiti_stations_france.csv   : liste des stations DAHITI France
  - dahiti_nse_raw.csv           : NSE par station + métadonnées
═══════════════════════════════════════════════════════════════════════════
"""

import requests, json, sqlite3, time
import pandas as pd
import numpy as np
import geopandas as gpd
from pathlib import Path
from shapely.geometry import Point

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
API_KEY        = "D0EBD81E7279ACA2C6597A8C5153E8B20013DF650855CB39B19695C8E80BB484"
BASE_URL       = "https://dahiti.dgfi.tum.de/api/v2/"

INSITU_DB_PATH = "./data/insitu_data.db"
INSITU_SHP     = "./data/insitu/shp/station_schapi_alti_ref_2025_river.gpkg"

OUTPUT_DIR     = Path("./data/outlier_detection/dahiti")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

DIST_MAX_KM    = 50.0
DATE_MIN       = "2016-01-01"
DATE_MAX       = "2025-12-31"
PAUSE_API      = 0.25

# ═══════════════════════════════════════════════════════════════
# HELPERS API
# ═══════════════════════════════════════════════════════════════
def dahiti_post(endpoint, args):
    args['api_key'] = API_KEY
    try:
        r = requests.post(BASE_URL + endpoint, json=args, timeout=30)
        if r.status_code == 200:
            return json.loads(r.text)
        print(f"  ⚠️  HTTP {r.status_code} — {r.text[:200]}")
        return {}
    except Exception as e:
        print(f"  ❌ {endpoint} : {e}")
        return {}

# ═══════════════════════════════════════════════════════════════
# INSITU
# ═══════════════════════════════════════════════════════════════
print("Chargement shapefile insitu...")
gdf_proj = gpd.read_file(INSITU_SHP).to_crs("EPSG:2154")
_cache: dict = {}

def get_insitu_proche(lon, lat):
    pt   = gpd.GeoSeries([Point(lon, lat)], crs="EPSG:4326").to_crs("EPSG:2154")[0]
    dist = gdf_proj.geometry.distance(pt)
    idx  = dist.idxmin()
    return gdf_proj.loc[idx, 'code_sta'], dist[idx] / 1000

def get_insitu_series(code_sta):
    if code_sta not in _cache:
        conn = sqlite3.connect(INSITU_DB_PATH)
        df = pd.read_sql_query("""
            SELECT date, h_med_wsh FROM mesures_insitu
            WHERE code_sta = ? AND date >= ? AND date <= ?
            ORDER BY date
        """, conn, params=(code_sta, DATE_MIN, DATE_MAX))
        conn.close()
        df['date'] = pd.to_datetime(df['date'])
        df = df.dropna(subset=['h_med_wsh'])
        _cache[code_sta] = df if len(df) >= 5 else None
    return _cache[code_sta]

# ═══════════════════════════════════════════════════════════════
# NSE
# ═══════════════════════════════════════════════════════════════
def zscore(arr):
    mu, sig = np.nanmean(arr), np.nanstd(arr)
    return (arr - mu) / sig if sig > 0 else np.zeros_like(arr)

def align_and_nse(dates_alti, wl_alti, df_insitu, window_days=14):
    insitu_wl = np.full(len(dates_alti), np.nan)
    for i, d in enumerate(pd.to_datetime(dates_alti)):
        diff = (df_insitu['date'] - d).abs()
        idx  = diff.idxmin()
        if diff[idx] <= pd.Timedelta(days=window_days):
            insitu_wl[i] = df_insitu.loc[idx, 'h_med_wsh']
    mask = ~(np.isnan(wl_alti) | np.isnan(insitu_wl))
    n    = int(mask.sum())
    if n < 5:
        return np.nan, n
    o = zscore(wl_alti[mask])
    s = zscore(insitu_wl[mask])
    d = np.sum((o - o.mean()) ** 2)
    return (1 - np.sum((o - s) ** 2) / d if d > 0 else np.nan), n

# ═══════════════════════════════════════════════════════════════
# ÉTAPE 1 : STATIONS DAHITI FRANCE
# ═══════════════════════════════════════════════════════════════
print("\nRécupération stations DAHITI Europe...")
resp = dahiti_post("list-targets/", {'continent': 'Europe'})
all_stations = resp.get('data', [])
print(f"  Europe total : {len(all_stations)}")

stations_fr = [
    s for s in all_stations
    if s.get('country', '').lower() == 'france'
    and s.get('data_access', {}).get('water_level_altimetry') == 'public'
]
print(f"  France avec altimétrie publique : {len(stations_fr)}")
print(f"  Types : {dict(pd.Series([s.get('type','?') for s in stations_fr]).value_counts())}")

df_sta = pd.DataFrame(stations_fr)
df_sta.to_csv(OUTPUT_DIR / "dahiti_stations_france.csv", index=False)
print(f"✅ dahiti_stations_france.csv exporté")

# ═══════════════════════════════════════════════════════════════
# ÉTAPE 2 : TÉLÉCHARGEMENT + NSE
# ═══════════════════════════════════════════════════════════════
print(f"\nTéléchargement + NSE ({len(stations_fr)} stations)...")

results = []
for i, sta in enumerate(stations_fr):
    dahiti_id = sta['dahiti_id']
    lon, lat  = float(sta['longitude']), float(sta['latitude'])
    name      = sta.get('target_name', f'ID_{dahiti_id}')
    typ       = sta.get('type', '?')

    code_ins, dist_km = get_insitu_proche(lon, lat)
    if dist_km > DIST_MAX_KM:
        results.append({'dahiti_id': dahiti_id, 'name': name, 'type': typ,
                        'lon': lon, 'lat': lat, 'code_insitu': code_ins,
                        'dist_km': dist_km, 'n_pairs': 0,
                        'nse_dahiti': np.nan, 'unc_median': np.nan,
                        'status': 'insitu_trop_loin'})
        continue

    df_ins = get_insitu_series(code_ins)
    if df_ins is None:
        results.append({'dahiti_id': dahiti_id, 'name': name, 'type': typ,
                        'lon': lon, 'lat': lat, 'code_insitu': code_ins,
                        'dist_km': dist_km, 'n_pairs': 0,
                        'nse_dahiti': np.nan, 'unc_median': np.nan,
                        'status': 'insitu_vide'})
        continue

    time.sleep(PAUSE_API)
    resp2 = dahiti_post("download-water-level/", {'dahiti_id': dahiti_id, 'format': 'json'})
    serie = resp2.get('data', [])
    if not serie:
        results.append({'dahiti_id': dahiti_id, 'name': name, 'type': typ,
                        'lon': lon, 'lat': lat, 'code_insitu': code_ins,
                        'dist_km': dist_km, 'n_pairs': 0,
                        'nse_dahiti': np.nan, 'unc_median': np.nan,
                        'status': 'dahiti_vide'})
        continue

    df_wl = pd.DataFrame(serie)
    df_wl['datetime'] = pd.to_datetime(df_wl['datetime'])
    df_wl = df_wl[(df_wl['datetime'] >= DATE_MIN) & (df_wl['datetime'] <= DATE_MAX)]
    df_wl = df_wl.dropna(subset=['wse'])

    if len(df_wl) < 5:
        results.append({'dahiti_id': dahiti_id, 'name': name, 'type': typ,
                        'lon': lon, 'lat': lat, 'code_insitu': code_ins,
                        'dist_km': dist_km, 'n_pairs': len(df_wl),
                        'nse_dahiti': np.nan, 'unc_median': np.nan,
                        'status': 'trop_peu_mesures'})
        continue

    nse, n_pairs  = align_and_nse(df_wl['datetime'].values, df_wl['wse'].values, df_ins)
    unc_median    = float(df_wl['wse_u'].median()) if 'wse_u' in df_wl.columns else np.nan
    flag          = f"NSE={nse:.3f}" if not np.isnan(nse) else "NSE=NaN"

    print(f"  [{i+1:3d}/{len(stations_fr)}] {name:45s} {typ:10s} | "
          f"n={n_pairs:3d} | {flag} | unc={unc_median:.3f}m | {dist_km:.1f}km")

    results.append({'dahiti_id': dahiti_id, 'name': name, 'type': typ,
                    'lon': lon, 'lat': lat, 'code_insitu': code_ins,
                    'dist_km': dist_km, 'n_pairs': n_pairs,
                    'nse_dahiti': nse, 'unc_median': unc_median,
                    'status': 'ok'})

df_res = pd.DataFrame(results)
df_res.to_csv(OUTPUT_DIR / "dahiti_nse_raw.csv", index=False)

# ═══════════════════════════════════════════════════════════════
# SYNTHÈSE
# ═══════════════════════════════════════════════════════════════
df_ok  = df_res[df_res['status'] == 'ok'].dropna(subset=['nse_dahiti'])
nse_ok = df_ok['nse_dahiti']

print("\n" + "═"*55)
print(f"DAHITI France — stations valides : {len(nse_ok)}")
print(f"  NSE médian : {nse_ok.median():.3f}")
print(f"  NSE moyen  : {nse_ok.mean():.3f}")
print(f"  NSE > 0.5  : {(nse_ok > 0.5).sum()}")
print(f"  NSE < 0    : {(nse_ok < 0).sum()}")
print(f"\n  Par type :")
for typ, grp in df_ok.groupby('type'):
    n = grp['nse_dahiti'].dropna()
    print(f"    {typ:12s} | n={len(n):3d} | NSE médian={n.median():.3f}")
print(f"\n  Statuts :")
print(df_res['status'].value_counts().to_string())
print(f"\n✅ dahiti_nse_raw.csv → {OUTPUT_DIR}")