"""
generate_cartes_stations_27j.py
═══════════════════════════════════════════════════════════════════════════
Génère une carte Folium HTML par station satellite 27j.
Chaque carte montre uniquement la station alti + sa station insitu associée.

Lit les résidus depuis le CSV déjà calculé par zeroshot_eval_outliers_27j.py

Produit :
  ./figures_zeroshot_satellite/<MODEL>/Outlier_27j/<station>/carte_<station>.html
═══════════════════════════════════════════════════════════════════════════
"""

import sqlite3
import numpy as np
import pandas as pd
import geopandas as gpd
import folium
from pathlib import Path
from shapely.geometry import Point

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
MODEL  = "arlstm_feat27jHigh_modele2_2205_152119"
EPOCH  = 5
PERIOD = "validation"

RUN_DIR       = Path(f"./runs/{MODEL}")
METRICS_CSV   = RUN_DIR / PERIOD / f"model_epoch{EPOCH:03d}" / f"{PERIOD}_metrics.csv"
CSV_RESIDUALS = Path("./data/outlier_detection/residuals_27j_all_stations.csv")
OUT_PLOTS     = Path(f"./figures_zeroshot_satellite/{MODEL}/Outlier_27j")

HYDRO_DB_PATH = "./data/hydro_data.db"
INSITU_SHP    = "./data/insitu/shp/station_schapi_alti_ref_2025_river.gpkg"

DIST_MAX_KM = 50.0


# ═══════════════════════════════════════════════════════════════
# CHARGEMENT
# ═══════════════════════════════════════════════════════════════
print("Chargement stations insitu...")
gdf_insitu      = gpd.read_file(INSITU_SHP)
gdf_insitu_proj = gdf_insitu.to_crs("EPSG:2154")
gdf_insitu_wgs  = gdf_insitu.to_crs("EPSG:4326")

print("Chargement résidus...")
df = pd.read_csv(CSV_RESIDUALS, parse_dates=['date'])
df['station']    = df['station'].astype(str)
df['is_outlier'] = df['is_outlier'].astype(bool)

print("Chargement métriques NSE/KGE...")
df_metrics = pd.read_csv(METRICS_CSV, header=None, names=["station", "NSE", "KGE"])
df_metrics["NSE"]     = pd.to_numeric(df_metrics["NSE"], errors="coerce")
df_metrics["station"] = df_metrics["station"].astype(str)
df_metrics = df_metrics.set_index("station")


# ═══════════════════════════════════════════════════════════════
# FONCTIONS
# ═══════════════════════════════════════════════════════════════
def get_coords_hydro(station_code):
    conn = sqlite3.connect(HYDRO_DB_PATH)
    for code in [str(station_code), str(station_code).zfill(13)]:
        df_q = pd.read_sql_query(
            "SELECT reference_longitude, reference_latitude "
            "FROM stations WHERE station_code = ?",
            conn, params=(code,)
        )
        if not df_q.empty:
            conn.close()
            return float(df_q.iloc[0]['reference_longitude']), \
                   float(df_q.iloc[0]['reference_latitude'])
    conn.close()
    return None, None


def get_insitu_proche(lon_h, lat_h):
    point     = gpd.GeoSeries([Point(lon_h, lat_h)],
                              crs="EPSG:4326").to_crs("EPSG:2154")[0]
    distances = gdf_insitu_proj.geometry.distance(point)
    idx       = distances.idxmin()
    dist_km   = distances[idx] / 1000
    code_sta  = gdf_insitu_proj.loc[idx, 'code_sta']
    geom_wgs  = gdf_insitu_wgs.loc[idx, 'geometry']
    return code_sta, dist_km, geom_wgs.x, geom_wgs.y


def couleur_nse(nse):
    if np.isnan(nse): return 'gray'
    if nse >= 0.7:    return 'darkgreen'
    if nse >= 0.5:    return 'green'
    if nse >= 0.0:    return 'orange'
    return 'red'


def couleur_lien(dist):
    if dist <= 5:  return '#2E7D32'
    if dist <= 15: return '#F9A825'
    return '#C62828'


# ═══════════════════════════════════════════════════════════════
# GÉNÉRATION DES CARTES
# ═══════════════════════════════════════════════════════════════
stations = sorted(df['station'].unique())
print(f"\n📍 {len(stations)} stations\n")

n_cartes = 0

for sta in stations:
    grp         = df[df['station'] == sta]
    n_out_total = int(grp['is_outlier'].sum())

    lon_h, lat_h = get_coords_hydro(sta)
    if lon_h is None:
        print(f"  {sta} : coords introuvables → skip")
        continue

    nse_val = float(df_metrics.loc[sta, 'NSE']) if sta in df_metrics.index else float('nan')

    # Station insitu la plus proche
    code_ins, dist_km, lon_i, lat_i = get_insitu_proche(lon_h, lat_h)
    has_insitu = dist_km <= DIST_MAX_KM

    # Centre de la carte
    if has_insitu:
        lat_c = (lat_h + lat_i) / 2
        lon_c = (lon_h + lon_i) / 2
    else:
        lat_c, lon_c = lat_h, lon_h

    m = folium.Map(location=[lat_c, lon_c], zoom_start=9, tiles='OpenStreetMap')

    # ── Marqueur station alti ────────────────────────────────────────────
    folium.CircleMarker(
        location=[lat_h, lon_h],
        radius=9,
        color='white', weight=2,
        fill=True, fill_color=couleur_nse(nse_val), fill_opacity=0.9,
        popup=folium.Popup(
            f"<b>Station alti {sta}</b><br>"
            f"NSE = {nse_val:.3f}<br>"
            f"Outliers : {n_out_total}<br>"
            f"lon/lat : {lon_h:.4f}, {lat_h:.4f}",
            max_width=250),
        tooltip=f"ALTI {sta} | NSE={nse_val:.3f}",
    ).add_to(m)

    folium.Marker(
        location=[lat_h, lon_h],
        icon=folium.DivIcon(
            html=f'<div style="font-size:9px;color:#1565C0;font-weight:bold;'
                 f'margin-left:12px;margin-top:-6px;">{sta}</div>'
        )
    ).add_to(m)

    # ── Marqueur + lien station insitu ───────────────────────────────────
    if has_insitu:
        folium.CircleMarker(
            location=[lat_i, lon_i],
            radius=8,
            color='white', weight=2,
            fill=True, fill_color='#E65100', fill_opacity=0.9,
            popup=folium.Popup(
                f"<b>Station insitu {code_ins}</b><br>"
                f"Distance : {dist_km:.1f} km<br>"
                f"lon/lat : {lon_i:.4f}, {lat_i:.4f}",
                max_width=250),
            tooltip=f"INSITU {code_ins} ({dist_km:.1f} km)",
        ).add_to(m)

        folium.Marker(
            location=[lat_i, lon_i],
            icon=folium.DivIcon(
                html=f'<div style="font-size:9px;color:#E65100;font-weight:bold;'
                     f'margin-left:12px;margin-top:-6px;">{code_ins}</div>'
            )
        ).add_to(m)

        folium.PolyLine(
            locations=[[lat_h, lon_h], [lat_i, lon_i]],
            color=couleur_lien(dist_km), weight=2.5, opacity=0.8,
            tooltip=f"{dist_km:.1f} km",
        ).add_to(m)

    # ── Légende ──────────────────────────────────────────────────────────
    insitu_label = f"{code_ins} ({dist_km:.1f} km)" if has_insitu else "aucune (<50 km)"
    legende = f"""
    <div style="position:fixed;bottom:20px;left:20px;z-index:1000;
                background:white;padding:10px 14px;border-radius:6px;
                box-shadow:0 1px 5px rgba(0,0,0,0.4);font-size:12px;">
      <b>{sta}</b><br>
      <span style="color:{couleur_nse(nse_val)};">●</span> Station alti — NSE={nse_val:.3f}<br>
      <span style="color:#E65100;">●</span> Station insitu — {insitu_label}<br>
      Outliers détectés : {n_out_total}<br><br>
      <b>NSE :</b><br>
      <span style="color:darkgreen;">●</span> ≥ 0.7 &nbsp;
      <span style="color:green;">●</span> ≥ 0.5 &nbsp;
      <span style="color:orange;">●</span> ≥ 0.0 &nbsp;
      <span style="color:red;">●</span> &lt; 0
    </div>
    """
    m.get_root().html.add_child(folium.Element(legende))

    # ── Sauvegarde ───────────────────────────────────────────────────────
    sta_dir = OUT_PLOTS / sta
    sta_dir.mkdir(parents=True, exist_ok=True)
    carte_path = sta_dir / f"carte_{sta}.html"
    m.save(str(carte_path))
    n_cartes += 1

    print(f"  {sta:>15s} | NSE={nse_val:.3f} | {n_out_total:2d} outliers | "
          f"insitu={'oui' if has_insitu else 'non'} → carte_{sta}.html")

print(f"\n✅ {n_cartes} cartes générées dans {OUT_PLOTS}")