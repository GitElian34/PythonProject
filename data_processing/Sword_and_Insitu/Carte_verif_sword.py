"""
carte_verification_sword.py
════════════════════════════════════════════════════════════════════════
À coller dans une cellule de notebook.

Pour UNE station alti donnée, affiche une carte Folium avec :
  - la station alti (bleu)
  - l'insitu le plus proche (vert)
  - l'insitu sélectionné par la méthode SWORD, si différent (orange)
  - les polygones SWORD locaux (gris = neutre, rouge = confluence
    n_rch_up>1 ou trib_flag=1, bleu = sur le chemin vers l'insitu le
    plus proche, orange = sur le chemin vers l'insitu SWORD)
  - popups cliquables sur chaque polygone SWORD (reach_id, river_name,
    n_rch_up, n_rch_dn, trib_flag) pour vérifier les attributs à la main

Charge SWORD uniquement sur une bbox LOCALE autour de la station
(rapide, pas besoin de charger toute la France pour une seule station).
════════════════════════════════════════════════════════════════════════
"""

import sqlite3
import sys
import numpy as np
import pandas as pd
import geopandas as gpd
import networkx as nx
import folium
from pathlib import Path
from shapely.geometry import Point

sys.path.insert(0, ".")
from Sword_connectivity import load_sword_reaches, build_graph, check_connectivity, parse_neighbor_ids

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES — à modifier ici à chaque utilisation
# ═══════════════════════════════════════════════════════════════
STATION_CODE = "6325"     # code de la station alti à inspecter
SOURCE       = "hwnext"   # "hwnext" ou "dahiti"

BUFFER_DEG   = 0.35       # marge autour de la station pour charger SWORD localement (~35 km)
DIST_MAX_KM  = 50.0       # rayon de recherche des candidats insitu

HWNEXT_DB  = "./data/hydroweb_next.db"
DAHITI_DB  = "./data/dahiti.db"
INSITU_SHP = "./data/insitu/shp/station_schapi_alti_ref_2025_river.gpkg"

SAT_DB = HWNEXT_DB if SOURCE == "hwnext" else DAHITI_DB

# ═══════════════════════════════════════════════════════════════
# 1. COORDONNÉES DE LA STATION
# ═══════════════════════════════════════════════════════════════
def get_coords_station(source, code):
    conn = sqlite3.connect(SAT_DB)
    for c in [str(code), str(code).zfill(13)]:
        df = pd.read_sql(
            "SELECT station_code, reference_longitude AS lon, reference_latitude AS lat "
            "FROM stations WHERE station_code = ?",
            conn, params=(c,)
        )
        if not df.empty:
            conn.close()
            return float(df.iloc[0]["lon"]), float(df.iloc[0]["lat"]), df.iloc[0]["station_code"]
    conn.close()
    return None, None, None

lon_a, lat_a, code_norm = get_coords_station(SOURCE, STATION_CODE)
if lon_a is None:
    raise SystemExit(f"⚠ Station {STATION_CODE} introuvable dans {SOURCE}")

print(f"Station {SOURCE.upper()} {code_norm} : lon={lon_a:.4f}, lat={lat_a:.4f}")

# ═══════════════════════════════════════════════════════════════
# 2. CHARGEMENT SWORD LOCAL (bbox autour de la station)
# ═══════════════════════════════════════════════════════════════
local_bbox = (lon_a - BUFFER_DEG, lat_a - BUFFER_DEG, lon_a + BUFFER_DEG, lat_a + BUFFER_DEG)
gdf_sword, gdf_sword_proj = load_sword_reaches(bbox=local_bbox)
G, info = build_graph(gdf_sword)
gdf_sword_wgs = gdf_sword.to_crs("EPSG:4326")

# ═══════════════════════════════════════════════════════════════
# 3. INSITU : LE PLUS PROCHE + CANDIDAT SWORD
# ═══════════════════════════════════════════════════════════════
gdf_insitu = gpd.read_file(INSITU_SHP).to_crs("EPSG:4326")
gdf_insitu_proj = gdf_insitu.to_crs("EPSG:2154")

def get_insitu_candidats(lon, lat, dist_max_km):
    pt = gpd.GeoSeries([Point(lon, lat)], crs="EPSG:4326").to_crs("EPSG:2154")[0]
    dist = gdf_insitu_proj.geometry.distance(pt) / 1000
    candidats = dist[dist <= dist_max_km].sort_values()
    return [(gdf_insitu_proj.loc[idx, "code_sta"], d,
              gdf_insitu.loc[idx, "geometry"].x, gdf_insitu.loc[idx, "geometry"].y)
            for idx, d in candidats.items()]

candidats = get_insitu_candidats(lon_a, lat_a, DIST_MAX_KM)
if not candidats:
    raise SystemExit("⚠ Aucun insitu dans le rayon DIST_MAX_KM")

code_nearest, dist_nearest, lon_n, lat_n = candidats[0]

code_sword, dist_sword, lon_s, lat_s = None, None, None, None
res_sword = None
for code_ins, dist_km, lon_b, lat_b in candidats:
    res = check_connectivity(lon_a, lat_a, lon_b, lat_b, G, info, gdf_sword_proj)
    if res["connected"] and not res["has_confluence"]:
        code_sword, dist_sword, lon_s, lat_s = code_ins, dist_km, lon_b, lat_b
        res_sword = res
        break

# Chemin vers le plus proche aussi (pour affichage, même s'il a une confluence)
res_nearest = check_connectivity(lon_a, lat_a, lon_n, lat_n, G, info, gdf_sword_proj)

print(f"\nInsitu le plus proche : {code_nearest}  ({dist_nearest:.1f} km)  "
      f"connecté={res_nearest['connected']}  confluence={res_nearest['has_confluence']}")
if code_sword:
    same = (code_sword == code_nearest)
    print(f"Insitu SWORD (1er valide) : {code_sword}  ({dist_sword:.1f} km)  "
          f"{'(= le plus proche)' if same else '(DIFFERENT du plus proche)'}")
else:
    print("Insitu SWORD : aucun candidat connecté sans confluence trouvé dans le rayon")

# ═══════════════════════════════════════════════════════════════
# 4. CARTE
# ═══════════════════════════════════════════════════════════════
COLOR_SAT      = "#1565C0"
COLOR_NEAREST  = "#2E7D32"
COLOR_SWORD    = "#FB8C00"
COLOR_CONFLU   = "#C0392B"
COLOR_PATH_N   = "#1E88E5"
COLOR_PATH_S   = "#FB8C00"
COLOR_NEUTRAL  = "#BDBDBD"

m = folium.Map(location=[lat_a, lon_a], zoom_start=10, tiles="OpenStreetMap")

path_nearest_ids = set(res_nearest["path"]) if res_nearest["path"] else set()
path_sword_ids   = set(res_sword["path"]) if (res_sword and res_sword["path"]) else set()

# Polygones SWORD
for _, row in gdf_sword_wgs.iterrows():
    rid = row["reach_id"]
    n_up = row.get("n_rch_up", 0) or 0
    trib = row.get("trib_flag", 0) or 0
    is_confluence = (n_up > 1) or (trib == 1)

    if rid in path_sword_ids and rid in path_nearest_ids:
        color, weight = "#8E24AA", 3   # sur les 2 chemins -> violet
    elif rid in path_sword_ids:
        color, weight = COLOR_PATH_S, 3
    elif rid in path_nearest_ids:
        color, weight = COLOR_PATH_N, 3
    elif is_confluence:
        color, weight = COLOR_CONFLU, 1.5
    else:
        color, weight = COLOR_NEUTRAL, 0.8

    folium.GeoJson(
        row["geometry"],
        style_function=lambda feat, color=color, weight=weight: {
            "fillColor": color, "color": color, "weight": weight, "fillOpacity": 0.35,
        },
        tooltip=folium.GeoJsonTooltip(fields=[], aliases=[]),
        popup=folium.Popup(
            f"<b>reach_id</b>: {rid}<br>"
            f"<b>river_name</b>: {row.get('river_name','?')}<br>"
            f"<b>n_rch_up</b>: {n_up}  |  <b>n_rch_dn</b>: {row.get('n_rch_dn','?')}<br>"
            f"<b>trib_flag</b>: {trib}<br>"
            f"<b>facc</b>: {row.get('facc','?')}",
            max_width=260
        ),
    ).add_to(m)

# Station alti
folium.CircleMarker(
    location=[lat_a, lon_a], radius=10, color="white", weight=2,
    fill=True, fill_color=COLOR_SAT, fill_opacity=0.95,
    popup=f"<b>{SOURCE.upper()} {code_norm}</b>",
    tooltip=f"{SOURCE.upper()} {code_norm}",
).add_to(m)

# Insitu le plus proche
folium.CircleMarker(
    location=[lat_n, lon_n], radius=9, color="white", weight=2,
    fill=True, fill_color=COLOR_NEAREST, fill_opacity=0.95,
    popup=f"<b>Insitu plus proche</b><br>{code_nearest}<br>{dist_nearest:.1f} km<br>"
          f"connecté={res_nearest['connected']} | confluence={res_nearest['has_confluence']}",
    tooltip=f"Plus proche: {code_nearest}",
).add_to(m)

# Insitu SWORD (si différent)
if code_sword and code_sword != code_nearest:
    folium.CircleMarker(
        location=[lat_s, lon_s], radius=9, color="white", weight=2,
        fill=True, fill_color=COLOR_SWORD, fill_opacity=0.95,
        popup=f"<b>Insitu SWORD</b><br>{code_sword}<br>{dist_sword:.1f} km",
        tooltip=f"SWORD: {code_sword}",
    ).add_to(m)

# Légende
legende = f"""
<div style="position:fixed;bottom:20px;left:20px;z-index:1000;
            background:white;padding:10px 14px;border-radius:6px;
            box-shadow:0 1px 5px rgba(0,0,0,0.4);font-size:11px;">
  <b>{SOURCE.upper()} {code_norm}</b><br>
  <span style="color:{COLOR_SAT};">●</span> Station alti<br>
  <span style="color:{COLOR_NEAREST};">●</span> Insitu le plus proche<br>
  <span style="color:{COLOR_SWORD};">●</span> Insitu SWORD (si différent)<br>
  <span style="color:{COLOR_CONFLU};">▬</span> Tronçon confluence (n_rch_up&gt;1 ou trib_flag)<br>
  <span style="color:{COLOR_PATH_N};">▬</span> Chemin vers le plus proche<br>
  <span style="color:{COLOR_PATH_S};">▬</span> Chemin vers le candidat SWORD<br>
  <span style="color:{COLOR_NEUTRAL};">▬</span> Autres tronçons (zone chargée)
</div>
"""
m.get_root().html.add_child(folium.Element(legende))

display(m)