"""
test_all_stations_sword.py
════════════════════════════════════════════════════════════════════════
Teste la connectivité réseau SWORD entre TOUTES les stations HW Next et
leur insitu le plus proche — avec le critère facc en plus de la
détection de confluence.

SWORD est chargé et le graphe construit UNE SEULE FOIS au début, puis
réutilisés pour toutes les stations.

Étapes :
  1. Charger SWORD (bbox France) + construire le graphe -> 1 seule fois
  2. Charger TOUTES les stations HW Next (hydroweb_next.db)
  3. Charger les stations insitu (shapefile, cohérent avec le reste du pipeline)
  4. Pour chaque station : trouver l'insitu le plus proche, vérifier la
     connectivité SWORD (accrochage + parcours de graphe + confluence + facc)
  5. Résumé + CSV

Usage :
  python test_all_stations_sword.py
════════════════════════════════════════════════════════════════════════
"""

import sqlite3
import sys
import pandas as pd
import numpy as np
import geopandas as gpd
from pathlib import Path
from shapely.geometry import Point

sys.path.insert(0, str(Path(__file__).parent))
from Sword_connectivity import load_sword_reaches, build_graph, check_connectivity

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
HWNEXT_DB  = "./data/hydroweb_next.db"
INSITU_SHP = "./data/insitu/shp/station_schapi_alti_ref_2025_river.gpkg"

N_STATIONS = None     # None = toutes les stations
FACC_MAX_RATIO = 2.0  # seuil de rejet sur le ratio de surface drainée

OUTPUT_CSV = Path("./data_processing/Sword_and_Insitu/connectivite_all_stations.csv")
OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)

# ═══════════════════════════════════════════════════════════════
# 1. CHARGEMENT SWORD — UNE SEULE FOIS
# ═══════════════════════════════════════════════════════════════
print("### Chargement SWORD (une seule fois) ###")
gdf_sword, gdf_sword_proj = load_sword_reaches()
G, info = build_graph(gdf_sword)

# ═══════════════════════════════════════════════════════════════
# 2. CHARGEMENT DES STATIONS HW NEXT
# ═══════════════════════════════════════════════════════════════
print(f"\n### Chargement des stations HW Next ###")
conn = sqlite3.connect(HWNEXT_DB)
limit_clause = f"LIMIT {N_STATIONS}" if N_STATIONS else ""
df_stations = pd.read_sql(f"""
    SELECT station_code, reference_longitude AS lon, reference_latitude AS lat
    FROM stations
    WHERE reference_longitude IS NOT NULL AND reference_latitude IS NOT NULL
    {limit_clause}
""", conn)
conn.close()
print(f"  {len(df_stations)} stations chargées")

# ═══════════════════════════════════════════════════════════════
# 3. CHARGEMENT DES STATIONS INSITU (shapefile)
# ═══════════════════════════════════════════════════════════════
print(f"\n### Chargement des stations insitu (shapefile) ###")
gdf_insitu_pts = gpd.read_file(INSITU_SHP)[["code_sta", "geometry"]].to_crs("EPSG:4326")
gdf_insitu_proj = gdf_insitu_pts.to_crs("EPSG:2154")
print(f"  {len(gdf_insitu_pts)} stations insitu chargées")

def get_insitu_proche(lon, lat):
    pt = gpd.GeoSeries([Point(lon, lat)], crs="EPSG:4326").to_crs("EPSG:2154")[0]
    dist = gdf_insitu_proj.geometry.distance(pt)
    idx = dist.idxmin()
    return gdf_insitu_proj.loc[idx, "code_sta"], dist[idx] / 1000, \
           gdf_insitu_pts.loc[idx, "geometry"].x, gdf_insitu_pts.loc[idx, "geometry"].y

# ═══════════════════════════════════════════════════════════════
# 4. TEST DE CONNECTIVITÉ SUR TOUTES LES STATIONS
# ═══════════════════════════════════════════════════════════════
print(f"\n### Test de connectivité sur {len(df_stations)} stations ###")
rows = []

for i, sta in df_stations.iterrows():
    code = sta["station_code"]
    lon_a, lat_a = sta["lon"], sta["lat"]

    code_ins, dist_ins_km, lon_b, lat_b = get_insitu_proche(lon_a, lat_a)

    res = check_connectivity(lon_a, lat_a, lon_b, lat_b, G, info, gdf_sword_proj,
                              label_a=str(code), label_b=str(code_ins),
                              facc_max_ratio=FACC_MAX_RATIO)

    rows.append({
        "station": code,
        "insitu": code_ins,
        "dist_insitu_km": round(dist_ins_km, 2),
        "reach_station": res["reach_a"],
        "reach_insitu": res["reach_b"],
        "snap_dist_station_km": res["snap_dist_a_km"],
        "snap_dist_insitu_km": res["snap_dist_b_km"],
        "connected": res["connected"],
        "n_hops": res["n_hops"],
        "has_confluence": res["has_confluence"],
        "n_confluences": len(res["confluence_reaches"]),
        "facc_station": res["facc_a"],
        "facc_insitu": res["facc_b"],
        "facc_ratio": res["facc_ratio"],
        "facc_ok": res["facc_ok"],
    })

    if (i + 1) % 50 == 0:
        print(f"  ... {i+1}/{len(df_stations)} stations traitées")

df_res = pd.DataFrame(rows)
df_res.to_csv(OUTPUT_CSV, index=False)

# ═══════════════════════════════════════════════════════════════
# 5. RÉSUMÉ
# ═══════════════════════════════════════════════════════════════
n_total = len(df_res)
n_connected = int(df_res["connected"].sum())
n_confluence = int(df_res["has_confluence"].sum())

# Validité globale = connecté + pas de confluence + facc_ok pas explicitement False
df_res["valide"] = (
    df_res["connected"]
    & ~df_res["has_confluence"]
    & (df_res["facc_ok"] != False)   # True ou None (donnée manquante) acceptés
)
n_valide = int(df_res["valide"].sum())

n_facc_reject = int((df_res["facc_ok"] == False).sum())
n_facc_unknown = int(df_res["facc_ok"].isna().sum())

print(f"\n{'='*70}")
print(f"  RÉSUMÉ — {n_total} stations testées (insitu le plus proche)")
print(f"{'='*70}")
print(f"  Connectées dans SWORD                    : {n_connected} / {n_total} ({n_connected/n_total*100:.1f}%)")
print(f"  Avec confluence sur le trajet             : {n_confluence} / {n_total} ({n_confluence/n_total*100:.1f}%)")
print(f"  Rejetées sur le critère facc (ratio>{FACC_MAX_RATIO}) : {n_facc_reject} / {n_total} ({n_facc_reject/n_total*100:.1f}%)")
print(f"  Donnée facc manquante (non vérifiable)    : {n_facc_unknown} / {n_total} ({n_facc_unknown/n_total*100:.1f}%)")
print(f"  --------------------------------------------------------------")
print(f"  VALIDES (connecté + sans confluence + facc ok) : {n_valide} / {n_total} ({n_valide/n_total*100:.1f}%)")
print(f"  Non connectées (réseaux différents)       : {n_total - n_connected} / {n_total}")
print(f"\n  CSV détaillé -> {OUTPUT_CSV}")