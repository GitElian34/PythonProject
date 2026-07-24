"""
sword_connectivity.py
════════════════════════════════════════════════════════════════════════
Vérifie si deux points (ex: station alti et station insitu) sont reliés
par le réseau hydrographique SWORD sans confluence majeure entre eux.

Principe :
  1. Charger les tronçons (reaches) SWORD sur l'emprise France (bbox),
     filtrage rapide via l'index spatial du gpkg (pas besoin de charger
     les 241 855 tronçons mondiaux).
  2. Construire un graphe de connectivité à partir de rch_id_up / rch_id_dn.
  3. "Accrocher" chaque point (lon, lat) au tronçon SWORD le plus proche.
  4. Chercher un chemin (BFS) entre les deux tronçons dans le graphe.
  5. Le long du chemin, détecter une confluence si un tronçon intermédiaire
     a n_rch_up > 1 (plusieurs tronçons amont se rejoignent) ou trib_flag=1
     (affluent majeur non modélisé dans SWORD qui entre à cet endroit).

Usage :
  python sword_connectivity.py
  (modifier les coordonnées de test en bas du fichier, ou importer les
   fonctions dans un autre script)
════════════════════════════════════════════════════════════════════════
"""

import geopandas as gpd
import networkx as nx
from shapely.geometry import Point

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
SWORD_GLOBAL = "./data/insitu/sword/global_reaches_SWORD_v16_poly.gpkg"

# Bbox France métropolitaine (lon_min, lat_min, lon_max, lat_max), un peu large
FRANCE_BBOX = (-5.5, 41.0, 9.7, 51.5)

MAX_HOPS = 60          # limite de sécurité pour le parcours BFS
MAX_SNAP_DIST_KM = 5.0 # distance max acceptable pour accrocher un point à un tronçon

# ═══════════════════════════════════════════════════════════════
# CHARGEMENT (filtré par bbox -> rapide même sur le fichier global)
# ═══════════════════════════════════════════════════════════════
def load_sword_reaches(bbox=FRANCE_BBOX, path=SWORD_GLOBAL):
    print(f"Chargement des reaches SWORD sur la bbox {bbox} ...")
    gdf = gpd.read_file(path, bbox=bbox)
    print(f"  {len(gdf)} tronçons chargés (sur 241 855 mondiaux)")

    gdf["reach_id"] = gdf["reach_id"].astype(str)
    gdf_proj = gdf.to_crs("EPSG:2154")  # projection métrique (Lambert-93) pour les distances
    return gdf, gdf_proj


# ═══════════════════════════════════════════════════════════════
# CONSTRUCTION DU GRAPHE DE CONNECTIVITÉ
# ═══════════════════════════════════════════════════════════════
def parse_neighbor_ids(raw):
    """rch_id_up / rch_id_dn peuvent contenir un seul id, plusieurs séparés
    par des espaces, ou '0' (pas de voisin / exutoire/source)."""
    if raw is None:
        return []
    s = str(raw).strip()
    if s in ("", "0", "nan", "NaN", "None"):
        return []
    return [tok for tok in s.split() if tok not in ("0", "")]


def build_graph(gdf):
    G = nx.Graph()
    info = {}  # reach_id -> dict(n_rch_up, trib_flag, river_name, facc, x, y)

    for _, row in gdf.iterrows():
        rid = row["reach_id"]
        G.add_node(rid)
        info[rid] = {
            "n_rch_up": row.get("n_rch_up", 0),
            "n_rch_dn": row.get("n_rch_dn", 0),
            "trib_flag": row.get("trib_flag", 0),
            "river_name": row.get("river_name", "NODATA"),
            "facc": row.get("facc", None),
            "x": row.get("x", None),
            "y": row.get("y", None),
        }

        for up_id in parse_neighbor_ids(row.get("rch_id_up")):
            G.add_edge(rid, up_id)
        for dn_id in parse_neighbor_ids(row.get("rch_id_dn")):
            G.add_edge(rid, dn_id)

    print(f"Graphe construit : {G.number_of_nodes()} noeuds, {G.number_of_edges()} arêtes")
    return G, info


# ═══════════════════════════════════════════════════════════════
# ACCROCHAGE (SNAP) D'UN POINT AU TRONÇON SWORD LE PLUS PROCHE
# ═══════════════════════════════════════════════════════════════
def snap_to_reach(lon, lat, gdf_proj):
    pt = gpd.GeoSeries([Point(lon, lat)], crs="EPSG:4326").to_crs("EPSG:2154")[0]
    dist = gdf_proj.geometry.distance(pt)
    idx = dist.idxmin()
    dist_km = dist[idx] / 1000
    reach_id = gdf_proj.loc[idx, "reach_id"]
    if dist_km > MAX_SNAP_DIST_KM:
        print(f"  ⚠ Point ({lon},{lat}) : tronçon le plus proche à {dist_km:.2f} km "
              f"(> seuil {MAX_SNAP_DIST_KM} km) -> accrochage incertain")
    return reach_id, dist_km


# ═══════════════════════════════════════════════════════════════
# VÉRIFICATION DE CONNECTIVITÉ ENTRE 2 POINTS
# ═══════════════════════════════════════════════════════════════
def check_connectivity(lon_a, lat_a, lon_b, lat_b, G, info, gdf_proj, label_a="A", label_b="B",
                        facc_max_ratio=2.0):
    """
    facc_max_ratio : seuil de rejet sur le ratio de surface drainée (facc) entre les
    2 tronçons accrochés. None = ne pas calculer ce critère. Détecte une accumulation
    progressive de bassin versant (petits affluents diffus non modélisés individuellement
    dans SWORD), complémentaire de la détection de confluence discrète (n_rch_up/trib_flag).
    """
    reach_a, dist_a = snap_to_reach(lon_a, lat_a, gdf_proj)
    reach_b, dist_b = snap_to_reach(lon_b, lat_b, gdf_proj)

    result = {
        "label_a": label_a, "label_b": label_b,
        "reach_a": reach_a, "reach_b": reach_b,
        "snap_dist_a_km": round(dist_a, 2), "snap_dist_b_km": round(dist_b, 2),
        "connected": False, "n_hops": None, "has_confluence": False,
        "confluence_reaches": [], "path": [],
        "facc_a": None, "facc_b": None, "facc_ratio": None, "facc_ok": None,
    }

    # ── Ratio facc — indépendant du chemin, juste les 2 tronçons accrochés ──
    facc_a = info.get(reach_a, {}).get("facc")
    facc_b = info.get(reach_b, {}).get("facc")
    result["facc_a"], result["facc_b"] = facc_a, facc_b

    if facc_a is not None and facc_b is not None and facc_a > 0 and facc_b > 0:
        ratio = max(facc_a, facc_b) / min(facc_a, facc_b)
        result["facc_ratio"] = round(ratio, 2)
        if facc_max_ratio is not None:
            result["facc_ok"] = ratio <= facc_max_ratio
        # si facc_max_ratio est None, facc_ok reste None (= "non vérifié", pas "rejeté")
    # si facc manquant (NaN/0) -> facc_ratio et facc_ok restent None ("inconnu", pas un rejet)

    if reach_a == reach_b:
        result["connected"] = True
        result["n_hops"] = 0
        return result

    if not nx.has_path(G, reach_a, reach_b):
        return result   # not connected dans le réseau (rivières différentes / hors bbox)

    path = nx.shortest_path(G, reach_a, reach_b)
    if len(path) - 1 > MAX_HOPS:
        result["connected"] = False  # chemin trop long -> jugé non pertinent
        result["n_hops"] = len(path) - 1
        return result

    result["connected"] = True
    result["n_hops"] = len(path) - 1
    result["path"] = path

    # Détection de confluence le long du chemin (hors les 2 extrémités)
    confluences = []
    for rid in path[1:-1]:
        meta = info.get(rid, {})
        n_up = meta.get("n_rch_up", 0) or 0
        trib = meta.get("trib_flag", 0) or 0
        if n_up > 1 or trib == 1:
            confluences.append(rid)

    result["has_confluence"] = len(confluences) > 0
    result["confluence_reaches"] = confluences
    return result


# ═══════════════════════════════════════════════════════════════
# DÉMO / TEST
# ═══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    gdf, gdf_proj = load_sword_reaches()
    G, info = build_graph(gdf)

    # ── Exemple : remplacer par tes vraies coordonnées station alti / insitu ──
    LON_ALTI, LAT_ALTI = 0.3695, 47.3029       # ex: station hwnext 6325
    LON_INSITU, LAT_INSITU = 0.40, 47.33       # ex: insitu K683002001 (approx, à ajuster)

    res = check_connectivity(LON_ALTI, LAT_ALTI, LON_INSITU, LAT_INSITU,
                              G, info, gdf_proj, label_a="Alti", label_b="Insitu")

    print(f"\n{'='*70}")
    print(f"  Alti  -> tronçon {res['reach_a']}  (snap {res['snap_dist_a_km']} km)")
    print(f"  Insitu-> tronçon {res['reach_b']}  (snap {res['snap_dist_b_km']} km)")
    print(f"  Connectés dans le réseau : {res['connected']}")
    if res["connected"]:
        print(f"  Nombre de tronçons entre les deux : {res['n_hops']}")
        print(f"  Confluence détectée sur le chemin : {res['has_confluence']}")
        if res["has_confluence"]:
            print(f"  Tronçon(s) de confluence : {res['confluence_reaches']}")
    print(f"  facc alti={res['facc_a']}  facc insitu={res['facc_b']}  "
          f"ratio={res['facc_ratio']}  facc_ok={res['facc_ok']}")
    print(f"{'='*70}")