import re
import os
import glob
import sqlite3
import numpy as np
import pandas as pd
import geopandas as gpd
import folium
from collections import defaultdict

GPKG_PATH = "./data/insitu/shp/station_schapi_alti_ref_2025_river.gpkg"
CSV_DIR   = "./data/insitu/data"
DB_PATH   = "./data/insitu_data.db"

gdf_insitu = gpd.read_file(GPKG_PATH)

# ── Seuils ────────────────────────────────────────────
SEUIL_SAUT_CM        = 30    # cm
SEUIL_DELAI_MIN      = 10    # minutes
SEUIL_STD_GLOBALE_CM = 10.0   # cm
SEUIL_STD_FENETRE_CM = 3.0   # cm
FENETRE_JOURS        = 90


# ─────────────────────────────────────────
# Utilitaires
# ─────────────────────────────────────────
def extract_station_code(fichier):
    match = re.match(r'WSH_([A-Z0-9]\d{9})\.csv', os.path.basename(fichier))
    return match.group(1) if match else None


def get_coords(station_code):
    row = gdf_insitu[gdf_insitu['code_sta'] == station_code]
    if row.empty:
        return None, None
    geom = row.iloc[0].geometry
    return geom.y, geom.x  # lat, lon


def get_stations_rivieres(db_path):
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query("""
        SELECT code_sta FROM stations_insitu
        WHERE dans_lac IS NULL OR dans_lac NOT IN ('dans_lac', 'proche_lac')
    """, conn)
    conn.close()
    return set(df['code_sta'].tolist())


# ─────────────────────────────────────────
# Détection sauts brutaux (données brutes CSV)
# ─────────────────────────────────────────
def detecter_sauts(fichier, seuil_cm=SEUIL_SAUT_CM, delai_min=SEUIL_DELAI_MIN):
    df = pd.read_csv(fichier)
    df['Date'] = pd.to_datetime(df['Date'], utc=True)
    df = df.dropna(subset=['WSH']).sort_values('Date').reset_index(drop=True)

    if len(df) < 2:
        return 0, []

    delta_h   = (df['WSH'].diff().abs() * 100)           # en cm
    delta_t   = df['Date'].diff().dt.total_seconds() / 60 # en minutes

    masque = (delta_h > seuil_cm) & (delta_t <= delai_min) & (delta_t > 0)
    n_sauts = masque.sum()

    exemples = []
    for idx in df[masque].index[:3]:  # garder 3 exemples max
        exemples.append({
            'date':     df.loc[idx, 'Date'].isoformat(),
            'delta_cm': round(delta_h[idx], 2),
            'delta_min': round(delta_t[idx], 1)
        })

    return int(n_sauts), exemples


# ─────────────────────────────────────────
# Détection signal plat (médiane journalière BDD)
# ─────────────────────────────────────────
def detecter_signal_plat(station_code, conn,
                          seuil_cm=SEUIL_STD_GLOBALE_CM,
                          fenetre=FENETRE_JOURS,
                          seuil_fen_cm=SEUIL_STD_FENETRE_CM):
    df = pd.read_sql_query("""
        SELECT date, h_med_wsh FROM mesures_insitu
        WHERE code_sta = ? AND h_med_wsh IS NOT NULL
        ORDER BY date
    """, conn, params=(station_code,))

    if len(df) < 30:
        return False, False, {}

    h = df['h_med_wsh'] * 100  # en cm

    # 1. Std globale
    plat_global = h.std() < seuil_cm

    # 2. Fenêtre glissante : proportion de fenêtres plates
    rolling_std  = h.rolling(window=fenetre).std().dropna()
    n_fenetres   = len(rolling_std)
    n_plates     = (rolling_std < seuil_fen_cm).sum()
    ratio_plat   = n_plates / n_fenetres if n_fenetres > 0 else 0
    plat_fenetre = ratio_plat > 0.3  # >30% des fenêtres sont plates

    details = {
        'std_globale_cm': round(h.std(), 3),
        'ratio_fenetres_plates': round(ratio_plat, 3),
        'n_obs': len(df)
    }

    return plat_global, plat_fenetre, details


# ─────────────────────────────────────────
# Analyse complète
# ─────────────────────────────────────────
def analyser_stations(fichiers, stations_rivieres, db_path):
    conn      = sqlite3.connect(db_path)
    resultats = []

    fichiers_rivieres = [
        f for f in fichiers
        if extract_station_code(f) in stations_rivieres
    ]
    print(f"🔍 {len(fichiers_rivieres)} stations rivières à analyser")

    for i, fichier in enumerate(fichiers_rivieres):
        station_code = extract_station_code(fichier)

        n_sauts, exemples_sauts          = detecter_sauts(fichier)
        plat_global, plat_fen, details   = detecter_signal_plat(station_code, conn)

        suspect = n_sauts > 0 or plat_global or plat_fen

        resultats.append({
            'code_sta':            station_code,
            'suspect':             suspect,
            'n_sauts':             n_sauts,
            'exemples_sauts':      exemples_sauts,
            'plat_global':         plat_global,
            'plat_fenetre':        plat_fen,
            **details
        })

        if (i + 1) % 100 == 0:
            print(f"  [{i+1}/{len(fichiers_rivieres)}] en cours...")

    conn.close()
    return pd.DataFrame(resultats)


# ─────────────────────────────────────────
# Rapport texte
# ─────────────────────────────────────────
def generer_rapport(df_res, output_path="./data/insitu/visualisation/rapport_outliers.txt"):
    suspects = df_res[df_res['suspect']]

    lignes = [
        "═" * 60,
        "  RAPPORT STATIONS ABERRANTES — RIVIÈRES",
        "═" * 60,
        f"  Stations analysées  : {len(df_res)}",
        f"  Stations suspectes  : {len(suspects)} ({len(suspects)/len(df_res)*100:.1f}%)",
        f"  — Sauts brutaux     : {(df_res['n_sauts'] > 0).sum()}",
        f"  — Signal plat global: {df_res['plat_global'].sum()}",
        f"  — Signal plat fenêt.: {df_res['plat_fenetre'].sum()}",
        "═" * 60, ""
    ]

    for _, row in suspects.sort_values('n_sauts', ascending=False).iterrows():
        raisons = []
        if row['n_sauts'] > 0:
            raisons.append(f"sauts brutaux (n={row['n_sauts']})")
        if row['plat_global']:
            raisons.append(f"plat global (std={row.get('std_globale_cm','?')} cm)")
        if row['plat_fenetre']:
            raisons.append(f"plat fenêtre (ratio={row.get('ratio_fenetres_plates','?')})")

        lignes.append(f"▶ {row['code_sta']}  —  {' | '.join(raisons)}")
        for ex in row.get('exemples_sauts', []):
            lignes.append(f"    saut {ex['delta_cm']} cm en {ex['delta_min']} min  ({ex['date']})")
        lignes.append("")

    rapport = "\n".join(lignes)
    print(rapport)

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(rapport)
    print(f"📄 Rapport sauvegardé : {output_path}")


# ─────────────────────────────────────────
# Carte folium
# ─────────────────────────────────────────
def generer_carte(df_res, output_path="./data/insitu/visualisation/carte_outliers.html"):
    suspects = df_res[df_res['suspect']]

    m = folium.Map(location=[46.5, 2.5], zoom_start=6, tiles="CartoDB positron")

    # Stations normales — points discrets
    for _, row in df_res[~df_res['suspect']].iterrows():
        lat, lon = get_coords(row['code_sta'])
        if lat is None:
            continue
        folium.CircleMarker(
            location=[lat, lon], radius=3,
            color='#2196F3', fill=True, fill_opacity=0.4,
            tooltip=row['code_sta']
        ).add_to(m)

    # Stations suspectes — colorées par type de problème
    for _, row in suspects.iterrows():
        lat, lon = get_coords(row['code_sta'])
        if lat is None:
            continue

        if row['n_sauts'] > 0 and (row['plat_global'] or row['plat_fenetre']):
            color = '#9C27B0'  # violet = les deux
        elif row['n_sauts'] > 0:
            color = '#FF5722'  # orange = sauts
        else:
            color = '#FFC107'  # jaune = plat

        raisons = []
        if row['n_sauts'] > 0:
            raisons.append(f"⚡ {row['n_sauts']} saut(s) brutal(aux)")
        if row['plat_global']:
            raisons.append(f"📉 Plat global (std={row.get('std_globale_cm','?')} cm)")
        if row['plat_fenetre']:
            raisons.append(f"📉 Plat fenêtre (ratio={row.get('ratio_fenetres_plates','?')})")

        folium.CircleMarker(
            location=[lat, lon], radius=8,
            color=color, fill=True, fill_color=color, fill_opacity=0.85,
            popup=folium.Popup(
                f"<b>{row['code_sta']}</b><br>" + "<br>".join(raisons),
                max_width=250
            ),
            tooltip=row['code_sta']
        ).add_to(m)

    # Légende
    legend = """
    <div style="position:fixed;bottom:30px;left:30px;z-index:1000;
         background:white;padding:12px;border-radius:6px;
         border:1px solid #ccc;font-size:12px;line-height:1.8">
      <b>Stations rivières</b><br>
      <span style="color:#2196F3">●</span> Normale<br>
      <span style="color:#FF5722">●</span> Sauts brutaux<br>
      <span style="color:#FFC107">●</span> Signal plat<br>
      <span style="color:#9C27B0">●</span> Les deux
    </div>"""
    m.get_root().html.add_child(folium.Element(legend))

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    m.save(output_path)
    print(f"🗺️  Carte sauvegardée : {output_path}")


# ─────────────────────────────────────────
# Main
# ─────────────────────────────────────────
if __name__ == "__main__":
    fichiers          = glob.glob(os.path.join(CSV_DIR, "*.csv"))
    stations_rivieres = get_stations_rivieres(DB_PATH)

    df_resultats = analyser_stations(fichiers, stations_rivieres, DB_PATH)

    generer_rapport(df_resultats)
    generer_carte(df_resultats)