import sqlite3
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import geopandas as gpd
import glob
import os
from shapely.geometry import Point
from sklearn.preprocessing import MinMaxScaler

from Geopackage.comparaison_hydro_insitu import station_la_plus_proche
from Geopackage.visualisation import sword_nodes_proj
from data_processing.db_manager import get_station_coordinates

HYDRO_DB_PATH  = "./data/hydro_data.db"
INSITU_DB_PATH = "./data/insitu_data.db"
CSV_DIR        = "./data/insitu/data"
OUTPUT_DIR     = "./data/comparaison_hydro_insitu"
N_STATIONS     = 230
TOLERANCE_H    = 5
SEUIL_DIST_KM  = 10
os.makedirs(OUTPUT_DIR, exist_ok=True)

INSITU_COLS      = ['h_01h_wsh', 'h_09h_wsh', 'h_17h_wsh', 'h_med_wsh']
CATEGORIES_SAUTS = ['aucun', '< 10', '10-100', '100-500', '> 500']
COLORS_SAUTS     = ['#4CAF50', '#2196F3', '#FF9800', '#FF5722', '#9C27B0']


# ─────────────────────────────────────────────
# Chargement stations hydro — ordre fixe
# ─────────────────────────────────────────────
def get_stations_hydro(n):
    conn     = sqlite3.connect(HYDRO_DB_PATH)
    stations = pd.read_sql_query(
        f"SELECT station_code FROM stations ORDER BY station_code LIMIT {n}", conn
    )['station_code'].tolist()
    conn.close()
    return stations


# ─────────────────────────────────────────────
# Qualité sauts station insitu
# ─────────────────────────────────────────────
def get_qualite_sauts(station_code):
    conn = sqlite3.connect(INSITU_DB_PATH)
    df   = pd.read_sql_query("""
        SELECT qualite_sauts FROM stations_insitu WHERE code_sta = ?
    """, conn, params=(station_code,))
    conn.close()
    if df.empty or df.iloc[0]['qualite_sauts'] is None:
        return None
    return df.iloc[0]['qualite_sauts']


# ─────────────────────────────────────────────
# Flags station insitu
# ─────────────────────────────────────────────
def get_flags_insitu(station_code):
    conn = sqlite3.connect(INSITU_DB_PATH)
    df   = pd.read_sql_query("""
        SELECT dans_lac, qualite_sauts, signal_plat, gap_max_jours
        FROM stations_insitu WHERE code_sta = ?
    """, conn, params=(station_code,))
    conn.close()
    if df.empty:
        return None
    row = df.iloc[0]
    return {
        'dans_lac':    row['dans_lac'] in ['dans_lac', 'proche_lac'],
        'signal_plat': bool(row['signal_plat']),
        'sauts_ok':    row['qualite_sauts'] in ['aucun', '< 10'],
        'gap':         row['gap_max_jours'] is not None and not pd.isna(row['gap_max_jours']),
    }


# ─────────────────────────────────────────────
# Vérification même rivière
# ─────────────────────────────────────────────
def verifier_meme_riviere(lon_h, lat_h, lon_i, lat_i, seuil_dist_km=SEUIL_DIST_KM):
    point_h = gpd.GeoSeries([Point(lon_h, lat_h)], crs='EPSG:4326').to_crs('EPSG:2154')[0]
    point_i = gpd.GeoSeries([Point(lon_i, lat_i)], crs='EPSG:4326').to_crs('EPSG:2154')[0]
    idx_h   = sword_nodes_proj.geometry.distance(point_h).idxmin()
    idx_i   = sword_nodes_proj.geometry.distance(point_i).idxmin()
    reach_h = sword_nodes_proj.loc[idx_h, 'reach_id']
    reach_i = sword_nodes_proj.loc[idx_i, 'reach_id']
    dist_km = abs(sword_nodes_proj.loc[idx_h, 'dist_out'] -
                  sword_nodes_proj.loc[idx_i, 'dist_out']) / 1000
    meme_riviere = (reach_h == reach_i or dist_km <= seuil_dist_km) and dist_km <= seuil_dist_km
    return meme_riviere, round(dist_km, 2)


# ─────────────────────────────────────────────
# Chargement données hydro
# ─────────────────────────────────────────────
def get_donnees_hydro(station_code):
    conn = sqlite3.connect(HYDRO_DB_PATH)
    df   = pd.read_sql_query("""
        SELECT measure_date, measure_time, orthometric_height
        FROM measurements
        WHERE station_code = ?
          AND orthometric_height IS NOT NULL
        ORDER BY measure_date, measure_time
    """, conn, params=(station_code,))
    conn.close()
    df['datetime'] = pd.to_datetime(
        df['measure_date'] + ' ' + df['measure_time'].fillna('00:00:00'), utc=True
    )
    df['date'] = pd.to_datetime(df['measure_date'])
    return df


# ─────────────────────────────────────────────
# Chargement données insitu BDD
# ─────────────────────────────────────────────
def get_donnees_insitu_db(station_code):
    conn = sqlite3.connect(INSITU_DB_PATH)
    df   = pd.read_sql_query("""
        SELECT date, h_01h_wsh, h_09h_wsh, h_17h_wsh, h_med_wsh
        FROM mesures_insitu WHERE code_sta = ? ORDER BY date
    """, conn, params=(station_code,))
    conn.close()
    df['date'] = pd.to_datetime(df['date'])
    return df


# ─────────────────────────────────────────────
# Chargement CSV brut
# ─────────────────────────────────────────────
def charger_csv_insitu(station_code):
    fichiers = glob.glob(os.path.join(CSV_DIR, f"WSH_{station_code}.csv"))
    if not fichiers:
        return None
    df         = pd.read_csv(fichiers[0])
    df['Date'] = pd.to_datetime(df['Date'], utc=True)
    df         = df.dropna(subset=['WSH']).sort_values('Date').reset_index(drop=True)
    return df if not df.empty else None


def get_wsh_proche(df_csv, datetime_cible):
    if df_csv is None or df_csv.empty:
        return None
    diff = (df_csv['Date'] - datetime_cible).abs()
    if diff.empty:
        return None
    idx = diff.idxmin()
    if diff[idx] > pd.Timedelta(hours=TOLERANCE_H):
        return None
    return df_csv.loc[idx, 'WSH']


# ─────────────────────────────────────────────
# Normalisation
# ─────────────────────────────────────────────
def normaliser(df, cols):
    df_norm = df.copy()
    for col in cols:
        if col in df.columns:
            scaler       = MinMaxScaler()
            df_norm[col] = scaler.fit_transform(df[[col]])
    return df_norm


# ─────────────────────────────────────────────
# Métriques
# ─────────────────────────────────────────────
def calculer_metriques(h, p):
    if len(h) < 5:
        return None
    rmse  = np.sqrt(np.mean((h - p) ** 2))
    r     = np.corrcoef(h, p)[0, 1]
    alpha = np.std(p)  / np.std(h)  if np.std(h)  > 0 else 0
    beta  = np.mean(p) / np.mean(h) if np.mean(h) != 0 else 0
    kge   = 1 - np.sqrt((r-1)**2 + (alpha-1)**2 + (beta-1)**2)
    return {'rmse': rmse, 'kge': kge, 'r': r}


# ─────────────────────────────────────────────
# Visualisation — comparaison BDD vs CSV
# ─────────────────────────────────────────────
def plot_resultats(resultats_db, resultats_csv):
    metriques = [('rmse', 'RMSE', False), ('kge', 'KGE', True), ('r', 'Corrélation (r)', True)]
    colors    = ['#FF5722', '#2196F3', '#4CAF50', '#9C27B0', '#FF9800']
    noms      = INSITU_COLS + ['wsh_csv']
    labels    = ['h_01h', 'h_09h', 'h_17h', 'h_med', 'WSH brut']

    fig, axes = plt.subplots(1, 3, figsize=(18, 6))
    fig.suptitle(f"Hydro vs Insitu — {len(resultats_db)} stations\nBDD (4 variables) + WSH brut CSV",
                 fontsize=13, fontweight='bold')

    for ax, (key, label, meilleur_haut) in zip(axes, metriques):
        valeurs = []
        for col in INSITU_COLS:
            vals = [resultats_db[s][col][key] for s in resultats_db
                    if col in resultats_db[s] and resultats_db[s][col] is not None
                    and not np.isnan(resultats_db[s][col][key])]
            valeurs.append(np.mean(vals) if vals else np.nan)

        vals_csv = [resultats_csv[s][key] for s in resultats_csv
                    if not np.isnan(resultats_csv[s][key]) and resultats_csv[s]['kge'] > -1]
        valeurs.append(np.mean(vals_csv) if vals_csv else np.nan)

        vals_ok  = [v for v in valeurs if not np.isnan(v)]
        if not vals_ok:
            continue

        clrs     = list(colors[:len(noms)])
        best_idx = int(np.argmax(valeurs)) if meilleur_haut else int(np.argmin(valeurs))
        clrs[best_idx] = 'seagreen'

        ax.bar(range(len(noms)), valeurs, color=clrs, alpha=0.85, edgecolor='white', width=0.6)
        ax.text(best_idx, valeurs[best_idx] + max(vals_ok) * 0.03,
                '★', ha='center', va='bottom', fontsize=14, color='gold')
        for i, v in enumerate(valeurs):
            if not np.isnan(v):
                ax.text(i, v + max(vals_ok) * 0.01, f"{v:.3f}",
                        ha='center', va='bottom', fontsize=8, fontweight='bold')

        ax.set_xticks(range(len(noms)))
        ax.set_xticklabels(labels, fontsize=9)
        ax.set_title(label, fontsize=11, fontweight='bold')
        ax.set_ylabel(label, fontsize=9)
        ax.grid(axis='y', alpha=0.3)

    plt.tight_layout()
    path = os.path.join(OUTPUT_DIR, f"comparaison_db_vs_csv_{N_STATIONS}stations.png")
    plt.savefig(path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  📊 {path}")


# ─────────────────────────────────────────────
# Visualisation — métriques par catégorie sauts
# ─────────────────────────────────────────────
def plot_par_sauts(df_res):
    METRIQUES = [
        ('rmse', 'RMSE',            False),
        ('kge',  'KGE',             True),
        ('r',    'Corrélation (r)', True),
    ]

    for metrique, titre_metrique, meilleur_haut in METRIQUES:
        fig, ax = plt.subplots(figsize=(10, 6))
        fig.suptitle(
            f"{titre_metrique} par catégorie de sauts — WSH brut CSV\n"
            f"({len(df_res)} paires valides sur {N_STATIONS} stations)",
            fontsize=13, fontweight='bold'
        )

        moyennes = []
        labels   = []
        for cat in CATEGORIES_SAUTS:
            groupe = df_res[df_res['qualite_sauts'] == cat][metrique].dropna()
            moyennes.append(groupe.mean() if not groupe.empty else np.nan)
            labels.append(f"{cat}\n(n={len(groupe)})")

        vals_ok = [v for v in moyennes if not np.isnan(v)]
        if not vals_ok:
            continue

        best_idx = int(np.argmax(moyennes)) if meilleur_haut else int(np.argmin(moyennes))
        colors   = list(COLORS_SAUTS)
        colors[best_idx] = 'seagreen'

        ax.bar(range(len(CATEGORIES_SAUTS)), moyennes,
               color=colors, alpha=0.85, edgecolor='white', width=0.6)
        ax.text(best_idx, moyennes[best_idx] + max(vals_ok) * 0.02,
                '★', ha='center', va='bottom', fontsize=16, color='gold')

        for i, v in enumerate(moyennes):
            if not np.isnan(v):
                ax.text(i, v + max(vals_ok) * 0.01, f"{v:.3f}",
                        ha='center', va='bottom', fontsize=10, fontweight='bold')

        ax.set_xticks(range(len(CATEGORIES_SAUTS)))
        ax.set_xticklabels(labels, fontsize=10)
        ax.set_ylabel(titre_metrique, fontsize=10)
        ax.set_xlabel("Catégorie de sauts brutaux", fontsize=10)
        ax.grid(axis='y', alpha=0.3)

        plt.tight_layout()
        path = os.path.join(OUTPUT_DIR, f"{metrique}_par_sauts.png")
        plt.savefig(path, dpi=150, bbox_inches='tight')
        plt.close()
        print(f"  📊 {path}")


# ─────────────────────────────────────────────
# Visualisation — métriques par flags
# ─────────────────────────────────────────────
def plot_par_flags(df_res):
    FLAGS = [
        ('dans_lac',    'Dans/proche lac', 'Non lac',    'Lac/proche lac'),
        ('signal_plat', 'Signal plat',     'Signal OK',  'Signal plat'),
        ('sauts_ok',    'Qualité sauts',   '> 10 sauts', 'Aucun/< 10'),
        ('gap',         'Gap temporel',    'Pas de gap', 'Gap détecté'),
    ]
    METRIQUES = [
        ('rmse', 'RMSE',            False),
        ('kge',  'KGE',             True),
        ('r',    'Corrélation (r)', True),
    ]

    for metrique, titre_metrique, meilleur_haut in METRIQUES:
        fig, axes = plt.subplots(1, 4, figsize=(18, 6))
        fig.suptitle(
            f"{titre_metrique} par flag — WSH brut CSV\n"
            f"({len(df_res)} paires valides sur {N_STATIONS} stations)",
            fontsize=13, fontweight='bold'
        )

        for ax, (flag_col, titre, label_false, label_true) in zip(axes, FLAGS):
            if flag_col not in df_res.columns:
                continue

            groupe_false = df_res[df_res[flag_col] == False][metrique].dropna()
            groupe_true  = df_res[df_res[flag_col] == True][metrique].dropna()

            moy_false = groupe_false.mean() if not groupe_false.empty else np.nan
            moy_true  = groupe_true.mean()  if not groupe_true.empty  else np.nan

            valeurs = [moy_false, moy_true]
            labels  = [
                f"{label_false}\n(n={len(groupe_false)})",
                f"{label_true}\n(n={len(groupe_true)})"
            ]
            colors  = ['#2196F3', '#FF5722']

            vals_ok = [v for v in valeurs if not np.isnan(v)]
            if not vals_ok:
                continue

            best_idx = int(np.argmax(valeurs)) if meilleur_haut else int(np.argmin(valeurs))
            colors[best_idx] = 'seagreen'

            ax.bar(range(2), valeurs, color=colors, alpha=0.85, edgecolor='white', width=0.5)
            ax.text(best_idx, valeurs[best_idx] + max(vals_ok) * 0.02,
                    '★', ha='center', va='bottom', fontsize=14, color='gold')

            for i, v in enumerate(valeurs):
                if not np.isnan(v):
                    ax.text(i, v + max(vals_ok) * 0.01, f"{v:.3f}",
                            ha='center', va='bottom', fontsize=10, fontweight='bold')

            ax.set_xticks(range(2))
            ax.set_xticklabels(labels, fontsize=9)
            ax.set_title(titre, fontsize=10, fontweight='bold')
            ax.set_ylabel(titre_metrique, fontsize=9)
            ax.grid(axis='y', alpha=0.3)

        plt.tight_layout()
        path = os.path.join(OUTPUT_DIR, f"{metrique}_par_flags.png")
        plt.savefig(path, dpi=150, bbox_inches='tight')
        plt.close()
        print(f"  📊 {path}")


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────
if __name__ == "__main__":
    hydro_conn     = sqlite3.connect(HYDRO_DB_PATH)
    stations_hydro = get_stations_hydro(N_STATIONS)

    resultats_db  = {}
    resultats_csv = {}
    paires        = []
    traitees      = 0

    for station_hydro in stations_hydro:
        coords = get_station_coordinates(hydro_conn, station_hydro)
        if coords is None:
            continue
        lon_h, lat_h, _ = coords

        station_insitu, dist_m, lon_i, lat_i = station_la_plus_proche(lon_h, lat_h)
        if station_insitu is None:
            continue

        meme_riviere, dist_km = verifier_meme_riviere(lon_h, lat_h, lon_i, lat_i)
        if not meme_riviere:
            continue

        # Données hydro
        df_hydro = get_donnees_hydro(station_hydro)
        if df_hydro.empty:
            continue

        # BDD insitu — 4 variables
        df_insitu = get_donnees_insitu_db(station_insitu)
        df_merged = pd.merge(
            df_hydro[['date', 'orthometric_height']],
            df_insitu, on='date', how='inner'
        ).dropna()

        if len(df_merged) >= 5:
            df_norm = normaliser(df_merged, ['orthometric_height'] + INSITU_COLS)
            resultats_db[station_hydro] = {}
            for col in INSITU_COLS:
                h = df_norm['orthometric_height'].values
                p = df_norm[col].values
                resultats_db[station_hydro][col] = calculer_metriques(h, p)

        # CSV brut
        df_csv = charger_csv_insitu(station_insitu)
        if df_csv is not None:
            rows = []
            for _, row in df_hydro.iterrows():
                wsh = get_wsh_proche(df_csv, row['datetime'])
                if wsh is not None:
                    rows.append({'orthometric_height': row['orthometric_height'], 'wsh': wsh})

            if len(rows) >= 5:
                df_paire = pd.DataFrame(rows)
                df_norm  = normaliser(df_paire, ['orthometric_height', 'wsh'])
                m = calculer_metriques(df_norm['orthometric_height'].values,
                                       df_norm['wsh'].values)
                if m:
                    resultats_csv[station_hydro] = m
                    qualite = get_qualite_sauts(station_insitu)
                    flags   = get_flags_insitu(station_insitu) or {}
                    paires.append({
                        'station_hydro':  station_hydro,
                        'station_insitu': station_insitu,
                        'qualite_sauts':  qualite,
                        **flags,
                        **m
                    })

        traitees += 1
        if traitees % 10 == 0:
            print(f"  [{traitees}/{N_STATIONS}] en cours...")

    hydro_conn.close()

    # Résumé terminal global
    print(f"\n✅ {traitees} stations traitées")
    print(f"\n{'═'*58}")
    print(f"  {'Variable':<15} {'RMSE':>8} {'KGE':>8} {'r':>8}")
    print(f"  {'─'*50}")
    for col in INSITU_COLS:
        rmse_v = [resultats_db[s][col]['rmse'] for s in resultats_db if resultats_db[s].get(col)]
        kge_v  = [resultats_db[s][col]['kge']  for s in resultats_db if resultats_db[s].get(col)]
        r_v    = [resultats_db[s][col]['r']     for s in resultats_db if resultats_db[s].get(col)]
        print(f"  {col:<15} {np.mean(rmse_v):>8.4f} {np.mean(kge_v):>8.4f} {np.mean(r_v):>8.4f}")

    csv_valides = {s: m for s, m in resultats_csv.items() if m['kge'] > -1}
    print(f"  {'WSH brut CSV':<15} "
          f"{np.mean([m['rmse'] for m in csv_valides.values()]):>8.4f} "
          f"{np.mean([m['kge']  for m in csv_valides.values()]):>8.4f} "
          f"{np.mean([m['r']    for m in csv_valides.values()]):>8.4f}")
    print(f"{'═'*58}")

    # Résumé par catégorie de sauts
    df_paires = pd.DataFrame(paires)
    print(f"\n{'═'*58}")
    print(f"  {'Catégorie':<12} {'n':>5} {'RMSE':>8} {'KGE':>8} {'r':>8}")
    print(f"  {'─'*50}")
    for cat in CATEGORIES_SAUTS:
        g = df_paires[df_paires['qualite_sauts'] == cat]
        if not g.empty:
            print(f"  {cat:<12} {len(g):>5} "
                  f"{g['rmse'].mean():>8.3f} "
                  f"{g['kge'].mean():>8.3f} "
                  f"{g['r'].mean():>8.3f}")
    print(f"{'═'*58}")

    # Résumé par flags
    print(f"\n{'═'*58}")
    for flag_col, titre, label_false, label_true in [
        ('dans_lac',    'Dans lac',    'Non', 'Oui'),
        ('signal_plat', 'Signal plat', 'Non', 'Oui'),
        ('sauts_ok',    'Sauts OK',    'Non', 'Oui'),
        ('gap',         'Gap',         'Non', 'Oui'),
    ]:
        if flag_col not in df_paires.columns:
            continue
        print(f"\n  {titre}")
        for val, label in [(False, label_false), (True, label_true)]:
            g = df_paires[df_paires[flag_col] == val]
            if not g.empty:
                print(f"    {label:<6} (n={len(g):>3}) "
                      f"RMSE={g['rmse'].mean():.3f} "
                      f"KGE={g['kge'].mean():.3f} "
                      f"r={g['r'].mean():.3f}")
    print(f"{'═'*58}")

    # Visualisations
    plot_resultats(resultats_db, resultats_csv)
    plot_par_sauts(df_paires)
    plot_par_flags(df_paires)