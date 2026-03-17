from Geopackage.Sword_request import point_dans_riviere
from Geopackage.visualisation import station_la_plus_proche
from data_processing.db_manager import *
import pandas as pd
import glob
import os
import matplotlib.pyplot as plt
import numpy as np


def get_closest_measurements_bulk(station_code, date_time_list, csv_dir="."):
    pattern = os.path.join(csv_dir, f"*{station_code}*.csv")
    fichiers = glob.glob(pattern)
    if not fichiers:
        print(f"❌ Fichier non trouvé pour {station_code} dans {csv_dir}")
        return [None] * len(date_time_list)
    df = pd.read_csv(fichiers[0])
    if 'Date' not in df.columns or 'WSH' not in df.columns:
        print(f"❌ Colonnes attendues non trouvées. Colonnes: {list(df.columns)}")
        return [None] * len(date_time_list)
    df['Date'] = pd.to_datetime(df['Date'])
    resultats = []
    for date, time in date_time_list:
        cible = pd.to_datetime(f"{date} {time}").tz_localize("UTC")
        diff = abs(df['Date'] - cible)
        idx = diff.idxmin()
        if diff[idx] > pd.Timedelta(days=1):  # tolérance à ajuster
            resultats.append(None)
            continue
        resultats.append(df.loc[idx, 'WSH'])
    return resultats


def compare_hydro_insitu(station_hydro):
    conn = sqlite3.connect('./data/hydro_data.db')
    seuil = 8000.0
    data_hydro = get_station_measurements(conn, station_hydro)
    lon, lat,river_name  = get_station_coordinates(conn, station_hydro)
    conn.close()

    station_insitu, distance,lon_insitu,lat_insitu  = station_la_plus_proche(lon, lat,river_name)
    #point_dans_riviere(lon_insitu, lat_insitu) and
    if distance < seuil:
        print(f"  📍 Station insitu: {station_insitu} | Distance: {distance:.0f} m")

        hauteurs_hydro = [row[2] for row in data_hydro]
        date_time_list = [(row[0], row[1]) for row in data_hydro]
        data_insitu = get_closest_measurements_bulk(station_insitu, date_time_list, "./data/insitu/data")

        df = normalise_et_compare(hauteurs_hydro, data_insitu, dates=[row[0] for row in data_hydro])

        if df is not None and len(df) > 0:
            ecart = df['ecart_norm'].mean()
            pearson = df['pearson'].iloc[0]
            nse = df['nse'].iloc[0]
            print(f"  ecart_norm={ecart:.3f} | Pearson={pearson:.3f} | NSE={nse:.3f}")
            return ecart, pearson, nse, distance
        return None, None, None, distance


def normalise_et_compare(hauteurs_hydro, hauteurs_insitu, dates=None):
    df = pd.DataFrame({
        'date': dates if dates else range(len(hauteurs_hydro)),
        'hydro_brut': hauteurs_hydro,
        'insitu_brut': hauteurs_insitu
    })
    df = df.dropna(subset=['hydro_brut', 'insitu_brut'])
    if df.empty:
        return df

    # Normalisation min-max (gardée pour l'écart_norm existant)
    for col, col_norm in [('hydro_brut', 'hydro_norm'), ('insitu_brut', 'insitu_norm')]:
        min_val, max_val = df[col].min(), df[col].max()
        df[col_norm] = (df[col] - min_val) / (max_val - min_val) if max_val != min_val else 0
    df['ecart_norm'] = abs(df['hydro_norm'] - df['insitu_norm'])

    # Pearson sur les valeurs brutes
    if df['hydro_brut'].std() > 0 and df['insitu_brut'].std() > 0:
        df['pearson'] = df['hydro_brut'].corr(df['insitu_brut'])
    else:
        df['pearson'] = np.nan

    # NSE sur les valeurs normalisées (même référentiel)
    obs = df['insitu_norm'].values
    sim = df['hydro_norm'].values
    denom = np.sum((obs - obs.mean()) ** 2)
    df['nse'] = 1 - np.sum((obs - sim) ** 2) / denom if denom > 0 else np.nan
    return df

def plot_distance_vs_ecart(resultats):
    df = pd.DataFrame(resultats, columns=['station', 'ecart_moyen', 'pearson', 'nse', 'distance_m'])
    df = df.dropna()
    df['distance_km'] = df['distance_m'] / 1000

    fig, axes = plt.subplots(2, 2, figsize=(14, 12))
    fig.suptitle("Comparaison stations hydro vs in-situ", fontsize=14)

    # --- 1. Scatter écart_norm vs distance ---
    ax1 = axes[0, 0]
    ax1.scatter(df['distance_km'], df['ecart_moyen'], alpha=0.6, color='steelblue', edgecolors='white', s=80)
    z = np.polyfit(df['distance_km'], df['ecart_moyen'], 1)
    p = np.poly1d(z)
    x_line = np.linspace(df['distance_km'].min(), df['distance_km'].max(), 100)
    ax1.plot(x_line, p(x_line), color='tomato', linewidth=2, linestyle='--', label='Tendance')
    corr = df['distance_km'].corr(df['ecart_moyen'])
    ax1.set_xlabel("Distance (km)", fontsize=11)
    ax1.set_ylabel("Écart moyen normalisé", fontsize=11)
    ax1.set_title(f"Écart norm vs Distance (r={corr:.3f})", fontsize=12)
    ax1.legend()
    ax1.grid(True, alpha=0.3)

    # --- 2. Scatter Pearson vs distance ---
    ax2 = axes[0, 1]
    ax2.scatter(df['distance_km'], df['pearson'], alpha=0.6, color='seagreen', edgecolors='white', s=80)
    z2 = np.polyfit(df['distance_km'], df['pearson'], 1)
    p2 = np.poly1d(z2)
    ax2.plot(x_line, p2(x_line), color='tomato', linewidth=2, linestyle='--', label='Tendance')
    corr2 = df['distance_km'].corr(df['pearson'])
    ax2.set_xlabel("Distance (km)", fontsize=11)
    ax2.set_ylabel("Corrélation de Pearson", fontsize=11)
    ax2.set_title(f"Pearson vs Distance (r={corr2:.3f})", fontsize=12)
    ax2.axhline(0, color='gray', linewidth=0.8, linestyle=':')
    ax2.legend()
    ax2.grid(True, alpha=0.3)

    # --- 3. Scatter NSE vs distance ---
    ax3 = axes[1, 0]
    # Clamp NSE pour la lisibilité (outliers extrêmes)
    nse_clamped = df['nse'].clip(lower=-2)
    ax3.scatter(df['distance_km'], nse_clamped, alpha=0.6, color='mediumpurple', edgecolors='white', s=80)
    z3 = np.polyfit(df['distance_km'], nse_clamped, 1)
    p3 = np.poly1d(z3)
    ax3.plot(x_line, p3(x_line), color='tomato', linewidth=2, linestyle='--', label='Tendance')
    corr3 = df['distance_km'].corr(nse_clamped)
    ax3.set_xlabel("Distance (km)", fontsize=11)
    ax3.set_ylabel("NSE (clampé à -2)", fontsize=11)
    ax3.set_title(f"NSE vs Distance (r={corr3:.3f})", fontsize=12)
    ax3.axhline(0, color='gray', linewidth=0.8, linestyle=':')
    ax3.axhline(1, color='green', linewidth=0.8, linestyle=':')
    ax3.legend()
    ax3.grid(True, alpha=0.3)

    # --- 4. Boxplot écart_norm par tranches de distance ---
    ax4 = axes[1, 1]
    n_tranches = min(4, len(df))
    try:
        _, bins_used = pd.qcut(df['distance_km'], q=n_tranches, duplicates='drop', retbins=True)
        labels_used = [f"{bins_used[i]:.1f}-{bins_used[i+1]:.1f} km" for i in range(len(bins_used) - 1)]
        df['tranche'] = pd.qcut(df['distance_km'], q=n_tranches, duplicates='drop', labels=labels_used)
    except ValueError:
        df['tranche'] = 'toutes distances'
        labels_used = ['toutes distances']
    groupes = [(label, df[df['tranche'] == label]['ecart_moyen'].values)
               for label in labels_used if len(df[df['tranche'] == label]) > 0]
    bp = ax4.boxplot([g for _, g in groupes], labels=[l for l, _ in groupes], patch_artist=True)
    colors = plt.cm.RdYlGn_r(np.linspace(0.2, 0.8, len(groupes)))
    for patch, color in zip(bp['boxes'], colors):
        patch.set_facecolor(color)
        patch.set_alpha(0.7)
    for i, (label, groupe) in enumerate(groupes):
        ax4.text(i + 1, ax4.get_ylim()[0], f'n={len(groupe)}',
                 ha='center', va='bottom', fontsize=9, color='gray')
    ax4.set_title("Boxplot écart norm par tranche de distance", fontsize=12)
    ax4.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig('./data/distance_vs_ecart.png', dpi=150, bbox_inches='tight')
    print("\n📊 Graphique sauvegardé : ./data/distance_vs_ecart.png")
    plt.show()

    print("\n📈 Résumé statistique :")
    print(f"  Corrélation distance / écart_norm : {corr:.3f}")
    print(f"  Corrélation distance / Pearson    : {corr2:.3f}")
    print(f"  Corrélation distance / NSE        : {corr3:.3f}")
    print(f"  Écart norm moyen   : {df['ecart_moyen'].mean():.4f}")
    print(f"  Pearson moyen      : {df['pearson'].mean():.4f}")
    print(f"  NSE moyen          : {df['nse'].mean():.4f}  (brut, avant clamp)")
    print(f"  Station la plus proche  : {df['distance_km'].min():.1f} km → écart {df.loc[df['distance_km'].idxmin(), 'ecart_moyen']:.4f}")
    print(f"  Station la plus éloignée: {df['distance_km'].max():.1f} km → écart {df.loc[df['distance_km'].idxmax(), 'ecart_moyen']:.4f}")

if __name__ == "__main__":
    conn = sqlite3.connect('./data/hydro_data.db')
    cursor = conn.cursor()
    cursor.execute('SELECT station_code FROM stations LIMIT 200')
    stations = [row[0] for row in cursor.fetchall()]
    conn.close()

    resultats = []
    exclues = 0
    for i, station in enumerate(stations):
        print(f"\n🔄 [{i + 1}/{len(stations)}] Station {station}")
        try:
            ecart, pearson, nse, distance = compare_hydro_insitu(station)
            if ecart is not None and ecart <= 0.35:
                resultats.append((station, ecart, pearson, nse, distance))
            elif ecart is not None:
                exclues += 1
                print(f"  🚫 Exclue (écart_norm={ecart:.3f})")
        except Exception as e:
            print(f"  ⚠️  Erreur: {e}")

    print(f"\n✅ {len(resultats)}/{len(stations)} stations retenues ({exclues} exclues pour écart > 0.35)")
    print(f"Moyenne écart_norm : {np.mean([r[1] for r in resultats]):.4f}")
    print(f"Moyenne Pearson    : {np.mean([r[2] for r in resultats]):.4f}")
    print(f"Moyenne NSE        : {np.mean([r[3] for r in resultats]):.4f}")
    print(
        f"\n📊 Moyenne des écarts normalisés pour toutes les stations : {np.mean([r[1] for r in resultats if r[1] is not None]):.4f}")
    plot_distance_vs_ecart(resultats)