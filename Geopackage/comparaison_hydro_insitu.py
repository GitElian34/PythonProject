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
        idx = abs(df['Date'] - cible).idxmin()
        resultats.append(df.loc[idx, 'WSH'])
    return resultats


def compare_hydro_insitu(station_hydro):
    conn = sqlite3.connect('./data/hydro_data.db')
    data_hydro = get_station_measurements(conn, station_hydro)
    lon, lat = get_station_coordinates(conn, station_hydro)
    conn.close()

    station_insitu, distance = station_la_plus_proche(lon, lat)
    print(f"  📍 Station insitu: {station_insitu} | Distance: {distance:.0f} m")

    hauteurs_hydro = [row[2] for row in data_hydro]
    date_time_list = [(row[0], row[1]) for row in data_hydro]
    data_insitu = get_closest_measurements_bulk(station_insitu, date_time_list, "./data/insitu/data")

    df = normalise_et_compare(hauteurs_hydro, data_insitu, dates=[row[0] for row in data_hydro])

    if df is not None and len(df) > 0:
        return df['ecart_norm'].mean(), distance
    return None, distance


def normalise_et_compare(hauteurs_hydro, hauteurs_insitu, dates=None):
    df = pd.DataFrame({
        'date': dates if dates else range(len(hauteurs_hydro)),
        'hydro_brut': hauteurs_hydro,
        'insitu_brut': hauteurs_insitu
    })
    df = df.dropna(subset=['hydro_brut', 'insitu_brut'])
    if df.empty:
        return df
    for col, col_norm in [('hydro_brut', 'hydro_norm'), ('insitu_brut', 'insitu_norm')]:
        min_val, max_val = df[col].min(), df[col].max()
        df[col_norm] = (df[col] - min_val) / (max_val - min_val) if max_val != min_val else 0
    df['ecart_norm'] = abs(df['hydro_norm'] - df['insitu_norm'])
    return df


def plot_distance_vs_ecart(resultats):
    """
    Affiche la relation entre distance et écart moyen normalisé

    Args:
        resultats: liste de (station_code, ecart_moyen, distance_m)
    """
    df = pd.DataFrame(resultats, columns=['station', 'ecart_moyen', 'distance_m'])
    df = df.dropna()
    df['distance_km'] = df['distance_m'] / 1000

    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    fig.suptitle("Impact de la distance sur l'écart moyen normalisé\n(stations hydro vs in-situ)", fontsize=14)

    # --- Scatter plot avec régression ---
    ax1 = axes[0]
    ax1.scatter(df['distance_km'], df['ecart_moyen'], alpha=0.6, color='steelblue', edgecolors='white', s=80)

    # Ligne de tendance
    z = np.polyfit(df['distance_km'], df['ecart_moyen'], 1)
    p = np.poly1d(z)
    x_line = np.linspace(df['distance_km'].min(), df['distance_km'].max(), 100)
    ax1.plot(x_line, p(x_line), color='tomato', linewidth=2, linestyle='--', label=f'Tendance')

    # Corrélation
    corr = df['distance_km'].corr(df['ecart_moyen'])
    ax1.set_xlabel("Distance entre stations (km)", fontsize=11)
    ax1.set_ylabel("Écart moyen normalisé", fontsize=11)
    ax1.set_title(f"Scatter plot (r = {corr:.3f})", fontsize=12)
    ax1.legend()
    ax1.grid(True, alpha=0.3)

    # --- Boxplot par tranches dynamiques (4 quantiles) ---
    ax2 = axes[1]

    n_tranches = min(4, len(df))
    try:
        _, bins_used = pd.qcut(df['distance_km'], q=n_tranches, duplicates='drop', retbins=True)
        labels_used = [f"{bins_used[i]:.1f}-{bins_used[i + 1]:.1f} km"
                       for i in range(len(bins_used) - 1)]
        df['tranche'] = pd.qcut(df['distance_km'], q=n_tranches, duplicates='drop', labels=labels_used)
    except ValueError:
        df['tranche'] = 'toutes distances'
        labels_used = ['toutes distances']

    categories = df['tranche'].unique() if hasattr(df['tranche'], 'cat') else df['tranche'].unique()
    groupes = [(label, df[df['tranche'] == label]['ecart_moyen'].values)
               for label in labels_used if len(df[df['tranche'] == label]) > 0]

    bp = ax2.boxplot([g for _, g in groupes], labels=[l for l, _ in groupes], patch_artist=True)
    colors = plt.cm.RdYlGn_r(np.linspace(0.2, 0.8, len(groupes)))
    for patch, color in zip(bp['boxes'], colors):
        patch.set_facecolor(color)
        patch.set_alpha(0.7)
    for i, (label, groupe) in enumerate(groupes):
        ax2.text(i + 1, ax2.get_ylim()[0], f'n={len(groupe)}',
                 ha='center', va='bottom', fontsize=9, color='gray')

    plt.tight_layout()
    plt.savefig('./data/distance_vs_ecart.png', dpi=150, bbox_inches='tight')
    print("\n📊 Graphique sauvegardé : ./data/distance_vs_ecart.png")
    plt.show()

    # Résumé stats
    print("\n📈 Résumé statistique :")
    print(f"  Corrélation distance / écart : {corr:.3f}")
    print(f"  Écart moyen global           : {df['ecart_moyen'].mean():.4f}")
    print(f"  Station la plus proche       : {df['distance_km'].min():.1f} km → écart {df.loc[df['distance_km'].idxmin(), 'ecart_moyen']:.4f}")
    print(f"  Station la plus éloignée     : {df['distance_km'].max():.1f} km → écart {df.loc[df['distance_km'].idxmax(), 'ecart_moyen']:.4f}")


if __name__ == "__main__":
    conn = sqlite3.connect('./data/hydro_data.db')
    cursor = conn.cursor()
    cursor.execute('SELECT station_code FROM stations LIMIT 200')
    stations = [row[0] for row in cursor.fetchall()]
    conn.close()

    resultats = []
    for i, station in enumerate(stations):
        print(f"\n🔄 [{i+1}/{len(stations)}] Station {station}")
        try:
            ecart, distance = compare_hydro_insitu(station)
            if ecart is not None:
                resultats.append((station, ecart, distance))
        except Exception as e:
            print(f"  ⚠️  Erreur: {e}")

    print(f"\n✅ {len(resultats)}/{len(stations)} stations traitées avec succès")
    plot_distance_vs_ecart(resultats)