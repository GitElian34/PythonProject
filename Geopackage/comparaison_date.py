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
        return [None] * len(date_time_list)
    df = pd.read_csv(fichiers[0])
    if 'Date' not in df.columns or 'WSH' not in df.columns:
        return [None] * len(date_time_list)
    df['Date'] = pd.to_datetime(df['Date'])
    resultats = []
    for date, time in date_time_list:
        cible = pd.to_datetime(f"{date} {time}").tz_localize("UTC")
        diff = abs(df['Date'] - cible)
        idx = diff.idxmin()
        # Filtre temporel : 1 jour max
        if diff[idx] > pd.Timedelta(days=1):
            resultats.append(None)
        else:
            resultats.append(df.loc[idx, 'WSH'])
    return resultats


def compare_hydro_insitu_par_date(station_hydro):
    conn = sqlite3.connect('./data/hydro_data.db')
    data_hydro = get_station_measurements(conn, station_hydro)
    lon, lat,_ = get_station_coordinates(conn, station_hydro)
    conn.close()

    station_insitu, distance, lon_insitu, lat_insitu, dans_riviere = station_la_plus_proche(lon, lat)

    if not dans_riviere or distance is None:
        print(f"  ❌ Station ignorée (pas dans rivière)")
        return None

    print(f"  📍 Station insitu: {station_insitu} | Distance: {distance:.0f} m")

    hauteurs_hydro = [row[2] for row in data_hydro]
    date_time_list = [(row[0], row[1]) for row in data_hydro]
    dates = [row[0] for row in data_hydro]

    data_insitu = get_closest_measurements_bulk(station_insitu, date_time_list, "./data/insitu/data")

    df = pd.DataFrame({
        'date': pd.to_datetime(dates),
        'hydro_brut': hauteurs_hydro,
        'insitu_brut': data_insitu
    })
    df = df.dropna(subset=['hydro_brut', 'insitu_brut'])
    if df.empty:
        return None

    # Normalisation
    for col, col_norm in [('hydro_brut', 'hydro_norm'), ('insitu_brut', 'insitu_norm')]:
        min_val, max_val = df[col].min(), df[col].max()
        df[col_norm] = (df[col] - min_val) / (max_val - min_val) if max_val != min_val else 0

    df['ecart_norm'] = abs(df['hydro_norm'] - df['insitu_norm'])

    # Filtre qualité : station avec écart moyen trop élevé
    if df['ecart_norm'].mean() > 0.35:
        print(f"  🚫 Station exclue (écart_norm moyen={df['ecart_norm'].mean():.3f} > 0.35)")
        return None

    # Pearson
    if df['hydro_norm'].std() > 0 and df['insitu_norm'].std() > 0:
        df['pearson'] = df['hydro_norm'].corr(df['insitu_norm'])
    else:
        df['pearson'] = np.nan

    # NSE sur valeurs normalisées
    obs = df['insitu_norm'].values
    sim = df['hydro_norm'].values
    denom = np.sum((obs - obs.mean()) ** 2)
    df['nse'] = 1 - np.sum((obs - sim) ** 2) / denom if denom > 0 else np.nan

    df['mois'] = df['date'].dt.month
    df['annee'] = df['date'].dt.year
    df['station'] = station_hydro
    df['distance'] = distance

    return df


def plot_ecart_par_mois(df_all):
    mois_labels = ['Jan', 'Fév', 'Mar', 'Avr', 'Mai', 'Jun',
                   'Jul', 'Aoû', 'Sep', 'Oct', 'Nov', 'Déc']

    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle("Variation des écarts normalisés selon le mois\n(agrégation sur 100 stations)",
                 fontsize=14, fontweight='bold')

    # --- 1. Boxplot par mois ---
    ax1 = axes[0, 0]
    groupes_mois = [df_all[df_all['mois'] == m]['ecart_norm'].values for m in range(1, 13)]
    groupes_mois_non_vides = [(mois_labels[i], g) for i, g in enumerate(groupes_mois) if len(g) > 0]
    bp = ax1.boxplot([g for _, g in groupes_mois_non_vides],
                     labels=[l for l, _ in groupes_mois_non_vides],
                     patch_artist=True, showfliers=False)
    colors = plt.cm.coolwarm(np.linspace(0, 1, len(groupes_mois_non_vides)))
    for patch, color in zip(bp['boxes'], colors):
        patch.set_facecolor(color)
        patch.set_alpha(0.7)
    ax1.set_title("Distribution des écarts par mois", fontsize=11)
    ax1.set_xlabel("Mois")
    ax1.set_ylabel("Écart normalisé")
    ax1.grid(True, alpha=0.3, axis='y')

    # --- 2. Pearson et NSE moyens par mois ---
    ax2 = axes[0, 1]
    pearson_mois = df_all.groupby('mois')['pearson'].mean().reindex(range(1, 13))
    nse_mois = df_all.groupby('mois')['nse'].mean().reindex(range(1, 13))
    ax2.plot(range(1, 13), pearson_mois, marker='o', color='seagreen', linewidth=2, label='Pearson')
    ax2.plot(range(1, 13), nse_mois.clip(lower=-2), marker='s', color='mediumpurple', linewidth=2, label='NSE (clampé -2)')
    ax2.axhline(0, color='gray', linewidth=0.8, linestyle=':')
    ax2.set_xticks(range(1, 13))
    ax2.set_xticklabels(mois_labels)
    ax2.set_title("Pearson et NSE moyens par mois", fontsize=11)
    ax2.set_xlabel("Mois")
    ax2.legend()
    ax2.grid(True, alpha=0.3)

    # --- 3. Heatmap mois x année ---
    ax3 = axes[1, 0]
    pivot = df_all.groupby(['annee', 'mois'])['ecart_norm'].mean().unstack(level='mois')
    pivot.columns = [mois_labels[m - 1] for m in pivot.columns]
    im = ax3.imshow(pivot.values, aspect='auto', cmap='RdYlGn_r', interpolation='nearest')
    ax3.set_xticks(range(len(pivot.columns)))
    ax3.set_xticklabels(pivot.columns, fontsize=9)
    ax3.set_yticks(range(len(pivot.index)))
    ax3.set_yticklabels(pivot.index, fontsize=9)
    ax3.set_title("Heatmap écart moyen (année × mois)", fontsize=11)
    plt.colorbar(im, ax=ax3, label='Écart normalisé moyen')

    # --- 4. Écart moyen par année ---
    ax4 = axes[1, 1]
    ecart_annee = df_all.groupby('annee')['ecart_norm'].mean()
    bars = ax4.bar(ecart_annee.index, ecart_annee.values, alpha=0.7, edgecolor='white')
    norm_vals = (ecart_annee.values - ecart_annee.values.min()) / (ecart_annee.values.max() - ecart_annee.values.min() + 1e-9)
    for bar, nv in zip(bars, norm_vals):
        bar.set_facecolor(plt.cm.RdYlGn_r(nv))
    ax4.set_title("Écart moyen par année", fontsize=11)
    ax4.set_xlabel("Année")
    ax4.set_ylabel("Écart moyen normalisé")
    ax4.grid(True, alpha=0.3, axis='y')

    plt.tight_layout()
    plt.savefig('./data/ecart_par_mois.png', dpi=150, bbox_inches='tight')
    print("\n📊 Graphique sauvegardé : ./data/ecart_par_mois.png")
    plt.show()

    ecart_mois_mean = df_all.groupby('mois')['ecart_norm'].mean()
    print("\n📈 Résumé statistique :")
    print(f"  Mois avec le plus grand écart : {mois_labels[ecart_mois_mean.idxmax() - 1]} ({ecart_mois_mean.max():.4f})")
    print(f"  Mois avec le plus petit écart : {mois_labels[ecart_mois_mean.idxmin() - 1]} ({ecart_mois_mean.min():.4f})")
    print(f"  Écart moyen global            : {df_all['ecart_norm'].mean():.4f}")
    print(f"  Pearson moyen global          : {df_all['pearson'].mean():.4f}")
    print(f"  NSE moyen global              : {df_all['nse'].mean():.4f}")
    print(f"  Nb total de mesures analysées : {len(df_all)}")
    print(f"  Nb stations retenues          : {df_all['station'].nunique()}")


if __name__ == "__main__":
    conn = sqlite3.connect('./data/hydro_data.db')
    cursor = conn.cursor()
    cursor.execute('SELECT station_code FROM stations LIMIT 200')
    stations = [row[0] for row in cursor.fetchall()]
    conn.close()

    df_all = []
    exclues = 0
    for i, station in enumerate(stations):
        print(f"\n🔄 [{i+1}/{len(stations)}] Station {station}")
        try:
            df = compare_hydro_insitu_par_date(station)
            if df is not None:
                df_all.append(df)
            else:
                exclues += 1
        except Exception as e:
            print(f"  ⚠️  Erreur: {e}")

    print(f"\n✅ {len(df_all)} stations retenues ({exclues} exclues)")

    if df_all:
        df_all = pd.concat(df_all, ignore_index=True)
        print(f"📊 {len(df_all)} mesures agrégées")
        plot_ecart_par_mois(df_all)
    else:
        print("❌ Aucune donnée valide collectée")