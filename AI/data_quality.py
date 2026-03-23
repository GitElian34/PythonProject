import pandas as pd
import numpy as np

FEATURES_WSH = ['h_01h_wsh', 'h_09h_wsh', 'h_17h_wsh']

CRITERES = {
    'min_lignes': 500,
    'min_std': 0.05,
    'max_ratio_zeros': 0.30,
    'date_min': pd.Timestamp('2020-01-01'),
    'max_trous': 20,
    'trou_jours': 30,
    'max_ratio_outliers': 0.01,
    'z_score_seuil': 5.0
}


def station_est_valide(df, code_sta=None):
    """
    Vérifie si une station est valide pour l'entraînement.
    Retourne (bool, list[str]) — valide ou non + liste des raisons de rejet.
    """
    rejets = []
    label = f"[{code_sta}]" if code_sta else ""

    # 1. Nombre de lignes suffisant
    if len(df) < CRITERES['min_lignes']:
        rejets.append(f"Trop peu de données : {len(df)} lignes < {CRITERES['min_lignes']}")

    # 2. Variance suffisante
    std = df['h_09h_wsh'].std()
    if std < CRITERES['min_std']:
        rejets.append(f"Variance trop faible : std={std:.4f} < {CRITERES['min_std']}")

    # 3. Trop de zéros
    ratio_zeros = (df['h_09h_wsh'] == 0).mean()
    if ratio_zeros > CRITERES['max_ratio_zeros']:
        rejets.append(f"Trop de zéros : {ratio_zeros:.1%} > {CRITERES['max_ratio_zeros']:.0%}")

    # 4. Données trop récentes
    date_min = df['date'].min()
    if date_min > CRITERES['date_min']:
        rejets.append(f"Données trop récentes : début={date_min.date()} > {CRITERES['date_min'].date()}")

    # 5. Trop de trous temporels
    gaps = df['date'].diff().dt.days
    nb_trous = (gaps > CRITERES['trou_jours']).sum()
    if nb_trous > CRITERES['max_trous']:
        rejets.append(f"Trop de trous : {nb_trous} trous > {CRITERES['trou_jours']}j")

    # 6. Valeurs aberrantes extrêmes
    mean = df['h_09h_wsh'].mean()
    std_val = df['h_09h_wsh'].std()
    if std_val > 0:
        z_scores = (df['h_09h_wsh'] - mean) / std_val
        ratio_outliers = (z_scores.abs() > CRITERES['z_score_seuil']).mean()
        if ratio_outliers > CRITERES['max_ratio_outliers']:
            rejets.append(f"Trop d'outliers extrêmes : {ratio_outliers:.2%} > {CRITERES['max_ratio_outliers']:.0%}")

    valide = len(rejets) == 0

    if valide:
        print(f"  ✅ {label} Station valide")
    else:
        print(f"  ❌ {label} Station rejetée :")
        for r in rejets:
            print(f"     → {r}")

    return valide, rejets


def filtrer_stations(stations_df, code_col='code_sta'):
    """
    Filtre une liste de stations et retourne les valides.

    Args:
        stations_df: dict {code_sta: df} ou liste de (code_sta, df)
        code_col: nom de la colonne code station si dict

    Returns:
        valides: list de code_sta valides
        rejetes: dict {code_sta: list[str]} des raisons de rejet
    """
    valides = []
    rejetes = {}

    items = stations_df.items() if isinstance(stations_df, dict) else stations_df

    for code_sta, df in items:
        valide, rejets = station_est_valide(df, code_sta)
        if valide:
            valides.append(code_sta)
        else:
            rejetes[code_sta] = rejets

    print(f"\n📊 Résumé filtrage :")
    print(f"  ✅ {len(valides)} stations valides")
    print(f"  ❌ {len(rejetes)} stations rejetées")

    return valides, rejetes


if __name__ == "__main__":
    # Test rapide sur quelques stations
    import sqlite3
    from data_processing.insitu.db_insitu import get_donnees_station, get_stations_insitu

    DB_PATH = "./data/insitu_data.db"
    conn = sqlite3.connect(DB_PATH)
    stations = [row[0] for row in get_stations_insitu(conn)]
    conn.close()

    print(f"🔍 Vérification de {len(stations)} stations...\n")

    dfs = {}
    for code_sta in stations:
        df = get_donnees_station(code_sta)
        if df is not None:
            dfs[code_sta] = df

    valides, rejetes = filtrer_stations(dfs)

    print(f"\n✅ Stations valides : {valides}")
    print(f"\n❌ Détail des rejets :")
    for code, raisons in rejetes.items():
        print(f"  {code} : {raisons}")