import re
import sqlite3

RAPPORT_PATH = "./data/insitu/visualisation/rapport_outliers.txt"
DB_PATH      = "./data/insitu_data.db"


def lire_rapport(path):
    with open(path, "r", encoding="utf-8") as f:
        contenu = f.read()

    blocs = re.findall(
        r'▶ (\w+)\s+—\s+(.+?)(?=\n▶|\Z)',
        contenu, re.DOTALL
    )

    stations = []
    for code_sta, details in blocs:
        match_sauts = re.search(r'sauts brutaux \(n=(\d+)\)', details)
        n_sauts     = int(match_sauts.group(1)) if match_sauts else 0

        plat = 'plat global' in details or 'plat fenêtre' in details

        if n_sauts == 0:
            qualite = 'aucun'
        elif n_sauts < 10:
            qualite = '< 10'
        elif n_sauts < 100:
            qualite = '10-100'
        elif n_sauts < 500:
            qualite = '100-500'
        else:
            qualite = '> 500'

        stations.append({
            'code_sta':      code_sta,
            'qualite_sauts': qualite,
            'signal_plat':   1 if plat else 0,
        })

    return stations


def migrer_colonnes(conn):
    cursor = conn.cursor()
    for col, typ in [('qualite_sauts', 'TEXT'), ('signal_plat', 'INTEGER')]:
        try:
            cursor.execute(f"ALTER TABLE stations_insitu ADD COLUMN {col} {typ}")
            print(f"  ✅ Colonne '{col}' ajoutée")
        except Exception as e:
            print(f"  ℹ️  {col} : {e}")
    conn.commit()


def mettre_a_jour(stations, conn):
    cursor = conn.cursor()

    # Stations suspectes — valeurs depuis le rapport
    cursor.executemany("""
        UPDATE stations_insitu
        SET qualite_sauts = ?, signal_plat = ?
        WHERE code_sta = ?
    """, [(s['qualite_sauts'], s['signal_plat'], s['code_sta']) for s in stations])

    # Stations non suspectes — aucun problème détecté
    codes_suspects = tuple(s['code_sta'] for s in stations)
    cursor.execute(f"""
        UPDATE stations_insitu
        SET qualite_sauts = 'aucun', signal_plat = 0
        WHERE code_sta NOT IN ({','.join('?' * len(codes_suspects))})
          AND qualite_sauts IS NULL
    """, codes_suspects)

    conn.commit()
    print(f"  ✅ {len(stations)} stations suspectes mises à jour")


if __name__ == "__main__":
    stations = lire_rapport(RAPPORT_PATH)
    print(f"  {len(stations)} stations parsées depuis le rapport")

    conn = sqlite3.connect(DB_PATH)
    migrer_colonnes(conn)
    mettre_a_jour(stations, conn)
    conn.close()

    # Vérification rapide
    conn = sqlite3.connect(DB_PATH)
    df = __import__('pandas').read_sql_query("""
        SELECT qualite_sauts, signal_plat, COUNT(*) as n
        FROM stations_insitu
        GROUP BY qualite_sauts, signal_plat
        ORDER BY qualite_sauts
    """, conn)
    conn.close()
    print(f"\n── Répartition ──────────────────────────")
    print(df.to_string(index=False))