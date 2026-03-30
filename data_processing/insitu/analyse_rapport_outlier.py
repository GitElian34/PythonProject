import sqlite3
import pandas as pd

DB_PATH         = "./data/insitu_data.db"
SEUIL_GAP_JOURS = 30


def migrer_colonne(conn):
    try:
        conn.execute("ALTER TABLE stations_insitu ADD COLUMN gap_max_jours INTEGER")
        print("  ✅ Colonne 'gap_max_jours' ajoutée")
    except Exception as e:
        print(f"  ℹ️  {e}")
    conn.commit()


def calculer_et_flaguer(db_path, seuil_jours=SEUIL_GAP_JOURS):
    conn = sqlite3.connect(db_path)
    migrer_colonne(conn)

    stations = pd.read_sql_query("""
        SELECT code_sta FROM stations_insitu
        WHERE dans_lac IS NULL OR dans_lac NOT IN ('dans_lac', 'proche_lac')
    """, conn)['code_sta'].tolist()

    updates  = []
    traites  = 0

    for code_sta in stations:
        df = pd.read_sql_query("""
            SELECT date FROM mesures_insitu
            WHERE code_sta = ? AND h_09h_wsh IS NOT NULL
            ORDER BY date
        """, conn, params=(code_sta,))

        if len(df) < 2:
            continue

        df['date'] = pd.to_datetime(df['date'])
        gap_max    = int(df['date'].diff().dt.days.max())

        updates.append((gap_max if gap_max >= seuil_jours else None, code_sta))

        traites += 1
        if traites % 200 == 0:
            print(f"  [{traites}/{len(stations)}] en cours...")

    conn.cursor().executemany("""
        UPDATE stations_insitu SET gap_max_jours = ? WHERE code_sta = ?
    """, updates)
    conn.commit()

    # Résumé
    df_res = pd.read_sql_query("""
        SELECT gap_max_jours, COUNT(*) as n
        FROM stations_insitu
        WHERE gap_max_jours IS NOT NULL
        GROUP BY
            CASE
                WHEN gap_max_jours < 90  THEN '30-90j'
                WHEN gap_max_jours < 180 THEN '90-180j'
                WHEN gap_max_jours < 365 THEN '180-365j'
                ELSE '> 1 an'
            END
    """, conn)

    print(f"\n  ✅ {traites} stations traitées")
    print(f"  Flagguées (gap > {seuil_jours}j) : "
          f"{sum(1 for u in updates if u[0] is not None)}")

    conn.close()


if __name__ == "__main__":
    print(f"🔍 Calcul et flags gaps > {SEUIL_GAP_JOURS} jours...")
    calculer_et_flaguer(DB_PATH)