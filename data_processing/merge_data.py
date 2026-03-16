import os
import sqlite3
import sys
from datetime import timedelta, datetime
from tokenize import String

from Exploring_data.get_dataF import analyse_point
from data_processing.db_manager import create_tables, insert_station, get_stats, insert_climate_data


def merge_data(id_station ):
    conn = sqlite3.connect('./data/hydro_data.db')
    cursor = conn.cursor()
    query_station = '''
                    SELECT reference_longitude, reference_latitude
                    FROM stations
                    WHERE station_code = ? \
                    '''
    cursor.execute(query_station, (id_station,))
    station_coords = cursor.fetchone()

    if not station_coords:
        print(f"❌ Station {id_station} non trouvée")
        conn.close()
        return None, None, []

    longitude, latitude = station_coords

    query_dates = '''
                  SELECT DISTINCT measure_date
                  FROM measurements
                  WHERE station_code = ?
                  ORDER BY measure_date \
                  '''
    cursor.execute(query_dates, (id_station,))
    dates = [row[0] for row in cursor.fetchall()]
    dates_filtrees = [d for d in dates if '2016-01-11' < d < '2026-01-01']
    print(dates_filtrees)
    # Récupérer tous les résultats
    measurements = cursor.fetchall()
    for d in dates_filtrees :
        date_obj = datetime.strptime(d, '%Y-%m-%d')
        date_moins_10 = date_obj - timedelta(days=10)
        date_moins_10_str = date_moins_10.strftime('%Y-%m-%d')
        metadata = analyse_point(latitude, longitude, d)
        insert_climate_data(conn, id_station, d, metadata, date_moins_10_str)


