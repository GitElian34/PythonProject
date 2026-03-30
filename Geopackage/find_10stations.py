import sqlite3
import geopandas as gpd
import numpy as np
from shapely.geometry import Point

from Geopackage.visualisation import distance_euclidienne_m
from Geopackage.visualisation import station_la_plus_proche
from data_processing.db_manager import get_station_coordinates
import pandas as pd

DB_PATH = './data/hydro_data.db'

# Stations hydro fixées
stations_hydro = [
    '0000000010852',
    '0000000010843',
    '0000000008762',
    '0000000010838',
    '0000000005744',
    '0000000006358',
    '112558',
    '0000000006361',
    '0000000008748',
    '0000000202497',
]

if __name__ == "__main__":
    print(f"📍 {len(stations_hydro)} stations hydro fixes")

    paires = []
    for i, station_hydro in enumerate(stations_hydro):
        print(f"\n🔄 [{i+1}/{len(stations_hydro)}] Station hydro : {station_hydro}")
        try:
            conn = sqlite3.connect(DB_PATH)
            result = get_station_coordinates(conn, station_hydro)
            conn.close()

            if result is None:
                print(f"  ❌ Coordonnées non trouvées")
                continue

            lon, lat, river_name = result
            station_insitu, distance_curv, lon_insitu, lat_insitu = station_la_plus_proche(lon, lat)

            if station_insitu is None:
                print(f"  ❌ Aucune station insitu trouvée")
                continue

            # Distance euclidienne pour comparaison
            point_h = gpd.GeoSeries([Point(lon, lat)], crs='EPSG:4326').to_crs('EPSG:2154')[0]
            point_i = gpd.GeoSeries([Point(lon_insitu, lat_insitu)], crs='EPSG:4326').to_crs('EPSG:2154')[0]
            distance_eucl = distance_euclidienne_m(lon, lat, lon_insitu, lat_insitu)

            if distance_curv > 8000:
                print(f"  ❌ Trop loin : {distance_curv:.0f} m (curviligne)")
                continue

            paire = {
                'station_hydro': station_hydro,
                'lon_hydro': lon,
                'lat_hydro': lat,
                'river_name': river_name,
                'station_insitu': station_insitu,
                'lon_insitu': lon_insitu,
                'lat_insitu': lat_insitu,
                'distance_curv_m': distance_curv,
                'distance_eucl_m': distance_eucl,
            }
            paires.append(paire)
            print(f"  ✅ Paire : {station_hydro} ↔ {station_insitu}|{distance_curv:.0f} | {distance_eucl:.0f} m | {river_name}")

        except Exception as e:
            print(f"  ⚠️  Erreur : {e}")

    print(f"\n{'='*60}")
    print(f"📊 {len(paires)}/{len(stations_hydro)} paires trouvées")
    print(f"{'='*60}")
    for p in paires:
        print(f"  {p['station_hydro']} ↔ {p['station_insitu']} | curv={p['distance_curv_m']:.0f} m | eucl={p['distance_eucl_m']:.0f} m | {p['river_name']}")

    df_paires = pd.DataFrame(paires)
    df_paires.to_csv('./data/paires_hydro_insitu.csv', index=False)
    print(f"\n💾 Sauvegardé : ./data/paires_hydro_insitu.csv")