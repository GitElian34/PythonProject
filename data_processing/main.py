#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Point d'entrée principal pour l'importation des stations hydrologiques

Usage: python main.py <fichier_station.txt> [base_de_donnees.db]
"""

import sys
import os
import sqlite3

# Import de nos modules
from data_processing.db_manager import create_tables, insert_station, get_stats, get_stations_by_basin_river, \
    get_climate_data_matrix
from data_processing.merge_data import merge_data
from file_parser import db_filling

from tabulate import tabulate


def main():
    # Connexion à la base de données
    conn = sqlite3.connect('./data/hydro_data.db')

    # Créer les tables si elles n'existent pas
    create_tables(conn)

    # Liste des rivières à traiter
    rivieres = ["AIN","ARVE","DOUBS","DURANCE","GARDON","ISERE","OGNON","RHONE","SAONE","SEILLE"]

    # Pour chaque rivière, récupérer les stations et traiter les données
    for riviere in rivieres:
        print(f"\n{'=' * 60}")
        print(f"🔍 Traitement de la rivière: {riviere}")
        print('=' * 60)

        # Récupérer les stations pour cette rivière
        stations_ids = get_stations_by_basin_river(conn, "RHONE", riviere)

        if not stations_ids:
            print(f"⚠️  Aucune station trouvée pour {riviere}")
            continue

        print(f"📊 {len(stations_ids)} stations trouvées pour {riviere}")

        # Traiter chaque station
        for i, station in enumerate(stations_ids, 1):
            print(f"\n  📍 Station {i}/{len(stations_ids)}: {station}")
            merge_data(station)

    # Fermer la connexion
    conn.close()
    print("\n✅ Traitement terminé pour toutes les rivières !")

    # # Récupérer les données
    # data = get_climate_data_matrix('0000000005718')
    #
    # # Définir les en-têtes
    # headers = ['Date', 'T° Min', 'T° Max', 'T° Moy', 'Precip (mm)',
    #            'T° Moy 10j', 'Precip Moy 10j', 'Hauteur (m)']
    #
    # # Afficher avec tabulate
    # print("\n" + "=" * 100)
    # print(f"📊 DONNÉES CLIMATIQUES - Station 0000000005718")
    # print("=" * 100)
    # print(tabulate(data, headers=headers, tablefmt='grid', floatfmt='.2f'))
    # print(f"\n✅ Total: {len(data)} enregistrements")
    # #deduplicate_climate_data()
if __name__ == '__main__':
    main()