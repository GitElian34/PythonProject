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
from data_processing.db_manager import create_tables, insert_station, get_stats
from file_parser import parse_station_file, print_metadata_summary, get_filenames



def main():
    """Fonction principale"""
    files_PATH = './data/Garonne_hw/'
    all_files = get_filenames(files_PATH)
    # Récupération des arguments
    for file in all_files:
        filepath = files_PATH + file
        db_path = "./data/hydro_data.db"

        # Vérification que le fichier existe
        if not os.path.exists(filepath):
            print(f"❌ Fichier non trouvé: {filepath}")
            fichiers = os.listdir('./data/Garonne_hw')
            print(fichiers)
            sys.exit(1)

        print(f"\n📄 [1/5] Parsing du fichier: {os.path.basename(filepath)}")
        metadata, measurements = parse_station_file(filepath)

        print(f"   Métadonnées trouvées: {len(metadata)}")
        print(f"   Mesures trouvées: {len(measurements)}")

        # Afficher un résumé des métadonnées
        print_metadata_summary(metadata)

        if len(measurements) == 0:
            print("❌ Aucune mesure trouvée dans le fichier")

            sys.exit(1)

        print(f"\n💾 [2/5] Connexion à la base: {db_path}")
        conn = sqlite3.connect(db_path)

        print("🏗️  [3/5] Création/Vérification des tables...")
        create_tables(conn)

        print("📥 [4/5] Insertion des données...")
        success = insert_station(conn, metadata, measurements)

        if success:
            # Statistiques finales
            nb_stations, nb_measures = get_stats(conn)
            print(f"\n📊 [5/5] Bilan final:")
            print(f"   Stations dans la base: {nb_stations}")
            print(f"   Mesures totales: {nb_measures}")
        else:
            print("\n❌ Échec de l'insertion")

        conn.close()
        print("\n✨ Terminé!")


if __name__ == '__main__':
    main()