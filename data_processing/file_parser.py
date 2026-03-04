#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Parseur pour les fichiers de station HydroWeb
"""

import re
import os
import sqlite3
import sys
from tokenize import String

from data_processing.db_manager import create_tables, insert_station, get_stats

def get_filenames(dossier):
    """
    Récupère tous les noms de fichiers d'un dossier

    Args:
        dossier (str): Chemin vers le dossier

    Returns:
        list: Liste des noms de fichiers
    """
    try:
        # Liste tous les éléments du dossier
        tous_elems = os.listdir(dossier)

        # Ne garder que les fichiers (pas les sous-dossiers)
        fichiers = [f for f in tous_elems if os.path.isfile(os.path.join(dossier, f))]

        return fichiers

    except FileNotFoundError:
        print(f"❌ Dossier non trouvé: {dossier}")
        return []
    except PermissionError:
        print(f"❌ Pas les droits d'accès au dossier: {dossier}")
        return []




def parse_station_file(filepath):
    """
    Parse un fichier de station HydroWeb
    """
    metadata = {}
    measurements = []

    with open(filepath, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    i = 0
    while i < len(lines):
        line = lines[i].strip()

        # Ignorer les lignes de séparation
        if line.startswith('#' * 50) or not line:
            i += 1
            continue

        # Traitement des métadonnées
        if line.startswith('#'):
            if '::' in line:
                parts = line[1:].split('::', 1)
                if len(parts) == 2:
                    key = parts[0].strip()
                    value = parts[1].strip()
                    metadata[key] = None if value == 'NA' else value
            i += 1

        # Traitement des données
        elif line and not line.startswith('#'):
            # Méthode 1: Remplacer ':' par espace puis split
            cleaned_line = line.replace(':', ' ')  # ← SOLUTION SIMPLE !
            parts = cleaned_line.split()

            # OU Méthode 2: Split plus sophistiqué
            # parts = re.split(r'\s+', line.replace(':', ' '))

            print(f"Nombre de parties: {len(parts)}")
            print(f"Parts: {parts}")

            if len(parts) >= 16:  # Maintenant on a besoin d'au moins 16 éléments
                try:
                    # L'élément à l'index 4 est vide (à cause du ':'), on l'ignore
                    # Donc on décale tous les index suivants de +1

                    dist_val = parts[9] if len(parts) > 9 else '9999.999'  # Au lieu de parts[8]
                    is_valid = dist_val != '9999.999'

                    measurement = {
                        'date': parts[0],
                        'time': f"{parts[1]}:{parts[2]}",  # Recomposer l'heure
                        'height': float(parts[3]) if parts[3] != 'NA' else None,  # Maintenant à 3
                        'uncertainty': float(parts[4]) if parts[4] != 'NA' else None,  # Maintenant à 4
                        'longitude': float(parts[5]) if parts[5] != 'NA' else None,  # 5 au lieu de 4
                        'latitude': float(parts[6]) if parts[6] != 'NA' else None,  # 6 au lieu de 5
                        'ellipsoidal_height': float(parts[7]) if parts[7] != 'NA' else None,  # 7 au lieu de 6
                        'geoidal_ondulation': float(parts[8]) if parts[8] != 'NA' else None,  # 8 au lieu de 7
                        'distance_to_ref': float(dist_val) if dist_val != '9999.999' else None,  # dist_val = parts[9]
                        'satellite': parts[10] if len(parts) > 10 else None,  # 10 au lieu de 9
                        'orbit_mission': parts[11] if len(parts) > 11 else None,  # 11 au lieu de 10
                        'track_number': int(parts[12]) if len(parts) > 12 and parts[12] != 'NA' else None,
                        # 12 au lieu de 11
                        'cycle_number': int(parts[13]) if len(parts) > 13 and parts[13] != 'NA' else None,
                        # 13 au lieu de 12
                        'retracking_algo': parts[14] if len(parts) > 14 else None,  # 14 au lieu de 13
                        'gdr_version': parts[15] if len(parts) > 15 and parts[15] != 'NA' else None,  # 15 au lieu de 14
                        'is_valid': is_valid
                    }
                    measurements.append(measurement)

                except (ValueError, IndexError) as e:
                    print(f"⚠️  Ligne ignorée: {line[:80]}...")
                    print(f"   Erreur: {e}")
            else:
                print(f"⚠️  Pas assez d'éléments ({len(parts)}): {line[:80]}...")

            i += 1
        else:
            i += 1

    return metadata, measurements


def print_metadata_summary(metadata):
    """Affiche un résumé des métadonnées principales"""
    print("\n🔑 Principales métadonnées:")
    for key in ['ID', 'BASIN', 'RIVER', 'REFERENCE LATITUDE', 'REFERENCE LONGITUDE',
                'MEAN ALTITUDE(M.mm)', 'NUMBER OF MEASUREMENTS IN DATASET']:
        if key in metadata:
            print(f"   {key}: {metadata[key]}")

def db_filling(files_PATH : str):
    """Fonction principale"""
    #files_PATH = './data/Garonne_hw/'
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