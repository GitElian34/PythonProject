#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Parseur pour les fichiers de station HydroWeb
"""

import re
import os

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