#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
═══════════════════════════════════════════════════════════════════════════
hydroweb_parser.py — Parsing des fichiers .txt HydroWeb
═══════════════════════════════════════════════════════════════════════════

Lit un fichier .txt au format HydroWeb (stations virtuelles altimétriques)
et retourne les métadonnées + mesures sous forme de dicts Python.

Aucun contact avec la BDD — c'est un pur parseur de fichiers.

Format attendu :
    #KEY:: VALUE              (métadonnées, header)
    ################          (séparateur)
    YYYY-MM-DD HH:MM  height uncertainty : lon lat ellh geoid dist sat orbit track cycle retrak gdr

Usage standalone (test) :
    python hydroweb_parser.py ./data/stations_hw/mon_fichier.txt
    python hydroweb_parser.py ./data/stations_hw/    (tout le dossier)
═══════════════════════════════════════════════════════════════════════════
"""

import argparse
import logging
import sys
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("parser")


# ═══════════════════════════════════════════════════════════════════════
# PARSING D'UN FICHIER
# ═══════════════════════════════════════════════════════════════════════

def parse_hydroweb_file(filepath: Path) -> tuple[dict, list[dict]]:
    """
    Parse un fichier .txt au format HydroWeb.

    Args:
        filepath: Chemin vers le fichier .txt

    Returns:
        (metadata_dict, list_of_measurement_dicts)
        metadata_dict : clés = champs HydroWeb ("ID", "BASIN", "RIVER", ...)
        measurement_dict : clés = "date", "time", "height", "uncertainty", ...
    """
    metadata = {}
    measurements = []

    with open(filepath, "r", encoding="utf-8") as f:
        lines = f.readlines()

    in_header = True

    for line_raw in lines:
        line = line_raw.strip()

        if not line:
            continue

        # Ligne de séparation #### → fin du header
        if line.startswith("####"):
            in_header = False
            continue

        # Header : #KEY:: VALUE
        if in_header and line.startswith("#"):
            if "::" in line:
                key, _, value = line[1:].partition("::")
                key = key.strip()
                value = value.strip()
                metadata[key] = None if value in ("NA", "") else value
            continue

        # Données : lignes de mesures
        if not in_header and not line.startswith("#"):
            measurement = _parse_measurement_line(line)
            if measurement is not None:
                measurements.append(measurement)

    return metadata, measurements


def parse_hydroweb_directory(dossier: Path) -> list[tuple[Path, dict, list[dict]]]:
    """
    Parse tous les fichiers .txt d'un dossier.

    Args:
        dossier: Chemin vers le dossier

    Returns:
        Liste de tuples (filepath, metadata, measurements)
    """
    fichiers = sorted(
        [f for f in dossier.iterdir()
         if f.is_file() and not f.name.startswith(".")]
    )
    if not fichiers:
        log.warning(f"Aucun .txt trouvé dans {dossier}")
        return []

    log.info(f"{len(fichiers)} fichiers .txt trouvés dans {dossier}")

    results = []
    for filepath in fichiers:
        try:
            metadata, measurements = parse_hydroweb_file(filepath)
            results.append((filepath, metadata, measurements))
            station_id = metadata.get("ID", "?")
            log.info(f"  {filepath.name} → {station_id} ({len(measurements)} mesures)")
        except Exception as e:
            log.error(f"  Erreur {filepath.name}: {e}")

    return results


# ═══════════════════════════════════════════════════════════════════════
# PARSING D'UNE LIGNE DE MESURE
# ═══════════════════════════════════════════════════════════════════════

def _parse_measurement_line(line: str) -> dict | None:
    """
    Parse une ligne de données HydroWeb.

    Exemple :
        2016-08-03 10:14   282.06     0.01 :   5.5181  46.2646   332.20    50.14     0.15 S3A REP 0244 007 OCOG 2

    Le ':' est le field separator entre col4 (uncertainty) et col5 (lon).
    On le remplace par un espace avant de split.
    """
    # Remplacer le ':' séparateur par un espace (le ':' de HH:MM aussi,
    # mais on recompose l'heure après)
    parts = line.replace(":", " ").split()

    # Ligne complète : 16 éléments après split
    # (date, HH, MM, height, uncert, lon, lat, ellh, geoid, dist, sat, orbit, track, cycle, retrak, gdr)
    if len(parts) >= 16:
        return _parse_full_line(parts)

    # Ligne partielle : au moins date + heure + hauteur (4 éléments = date, HH, MM, height)
    if len(parts) >= 4:
        return _parse_minimal_line(parts)

    return None


def _parse_full_line(parts: list[str]) -> dict | None:
    """Parse une ligne complète (16+ éléments)."""
    try:
        dist_raw = parts[9]
        is_valid = dist_raw != "9999.999"

        return {
            "date":                 parts[0],
            "time":                 f"{parts[1]}:{parts[2]}",
            "height":              _to_float(parts[3]),
            "uncertainty":         _to_float(parts[4]),
            "longitude":           _to_float(parts[5]),
            "latitude":            _to_float(parts[6]),
            "ellipsoidal_height":  _to_float(parts[7]),
            "geoidal_ondulation":  _to_float(parts[8]),
            "distance_to_ref":     _to_float(dist_raw) if is_valid else None,
            "satellite":           parts[10] if len(parts) > 10 else None,
            "orbit_mission":       parts[11] if len(parts) > 11 else None,
            "track_number":        _to_int(parts[12]) if len(parts) > 12 else None,
            "cycle_number":        _to_int(parts[13]) if len(parts) > 13 else None,
            "retracking_algo":     parts[14] if len(parts) > 14 else None,
            "gdr_version":         parts[15] if len(parts) > 15 else None,
            "is_valid":            is_valid,
        }
    except (ValueError, IndexError) as e:
        log.warning(f"Ligne ignorée ({e})")
        return None


def _parse_minimal_line(parts: list[str]) -> dict | None:
    """
    Parse une ligne incomplète (tronquée en fin de fichier).
    On garde date + heure + hauteur, le reste est None.
    Flaggée is_valid = False.
    """
    try:
        return {
            "date":                 parts[0],
            "time":                 f"{parts[1]}:{parts[2]}" if len(parts) > 2 else None,
            "height":              _to_float(parts[3]) if len(parts) > 3 else None,
            "uncertainty":         _to_float(parts[4]) if len(parts) > 4 else None,
            "longitude":           None,
            "latitude":            None,
            "ellipsoidal_height":  None,
            "geoidal_ondulation":  None,
            "distance_to_ref":     None,
            "satellite":           None,
            "orbit_mission":       None,
            "track_number":        None,
            "cycle_number":        None,
            "retracking_algo":     None,
            "gdr_version":         None,
            "is_valid":            False,
        }
    except (ValueError, IndexError):
        return None


# ═══════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════

def _to_float(val: str) -> float | None:
    if val in ("NA", "9999.999", ""):
        return None
    try:
        return float(val)
    except ValueError:
        return None


def _to_int(val: str) -> int | None:
    if val in ("NA", ""):
        return None
    try:
        return int(val)
    except ValueError:
        return None


# ═══════════════════════════════════════════════════════════════════════
# CLI (test standalone)
# ═══════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test du parser HydroWeb")
    parser.add_argument("path", type=str, help="Fichier .txt ou dossier")
    args = parser.parse_args()

    p = Path(args.path)

    if p.is_file():
        metadata, measurements = parse_hydroweb_file(p)
        print(f"\nMétadonnées ({len(metadata)} champs) :")
        for k in ["ID", "BASIN", "RIVER", "REFERENCE LATITUDE",
                   "REFERENCE LONGITUDE", "MEAN ALTITUDE(M.mm)",
                   "NUMBER OF MEASUREMENTS IN DATASET"]:
            print(f"  {k}: {metadata.get(k, '?')}")
        print(f"\nMesures : {len(measurements)}")
        if measurements:
            m = measurements[0]
            print(f"  Première : {m['date']} {m['time']} h={m['height']}m ±{m['uncertainty']}m")
            m = measurements[-1]
            print(f"  Dernière : {m['date']} {m['time']} h={m['height']}m ±{m['uncertainty']}m")

    elif p.is_dir():
        results = parse_hydroweb_directory(p)
        print(f"\n{len(results)} fichiers parsés")
        total_meas = sum(len(m) for _, _, m in results)
        print(f"Total mesures : {total_meas}")

    else:
        print(f"Chemin introuvable : {p}")
        sys.exit(1)