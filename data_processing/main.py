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
from file_parser import db_filling



def main():
    db_filling('./data/Seine_hw/')


if __name__ == '__main__':
    main()