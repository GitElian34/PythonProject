#!/usr/bin/env python3
import sqlite3

DB_PATH = "./data/hydro_data.db"

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

cursor.execute("SELECT COUNT(*) FROM stations")
nb_stations = cursor.fetchone()[0]

cursor.execute("SELECT COUNT(*) FROM measurements")
nb_total = cursor.fetchone()[0]

moyenne = nb_total / nb_stations if nb_stations > 0 else 0

conn.close()

# Tableau
col = 28
sep = "+" + "-"*col + "+" + "-"*col + "+"
print(sep)
print(f"| {'Stations':^{col-2}} | {'Total mesures':^{col-2}} |")
print(sep)
print(f"| {nb_stations:^{col-2}} | {nb_total:^{col-2}} |")
print(sep)
print(f"| {'1 station':^{col-2}} | {'Moyenne mesures/station':^{col-2}} |")
print(sep)
print(f"| {'1':^{col-2}} | {moyenne:^{col-2}.1f} |")
print(sep)