import sqlite3
from db_insitu import ajouter_colonne_dist_barrage, mettre_a_jour_distances_barrages

conn = sqlite3.connect("./data/insitu_data.db")
ajouter_colonne_dist_barrage(conn)
mettre_a_jour_distances_barrages(conn)
conn.close()