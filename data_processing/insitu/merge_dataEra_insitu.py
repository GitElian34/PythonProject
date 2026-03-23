import sqlite3
import pandas as pd

from data_processing.insitu.db_insitu import get_donnees_station

if __name__ == "__main__":
    df = get_donnees_station('A343021001')
    print(df.info())