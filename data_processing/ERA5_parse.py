import os
import cdsapi
from Exploring_data.dezip import unzip
# Définir les variables d'environnement
os.environ['CDSAPI_URL'] = 'https://cds.climate.copernicus.eu/api'
os.environ['CDSAPI_KEY'] = '68ee5c49-20f5-4bf8-865d-5c0c270376a7'

# Utiliser normalement
# Dossier de destination
dossier_destination = '/home/sar_hydro/STUDIES/EtudesEB/PythonProject/data/ERA5/raw_data/'

# Créer le dossier s'il n'existe pas
os.makedirs(dossier_destination, exist_ok=True)
client = cdsapi.Client()
years = [ '2015']
mois = [f"{i:02d}" for i in range(12, 13)]  # ['01', '02', ..., '12']

for year in years:
    for m in mois:
        print(f"Téléchargement du mois {m} de l'année {year}")

        # Chemin complet du fichier
        file_path = os.path.join(dossier_destination, f'downloadfr_{year}_{m}.nc')

        client.retrieve(
            'reanalysis-era5-land',
            {
                'product_type': 'reanalysis',
                'variable': [
                    '2m_temperature', 'total_precipitation',
                ],
                'year': year,
                'month': m,
                'day': [f"{i:02d}" for i in range(1, 32)],
                'time': [f"{h:02d}:00" for h in range(24)],
                'area': [52, -6, 40, 10],  # [Nord, Ouest, Sud, Est]
                'format': 'netcdf',
            },
            file_path)

        # Appel de la fonction unzip
        unzip(file_path, mois=m, year=year)