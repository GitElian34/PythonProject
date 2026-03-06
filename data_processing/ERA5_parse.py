import os
import cdsapi

# Définir les variables d'environnement
os.environ['CDSAPI_URL'] = 'https://cds.climate.copernicus.eu/api'
os.environ['CDSAPI_KEY'] = '68ee5c49-20f5-4bf8-865d-5c0c270376a7'

# Utiliser normalement

client = cdsapi.Client()

client.retrieve(
    'reanalysis-era5-single-levels',
    {
        'product_type': 'reanalysis',
        'variable': [
            '2m_temperature', 'total_precipitation',
        ],
        'year': '2020',  # À modifier
        'month': '07',    # À modifier
        'day': [f"{i:02d}" for i in range(1, 32)],
        'time': [
            '00:00', '01:00', '02:00',
            # ... pour les données horaires, il faut lister toutes les heures
            '03:00', '04:00', '05:00',
            '06:00', '07:00', '08:00',
            '09:00', '10:00', '11:00',
            '12:00', '13:00', '14:00',
            '15:00', '16:00', '17:00',
            '18:00', '19:00', '20:00',
            '21:00', '22:00', '23:00',
        ],
        # Définis une zone géographique [Nord, Ouest, Sud, Est]
        'area': [52, -6, 40, 10],  # Exemple pour une partie de l'Europe de l'Ouest
        'format': 'netcdf',       # Ou 'grib'
    },
    'downloadfr.nc') # Nom du  fichier de sortie