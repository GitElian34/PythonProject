import os
import cdsapi
from Exploring_data.dezip import unzip

os.environ['CDSAPI_URL'] = 'https://cds.climate.copernicus.eu/api'
os.environ['CDSAPI_KEY'] = '68ee5c49-20f5-4bf8-865d-5c0c270376a7'

dossier_destination = '/home/sar_hydro/STUDIES/EtudesEB/PythonProject/data/ERA5/raw_data/Snow/'
os.makedirs(dossier_destination, exist_ok=True)

client = cdsapi.Client()

years = [str(y) for y in range(2016, 2026)]  # 2016 à 2025
mois  = [f"{i:02d}" for i in range(1, 13)]   # 01 à 12

for year in years:
    for m in mois:
        file_path = os.path.join(dossier_destination, f'snow_{year}_{m}.nc')

        # Passe si déjà téléchargé
        if os.path.exists(file_path):
            print(f"  ⏭️  Déjà téléchargé : {year}-{m}")
            continue

        print(f"Téléchargement Snow {year}-{m}...")

        try:
            client.retrieve(
                'reanalysis-era5-land',
                {
                    'product_type': 'reanalysis',
                    'variable': [
                        'snow_depth',
                        'snowmelt',
                    ],
                    'year': year,
                    'month': m,
                    'day': [f"{i:02d}" for i in range(1, 32)],
                    'time': ["23:00"],
                    'area': [52, -6, 40, 10],  # [Nord, Ouest, Sud, Est] — France
                    'format': 'netcdf',
                },
                file_path
            )
            print(f"  ✅ Sauvegardé : {file_path}")
            extract_dir = '/home/sar_hydro/STUDIES/EtudesEB/PythonProject/data/ERA5/usable_data_LAND_France/Snow/' + year + '/' + m
            unzip(file_path, mois=m, year=year,extract_dir=extract_dir)

        except Exception as e:
            print(f"  ❌ Erreur {year}-{m} : {e}")

print("\n✅ Téléchargement Snow terminé !")