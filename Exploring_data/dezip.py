import zipfile
import os
import xarray as xr

# Chemin du fichier
zip_path = '/home/sar_hydro/STUDIES/EtudesEB/PythonProject/downloadfr.nc'
extract_dir = '/home/sar_hydro/STUDIES/EtudesEB/PythonProject/data/ERA5'

# Vérifier si c'est un zip
with zipfile.ZipFile(zip_path, 'r') as zip_ref:
    print("Contenu du zip:", zip_ref.namelist())
    # Extraire tous les fichiers
    zip_ref.extractall(extract_dir)
    # Le fichier extrait aura probablement un nom comme 'data.nc' ou 'download.nc'
    extracted_files = zip_ref.namelist()

# Trouver le fichier .nc extrait
for f in extracted_files:
    if f.endswith('.nc'):
        nc_file = os.path.join(extract_dir, f)
        print(f"Fichier NetCDF trouvé: {nc_file}")
        # Lire avec xarray
        ds = xr.open_dataset(nc_file)
        print(ds)
        break