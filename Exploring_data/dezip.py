import zipfile
import os
import xarray as xr

# Chemin du fichier

def unzip(zip_path,mois, year):
    extract_dir = '/home/sar_hydro/STUDIES/EtudesEB/PythonProject/data/ERA5/usable_data_LAND_France/'+ year+'/'+mois
    # Créer le dossier s'il n'existe pas
    os.makedirs(extract_dir, exist_ok=True)
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


def main():
    """
    Fonction principale pour tester unzip
    """
    # Test avec un mois spécifique
    mois_test = '01'
    fichier_test = f'/home/sar_hydro/STUDIES/EtudesEB/PythonProject/data/ERA5/raw_data/downloadfr_{mois_test}.nc'

    print("=" * 60)
    print("TEST DE LA FONCTION UNZIP")
    print("=" * 60)

    # Vérifier si le fichier existe
    if os.path.exists(fichier_test):
        print(f"Fichier trouvé: {fichier_test}")
        ds = unzip(fichier_test, mois_test)
        if ds:
            print("\n✅ Test réussi!")
        else:
            print("\n❌ Échec du test")
    else:
        print(f"❌ Fichier non trouvé: {fichier_test}")
        print("Vérifie que le téléchargement a bien eu lieu")


if __name__ == "__main__":
    main()