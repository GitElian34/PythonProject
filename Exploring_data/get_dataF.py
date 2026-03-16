import xarray as xr
import pandas as pd
import os
import numpy as np
from xarray.backends.zarr import extract_zarr_variable_encoding



def analyse_point(lat, lon, date):
    """
    Analyse les précipitations et température à un point donné à partir d'un seul fichier

    Args:
        lat, lon: coordonnées du point
        date: date au format 'YYYY-MM-DD'
        fichier: chemin vers le fichier NetCDF contenant t2m et tp
    """

    # === CHARGEMENT ===
    annee, mois, _ = date.split('-')
    fichier = f'/home/sar_hydro/STUDIES/EtudesEB/PythonProject/data/ERA5/usable_data_LAND_France/{annee}/{mois}/data_0.nc'
    #fichier ='/home/sar_hydro/STUDIES/EtudesEB/PythonProject/data/ERA5/usable_data_LAND_France/{annee}/{mois}/data_0.nc'
    # Vérification que le fichier existe
    if os.path.exists(fichier):
        ds = xr.open_dataset(fichier)
        print(f"✅ Fichier chargé: {fichier}")
    else:
        print(f"❌ Fichier non trouvé: {fichier}")
        print(f"Vérifie le chemin: /data/ERA5/usable_data_LAND_France/{annee}/{mois}/")
        raise Exception # ou raise Exception selon le contexte

    # Conversions
    ds['t2m_c'] = ds['t2m'] - 273.15
    ds['tp_mm'] = ds['tp'] * 1000

    # Extraction au point (toutes les dates)
    temp_point = ds['t2m_c'].sel(latitude=lat, longitude=lon, method='nearest')
    precip_point = ds['tp_mm'].sel(latitude=lat, longitude=lon, method='nearest')

    # Périodes
    # Périodes
    date_ts = pd.Timestamp(date)
    debut_jour = date_ts
    fin_jour = date_ts + pd.Timedelta(days=1)  # jour suivant à minuit
    date_debut_10j = date_ts - pd.Timedelta(days=10)
    annee10j, mois10j, _ = str(date_debut_10j).split('-')
    if  mois10j != mois :

        nfichier = f'/home/sar_hydro/STUDIES/EtudesEB/PythonProject/data/ERA5/usable_data_LAND_France/{annee10j}/{mois10j}/data_0.nc'
        if os.path.exists(nfichier):
            dsy = xr.open_dataset(nfichier)
            print(f"✅ Fichier chargé: {nfichier}")
            dsy['t2m_c'] = dsy['t2m'] - 273.15
            dsy['tp_mm'] = dsy['tp'] * 1000
            temp_point = xr.concat([dsy['t2m_c'].sel(latitude=lat, longitude=lon, method='nearest'),temp_point,
                                    ],
                                   dim='valid_time')
            precip_point = xr.concat([dsy['tp_mm'].sel(latitude=lat, longitude=lon, method='nearest'),precip_point,
                                      ],
                                     dim='valid_time')
        else:
            print(f"❌ Fichier non trouvé: {nfichier}")
            print(f"Vérifie le chemin: /data/ERA5/usable_data_LAND_France/{annee10j}/{mois10j}/")
            return None  # ou raise Exception selon le contexte

    # === PRÉCIPITATIONS ===
    precip_jour = precip_point.sel(valid_time=slice(debut_jour, fin_jour)).mean().values

    precip_moy10j = precip_point.sel(valid_time=slice(date_debut_10j, date_ts)).mean().values

    # === TEMPÉRATURE ===
    temp_jour = temp_point.sel(valid_time=slice(debut_jour, fin_jour))
    temp_min_jour = temp_jour.min().values
    temp_max_jour = temp_jour.max().values
    temp_moy_jour = temp_jour.mean().values
    temp_moy10j = temp_point.sel(valid_time=slice(date_debut_10j, date_ts)).mean().values

    # === AFFICHAGE ===
    print(f"\n{'=' * 50}")
    print(f"ANALYSE AU POINT {lat}°N, {lon}°E")
    print(f"Date: {date}")
    print(f"{'=' * 50}")

    print(f"\n🌧️ PRÉCIPITATIONS:")
    print(f"  Jour: {precip_jour:.2f} mm/h")
    print(f"  Moyenne 10j: {precip_moy10j:.2f} mm/h")

    print(f"\n🌡️ TEMPÉRATURE:")
    print(f"  Jour - Min: {temp_min_jour:.2f}°C")
    print(f"  Jour - Max: {temp_max_jour:.2f}°C")
    print(f"  Jour - Moy: {temp_moy_jour:.2f}°C")
    print(f"  Moyenne 10j: {temp_moy10j:.2f}°C")

    ds.close()

    return {
        'precip_jour': precip_jour,
        'precip_moy10j': precip_moy10j,
        'temp_min_jour': temp_min_jour,
        'temp_max_jour': temp_max_jour,
        'temp_moy_jour': temp_moy_jour,
        'temp_moy10j': temp_moy10j
    }


# === EXEMPLE ===
if __name__ == "__main__":
    analyse_point(
        lat=43.5,
        lon=2.5,
        date='2017-01-03'
    )