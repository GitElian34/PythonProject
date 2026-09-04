import os
import calendar
import cdsapi
import xarray as xr
import pandas as pd
from Exploring_data.dezip import unzip

os.environ['CDSAPI_URL'] = 'https://cds.climate.copernicus.eu/api'
os.environ['CDSAPI_KEY'] = '68ee5c49-20f5-4bf8-865d-5c0c270376a7'

dossier_destination = '/home/sar_hydro/STUDIES/EtudesEB/PythonProject/data/ERA5/raw_data/'
os.makedirs(dossier_destination, exist_ok=True)

dossier_usable = '/home/sar_hydro/STUDIES/EtudesEB/PythonProject/data/ERA5/usable_data_LAND_Allemagne/'

BBOX_GERMANY = [55.5, 5.5, 47.0, 15.5]  # [Nord, Ouest, Sud, Est]
BBOX_FRANCE = [52, -6, 40, 10]

client = cdsapi.Client()

years = [str(y) for y in range(2016, 2026)]  # 2016 a 2025
mois  = [f"{i:02d}" for i in range(1, 13)]   # 01 a 12

BBOX = BBOX_GERMANY  # zone active pour ce run

KEEP_RAW_PIECES = False  # supprimer les fichiers intermediaires apres fusion


import zipfile
import glob


def resolve_netcdf(path: str, extract_dir: str) -> str:
    """
    CDS renvoie parfois un vrai .nc, parfois un zip contenant un .nc
    (observe : 'data_0.nc' a l'interieur), meme quand on demande
    format='netcdf'. Cette fonction retourne toujours le chemin du
    vrai fichier NetCDF a ouvrir avec xarray.
    """
    if zipfile.is_zipfile(path):
        os.makedirs(extract_dir, exist_ok=True)
        with zipfile.ZipFile(path, 'r') as zf:
            zf.extractall(extract_dir)
        nc_candidates = sorted(glob.glob(os.path.join(extract_dir, "*.nc")))
        if not nc_candidates:
            raise FileNotFoundError(f"Aucun .nc trouve dans le zip extrait : {extract_dir}")
        return nc_candidates[0]
    return path


def normalize_time_dim(ds: xr.Dataset) -> xr.Dataset:
    """CDS nomme parfois la dimension temporelle 'valid_time' au lieu de 'time'."""
    if "valid_time" in ds.dims and "time" not in ds.dims:
        ds = ds.rename({"valid_time": "time"})
    return ds


def next_month(year: str, m: str):
    y, mo = int(year), int(m)
    if mo == 12:
        return str(y + 1), "01"
    return str(y), f"{mo + 1:02d}"


def fetch_hourly_00(year: str, m: str, days: list[str], out_path: str) -> str:
    """
    Recupere le pas de temps 00:00 UTC pour les jours demandes d'un mois donne.
    Chaque valeur a 00:00 sur le jour D represente le cumul (precip/PET) du
    jour D-1 (0h a 24h) : c'est le vrai total journalier du jour precedent.
    """
    client.retrieve(
        'reanalysis-era5-land',
        {
            'product_type': 'reanalysis',
            'variable': ['total_precipitation', 'potential_evaporation'],
            'year': year,
            'month': m,
            'day': days,
            'time': ['00:00'],
            'area': BBOX,
            'format': 'netcdf',
        },
        out_path
    )
    extract_dir = os.path.join(dossier_usable, year, m)
    unzip(out_path, mois=m, year=year, extract_dir=extract_dir)  # garde le print diagnostic existant
    return resolve_netcdf(out_path, extract_dir)


def download_precip_pet_daily(year: str, m: str):
    """
    Precipitation + PET : on ne telecharge qu'un seul pas de temps par jour
    (00:00 UTC), qui contient deja le cumul exact des 24h precedentes.
    Pas besoin de sommer nous-memes : juste decaler l'etiquette de date
    d'un jour en arriere pour que la valeur soit attribuee au bon jour.
    """
    daily_path = os.path.join(dossier_destination, f'precip_pet_{year}_{m}.nc')
    if os.path.exists(daily_path):
        print(f"  Deja telecharge : precip_pet {year}-{m}")
        return daily_path

    n_days = calendar.monthrange(int(year), int(m))[1]
    ny, nm = next_month(year, m)

    print(f"Telechargement precip_pet {year}-{m} (1 pas de temps/jour)...")
    try:
        # Jours 2 -> dernier jour du mois M, a 00:00 -> donne les totaux
        # des jours 1 -> avant-dernier du mois M.
        main_days = [f"{d:02d}" for d in range(2, n_days + 1)]
        main_zip_path = os.path.join(dossier_destination, f'_tmp_main_{year}_{m}.nc')
        main_nc_path = fetch_hourly_00(year, m, main_days, main_zip_path)

        # Jour 1 du mois suivant, a 00:00 -> donne le total du dernier
        # jour du mois M.
        tail_zip_path = os.path.join(dossier_destination, f'_tmp_tail_{year}_{m}.nc')
        tail_nc_path = fetch_hourly_00(ny, nm, ['01'], tail_zip_path)

        # Fusion + decalage des dates d'un jour en arriere
        ds_main = normalize_time_dim(xr.open_dataset(main_nc_path))
        ds_tail = normalize_time_dim(xr.open_dataset(tail_nc_path))
        ds = xr.concat([ds_main, ds_tail], dim="time")
        ds = ds.sortby("time")
        ds["time"] = ds["time"] - pd.Timedelta(days=1)

        ds.to_netcdf(daily_path)
        ds_main.close()
        ds_tail.close()
        ds.close()

        if not KEEP_RAW_PIECES:
            for p in [main_zip_path, tail_zip_path, main_nc_path, tail_nc_path]:
                if p and os.path.exists(p):
                    os.remove(p)

        print(f"  OK Sauvegarde (journalier exact) : {daily_path}")
        return daily_path

    except Exception as e:
        print(f"  ERREUR precip_pet {year}-{m} : {e}")
        return None


def download_temperature_daily(year: str, m: str):
    """
    Temperature 2m : variable instantanee -> daily-statistics fonctionne
    directement cote serveur (1 valeur/jour = moyenne journaliere).
    """
    file_path = os.path.join(dossier_destination, f'temperature_{year}_{m}.nc')
    if os.path.exists(file_path):
        print(f"  Deja telecharge : temperature {year}-{m}")
        return file_path

    jours = [f"{i:02d}" for i in range(1, 32)]
    print(f"Telechargement temperature {year}-{m}...")
    try:
        client.retrieve(
            'derived-era5-land-daily-statistics',
            {
                'variable': ['2m_temperature'],
                'year': year,
                'month': m,
                'day': jours,
                'daily_statistic': 'daily_mean',
                'time_zone': 'utc+00:00',
                'frequency': '1_hourly',
                'area': BBOX,
                'format': 'netcdf',
            },
            file_path
        )
        print(f"  OK Sauvegarde : {file_path}")
        extract_dir = os.path.join(dossier_usable, year, m)
        unzip(file_path, mois=m, year=year, extract_dir=extract_dir)
        return file_path
    except Exception as e:
        print(f"  ERREUR temperature {year}-{m} : {e}")
        return None


for year in years:
    for m in mois:
        download_precip_pet_daily(year, m)
        download_temperature_daily(year, m)

print("\nTelechargement ERA5-Land (Allemagne) termine !")