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
    CDS renvoie parfois un vrai .nc, parfois un zip contenant un .nc,
    meme quand on demande format='netcdf'. Retourne toujours le vrai
    chemin NetCDF a ouvrir avec xarray.
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


def fetch_hourly_00(year: str, m: str, days: list[str], variables: list[str], out_path: str) -> str:
    """
    Recupere le pas de temps 00:00 UTC pour les jours demandes d'un mois donne.
    Utilise pour les variables cumulees (snowmelt) : la valeur a 00:00 sur le
    jour D represente le cumul du jour D-1 (0h a 24h).
    """
    client.retrieve(
        'reanalysis-era5-land',
        {
            'product_type': 'reanalysis',
            'variable': variables,
            'year': year,
            'month': m,
            'day': days,
            'time': ['00:00'],
            'area': BBOX,
            'format': 'netcdf',
        },
        out_path
    )
    extract_dir = os.path.join(dossier_usable, year, m, "snow_tmp")
    if zipfile.is_zipfile(out_path):
        unzip(out_path, mois=m, year=year, extract_dir=extract_dir)  # garde le print diagnostic si zip
    return resolve_netcdf(out_path, extract_dir)


def download_snowmelt_daily(year: str, m: str) -> str | None:
    """
    Snowmelt : variable cumulee (comme precip/PET) -> daily-statistics ne
    la supporte pas cote serveur. Meme methode du decalage a 00:00 que
    pour precip_pet : jours 2->N du mois M donnent les totaux 1->N-1,
    + jour 1 du mois suivant donne le total du dernier jour.
    """
    n_days = calendar.monthrange(int(year), int(m))[1]
    ny, nm = next_month(year, m)

    main_days = [f"{d:02d}" for d in range(2, n_days + 1)]
    main_zip_path = os.path.join(dossier_destination, f'_tmp_smlt_main_{year}_{m}.nc')
    main_nc_path = fetch_hourly_00(year, m, main_days, ['snowmelt'], main_zip_path)

    tail_zip_path = os.path.join(dossier_destination, f'_tmp_smlt_tail_{year}_{m}.nc')
    tail_nc_path = fetch_hourly_00(ny, nm, ['01'], ['snowmelt'], tail_zip_path)

    ds_main = normalize_time_dim(xr.open_dataset(main_nc_path))
    ds_tail = normalize_time_dim(xr.open_dataset(tail_nc_path))
    ds = xr.concat([ds_main, ds_tail], dim="time")
    ds = ds.sortby("time")
    ds["time"] = ds["time"] - pd.Timedelta(days=1)

    tmp_out = os.path.join(dossier_destination, f'_tmp_smlt_daily_{year}_{m}.nc')
    ds.to_netcdf(tmp_out)
    ds_main.close()
    ds_tail.close()
    ds.close()

    if not KEEP_RAW_PIECES:
        for p in [main_zip_path, tail_zip_path, main_nc_path, tail_nc_path]:
            if p and os.path.exists(p):
                os.remove(p)

    return tmp_out


def download_snow_depth_daily(year: str, m: str) -> str | None:
    """
    Snow depth : variable instantanee (comme la temperature) -> daily-statistics
    fonctionne directement cote serveur (1 valeur/jour = moyenne journaliere).
    """
    jours = [f"{i:02d}" for i in range(1, 32)]
    tmp_out = os.path.join(dossier_destination, f'_tmp_sd_daily_{year}_{m}.nc')

    client.retrieve(
        'derived-era5-land-daily-statistics',
        {
            'variable': ['snow_depth'],
            'year': year,
            'month': m,
            'day': jours,
            'daily_statistic': 'daily_mean',
            'time_zone': 'utc+00:00',
            'frequency': '1_hourly',
            'area': BBOX,
            'format': 'netcdf',
        },
        tmp_out
    )
    extract_dir = os.path.join(dossier_usable, year, m, "snow_tmp")
    if zipfile.is_zipfile(tmp_out):
        unzip(tmp_out, mois=m, year=year, extract_dir=extract_dir)
    return resolve_netcdf(tmp_out, extract_dir)


def download_snow_daily(year: str, m: str):
    """
    Fusionne snow_depth (instantane, daily-statistics) et snowmelt (cumule,
    decalage 00:00) dans un seul fichier snow_{year}_{month}.nc, meme
    convention que precip_pet_*.nc / temperature_*.nc.
    """
    daily_path = os.path.join(dossier_destination, f'snow_{year}_{m}.nc')
    if os.path.exists(daily_path):
        print(f"  Deja telecharge : snow {year}-{m}")
        return daily_path

    print(f"Telechargement snow {year}-{m} (snow_depth + snowmelt)...")
    try:
        sd_nc_path = download_snow_depth_daily(year, m)
        smlt_nc_path = download_snowmelt_daily(year, m)

        ds_sd = normalize_time_dim(xr.open_dataset(sd_nc_path))
        ds_smlt = normalize_time_dim(xr.open_dataset(smlt_nc_path))

        # Aligner les deux jeux de temps (securite si un jour manque d'un cote)
        ds_sd, ds_smlt = xr.align(ds_sd, ds_smlt, join="inner")

        ds = xr.merge([ds_sd, ds_smlt])
        ds.to_netcdf(daily_path)

        ds_sd.close()
        ds_smlt.close()
        ds.close()

        if not KEEP_RAW_PIECES:
            for p in [sd_nc_path, smlt_nc_path]:
                if p and os.path.exists(p):
                    os.remove(p)

        print(f"  OK Sauvegarde (journalier exact) : {daily_path}")
        return daily_path

    except Exception as e:
        print(f"  ERREUR snow {year}-{m} : {e}")
        return None


for year in years:
    for m in mois:
        download_snow_daily(year, m)

print("\nTelechargement ERA5-Land snow (Allemagne) termine !")