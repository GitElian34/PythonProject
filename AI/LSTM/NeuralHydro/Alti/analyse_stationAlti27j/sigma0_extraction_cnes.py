"""
sigma0_extraction_cnes.py
─────────────────────────
À exécuter dans le venv CNES.
Lit sigma0_stations_config.csv et extrait le sigma0 + WSH pour chaque station.
Produit : ./data/sigma0/sigma0_all_stations.csv

Optimisations :
  - TableMeasure ouvert une seule fois par station
  - pd.concat sur liste (pas en boucle)
  - Sauvegarde par station (reprise après crash)
  - Parallélisation par station avec ProcessPoolExecutor
"""

import os
import numpy as np
import pandas as pd
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed

CONFIG_CSV  = "./sigma0_stations_config.csv"
OUT_DIR     = Path("./data/sigma0")
OUT_DIR.mkdir(parents=True, exist_ok=True)

LAT_WINDOW  = 0.02
N_WORKERS   = 8   # ← ajuste selon les CPU disponibles

# ── Config par satellite ─────────────────────────────────────────────────
SAT_CONFIG = {
    'S3A': {
        'orf':           'T_L2E_HS3A_HYDRO_ORF',
        'table_l2_name': 'TABLE_L2E_HS3A_HYDRO',
        'ges_table_dir': '/data/MPC_S3_BC05/Reprocessing_data_bc05/TABLES/DSC',
        'sigma0_col':    'SIGMA0.ALTI.CORR_OCOG',
        'orbit_col':     'ORBIT.ALTI.MOE_C',
        'range_col':     'RANGE.ALTI.CORR_OCOG',
        'dry_col':       'DRY_TROPOSPHERIC_CORRECTION.MODEL.ECMWF_DIRECT',
        'wet_col':       'WET_TROPOSPHERIC_CORRECTION.MODEL.ECMWF_DIRECT',
        'pole_col':      'POLE_TIDE_HEIGHT.MODEL.WAHR_85',
        'solid_col':     'SOLID_EARTH_TIDE_HEIGHT.MODEL.CARTWRIGHT_TAYLER_71',
        'iono_col':      'IONOSPHERIC_CORRECTION.MODEL.GIM',
        'geoid_col':     'GEOID_HEIGHT.MODEL.EGM2008',
        'list_clips': [
            'LONGITUDE', 'LATITUDE', 'PASS_NUMBER', 'CYCLE_NUMBER',
            'SIGMA0.ALTI.CORR_OCOG', 'SIGMA0.ALTI',
            'ORBIT.ALTI.MOE_C', 'RANGE.ALTI.CORR_OCOG',
            'DRY_TROPOSPHERIC_CORRECTION.MODEL.ECMWF_DIRECT',
            'WET_TROPOSPHERIC_CORRECTION.MODEL.ECMWF_DIRECT',
            'POLE_TIDE_HEIGHT.MODEL.WAHR_85',
            'SOLID_EARTH_TIDE_HEIGHT.MODEL.CARTWRIGHT_TAYLER_71',
            'IONOSPHERIC_CORRECTION.MODEL.GIM',
            'GEOID_HEIGHT.MODEL.EGM2008',
        ],
    },
    'S3B': {
        'orf':           'T_L2E_HS3B_HYDRO_ORF',
        'table_l2_name': 'TABLE_L2E_HS3B_HYDRO',
        'ges_table_dir': '/data/MPC_S3_BC05/Reprocessing_data_bc05/TABLES/DSC',
        'sigma0_col':    'SIGMA0.ALTI.CORR_OCOG',
        'orbit_col':     'ORBIT.ALTI.MOE_C',
        'range_col':     'RANGE.ALTI.CORR_OCOG',
        'dry_col':       'DRY_TROPOSPHERIC_CORRECTION.MODEL.ECMWF_DIRECT',
        'wet_col':       'WET_TROPOSPHERIC_CORRECTION.MODEL.ECMWF_DIRECT',
        'pole_col':      'POLE_TIDE_HEIGHT.MODEL.WAHR_85',
        'solid_col':     'SOLID_EARTH_TIDE_HEIGHT.MODEL.CARTWRIGHT_TAYLER_71',
        'iono_col':      'IONOSPHERIC_CORRECTION.MODEL.GIM',
        'geoid_col':     'GEOID_HEIGHT.MODEL.EGM2008',
        'list_clips': [
            'LONGITUDE', 'LATITUDE', 'PASS_NUMBER', 'CYCLE_NUMBER',
            'SIGMA0.ALTI.CORR_OCOG', 'SIGMA0.ALTI',
            'ORBIT.ALTI.MOE_C', 'RANGE.ALTI.CORR_OCOG',
            'DRY_TROPOSPHERIC_CORRECTION.MODEL.ECMWF_DIRECT',
            'WET_TROPOSPHERIC_CORRECTION.MODEL.ECMWF_DIRECT',
            'POLE_TIDE_HEIGHT.MODEL.WAHR_85',
            'SOLID_EARTH_TIDE_HEIGHT.MODEL.CARTWRIGHT_TAYLER_71',
            'IONOSPHERIC_CORRECTION.MODEL.GIM',
            'GEOID_HEIGHT.MODEL.EGM2008',
        ],
    },
    'J3': {
        'orf':           'L2E_HJ3_ORF',
        'table_l2_name': 'TABLE_L2E_HJ3',
        'ges_table_dir': '/data/L2HR_J3/data/TABLES/DSC/',
        'sigma0_col':    'SIGMA0.ALTI.RTK_ICE1',
        'orbit_col':     'ORBIT.ALTI.CNES_POE_F',
        'range_col':     'RANGE.ALTI.RTK_ICE1',
        'dry_col':       'DRY_TROPOSPHERIC_CORRECTION.MODEL.ERA5',
        'wet_col':       'WET_TROPOSPHERIC_CORRECTION.MODEL.3D',
        'pole_col':      'POLE_TIDE_HEIGHT.MODEL.DESAI_2015_MPL2017',
        'solid_col':     'SOLID_EARTH_TIDE_HEIGHT.MODEL.CARTWRIGHT_TAYLER_71',
        'iono_col':      'IONOSPHERIC_CORRECTION.MODEL.GIM',
        'geoid_col':     'GEOID_HEIGHT.MODEL.EGM2008',
        'list_clips': [
            'LONGITUDE', 'LATITUDE', 'PASS_NUMBER', 'CYCLE_NUMBER',
            'SIGMA0.ALTI.RTK_ICE1',
            'ORBIT.ALTI.CNES_POE_F', 'RANGE.ALTI.RTK_ICE1',
            'DRY_TROPOSPHERIC_CORRECTION.MODEL.ERA5',
            'WET_TROPOSPHERIC_CORRECTION.MODEL.3D',
            'POLE_TIDE_HEIGHT.MODEL.DESAI_2015_MPL2017',
            'SOLID_EARTH_TIDE_HEIGHT.MODEL.CARTWRIGHT_TAYLER_71',
            'IONOSPHERIC_CORRECTION.MODEL.GIM',
            'GEOID_HEIGHT.MODEL.EGM2008',
        ],
    },
    'S6A': {
        'orf':           'T_HS6A_IGDR_SARM_ORF',
        'table_l2_name': 'TABLE_T_HS6A_IGDR_SARM_B',
        'ges_table_dir': '/data/peachis6_ex/TABLES/DSC/',
        'sigma0_col':    'SIGMA0.ALTI',
        'orbit_col':     'ORBIT.ALTI',
        'range_col':     'RANGE.ALTI',
        'dry_col':       'DRY_TROPOSPHERIC_CORRECTION.MODEL.DIRECT',
        'wet_col':       'WET_TROPOSPHERIC_CORRECTION.MODEL.DIRECT',
        'pole_col':      'POLE_TIDE_HEIGHT.MODEL.DESAI_2015_MPL2017',
        'solid_col':     'SOLID_EARTH_TIDE_HEIGHT.MODEL.CARTWRIGHT_TAYLER_71',
        'iono_col':      'IONOSPHERIC_CORRECTION.MODEL.GIM',
        'geoid_col':     'GEOID_HEIGHT.MODEL',
        'list_clips': [
            'LONGITUDE', 'LATITUDE', 'PASS_NUMBER', 'CYCLE_NUMBER',
            'SIGMA0.ALTI',
            'ORBIT.ALTI', 'RANGE.ALTI',
            'DRY_TROPOSPHERIC_CORRECTION.MODEL.DIRECT',
            'WET_TROPOSPHERIC_CORRECTION.MODEL.DIRECT',
            'POLE_TIDE_HEIGHT.MODEL.DESAI_2015_MPL2017',
            'SOLID_EARTH_TIDE_HEIGHT.MODEL.CARTWRIGHT_TAYLER_71',
            'IONOSPHERIC_CORRECTION.MODEL.GIM',
            'GEOID_HEIGHT.MODEL',
        ],
    },
}


# ═══════════════════════════════════════════════════════════════
# FONCTION PAR STATION — exécutée dans un worker séparé
# Chaque worker instancie ses propres objets Orf et TableMeasure
# ═══════════════════════════════════════════════════════════════
def extract_station(args):
    """Traite une station, retourne un DataFrame ou None."""
    # Import ici pour que chaque worker ait ses propres instances
    from octantng.vanilla.data import table as tbl
    from octantng.vanilla.data.orf import Orf as Orf

    sid, sat, pnum, lat, fc, lc, out_dir_str, lat_window = args
    out_dir  = Path(out_dir_str)
    out_path = out_dir / f"{sid}.csv"

    # Reprise : fichier déjà produit
    if out_path.exists():
        return pd.read_csv(out_path), sid, "skip"

    if sat not in SAT_CONFIG:
        return None, sid, f"satellite {sat} non configuré"

    cfg = SAT_CONFIG[sat]
    os.environ['GES_TABLE_DIR'] = cfg['ges_table_dir']
    os.environ['OCE_DATA']      = '/data/SUPPORT/DONNEES'

    try:
        orf_object = Orf(cfg['orf'])
        table_meas = tbl.TableMeasure(cfg['table_l2_name'], mode='r', mask_default=False)
    except Exception as e:
        return None, sid, f"init error: {e}"

    chunks = []
    for cycle in np.arange(fc, lc + 1):
        try:
            track      = orf_object.find_track_from_indices(cycle, pnum, method='equal')
            track_data = table_meas.read_values_as_dataset(
                cfg['list_clips'], track.first_date, track.last_date
            )
            pass_data  = track_data.to_dataframe()
            pass_data['LONGITUDE'] = (pass_data['LONGITUDE'] - 180) % 360 - 180
            chunks.append(pass_data)
        except Exception:
            continue

    if not chunks:
        return None, sid, "aucune donnée"

    out_data = pd.concat(chunks, ignore_index=False)
    out_data = out_data[
        (out_data['LATITUDE'] > lat - lat_window) &
        (out_data['LATITUDE'] < lat + lat_window)
    ]

    if out_data.empty:
        return None, sid, "aucune donnée dans la fenêtre lat"

    out_data['sigma0'] = out_data[cfg['sigma0_col']]
    try:
        out_data['WSH'] = (
            out_data[cfg['orbit_col']]
            - out_data[cfg['range_col']]
            - out_data[cfg['dry_col']]
            - out_data[cfg['wet_col']]
            - out_data[cfg['pole_col']]
            - out_data[cfg['solid_col']]
            - out_data[cfg['iono_col']]
            - out_data[cfg['geoid_col']]
        )
    except Exception:
        out_data['WSH'] = np.nan

    out_data['date'] = pd.to_datetime(out_data.index).date

    df_sta = (out_data
              .groupby('CYCLE_NUMBER')[['sigma0', 'WSH', 'LATITUDE', 'LONGITUDE']]
              .median()
              .reset_index())
    df_sta['date']         = out_data.groupby('CYCLE_NUMBER')['date'].first().values
    df_sta['station_code'] = sid
    df_sta['satellite']    = sat

    df_sta = df_sta[['date', 'station_code', 'satellite',
                     'sigma0', 'WSH', 'LATITUDE', 'LONGITUDE', 'CYCLE_NUMBER']]

    df_sta.to_csv(out_path, index=False)
    return df_sta, sid, f"{len(df_sta)} passages"


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════
if __name__ == '__main__':
    df_cfg = pd.read_csv(CONFIG_CSV)
    print(f"{len(df_cfg)} stations à extraire  |  {N_WORKERS} workers")
    print(df_cfg['satellite'].value_counts().to_string())

    # Prépare les arguments pour chaque station
    tasks = [
        (str(row['station_code']), row['satellite'], int(row['pass_number']),
         float(row['lat']), int(row['first_cycle']), int(row['last_cycle']),
         str(OUT_DIR), LAT_WINDOW)
        for _, row in df_cfg.iterrows()
    ]

    all_results = []
    n_done = 0

    with ProcessPoolExecutor(max_workers=N_WORKERS) as executor:
        futures = {executor.submit(extract_station, t): t[0] for t in tasks}
        for future in as_completed(futures):
            sid = futures[future]
            try:
                df, sid_ret, status = future.result()
                n_done += 1
                if df is not None:
                    all_results.append(df)
                    print(f"  [{n_done}/{len(tasks)}] {sid_ret} ✅ {status}")
                else:
                    print(f"  [{n_done}/{len(tasks)}] {sid_ret} ⚠  {status}")
            except Exception as e:
                n_done += 1
                print(f"  [{n_done}/{len(tasks)}] {sid} ❌ {e}")

    # Consolidation
    if all_results:
        df_all = pd.concat(all_results, ignore_index=True)
        out_path = OUT_DIR / "sigma0_all_stations.csv"
        df_all.to_csv(out_path, index=False)
        print(f"\n✅ {out_path}")
        print(f"   {len(df_all)} mesures  |  {df_all['station_code'].nunique()} stations")
    else:
        print("❌ Aucun résultat")