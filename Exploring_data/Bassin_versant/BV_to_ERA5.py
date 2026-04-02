import os
import pandas as pd
import xarray as xr
from shapely.wkt import loads
from shapely.geometry import Point

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
CSV_BV    = './data/hydroweb/bassins_versants/bassins_versants_GARONNE_10.csv'
ERA5_BASE = './data/ERA5/usable_data_LAND_France'
OUTPUT_DIR = './data/hydroweb/ERA5'
DATE      = '2016-01-15'

# ═══════════════════════════════════════════════════════════════
# FONCTIONS
# ═══════════════════════════════════════════════════════════════

def charger_era5_jour(era5_base, date):
    """Charge le cumul journalier ERA5 pour une date donnée."""
    annee, mois = date[:4], date[5:7]
    ds = xr.open_dataset(f'{era5_base}/{annee}/{mois}/data_0.nc')

    # Sélectionner uniquement les heures du jour demandé
    # puis prendre la dernière heure (23h) = cumul total de la journée
    tp = (
        ds['tp']
        .sel(valid_time=date)          # toutes les heures de ce jour
        .isel(valid_time=-1)           # dernière heure du jour = 23h
        * 1000                         # m → mm
    )
    print(f"  ERA5 {date} : min={float(tp.min().values):.3f} max={float(tp.max().values):.3f} mm")
    return tp

def extraire_pixels_bv(polygone, tp_jour, lats, lons):
    """Extrait les pixels ERA5 dans le polygone du BV."""
    minx, miny, maxx, maxy = polygone.bounds
    lats_f = lats[(lats >= miny) & (lats <= maxy)]
    lons_f = lons[(lons >= minx) & (lons <= maxx)]

    pixels = []
    for la in lats_f:
        for lo in lons_f:
            if polygone.contains(Point(lo, la)):
                tp_val = float(tp_jour.sel(
                    latitude=la, longitude=lo, method='nearest'
                ).values)
                pixels.append({'lat': la, 'lon': lo, 'tp_mm': round(tp_val, 3)})
    return pd.DataFrame(pixels)


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════
def main():
    # Charger les BV et ERA5 une seule fois
    df_bv = pd.read_csv(CSV_BV).dropna(subset=['polygone'])
    print(f"Stations avec polygone : {len(df_bv)}")

    tp_jour, lats, lons = charger_era5(ERA5_BASE, DATE)

    # Boucle sur les stations
    resultats = []
    for i, row in df_bv.iterrows():
        print(f"[{i+1}/{len(df_bv)}] {row['name']}...")
        polygone = loads(row['polygone'])
        pixels   = extraire_pixels_bv(polygone, tp_jour, lats, lons)

        if pixels.empty:
            print(f"  → Aucun pixel ERA5 trouvé !")
            continue

        print(f"  → {len(pixels)} pixels | moy: {pixels['tp_mm'].mean():.3f} mm | max: {pixels['tp_mm'].max():.3f} mm")
        resultats.append({
            'name'         : row['name'],
            'date'         : DATE,
            'n_pixels'     : len(pixels),
            'cumul_moy_mm' : round(pixels['tp_mm'].mean(), 3),
            'cumul_tot_mm' : round(pixels['tp_mm'].sum(), 3),
            'cumul_max_mm' : round(pixels['tp_mm'].max(), 3),
            'pixels_detail': pixels.to_json(orient='records')
        })

    # Sauvegarder
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_path = f'{OUTPUT_DIR}/precipitations_GARONNE_{DATE}.csv'
    df_out   = pd.DataFrame(resultats)
    df_out.to_csv(out_path, index=False)

    print(f"\n── Résultats ──")
    print(df_out[['name', 'n_pixels', 'cumul_moy_mm', 'cumul_max_mm']].to_string())
    print(f"\nSauvegardé dans {out_path}")


if __name__ == '__main__':
    main()