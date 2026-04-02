import os
import numpy as np
import pandas as pd
import geopandas as gpd
import xarray as xr
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import contextily as ctx
from shapely.wkt import loads
from datetime import datetime, timedelta
import rasterio
from rasterio.windows import from_bounds

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
NOM_STATION   = 'R_GARONNE_GARONNE_KM0084'
CSV_BV        = './data/hydroweb/bassins_versants/bassins_versants_GARONNE_10.csv'
CSV_TRANSFERT = './data/hydroweb/ERA5/transfert_R_GARONNE_GARONNE_KM0084.csv'
ERA5_BASE     = './data/ERA5/usable_data_LAND_France'
OUTPUT_DIR    = './data/hydroweb/ERA5'
DATE_REF      = '2021-01-22'
HEURE_REF     = 12
K, N          = 0.5, 0.3
FENETRES      = [(0, 12), (12, 36), (36, 60), (60, 84), (84, 108)]

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

def determiner_jours(temps_h, date_ref, heure_ref):
    """Retourne les jours ERA5 à prendre selon la fenêtre temporelle."""
    dt_ref = datetime.strptime(f'{date_ref} {heure_ref:02d}:00', '%Y-%m-%d %H:%M')

    for i, (debut, fin) in enumerate(FENETRES):
        if debut <= temps_h < fin:
            if i == 0:
                return [str(dt_ref.date())]
            else:
                j1 = dt_ref.date() - timedelta(days=i - 1)
                j2 = dt_ref.date() - timedelta(days=i)
                return [str(j1), str(j2)]

    # Au-delà de la dernière fenêtre
    n  = len(FENETRES)
    j1 = dt_ref.date() - timedelta(days=n - 1)
    j2 = dt_ref.date() - timedelta(days=n)
    return [str(j1), str(j2)]


def fenetre_label(temps_h):
    """Retourne le label de fenêtre temporelle pour un temps donné."""
    for debut, fin in FENETRES:
        if debut <= temps_h < fin:
            return f'{debut}-{fin}h'
    return f'>{FENETRES[-1][1]}h'


def get_cumul_pixel(lo, la, jours, cache_era5, era5_base):
    """Cumul ERA5 d'un pixel sur les jours donnés avec cache."""
    cumul = 0.0
    for jour in jours:
        if jour not in cache_era5:
            cache_era5[jour] = charger_era5_jour(era5_base, jour)
        cumul += float(cache_era5[jour].sel(
            latitude=la, longitude=lo, method='nearest'
        ).values)
    return round(cumul, 3)


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════
def main():
    # ── 1. Charger station et transfert ──
    df_bv       = pd.read_csv(CSV_BV)
    row_bv      = df_bv[df_bv['name'] == NOM_STATION].iloc[0]
    polygone_bv = loads(row_bv['polygone'])
    lon, lat    = row_bv['lon'], row_bv['lat']

    df_tr = pd.read_csv(CSV_TRANSFERT)
    print(f"Station : {NOM_STATION} | Pixels ERA5 : {len(df_tr)}")

    # ── 2. Calculer cumuls adaptés par pixel ──
    cache_era5 = {}
    resultats  = []

    for _, pixel in df_tr.iterrows():
        lo, la   = pixel['lon'], pixel['lat']
        temps_h  = pixel['temps_B_h']
        jours    = determiner_jours(temps_h, DATE_REF, HEURE_REF)
        cumul_mm = get_cumul_pixel(lo, la, jours, cache_era5, ERA5_BASE)

        resultats.append({
            'lon'           : lo,
            'lat'           : la,
            'temps_B_h'     : temps_h,
            'fenetre'       : fenetre_label(temps_h),
            'jours_pris'    : ' + '.join(jours),
            'cumul_mm'      : cumul_mm
        })

    df_res = pd.DataFrame(resultats)

    # ── DIAGNOSTIC ──

    tp_simple = cache_era5.get(DATE_REF)
    if tp_simple is None:
        tp_simple = charger_era5_jour(ERA5_BASE, DATE_REF)

    df_res['cumul_simple_mm'] = [
        round(float(tp_simple.sel(
            latitude=row['lat'], longitude=row['lon'], method='nearest'
        ).values), 3)
        for _, row in df_res.iterrows()
    ]

    # Vérification pixels proches (0-12h) : doit être identique au simple
    pixels_proches = df_res[df_res['fenetre'] == '0-12h']
    if not pixels_proches.empty:
        print(f"\nVérification pixels 0-12h (doit être = simple) :")
        print(pixels_proches[['lon', 'lat', 'cumul_mm', 'cumul_simple_mm']].to_string())

    # ── 4. Tableau synthétique par fenêtre ──
    synthese = df_res.groupby('fenetre').agg(
        n_pixels    =('cumul_mm', 'count'),
        cumul_moy_mm=('cumul_mm', 'mean'),
        cumul_tot_mm=('cumul_mm', 'sum')
    ).round(3)
    ordre    = [f'{d}-{f}h' for d, f in FENETRES] + [f'>{FENETRES[-1][1]}h']
    synthese = synthese.reindex([f for f in ordre if f in synthese.index])

    print(f"\n── Synthèse par fenêtre ──")
    print(synthese.to_string())

    # ── 5. Sauvegarder CSV ──
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    tag = f'{NOM_STATION}_{DATE_REF}_{HEURE_REF}h'
    df_res.to_csv(f'{OUTPUT_DIR}/cumul_pixels_{tag}.csv', index=False)
    synthese.to_csv(f'{OUTPUT_DIR}/synthese_fenetres_{tag}.csv')
    print("\nCSV sauvegardés !")

    # ── 6. Visualisation ──
    bv_gdf = gpd.GeoDataFrame(
        geometry=[polygone_bv], crs='EPSG:4326'
    ).to_crs('EPSG:3857')

    gdf_res = gpd.GeoDataFrame(
        df_res,
        geometry=gpd.points_from_xy(df_res.lon, df_res.lat),
        crs='EPSG:4326'
    ).to_crs('EPSG:3857')

    station_gdf = gpd.GeoDataFrame(
        [{'name': NOM_STATION}],
        geometry=gpd.points_from_xy([lon], [lat]),
        crs='EPSG:4326'
    ).to_crs('EPSG:3857')

    bounds       = bv_gdf.total_bounds
    marge        = 50000
    PIXEL_SIZE   = (bounds[2] - bounds[0]) / len(df_res['lon'].unique()) * 0.9
    PIXEL_HEIGHT = PIXEL_SIZE * 1.4

    fig, axes = plt.subplots(1, 2, figsize=(22, 10))

    for ax, col, titre in zip(
        axes,
        ['cumul_simple_mm', 'cumul_mm'],
        [f'SANS adaptation — Jour J ({DATE_REF}) uniquement',
         f'AVEC adaptation — Jours adaptés au temps de transfert']
    ):
        ax.set_xlim(bounds[0] - marge, bounds[2] + marge)
        ax.set_ylim(bounds[1] - marge, bounds[3] + marge)
        ctx.add_basemap(ax, source=ctx.providers.OpenStreetMap.Mapnik, zoom=7)
        bv_gdf.plot(ax=ax, color='none', edgecolor='navy', linewidth=2, zorder=3)

        norm = plt.Normalize(vmin=df_res[col].min(), vmax=df_res[col].max())
        cmap = cm.get_cmap('Blues')

        for _, row in gdf_res.iterrows():
            x, y  = row.geometry.x, row.geometry.y
            color = cmap(norm(row[col]))
            rect  = plt.Rectangle(
                (x - PIXEL_SIZE/2, y - PIXEL_HEIGHT/2),
                PIXEL_SIZE, PIXEL_HEIGHT,
                color=color, alpha=0.8, zorder=2
            )
            ax.add_patch(rect)

        sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
        sm.set_array([])
        plt.colorbar(sm, ax=ax, shrink=0.6, label='Cumul précipitations (mm)')
        station_gdf.plot(ax=ax, color='red', markersize=150, marker='v', zorder=5)
        ax.set_title(titre, fontsize=12, fontweight='bold')
        ax.set_axis_off()

    plt.suptitle(
        f'{NOM_STATION} — {DATE_REF} {HEURE_REF}h UTC',
        fontsize=14, fontweight='bold', y=1.01
    )
    plt.tight_layout()
    plt.savefig(f'{OUTPUT_DIR}/carte_cumul_{tag}.png', dpi=150, bbox_inches='tight')
    plt.show()
    print("Carte sauvegardée !")


if __name__ == '__main__':
    main()