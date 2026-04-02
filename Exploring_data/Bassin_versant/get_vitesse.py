import numpy as np
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import contextily as ctx
import json
import os
from pysheds.grid import Grid
from shapely.wkt import loads
from shapely.geometry import Point
from rasterio.windows import from_bounds
import rasterio

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES GLOBAUX
# ═══════════════════════════════════════════════════════════════
DIR_PATH  = '/home/sar_hydro/STUDIES/EtudesEB/HydroSHED/other/hyd_eu_dir_15s.tif'
ACC_PATH  = '/home/sar_hydro/STUDIES/EtudesEB/HydroSHED/other/hyd_eu_acc_15s.tif'
LDN_PATH  = '/home/sar_hydro/STUDIES/EtudesEB/HydroSHED/other/hyd_eu_ldn_15s.tif'
NODATA    = 4294967295
ERA5_RES  = 0.1
BBOX      = {'left': -6.0, 'right': 10.0, 'bottom': 41.0, 'top': 52.0}

# ═══════════════════════════════════════════════════════════════
# FONCTIONS
# ═══════════════════════════════════════════════════════════════

def charger_station(csv_bv, nom_station):
    """
    Charge les infos d'une station depuis le CSV des bassins versants.
    Retourne le polygone, lon, lat et l'aire.
    """
    df_bv   = pd.read_csv(csv_bv)
    row_bv  = df_bv[df_bv['name'] == nom_station].iloc[0]
    polygone = loads(row_bv['polygone'])
    lon, lat = row_bv['lon'], row_bv['lat']
    print(f"Station : {nom_station} ({lon}, {lat})")
    print(f"Aire    : {row_bv['aire_km2']} km²")
    return polygone, lon, lat, row_bv['aire_km2']


def load_clipped(path, bbox):
    """
    Charge un raster HydroSHEDS clippé sur une bbox donnée.
    Retourne les données et le transform rasterio.
    """
    with rasterio.open(path) as src:
        window    = from_bounds(
            bbox['left'], bbox['bottom'],
            bbox['right'], bbox['top'],
            src.transform
        )
        data      = src.read(1, window=window)
        transform = src.window_transform(window)
    return data, transform


def construire_grille(transform, shape):
    """
    Construit les tableaux de coordonnées lon/lat
    à partir du transform rasterio et de la shape du raster.
    """
    nrows, ncols = shape
    lons_grid = np.array([transform.c + (j + 0.5) * transform.a for j in range(ncols)])
    lats_grid = np.array([transform.f + (i + 0.5) * transform.e for i in range(nrows)])
    return lons_grid, lats_grid


def creer_masque_bv(polygone, lons_grid, lats_grid):
    """
    Crée un masque booléen des pixels HydroSHEDS
    qui tombent à l'intérieur du polygone du bassin versant.
    """
    nrows   = len(lats_grid)
    ncols   = len(lons_grid)
    minx, miny, maxx, maxy = polygone.bounds

    mask_lon = (lons_grid >= minx) & (lons_grid <= maxx)
    mask_lat = (lats_grid >= miny) & (lats_grid <= maxy)

    catch_mask = np.zeros((nrows, ncols), dtype=bool)
    for i in np.where(mask_lat)[0]:
        for j in np.where(mask_lon)[0]:
            if polygone.contains(Point(lons_grid[j], lats_grid[i])):
                catch_mask[i, j] = True

    print(f"Pixels HydroSHEDS dans le BV : {catch_mask.sum()}")
    return catch_mask


def calculer_distances(ldn_data, acc_data, catch_mask):
    """
    Calcule la distance de chaque pixel HydroSHEDS vers la station
    en utilisant LDN. Retourne dist_view et acc_view masqués.
    """
    ldn_view = ldn_data.astype(np.float64)
    acc_view = acc_data.astype(np.float64)

    ldn_view[ldn_view == NODATA] = np.nan
    acc_view[acc_view == NODATA] = np.nan
    ldn_view[~catch_mask]        = np.nan
    acc_view[~catch_mask]        = np.nan

    ldn_station = np.nanmin(ldn_view)
    print(f"LDN à la station  : {ldn_station/1000:.0f} km jusqu'à l'exutoire")

    dist_view = ldn_view - ldn_station
    dist_view[dist_view < 0] = np.nan
    print(f"Distance max dans le BV : {np.nanmax(dist_view)/1000:.0f} km")
    print(f"Distance moy dans le BV : {np.nanmean(dist_view)/1000:.0f} km")

    return dist_view, acc_view


def calculer_temps_transfert(pixels_era5, dist_view, acc_view,
                              lons_grid, lats_grid, K, N):
    """
    Pour chaque pixel ERA5, calcule le temps de transfert
    selon la méthode A (vitesse constante) et B (vitesse variable).
    Retourne un DataFrame avec les résultats.
    """
    resultats = []

    for _, pixel in pixels_era5.iterrows():
        lo, la = pixel['lon'], pixel['lat']

        mask_lon = (lons_grid >= lo - ERA5_RES/2) & (lons_grid < lo + ERA5_RES/2)
        mask_lat = (lats_grid >= la - ERA5_RES/2) & (lats_grid < la + ERA5_RES/2)

        dist_subset  = dist_view[np.ix_(mask_lat, mask_lon)]
        acc_subset   = acc_view[np.ix_(mask_lat, mask_lon)]
        dist_valides = dist_subset[~np.isnan(dist_subset)]
        acc_valides  = acc_subset[~np.isnan(acc_subset)]

        if len(dist_valides) == 0:
            print(f"  → Aucun pixel HydroSHEDS pour ERA5 ({lo}, {la})")
            continue

        dist_moy  = dist_valides.mean()
        acc_moy   = acc_valides.mean()

        # Méthode A : vitesse constante 1 m/s
        temps_A_h = dist_moy / 1.0 / 3600

        # Méthode B : vitesse variable selon accumulation
        vitesse_B = K * (max(acc_moy, 1) ** N)
        temps_B_h = dist_moy / vitesse_B / 3600

        resultats.append({
            'lon'       : lo,
            'lat'       : la,
            'tp_mm'     : pixel['tp_mm'],
            'dist_km'   : round(dist_moy / 1000, 1),
            'temps_A_h' : round(temps_A_h, 1),
            'temps_A_j' : round(temps_A_h / 24, 1),
            'vitesse_B' : round(vitesse_B, 3),
            'temps_B_h' : round(temps_B_h, 1),
            'temps_B_j' : round(temps_B_h / 24, 1),
        })

    return pd.DataFrame(resultats)


def visualiser_comparaison(df_res, polygone_bv, lon, lat,
                            nom_station, K, N, output_dir):
    """
    Affiche et sauvegarde une carte comparative
    des temps de transfert méthode A vs méthode B.
    """
    bv_gdf = gpd.GeoDataFrame(
        geometry=[polygone_bv], crs='EPSG:4326'
    ).to_crs('EPSG:3857')

    gdf_res = gpd.GeoDataFrame(
        df_res,
        geometry=gpd.points_from_xy(df_res.lon, df_res.lat),
        crs='EPSG:4326'
    ).to_crs('EPSG:3857')

    station_gdf = gpd.GeoDataFrame(
        [{'name': nom_station}],
        geometry=gpd.points_from_xy([lon], [lat]),
        crs='EPSG:4326'
    ).to_crs('EPSG:3857')

    bounds       = bv_gdf.total_bounds
    marge        = 50000
    PIXEL_SIZE   = (bounds[2] - bounds[0]) / len(df_res['lon'].unique()) * 0.9
    PIXEL_HEIGHT = PIXEL_SIZE * 1.4

    fig, axes = plt.subplots(1, 2, figsize=(20, 10))

    for ax, col, titre in zip(
        axes,
        ['temps_A_h', 'temps_B_h'],
        [f'Méthode A — Vitesse constante (1 m/s)',
         f'Méthode B — Vitesse variable (k={K}, n={N})']
    ):
        ax.set_xlim(bounds[0] - marge, bounds[2] + marge)
        ax.set_ylim(bounds[1] - marge, bounds[3] + marge)
        ctx.add_basemap(ax, source=ctx.providers.OpenStreetMap.Mapnik, zoom=7)
        bv_gdf.plot(ax=ax, color='none', edgecolor='navy', linewidth=2, zorder=3)

        norm = plt.Normalize(vmin=df_res[col].min(), vmax=df_res[col].max())
        cmap = plt.cm.get_cmap('RdYlGn_r')

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
        cbar = plt.colorbar(sm, ax=ax, shrink=0.6, pad=0.02)
        cbar.set_label('Temps de transfert (heures)', fontsize=11)

        station_gdf.plot(ax=ax, color='red', markersize=150,
                         marker='v', zorder=5)
        ax.set_title(titre, fontsize=13, fontweight='bold', pad=12)
        ax.set_axis_off()

    plt.suptitle(
        f'Comparaison temps de transfert — {nom_station}',
        fontsize=15, fontweight='bold', y=1.01
    )
    plt.tight_layout()

    os.makedirs(output_dir, exist_ok=True)
    out_path = f'{output_dir}/comparaison_transfert_{nom_station}.png'
    plt.savefig(out_path, dpi=150, bbox_inches='tight')
    plt.show()
    print(f"Carte sauvegardée : {out_path}")


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════
def main():
    # ── Paramètres ──
    NOM_STATION = 'R_GARONNE_GARONNE_KM0084'
    CSV_BV      = './data/hydroweb/bassins_versants/bassins_versants_GARONNE_10.csv'
    CSV_PLUIE   = './data/hydroweb/ERA5/precipitations_GARONNE_2016-01-15.csv'
    OUTPUT_DIR  = './data/hydroweb/ERA5'
    K, N        = 0.5, 0.3

    # ── 1. Charger la station ──
    polygone_bv, lon, lat, aire = charger_station(CSV_BV, NOM_STATION)

    # ── 2. Charger HydroSHEDS clippé sur la France ──
    print("\nChargement HydroSHEDS...")
    ldn_data, ldn_transform = load_clipped(LDN_PATH, BBOX)
    acc_data, _             = load_clipped(ACC_PATH, BBOX)
    print(f"Rasters chargés : shape={ldn_data.shape}")

    # ── 3. Construire la grille de coordonnées ──
    lons_grid, lats_grid = construire_grille(ldn_transform, ldn_data.shape)
    print(f"lons_grid : {lons_grid.min():.2f} → {lons_grid.max():.2f}")
    print(f"lats_grid : {lats_grid.min():.2f} → {lats_grid.max():.2f}")

    # ── 4. Créer le masque du BV ──
    print("\nCréation du masque BV...")
    catch_mask = creer_masque_bv(polygone_bv, lons_grid, lats_grid)

    # ── 5. Calculer les distances ──
    print("\nCalcul des distances...")
    dist_view, acc_view = calculer_distances(ldn_data, acc_data, catch_mask)

    # ── 6. Charger les pixels ERA5 ──
    df_pluie    = pd.read_csv(CSV_PLUIE)
    row_pluie   = df_pluie[df_pluie['name'] == NOM_STATION].iloc[0]
    pixels_era5 = pd.DataFrame(json.loads(row_pluie['pixels_detail']))
    print(f"\nPixels ERA5 dans le BV : {len(pixels_era5)}")

    # ── 7. Calculer les temps de transfert ──
    print("\nCalcul des temps de transfert...")
    df_res = calculer_temps_transfert(
        pixels_era5, dist_view, acc_view,
        lons_grid, lats_grid, K, N
    )

    if df_res.empty:
        print("ERREUR : aucun résultat — vérifier les données.")
        return

    print(f"\n── Résultats ({len(df_res)} pixels) ──")
    print(df_res[['lon', 'lat', 'dist_km', 'temps_A_h',
                  'temps_A_j', 'vitesse_B', 'temps_B_h', 'temps_B_j']].to_string())

    # ── 8. Sauvegarder ──
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_csv = f'{OUTPUT_DIR}/transfert_{NOM_STATION}.csv'
    df_res.to_csv(out_csv, index=False)
    print(f"\nRésultats sauvegardés : {out_csv}")

    # ── 9. Visualisation ──
    visualiser_comparaison(
        df_res, polygone_bv, lon, lat,
        NOM_STATION, K, N, OUTPUT_DIR
    )


if __name__ == '__main__':
    main()