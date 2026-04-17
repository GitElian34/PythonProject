import os
import io
import numpy as np
import rasterio
from rasterio.transform import from_bounds
from owslib.wcs import WebCoverageService

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
OUTPUT_DIR  = "./data/Bassin_Versants/SoilGrids/"
BBOX_FRANCE = (-6.0, 41.0, 10.0, 52.0)  # min_lon, min_lat, max_lon, max_lat
RESOLUTION  = 0.002  # ~200m en degrés

VARIABLES = ["clay", "sand", "silt"]
DEPTHS    = ["0-5cm_mean", "5-15cm_mean", "15-30cm_mean"]

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ═══════════════════════════════════════════════════════════════
# TÉLÉCHARGEMENT
# ═══════════════════════════════════════════════════════════════
total    = len(VARIABLES) * len(DEPTHS)
compteur = 0

for var in VARIABLES:
    url = f"https://maps.isric.org/mapserv?map=/map/{var}.map"
    print(f"\n── {var.upper()} ──")

    try:
        wcs = WebCoverageService(url, version="1.0.0")
    except Exception as e:
        print(f"  ❌ Connexion impossible : {e}")
        continue

    for depth in DEPTHS:
        compteur += 1
        identifier  = f"{var}_{depth}"
        output_path = os.path.join(OUTPUT_DIR, f"{identifier}.tif")

        if os.path.exists(output_path):
            print(f"  [{compteur}/{total}] {identifier} — déjà téléchargé ✅")
            continue

        print(f"  [{compteur}/{total}] {identifier}...", end=" ", flush=True)
        try:
            response = wcs.getCoverage(
                identifier=identifier,
                crs="urn:ogc:def:crs:EPSG::4326",
                bbox=BBOX_FRANCE,
                resx=RESOLUTION,
                resy=RESOLUTION,
                format="GEOTIFF_INT16"
            )
            data = response.read()

            # Sauvegarder directement le GeoTIFF
            with open(output_path, 'wb') as f:
                f.write(data)

            # Vérification rapide
            with rasterio.open(output_path) as src:
                print(f"✅  {src.width}×{src.height} px | "
                      f"{os.path.getsize(output_path)/1e6:.1f} Mo")

        except Exception as e:
            print(f"❌ {e}")
            # Supprimer le fichier partiel si existant
            if os.path.exists(output_path):
                os.remove(output_path)

print(f"\n✅ Terminé ! Fichiers dans : {OUTPUT_DIR}")
print("Fichiers téléchargés :")
for f in sorted(os.listdir(OUTPUT_DIR)):
    size = os.path.getsize(os.path.join(OUTPUT_DIR, f)) / 1e6
    print(f"  {f:35s} {size:.1f} Mo")