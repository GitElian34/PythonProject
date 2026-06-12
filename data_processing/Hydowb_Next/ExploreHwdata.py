"""
explore_hydroweb_next.py
═══════════════════════════════════════════════════════════════════════════
Explore ce que HydroWeb Next retourne pour la France :
  1. Télécharge les rivières françaises (bbox France)
  2. Extrait le zip et affiche la structure
  3. Affiche le contenu d'un fichier exemple
═══════════════════════════════════════════════════════════════════════════
"""

import os
import zipfile
from pathlib import Path

import py_hydroweb

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
API_KEY    = "AJerWWCpm4wIaH8CMgPZlf67hNBC0VRMeCeeB1KgkaDHctfvYP"
OUTPUT_DIR = Path("./data/hydroweb_next")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
ZIP_PATH   = OUTPUT_DIR / "hydroweb_next_france_rivers.zip"

# ═══════════════════════════════════════════════════════════════
# ÉTAPE 1 : TÉLÉCHARGEMENT
# ═══════════════════════════════════════════════════════════════
print("Connexion à HydroWeb Next...")
client = py_hydroweb.Client(api_key=API_KEY)
basket = py_hydroweb.DownloadBasket("explore_france_rivers")

basket.add_collection(
    collection_id="HYDROWEB_RIVERS_OPE",
    bbox=[-5.5, 41.0, 9.5, 51.5],  # France métropolitaine
    query={
        "start_datetime": {"gte": "2016-01-01T00:00:00Z"},
        "end_datetime"  : {"lte": "2025-12-31T23:59:59Z"},
    }
)

print("Soumission de la requête...")
client.submit_and_download_zip(
    basket,
    zip_filename=str(ZIP_PATH.name),
    output_folder=str(OUTPUT_DIR)
)
print(f"✅ Zip téléchargé → {ZIP_PATH}")

# ═══════════════════════════════════════════════════════════════
# ÉTAPE 2 : STRUCTURE DU ZIP
# ═══════════════════════════════════════════════════════════════
print("\n── Contenu du zip ──")
with zipfile.ZipFile(ZIP_PATH) as z:
    names = z.namelist()
    print(f"  {len(names)} fichiers")

    # Extensions présentes
    exts = set(Path(n).suffix for n in names)
    print(f"  Extensions : {exts}")

    # Arborescence (5 premiers)
    print(f"\n  5 premiers fichiers :")
    for n in names[:5]:
        print(f"    {n}")

    # Extraire tout
    extract_dir = OUTPUT_DIR / "extracted"
    extract_dir.mkdir(exist_ok=True)
    z.extractall(extract_dir)
    print(f"\n✅ Extrait → {extract_dir}")

# ═══════════════════════════════════════════════════════════════
# ÉTAPE 3 : APERÇU D'UN FICHIER
# ═══════════════════════════════════════════════════════════════
print("\n── Aperçu d'un fichier ──")
all_files = list(extract_dir.rglob("*.*"))
print(f"  {len(all_files)} fichiers extraits")

# Trouver les formats disponibles
for ext in [".csv", ".json", ".txt", ".nc", ".geojson"]:
    sample = next((f for f in all_files if f.suffix == ext), None)
    if sample:
        print(f"\n  Format {ext} trouvé : {sample.name}")
        if ext in [".csv", ".txt", ".json"]:
            with open(sample, encoding="utf-8", errors="replace") as f:
                content = f.read(3000)
            print(content)
        break
