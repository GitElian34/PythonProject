import xarray as xr
import pandas as pd
import numpy as np

# === CONFIGURATION ===
fichier = '/home/sar_hydro/STUDIES/EtudesEB/PythonProject/data/ERA5/usable_data_LAND_France/2016/02/data.nc'  # Adapte le nom si nécessaire
lat_cible = 43.5  # latitude exemple (sud de la France)
lon_cible = 2.5  # longitude exemple (Toulouse)
date_cible = '2016-02-03T12:00'  # format: YYYY-MM-DDTHH:MM

# === CHARGEMENT ===
ds = xr.open_dataset(fichier)

# === CONVERSION PRÉCIPITATIONS (mètres → millimètres) ===
# Les précipitations ERA5 sont en mètres par heure
ds['tp_mm'] = ds['tp'] * 1000
ds['tp_mm'].attrs['units'] = 'mm/h'
ds['tp_mm'].attrs['long_name'] = 'Total precipitation (mm/h)'

# === EXTRACTION AU POINT LE PLUS PROCHE ===
precip = ds['tp_mm'].sel(
    latitude=lat_cible,
    longitude=lon_cible,
    valid_time=date_cible,
    method='nearest'  # prend le point de grille le plus proche
)

# === RÉSULTAT ===
print(f"\n📍 Position: {lat_cible}°N, {lon_cible}°E")
print(f"📅 Date: {date_cible}")
print(f"🌧️ Précipitations: {precip.values:.2f} mm/h")
print(f"\nPoint de grille utilisé:")
print(f"  Latitude: {precip.latitude.values:.2f}°")
print(f"  Longitude: {precip.longitude.values:.2f}°")

# === SI VOUS VOULEZ PLUSIEURS DATES ===
print("\n" + "=" * 50)
print("SÉRIE TEMPORELLE COMPLÈTE AU MÊME POINT")
print("=" * 50)

serie_precip = ds['tp_mm'].sel(
    latitude=lat_cible,
    longitude=lon_cible,
    method='nearest'
)

print(f"Moyenne: {serie_precip.mean().values:.2f} mm/h")
print(f"Min: {serie_precip.min().values:.2f} mm/h")
print(f"Max: {serie_precip.max().values:.2f} mm/h")
print(f"Écart-type: {serie_precip.std().values:.2f} mm/h")
print(f"Cumul total: {serie_precip.sum().values:.2f} mm")

# === STATISTIQUES SUPPLÉMENTAIRES POUR PRÉCIPITATIONS ===
print("\n" + "=" * 50)
print("STATISTIQUES COMPLÉMENTAIRES")
print("=" * 50)

# Nombre d'heures avec précipitations (> 0.1 mm/h)
heures_pluie = (serie_precip > 0.1).sum().values
total_heures = len(serie_precip)
pourcentage_pluie = (heures_pluie / total_heures) * 100

print(f"Heures avec pluie (>0.1mm/h): {heures_pluie} / {total_heures}")
print(f"Pourcentage de temps avec pluie: {pourcentage_pluie:.1f}%")

# Intensité moyenne quand il pleut
intensite_moyenne_pluie = serie_precip.where(serie_precip > 0.1).mean().values
print(f"Intensité moyenne quand il pleut: {intensite_moyenne_pluie:.2f} mm/h")

# === EXPORT OPTIONNEL ===
# Sauvegarder en CSV
df = serie_precip.to_dataframe(name='precipitations_mm_h')
df.to_csv('precipitations_point_unique.csv')
print("\n✅ Données sauvegardées dans 'precipitations_point_unique.csv'")

# === EXPORT AVEC STATISTIQUES JOURNALIÈRES ===
print("\n📊 Calcul des cumuls journaliers...")

# Ajouter une colonne date (sans l'heure)
df['date'] = df.index.date
cumuls_jour = df.groupby('date')['precipitations_mm_h'].sum()
cumuls_jour.to_csv('precipitations_cumuls_jour.csv')
print("✅ Cumuls journaliers sauvegardés dans 'precipitations_cumuls_jour.csv'")

# === PETIT GRAPHIQUE RAPIDE (optionnel) ===
try:
    import matplotlib.pyplot as plt

    plt.figure(figsize=(12, 5))
    serie_precip.plot()
    plt.title(f'Précipitations horaires à {lat_cible}°N, {lon_cible}°E')
    plt.ylabel('mm/h')
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig('precipitations_serie_temporelle.png')
    plt.show()
    print("✅ Graphique sauvegardé")
except:
    print("ℹ️ matplotlib non installé, pas de graphique")

# Fermeture propre
ds.close()