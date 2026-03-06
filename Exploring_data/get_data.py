import xarray as xr
import pandas as pd
import numpy as np

# === CONFIGURATION ===
fichier = '/home/sar_hydro/STUDIES/EtudesEB/PythonProject/Exploring_data/data_stream-oper_stepType-instant.nc'
lat_cible = 43.5      # latitude exemple (sud de la France)
lon_cible = 2.5        # longitude exemple (Toulouse)
date_cible = '2020-07-03T12:00'  # format: YYYY-MM-DDTHH:MM

# === CHARGEMENT ===
ds = xr.open_dataset(fichier)

# === CONVERSION TEMPÉRATURE (Kelvin → Celsius) ===
ds['t2m_c'] = ds['t2m'] - 273.15

# === EXTRACTION AU POINT LE PLUS PROCHE ===
temp = ds['t2m_c'].sel(
    latitude=lat_cible,
    longitude=lon_cible,
    valid_time=date_cible,
    method='nearest'  # prend le point de grille le plus proche
)

# === RÉSULTAT ===
print(f"\n📍 Position: {lat_cible}°N, {lon_cible}°E")
print(f"📅 Date: {date_cible}")
print(f"🌡️ Température: {temp.values:.2f} °C")
print(f"\nPoint de grille utilisé:")
print(f"  Latitude: {temp.latitude.values:.2f}°")
print(f"  Longitude: {temp.longitude.values:.2f}°")

# === SI VOUS VOULEZ PLUSIEURS DATES ===
print("\n" + "="*50)
print("SÉRIE TEMPORELLE COMPLÈTE AU MÊME POINT")
print("="*50)

serie_temp = ds['t2m_c'].sel(
    latitude=lat_cible,
    longitude=lon_cible,
    method='nearest'
)

print(f"Moyenne: {serie_temp.mean().values:.2f}°C")
print(f"Min: {serie_temp.min().values:.2f}°C")
print(f"Max: {serie_temp.max().values:.2f}°C")
print(f"Écart-type: {serie_temp.std().values:.2f}°C")

# === EXPORT OPTIONNEL ===
# Sauvegarder en CSV
df = serie_temp.to_dataframe(name='temperature_celsius')
df.to_csv('temperature_point_unique.csv')
print("\n✅ Données sauvegardées dans 'temperature_point_unique.csv'")

# Fermeture propre
ds.close()