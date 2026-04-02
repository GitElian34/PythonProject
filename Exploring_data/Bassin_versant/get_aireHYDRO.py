import requests
import pandas as pd

def get_aire_banque_hydro(code_station):
    """
    Récupère la superficie du bassin versant d'une station
    depuis la Banque HYDRO (API officielle française).
    Le code station est au format 8 caractères, ex: 'O200001001'
    """
    url = f"https://hubeau.eaufrance.fr/api/v1/hydrometrie/referentiel/stations"
    params = {
        'code_station': code_station,
        'format': 'json',
        'size': 1
    }
    response = requests.get(url, params=params)
    data = response.json()

    if data['count'] > 0:
        station = data['data'][0]
        aire = station.get('superficie_bv', None)
        nom = station.get('libelle_station', '')
        return nom, aire
    return None, None

# Exemple avec la Garonne à Tonneins (une station bien documentée)
nom, aire_ref = get_aire_banque_hydro('O200001001')
print(f"Station : {nom}")
print(f"Aire référence Banque HYDRO : {aire_ref} km²")