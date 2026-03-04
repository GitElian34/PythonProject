import os
from collections import Counter


def analyser_dossier(dossier):
    """Analyse un dossier et affiche les 10 bassins avec le plus de fichiers"""

    compteur = Counter()

    for fichier in os.listdir(dossier):
        chemin = os.path.join(dossier, fichier)
        if os.path.isfile(chemin):
            parties = fichier.split('_')
            if len(parties) >= 3 and parties[0] == 'R':
                compteur[parties[1]] += 1

    if compteur:
        print(f"\nTop 10 bassins :")
        for i, (bassin, nb) in enumerate(compteur.most_common(10), 1):
            print(f"{i}. {bassin} : {nb} fichier(s)")

        print(f"\nTotal : {sum(compteur.values())} fichiers")
        print(f"Bassins distincts : {len(compteur)}")
    else:
        print("Aucun fichier trouvé au format R_Basin_River")


def analyser_dossier_lignes(dossier):
    """Analyse un dossier et retourne le nombre de lignes par bassin"""

    compteur_lignes = Counter()

    for fichier in os.listdir(dossier):
        chemin = os.path.join(dossier, fichier)

        if os.path.isfile(chemin):
            parties = fichier.split('_')
            if len(parties) >= 3 and parties[0] == 'R':
                bassin = parties[1]

                try:
                    # Compter les lignes avec gestion d'encodage
                    with open(chemin, 'r', encoding='utf-8') as f:
                        compteur_lignes[bassin] += sum(1 for ligne in f)
                except UnicodeDecodeError:
                    # Fallback sur latin-1 si UTF-8 échoue
                    with open(chemin, 'r', encoding='latin-1') as f:
                        compteur_lignes[bassin] += sum(1 for ligne in f)
                except Exception as e:
                    print(f"Erreur avec {fichier}: {e}")

    # Affichage des résultats
    if compteur_lignes:
        print("\n📊 Top 10 bassins par nombre de lignes :")
        for i, (bassin, lignes) in enumerate(compteur_lignes.most_common(10), 1):
            print(f"{i:2d}. {bassin:20} : {lignes:8,} lignes")

        print(f"\n📈 Total lignes : {sum(compteur_lignes.values()):,}")
        print(f"🗺️  Bassins distincts : {len(compteur_lignes)}")
    else:
        print("❌ Aucun fichier trouvé au format R_Basin_River")

    return compteur_lignes

# Utilisation
dossier_a_analyser = "/archive/SAR_HYDRO/DONNEES/DAD/hydroweb_data/Rivers"  # À modifier
analyser_dossier_lignes(dossier_a_analyser)