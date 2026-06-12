import csv
import statistics

PATH1 = "./runs/arlstm_feat27jHigh_modele2_2205_151825/validation/model_epoch0"
PATH2 = "./runs/arlstm_feat27jHigh_modele2_2205_151905/validation/model_epoch0"
PATH3 = "./runs/arlstm_feat27jHigh_modele2_2205_152044/validation/model_epoch0"
PATH4 = "./runs/arlstm_feat27jHigh_modele2_2205_152119/validation/model_epoch0"
PATHs = [PATH1, PATH2, PATH3, PATH4]

def print_CSV(path: str):
    NSE_list = []
    KGE_list = []

    with open(path, 'r', encoding='utf-8') as fichier:
        lecteur = csv.reader(fichier)
        next(lecteur)  # Saute la ligne d'en-tête
        for ligne in lecteur:
            NSE_list.append(float(ligne[1]))
            KGE_list.append(float(ligne[2]))

    # Calcul de la médiane
    if len(NSE_list) > 0:
        NSE_median = statistics.median(NSE_list)
        KGE_median = statistics.median(KGE_list)
    else:
        NSE_median, KGE_median = 0.0, 0.0

    return NSE_median, KGE_median

# Stocker tous les résultats
resultats = []

for path in PATHs:
    for i in range(1, 31):
        num = format(i, '02d')
        PATH = path + num + '/validation_metrics.csv'
        try:
            NSE, KGE = print_CSV(PATH)
            resultats.append({
                'modele': path,
                'epoch': num,
                'NSE': NSE,
                'KGE': KGE
            })
        except FileNotFoundError:
            print(f"Fichier non trouvé : {PATH}")
        except (ValueError, IndexError) as e:
            print(f"Erreur de données dans {PATH} : {e}")

# Trier par NSE (du meilleur au moins bon = ordre décroissant)
resultats_tries = sorted(resultats, key=lambda x: x['NSE'], reverse=True)

# Afficher les résultats triés
print("\n" + "="*80)
print("RÉSULTATS TRIÉS PAR NSE (meilleur → moins bon)")
print("="*80)
for idx, res in enumerate(resultats_tries, 1):
    print(f"{idx:2d}. Epoch {res['epoch']} - NSE: {res['NSE']:.4f} | KGE: {res['KGE']:.4f}")
    print(f"    Modèle: {res['modele']}")
    print()