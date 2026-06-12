# TFT — Détection d'outliers hauteur d'eau satellite

## Structure des fichiers

```
tft_config.yaml       ← configuration centrale (données, modèle, entraînement)
data_loader.py        ← chargement .nc + attributs + construction DataFrame
train.py              ← entraînement TFT
detect_outliers.py    ← inférence + détection + visualisation
```

## Installation

```bash
pip install pytorch-forecasting lightning netCDF4 pandas numpy tqdm pyyaml matplotlib pyarrow
```

Version testée : Python 3.10, PyTorch 2.x, pytorch-forecasting 1.x

---

## Ordre d'exécution

### 1. Vérifier la config
Ouvrir `tft_config.yaml` et vérifier les chemins :
```yaml
data:
  time_series_dir: ./data/IA/NeuralHydrology_feat27j/time_series
  attributes_path: ./data/IA/NeuralHydrology_feat27j/attributes/attributes.csv
```

### 2. Test rapide sur 500 stations (recommandé avant le full run)
```bash
python train.py --config tft_config.yaml --debug
```
Durée estimée : ~10-15 min. Vérifie que tout tourne sans erreur.

### 3. Entraînement complet
```bash
python train.py --config tft_config.yaml
```
Durée estimée : plusieurs heures selon GPU disponible.
Les checkpoints sont sauvegardés dans `./outputs/tft/checkpoints/`.
Suivre l'entraînement avec TensorBoard :
```bash
tensorboard --logdir ./outputs/tft/logs
```

### 4. Détection d'outliers
```bash
python detect_outliers.py \
    --config tft_config.yaml \
    --checkpoint ./outputs/tft/checkpoints/tft-best-XX-XXXX.ckpt \
    --plot_n 10
```
Sorties :
- `./outputs/tft/results/all_predictions.parquet` — toutes les prédictions
- `./outputs/tft/results/outliers.csv`            — outliers uniquement
- `./outputs/tft/results/outliers_STATION_ID.png` — graphiques par station

---

## Logique de détection

Pour chaque pas de temps $t$ d'une station, le TFT prédit un intervalle
de confiance $[q_{0.05}, q_{0.95}]$. Une valeur est flaggée outlier si :

```
y_true < q_0.05  OU  y_true > q_0.95
```

Le **score de sévérité** mesure de combien la valeur dépasse l'intervalle,
exprimé en IQR ($q_{0.75} - q_{0.25}$). Cela permet de trier les outliers
du plus au moins sévère.

---

## Points d'attention

**Split par station** : le split train/val/test est fait au niveau des stations
entières, pas des fenêtres temporelles. Cela garantit que le modèle est évalué
sur des stations non vues à l'entraînement.

**water_level déjà normalisé** : les valeurs sont dans [-2, +1.6].
Le `target_normalizer` est désactivé dans `TimeSeriesDataSet` en conséquence.
Si tu travailles avec des valeurs brutes en mètres, réactiver le `GroupNormalizer`.

**known_inputs** : `doy_sin` et `doy_cos` sont déclarés comme covariables
connues à l'avance (disponibles encodeur ET décodeur). Toutes les autres
covariables météo sont déclarées observées (encodeur uniquement).

**allow_missing_timesteps=True** : active la tolérance aux pas de temps
manquants dans les séries. Utile si certaines stations ont des lacunes
dans les données satellitaires.