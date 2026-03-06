# ============================================
# LSTM ULTRA SIMPLE POUR DÉBUTER
# ============================================

import numpy as np
import matplotlib.pyplot as plt
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense
from sklearn.preprocessing import MinMaxScaler
  # 1. DONNÉES SIMPLES (une sinusoïde)
print("📊 Création des données...")
x = np.linspace(0, 50, 500)
y = np.sin(x)  # une simple sinusoïde

# Normalisation
scaler = MinMaxScaler()
y_scaled = scaler.fit_transform(y.reshape(-1, 1)).flatten()

# 2. PRÉPARATION SIMPLE
def create_sequences(data, lookback=10):
    X, y = [], []
    for i in range(len(data) - lookback):
        X.append(data[i:i+lookback])
        y.append(data[i+lookback])
    return np.array(X), np.array(y)

# Créer les séquences (10 jours d'entrée, 1 jour de sortie)
lookback = 10
X, y = create_sequences(y_scaled, lookback)

# Split train/test (80/20)
split = int(0.8 * len(X))
X_train, X_test = X[:split], X[split:]
y_train, y_test = y[:split], y[split:]

print(f"X_train: {X_train.shape}")

# 3. MODÈLE LSTM SIMPLE (juste 1 couche !)
model = Sequential([
    LSTM(20, input_shape=(lookback, 1)),
    Dense(1)
])

model.compile(optimizer='adam', loss='mse')
model.summary()

# 4. ENTRAÎNEMENT (5 secondes)
print("\n🎯 Entraînement...")
history = model.fit(
    X_train, y_train,
    validation_data=(X_test, y_test),
    epochs=20,
    verbose=1
)

# 5. PRÉDICTION
y_pred = model.predict(X_test)

# Remettre à l'échelle
y_test_real = scaler.inverse_transform(y_test.reshape(-1, 1))
y_pred_real = scaler.inverse_transform(y_pred)

# 6. VISUALISATION RAPIDE
plt.figure(figsize=(10, 4))
plt.plot(y_test_real[:100], label='Vrai', linewidth=2)
plt.plot(y_pred_real[:100], label='Prédit', linewidth=2)
plt.title('Prédictions LSTM (sinusoïde)')
plt.legend()
plt.grid(True)
plt.show()

print(f"✅ Terminé! Erreur moyenne: {np.mean(np.abs(y_test_real - y_pred_real)):.3f}")