#!/usr/bin/env python3
"""
model.py — Architecture Bi-LSTM pour détection d'outliers
"""

import torch
import torch.nn as nn
from config import WINDOW, HIDDEN_SIZE, N_LAYERS, DROPOUT


class BiLSTMOutlierDetector(nn.Module):
    """
    Bi-LSTM qui prédit water_level(T) à partir du contexte ±WINDOW jours.

    Architecture :
      1. Projection des statiques → même dimension que les dynamiques
      2. Concaténation dynamique + statique projeté à chaque pas de temps
      3. Bi-LSTM — lit la séquence dans les 2 sens simultanément
      4. Sortie à la position centrale T (le point masqué)
      5. Tête FC → prédiction scalaire

    La bidirectionnalité permet au modèle d'utiliser le contexte
    passé ET futur pour évaluer la cohérence de T.
    """

    def __init__(self, n_dynamic, n_static):
        super().__init__()

        # Projette les statiques pour les combiner aux dynamiques
        self.static_proj = nn.Sequential(
            nn.Linear(n_static, n_dynamic),
            nn.Tanh()
        )

        # Bi-LSTM — produit hidden_size*2 en sortie (forward + backward)
        self.lstm = nn.LSTM(
            input_size    = n_dynamic * 2,  # dynamique + statique projeté
            hidden_size   = HIDDEN_SIZE,
            num_layers    = N_LAYERS,
            batch_first   = True,
            dropout       = DROPOUT if N_LAYERS > 1 else 0.0,
            bidirectional = True             # ← la clé
        )

        self.dropout = nn.Dropout(DROPOUT)

        # Tête de régression
        self.head = nn.Sequential(
            nn.Linear(HIDDEN_SIZE * 2, 64),  # *2 car bidirectionnel
            nn.ReLU(),
            nn.Dropout(DROPOUT),
            nn.Linear(64, 1)
        )

    def forward(self, seq, static):
        """
        seq    : (batch, seq_len, n_dynamic)
        static : (batch, n_static)
        retour : (batch,) — prédiction water_level(T)
        """
        B, L, _ = seq.shape

        # Répéter les statiques projetés sur toute la séquence
        s_proj = self.static_proj(static)           # (B, n_dynamic)
        s_rep  = s_proj.unsqueeze(1).expand(B, L, -1)  # (B, L, n_dynamic)

        # Concaténer dynamique + statique
        x = torch.cat([seq, s_rep], dim=-1)         # (B, L, n_dynamic*2)

        # Bi-LSTM — out contient forward+backward concaténés à chaque T
        out, _ = self.lstm(x)                       # (B, L, hidden*2)

        # Extraire la sortie à la position centrale (T masqué)
        center = out[:, WINDOW, :]                  # (B, hidden*2)
        center = self.dropout(center)

        # Prédiction
        pred = self.head(center).squeeze(-1)        # (B,)
        return pred