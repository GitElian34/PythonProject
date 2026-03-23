import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np

def visualiser_outliers(predictions, actuals, outliers, erreurs, station, df_dates):
    fig, axes = plt.subplots(3, 1, figsize=(14, 12))
    fig.suptitle(f"Détection d'outliers — Station {station}", fontsize=14, fontweight='bold')

    dates = df_dates.iloc[-len(actuals):]['date'].values

    # --- 1. Série temporelle : réel vs prédit ---
    ax1 = axes[0]
    ax1.plot(dates, actuals, label='Réel', color='steelblue', linewidth=1)
    ax1.plot(dates, predictions, label='Prédit', color='orange', linewidth=1, linestyle='--')
    ax1.scatter(dates[outliers], actuals[outliers], color='red', zorder=5, s=50, label='Outliers')
    ax1.set_title("Hauteur d'eau réelle vs prédite")
    ax1.set_ylabel("Hauteur normalisée")
    ax1.legend()
    ax1.grid(True, alpha=0.3)

    # --- 2. Erreur absolue par jour ---
    ax2 = axes[1]
    ax2.plot(dates, erreurs, color='gray', linewidth=0.8, label='Erreur absolue')
    seuil = erreurs.mean() + 2.5 * erreurs.std()
    ax2.axhline(seuil, color='red', linestyle='--', linewidth=1.5, label=f'Seuil (mean + 2.5σ = {seuil:.4f})')
    ax2.scatter(dates[outliers], erreurs[outliers], color='red', zorder=5, s=50)
    ax2.set_title("Erreur absolue par jour")
    ax2.set_ylabel("Erreur absolue")
    ax2.legend()
    ax2.grid(True, alpha=0.3)

    # --- 3. Zoom sur les outliers ---
    ax3 = axes[2]
    outlier_indices = np.where(outliers)[0]
    ax3.bar(range(len(outlier_indices)),
            erreurs[outlier_indices],
            color='red', alpha=0.7, edgecolor='white')
    ax3.set_xticks(range(len(outlier_indices)))
    ax3.set_xticklabels([str(dates[i])[:10] for i in outlier_indices],
                        rotation=45, ha='right', fontsize=8)
    ax3.set_title(f"{len(outlier_indices)} outliers détectés")
    ax3.set_ylabel("Erreur absolue")
    ax3.grid(True, alpha=0.3, axis='y')

    plt.tight_layout()
    plt.savefig(f'./data/IA/Visualisation/outliers_{station}.png', dpi=150, bbox_inches='tight')
    print(f"📊 Graphique sauvegardé : ./data/models/outliers_{station}.png")
    plt.show()