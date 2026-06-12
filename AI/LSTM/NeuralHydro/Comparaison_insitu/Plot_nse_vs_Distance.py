"""
plot_nse_vs_distance.py
═══════════════════════
Graphes de corrélation à partir des résultats zero-shot + insitu :
  1. NSE modèle vs distance insitu (km)
  2. Différence NSE (modèle - insitu) vs distance insitu (km)
"""

import numpy as np
import matplotlib.pyplot as plt
import pandas as pd

# ═══════════════════════════════════════════════════════════════
# DONNÉES — copier-coller depuis la sortie du script
# ═══════════════════════════════════════════════════════════════
RAW = [
    ("0000000005729", 0.79, "O919001001", 3.5,  0.73),
    ("0000000005735", 0.81, "O909001001", 1.0,  0.75),
    ("0000000005736", 0.74, "O866151002", 1.9,  0.53),
    ("0000000006310", 0.61, "K548092010", 6.1,  0.52),
    ("0000000006325", 0.86, "K683002001", 3.3,  0.83),
    ("0000000006326", 0.90, "L870001030", 3.7,  0.90),
    ("0000000008740",-0.03, "W334000102", 5.3, -0.74),
    ("0000000008748", 0.23, "V721651001",24.8, -0.69),
    ("0000000008751", 0.59, "V302511002", 1.5, -0.91),
    ("0000000008761", 0.59, "U063501001", 3.9,  0.43),
    ("0000000010842", 0.25, "H505104001", 1.7, -0.19),
    ("0000000010843", 0.58, "H501012001", 2.7,  0.36),
    ("0000000010844", 0.38, "H501012001", 3.3,  0.12),
    ("0000000010860", 0.57, "F358000101",10.7, -0.29),
    ("110986",        0.41, "L070061001", 2.5,  0.15),
    ("110987",        0.61, "H170001001",10.8,  0.65),
    ("111157",        0.15, "V000201201",34.0, -0.26),
    ("111158",        0.57, "U251542001", 3.0,  0.46),
    ("111159",        0.80, "H716201001", 0.8,  0.65),
    ("111511",        0.81, "O913401001", 5.8, -0.04),
    ("112064",        0.34, "O400101001", 6.8, -0.30),
    ("112066",        0.38, "K091001010", 1.6,  0.48),
    ("112556",        0.61, "M151161010", 3.4,  0.13),
    ("112557",        0.68, "M055603010", 4.6, -0.06),
    ("112558",        0.64, "M325311010", 1.3,  0.56),
    ("113449",        0.45, "M151161010", 7.9,  0.43),
    ("113450",        0.55, "M042151010", 1.9,  0.52),
    ("113598",        0.44, "K139181001", 0.3,  0.21),
    ("113599",        0.52, "H438021010", 0.5,  0.49),
]

df = pd.DataFrame(RAW, columns=["station", "nse_model", "insitu", "dist_km", "nse_insitu"])
df["delta_nse"] =abs(df["nse_model"] - df["nse_insitu"])

# ═══════════════════════════════════════════════════════════════
# FIGURE
# ═══════════════════════════════════════════════════════════════
fig, axes = plt.subplots(1, 2, figsize=(14, 6))
fig.suptitle("NSE modèle et écart NSE vs distance station insitu",
             fontsize=13, fontweight='bold')

def add_trendline(ax, x, y, color='tomato'):
    mask = ~(np.isnan(x) | np.isnan(y))
    if mask.sum() < 3:
        return np.nan
    z = np.polyfit(x[mask], y[mask], 1)
    p = np.poly1d(z)
    xl = np.linspace(x[mask].min(), x[mask].max(), 100)
    ax.plot(xl, p(xl), color=color, lw=2, ls='--', label='Tendance')
    r = np.corrcoef(x[mask], y[mask])[0, 1]
    return r

# ── Graphe 1 : NSE modèle vs distance ───────────────────────────────────
ax1 = axes[0]
sc1 = ax1.scatter(df['dist_km'], df['nse_model'],
                  c=df['nse_model'], cmap='RdYlGn', vmin=-0.5, vmax=1.0,
                  s=80, edgecolors='white', linewidths=0.5, zorder=3)
r1 = add_trendline(ax1, df['dist_km'].values, df['nse_model'].values)
plt.colorbar(sc1, ax=ax1, label='NSE modèle')
ax1.axhline(0,   color='gray', lw=0.8, ls=':')
ax1.axhline(0.5, color='steelblue', lw=0.8, ls=':', alpha=0.5, label='NSE=0.5')
ax1.set_xlabel("Distance station insitu (km)", fontsize=11)
ax1.set_ylabel("NSE modèle zero-shot", fontsize=11)
ax1.set_title(f"NSE modèle vs distance  (r={r1:.3f})", fontsize=11)
ax1.legend(fontsize=9)
ax1.grid(True, alpha=0.3)

# Annotations stations extrêmes
for _, row in df.iterrows():
    if row['nse_model'] < 0.1 or row['dist_km'] > 20:
        ax1.annotate(row['station'][-6:],
                     (row['dist_km'], row['nse_model']),
                     textcoords="offset points", xytext=(5, 4),
                     fontsize=7, color='gray')

# ── Graphe 2 : delta NSE vs distance ────────────────────────────────────
ax2 = axes[1]
colors_delta = ['#2196F3' if d >= 0 else '#FF5722' for d in df['delta_nse']]
sc2 = ax2.scatter(df['dist_km'], df['delta_nse'],
                  c=df['delta_nse'], cmap='RdYlGn', vmin=-1.0, vmax=1.0,
                  s=80, edgecolors='white', linewidths=0.5, zorder=3)
r2 = add_trendline(ax2, df['dist_km'].values, df['delta_nse'].values)
plt.colorbar(sc2, ax=ax2, label='Δ NSE (modèle − insitu)')
ax2.axhline(0, color='gray', lw=1.2, ls='-', alpha=0.5)
ax2.set_xlabel("Distance station insitu (km)", fontsize=11)
ax2.set_ylabel("Δ NSE  (modèle − insitu)", fontsize=11)
ax2.set_title(f"Δ NSE vs distance  (r={r2:.3f})", fontsize=11)
ax2.legend(fontsize=9)
ax2.grid(True, alpha=0.3)

# Annotations
for _, row in df.iterrows():
    if abs(row['delta_nse']) > 0.6 or row['dist_km'] > 20:
        ax2.annotate(row['station'][-6:],
                     (row['dist_km'], row['delta_nse']),
                     textcoords="offset points", xytext=(5, 4),
                     fontsize=7, color='gray')

# ── Stats terminales ─────────────────────────────────────────────────────
print("\n" + "="*55)
print(f"  n stations avec insitu     : {len(df)}")
print(f"  NSE modèle médian          : {df['nse_model'].median():.3f}")
print(f"  NSE insitu médian          : {df['nse_insitu'].median():.3f}")
print(f"  Δ NSE médian               : {df['delta_nse'].median():.3f}")
print(f"  Distance médiane (km)      : {df['dist_km'].median():.1f}")
print(f"  r(dist, NSE modèle)        : {r1:.3f}")
print(f"  r(dist, Δ NSE)             : {r2:.3f}")
print(f"  Modèle > insitu (ΔNSE > 0) : {(df['delta_nse'] > 0).sum()}/{len(df)}")
print("="*55)

plt.tight_layout()
plt.savefig("./figures_nse_vs_distance.png", dpi=150, bbox_inches='tight')
print("\n✅ Figure : ./figures_nse_vs_distance.png")
plt.show()