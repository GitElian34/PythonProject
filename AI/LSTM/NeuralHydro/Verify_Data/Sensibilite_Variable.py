#!/usr/bin/env python3
"""
Corrélation NSE par station vs attributs statiques — EA-LSTM run 2304_145549
Répond à : quels attributs statiques sont liés aux bonnes/mauvaises performances ?
"""

import pickle
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy import stats
from pathlib import Path

# ─── Chemins ────────────────────────────────────────────────────────────────
RUN_DIR   = Path("./runs/satellite_water_level_test_2304_145549")
EPOCH     = 20
RESULTS_P = RUN_DIR / f"validation/model_epoch{EPOCH:03d}/validation_results.p"
ATTRS_CSV = Path("./data/IA/NeuralHydrology/attributes/attributes.csv")

# ─── 1. Charger NSE/KGE par station ─────────────────────────────────────────
print("📂 Chargement des résultats...")
with open(RESULTS_P, "rb") as f:
    results = pickle.load(f)

records = []
for station_id, content in results.items():
    d       = content['1D']
    nse_val = float(np.squeeze(d.get('NSE', np.nan)))
    kge_val = float(np.squeeze(d.get('KGE', np.nan)))
    records.append({"station_id": station_id, "nse": nse_val, "kge": kge_val})

df_metrics = pd.DataFrame(records)
print(f"   {len(df_metrics)} stations | NSE médian = {df_metrics['nse'].median():.3f}")

# ─── 2. Charger les attributs statiques ──────────────────────────────────────
print("📂 Chargement des attributs...")
attrs = pd.read_csv(ATTRS_CSV)
attrs["station_id"] = attrs["station_id"].astype(str)

# ─── 3. Joindre NSE + attributs ──────────────────────────────────────────────
df = df_metrics.merge(attrs, on="station_id", how="inner")
df = df.dropna(subset=["nse"])
print(f"   {len(df)} stations après jointure")

# Colonnes attributs statiques — on exclut station_id, nse, kge
ATTR_COLS = [c for c in attrs.columns if c != "station_id"]
print(f"   {len(ATTR_COLS)} attributs : {ATTR_COLS}")

# ─── 4. Corrélation de Spearman NSE ~ chaque attribut ────────────────────────
# On utilise Spearman (robuste aux outliers et distributions non-normales)
# plutôt que Pearson
print("\n🔢 Calcul des corrélations Spearman (NSE ~ attribut)...")

corr_results = []
for col in ATTR_COLS:
    sub = df[["nse", col]].dropna()
    if len(sub) < 10 or sub[col].std() == 0:
        continue
    r, p = stats.spearmanr(sub["nse"], sub[col])
    corr_results.append({
        "attribut"  : col,
        "spearman_r": round(r, 4),
        "p_value"   : round(p, 4),
        "significatif": "✅" if p < 0.05 else "❌",
        "n"         : len(sub)
    })

df_corr = pd.DataFrame(corr_results).sort_values("spearman_r", key=abs, ascending=False)

print("\n📊 Corrélations Spearman (NSE ~ attribut statique) :")
print(f"{'Attribut':<22} {'r':>8} {'p-value':>10} {'Sig':>5}")
print("-" * 50)
for _, row in df_corr.iterrows():
    print(f"{row['attribut']:<22} {row['spearman_r']:>8.3f} {row['p_value']:>10.4f} {row['significatif']:>5}")

# ─── 5. Visualisations ───────────────────────────────────────────────────────
n_attrs = len(df_corr)
fig, axes = plt.subplots(1, 2, figsize=(16, 6))
fig.suptitle(
    f"Impact des attributs statiques sur NSE — EA-LSTM / Epoch {EPOCH}\n"
    f"NSE médian = {df['nse'].median():.3f}  |  N stations = {len(df)}",
    fontsize=13, fontweight="bold"
)

# ── 5a. Barplot corrélations ──────────────────────────────────────────────────
ax = axes[0]
colors = ["#22c55e" if r > 0 else "#ef4444" for r in df_corr["spearman_r"]]
hatches = ["" if p < 0.05 else "///" for p in df_corr["p_value"]]
bars = ax.barh(df_corr["attribut"], df_corr["spearman_r"],
               color=colors, edgecolor="white", linewidth=0.5)

# Hachures pour les corrélations non significatives
for bar, hatch in zip(bars, hatches):
    bar.set_hatch(hatch)

ax.axvline(0, color="black", linewidth=1)
ax.axvline(0.3,  color="green", linewidth=0.8, linestyle="--", alpha=0.4)
ax.axvline(-0.3, color="red",   linewidth=0.8, linestyle="--", alpha=0.4)
ax.set_xlabel("Corrélation de Spearman (r)")
ax.set_title("Corrélation NSE ~ attribut\n(hachuré = non significatif p>0.05)", fontweight="bold")
ax.spines[["top", "right"]].set_visible(False)

# ── 5b. Scatter NSE ~ attribut le plus corrélé ───────────────────────────────
ax      = axes[1]
best    = df_corr.iloc[0]["attribut"]
best_r  = df_corr.iloc[0]["spearman_r"]
best_p  = df_corr.iloc[0]["p_value"]

sub = df[["nse", best, "station_id"]].dropna()
ax.scatter(sub[best], sub["nse"], alpha=0.6, s=30, color="#3b82f6", edgecolors="white", linewidths=0.3)

# Droite de tendance
z    = np.polyfit(sub[best], sub["nse"], 1)
xfit = np.linspace(sub[best].min(), sub[best].max(), 100)
ax.plot(xfit, np.poly1d(z)(xfit), "r--", linewidth=1.5,
        label=f"r={best_r:.3f}, p={best_p:.4f}")
ax.axhline(0, color="gray", linewidth=0.8, linestyle="--", alpha=0.5)

ax.set_xlabel(best)
ax.set_ylabel("NSE")
ax.set_title(f"NSE ~ {best} (attribut le plus corrélé)", fontweight="bold")
ax.legend(fontsize=10)
ax.spines[["top", "right"]].set_visible(False)

plt.tight_layout()
out_png = f"corr_nse_attrs_ealstm_epoch{EPOCH}.png"
plt.savefig(out_png, dpi=150, bbox_inches="tight")
print(f"\n✅ Figure sauvegardée : {out_png}")
plt.show()

# ─── 6. Export CSV ───────────────────────────────────────────────────────────
out_csv = f"corr_nse_attrs_ealstm_epoch{EPOCH}.csv"
df_corr.to_csv(out_csv, index=False)
print(f"✅ CSV sauvegardé : {out_csv}")

# ─── 7. Résumé texte ─────────────────────────────────────────────────────────
sig = df_corr[df_corr["p_value"] < 0.05]
pos = sig[sig["spearman_r"] > 0]
neg = sig[sig["spearman_r"] < 0]

print(f"\n📋 Résumé :")
print(f"   {len(sig)} attributs significatifs (p<0.05) sur {len(df_corr)}")
if not pos.empty:
    print(f"\n   ✅ Corrélation POSITIVE (plus de cet attribut → meilleur NSE) :")
    for _, r in pos.iterrows():
        print(f"      {r['attribut']:<22} r = {r['spearman_r']:+.3f}")
if not neg.empty:
    print(f"\n   ❌ Corrélation NÉGATIVE (plus de cet attribut → pire NSE) :")
    for _, r in neg.iterrows():
        print(f"      {r['attribut']:<22} r = {r['spearman_r']:+.3f}")