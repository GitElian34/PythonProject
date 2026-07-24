"""
plot_station_13412_zscore_vs_metres.py
════════════════════════════════════════════════════════════════════════
Test isolé, une seule station (DAHITI 0000000013412, freq 27j, modèle
DtoD90%, insitu M530001030 — celle du screenshot).

Objectif : reproduire la comparaison z-score / mètres avec une formule de
reconstruction qu'on a déjà VALIDÉE indépendamment (diagnostic précédent :
sur 87 dates appariées exactement, ratio alti_brute/obs_reconstruit =
0.993, std=0.001 -> la formule est juste).

Si ce script montre encore un modèle "à moitié" de l'alti en mètres (comme
dans le plot original), le problème est dans LA DONNÉE/le pipeline, pas
dans le script de plot. Si ce script montre un bon accord modèle/alti en
mètres (contrairement au plot original), le bug est dans le SCRIPT DE PLOT
ORIGINAL (top5_stations_alti_modele_insitu.py), pas dans les données.

Usage :
    python plot_station_13412_zscore_vs_metres.py
════════════════════════════════════════════════════════════════════════
"""

import sqlite3
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

CODE_ALTI = "0000000013412"
CODE_ALTI_COURT = "13412"
CODE_INSITU = "M530001030"   # déjà identifiée dans le screenshot, pas besoin de SWORD ici
FREQ = "27j"
MASK = 90
ANNEE = 2020

SAT_DB = "./data/dahiti.db"
INSITU_DB = "./data/insitu_data.db"
RESIDUALS_CSV = f"./data_processing/AnalyseModelsDtoD/residuals/residuals_dahiti_{FREQ}_{MASK}pct.csv"

DATE_MIN, DATE_MAX = "2016-01-01", "2025-12-31"


def zscore(arr):
    a = np.asarray(arr, dtype=float)
    m = ~np.isnan(a)
    if m.sum() < 2:
        return a * np.nan
    mu, sig = a[m].mean(), a[m].std()
    return (a - mu) / sig if sig > 0 else a * 0


def main():
    # ── 1. Alti brute (DB), jamais transformée ───────────────────────────
    conn = sqlite3.connect(SAT_DB)
    df_alti = pd.read_sql("""
        SELECT measure_date AS date, orthometric_height AS wl
        FROM measurements
        WHERE station_code = ? AND is_valid = 1
          AND measure_date >= ? AND measure_date <= ?
        ORDER BY measure_date
    """, conn, params=(CODE_ALTI, DATE_MIN, DATE_MAX))
    conn.close()
    df_alti["date"] = pd.to_datetime(df_alti["date"])

    mean_alti = df_alti["wl"].mean()
    std_alti = df_alti["wl"].std()
    print(f"mean_alti = {mean_alti:.4f}   std_alti = {std_alti:.4f}   "
          f"(n={len(df_alti)} mesures, toute la série)")

    # ── 2. Insitu brute (DB), jamais transformée ─────────────────────────
    conn = sqlite3.connect(INSITU_DB)
    df_ins = pd.read_sql("""
        SELECT date, h_med_wsh AS wl FROM mesures_insitu
        WHERE code_sta = ? AND date >= ? AND date <= ?
        ORDER BY date
    """, conn, params=(CODE_INSITU, DATE_MIN, DATE_MAX))
    conn.close()
    df_ins["date"] = pd.to_datetime(df_ins["date"])
    df_ins = df_ins.dropna(subset=["wl"])

    # ── 3. Résidus modèle : pred (z-score amont) + obs (z-score amont) ──
    df_res = pd.read_csv(RESIDUALS_CSV)
    df_res["date"] = pd.to_datetime(df_res["date"])
    df_res["station"] = df_res["station"].astype(str)
    sub = df_res[df_res["station"] == CODE_ALTI_COURT].sort_values("date").reset_index(drop=True)
    sub = sub.dropna(subset=["pred"])

    # ⚠ RECONSTRUCTION VALIDÉE (diagnostic précédent : ratio=0.993, std=0.001)
    sub["pred_metres"] = sub["pred"].values * std_alti + mean_alti
    has_obs = sub["obs"].notna().values if "obs" in sub.columns else np.zeros(len(sub), dtype=bool)
    sub["obs_metres"] = np.where(has_obs, sub["obs"].values * std_alti + mean_alti, np.nan)

    # ── 4. Restriction à l'année demandée pour la lisibilité du plot ────
    m_alti = df_alti["date"].dt.year == ANNEE
    m_ins = df_ins["date"].dt.year == ANNEE
    m_mod = sub["date"].dt.year == ANNEE

    # ── 4bis. Recalage VISUEL de l'insitu sur l'alti (décalage de datum) ──
    # Décalage = différence des médianes (insensible aux crues, donc ne
    # déforme pas l'amplitude). Aucune incidence sur les calculs d'écart
    # déjà faits ailleurs -> uniquement pour comparer les FORMES ici.
    med_alti = float(np.median(df_alti["wl"].values))
    med_ins = float(np.median(df_ins["wl"].values))
    offset_insitu = med_alti - med_ins
    df_ins_recale = df_ins.copy()
    df_ins_recale["wl_recale"] = df_ins_recale["wl"] + offset_insitu
    print(f"Décalage appliqué à l'insitu pour le recalage visuel : {offset_insitu:+.3f} m "
          f"(médiane alti={med_alti:.3f} - médiane insitu={med_ins:.3f})")

    # ── 5. Figure : 2 lignes (z-score / mètres) ──────────────────────────
    fig, (ax_z, ax_m) = plt.subplots(2, 1, figsize=(13, 9), sharex=True)

    # -- Panel z-score : chaque série normalisée indépendamment (sauf pred,
    #    déjà z-scorée en amont par le pipeline .nc -> utilisée telle quelle) --
    alti_z_full = zscore(df_alti["wl"].values)
    ins_z_full = zscore(df_ins["wl"].values)
    ax_z.plot(df_alti["date"][m_alti], alti_z_full[m_alti], "o-", color="#5B9BD5", ms=4, label="Alti (z-score, propre std)")
    ax_z.plot(sub["date"][m_mod], sub["pred"][m_mod], "-", color="#c0392b", lw=1.2, label="Modèle pred (déjà z-score, brut du CSV)")
    ax_z.plot(df_ins["date"][m_ins], ins_z_full[m_ins], "-", color="#e67e22", lw=0.9, label="Insitu (z-score, propre std)")
    ax_z.set_title(f"Station DAHITI {CODE_ALTI} — {ANNEE} — Z-SCORE (chaque série normalisée indépendamment)",
                   fontsize=10, fontweight="bold")
    ax_z.set_ylabel("WL (z-score)")
    ax_z.axhline(0, color="grey", lw=0.6, ls="--")
    ax_z.legend(fontsize=8, loc="upper right")
    ax_z.grid(True, alpha=0.3)

    # -- Panel mètres : alti/insitu brutes, modèle reconstruit (formule validée) --
    ax_m.plot(df_alti["date"][m_alti], df_alti["wl"][m_alti], "o-", color="#5B9BD5", ms=4,
              label="Alti (brute, mètres, jamais transformée)")
    ax_m.plot(sub["date"][m_mod], sub["pred_metres"][m_mod], "-", color="#c0392b", lw=1.2,
              label="Modèle pred reconstruit (pred_z × std_alti + mean_alti)")
    obs_mask = has_obs & m_mod.values
    ax_m.scatter(sub["date"][obs_mask], sub["obs_metres"][obs_mask], color="#2c3e50", s=28, zorder=3,
                 label="obs reconstruite (même formule, jours satellite)")
    ax_m.plot(df_ins["date"][m_ins], df_ins_recale["wl_recale"][m_ins], "-", color="#e67e22", lw=0.9,
              label=f"Insitu (recalée visuellement, {offset_insitu:+.2f} m de décalage)")
    ax_m.set_title(f"Station DAHITI {CODE_ALTI} — {ANNEE} — MÈTRES, insitu recalée sur l'alti (datum)",
                   fontsize=10, fontweight="bold")
    ax_m.set_ylabel("WL (m)")
    ax_m.set_xlabel("Date")
    ax_m.legend(fontsize=8, loc="upper right")
    ax_m.grid(True, alpha=0.3)
    ax_m.xaxis.set_major_formatter(mdates.DateFormatter("%b"))
    ax_m.xaxis.set_major_locator(mdates.MonthLocator())
    plt.setp(ax_m.xaxis.get_majorticklabels(), rotation=30, ha="right")

    plt.tight_layout()
    out_path = f"./{CODE_ALTI}_{ANNEE}_zscore_vs_metres.png"
    fig.savefig(out_path, dpi=140, bbox_inches="tight")
    print(f"\nFigure sauvegardée -> {out_path}")
    print(f"\nInsitu recalée de {offset_insitu:+.3f} m (décalage de médiane, datum) -> les niveaux")
    print("sont maintenant comparables visuellement, seules les FORMES/AMPLITUDES comptent ici.")
    print("\nCe qu'il faut regarder :")
    print("  - les points noirs (obs reconstruite) DOIVENT coller sur la courbe bleue (alti brute)")
    print("    aux mêmes dates -> si oui, la formule est confirmée bonne (cohérent avec le diagnostic).")
    print("  - la courbe rouge (pred reconstruit) doit être comparée à la courbe bleue (alti) :")
    print("    si elle suit fidèlement en MÊME amplitude -> le bug du plot original venait du SCRIPT,")
    print("    pas de la donnée. Si elle reste 'à moitié' comme dans le plot original -> le problème")
    print("    est réel et vient des données/du modèle, pas d'un bug de plot.")


if __name__ == "__main__":
    main()