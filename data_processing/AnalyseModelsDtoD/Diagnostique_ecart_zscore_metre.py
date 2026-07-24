"""
diagnostic_obs_reconstruit_vs_alti_brute.py
════════════════════════════════════════════════════════════════════════
Test direct : la colonne `obs` du CSV de résidus, une fois reconstruite en
mètres (obs_zscore * std_alti + mean_alti), doit retomber EXACTEMENT sur la
mesure alti brute lue dans dahiti.db, puisque c'est censé être la même
mesure physique, juste passée par deux chemins différents.

Sur la station 13412 (27j, DtoD90%), on observe à l'œil un écart qui
RÉTRÉCIT systématiquement la valeur reconstruite vers zéro (pics moins
hauts, creux moins bas), peu importe le sens de la pente locale.

Pourquoi ça exclut un simple décalage de date :
  un décalage temporel change de SIGNE selon la pente locale (regarder
  "trop tôt" sous-estime en montée, surestime en descente). Un
  rétrécissement vers la moyenne dans LES DEUX sens (pics ET creux) n'a
  pas cette signature -> ça pointe vers un facteur d'échelle (std) trop
  petit dans la reconstruction, pas un problème d'alignement temporel.

Ce script calcule, pour chaque date appariée exactement (pas de fenêtre de
tolérance) :
  - alti_brute_m       : lue direction depuis measurements.orthometric_height
  - obs_reconstruit     : obs_zscore (CSV résidus) * std_alti + mean_alti
  - diff_m, ratio       : pour juger échelle vs date
  - diff_relatif_anomalie : (valeur - moyenne) comparée entre les deux
    sources, signé -> si TOUJOURS plus proche de 0 côté reconstruit,
    quel que soit le signe de l'anomalie, c'est la signature du
    rétrécissement (std trop petit), pas d'un décalage de date.

Usage :
    python diagnostic_obs_reconstruit_vs_alti_brute.py
════════════════════════════════════════════════════════════════════════
"""

import sqlite3
import pandas as pd
import numpy as np

CODE_ALTI = "0000000013412"
CODE_ALTI_COURT = "13412"
FREQ = "27j"
MASK = 90   # d'après le titre du plot : DtoD90%

SAT_DB = "./data/dahiti.db"
RESIDUALS_CSV = f"./data_processing/AnalyseModelsDtoD/residuals/residuals_dahiti_{FREQ}_{MASK}pct.csv"


def main():
    # --- 1. Alti brute, directe, depuis la DB (panel 1 du plot) ---
    conn = sqlite3.connect(SAT_DB)
    df_alti = pd.read_sql("""
        SELECT measure_date AS date, orthometric_height AS wl
        FROM measurements
        WHERE station_code = ? AND is_valid = 1
        ORDER BY measure_date
    """, conn, params=(CODE_ALTI,))
    conn.close()
    df_alti["date"] = pd.to_datetime(df_alti["date"])

    mean_alti = df_alti["wl"].mean()
    std_alti = df_alti["wl"].std()
    print(f"mean_alti recalculé = {mean_alti:.4f}   std_alti recalculé = {std_alti:.4f}")
    print(f"n_obs_alti (toute la série) = {len(df_alti)}\n")

    # --- 2. obs reconstruite depuis le CSV de résidus (panel 3, points bleus) ---
    df_res = pd.read_csv(RESIDUALS_CSV)
    df_res["date"] = pd.to_datetime(df_res["date"])
    df_res["station"] = df_res["station"].astype(str)
    sub = df_res[df_res["station"] == CODE_ALTI_COURT].sort_values("date")
    sub = sub[sub["obs"].notna()].copy()
    sub["obs_reconstruit"] = sub["obs"] * std_alti + mean_alti

    # --- 3. Jointure EXACTE sur la date (pas de fenêtre de tolérance) ---
    merged = pd.merge(
        df_alti.rename(columns={"wl": "alti_brute_m"}),
        sub[["date", "obs", "obs_reconstruit"]],
        on="date", how="inner"
    )

    if merged.empty:
        print("⚠ AUCUNE date appariée exactement entre l'alti brute et le CSV de "
              "résidus -> décalage de date à confirmer, ou format de date incompatible.")
        return

    merged["diff_m"] = merged["alti_brute_m"] - merged["obs_reconstruit"]
    merged["ratio"] = merged["alti_brute_m"] / merged["obs_reconstruit"]

    # --- 4. Test du sens : rétrécissement vers la moyenne (échelle) vs lag (date) ---
    # anomalie = écart à la moyenne recalculée, dans chaque source
    merged["anomalie_brute"] = merged["alti_brute_m"] - mean_alti
    merged["anomalie_reconstruite"] = merged["obs_reconstruit"] - mean_alti
    # si |anomalie_reconstruite| < |anomalie_brute| TOUJOURS, peu importe le
    # signe -> rétrécissement uniforme -> std trop petit dans la reconstruction
    merged["retrecissement"] = merged["anomalie_reconstruite"].abs() < merged["anomalie_brute"].abs()

    pd.set_option("display.float_format", lambda x: f"{x:.3f}")
    print(merged[["date", "alti_brute_m", "obs_reconstruit", "diff_m", "ratio",
                  "anomalie_brute", "anomalie_reconstruite", "retrecissement"]].to_string(index=False))

    print(f"\nNb de dates appariées exactement : {len(merged)}")
    print(f"ratio  médian = {merged['ratio'].median():.3f}   std = {merged['ratio'].std():.3f}")
    pct_retrecissement = merged["retrecissement"].mean() * 100
    print(f"\n% de points où la valeur reconstruite est PLUS PROCHE DE LA MOYENNE "
          f"que la vraie valeur (peu importe le signe) : {pct_retrecissement:.0f}%")
    print("  -> proche de 100% sur les deux signes (pics ET creux) = signature d'un")
    print("     facteur d'échelle (std) trop petit dans la reconstruction.")
    print("     Un simple décalage de date donnerait un pourcentage proche de 50%,")
    print("     car son effet change de signe selon la pente locale (montée/descente).")


if __name__ == "__main__":
    main()