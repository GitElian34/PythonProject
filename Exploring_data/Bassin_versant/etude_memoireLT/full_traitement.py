"""
Analyse DFA — données journalières (h_med_wsh)
===============================================
Workflow en deux passes :

  PASSE 1 : laisser SCALE_BREAKS à None pour toutes les stations
            → génère les courbes F2(s)
            → inspecter visuellement les graphiques
            → identifier le coude sur chaque courbe

  PASSE 2 : renseigner SCALE_BREAKS avec les valeurs lues sur les graphiques
            → relancer le script
            → calcule h(2) court/long terme + synthèse comparative
"""

import sqlite3
import warnings
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

from numpy.exceptions import RankWarning
from scipy.stats import linregress

DB_PATH  = './data/insitu_data.db'
DOSSIER  = Path('./data/Bassin_Versants')
DOSSIER.mkdir(parents=True, exist_ok=True)


# ═══════════════════════════════════════════════════════════════
# !! À REMPLIR APRÈS INSPECTION VISUELLE DES COURBES F2(s) !!
#
# Pour chaque station, noter le nombre de jours où tu vois
# le coude sur la courbe F2(s) (panneau du haut du graphique).
# Laisser None si pas encore inspecté → courbe générée sans h(2)
# ═══════════════════════════════════════════════════════════════

SCALE_BREAKS = {
    'M410191010': None,  # ~10000 km²
    'M410191050':  12,  # ~10000 km²
    'K338201003':  10,  # ~1000km²
    'Y531201001': 9,  # ~1000 km²
    'Q218000101':  5, # 300 km2
    'M034151010':  10, # ~300 km2
}


# ═══════════════════════════════════════════════════════════════
# 1. CHARGEMENT ET DÉSAISONNALISATION (journalier)
# ═══════════════════════════════════════════════════════════════

def charger_et_desaisonnaliser(code_sta):
    """
    Charge h_med_wsh (médiane journalière) depuis la BDD.
    Une ligne par jour — pas de conversion en format long.
    Désaisonnalisation sur 365 cases (mois, jour) au lieu de 1095.
    """
    conn = sqlite3.connect(DB_PATH)

    df = pd.read_sql_query('''
        SELECT date, h_med_wsh
        FROM mesures_insitu
        WHERE code_sta = ?
        ORDER BY date
    ''', conn, params=(code_sta,))

    res_aire = pd.read_sql_query(
        'SELECT aire_km2 FROM bv_data WHERE code_sta = ?',
        conn, params=(code_sta,))
    res_nom = pd.read_sql_query(
        'SELECT river_name FROM stations_insitu WHERE code_sta = ?',
        conn, params=(code_sta,))
    conn.close()

    if df.empty:
        print(f"  [!] Aucune donnée pour {code_sta}")
        return None, None, None

    aire       = float(res_aire['aire_km2'].iloc[0]) if len(res_aire) > 0 else None
    river_name = res_nom['river_name'].iloc[0] if len(res_nom) > 0 else ''

    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date').sort_index()

    # Valeurs manquantes
    n_manquant = df['h_med_wsh'].isna().sum()
    df['h_med_wsh'] = df['h_med_wsh'].interpolate(
        method='linear', limit=5, limit_direction='both')

    # Désaisonnalisation : moyenne par (mois, jour) sur toutes les années
    df['mois'] = df.index.month
    df['jour'] = df.index.day

    moy = (df.groupby(['mois', 'jour'])['h_med_wsh']
             .mean().rename('niveau_moyen'))

    df = df.join(moy, on=['mois', 'jour'])
    df['residuel'] = df['h_med_wsh'] - df['niveau_moyen']

    print(f"  Rivière   : {river_name}")
    print(f"  Aire BV   : {aire:.0f} km²" if aire else "  Aire BV  : inconnue")
    print(f"  Jours     : {len(df)} — Manquants : {n_manquant} ({100*n_manquant/len(df):.1f}%)")
    print(f"  Résidu σ  : {df['residuel'].std():.4f} m")

    return df[['h_med_wsh', 'niveau_moyen', 'residuel']], aire, river_name


# ═══════════════════════════════════════════════════════════════
# 2. DFA
# ═══════════════════════════════════════════════════════════════

def dfa(signal, scales, order=2):
    """DFA ordre 2 (retire les tendances linéaires locales)."""
    N  = len(signal)
    Y  = np.cumsum(signal - signal.mean())
    F2 = np.zeros(len(scales))
    for idx, s in enumerate(scales):
        if s > N // 4:
            F2[idx] = np.nan
            continue
        n_seg   = N // s
        residus = []
        for direction in [1, -1]:
            seg_Y = Y if direction == 1 else Y[::-1]
            for i in range(n_seg):
                seg   = seg_Y[i*s:(i+1)*s]
                x     = np.arange(s)
                with warnings.catch_warnings():
                    warnings.simplefilter('ignore', RankWarning)
                    trend = np.polyval(np.polyfit(x, seg, order), x)
                residus.append(np.mean((seg - trend)**2))
        F2[idx] = np.sqrt(np.mean(residus))
    return F2


def calculer_dfa(signal):
    """
    Lance la DFA sur des échelles de 2 à N/4 jours.
    Retourne s_val (jours) et F_val.
    """
    n_jours = len(signal)
    scales  = np.unique(np.round(
        np.logspace(
            np.log10(2),              # 2 jours minimum
            np.log10(n_jours // 4),   # N/4 jours maximum
            60
        )
    ).astype(int))

    F2      = dfa(signal, scales, order=2)
    masque  = ~np.isnan(F2) & (F2 > 0) & (scales >= 4)
    s_val   = scales[masque].astype(float)
    F_val   = F2[masque]

    return s_val, F_val


def calculer_h2(s_val, F_val, break_jours):
    """
    Calcule h(2) court et long terme à partir du scale break fourni.
    Retourne un dict avec les résultats des deux régressions.
    """
    m_court = s_val <= break_jours
    m_long  = s_val >= break_jours

    if m_court.sum() < 3 or m_long.sum() < 3:
        print(f"  [!] Pas assez de points autour du break {break_jours}j")
        return None

    sl_ct, ic_ct, r_ct, *_ = linregress(
        np.log10(s_val[m_court]), np.log10(F_val[m_court]))
    sl_lt, ic_lt, r_lt, *_ = linregress(
        np.log10(s_val[m_long]),  np.log10(F_val[m_long]))

    return {
        'h2_court': sl_ct, 'r2_court': r_ct**2, 'ic_court': ic_ct,
        'h2_long' : sl_lt, 'r2_long' : r_lt**2, 'ic_long' : ic_lt,
        'm_court' : m_court, 'm_long': m_long,
    }


# ═══════════════════════════════════════════════════════════════
# 3. GRAPHIQUE F2(s)
# ═══════════════════════════════════════════════════════════════

def tracer_f2(code_sta, river_name, aire, s_val, F_val,
              break_jours=None, h2_res=None):
    """
    Trace la courbe F2(s) en log-log.

    Si break_jours=None (passe 1) :
      → courbe seule avec grille de lecture, pas de droites
      → message "à inspecter" sur le graphique

    Si break_jours renseigné (passe 2) :
      → droites court/long terme + h(2) + résumé
    """
    fig, ax = plt.subplots(figsize=(10, 6))

    titre = f'DFA — {code_sta}  ({river_name})'
    if aire:
        titre += f'  —  {aire:.0f} km²'
    fig.suptitle(titre, fontsize=12, fontweight='bold')

    # Courbe F2(s)
    ax.scatter(s_val, F_val, s=20, color='#444444', alpha=0.8,
               zorder=3, label='F2(s)')

    # Lignes de référence h=0.5 et h=1
    x_ref = np.array([s_val.min(), s_val.max()])
    c0    = F_val[0] / s_val[0]**0.5
    ax.plot(x_ref, c0 * x_ref**0.5, color='#aaaaaa', lw=1, ls=':',
            label='référence h=0.5 (bruit blanc)')
    ax.plot(x_ref, c0 * x_ref**1.0, color='#aaaaaa', lw=1, ls='-.',
            label='référence h=1.0 (bruit rose)')

    if break_jours is None:
        # ── PASSE 1 : juste la courbe ──
        # Grille verticale pour faciliter la lecture du coude
        for nb_jours in [3, 5, 7, 10, 15, 20, 30, 60, 90, 180, 365]:
            if s_val.min() <= nb_jours <= s_val.max():
                ax.axvline(nb_jours, color='#dddddd', lw=0.5, ls='--', zorder=1)
                ax.text(nb_jours, ax.get_ylim()[0] if ax.get_ylim()[0] > 0 else F_val.min()*0.5,
                        f'{nb_jours}j', fontsize=7, color='#aaaaaa',
                        ha='center', va='bottom')

        ax.text(0.5, 0.97,
                "PASSE 1 — Identifie le coude visuellement\n"
                "puis renseigne SCALE_BREAKS dans le script",
                transform=ax.transAxes, fontsize=10, ha='center', va='top',
                bbox=dict(boxstyle='round', facecolor='#fff9c4', alpha=0.9))

    else:
        # ── PASSE 2 : droites + h(2) ──
        ax.axvline(break_jours, color='gray', lw=1.5, ls='--', alpha=0.8,
                   label=f'Scale break = {break_jours} j')

        if h2_res:
            sv, fv = s_val, F_val
            mc, ml = h2_res['m_court'], h2_res['m_long']

            s_ct = np.array([sv[mc].min(), sv[mc].max()])
            s_lt = np.array([sv[ml].min(), sv[ml].max()])

            ax.plot(s_ct,
                    10**(h2_res['ic_court'] + h2_res['h2_court']*np.log10(s_ct)),
                    color='#d95f02', lw=2,
                    label=f"Court terme  h={h2_res['h2_court']:.3f}  R²={h2_res['r2_court']:.3f}")
            ax.plot(s_lt,
                    10**(h2_res['ic_long'] + h2_res['h2_long']*np.log10(s_lt)),
                    color='#1b9e77', lw=2.5,
                    label=f"Long terme   h={h2_res['h2_long']:.3f}  R²={h2_res['r2_long']:.3f}")

            def interp(h):
                if h < 0.5:  return 'Pas de mémoire'
                if h < 0.75: return 'Mémoire faible'
                if h < 1.0:  return 'Mémoire forte'
                return 'Très forte (h≥1)'

            texte = (
                f"Scale break : {break_jours} j\n\n"
                f"Court terme\n"
                f"  h(2) = {h2_res['h2_court']:.3f}  R²={h2_res['r2_court']:.3f}\n\n"
                f"Long terme\n"
                f"  h(2) = {h2_res['h2_long']:.3f}  R²={h2_res['r2_long']:.3f}\n\n"
                f"→ {interp(h2_res['h2_long'])}"
            )
            ax.text(0.02, 0.97, texte, transform=ax.transAxes,
                    fontsize=9, va='top', fontfamily='monospace',
                    bbox=dict(boxstyle='round', facecolor='#f5f5f5', alpha=0.85))

    ax.set_xscale('log')
    ax.set_yscale('log')
    ax.set_xlabel('Échelle s (jours)', fontsize=10)
    ax.set_ylabel('F2(s)', fontsize=10)
    ax.legend(fontsize=8, loc='lower right')
    ax.grid(True, which='both', alpha=0.15)

    plt.tight_layout()
    chemin = DOSSIER / f'dfa_{code_sta}.png'
    plt.savefig(chemin, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  Graphique → {chemin}")


# ═══════════════════════════════════════════════════════════════
# 4. SYNTHÈSE (passe 2 uniquement)
# ═══════════════════════════════════════════════════════════════

def sauver_synthese(synthese):
    df = pd.DataFrame(synthese).sort_values('aire_km2')
    chemin = DOSSIER / 'dfa_synthese.csv'
    df.to_csv(chemin, index=False)

    print(f"\n{'='*70}")
    print("  SYNTHÈSE COMPARATIVE")
    print(f"{'='*70}")
    print(df[['code_sta', 'river_name', 'aire_km2',
              'scale_break_j', 'h2_long', 'r2_long']].to_string(index=False))

    if len(df) >= 3:
        corr = df['aire_km2'].apply(np.log10).corr(df['h2_long'])
        print(f"\nCorrélation log(aire) / h(2) : {corr:.3f}")
        if corr > 0.5:
            print("→ Tendance confirmée : h(2) augmente avec l'aire")
        elif corr < -0.5:
            print("→ Tendance inverse : à investiguer")
        else:
            print("→ Pas de tendance claire sur cet échantillon")

    print(f"\nSynthèse → {chemin}")


# ═══════════════════════════════════════════════════════════════
# 5. PIPELINE PRINCIPAL
# ═══════════════════════════════════════════════════════════════

def main():
    passe2_complet = all(v is not None for v in SCALE_BREAKS.values())
    passe2_partiel = any(v is not None for v in SCALE_BREAKS.values())

    if not passe2_partiel:
        print("PASSE 1 — Génération des courbes F2(s) pour inspection visuelle\n")
    else:
        print("PASSE 2 — Calcul de h(2) avec les scale breaks renseignés\n")

    synthese = []

    for code_sta, break_jours in SCALE_BREAKS.items():
        print(f"\n{'='*55}")
        print(f"  {code_sta}"
              + (f"  — break = {break_jours} j" if break_jours else "  — à inspecter"))
        print(f"{'='*55}")

        # Chargement + désaisonnalisation
        df, aire, river_name = charger_et_desaisonnaliser(code_sta)
        if df is None:
            continue

        # Sauvegarde du résidu
        df.to_csv(DOSSIER / f'residuel_{code_sta}.csv')

        # DFA
        signal        = df['residuel'].dropna().values
        s_val, F_val  = calculer_dfa(signal)
        print(f"  DFA : {len(signal)} points, "
              f"échelles {s_val.min():.0f}–{s_val.max():.0f} j")

        # Calcul h(2) si break renseigné
        h2_res = None
        if break_jours is not None:
            h2_res = calculer_h2(s_val, F_val, break_jours)
            if h2_res:
                print(f"  h(2) court : {h2_res['h2_court']:.3f}  "
                      f"(R²={h2_res['r2_court']:.3f})")
                print(f"  h(2) long  : {h2_res['h2_long']:.3f}  "
                      f"(R²={h2_res['r2_long']:.3f})")

                synthese.append({
                    'code_sta'     : code_sta,
                    'river_name'   : river_name,
                    'aire_km2'     : aire,
                    'scale_break_j': break_jours,
                    'h2_court'     : round(h2_res['h2_court'], 3),
                    'r2_court'     : round(h2_res['r2_court'], 3),
                    'h2_long'      : round(h2_res['h2_long'],  3),
                    'r2_long'      : round(h2_res['r2_long'],  3),
                    'n_jours'      : len(signal),
                })
        else:
            print("  → Break non renseigné, courbe F2(s) générée pour inspection")

        # Graphique
        tracer_f2(code_sta, river_name, aire, s_val, F_val, break_jours, h2_res)

    # Synthèse uniquement si au moins une station complète
    if synthese:
        sauver_synthese(synthese)

    if not passe2_complet:
        print(f"\n{'='*55}")
        print("  PROCHAINE ÉTAPE")
        print(f"{'='*55}")
        print("  1. Ouvre les graphiques dfa_*.png dans :")
        print(f"     {DOSSIER}/")
        print("  2. Identifie le coude sur chaque courbe F2(s)")
        print("  3. Renseigne SCALE_BREAKS en haut de ce script")
        print("  4. Relance le script")


if __name__ == '__main__':
    main()