"""
Diagnostic visuel du scale break
=================================
Pour chaque station, affiche :
  - La courbe F2(s) en log-log
  - La pente locale glissante (dérivée de log F2 / log s)
  - La dérivée seconde lissée

Le scale break est là où la pente locale chute brutalement
et où la dérivée seconde est minimale.
Ça permet de valider (ou corriger) la détection automatique.
"""

import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
from scipy.signal import savgol_filter

DB_PATH  = './data/insitu_data.db'
DOSSIER  = Path('./data/Bassin_Versants')
DOSSIER.mkdir(parents=True, exist_ok=True)

POINTS_PAR_JOUR = 3

STATIONS = {
    'O184402001': 'Lèze ~100 km²',
    'Q124001001': 'Adour ~1347 km²',
    'U331001001': 'Saône ~9684 km²',
    'A375005050': 'Rhin ~21319 km²',
}


# ── Chargement + désaisonnalisation (identique au script principal) ──

def charger_signal(code_sta):
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query('''
        SELECT date, h_01h_wsh, h_09h_wsh, h_17h_wsh
        FROM mesures_insitu WHERE code_sta = ? ORDER BY date
    ''', conn, params=(code_sta,))
    aire = pd.read_sql_query(
        'SELECT aire_km2 FROM bv_data WHERE code_sta = ?',
        conn, params=(code_sta,))
    conn.close()

    df['date'] = pd.to_datetime(df['date'])
    series = []
    for col, h in [('h_01h_wsh',1),('h_09h_wsh',9),('h_17h_wsh',17)]:
        tmp = df[['date',col]].copy()
        tmp['datetime'] = tmp['date'] + pd.to_timedelta(h, unit='h')
        tmp = tmp.rename(columns={col:'niveau'})[['datetime','niveau']]
        series.append(tmp)

    serie = pd.concat(series).sort_values('datetime').set_index('datetime')
    serie['niveau'] = serie['niveau'].interpolate(method='linear', limit=5)
    serie['mois']  = serie.index.month
    serie['jour']  = serie.index.day
    serie['heure'] = serie.index.hour
    moy = serie.groupby(['mois','jour','heure'])['niveau'].mean().rename('niveau_moyen')
    serie = serie.join(moy, on=['mois','jour','heure'])
    serie['residuel'] = serie['niveau'] - serie['niveau_moyen']

    a = float(aire['aire_km2'].iloc[0]) if len(aire) > 0 else None
    return serie['residuel'].dropna().values, a


# ── DFA ──

def dfa(signal, scales, order=2):
    N = len(signal)
    Y = np.cumsum(signal - signal.mean())
    F2 = np.zeros(len(scales))
    for idx, s in enumerate(scales):
        if s > N // 4:
            F2[idx] = np.nan
            continue
        n_seg = N // s
        residus = []
        for direction in [1, -1]:
            seg_Y = Y if direction == 1 else Y[::-1]
            for i in range(n_seg):
                seg = seg_Y[i*s:(i+1)*s]
                x = np.arange(s)
                trend = np.polyval(np.polyfit(x, seg, order), x)
                residus.append(np.mean((seg - trend)**2))
        F2[idx] = np.sqrt(np.mean(residus))
    return F2


# ── Pente locale glissante ──

def pente_locale(s_log, F_log, fenetre=5):
    """
    Calcule la pente locale de log(F2) en fonction de log(s)
    sur une fenêtre glissante de `fenetre` points.
    C'est une estimation point par point de h(2) local.
    Si la courbe avait un seul régime, cette pente serait constante.
    Le scale break est là où elle change brutalement.
    """
    pentes = np.full(len(s_log), np.nan)
    demi = fenetre // 2
    for i in range(demi, len(s_log) - demi):
        x = s_log[i-demi : i+demi+1]
        y = F_log[i-demi : i+demi+1]
        pentes[i] = np.polyfit(x, y, 1)[0]
    return pentes


# ── Graphique de diagnostic ──

def tracer_diagnostic(code_sta, label):
    signal, aire = charger_signal(code_sta)

    scales = np.unique(np.round(
        np.logspace(
            np.log10(POINTS_PAR_JOUR * 1),
            np.log10(POINTS_PAR_JOUR * 730),
            80   # plus de points pour mieux voir la courbure
        )
    ).astype(int))

    F2 = dfa(signal, scales, order=2)
    scales_j = scales / POINTS_PAR_JOUR

    masque = ~np.isnan(F2) & (F2 > 0) & (scales_j >= 2)
    s_val  = scales_j[masque]
    F_val  = F2[masque]
    s_log  = np.log10(s_val)
    F_log  = np.log10(F_val)

    # Pente locale
    pentes = pente_locale(s_log, F_log, fenetre=5)

    # Dérivée seconde lissée (= variation de la pente)
    valides = ~np.isnan(pentes)
    s_log_v = s_log[valides]
    p_v     = pentes[valides]
    wl = min(9, len(p_v) if len(p_v) % 2 == 1 else len(p_v)-1)
    p_lisse  = savgol_filter(p_v, window_length=wl, polyorder=2)
    d_pente  = np.gradient(p_lisse, s_log_v)  # variation de la pente locale

    # Minimum de d_pente dans [5, 120] jours = scale break candidat
    fenetre_break = (10**s_log_v >= 5) & (10**s_log_v <= 120)
    if fenetre_break.sum() > 0:
        idx_break   = np.argmin(d_pente[fenetre_break])
        s_log_f     = s_log_v[fenetre_break]
        scale_break = 10**s_log_f[idx_break]
    else:
        scale_break = None

    # ── Figure 3 panneaux ──
    fig, axes = plt.subplots(3, 1, figsize=(10, 11), sharex=False)
    titre = f'Diagnostic scale break — {code_sta} ({label})'
    if aire:
        titre += f' — {aire:.0f} km²'
    fig.suptitle(titre, fontsize=11, fontweight='bold')

    # Panneau 1 : F2(s) en log-log
    ax = axes[0]
    ax.scatter(s_val, F_val, s=12, color='#444444', alpha=0.7)
    if scale_break:
        ax.axvline(scale_break, color='#d95f02', lw=1.5, ls='--',
                   label=f'Scale break candidat : {scale_break:.1f} j')
        ax.legend(fontsize=9)
    ax.set_xscale('log'); ax.set_yscale('log')
    ax.set_ylabel('F2(s)')
    ax.set_title('Courbe F2(s) — cherche le coude visuel', fontsize=9)
    ax.grid(True, which='both', alpha=0.2)

    # Panneau 2 : pente locale (= h local)
    ax = axes[1]
    ax.plot(10**s_log_v, p_v,     color='#aaaaaa', lw=0.8, alpha=0.6, label='brut')
    ax.plot(10**s_log_v, p_lisse, color='#1b9e77', lw=2,   label='lissé')
    ax.axhline(0.5, color='lightgray', lw=1, ls=':', label='h=0.5 (bruit blanc)')
    ax.axhline(1.0, color='lightgray', lw=1, ls='-.', label='h=1.0 (bruit rose)')
    if scale_break:
        ax.axvline(scale_break, color='#d95f02', lw=1.5, ls='--')
    ax.set_xscale('log')
    ax.set_ylabel('Pente locale h(s)')
    ax.set_title('Pente locale — doit chuter au niveau du scale break', fontsize=9)
    ax.legend(fontsize=8, ncol=2)
    ax.grid(True, which='both', alpha=0.2)

    # Panneau 3 : variation de la pente (dérivée de la pente)
    ax = axes[2]
    ax.plot(10**s_log_v, d_pente, color='#534AB7', lw=1.5)
    ax.axhline(0, color='gray', lw=0.8, ls='--', alpha=0.5)
    if scale_break:
        ax.axvline(scale_break, color='#d95f02', lw=1.5, ls='--',
                   label=f'Minimum → scale break : {scale_break:.1f} j')
        ax.legend(fontsize=9)
    ax.fill_between(10**s_log_v, d_pente, 0,
                    where=d_pente < 0, alpha=0.15, color='#534AB7',
                    label='Zone de transition')
    ax.set_xscale('log')
    ax.set_xlabel('Échelle s (jours)')
    ax.set_ylabel('Δ pente locale')
    ax.set_title('Variation de la pente — minimum = scale break', fontsize=9)
    ax.grid(True, which='both', alpha=0.2)

    plt.tight_layout()
    chemin = DOSSIER / f'diagnostic_break_{code_sta}.png'
    plt.savefig(chemin, dpi=150, bbox_inches='tight')
    plt.close()

    sb_str = f'{scale_break:.1f} j' if scale_break else 'non détecté'
    print(f"  {code_sta} ({label:20s}) — scale break candidat : {sb_str}")
    print(f"  Graphique → {chemin}")
    return scale_break


# ── Main ──

if __name__ == '__main__':
    print("Diagnostic scale break — pente locale glissante\n")
    breaks = {}
    for code_sta, label in STATIONS.items():
        sb = tracer_diagnostic(code_sta, label)
        breaks[code_sta] = sb

    print("\nRésumé :")
    for code, sb in breaks.items():
        print(f"  {code} : {sb:.1f} j" if sb else f"  {code} : non détecté")
    print("\nRegarde les graphiques et dis-moi où tu vois le coude visuellement.")
    print("On ajustera ensuite la fenêtre [s_min, s_max] en conséquence.")