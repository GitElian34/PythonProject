import sqlite3
import numpy as np
import pandas as pd
import xarray as xr
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
DB_PATH   = "./data/insitu_data.db"
ERA5_BASE = "./data/ERA5/usable_data_LAND_France"
CODE_STA  = "H227000102"
DATE_DEB  = "2020-01-01"
DATE_FIN  = "2021-12-31"

TRANCHES = {
    '0-40km'   : (0,   40),
    '40-80km'  : (40,  80),
    '80-150km' : (80,  150),
    '150-300km': (150, 300),
    '>300km'   : (300, None)
}
COULEURS = {
    '0-40km'   : '#1a9850',
    '40-80km'  : '#91cf60',
    '80-150km' : '#fee08b',
    '150-300km': '#fc8d59',
    '>300km'   : '#d73027'
}

# 3 deltas × fenêtres ERA5 associées (heures avant l'heure de mesure)
# h_01h → mesure à 1h du matin
# h_09h → mesure à 9h
# h_17h → mesure à 17h
DELTAS = {
    'nuit (01h→09h)'  : {'h_deb': 'h_01h_wsh', 'h_fin': 'h_09h_wsh', 'ref_h': 9},
    'matin (09h→17h)' : {'h_deb': 'h_09h_wsh', 'h_fin': 'h_17h_wsh', 'ref_h': 17},
    'jour (01h→17h)'  : {'h_deb': 'h_01h_wsh', 'h_fin': 'h_17h_wsh', 'ref_h': 17},
}

# Blocs ERA5 de 4h sur 48h = 12 blocs
FENETRE_H = 4
NB_BLOCS  = 12  # 48h

# ═══════════════════════════════════════════════════════════════
# CHARGEMENT DB
# ═══════════════════════════════════════════════════════════════
conn = sqlite3.connect(DB_PATH)

row = pd.read_sql_query(
    "SELECT lon, lat, river_name FROM stations_insitu WHERE code_sta = ?",
    conn, params=(CODE_STA,)
).iloc[0]
lon_sta, lat_sta = row['lon'], row['lat']
river_name = row['river_name']
print(f"Station : {CODE_STA} — {river_name}")

pixels = pd.read_sql_query(
    "SELECT pixel_lon, pixel_lat, dist_km FROM era5_transfert WHERE code_sta = ?",
    conn, params=(CODE_STA,)
)

def tranche_label(dist_km):
    for label, (dmin, dmax) in TRANCHES.items():
        if dmax is None and dist_km >= dmin:
            return label
        if dmax and dmin <= dist_km < dmax:
            return label
    return '>300km'

pixels['tranche'] = pixels['dist_km'].apply(tranche_label)
print(f"Pixels par tranche :\n{pixels.groupby('tranche').size().rename('nb').to_string()}")

# Hauteurs d'eau
hauteurs = pd.read_sql_query('''
    SELECT date, h_01h_wsh, h_09h_wsh, h_17h_wsh
    FROM mesures_insitu
    WHERE code_sta = ?
      AND date >= ? AND date <= ?
    ORDER BY date
''', conn, params=(CODE_STA, DATE_DEB, DATE_FIN))
conn.close()

hauteurs['date'] = pd.to_datetime(hauteurs['date'])
hauteurs = hauteurs.dropna().reset_index(drop=True)
print(f"Hauteurs : {len(hauteurs)} jours")

# ═══════════════════════════════════════════════════════════════
# CHARGEMENT ERA5 HORAIRE
# ═══════════════════════════════════════════════════════════════
print("\nChargement ERA5 horaire...")
date_deb = datetime.strptime(DATE_DEB, "%Y-%m-%d") - timedelta(days=3)
date_fin = datetime.strptime(DATE_FIN, "%Y-%m-%d")

mois_a_charger = []
d = date_deb.replace(day=1)
while d <= date_fin:
    mois_a_charger.append((d.strftime('%Y'), d.strftime('%m')))
    d = (d + timedelta(days=32)).replace(day=1)

datasets = []
for annee, mois in mois_a_charger:
    path = f"{ERA5_BASE}/{annee}/{mois}/data_0.nc"
    try:
        ds = xr.open_dataset(path)
        datasets.append(ds['tp'] * 1000)
        print(f"  ✅ {annee}/{mois}")
    except FileNotFoundError:
        print(f"  ⚠️  {annee}/{mois} manquant")

tp_horaire = xr.concat(datasets, dim='valid_time').sortby('valid_time')
tp_diff    = tp_horaire.diff(dim='valid_time').clip(min=0)
print(f"ERA5 horaire : {len(tp_diff.valid_time)} heures")

# ═══════════════════════════════════════════════════════════════
# EXTRACTION PIXELS PAR TRANCHE
# ═══════════════════════════════════════════════════════════════
pixels_par_tranche = {}
for tranche in TRANCHES:
    px = pixels[pixels['tranche'] == tranche]
    if px.empty:
        continue
    lons = xr.DataArray(px['pixel_lon'].values, dims='pixel')
    lats = xr.DataArray(px['pixel_lat'].values, dims='pixel')
    vals = tp_diff.sel(longitude=lons, latitude=lats, method='nearest')
    pixels_par_tranche[tranche] = vals
    print(f"  {tranche} : {len(px)} pixels extraits")

# ═══════════════════════════════════════════════════════════════
# CALCUL FEATURES ET CORRÉLATIONS
# ═══════════════════════════════════════════════════════════════
tranches_list = [t for t in TRANCHES if t in pixels_par_tranche]

# Une matrice de corrélation par delta
resultats = {}

for delta_name, delta_cfg in DELTAS.items():
    print(f"\nCalcul delta : {delta_name}")
    records = []

    for _, row in hauteurs.iterrows():
        date_j  = row['date']
        h_deb   = row[delta_cfg['h_deb']]
        h_fin   = row[delta_cfg['h_fin']]
        delta_h = h_fin - h_deb

        if pd.isna(delta_h):
            continue

        record = {'date': date_j, 'delta_h': delta_h}

        # Heure de référence = heure de la mesure finale
        t_ref = pd.Timestamp(date_j) + timedelta(hours=delta_cfg['ref_h'])

        for tranche, vals in pixels_par_tranche.items():
            for bloc in range(NB_BLOCS):
                t_fin_bloc = t_ref - timedelta(hours=bloc * FENETRE_H)
                t_deb_bloc = t_ref - timedelta(hours=(bloc + 1) * FENETRE_H)
                try:
                    vals_bloc = vals.sel(valid_time=slice(t_deb_bloc, t_fin_bloc))
                    pluie = float(vals_bloc.sum(dim='valid_time').mean(dim='pixel').values)
                except Exception:
                    pluie = 0.0
                record[f'{tranche}_B{bloc}'] = pluie

        records.append(record)

    df = pd.DataFrame(records).dropna().reset_index(drop=True)

    corr_matrix = np.zeros((len(tranches_list), NB_BLOCS))
    for ti, tranche in enumerate(tranches_list):
        for bloc in range(NB_BLOCS):
            col = f'{tranche}_B{bloc}'
            if col not in df.columns:
                corr_matrix[ti, bloc] = np.nan
                continue
            corr = df[col].corr(df['delta_h'])
            corr_matrix[ti, bloc] = corr if not np.isnan(corr) else 0

    resultats[delta_name] = corr_matrix

# ═══════════════════════════════════════════════════════════════
# VISUALISATION
# ═══════════════════════════════════════════════════════════════
labels_x = [f'-{(b+1)*FENETRE_H}h→-{b*FENETRE_H}h' for b in range(NB_BLOCS)]

fig, axes = plt.subplots(len(DELTAS), 2, figsize=(18, 6 * len(DELTAS)))
fig.suptitle(
    f'{CODE_STA} — {river_name}\n'
    f'Corrélation pluie (blocs {FENETRE_H}h) × delta_h | {DATE_DEB} → {DATE_FIN}',
    fontsize=14, fontweight='bold'
)

for ri, (delta_name, corr_matrix) in enumerate(resultats.items()):
    # Heatmap
    ax = axes[ri, 0]
    im = ax.imshow(corr_matrix, aspect='auto', cmap='RdBu_r', vmin=-0.5, vmax=0.5)
    ax.set_yticks(range(len(tranches_list)))
    ax.set_yticklabels(tranches_list, fontsize=9)
    ax.set_xticks(range(NB_BLOCS))
    ax.set_xticklabels(labels_x, fontsize=7, rotation=30)
    for ti in range(len(tranches_list)):
        for bi in range(NB_BLOCS):
            val = corr_matrix[ti, bi]
            ax.text(bi, ti, f'{val:.2f}', ha='center', va='center',
                    fontsize=7, color='black' if abs(val) < 0.3 else 'white')
    plt.colorbar(im, ax=ax, label='Pearson')
    ax.set_title(f'Heatmap — {delta_name}', fontweight='bold')
    ax.set_ylabel('Tranche distance')

    # Profil
    ax = axes[ri, 1]
    for ti, tranche in enumerate(tranches_list):
        ax.plot(range(NB_BLOCS), corr_matrix[ti, :],
                color=COULEURS[tranche], linewidth=2,
                marker='o', markersize=4, label=tranche)
    ax.axhline(0, color='black', linewidth=0.8, linestyle='--')
    ax.set_xticks(range(NB_BLOCS))
    ax.set_xticklabels(labels_x, fontsize=7, rotation=30)
    ax.set_title(f'Profil — {delta_name}', fontweight='bold')
    ax.legend(fontsize=8)
    ax.grid(True, alpha=0.3)

plt.tight_layout()
path = f"./data/IA/Visualisation/Exploration/heatmap_sousjournal_{CODE_STA}.png"
plt.savefig(path, dpi=150, bbox_inches='tight')
plt.show()
print(f"\n✅ Sauvegardé : {path}")