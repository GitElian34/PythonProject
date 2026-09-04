"""
plot_france_gain_map_html.py
════════════════════════════════════════════════════════════════════════
Carte HTML interactive (Leaflet + fond de carte CartoDB) des stations
France, colorées par CATÉGORIE de gain NSE (pas de dégradé continu) :
gain = NSE(modèle) - NSE(alti-insitu), calculé station par station.

Un seul type de marqueur (cercle) pour toutes les stations, 10j et 27j
confondus -- seule la couleur (catégorie de gain) varie. La fréquence
(10j/27j) reste visible au survol de chaque point (popup).

Fichier de sortie autonome : ouvrez-le dans un navigateur, zoomez /
déplacez-vous, puis faites votre capture d'écran.

Sources :
  Models_Testing/DtoD/residus/metrics_DtoD96_hwnext_10j_sword_insitu.csv
  Models_Testing/DtoD/residus/metrics_DtoD96_hwnext_27j_sword_insitu.csv
  data/hydroweb_next.db, table "stations" (coordonnées)

Sortie :
  Models_Testing/DtoD/figures/map_gain_nse_france.html
════════════════════════════════════════════════════════════════════════
"""

import json
import sqlite3
import pandas as pd
from pathlib import Path

MODEL_LABEL = "DtoD96"
FREQS = ["10j", "27j"]

RESIDUS_DIR = Path("./Models_Testing/DtoD/residus")
OUT_DIR = Path("./Models_Testing/DtoD/figures")
HW_DB = "./data/hydroweb_next.db"

# Catégories de gain (borne haute incluse, la dernière va jusqu'à +inf).
# À ajuster si la distribution réelle est différente (imprimée à l'exécution).
GAIN_BINS = [
    (0.0, "Dégradation (< 0)", "#C0392B"),
    (0.15, "Gain faible (0 – 0.15)", "#F4D03F"),
    (0.30, "Gain modéré (0.15 – 0.30)", "#82E0AA"),
    (0.45, "Bon gain (0.30 – 0.45)", "#27AE60"),
    (float("inf"), "Très bon gain (> 0.45)", "#145A32"),
]


# ═══════════════════════════════════════════════════════════════
# COORDONNÉES STATIONS
# ═══════════════════════════════════════════════════════════════
_cache_coords = {}


def get_coords(db_path, code):
    key = (db_path, code)
    if key in _cache_coords:
        return _cache_coords[key]
    conn = sqlite3.connect(db_path)
    for c in [str(code), str(code).zfill(13)]:
        df = pd.read_sql(
            "SELECT reference_longitude AS lon, reference_latitude AS lat "
            "FROM stations WHERE station_code = ?", conn, params=(c,))
        if not df.empty:
            conn.close()
            _cache_coords[key] = (float(df.iloc[0]["lon"]), float(df.iloc[0]["lat"]))
            return _cache_coords[key]
    conn.close()
    _cache_coords[key] = (None, None)
    return None, None


def categorize(gain: float) -> tuple:
    for upper, label, color in GAIN_BINS:
        if gain < upper:
            return label, color
    return GAIN_BINS[-1][1], GAIN_BINS[-1][2]


# ═══════════════════════════════════════════════════════════════
# CHARGEMENT DES GAINS PAR STATION
# ═══════════════════════════════════════════════════════════════
def load_stations_with_gain() -> pd.DataFrame:
    rows = []
    for freq in FREQS:
        path = RESIDUS_DIR / f"metrics_{MODEL_LABEL}_hwnext_{freq}_sword_insitu.csv"
        if not path.exists():
            print(f"⚠ Fichier introuvable : {path} -> ignoré")
            continue
        df = pd.read_csv(path)
        df["freq"] = freq
        df["gain_NSE"] = df["NSE"] - df["NSE_alti_insitu"]
        rows.append(df)
    if not rows:
        raise FileNotFoundError(f"Aucun fichier metrics_{MODEL_LABEL}_*_sword_insitu.csv trouvé dans {RESIDUS_DIR}.")
    df_all = pd.concat(rows, ignore_index=True)

    lons, lats = [], []
    n_missing = 0
    for code in df_all["station"]:
        lon, lat = get_coords(HW_DB, code)
        if lon is None:
            n_missing += 1
        lons.append(lon)
        lats.append(lat)
    df_all["lon"], df_all["lat"] = lons, lats

    if n_missing:
        print(f"⚠ Coordonnées introuvables pour {n_missing}/{len(df_all)} stations -> exclues de la carte")
    df_all = df_all.dropna(subset=["lon", "lat", "gain_NSE"])

    cats = df_all["gain_NSE"].apply(categorize)
    df_all["cat_label"] = [c[0] for c in cats]
    df_all["cat_color"] = [c[1] for c in cats]
    return df_all


# ═══════════════════════════════════════════════════════════════
# GÉNÉRATION HTML (Leaflet)
# ═══════════════════════════════════════════════════════════════
HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="utf-8">
<title>Gain NSE par station — {model_label}</title>
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/leaflet.min.css" />
<script src="https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/leaflet.min.js"></script>
<style>
  html, body {{ margin: 0; padding: 0; height: 100%; font-family: "Segoe UI", Arial, sans-serif; }}
  #header {{
    padding: 14px 22px; background: #2C3E50; color: white;
  }}
  #header h1 {{ margin: 0; font-size: 17px; }}
  #header p {{ margin: 4px 0 0; font-size: 12.5px; color: #BDC3C7; }}
  #map {{ position: absolute; top: 62px; bottom: 0; left: 0; right: 0; }}
  .legend {{
    background: white; padding: 10px 14px; border-radius: 6px;
    box-shadow: 0 1px 6px rgba(0,0,0,0.25); font-size: 12.5px; color: #2C3E50;
    line-height: 1.6;
  }}
  .legend b {{ display: block; margin-bottom: 6px; font-size: 12.5px; }}
  .legend .swatch {{
    display: inline-block; width: 12px; height: 12px; border-radius: 50%;
    margin-right: 7px; vertical-align: middle; border: 1px solid rgba(0,0,0,0.15);
  }}
  .leaflet-popup-content {{ font-size: 12.5px; }}
</style>
</head>
<body>

<div id="header">
  <h1>Gain de NSE apporté par le modèle par rapport à l'altimétrie seule</h1>
  <p>Modèle {model_label} · NSE(modèle) − NSE(alti-insitu), par station · {n_stations} stations (10j + 27j)</p>
</div>
<div id="map"></div>

<script>
const stations = {stations_json};
const categories = {categories_json};

const map = L.map('map', {{ zoomControl: true }}).setView([46.6, 2.3], 6);

L.tileLayer('https://{{s}}.basemaps.cartocdn.com/light_all/{{z}}/{{x}}/{{y}}{{r}}.png', {{
  attribution: '&copy; OpenStreetMap contributors &copy; CARTO',
  subdomains: 'abcd',
  maxZoom: 19
}}).addTo(map);

stations.forEach(function(s) {{
  const marker = L.circleMarker([s.lat, s.lon], {{
    radius: 6,
    fillColor: s.cat_color,
    color: '#FFFFFF',
    weight: 1,
    fillOpacity: 0.9
  }}).addTo(map);
  marker.bindPopup(
    '<b>' + s.station + '</b><br>' +
    'Fréquence : ' + s.freq + '<br>' +
    'Gain NSE : ' + s.gain_NSE.toFixed(3) + '<br>' +
    'Catégorie : ' + s.cat_label
  );
}});

const legend = L.control({{ position: 'bottomleft' }});
legend.onAdd = function() {{
  const div = L.DomUtil.create('div', 'legend');
  let html = '<b>Gain NSE (modèle − alti-insitu)</b>';
  categories.forEach(function(c) {{
    html += '<div><span class="swatch" style="background:' + c.color + '"></span>' +
            c.label + ' — n=' + c.count + '</div>';
  }});
  div.innerHTML = html;
  return div;
}};
legend.addTo(map);
</script>

</body>
</html>
"""


def build_html(df: pd.DataFrame, out_path: Path) -> None:
    stations = df[["station", "freq", "lat", "lon", "gain_NSE", "cat_label", "cat_color"]].to_dict("records")

    # Comptage par catégorie, dans l'ordre défini par GAIN_BINS
    cat_counts = df["cat_label"].value_counts().to_dict()
    categories = [{"label": label, "color": color, "count": cat_counts.get(label, 0)}
                  for _, label, color in GAIN_BINS]

    html = HTML_TEMPLATE.format(
        model_label=MODEL_LABEL,
        n_stations=len(df),
        stations_json=json.dumps(stations, ensure_ascii=False),
        categories_json=json.dumps(categories, ensure_ascii=False),
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html, encoding="utf-8")
    print(f"✅ {out_path}  (ouvrez ce fichier dans un navigateur)")


def main():
    df = load_stations_with_gain()
    print(f"  {len(df)} stations avec coordonnées et gain NSE")
    print(f"  Distribution du gain : min={df['gain_NSE'].min():.3f}  "
          f"médiane={df['gain_NSE'].median():.3f}  max={df['gain_NSE'].max():.3f}")
    print("  Répartition par catégorie :")
    for _, label, _ in GAIN_BINS:
        n = (df["cat_label"] == label).sum()
        print(f"    {label:<28} n={n}")

    build_html(df, OUT_DIR / "map_gain_nse_france.html")


if __name__ == "__main__":
    main()