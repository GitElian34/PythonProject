"""
Carte HTML des 222 stations satellite avec leur altitude moyenne du BV.
Marqueurs colorés par tranche d'altitude.
"""

import sqlite3
import pandas as pd
import folium
from pathlib import Path

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
DB_PATH  = "./data/hydro_data.db"
OUT_HTML   = Path("./data/IA/NeuralHydrology/Visualisation/carte_stations.html")

# ═══════════════════════════════════════════════════════════════
# CHARGEMENT
# ═══════════════════════════════════════════════════════════════
conn = sqlite3.connect(DB_PATH)
df = pd.read_sql("""
    SELECT s.station_code,
           s.river_name,
           s.reference_longitude AS lon,
           s.reference_latitude  AS lat,
           s.elevation_mean,
           s.slope_mean,
           s.strahler,
           b.aire_km2
    FROM stations s
    LEFT JOIN bv_data b ON s.station_code = b.station_code
    WHERE s.reference_longitude IS NOT NULL
      AND s.reference_latitude IS NOT NULL
""", conn)
conn.close()

print(f"Total stations satellite : {len(df)}")
print()
print(f"Elevation (m) :")
print(f"  médiane : {df['elevation_mean'].median():.0f}")
print(f"  moyenne : {df['elevation_mean'].mean():.0f}")
print(f"  min/max : {df['elevation_mean'].min():.0f} / {df['elevation_mean'].max():.0f}")
print()
print(f"Slope (%) :")
print(f"  médiane : {df['slope_mean'].median():.2f}")
print(f"  moyenne : {df['slope_mean'].mean():.2f}")
print()

# Distribution par tranche
bins = [0, 100, 300, 600, 1000, 5000]
labels = ["<100m", "100-300m", "300-600m", "600-1000m", ">1000m"]
df["tranche"] = pd.cut(df["elevation_mean"], bins=bins, labels=labels, right=False)
print("Distribution par tranche d'altitude :")
print(df["tranche"].value_counts().sort_index())

# ═══════════════════════════════════════════════════════════════
# CARTE
# ═══════════════════════════════════════════════════════════════
m = folium.Map(location=[46.5, 2.5], zoom_start=6, tiles="OpenStreetMap")

couleurs = {
    "<100m":      "#1f77b4",   # bleu
    "100-300m":   "#2ca02c",   # vert
    "300-600m":   "#ff7f0e",   # orange
    "600-1000m":  "#d62728",   # rouge
    ">1000m":     "#7f0000",   # rouge foncé / brun
}

groupes = {label: folium.FeatureGroup(name=f"{label} ({(df['tranche']==label).sum()})", show=True)
           for label in labels}

for _, row in df.iterrows():
    if pd.isna(row["lon"]) or pd.isna(row["lat"]):
        continue
    if pd.isna(row["tranche"]):
        continue

    color = couleurs[str(row["tranche"])]
    popup_html = f"""
    <b>{row['station_code']}</b><br>
    {row['river_name'] or 'N/A'}<br>
    <hr style='margin:3px'>
    Elevation : {row['elevation_mean']:.0f} m<br>
    Slope : {row['slope_mean']:.2f} %<br>
    Strahler : {int(row['strahler']) if pd.notna(row['strahler']) else 'N/A'}<br>
    Aire BV : {row['aire_km2']:.0f} km² 
    """ if pd.notna(row['aire_km2']) else f"""
    <b>{row['station_code']}</b><br>
    {row['river_name'] or 'N/A'}<br>
    <hr style='margin:3px'>
    Elevation : {row['elevation_mean']:.0f} m<br>
    Slope : {row['slope_mean']:.2f} %
    """

    folium.CircleMarker(
        location=[row["lat"], row["lon"]],
        radius=6,
        color=color,
        fill=True,
        fill_color=color,
        fill_opacity=0.75,
        weight=1.5,
        popup=folium.Popup(popup_html, max_width=250),
        tooltip=f"{row['station_code']} — {row['elevation_mean']:.0f}m",
    ).add_to(groupes[str(row["tranche"])])

for g in groupes.values():
    g.add_to(m)
folium.LayerControl(collapsed=False).add_to(m)

# Légende
legend_html = """
<div style='position: fixed; bottom: 20px; left: 20px; background: white;
            padding: 10px; border: 1px solid #999; border-radius: 5px;
            font-size: 13px; z-index: 9999;'>
  <b>Altitude moyenne du BV</b><br>
"""
for label in labels:
    n = (df["tranche"] == label).sum()
    legend_html += f"  <span style='color:{couleurs[label]}; font-size: 16px'>●</span> {label} ({n})<br>"
legend_html += "</div>"
m.get_root().html.add_child(folium.Element(legend_html))

m.save(str(OUT_HTML))
print(f"\n✅ Carte sauvegardée : {OUT_HTML}")