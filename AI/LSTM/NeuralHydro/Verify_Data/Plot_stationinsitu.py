"""
Carte HTML interactive des 20 MEILLEURES et 20 PIRES stations
Utilise folium pour générer une carte cliquable avec popups.
"""

import pandas as pd
from pathlib import Path
import folium

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
CSV_PATH   = Path("./runs/arlstm_feat10j_modele2_2704_112827/test/model_epoch005/test_metrics.csv")
ATTRS_PATH = Path("./data/IA/NeuralHydrology_feat10j/attributes/attributes.csv")
OUT_HTML   = Path("./data/IA/NeuralHydrology/Visualisation/carte_stations.html")

N_TOP = 20

# ═══════════════════════════════════════════════════════════════
# CHARGEMENT + AGRÉGATION
# ═══════════════════════════════════════════════════════════════
df_scores = pd.read_csv(CSV_PATH, header=None, names=["station_d", "NSE", "KGE"])
df_scores["NSE"] = pd.to_numeric(df_scores["NSE"], errors="coerce")
df_scores["KGE"] = pd.to_numeric(df_scores["KGE"], errors="coerce")
df_scores = df_scores.dropna(subset=["NSE", "KGE"])
df_scores["station"] = df_scores["station_d"].str.replace(r"_d\d+$", "", regex=True)

df_agg = df_scores.groupby("station").agg(
    NSE_mean=("NSE", "mean"),
    KGE_mean=("KGE", "mean"),
).reset_index()

attrs = pd.read_csv(ATTRS_PATH)
attrs["station"] = attrs["station_id"].str.replace(r"_d\d+$", "", regex=True)
attrs = attrs.drop_duplicates(subset=["station"])

df = df_agg.merge(
    attrs[["station", "lon", "lat", "aire_km2", "elevation_mean", "slope_mean", "strahler"]],
    on="station", how="left"
)
df = df.dropna(subset=["lon", "lat"])

# ═══════════════════════════════════════════════════════════════
# SÉLECTION TOP/BOTTOM
# ═══════════════════════════════════════════════════════════════
top_stations    = df.nlargest(N_TOP, "NSE_mean").copy()
bottom_stations = df.nsmallest(N_TOP, "NSE_mean").copy()
top_stations["categorie"]    = "TOP"
bottom_stations["categorie"] = "BOTTOM"

print(f"TOP {N_TOP} : NSE de {top_stations['NSE_mean'].min():.3f} à {top_stations['NSE_mean'].max():.3f}")
print(f"BOTTOM {N_TOP} : NSE de {bottom_stations['NSE_mean'].min():.3f} à {bottom_stations['NSE_mean'].max():.3f}")

# ═══════════════════════════════════════════════════════════════
# CRÉATION DE LA CARTE
# ═══════════════════════════════════════════════════════════════
m = folium.Map(location=[46.5, 2.5], zoom_start=6, tiles="OpenStreetMap")

fg_top    = folium.FeatureGroup(name=f"TOP {N_TOP} (vert)", show=True)
fg_bottom = folium.FeatureGroup(name=f"BOTTOM {N_TOP} (rouge)", show=True)

for df_grp, group, color in [(top_stations, fg_top, "green"),
                              (bottom_stations, fg_bottom, "red")]:
    for _, row in df_grp.iterrows():
        popup_html = f"""
        <b>{row['station']}</b><br>
        <b>NSE moyen : {row['NSE_mean']:.3f}</b><br>
        KGE moyen : {row['KGE_mean']:.3f}<br>
        <hr style='margin:3px'>
        Aire : {row['aire_km2']:.0f} km²<br>
        Strahler : {int(row['strahler']) if pd.notna(row['strahler']) else 'N/A'}<br>
        Elevation : {row['elevation_mean']:.0f} m<br>
        Slope : {row['slope_mean']:.2f} %
        """
        folium.CircleMarker(
            location=[row["lat"], row["lon"]],
            radius=8,
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.75,
            weight=2,
            popup=folium.Popup(popup_html, max_width=250),
            tooltip=f"{row['station']} — NSE {row['NSE_mean']:.2f}",
        ).add_to(group)

fg_top.add_to(m)
fg_bottom.add_to(m)
folium.LayerControl(collapsed=False).add_to(m)

# Légende
legend_html = f"""
<div style='position: fixed; bottom: 20px; left: 20px; background: white;
            padding: 10px; border: 1px solid #999; border-radius: 5px;
            font-size: 13px; z-index: 9999;'>
  <b>Performances du modèle</b><br>
  <span style='color:green; font-size: 18px'>●</span> TOP {N_TOP} (NSE moyen le + haut)<br>
  <span style='color:red; font-size: 18px'>●</span> BOTTOM {N_TOP} (NSE moyen le + bas)
</div>
"""
m.get_root().html.add_child(folium.Element(legend_html))

# Sauvegarde
OUT_HTML.parent.mkdir(parents=True, exist_ok=True)
m.save(str(OUT_HTML))
print(f"\n✅ Carte sauvegardée : {OUT_HTML}")