import sqlite3
import pandas as pd
from math import radians, cos, sin, asin, sqrt

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
DB_PATH = "./data/insitu_data.db"

# ═══════════════════════════════════════════════════════════════
# FONCTIONS
# ═══════════════════════════════════════════════════════════════
def haversine(lon1, lat1, lon2, lat2):
    R = 6371
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1)*cos(lat2)*sin(dlon/2)**2
    return 2 * R * asin(sqrt(a))

# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════
conn = sqlite3.connect(DB_PATH)

stations = pd.read_sql_query(
    "SELECT code_sta, lon, lat FROM stations_insitu WHERE lon IS NOT NULL AND lat IS NOT NULL",
    conn
)
print(f"{len(stations)} stations à corriger")

total_pixels = 0
for _, row in stations.iterrows():
    code_sta = row['code_sta']
    lon_sta  = row['lon']
    lat_sta  = row['lat']

    pixels = pd.read_sql_query(
        "SELECT transfert_id, pixel_lon, pixel_lat FROM era5_transfert WHERE code_sta = ?",
        conn, params=(code_sta,)
    )
    if pixels.empty:
        continue

    pixels['dist_km'] = pixels.apply(
        lambda r: round(haversine(lon_sta, lat_sta, r['pixel_lon'], r['pixel_lat']), 2),
        axis=1
    )

    conn.executemany(
        "UPDATE era5_transfert SET dist_km = ? WHERE transfert_id = ?",
        [(row['dist_km'], row['transfert_id']) for _, row in pixels.iterrows()]
    )
    conn.commit()
    total_pixels += len(pixels)
    print(f"  {code_sta} — {len(pixels)} pixels corrigés")

print(f"\n✅ Terminé ! {total_pixels} pixels mis à jour")
conn.close()