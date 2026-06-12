"""
check_hw_files_vs_db.py
───────────────────────
Compare les fichiers bruts HydroWeb (.txt) avec la BDD SQLite.
Pour chaque station : vérifie dates, orthometric_height, ellipsoidal_height,
coordonnées (en utilisant la ref du header si 9999.999), cycle_number.

Dossiers scannés :
    ./data/Garonne_hw
    ./data/Loire_hw
    ./data/Seine_hw
    ./data/Rhone_hw

Usage :
    python check_hw_files_vs_db.py
    python check_hw_files_vs_db.py --basins Garonne Loire   # subset
    python check_hw_files_vs_db.py --plot                   # figures PNG
"""

import argparse
import re
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

HYDRO_DB  = Path("./data/hydro_data.db")
OUT_DIR   = Path("./data/check_hw")
HW_DIRS   = {
    "Garonne": Path("./data/Garonne_hw"),
    "Loire":   Path("./data/Loire_hw"),
    "Seine":   Path("./data/Seine_hw"),
    "Rhone":   Path("./data/Rhone_hw"),
}


# ═════════════════════════════════════════════════════════════
# PARSING FICHIER BRUT
# ═════════════════════════════════════════════════════════════

def parse_hw_file(path: Path) -> tuple[dict, pd.DataFrame]:
    """
    Lit un fichier HydroWeb brut.
    Retourne (header_dict, dataframe).

    Colonnes du dataframe :
        date, ortho, uncertainty, lon, lat, ellipso, geoid,
        distance, satellite, orbit, track, cycle, retracking, gdr
    """
    header = {}
    rows = []

    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.rstrip("\n")

            # ── Header ──────────────────────────────────────
            if line.startswith("#"):
                m = re.match(r"#(.+?)::\s*(.*)", line)
                if m:
                    key = m.group(1).strip()
                    val = m.group(2).strip()
                    header[key] = val
                continue

            # ── Données ─────────────────────────────────────
            # Format : DATE TIME  ORTHO  UNCERT : LON LAT ELLIPSO GEOID DIST SAT ORBIT TRACK CYCLE RETK GDR
            parts = line.split()
            if len(parts) < 13:
                continue
            try:
                date_str = parts[0]          # YYYY-MM-DD
                ortho    = float(parts[2])
                uncert   = float(parts[3])
                # parts[4] = ':'  séparateur
                lon      = float(parts[5])
                lat      = float(parts[6])
                ellipso  = float(parts[7])
                geoid    = float(parts[8])
                dist     = float(parts[9])
                sat      = parts[10]
                orbit    = parts[11]
                track    = int(parts[12])
                cycle    = int(parts[13])
                retk     = parts[14] if len(parts) > 14 else ""
                gdr      = parts[15] if len(parts) > 15 else ""

                # Remplace 9999.999 par NaN
                lon     = np.nan if abs(lon)     > 999 else lon
                lat     = np.nan if abs(lat)     > 999 else lat
                ellipso = np.nan if abs(ellipso) > 9990 else ellipso
                dist    = np.nan if abs(dist)    > 9990 else dist

                rows.append({
                    "date":       pd.to_datetime(date_str).normalize(),
                    "ortho":      ortho,
                    "uncertainty":uncert,
                    "lon":        lon,
                    "lat":        lat,
                    "ellipso":    ellipso,
                    "geoid":      geoid,
                    "distance":   dist,
                    "satellite":  sat,
                    "orbit":      orbit,
                    "track":      track,
                    "cycle":      cycle,
                    "retracking": retk,
                    "gdr":        gdr,
                })
            except (ValueError, IndexError):
                continue

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("date").reset_index(drop=True)

    # ── Coordonnées de référence du header ──────────────────
    try:
        header["ref_lon"] = float(header.get("REFERENCE LONGITUDE", "nan"))
        header["ref_lat"] = float(header.get("REFERENCE LATITUDE",  "nan"))
    except ValueError:
        header["ref_lon"] = np.nan
        header["ref_lat"] = np.nan

    # Remplit lon/lat manquants par la référence du header
    if not df.empty:
        df["lon"] = df["lon"].fillna(header["ref_lon"])
        df["lat"] = df["lat"].fillna(header["ref_lat"])

    # ID station depuis le header
    header["station_id"] = header.get("ID", "").strip()

    return header, df


# ═════════════════════════════════════════════════════════════
# CHARGEMENT BDD
# ═════════════════════════════════════════════════════════════

def load_db(station_code: str) -> pd.DataFrame:
    """Charge toutes les mesures d'une station depuis la BDD."""
    conn = sqlite3.connect(HYDRO_DB)
    query = """
        SELECT
            measure_date        AS date,
            orthometric_height  AS ortho_db,
            ellipsoidal_height  AS ellipso_db,
            geoidal_ondulation  AS geoid_db,
            latitude            AS lat_db,
            longitude           AS lon_db,
            cycle_number        AS cycle_db,
            track_number        AS track_db,
            uncertainty         AS uncert_db,
            satellite           AS sat_db
        FROM measurements
        WHERE station_code = ?
        ORDER BY measure_date
    """
    # Essai avec zero-padding 13 chiffres
    code_int = station_code.lstrip("0") or "0"
    candidates = list(dict.fromkeys([
        station_code, code_int,
        code_int.zfill(4), code_int.zfill(5),
        code_int.zfill(6), code_int.zfill(13),
    ]))
    df = pd.DataFrame()
    for code in candidates:
        df = pd.read_sql_query(query, conn, params=(code,))
        if not df.empty:
            break
    conn.close()

    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    return df.sort_values("date").reset_index(drop=True)


# ═════════════════════════════════════════════════════════════
# COMPARAISON PAR STATION
# ═════════════════════════════════════════════════════════════

def compare_station(header: dict, df_file: pd.DataFrame,
                    df_db: pd.DataFrame) -> dict:
    """Retourne un dict de métriques de comparaison."""
    sid = header["station_id"]
    res = {"station": sid, "basin": header.get("_basin", ""),
           "river": header.get("RIVER", ""), "file": header.get("_file", "")}

    res["n_file"] = len(df_file)
    res["n_db"]   = len(df_db)

    if df_file.empty:
        res["status"] = "EMPTY_FILE"; return res
    if df_db.empty:
        res["status"] = "NOT_IN_DB";  return res

    # ── 1. Dates ─────────────────────────────────────────────
    dates_file = set(df_file["date"])
    dates_db   = set(df_db["date"])
    common     = dates_file & dates_db
    only_file  = dates_file - dates_db
    only_db    = dates_file - dates_file   # dates en DB mais pas dans fichier

    res["n_dates_common"]    = len(common)
    res["n_dates_only_file"] = len(only_file)
    res["n_dates_only_db"]   = len(dates_db - dates_file)
    res["pct_dates_matched"] = round(len(common) / len(dates_file) * 100, 1)

    if not common:
        res["status"] = "NO_DATE_MATCH"; return res

    # ── 2. Alignement sur dates communes ────────────────────
    df_f = df_file[df_file["date"].isin(common)].set_index("date").sort_index()
    df_d = df_db[df_db["date"].isin(common)].set_index("date").sort_index()

    # ── 3. Orthometric height ────────────────────────────────
    diff_ortho = (df_f["ortho"] - df_d["ortho_db"]).dropna()
    res["bias_ortho_m"]  = round(float(diff_ortho.mean()), 4)  if len(diff_ortho) else np.nan
    res["rmse_ortho_m"]  = round(float(np.sqrt((diff_ortho**2).mean())), 4) if len(diff_ortho) else np.nan
    res["max_diff_ortho"]= round(float(diff_ortho.abs().max()), 4) if len(diff_ortho) else np.nan
    res["pct_exact_ortho"]= round(float((diff_ortho.abs() < 0.01).mean() * 100), 1) if len(diff_ortho) else np.nan

    # ── 4. Ellipsoidal height ────────────────────────────────
    diff_ellipso = (df_f["ellipso"] - df_d["ellipso_db"]).dropna()
    res["bias_ellipso_m"] = round(float(diff_ellipso.mean()), 4)  if len(diff_ellipso) else np.nan
    res["rmse_ellipso_m"] = round(float(np.sqrt((diff_ellipso**2).mean())), 4) if len(diff_ellipso) else np.nan

    # ── 5. Cycle number ──────────────────────────────────────
    diff_cycle = (df_f["cycle"] - df_d["cycle_db"]).dropna()
    res["pct_cycle_match"] = round(float((diff_cycle == 0).mean() * 100), 1) if len(diff_cycle) else np.nan
    res["mean_cycle_offset"]= round(float(diff_cycle.mean()), 2) if len(diff_cycle) else np.nan

    # ── 6. Coordonnées ───────────────────────────────────────
    # Nettoie les coords DB aberrantes (>90° impossible pour lat, >180° pour lon)
    lat_db_clean = df_d["lat_db"].where(df_d["lat_db"].abs() <= 90)
    lon_db_clean = df_d["lon_db"].where(df_d["lon_db"].abs() <= 180)

    delta_lat = (df_f["lat"] - lat_db_clean).dropna()
    delta_lon = (df_f["lon"] - lon_db_clean).dropna()
    res["mean_delta_lat_m"] = round(float(delta_lat.mean() * 111000), 1) if len(delta_lat) else np.nan
    res["mean_delta_lon_m"] = round(float(delta_lon.mean() * 111000), 1) if len(delta_lon) else np.nan
    res["n_coords_aberrant_db"] = int((df_d["lat_db"].abs() > 90).sum())

    # ── 7. Status global ─────────────────────────────────────
    if res["rmse_ortho_m"] is not None and not np.isnan(res["rmse_ortho_m"]):
        if res["rmse_ortho_m"] < 0.01 and res["pct_dates_matched"] > 95:
            res["status"] = "PERFECT"
        elif res["rmse_ortho_m"] < 0.1 and res["pct_dates_matched"] > 80:
            res["status"] = "GOOD"
        elif res["rmse_ortho_m"] < 0.5:
            res["status"] = "OK"
        else:
            res["status"] = "MISMATCH"
    else:
        res["status"] = "NO_ORTHO"

    return res


# ═════════════════════════════════════════════════════════════
# SCAN D'UN DOSSIER
# ═════════════════════════════════════════════════════════════

def scan_basin(basin_name: str, hw_dir: Path) -> list[dict]:
    results = []
    # Fichiers .txt ou sans extension
    files = sorted(hw_dir.glob("*.txt")) + sorted(
        p for p in hw_dir.glob("*") if p.is_file() and p.suffix == ""
    )
    if not files:
        print(f"  ⚠  Aucun .txt dans {hw_dir}")
        return results

    print(f"\n{'═'*60}")
    print(f"  {basin_name} — {len(files)} fichiers")
    print(f"{'═'*60}")
    print(f"  {'Station':<20} {'Status':<12} {'N_f':>5} {'N_db':>5} "
          f"{'%dates':>7} {'bias_ortho':>11} {'rmse_ortho':>11} "
          f"{'%cyc':>6} {'Δcyc':>6} {'n_aberr':>8}")
    print(f"  {'─'*100}")

    for fpath in files:
        try:
            header, df_file = parse_hw_file(fpath)
        except Exception as e:
            print(f"  ⚠  {fpath.name} — parse error: {e}")
            continue

        header["_basin"] = basin_name
        header["_file"]  = fpath.name
        sid = header.get("station_id", "")

        if not sid:
            print(f"  ⚠  {fpath.name} — pas d'ID dans le header")
            continue

        df_db = load_db(sid)
        res   = compare_station(header, df_file, df_db)
        results.append(res)

        icon = {"PERFECT": "✅", "GOOD": "🟢", "OK": "🟡",
                "MISMATCH": "🔴", "NOT_IN_DB": "—", "NO_DATE_MATCH": "⬜",
                "EMPTY_FILE": "⬜", "NO_ORTHO": "❓"
                }.get(res.get("status", ""), "❓")

        print(f"  {icon} {sid:<18} {res.get('status','?'):<12} "
              f"{res.get('n_file',0):>5} {res.get('n_db',0):>5} "
              f"{res.get('pct_dates_matched', float('nan')):>7.1f} "
              f"{res.get('bias_ortho_m', float('nan')):>+11.4f} "
              f"{res.get('rmse_ortho_m', float('nan')):>11.4f} "
              f"{res.get('pct_cycle_match', float('nan')):>6.1f} "
              f"{res.get('mean_cycle_offset', float('nan')):>+6.2f} "
              f"{res.get('n_coords_aberrant_db', 0):>8}")

    return results


# ═════════════════════════════════════════════════════════════
# MAIN
# ═════════════════════════════════════════════════════════════

def main():
    global HYDRO_DB, OUT_DIR

    parser = argparse.ArgumentParser()
    parser.add_argument("--basins", nargs="+", default=list(HW_DIRS.keys()),
                        help="Bassins à traiter (défaut: tous)")
    parser.add_argument("--hydro_db", default=str(HYDRO_DB))
    parser.add_argument("--out_dir",  default=str(OUT_DIR))
    args = parser.parse_args()

    HYDRO_DB = Path(args.hydro_db)
    OUT_DIR  = Path(args.out_dir)
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    all_results = []
    for basin in args.basins:
        if basin not in HW_DIRS:
            print(f"⚠  Bassin inconnu : {basin}  (disponibles: {list(HW_DIRS)})")
            continue
        results = scan_basin(basin, HW_DIRS[basin])
        all_results.extend(results)

    if not all_results:
        print("Aucun résultat."); return

    df_out = pd.DataFrame(all_results)
    out_csv = OUT_DIR / "hw_files_vs_db.csv"
    df_out.to_csv(out_csv, index=False)

    print(f"\n\n{'═'*60}")
    print(f"SYNTHÈSE GLOBALE — {len(df_out)} stations")
    print(f"{'═'*60}")
    if "status" in df_out.columns:
        print(df_out["status"].value_counts().to_string())

    if "pct_dates_matched" in df_out.columns:
        v = df_out["pct_dates_matched"].dropna()
        print(f"\n% dates appariées : médiane={v.median():.1f}%  "
              f"<80%: {(v<80).sum()}  ==100%: {(v==100.0).sum()}")

    if "rmse_ortho_m" in df_out.columns:
        v = df_out["rmse_ortho_m"].dropna()
        print(f"RMSE ortho        : médiane={v.median():.4f}m  "
              f"<0.01m: {(v<0.01).sum()}  >0.1m: {(v>0.1).sum()}")

    if "n_coords_aberrant_db" in df_out.columns:
        total = df_out["n_coords_aberrant_db"].sum()
        n_sta = (df_out["n_coords_aberrant_db"] > 0).sum()
        print(f"Coords aberrantes : {total} mesures sur {n_sta} stations")

    print(f"\n✅ {out_csv}")


if __name__ == "__main__":
    main()