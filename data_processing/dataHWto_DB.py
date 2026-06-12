"""
find_missing_measurements.py
─────────────────────────────
Compare les fichiers bruts HydroWeb avec la BDD pour trouver
les mesures présentes dans les fichiers mais absentes de la BDD.

Pour chaque mesure manquante : affiche date, cycle, ortho, uncertainty.
Génère un CSV de toutes les mesures manquantes à réimporter.

Usage :
    python find_missing_measurements.py --dry_run   # stats seulement
    python find_missing_measurements.py             # stats + CSV
    python find_missing_measurements.py --insert    # stats + CSV + insertion en BDD
"""

import argparse
import re
import sqlite3
import numpy as np
import pandas as pd
from pathlib import Path

DB_PATH  = "./data/hydro_data.db"
HW_DIRS  = [
    "./data/Garonne_hw",
    "./data/Loire_hw",
    "./data/Seine_hw",
    "./data/Rhone_hw",
]
OUT_CSV  = "./data/missing_measurements.csv"


# ═════════════════════════════════════════════════════════════
# PARSING FICHIER BRUT
# ═════════════════════════════════════════════════════════════

def parse_hw_file(path: Path) -> tuple[str, pd.DataFrame]:
    """Retourne (station_id, dataframe) depuis un fichier HydroWeb brut."""
    header = {}
    rows   = []

    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.rstrip("\n")
            if line.startswith("#"):
                m = re.match(r"#(.+?)::\s*(.*)", line)
                if m:
                    header[m.group(1).strip()] = m.group(2).strip()
                continue

            parts = line.split()
            if len(parts) < 13:
                continue
            try:
                lon    = float(parts[5])
                lat    = float(parts[6])
                ellipso= float(parts[7])
                geoid  = float(parts[8])
                dist   = float(parts[9])

                rows.append({
                    "date":        pd.to_datetime(parts[0]).normalize(),
                    "time":        parts[1],
                    "ortho":       float(parts[2]),
                    "uncertainty": float(parts[3]),
                    "lon":         np.nan if abs(lon)    > 999 else lon,
                    "lat":         np.nan if abs(lat)    > 999 else lat,
                    "ellipso":     np.nan if abs(ellipso)> 9990 else ellipso,
                    "geoid":       geoid,
                    "distance":    np.nan if abs(dist)   > 9990 else dist,
                    "satellite":   parts[10],
                    "orbit":       parts[11],
                    "track":       int(parts[12]),
                    "cycle":       int(parts[13]),
                    "retracking":  parts[14] if len(parts) > 14 else "",
                    "gdr":         parts[15] if len(parts) > 15 else "",
                })
            except (ValueError, IndexError):
                continue

    sid = header.get("ID", "").strip()

    # Coordonnées de référence pour remplacer les NaN
    try:
        ref_lon = float(header.get("REFERENCE LONGITUDE", "nan"))
        ref_lat = float(header.get("REFERENCE LATITUDE",  "nan"))
    except ValueError:
        ref_lon = ref_lat = np.nan

    df = pd.DataFrame(rows)
    if not df.empty:
        df["lon"] = df["lon"].fillna(ref_lon)
        df["lat"] = df["lat"].fillna(ref_lat)
        df = df.sort_values("date").reset_index(drop=True)

    return sid, df


# ═════════════════════════════════════════════════════════════
# CHARGEMENT BDD PAR STATION
# ═════════════════════════════════════════════════════════════

def load_db_dates(conn, station_code: str) -> set:
    """Retourne l'ensemble des dates présentes en BDD pour une station."""
    code_int = station_code.lstrip("0") or "0"
    candidates = list(dict.fromkeys([
        station_code, code_int,
        code_int.zfill(4), code_int.zfill(5),
        code_int.zfill(6), code_int.zfill(13),
    ]))
    for code in candidates:
        rows = conn.execute(
            "SELECT measure_date FROM measurements WHERE station_code = ?",
            (code,)
        ).fetchall()
        if rows:
            return {pd.to_datetime(r[0]).normalize() for r in rows}, code
    return set(), station_code


def get_db_station_code(conn, station_code: str) -> str:
    """Retourne le code exact utilisé dans la BDD."""
    code_int = station_code.lstrip("0") or "0"
    candidates = list(dict.fromkeys([
        station_code, code_int,
        code_int.zfill(4), code_int.zfill(5),
        code_int.zfill(6), code_int.zfill(13),
    ]))
    for code in candidates:
        r = conn.execute(
            "SELECT COUNT(*) FROM measurements WHERE station_code = ?", (code,)
        ).fetchone()[0]
        if r > 0:
            return code
    return station_code


# ═════════════════════════════════════════════════════════════
# MAIN
# ═════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db",       default=DB_PATH)
    parser.add_argument("--out_csv",  default=OUT_CSV)
    parser.add_argument("--dry_run",  action="store_true",
                        help="Stats seulement, pas de CSV ni d'insertion")
    parser.add_argument("--insert",   action="store_true",
                        help="Insère les mesures manquantes dans la BDD")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)

    all_missing = []
    total_file = total_db = total_missing = 0

    print(f"{'='*65}")
    print(f"RECHERCHE MESURES MANQUANTES — fichiers HW vs BDD")
    print(f"{'='*65}")
    print(f"{'Station':<22} {'N_file':>7} {'N_db':>7} {'N_miss':>7} {'%miss':>7}")
    print(f"{'-'*55}")

    for hw_dir in HW_DIRS:
        hw_path = Path(hw_dir)
        if not hw_path.exists():
            print(f"  ⚠  {hw_dir} introuvable")
            continue

        files = sorted([p for p in hw_path.glob("*") if p.is_file()])
        files += sorted(hw_path.glob("*.txt"))
        files = list(dict.fromkeys(files))  # déduplique

        for fpath in files:
            try:
                sid, df_file = parse_hw_file(fpath)
            except Exception as e:
                print(f"  ⚠  {fpath.name} parse error: {e}")
                continue

            if not sid or df_file.empty:
                continue

            # Dates présentes en BDD
            db_dates, db_code = load_db_dates(conn, sid)
            n_file = len(df_file)
            n_db   = len(db_dates)

            # Mesures dans le fichier mais absentes de la BDD
            missing_mask = ~df_file["date"].isin(db_dates)
            df_missing   = df_file[missing_mask].copy()
            n_missing    = len(df_missing)

            total_file   += n_file
            total_db     += n_db
            total_missing+= n_missing

            pct = n_missing / n_file * 100 if n_file > 0 else 0
            flag = "⚠ " if pct > 10 else "  "
            print(f"{flag}{sid:<20} {n_file:>7} {n_db:>7} {n_missing:>7} {pct:>6.1f}%")

            if n_missing > 0:
                df_missing["station_code"] = db_code
                df_missing["source_file"]  = fpath.name
                all_missing.append(df_missing)

    print(f"{'-'*55}")
    pct_tot = total_missing / total_file * 100 if total_file > 0 else 0
    print(f"{'TOTAL':<22} {total_file:>7} {total_db:>7} {total_missing:>7} {pct_tot:>6.1f}%")

    if not all_missing:
        print("\n✅ Aucune mesure manquante !")
        conn.close()
        return

    df_all_missing = pd.concat(all_missing, ignore_index=True)

    if args.dry_run:
        print(f"\n[DRY RUN] {len(df_all_missing)} mesures manquantes détectées.")
        print(df_all_missing[["station_code","date","ortho","uncertainty","cycle","satellite"]].head(20).to_string())
        conn.close()
        return

    # ── Sauvegarde CSV ───────────────────────────────────────
    df_all_missing.to_csv(args.out_csv, index=False)
    print(f"\n✅ CSV : {args.out_csv}  ({len(df_all_missing)} mesures manquantes)")

    # ── Insertion en BDD ─────────────────────────────────────
    if args.insert:
        print(f"\nInsertion dans la BDD...")
        cur     = conn.cursor()
        n_ins   = 0
        n_skip  = 0

        for _, row in df_all_missing.iterrows():
            try:
                cur.execute("""
                    INSERT OR IGNORE INTO measurements (
                        station_code, measure_date, measure_time,
                        orthometric_height, uncertainty,
                        longitude, latitude,
                        ellipsoidal_height, geoidal_ondulation,
                        distance_to_ref_km,
                        satellite, orbit_mission, track_number, cycle_number,
                        retracking_algorithm, gdr_version,
                        is_valid
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1)
                """, (
                    row["station_code"],
                    str(row["date"].date()),
                    row.get("time", None),
                    row["ortho"],
                    row["uncertainty"],
                    None if pd.isna(row["lon"])     else row["lon"],
                    None if pd.isna(row["lat"])     else row["lat"],
                    None if pd.isna(row["ellipso"]) else row["ellipso"],
                    row["geoid"],
                    None if pd.isna(row["distance"])else row["distance"],
                    row["satellite"],
                    row["orbit"],
                    int(row["track"]),
                    int(row["cycle"]),
                    row["retracking"],
                    row["gdr"],
                ))
                if cur.rowcount > 0:
                    n_ins += 1
                else:
                    n_skip += 1
            except Exception as e:
                print(f"  ⚠  {row['station_code']} {row['date']} : {e}")
                n_skip += 1

        conn.commit()
        print(f"✅ {n_ins} mesures insérées")
        print(f"   {n_skip} ignorées (doublons ou erreurs)")

        # Stats finales
        total_after = conn.execute(
            "SELECT COUNT(*) FROM measurements WHERE is_valid=1 AND orthometric_height IS NOT NULL"
        ).fetchone()[0]
        print(f"\n📊 Mesures valides après insertion : {total_after}")

    conn.close()


if __name__ == "__main__":
    main()