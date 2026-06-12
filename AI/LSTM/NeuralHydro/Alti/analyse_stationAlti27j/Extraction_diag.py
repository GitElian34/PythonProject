"""
compare_wsh_hydroweb.py
───────────────────────
Comparaison simple entre WSH CNES (CSV) et HydroWeb (BDD SQLite).
Pour chaque station : aligne par date, compare date/valeur/cycle/coordonnées.

Usage :
    python compare_wsh_hydroweb.py --station 5718
    python compare_wsh_hydroweb.py                   # toutes les stations
    python compare_wsh_hydroweb.py --plot             # avec figures PNG
"""

import argparse
import sqlite3
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

warnings.filterwarnings("ignore")

# ── Chemins par défaut ────────────────────────────────────────
SIGMA0_DIR  = Path("./data/sigma0")
HYDRO_DB    = Path("./data/hydro_data.db")
OUT_DIR     = Path("./data/comparison")
SNAP_DAYS   = 5   # fenêtre d'appariement ±N jours


# ═════════════════════════════════════════════════════════════
# CHARGEMENT
# ═════════════════════════════════════════════════════════════

def load_cnes(station_code: str) -> pd.DataFrame:
    """Charge le CSV CNES d'une station."""
    path = SIGMA0_DIR / f"{station_code}.csv"
    if not path.exists():
        return None
    df = pd.read_csv(path, parse_dates=["date"])
    df["date"] = df["date"].dt.normalize()
    df = df.sort_values("date").drop_duplicates("date").reset_index(drop=True)
    # Renomme pour clarté
    df = df.rename(columns={
        "WSH":          "wsh_cnes",
        "LATITUDE":     "lat_cnes",
        "LONGITUDE":    "lon_cnes",
        "CYCLE_NUMBER": "cycle_cnes",
    })
    return df[["date", "wsh_cnes", "lat_cnes", "lon_cnes", "cycle_cnes"]].dropna(subset=["wsh_cnes"])


def load_hydroweb(station_code: str) -> pd.DataFrame:
    """
    Charge les mesures HydroWeb depuis la BDD SQLite.
    Essaie le code tel quel, puis sans zeros en tete, puis zero-padde 4/5/6 chiffres.
    """
    conn = sqlite3.connect(HYDRO_DB)
    query = """
        SELECT
            measure_date            AS date,
            orthometric_height      AS wsh_hw,  -- hauteur orthometrique = meme referentiel que WSH CNES
            latitude                AS lat_hw,
            longitude               AS lon_hw,
            cycle_number            AS cycle_hw,
            track_number            AS track_hw,
            satellite               AS satellite_hw,
            ellipsoidal_height      AS ellipso_hw,
            uncertainty             AS uncertainty_hw
        FROM measurements
        WHERE station_code = ?
        ORDER BY measure_date
    """
    # Candidats : code brut, sans zeros en tete, zero-padde 4/5/6 chiffres
    code_int = station_code.lstrip("0") or "0"
    candidates = list(dict.fromkeys([
        station_code,
        code_int,
        code_int.zfill(4),
        code_int.zfill(5),
        code_int.zfill(6),
        code_int.zfill(13),
    ]))  # deduplication en preservant l ordre

    df = pd.DataFrame()
    for code in candidates:
        df = pd.read_sql_query(query, conn, params=(code,))
        if not df.empty:
            break

    conn.close()
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    df = df.sort_values("date").drop_duplicates("date").reset_index(drop=True)
    return df


# ═════════════════════════════════════════════════════════════
# COORDONNEES DE REFERENCE DEPUIS LA TABLE STATIONS
# ═════════════════════════════════════════════════════════════

def load_station_ref(station_code: str) -> tuple[float, float]:
    """Retourne (ref_lat, ref_lon) depuis la table stations."""
    conn = sqlite3.connect(HYDRO_DB)
    code_int = station_code.lstrip("0") or "0"
    candidates = list(dict.fromkeys([
        station_code, code_int,
        code_int.zfill(4), code_int.zfill(5),
        code_int.zfill(6), code_int.zfill(13),
    ]))
    ref_lat, ref_lon = np.nan, np.nan
    for code in candidates:
        cur = conn.execute(
            "SELECT reference_latitude, reference_longitude FROM stations WHERE station_code = ?",
            (code,)
        )
        row = cur.fetchone()
        if row and row[0] is not None:
            ref_lat, ref_lon = float(row[0]), float(row[1])
            break
    conn.close()
    return ref_lat, ref_lon


# ═════════════════════════════════════════════════════════════
# APPARIEMENT PAR CYCLE (exact) + fallback DATE (snap ±N jours)
# ═════════════════════════════════════════════════════════════

def pair_by_cycle(df_c: pd.DataFrame, df_h: pd.DataFrame, snap: int) -> pd.DataFrame:
    """
    Apparie CNES et HydroWeb sur cycle_number exact.
    Seuls les points validés HydroWeb (présents dans la BDD) sont utilisés
    → même référentiel, même passage satellite.
    Fallback sur snap temporel si cycle_hw est nul/manquant.
    Retourne uniquement les paires avec cycle identique.
    """
    # Normalise les cycles en int (NaN → -1 pour éviter faux match)
    df_c = df_c.copy()
    df_h = df_h.copy()
    df_c["cycle_int"] = df_c["cycle_cnes"].fillna(-1).astype(int)
    df_h["cycle_int"] = df_h["cycle_hw"].fillna(-1).astype(int)

    rows = []
    for _, rc in df_c.iterrows():
        cyc = rc["cycle_int"]

        # ── Cas 1 : cycle valide → match exact sur cycle ────
        if cyc >= 0:
            matches = df_h[df_h["cycle_int"] == cyc]
            if not matches.empty:
                rh = matches.iloc[0]
                rows.append(_make_row(rc, rh, match_type="cycle"))
                continue

        # ── Cas 2 : fallback snap temporel ──────────────────
        delta = (df_h["date"] - rc["date"]).abs()
        i = delta.idxmin()
        if delta[i].days <= snap:
            rh = df_h.loc[i]
            # N'inclut que si les cycles concordent (ou sont inconnus)
            if (rh["cycle_int"] < 0 or cyc < 0 or rh["cycle_int"] == cyc):
                rows.append(_make_row(rc, rh, match_type="date_snap"))

    df_p = pd.DataFrame(rows)
    # Supprime les doublons (même cycle HW apparié deux fois)
    if not df_p.empty and "cycle_cnes" in df_p.columns:
        df_p = df_p.drop_duplicates(subset=["cycle_cnes"])
    return df_p


def _make_row(rc, rh, match_type: str) -> dict:
    return {
        "match_type":  match_type,
        # Dates
        "date_cnes":   rc["date"],
        "date_hw":     rh["date"],
        "delta_days":  int((rc["date"] - rh["date"]).days),
        # WSH — uniquement points validés HydroWeb
        "wsh_cnes":    rc["wsh_cnes"],
        "wsh_hw":      rh["wsh_hw"],
        "diff_wsh":    rc["wsh_cnes"] - rh["wsh_hw"],
        # Cycle
        "cycle_cnes":  rc["cycle_cnes"],
        "cycle_hw":    rh["cycle_hw"],
        "cycle_match": (rc["cycle_int"] == rh["cycle_int"])
                       if rc["cycle_int"] >= 0 and rh["cycle_int"] >= 0
                       else None,
        # Coordonnées CNES
        "lat_cnes":    rc["lat_cnes"],
        "lon_cnes":    rc["lon_cnes"],
        # Infos HW supplémentaires
        "satellite_hw":   rh.get("satellite_hw", ""),
        "uncertainty_hw": rh.get("uncertainty_hw", np.nan),
        "ellipso_hw":     rh.get("ellipso_hw", np.nan),
    }


# ═════════════════════════════════════════════════════════════
# DIAGNOSTIC D'UNE STATION
# ═════════════════════════════════════════════════════════════

def diagnose(station_code: str, snap: int = SNAP_DAYS,
             plot: bool = False, out_dir: Path = OUT_DIR) -> dict:

    df_c = load_cnes(station_code)
    df_h = load_hydroweb(station_code)

    res = {"station": station_code}

    if df_c is None or df_c.empty:
        res["status"] = "NO_CSV";   return res
    if df_h is None or df_h.empty:
        res["status"] = "NO_HW";    return res

    res["n_cnes"] = len(df_c)
    res["n_hw"]   = len(df_h)
    res["date_min_cnes"] = str(df_c["date"].min().date())
    res["date_max_cnes"] = str(df_c["date"].max().date())
    res["date_min_hw"]   = str(df_h["date"].min().date())
    res["date_max_hw"]   = str(df_h["date"].max().date())

    # ── Overlap temporel ─────────────────────────────────────
    ov_start = max(df_c["date"].min(), df_h["date"].min())
    ov_end   = min(df_c["date"].max(), df_h["date"].max())
    res["overlap_days"] = max(0, (ov_end - ov_start).days)
    if res["overlap_days"] == 0:
        res["status"] = "NO_OVERLAP"; return res

    # ── Appariement ──────────────────────────────────────────
    df_p = pair_by_cycle(df_c, df_h, snap)
    res["n_paired"] = len(df_p)
    if len(df_p) < 3:
        res["status"] = "TOO_FEW_PAIRS"; return res

    # ── 1. DATES ─────────────────────────────────────────────
    res["mean_delta_days"]    = round(df_p["delta_days"].mean(), 2)
    res["max_abs_delta_days"] = int(df_p["delta_days"].abs().max())
    res["pct_exact_date"]     = round((df_p["delta_days"] == 0).mean() * 100, 1)

    # ── 2. CYCLES ────────────────────────────────────────────
    valid_cycles = df_p["cycle_match"].dropna()
    res["pct_cycle_match"] = round(valid_cycles.mean() * 100, 1) if len(valid_cycles) else np.nan
    # Décalage moyen de numéro de cycle
    both = df_p[df_p["cycle_cnes"].notna() & df_p["cycle_hw"].notna()]
    if len(both):
        res["mean_cycle_offset"] = round((both["cycle_cnes"] - both["cycle_hw"]).mean(), 2)
        res["std_cycle_offset"]  = round((both["cycle_cnes"] - both["cycle_hw"]).std(), 2)

    # ── 3. COORDONNÉES ───────────────────────────────────────
    ref_lat, ref_lon = load_station_ref(station_code)
    res["ref_lat_hw"] = round(ref_lat, 5) if not np.isnan(ref_lat) else np.nan
    res["ref_lon_hw"] = round(ref_lon, 5) if not np.isnan(ref_lon) else np.nan

    cnes_lat = df_p["lat_cnes"].mean()
    cnes_lon = df_p["lon_cnes"].mean()
    res["mean_lat_cnes"] = round(cnes_lat, 5)
    res["mean_lon_cnes"] = round(cnes_lon, 5)

    if not np.isnan(ref_lat):
        res["mean_delta_lat_m"] = round((cnes_lat - ref_lat) * 111000, 1)
        res["mean_delta_lon_m"] = round((cnes_lon - ref_lon) * 111000, 1)
    else:
        res["mean_delta_lat_m"] = np.nan
        res["mean_delta_lon_m"] = np.nan

    # % d'appariements par cycle exact vs fallback date
    if "match_type" in df_p.columns:
        res["pct_cycle_match"] = round(
            (df_p["match_type"] == "cycle").mean() * 100, 1)
    else:
        res["pct_cycle_match"] = np.nan

    # ── 4. WSH — valeurs absolues ────────────────────────────
    diff = df_p["diff_wsh"].dropna()
    res["bias_m"]      = round(float(diff.mean()), 4)
    res["std_diff_m"]  = round(float(diff.std()), 4)
    res["rmse_m"]      = round(float(np.sqrt((diff**2).mean())), 4)
    res["mae_m"]       = round(float(diff.abs().mean()), 4)
    res["min_diff_m"]  = round(float(diff.min()), 4)
    res["max_diff_m"]  = round(float(diff.max()), 4)

    # Corrélation pearson sur valeurs absolues
    c, h = df_p["wsh_cnes"].values, df_p["wsh_hw"].values

    # ── Normalisation (z-score par série) ───────────────────
    c_mean, c_std = np.mean(c), np.std(c)
    h_mean, h_std = np.mean(h), np.std(h)
    if c_std > 0 and h_std > 0:
        c_norm = (c - c_mean) / c_std
        h_norm = (h - h_mean) / h_std
        diff_norm = c_norm - h_norm
        res["bias_norm"]   = round(float(np.mean(diff_norm)), 4)
        res["rmse_norm"]   = round(float(np.sqrt((diff_norm**2).mean())), 4)
        res["mae_norm"]    = round(float(np.abs(diff_norm).mean()), 4)
        res["amp_ratio"]   = round(float(c_std / h_std), 4)
    else:
        res["bias_norm"] = res["rmse_norm"] = res["mae_norm"] = res["amp_ratio"] = np.nan

    if np.std(c) > 0 and np.std(h) > 0:
        r, p = stats.pearsonr(c, h)
        res["pearson_r"] = round(float(r), 4)
        res["pearson_p"] = round(float(p), 6)
    else:
        res["pearson_r"] = np.nan
        res["pearson_p"] = np.nan

    # ── 5. WSH — dynamique (différences inter-mesures) ───────
    if len(df_p) >= 4:
        dc = np.diff(df_p["wsh_cnes"].values)
        dh = np.diff(df_p["wsh_hw"].values)
        if np.std(dc) > 0 and np.std(dh) > 0:
            rd, pd_ = stats.pearsonr(dc, dh)
            res["r_delta"]          = round(float(rd), 4)
            res["pct_same_sign"]    = round(float(np.mean(np.sign(dc) == np.sign(dh)) * 100), 1)
            res["dynamic_inverted"] = bool(rd < -0.3 and pd_ < 0.05)
        else:
            res["r_delta"] = res["pct_same_sign"] = np.nan
            res["dynamic_inverted"] = False
    else:
        res["r_delta"] = res["pct_same_sign"] = np.nan
        res["dynamic_inverted"] = False

    # ── Debug coordonnees aberrantes ────────────────────────
    if not np.isnan(res.get("mean_delta_lat_m", np.nan)) and (
            abs(res.get("mean_delta_lat_m", 0)) > 1000 or
            abs(res.get("mean_delta_lon_m", 0)) > 1000):
        print(f"    ⚠  {station_code} coords aberrantes:")
        print(f"       CNES  lat={res['mean_lat_cnes']:.5f}  lon={res['mean_lon_cnes']:.5f}")
        print(f"       HW ref lat={res['ref_lat_hw']:.5f}    lon={res['ref_lon_hw']:.5f}")
        print(f"       Δlat={res['mean_delta_lat_m']:.1f}m  Δlon={res['mean_delta_lon_m']:.1f}m")

    # ── Status ───────────────────────────────────────────────
    if res["dynamic_inverted"]:
        res["status"] = "DYNAMIC_INVERTED"
    elif res["rmse_m"] < 0.3 and res["pearson_r"] > 0.8:
        res["status"] = "GOOD"
    elif res["rmse_m"] < 1.0 and res["pearson_r"] > 0.5:
        res["status"] = "OFFSET_ONLY"
    else:
        res["status"] = "POOR"

    # ── Sauvegarde CSV détaillé des paires ───────────────────
    out_dir.mkdir(parents=True, exist_ok=True)
    df_p.to_csv(out_dir / f"pairs_{station_code}.csv", index=False)

    # ── Figure optionnelle ───────────────────────────────────
    if plot:
        _plot(station_code, df_c, df_h, df_p, res, out_dir)

    return res


# ═════════════════════════════════════════════════════════════
# FIGURE
# ═════════════════════════════════════════════════════════════

def _plot(sid, df_c, df_h, df_p, res, out_dir):
    import matplotlib; matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates

    fig, axes = plt.subplots(3, 1, figsize=(14, 11))
    fig.suptitle(
        f"Station {sid}  |  status={res['status']}  |  n={res['n_paired']} paires\n"
        f"bias={res['bias_m']:+.3f}m  RMSE={res['rmse_m']:.3f}m  "
        f"r={res['pearson_r']:.3f}  r_delta={res['r_delta']:.3f}  "
        f"Δcycle={res.get('mean_cycle_offset', 'n/a')}  "
        f"Δlat={res['mean_delta_lat_m']:.0f}m",
        fontsize=10, y=0.99
    )

    fmt = mdates.DateFormatter("%Y-%m")
    loc = mdates.MonthLocator(interval=3)

    # Panneau 1 : séries brutes
    ax = axes[0]
    ax.plot(df_c["date"], df_c["wsh_cnes"], "b.-", lw=1, ms=4, label="WSH CNES")
    ax.plot(df_h["date"], df_h["wsh_hw"],   "r.-", lw=1, ms=4, label="HydroWeb (ellipsoïdal)")
    ax.set_ylabel("WSH (m)"); ax.set_title("Séries temporelles brutes")
    ax.legend(fontsize=9); ax.xaxis.set_major_formatter(fmt); ax.xaxis.set_major_locator(loc)
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=30, ha="right")

    # Panneau 2 : paires + différence
    ax = axes[1]
    if len(df_p):
        ax.plot(df_p["date_cnes"], df_p["wsh_cnes"], "b.-", lw=1, ms=5, label="CNES")
        ax.plot(df_p["date_cnes"], df_p["wsh_hw"],   "r.-", lw=1, ms=5, label="HydroWeb")
        ax2 = ax.twinx()
        ax2.bar(df_p["date_cnes"], df_p["diff_wsh"],
                color="purple", alpha=0.4, width=8, label="Δ CNES-HW")
        ax2.axhline(0, color="purple", lw=0.8, ls="--")
        ax2.set_ylabel("Différence (m)", color="purple")
    ax.set_ylabel("WSH (m)")
    ax.set_title(f"Paires appariées (snap ±{SNAP_DAYS}j)")
    ax.legend(fontsize=9, loc="upper left")
    ax.xaxis.set_major_formatter(fmt); ax.xaxis.set_major_locator(loc)
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=30, ha="right")

    # Panneau 3 : scatter
    ax = axes[2]
    sc = ax.scatter(df_p["wsh_hw"], df_p["wsh_cnes"],
                    c=df_p["delta_days"], cmap="RdYlGn_r", s=30, alpha=0.7,
                    vmin=-SNAP_DAYS, vmax=SNAP_DAYS)
    plt.colorbar(sc, ax=ax, label="Δ jours (CNES - HW)")
    lims = [min(df_p["wsh_hw"].min(), df_p["wsh_cnes"].min()),
            max(df_p["wsh_hw"].max(), df_p["wsh_cnes"].max())]
    ax.plot(lims, lims, "k--", lw=1, label="y=x")
    if np.std(df_p["wsh_hw"]) > 0:
        m, b, *_ = stats.linregress(df_p["wsh_hw"], df_p["wsh_cnes"])
        ax.plot(lims, [m*x + b for x in lims], "b-", lw=1.5,
                label=f"régression (pente={m:.2f}, b={b:.2f})")
    ax.set_xlabel("HydroWeb ellipsoïdal (m)")
    ax.set_ylabel("WSH CNES (m)")
    ax.set_title(f"Scatter — r={res['pearson_r']:.3f}")
    ax.legend(fontsize=9)

    plt.tight_layout()
    fig.savefig(out_dir / f"compare_{sid}.png", dpi=120, bbox_inches="tight")
    plt.close(fig)


# ═════════════════════════════════════════════════════════════
# MAIN
# ═════════════════════════════════════════════════════════════

def main():
    global SIGMA0_DIR, HYDRO_DB, OUT_DIR, SNAP_DAYS

    parser = argparse.ArgumentParser()
    parser.add_argument("--station",   default=None, help="Code station (ex: 5718)")
    parser.add_argument("--snap_days", type=int, default=SNAP_DAYS)
    parser.add_argument("--plot",      action="store_true")
    parser.add_argument("--sigma0_dir", default=str(SIGMA0_DIR))
    parser.add_argument("--hydro_db",   default=str(HYDRO_DB))
    parser.add_argument("--out_dir",    default=str(OUT_DIR))
    args = parser.parse_args()

    SIGMA0_DIR = Path(args.sigma0_dir)
    HYDRO_DB   = Path(args.hydro_db)
    OUT_DIR    = Path(args.out_dir)
    SNAP_DAYS  = args.snap_days
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # Liste des stations à traiter
    if args.station:
        stations = [args.station]
    else:
        # Toutes les stations dont un CSV existe
        stations = sorted([p.stem for p in SIGMA0_DIR.glob("*.csv")
                           if p.stem != "sigma0_all_stations"])

    print(f"{'Station':<12} {'Status':<20} {'N':>4} {'Δdate':>6} "
          f"{'bias':>8} {'RMSE':>7} {'r':>6} {'r_Δ':>6} "
          f"{'amp_ratio':>10} {'RMSE_norm':>10} {'Δlat_m':>10}")
    print("─" * 110)

    all_res = []
    for sid in stations:
        r = diagnose(sid, snap=SNAP_DAYS, plot=args.plot, out_dir=OUT_DIR)
        all_res.append(r)

        icon = {"GOOD": "✅", "OFFSET_ONLY": "🟡", "POOR": "🔴",
                "DYNAMIC_INVERTED": "🔄", "NO_OVERLAP": "⬜",
                "TOO_FEW_PAIRS": "⚪", "NO_CSV": "—", "NO_HW": "—"
                }.get(r.get("status", ""), "❓")

        print(f"{icon} {sid:<10} {r.get('status','?'):<20} "
              f"{r.get('n_paired', 0):>4} "
              f"{r.get('mean_delta_days', float('nan')):>+6.1f} "
              f"{r.get('bias_m', float('nan')):>+8.3f} "
              f"{r.get('rmse_m', float('nan')):>7.3f} "
              f"{r.get('pearson_r', float('nan')):>6.3f} "
              f"{r.get('r_delta', float('nan')):>6.3f} "
              f"{r.get('amp_ratio', float('nan')):>10.3f} "
              f"{r.get('rmse_norm', float('nan')):>10.3f} "
              f"{r.get('mean_delta_lat_m', float('nan')):>+10.1f}")

    # Synthèse
    df_out = pd.DataFrame(all_res)
    out_csv = OUT_DIR / "wsh_comparison_summary.csv"
    df_out.to_csv(out_csv, index=False)

    print(f"\n✅ {out_csv}  ({len(df_out)} stations)")
    if "status" in df_out.columns:
        print("\n── Statuts ──")
        print(df_out["status"].value_counts().to_string())
    if "dynamic_inverted" in df_out.columns:
        print(f"\n🔄 Dynamique inversée : {int(df_out['dynamic_inverted'].sum())} stations")
    if "pearson_r" in df_out.columns:
        v = df_out["pearson_r"].dropna()
        print(f"📊 r Pearson : médiane={v.median():.3f}  >0.8: {(v>0.8).sum()}  <0.5: {(v<0.5).sum()}")

if __name__ == "__main__":
    main()