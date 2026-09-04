"""
build_random_removal_control.py
════════════════════════════════════════════════════════════════════════
Test de contrôle simple : pour chaque dataset x méthode, on connaît déjà
(depuis table_outlier_nse_removal.png) le NOMBRE de points retirés et le
Δ NSE obtenu en retirant CES points précis (les outliers détectés). Ce
script retire à la place N points CHOISIS AU HASARD (même N, mais sans
lien avec les outliers) et recalcule le NSE -- répété sur plusieurs
tirages pour obtenir une distribution.

Si le Δ NSE réel (retrait ciblé) est nettement au-dessus de tout ce
qu'un retrait aléatoire produit, ça confirme que l'amélioration vient
bien de la détection des outliers, pas d'un simple effet de réduction
de l'échantillon (qui améliorerait le NSE même en retirant n'importe
quels points, par pur hasard statistique).

Chiffres réels (n_total, %flagué, Δ réel) copiés DIRECTEMENT depuis le
tableau déjà généré -- pas recalculés ici, pas besoin des fichiers
Quantile pour ça.

Sources (uniquement pour charger obs/insitu, pas les quantiles) :
  Models_Testing/Quantille/residus/residuals_Quantile96_{SOURCE}_{freq}_bands.csv
  Models_Testing/Quantille/residus/metrics_Quantile96_{SOURCE}_{freq}_sword_insitu.csv
  data/insitu_data.db

Sortie :
  Models_Testing/Quantille/figures/table_random_removal_control.png
  + CSV équivalent
════════════════════════════════════════════════════════════════════════
"""

import random
import sqlite3
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
from pathlib import Path

PRIMARY_MODEL = "Quantile96"
WINDOW_DAYS = {"10j": 5, "27j": 14}
MIN_PAIRS = 10
N_TRIALS = 300
RANDOM_SEED = 42

RESIDUS_DIR = Path("./Models_Testing/Quantille/residus")
OUT_DIR = Path("./Models_Testing/Quantille/figures")
INSITU_DB = "./data/insitu_data.db"
DATE_MIN, DATE_MAX = "2016-01-01", "2025-12-31"

# ── Chiffres copiés directement du tableau déjà généré (pct_flagué, Δ réel) ──
DATASETS = [
    {"origin": "HW Next 10j", "source": "hwnext", "freq": "10j", "n_total": 8208,
     "methods": [("≥2/3 modèles", 29.85, 0.330), ("3/3 modèles", 17.76, 0.288), ("3/3 + 0.5σ", 11.21, 0.268)]},
    {"origin": "HW Next 27j", "source": "hwnext", "freq": "27j", "n_total": 13093,
     "methods": [("≥2/3 modèles", 43.28, 0.291), ("3/3 modèles", 27.70, 0.252), ("3/3 + 0.5σ", 17.29, 0.210)]},
    {"origin": "Dahiti 10j", "source": "dahiti", "freq": "10j", "n_total": 3659,
     "methods": [("≥2/3 modèles", 26.51, 0.117), ("3/3 modèles", 15.22, 0.071), ("3/3 + 0.5σ", 8.39, 0.016)]},
    {"origin": "Dahiti 27j", "source": "dahiti", "freq": "27j", "n_total": 9092,
     "methods": [("≥2/3 modèles", 30.72, 0.165), ("3/3 modèles", 18.36, 0.123), ("3/3 + 0.5σ", 10.27, 0.097)]},
]

COLOR_HEADER = "#2C3E50"
COLOR_GROUP = "#5D6D7E"
COLOR_HEADER_TEXT = "white"
COLOR_ROW_A = "#F7F9F9"
COLOR_ROW_B = "#FFFFFF"
COLOR_GAIN_POS = "#1E8449"
COLOR_NEUTRAL = "#7F8C8D"
COLOR_GRID = "#D5D8DC"
COLOR_SUBTEXT = "#7F8C8D"


# ═══════════════════════════════════════════════════════════════
# CHARGEMENT (obs + insitu, PRIMARY_MODEL uniquement)
# ═══════════════════════════════════════════════════════════════
_cache_ins = {}


def get_insitu_series(code_sta):
    if code_sta not in _cache_ins:
        conn = sqlite3.connect(INSITU_DB)
        df = pd.read_sql("""
            SELECT date, h_med_wsh AS wl FROM mesures_insitu
            WHERE code_sta = ? AND date >= ? AND date <= ? ORDER BY date
        """, conn, params=(code_sta, DATE_MIN, DATE_MAX))
        conn.close()
        df["date"] = pd.to_datetime(df["date"])
        _cache_ins[code_sta] = df.dropna(subset=["wl"])
    return _cache_ins[code_sta]


def align_insitu(dates, df_ins, window_days):
    wl = np.full(len(dates), np.nan)
    if df_ins.empty:
        return wl
    id_ = np.array(df_ins["date"].values, dtype="datetime64[D]")
    iv = df_ins["wl"].values
    for i, d in enumerate(np.array(dates, dtype="datetime64[D]")):
        diff = np.abs((id_ - d).astype(float))
        idx = int(np.argmin(diff))
        if diff[idx] <= window_days:
            wl[i] = iv[idx]
    return wl


def zscore_params(values):
    v = np.asarray(values, dtype=float)
    v = v[~np.isnan(v)]
    if len(v) < 2:
        return 0.0, 1.0
    mu, sig = v.mean(), v.std()
    return mu, (sig if sig > 0 else 1.0)


def nse(obs, pred):
    obs, pred = np.asarray(obs, dtype=float), np.asarray(pred, dtype=float)
    if len(obs) < MIN_PAIRS:
        return np.nan
    denom = np.sum((obs - obs.mean()) ** 2)
    return 1 - np.sum((obs - pred) ** 2) / denom if denom > 0 else np.nan


def build_station_series(source: str, freq: str) -> dict:
    """Retourne {station: (dates, obs_z, ins_z, mask_valid)} pour PRIMARY_MODEL."""
    path = RESIDUS_DIR / f"residuals_{PRIMARY_MODEL}_{source}_{freq}_bands.csv"
    df = pd.read_csv(path)
    df["station"] = df["station"].astype(str)
    df["date"] = pd.to_datetime(df["date"])
    df = df.dropna(subset=["obs"])

    met_path = RESIDUS_DIR / f"metrics_{PRIMARY_MODEL}_{source}_{freq}_sword_insitu.csv"
    df_met = pd.read_csv(met_path)
    df_met["station"] = df_met["station"].astype(str)
    insitu_code_by_station = dict(zip(df_met["station"], df_met["insitu_code"]))

    stations = {}
    for station, sub in df.groupby("station"):
        insitu_code = insitu_code_by_station.get(station)
        if not insitu_code:
            continue
        sub = sub.sort_values("date")
        if len(sub) < MIN_PAIRS:
            continue
        df_ins = get_insitu_series(insitu_code)
        ins_wl = align_insitu(sub["date"].values, df_ins, WINDOW_DAYS[freq])
        obs_mu, obs_sigma = zscore_params(sub["obs"].values)
        obs_z = (sub["obs"].values - obs_mu) / obs_sigma
        ins_mu, ins_sigma = zscore_params(ins_wl)
        ins_z = (ins_wl - ins_mu) / ins_sigma
        mask_valid = ~(np.isnan(obs_z) | np.isnan(ins_z))
        if mask_valid.sum() < MIN_PAIRS:
            continue
        stations[station] = (sub["date"].values, obs_z, ins_z, mask_valid)
    return stations


# ═══════════════════════════════════════════════════════════════
# NSE MÉDIAN SUR L'ENSEMBLE DES STATIONS, AVEC UN SOUS-ENSEMBLE DE
# POINTS RETIRÉS (donné par un set de (station, date))
# ═══════════════════════════════════════════════════════════════
def median_nse_excluding(stations: dict, removed: set) -> float:
    vals = []
    for station, (dates, obs_z, ins_z, mask_valid) in stations.items():
        if removed:
            is_removed = np.array([(station, d) in removed for d in dates])
        else:
            is_removed = np.zeros(len(dates), dtype=bool)
        mask = mask_valid & ~is_removed
        v = nse(ins_z[mask], obs_z[mask])
        if pd.notna(v):
            vals.append(v)
    return float(np.median(vals)) if vals else np.nan


def build_global_pool(stations: dict) -> list:
    pool = []
    for station, (dates, obs_z, ins_z, mask_valid) in stations.items():
        for d, ok in zip(dates, mask_valid):
            if ok:
                pool.append((station, d))
    return pool


# ═══════════════════════════════════════════════════════════════
# BOUCLE PRINCIPALE
# ═══════════════════════════════════════════════════════════════
def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    rng = random.Random(RANDOM_SEED)
    rows = []

    for ds in DATASETS:
        print(f"\n{ds['origin']} :")
        stations = build_station_series(ds["source"], ds["freq"])
        nse_baseline = median_nse_excluding(stations, removed=set())
        pool = build_global_pool(stations)
        print(f"  NSE baseline (tous points) = {nse_baseline:.3f}  ({len(pool)} points valides)")

        for method, pct_flagged, delta_real in ds["methods"]:
            n_remove = round(pct_flagged / 100 * ds["n_total"])
            n_remove = min(n_remove, len(pool))

            random_deltas = []
            for _ in range(N_TRIALS):
                removed_sample = set(rng.sample(pool, n_remove))
                nse_random = median_nse_excluding(stations, removed_sample)
                if pd.notna(nse_random):
                    random_deltas.append(nse_random - nse_baseline)

            mean_rand = float(np.mean(random_deltas))
            std_rand = float(np.std(random_deltas))
            pct_random_beat_real = round(100 * np.mean(np.array(random_deltas) >= delta_real), 1)
            z_score = (delta_real - mean_rand) / std_rand if std_rand > 0 else np.nan

            print(f"  [{method}] n_retirés={n_remove}  Δréel={delta_real:+.3f}  "
                  f"Δhasard={mean_rand:+.3f}±{std_rand:.3f}  "
                  f"({pct_random_beat_real}% des tirages aléatoires font aussi bien ou mieux)")

            rows.append({
                "origin": ds["origin"], "method": method, "n_removed": n_remove,
                "delta_real": delta_real, "delta_random_mean": round(mean_rand, 3),
                "delta_random_std": round(std_rand, 3), "z_score": round(z_score, 1),
                "pct_random_as_good": pct_random_beat_real,
            })

    df = pd.DataFrame(rows)
    df.to_csv(OUT_DIR / "table_random_removal_control.csv", index=False)
    render_table(df, OUT_DIR / "table_random_removal_control.png")


# ═══════════════════════════════════════════════════════════════
# RENDU PNG
# ═══════════════════════════════════════════════════════════════
def render_table(df: pd.DataFrame, out_path: Path) -> None:
    columns = [
        ("Méthode", "method", "text", 1.3),
        ("n retirés", "n_removed", "int", 0.9),
        ("Δ NSE réel\n(outliers ciblés)", "delta_real", "float_accent", 1.2),
        ("Δ NSE hasard\n(moyenne ± écart-type)", "random_fmt", "text", 1.6),
        ("% tirages aléatoires\naussi bons ou mieux", "pct_random_as_good", "pct_warn", 1.4),
    ]
    n_cols = len(columns)
    widths = [w for _, _, _, w in columns]
    widths = [w / sum(widths) for w in widths]

    row_h, group_h, header_h, title_h, footnote_h = 0.5, 0.45, 0.75, 0.55, 0.7
    origins = list(dict.fromkeys(df["origin"]))
    n_rows = len(df)
    fig_h = title_h + header_h + len(origins) * group_h + n_rows * row_h + footnote_h + 0.15
    fig_w = 1.9 * n_cols + 1.0

    df = df.assign(random_fmt=df.apply(
        lambda r: f"{r['delta_random_mean']:+.3f} ± {r['delta_random_std']:.3f}", axis=1))

    fig = plt.figure(figsize=(fig_w, fig_h), dpi=200)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_xlim(0, fig_w)
    ax.set_ylim(0, fig_h)
    ax.invert_yaxis()
    ax.axis("off")

    margin = 0.15
    avail_w = fig_w - 2 * margin
    x_edges = [margin]
    for w in widths:
        x_edges.append(x_edges[-1] + w * avail_w)

    ax.text(fig_w / 2, title_h / 2, "Contrôle : retrait ciblé (outliers) vs retrait ALÉATOIRE (même nombre de points)",
            ha="center", va="center", fontsize=12, fontweight="bold", color=COLOR_HEADER)

    y = title_h
    separator_segments = [(y, y + header_h)]
    ax.add_patch(Rectangle((margin, y), avail_w, header_h, facecolor=COLOR_HEADER,
                            edgecolor="white", linewidth=1))
    for j, (label, _, _, _) in enumerate(columns):
        xc = (x_edges[j] + x_edges[j + 1]) / 2
        lines = label.split("\n")
        if len(lines) == 1:
            ax.text(xc, y + header_h / 2, label, ha="center", va="center",
                    fontsize=8.5, fontweight="bold", color=COLOR_HEADER_TEXT)
        else:
            ax.text(xc, y + header_h / 2 - 0.15, lines[0], ha="center", va="center",
                    fontsize=8, fontweight="bold", color=COLOR_HEADER_TEXT)
            ax.text(xc, y + header_h / 2 + 0.15, lines[1], ha="center", va="center",
                    fontsize=8, fontweight="bold", color=COLOR_HEADER_TEXT)
    y += header_h

    for origin in origins:
        sub_df = df[df["origin"] == origin].reset_index(drop=True)
        ax.add_patch(Rectangle((margin, y), avail_w, group_h, facecolor=COLOR_GROUP,
                                edgecolor="white", linewidth=1))
        ax.text(margin + 0.15, y + group_h / 2, origin, ha="left", va="center",
                fontsize=9.5, fontweight="bold", color="white")
        y += group_h
        row_block_start = y

        for i, r in sub_df.iterrows():
            band = COLOR_ROW_A if i % 2 == 0 else COLOR_ROW_B
            for j, (_, key, kind, _) in enumerate(columns):
                xc = (x_edges[j] + x_edges[j + 1]) / 2
                ax.add_patch(Rectangle((x_edges[j], y), x_edges[j + 1] - x_edges[j], row_h,
                                        facecolor=band, edgecolor=COLOR_GRID, linewidth=0.6))
                if kind == "text" and key == "method":
                    ax.text(xc, y + row_h / 2, r[key], ha="center", va="center",
                            fontsize=9, fontweight="bold", color=COLOR_HEADER)
                elif kind == "text":
                    ax.text(xc, y + row_h / 2, r[key], ha="center", va="center",
                            fontsize=9, color=COLOR_NEUTRAL)
                elif kind == "int":
                    ax.text(xc, y + row_h / 2, f"{int(r[key]):,}".replace(",", " "),
                            ha="center", va="center", fontsize=9, color=COLOR_HEADER)
                elif kind == "float_accent":
                    ax.text(xc, y + row_h / 2, f"+{r[key]:.3f}", ha="center", va="center",
                            fontsize=9.5, fontweight="bold", color=COLOR_GAIN_POS)
                elif kind == "pct_warn":
                    val = r[key]
                    color = "#C0392B" if val > 5 else COLOR_HEADER
                    ax.text(xc, y + row_h / 2, f"{val:.1f}%", ha="center", va="center",
                            fontsize=9, fontweight="bold", color=color)
            y += row_h
        separator_segments.append((row_block_start, y))

    ax.add_patch(Rectangle((margin, title_h), avail_w, y - title_h, fill=False,
                            edgecolor=COLOR_HEADER, linewidth=1.2))
    for xe in x_edges[1:-1]:
        for y_start, y_end in separator_segments:
            ax.plot([xe, xe], [y_start, y_end], color=COLOR_GRID, linewidth=0.5)

    footnote = (f"{N_TRIALS} tirages aléatoires par ligne, même n retiré que le retrait ciblé  •  "
                "% tirages aussi bons = part des tirages aléatoires atteignant le Δ réel  •  "
                "proche de 0% = l'amélioration n'est PAS due au hasard")
    ax.text(margin, y + footnote_h / 2 - 0.05, footnote, ha="left", va="center",
            fontsize=7, color=COLOR_SUBTEXT, style="italic")

    fig.savefig(out_path, facecolor="white")
    plt.close(fig)
    print(f"\n✅ {out_path}")


if __name__ == "__main__":
    main()