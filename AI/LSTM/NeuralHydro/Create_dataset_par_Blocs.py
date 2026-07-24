"""
create_datasets_DtoD_periodic.py
════════════════════════════════════════════════════════════════════════
Crée les datasets NeuralHydroDtoD80_periodic / DtoD90_periodic /
DtoD96_periodic en copiant les .nc de NeuralHydroDtoD0 et en masquant le
water_level selon un PATTERN PÉRIODIQUE FIXE : 1 donnée gardée, puis
toujours le même nombre de jours en NaN, en boucle sur toute la série.

Motivation : le masquage aléatoire point-par-point (create_dataset_DtoD96.py
original) simule un dropout uniforme, ce qui ne correspond pas au vrai
comportement des satellites altimétriques, qui repassent au-dessus d'une
station à un cycle de revisite FIXE (période orbitale). Ce script imite
directement ce cycle réel :

  - DtoD80      : 1 donnée tous les 5 jours  -> gap fixe de 4 jours de NaN
  - DtoD90      : 1 donnée tous les 10 jours -> gap fixe de 9 jours de NaN
                  (imite un cycle de revisite ~10j)
  - DtoD96      : 1 donnée tous les 27 jours -> gap fixe de 26 jours de NaN
                  (imite un cycle de revisite ~27j)

Un déphasage aléatoire (tiré par station) évite que toutes les stations
soient synchronisées sur le même jour de "passage satellite" simulé.

Les 3 versions sont traitées dans la même passe sur les .nc sources, pour
éviter de relire/dupliquer 3 fois le dataset source.

Ne modifie PAS le dataset source (NeuralHydroDtoD0).

Usage :
    python create_datasets_DtoD_periodic.py
════════════════════════════════════════════════════════════════════════
"""

import numpy as np
import xarray as xr
import shutil
from pathlib import Path

# ═══════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═══════════════════════════════════════════════════════════════
SRC_DIR = Path("./data/IA/NeuralHydroDtoD0")

# Pour chaque modèle : la période de revisite simulée, en jours
# (1 donnée gardée tous les PERIOD jours -> gap = PERIOD - 1 jours de NaN)
#   80% -> gap de 4j  (period=5)
#   90% -> gap de 9j  (period=10, imite un cycle ~10j)
#   96% -> gap de 26j (period=27, imite un cycle ~27j)
PERIODS = {
    80: 5,
    90: 10,
    96: 27,
}

# Dossier racine clair, distinct des versions "random masking" existantes
OUT_ROOT = Path("./data/IA/DtoD_periodic_masking")

SEED = 42

SRC_TS = SRC_DIR / "time_series"
SRC_ATT = SRC_DIR / "attributes"
SRC_BASINS = Path("./AI/LSTM/NeuralHydroDtoD0")


# ═══════════════════════════════════════════════════════════════
# MASQUAGE PÉRIODIQUE FIXE
# ═══════════════════════════════════════════════════════════════
def periodic_mask(wl: np.ndarray, period: int, rng: np.random.Generator) -> np.ndarray:
    """
    Garde 1 point tous les `period` jours (indices du tableau = jours),
    masque tous les autres. Le point de départ du cycle (déphasage) est
    tiré au hasard entre 0 et period-1 pour éviter que toutes les
    stations soient synchronisées sur le même jour simulé de passage.

    Les positions déjà NaN dans la donnée d'origine restent NaN (rien à
    masquer de plus pour elles), les autres suivent strictement le motif
    "1 gardée / (period-1) masquées" en boucle.
    """
    wl = wl.copy()
    n = len(wl)
    offset = int(rng.integers(0, period))

    day_idx = np.arange(n)
    keep_mask = ((day_idx - offset) % period) == 0

    wl[~keep_mask] = np.nan
    return wl


# ═══════════════════════════════════════════════════════════════
# COPIE DES ATTRIBUTES / BASINS (une seule fois, communs aux 3 taux)
# ═══════════════════════════════════════════════════════════════
def setup_common_dirs(rate_pct: int) -> tuple[Path, Path]:
    dst_dir = OUT_ROOT / f"NeuralHydroDtoD{rate_pct}_periodic"
    dst_ts = dst_dir / "time_series"
    dst_att = dst_dir / "attributes"
    dst_ts.mkdir(parents=True, exist_ok=True)
    dst_att.mkdir(parents=True, exist_ok=True)

    print(f"Copie attributes -> {dst_att} ...")
    for f in SRC_ATT.glob("*"):
        shutil.copy2(f, dst_att / f.name)

    dst_basins = Path("./AI/LSTM") / f"DtoD_periodic_masking_{rate_pct}"
    dst_basins.mkdir(parents=True, exist_ok=True)
    for f in SRC_BASINS.glob("*.txt"):
        shutil.copy2(f, dst_basins / f.name)

    return dst_ts, dst_basins


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════
def main():
    OUT_ROOT.mkdir(parents=True, exist_ok=True)

    dst_ts_by_rate = {}
    for rate_pct in PERIODS:
        dst_ts, dst_basins = setup_common_dirs(rate_pct)
        dst_ts_by_rate[rate_pct] = dst_ts
        print(f"  basins copiés dans : {dst_basins}")

    nc_files = sorted(SRC_TS.glob("*.nc"))
    print(f"\n{len(nc_files)} fichiers .nc source à traiter "
          f"pour {len(PERIODS)} taux ({list(PERIODS.keys())})\n")

    # Un générateur aléatoire par taux, seedé indépendamment pour que les
    # déphasages ne soient pas identiques d'une version à l'autre
    rngs = {rate_pct: np.random.default_rng(SEED + rate_pct)
            for rate_pct in PERIODS}

    counts_ok = {rate_pct: 0 for rate_pct in PERIODS}
    counts_skip = {rate_pct: 0 for rate_pct in PERIODS}

    for i, src_path in enumerate(nc_files):
        try:
            ds = xr.open_dataset(src_path, engine="scipy")
        except Exception as e:
            print(f"  ⚠ lecture impossible {src_path.name} : {e}")
            for rate_pct in PERIODS:
                counts_skip[rate_pct] += 1
            continue

        wl_original = None
        if "water_level" in ds:
            wl_original = ds["water_level"].values.copy().astype(float)

        for rate_pct, period in PERIODS.items():
            dst_path = dst_ts_by_rate[rate_pct] / src_path.name

            if dst_path.exists():
                counts_ok[rate_pct] += 1
                continue

            try:
                ds_new = ds.copy(deep=True)

                if wl_original is not None:
                    wl_masked = periodic_mask(
                        wl_original, period, rngs[rate_pct],
                    )
                    ds_new["water_level"].values[:] = wl_masked

                ds_new.attrs["nan_rate_nominal"] = rate_pct / 100
                ds_new.attrs["masking_mode"] = "periodic"
                ds_new.attrs["period_days"] = period
                ds_new.attrs["gap_days"] = period - 1

                ds_new.to_netcdf(dst_path, engine="scipy",
                                  format="NETCDF3_CLASSIC")
                ds_new.close()
                counts_ok[rate_pct] += 1

            except Exception as e:
                print(f"  ⚠ {src_path.name} [{rate_pct}%] : {e}")
                counts_skip[rate_pct] += 1

        ds.close()

        if (i + 1) % 200 == 0:
            print(f"  {i + 1}/{len(nc_files)} fichiers source traités...")

    print(f"\n{'=' * 60}")
    print(f"  Dossier racine : {OUT_ROOT}")
    print(f"  Masquage       : périodique fixe (gap constant)")
    for rate_pct, period in PERIODS.items():
        print(f"  DtoD{rate_pct}_periodic : gap={period - 1}j "
              f"(period={period}j) : "
              f"{counts_ok[rate_pct]} générés, "
              f"{counts_skip[rate_pct]} skippés")
    print(f"{'=' * 60}")
    print("""
Configs NeuralHydrology à créer (une par taux) :

  experiment_name: arlstm_DtoD80_periodic
  data_dir: ./data/IA/DtoD_periodic_masking/NeuralHydroDtoD80_periodic
  train_basin_file: ./AI/LSTM/DtoD_periodic_masking_80/train_basins.txt
  validation_basin_file: ./AI/LSTM/DtoD_periodic_masking_80/val_basins.txt
  (idem pour 90 et 96, en remplaçant 80 par 90/96 partout)
  (reste identique aux configs DtoD existantes)
""")


if __name__ == "__main__":
    main()