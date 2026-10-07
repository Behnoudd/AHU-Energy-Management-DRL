#!/usr/bin/env python3
"""
validate_sb_distribution.py
----------------------------

Focused raw-data validation for the relationship between:
- PV generation meter (MI_B)
- Solar distribution meter (SB_C)
- ESB grid import meter (MI_A)
- Heat pump 1 (HP_A)
- Heat pump 2 (HP_B)

Goal:
- Determine whether the solar distribution meter behaves like a net boundary meter
  that includes local heat-pump consumption before export/import crosses the boundary.

Assumptions:
- All files are parquet
- Timestamp column is: CreatedOn
- Active power columns are:
    L1_PWR_ACTV, L2_PWR_ACTV, L3_PWR_ACTV
- PV, solar DB, HP1, HP2 are in kW
- Grid (MI_A) is in W, so convert to kW
- Raw polarity/sign should be preserved

Outputs:
- aligned_raw_timeseries.csv
- summary_metrics.txt
- several PNG figures

Usage:
    python validate_solar_db_balance.py
"""

from __future__ import annotations

from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# -------------------------------------------------------
# CONFIG
# -------------------------------------------------------
FILES = {
    "pv": "MI_B.parquet",
    "solar_db": "SB_C.parquet",
    "grid": "MI_A.parquet",
    "hp1": "HP_A.parquet",
    "hp2": "HP_B.parquet",
}

OUTDIR = Path("solar_db_validation")

GRID_IS_WATTS = True

TS_COL = "CreatedOn"
PHASE_POWER_COLS = ["L1_PWR_ACTV", "L2_PWR_ACTV", "L3_PWR_ACTV"]

# Short rolling smooth to make high-frequency plots readable
ROLLING_WINDOW = "30s"

# Choose one sunny window and one night window after first run if needed
PLOT_START = None   # e.g. "2025-06-15 10:00:00"
PLOT_END   = None   # e.g. "2025-06-15 16:00:00"

# -------------------------------------------------------
# HELPERS
# -------------------------------------------------------
def load_meter(path: str, name: str) -> pd.DataFrame:
    df = pd.read_parquet(path)

    missing = [c for c in [TS_COL] + PHASE_POWER_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"{name}: missing required columns: {missing}")

    df = df[[TS_COL] + PHASE_POWER_COLS].copy()
    df[TS_COL] = pd.to_datetime(df[TS_COL], errors="coerce")
    df = df.dropna(subset=[TS_COL]).sort_values(TS_COL)

    df[f"{name}_raw_sum"] = df[PHASE_POWER_COLS].sum(axis=1)

    if name == "grid" and GRID_IS_WATTS:
        df[f"{name}_kW"] = df[f"{name}_raw_sum"] / 1000.0
    else:
        df[f"{name}_kW"] = df[f"{name}_raw_sum"]

    return df[[TS_COL, f"{name}_kW"]]


def merge_asof_on_time(frames: list[pd.DataFrame], tolerance="2s") -> pd.DataFrame:
    """
    Align meters by nearest timestamp within tolerance.
    This is usually better than exact inner joins for historian-style streams.
    """
    frames = [f.sort_values(TS_COL).copy() for f in frames]
    out = frames[0]
    for nxt in frames[1:]:
        out = pd.merge_asof(
            out,
            nxt,
            on=TS_COL,
            direction="nearest",
            tolerance=pd.Timedelta(tolerance),
        )
    out = out.dropna().sort_values(TS_COL)
    return out


def fit_quality(y_true: pd.Series, y_pred: pd.Series) -> dict[str, float]:
    resid = y_true - y_pred
    mae = float(np.mean(np.abs(resid)))
    rmse = float(np.sqrt(np.mean(resid**2)))
    med = float(np.median(resid))
    p95_abs = float(np.percentile(np.abs(resid), 95))
    corr = float(y_true.corr(y_pred))
    return {
        "mae": mae,
        "rmse": rmse,
        "median_residual": med,
        "p95_abs_residual": p95_abs,
        "corr": corr,
    }

def write_metrics(path: Path, sections: dict[str, dict[str, float]]) -> None:
    lines = []
    for title, metrics in sections.items():
        lines.append(title)
        lines.append("-" * len(title))
        for k, v in metrics.items():
            lines.append(f"{k}: {v:.6f}")
        lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def maybe_slice(df: pd.DataFrame) -> pd.DataFrame:
    if PLOT_START and PLOT_END:
        return df.loc[PLOT_START:PLOT_END].copy()
    return df.copy()


def save_plot(df: pd.DataFrame, cols: list[str], title: str, fname: str, ylabel="Power (kW)") -> None:
    plt.figure(figsize=(14, 6))
    for c in cols:
        plt.plot(df.index, df[c], label=c)
    plt.title(title)
    plt.ylabel(ylabel)
    plt.xlabel("Time")
    plt.legend()
    plt.tight_layout()
    plt.savefig(OUTDIR / fname, dpi=300, bbox_inches="tight")
    plt.close()

# -------------------------------------------------------
# MAIN
# -------------------------------------------------------
def main() -> None:
    OUTDIR.mkdir(parents=True, exist_ok=True)

    frames = [load_meter(path, name) for name, path in FILES.items()]
    df = merge_asof_on_time(frames, tolerance="2s")

    if df.empty:
        raise ValueError("No aligned rows after merge_asof. Relax tolerance or inspect timestamps.")

    df = df.set_index(TS_COL).sort_index()

    # Smoothed version for plotting only
    smooth = df.rolling(ROLLING_WINDOW).mean()

    # Derived series
    df["hp_total"] = df["hp1_kW"] + df["hp2_kW"]

    # Candidate models for solar_db behavior
    # Model A: db ≈ hp_total - pv
    df["model_A"] = df["hp_total"] - df["pv_kW"]

    # Model B: db ≈ pv - hp_total
    df["model_B"] = df["pv_kW"] - df["hp_total"]

    # Model C: db ≈ hp_total (night-time/simple downstream import hypothesis)
    df["model_C"] = df["hp_total"]

    # Model D: db ≈ -(hp_total)
    df["model_D"] = -df["hp_total"]

    # Also inspect whether db aligns more with net boundary including grid:
    # Candidate net relationships
    df["pv_plus_grid"] = df["pv_kW"] + df["grid_kW"]
    df["pv_minus_grid"] = df["pv_kW"] - df["grid_kW"]
    df["grid_minus_pv"] = df["grid_kW"] - df["pv_kW"]

    # Residuals
    for m in ["A", "B", "C", "D"]:
        df[f"resid_{m}"] = df["solar_db_kW"] - df[f"model_{m}"]

    # Metrics
    sections = {
        "model_A_db_vs_hp_minus_pv": fit_quality(df["solar_db_kW"], df["model_A"]),
        "model_B_db_vs_pv_minus_hp": fit_quality(df["solar_db_kW"], df["model_B"]),
        "model_C_db_vs_hp_total": fit_quality(df["solar_db_kW"], df["model_C"]),
        "model_D_db_vs_neg_hp_total": fit_quality(df["solar_db_kW"], df["model_D"]),
        "db_vs_pv_plus_grid": fit_quality(df["solar_db_kW"], df["pv_plus_grid"]),
        "db_vs_pv_minus_grid": fit_quality(df["solar_db_kW"], df["pv_minus_grid"]),
        "db_vs_grid_minus_pv": fit_quality(df["solar_db_kW"], df["grid_minus_pv"]),
    }
    write_metrics(OUTDIR / "summary_metrics.txt", sections)

    # Save aligned raw series
    df.reset_index().to_csv(OUTDIR / "aligned_raw_timeseries.csv", index=False)

    # Console summary
    print(f"Aligned rows: {len(df):,}")
    print(f"Time span: {df.index.min()} -> {df.index.max()}")
    print("\nModel fit summary:")
    for name, metrics in sections.items():
        print(f"\n{name}")
        for k, v in metrics.items():
            print(f"  {k}: {v:.6f}")

    # Full-window smoothed plots
    smooth["hp_total"] = smooth["hp1_kW"] + smooth["hp2_kW"]
    smooth["model_A"] = smooth["hp_total"] - smooth["pv_kW"]
    smooth["model_B"] = smooth["pv_kW"] - smooth["hp_total"]
    smooth["pv_plus_grid"] = smooth["pv_kW"] + smooth["grid_kW"]
    smooth["pv_minus_grid"] = smooth["pv_kW"] - smooth["grid_kW"]
    smooth["grid_minus_pv"] = smooth["grid_kW"] - smooth["pv_kW"]

    save_plot(
        maybe_slice(smooth),
        ["pv_kW", "solar_db_kW", "grid_kW", "hp_total"],
        "Raw signed power comparison: PV, Solar DB, Grid, HP total",
        "01_power_comparison.png",
    )

    save_plot(
        maybe_slice(smooth),
        ["solar_db_kW", "model_A", "model_B"],
        "Solar DB versus candidate models: hp_total - pv and pv - hp_total",
        "02_db_vs_hp_pv_models.png",
    )

    save_plot(
        maybe_slice(smooth),
        ["solar_db_kW", "pv_plus_grid", "pv_minus_grid", "grid_minus_pv"],
        "Solar DB versus PV/grid balance candidates",
        "03_db_vs_pv_grid_candidates.png",
    )

    # Residual plots
    resid_plot = smooth.copy()
    resid_plot["resid_A"] = resid_plot["solar_db_kW"] - resid_plot["model_A"]
    resid_plot["resid_B"] = resid_plot["solar_db_kW"] - resid_plot["model_B"]
    resid_plot["resid_C"] = resid_plot["solar_db_kW"] - resid_plot["hp_total"]
    resid_plot["resid_D"] = resid_plot["solar_db_kW"] + resid_plot["hp_total"]

    save_plot(
        maybe_slice(resid_plot),
        ["resid_A", "resid_B"],
        "Residuals: Solar DB minus (hp_total - pv) and minus (pv - hp_total)",
        "04_residual_hp_pv_models.png",
        ylabel="Residual (kW)",
    )

    save_plot(
        maybe_slice(resid_plot),
        ["resid_C", "resid_D"],
        "Residuals: Solar DB minus hp_total and minus (-hp_total)",
        "05_residual_hp_only_models.png",
        ylabel="Residual (kW)",
    )

    # Night-time subset where PV is near zero
    night = df[np.abs(df["pv_kW"]) < 0.1].copy()
    if len(night) > 100:
        night_metrics = {
            "night_db_vs_hp_total": fit_quality(night["solar_db_kW"], night["hp_total"]),
            "night_db_vs_neg_hp_total": fit_quality(night["solar_db_kW"], -night["hp_total"]),
        }
        write_metrics(OUTDIR / "night_only_metrics.txt", night_metrics)

        night_s = night.rolling(ROLLING_WINDOW).mean()
        save_plot(
            maybe_slice(night_s),
            ["solar_db_kW", "hp_total", "grid_kW"],
            "Night-time comparison: Solar DB, HP total, Grid",
            "06_nighttime_db_hp_grid.png",
        )

    # Sunny subset where PV is materially positive
    sunny = df[df["pv_kW"] > 0.5].copy()
    if len(sunny) > 100:
        sunny_metrics = {
            "sunny_db_vs_hp_minus_pv": fit_quality(sunny["solar_db_kW"], sunny["model_A"]),
            "sunny_db_vs_pv_minus_hp": fit_quality(sunny["solar_db_kW"], sunny["model_B"]),
            "sunny_db_vs_pv_plus_grid": fit_quality(sunny["solar_db_kW"], sunny["pv_plus_grid"]),
        }
        write_metrics(OUTDIR / "sunny_only_metrics.txt", sunny_metrics)

        sunny_s = sunny.rolling(ROLLING_WINDOW).mean()
        save_plot(
            maybe_slice(sunny_s),
            ["solar_db_kW", "pv_kW", "hp_total", "model_A", "model_B"],
            "Sunny-period comparison: Solar DB, PV, HP total, candidate balance models",
            "07_sunny_db_pv_hp_models.png",
        )

    print(f"\nWrote outputs to: {OUTDIR}")


if __name__ == "__main__":
    main()