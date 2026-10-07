#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import argparse
import pathlib
from collections import defaultdict
from typing import Dict, List
from tqdm import tqdm
import datetime as dt

import polars as pl

# ---------- IO helpers ----------

def scan_paths(root: pathlib.Path) -> List[pathlib.Path]:
    return [
        p for p in root.rglob("*.parquet")
        if p.is_file() and not any(part.startswith((".", "_")) for part in p.parts)
    ]

def write_partitioned(df: pl.DataFrame, out_root: pathlib.Path) -> None:
    """
    Idempotent writer:
    - partitions by (AssetId, dt_utc := date(window_start_utc))
    - deletes existing parquet files in that partition folder
    - writes a single deterministic file per partition
    """
    out_root.mkdir(parents=True, exist_ok=True)

    df = df.with_columns(
        pl.col("window_start_utc").dt.date().cast(pl.Utf8).alias("dt_utc")
    )

    for (asset_id, dt_utc), part in df.group_by(["AssetId", "dt_utc"], maintain_order=True):
        subdir = out_root / f"asset_id={asset_id}" / f"dt_utc={dt_utc}"
        subdir.mkdir(parents=True, exist_ok=True)

        # clear existing files for idempotency
        for old in subdir.glob("*.parquet"):
            try:
                old.unlink()
            except Exception:
                pass

        out_path = subdir / f"part-{dt_utc}.parquet"
        part = part.drop("dt_utc")
        part.write_parquet(out_path, compression="zstd", statistics=True)

# ---------- Core: interval 15m windows ----------

def floor_to_15m(expr: pl.Expr) -> pl.Expr:
    return expr.dt.truncate("15m")

def build_fragments_15m(df: pl.DataFrame, gap_threshold_sec: int) -> pl.DataFrame:
    """
    Build fragments from forward-hold intervals:
      interval = [t0, t1) where
        t0 = CreatedOnUtc (current row)
        t1 = next CreatedOnUtc (shift(-1))
      Power is taken from the start of the interval (t0).
    """
    if df.height == 0:
        return df

    # Ensure sorted before shift(-1)
    df = df.sort(["AssetId", "Phase", "CreatedOnUtc"])

    df = (
        df.with_columns([
            pl.col("CreatedOnUtc").alias("t0"),
            pl.col("CreatedOnUtc").shift(-1).over(["AssetId", "Phase"]).alias("t1"),
        ])
        .filter(
            pl.col("t1").is_not_null()
            & (pl.col("t1") > pl.col("t0"))
            & (pl.col("t1") - pl.col("t0")).dt.total_seconds() <= pl.lit(gap_threshold_sec))
    )

    # 15m window floors
    df = df.with_columns(pl.col("t0").dt.truncate("15m").alias("w0"))

    eps = pl.duration(microseconds=1)
    df = df.with_columns((pl.col("t1") - eps).dt.truncate("15m").alias("w_end"))

    df = df.with_columns(
        ((((pl.col("w_end") - pl.col("w0")).dt.total_seconds()) / 900.0) + 1.0)
        .cast(pl.Int32)
        .alias("nwins")
    )

    df = df.with_columns(
        pl.int_ranges(pl.lit(0, dtype=pl.Int32), pl.col("nwins")).alias("rng")
    ).explode("rng")

    df = df.with_columns(
        (pl.col("w0") + pl.col("rng") * pl.duration(minutes=15)).alias("window_start_utc")
    )

    df = df.with_columns([
        pl.max_horizontal(pl.col("t0"), pl.col("window_start_utc")).alias("frag_start"),
        pl.min_horizontal(pl.col("t1"), pl.col("window_start_utc") + pl.duration(minutes=15)).alias("frag_end"),
    ])

    df = (
        df.with_columns((pl.col("frag_end") - pl.col("frag_start")).dt.total_seconds().alias("frag_seconds"))
        .filter(pl.col("frag_seconds") > 0)
    )

    # Energy weights:
    df = df.with_columns([
        (pl.col("Power_kW_pos") * (pl.col("frag_seconds") / 3600.0)).alias("frag_energy_kWh"),
        (pl.col("Power_kW_pos") * pl.col("frag_seconds")).alias("frag_power_weight"),
        (pl.when(pl.col("IsReliableRow") == 1).then(pl.col("frag_seconds")).otherwise(0.0)).alias("frag_seconds_reliable"),
    ])
    
    df = df.with_columns([
        (pl.col("Power_kW_pos") * (pl.col("frag_seconds_reliable") / 3600.0)).alias("frag_energy_kWh_reliable"),
        (pl.col("Power_kW_pos") * pl.col("frag_seconds_reliable")).alias("frag_power_weight_reliable"),
    ])

    return df.select([
        "AssetId", "Phase", "window_start_utc",
        "frag_seconds", "frag_seconds_reliable",
        "frag_energy_kWh", "frag_power_weight",
        "frag_energy_kWh_reliable", "frag_power_weight_reliable",
    ])

def silver_to_15m(df: pl.DataFrame, coverage_threshold: float, local_tz: str, gap_threshold_sec: int) -> pl.DataFrame:
    frags = build_fragments_15m(df, gap_threshold_sec=gap_threshold_sec)
    if frags.height == 0:
        return pl.DataFrame(schema={
            "AssetId": pl.Utf8, "Phase": pl.Utf8, "window_start_utc": pl.Datetime,
            "DateKeyUtc": pl.Int32, "TimeKeyUtc": pl.Int32,
            "WindowStartLocal": pl.Datetime, "HourLocal": pl.Int32, "DateLocal": pl.Date,
            "Energy_kWh_15m": pl.Float64, "Demand_kW": pl.Float64, "AvgPower_kW_15m": pl.Float64,
            "Minutes": pl.Int32, "SecondsObserved": pl.Float64,
            "DataCoveragePct": pl.Float64, "IsReliableWindow": pl.Int8,
        })

    # Aggregate to 15m per phase
    secs_sum = pl.col("frag_seconds").sum()
    secs_sum_capped = pl.when(secs_sum > 900).then(900.0).otherwise(secs_sum)

    grouped = frags.group_by(["AssetId","Phase","window_start_utc"]).agg([
        pl.col("frag_energy_kWh_reliable").sum().alias("Energy_kWh_15m"),
        pl.when(pl.col("frag_seconds_reliable").sum() > 0).then(pl.col("frag_power_weight_reliable").sum() / pl.col("frag_seconds_reliable").sum()).otherwise(None).alias("AvgPower_kW_15m"),
        pl.col("frag_seconds").sum().clip(upper_bound=900.0).alias("SecondsObserved"),
        pl.col("frag_seconds_reliable").sum().clip(upper_bound=900.0).alias("SecondsReliable"),
    ])

    ws_utc = pl.col("window_start_utc").dt.replace_time_zone("UTC")
    ws_local = ws_utc.dt.convert_time_zone(local_tz)

    grouped = grouped.with_columns([
        (pl.col("SecondsObserved") / 60.0).round(2).alias("Minutes"),
        (pl.col("Energy_kWh_15m") * 4.0).alias("Demand_kW"),
        (pl.col("SecondsObserved") / 900.0 * 100.0).alias("DataCoveragePct"),
        (pl.col("SecondsReliable") / 900.0 * 100.0).alias("ReliableCoveragePct"),
        pl.when(pl.col("SecondsReliable") >= 900 * coverage_threshold).then(1).otherwise(0).alias("IsReliableWindow"),
        pl.col("window_start_utc").dt.strftime("%Y%m%d").cast(pl.Int32).alias("DateKeyUtc"),
        pl.col("window_start_utc").dt.strftime("%H%M%S").cast(pl.Int32).alias("TimeKeyUtc"),
        ws_local.alias("WindowStartLocal"),
        ws_local.dt.hour().alias("HourLocal"),
        ws_local.dt.date().alias("DateLocal"),
    ])

    grouped = grouped.select([
        "AssetId", "Phase",
        "window_start_utc", "DateKeyUtc", "TimeKeyUtc",
        "WindowStartLocal", "HourLocal", "DateLocal",
        "Energy_kWh_15m", "Demand_kW", "AvgPower_kW_15m",
        "Minutes", "SecondsObserved", "DataCoveragePct", "IsReliableWindow",
        "SecondsReliable", "ReliableCoveragePct"
    ])

    # Phase='ALL' rollup
    secs_sum2 = pl.col("SecondsObserved").sum()
    secs_sum2_capped = pl.when(secs_sum2 > 900).then(900.0).otherwise(secs_sum2)
    
    secs_rel2 = pl.col("SecondsReliable").sum()
    secs_rel2_capped = pl.when(secs_rel2 > 900).then(900.0).otherwise(secs_rel2)

    all_phase = (
        grouped
        .group_by(["AssetId", "window_start_utc", "DateKeyUtc", "TimeKeyUtc",
                   "WindowStartLocal", "HourLocal", "DateLocal"])
        .agg([
            pl.lit("ALL").alias("Phase"),
            pl.col("Energy_kWh_15m").sum().alias("Energy_kWh_15m"),
            pl.when(secs_rel2 > 0).then(((pl.col("AvgPower_kW_15m") * pl.col("SecondsReliable")).sum()) / secs_rel2).otherwise(None).alias("AvgPower_kW_15m"),
            pl.col("Demand_kW").sum().alias("Demand_kW"),
            (secs_sum2_capped / 60.0).round(2).alias("Minutes"),
            secs_sum2_capped.alias("SecondsObserved"),
            (secs_sum2_capped / 900.0 * 100.0).alias("DataCoveragePct"),
            secs_rel2_capped.alias("SecondsReliable"),
            (secs_rel2_capped / 900.0 * 100.0).alias("ReliableCoveragePct"),
            pl.when(secs_rel2_capped >= 900 * coverage_threshold).then(1).otherwise(0).alias("IsReliableWindow"),
        ])
        .select(grouped.columns)
    )

    return pl.concat([grouped, all_phase], how="vertical_relaxed")

# ---------- Main ----------
def main():
    ap = argparse.ArgumentParser(description="Build 15-minute fact directly from Silver telemetry_long")
    ap.add_argument("--silver-root", type=pathlib.Path, required=True)
    ap.add_argument("--out-root", type=pathlib.Path, required=True)
    ap.add_argument("--coverage-threshold", type=float, default=0.8)
    ap.add_argument("--local-tz", type=str, default="Europe/Dublin")
    ap.add_argument("--gap-threshold-sec", type=int, default=300)
    args = ap.parse_args()

    paths = scan_paths(args.silver_root)
    print(f"Found {len(paths)} silver files under {args.silver_root}")
    if not paths:
        print("No files found.")
        return

    # Build asset-day [files]
    by_asset_by_day: Dict[str, Dict[str, List[str]]] = defaultdict(lambda: defaultdict(list))
    for p in paths:
        aid = next((part.split("=",1)[1] for part in p.parts if part.startswith("asset_id=")), None)
        day = next((part.split("=",1)[1] for part in p.parts if part.startswith("dt_utc=")), None)
        if aid and day:
            by_asset_by_day[aid][day].append(str(p))

    print(f"Assets discovered: {len(by_asset_by_day)} -> {sorted(by_asset_by_day.keys())[:10]}{' ...' if len(by_asset_by_day)>10 else ''}")
    
    assets = sorted(by_asset_by_day.items())

    for asset_id, days in tqdm(assets, desc="Assets", unit="asset"):
        day_items = sorted(days.items())
        p_days = tqdm(day_items, desc=f"{asset_id}", unit="day", leave=False)
        for day, files in p_days:
            # Read only needed; ignore extras if any
            try:
                lf = pl.scan_parquet(files, extra_columns="ignore")
            except TypeError:
                lf = pl.scan_parquet(files)
            
            need = ["AssetId", "Phase", "CreatedOnUtc", "Power_kW", "IsReliableRow"]
            
            schema = lf.collect_schema()
            missing = [c for c in need if c not in schema]
            if missing:
                raise SystemExit(f"Missing required column(s) in Silver for {asset_id} {day}: {missing}")
                
            df_today = lf.select(need).collect()
            cols_today = df_today.columns
            
            # ---- lookahead anchor: first row per Phase from next day; prevents day-edge truncation ----
            day_date = dt.date.fromisoformat(day)
            next_day = (day_date + dt.timedelta(days=1)).isoformat()
            next_files = days.get(next_day, [])

            df_next_anchor = None
            if next_files:
                try:
                    lf_next = pl.scan_parquet(next_files, extra_columns="ignore")
                except TypeError:
                    lf_next = pl.scan_parquet(next_files)
                    
                schema_next = lf_next.collect_schema()
                missing_next = [c for c in need if c not in schema_next]
                if missing_next:
                    raise SystemExit(f"Missing required column(s) in Silver for {asset_id} {day}: {missing}")

                df_next_anchor = (
                    lf_next.select(need)
                    .sort(["Phase", "CreatedOnUtc"])
                    .group_by("Phase", maintain_order=True)
                    .head(1)   # first row per phase
                    .collect()
                )
            
            if df_next_anchor is not None and df_next_anchor.height:
                df_next_anchor = df_next_anchor.select(cols_today)
                df = pl.concat([df_today, df_next_anchor], how="vertical_relaxed")
            else:
                df = df_today
            df = df.sort(["Phase", "CreatedOnUtc"])
            
            # Known issue: L3 power tag misconfiguration (voltage logged as active power) ---
            L3_BAD_FROM = {
                "ahu_a": dt.datetime(2025, 3, 10, 9, 22),
                "mix_b": dt.datetime(2025, 3, 10, 9, 15),
            }

            if asset_id in L3_BAD_FROM:
                df = df.filter(
                    ~(
                        (pl.col("Phase") == "L3") &
                        (pl.col("CreatedOnUtc") >= pl.lit(L3_BAD_FROM[asset_id]))
                    )
                )
                
            # Known issue: CT ratio misconfiguration (magnitude invalid) ---
            if asset_id == "p_c":
                bad_start = dt.datetime(2025, 2, 20, 22, 5)
                bad_end   = dt.datetime(2025, 5, 21, 11, 48)
                df = df.filter(
                    ~(
                        (pl.col("CreatedOnUtc") >= pl.lit(bad_start)) &
                        (pl.col("CreatedOnUtc") <= pl.lit(bad_end))
                    )
                )    
            
            # Power sign policy ---
            SUPPLY = {"mi_a", "mi_b"} 

            df = df.with_columns(
                pl.when(pl.col("AssetId").is_in(list(SUPPLY)))
                  .then(pl.col("Power_kW"))
                  .otherwise(pl.col("Power_kW").clip(lower_bound=0.0))
                  .alias("Power_kW_pos")
            )
            
            if df.height == 0:
                tqdm.write(f"{asset_id} {day}: empty after issue filters")
                continue
            
            out = silver_to_15m(df, coverage_threshold=args.coverage_threshold, local_tz=args.local_tz, gap_threshold_sec=args.gap_threshold_sec)
            # Only write windows that *start* on this day; avoid double counting tomorrow’s windows
            out = out.filter(pl.col("window_start_utc").dt.date() == pl.lit(day_date))
            p_days.set_postfix(day=day, files=len(files), out=out.height)
            if out.height:
                write_partitioned(out, args.out_root)
            else:
                tqdm.write(f"{asset_id} {day}: no rows")
    print("Done.")

if __name__ == "__main__":
    pl.Config.set_tbl_rows(50)
    main()
