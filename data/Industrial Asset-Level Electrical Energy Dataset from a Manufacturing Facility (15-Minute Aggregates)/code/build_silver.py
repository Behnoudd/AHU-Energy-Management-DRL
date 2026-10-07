#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations
import argparse
import datetime as dt
import json
import os
import pathlib
import re
import shutil
from typing import Dict, Iterable, List, Tuple
import hashlib

import polars as pl

# ---------------- Config & helpers ----------------

PHASES = ("L1", "L2", "L3")
SUFFIX_MAP = {
    "_CRNT": "Current_A",
    "_PWR_ACTV": "Power_raw",        
    "_PWR_FACTOR": "PowerFactor",
    "_PWR_APPRNT": "Apparent_raw", 
}

def _state_dir(out_root: pathlib.Path) -> pathlib.Path:
    return out_root.parent / "_state" / "watermarks"

def _wm_path(out_root: pathlib.Path, asset_id: str) -> pathlib.Path:
    return _state_dir(out_root) / f"{asset_id}.json"

def _filesig(path: pathlib.Path) -> Dict:
    st = path.stat()
    return {"size": st.st_size, "mtime": int(st.st_mtime)}

def _load_watermark(path: pathlib.Path) -> Dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}

def _save_watermark(path: pathlib.Path, data: Dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data, indent=2), encoding="utf-8")
    tmp.replace(path)

def infer_asset_id_from_path(path: pathlib.Path) -> str:
    stem = path.stem.lower()
    return stem

def coerce_to_created_on_utc(df: pl.DataFrame, default_local_tz: str) -> pl.DataFrame:
    """
    Treat 'CreatedOn' as naive local time in default_local_tz, convert to UTC.
    Emulates 'shift_forward' for DST gaps by trying CreatedOn+1h.
    """
    # Ensure CreatedOn is a datetime
    if df.schema.get("CreatedOn") != pl.Datetime:
        df = df.with_columns(
            pl.col("CreatedOn").str.strptime(pl.Datetime, strict=False)
        )

    # First attempt: localize; non-existent to null (no 'shift_forward')
    localized = (
        pl.col("CreatedOn")
          .dt.replace_time_zone(default_local_tz, ambiguous="earliest", non_existent="null")
    )

    # Fallback: try +1h to emulate shift_forward for spring DST gap
    localized_fallback = (
        (pl.col("CreatedOn") + pl.duration(hours=1))
          .dt.replace_time_zone(default_local_tz, ambiguous="earliest", non_existent="null")
    )

    df = (df
        .with_columns(pl.coalesce([localized, localized_fallback]).alias("CreatedOnLocal"))
        .with_columns(pl.col("CreatedOnLocal").dt.convert_time_zone("UTC").alias("CreatedOnUtc"))
        .drop("CreatedOnLocal")
    )
    return df

def reshape_wide_to_long(df: pl.DataFrame, asset_id: str) -> pl.DataFrame:
    """Wide per-phase → long rows per (Phase, CreatedOnUtc)."""
    long_parts: List[pl.DataFrame] = []
    for phase in PHASES:
        rename_map = {}
        avail = []
        for suffix, norm in SUFFIX_MAP.items():
            raw = f"{phase}{suffix}"
            if raw in df.columns:
                rename_map[raw] = norm
                avail.append(raw)
        if not avail:
            continue
        sub = (
            df.select(["CreatedOnUtc"] + avail)
              .rename(rename_map)
              .with_columns(pl.lit(phase).alias("Phase"))
        )
        long_parts.append(sub)

    if not long_parts:
        return pl.DataFrame(schema={
            "AssetId": pl.Utf8, "Phase": pl.Utf8, "CreatedOnUtc": pl.Datetime,
            "Current_A": pl.Float64, "Power_raw": pl.Float64,
            "PowerFactor": pl.Float64, "Apparent_raw": pl.Float64
        })

    out = pl.concat(long_parts, how="vertical_relaxed")
    # Ensure all normalized cols exist
    for name, dtype in [("Current_A", pl.Float64), ("Power_raw", pl.Float64),
                        ("PowerFactor", pl.Float64), ("Apparent_raw", pl.Float64)]:
        if name not in out.columns:
            out = out.with_columns(pl.lit(None, dtype=dtype).alias(name))
    return out.with_columns(pl.lit(asset_id).alias("AssetId"))

def compute_long_fields(
    long_df: pl.DataFrame,
    power_unit: str,
    apparent_unit: str,
    derive_apparent_if_missing: bool,
    gap_threshold_sec: int,
) -> pl.DataFrame:
    pu = power_unit.upper()
    if pu == "W":
        long_df = long_df.with_columns((pl.col("Power_raw") / 1000.0).alias("Power_kW"))
    elif pu == "KW":
        long_df = long_df.with_columns(pl.col("Power_raw").alias("Power_kW"))
    else:
        raise SystemExit("--power-unit must be 'W' or 'kW'")
    
    # Preserve raw PF
    long_df = long_df.with_columns(
        pl.col("PowerFactor").alias("PowerFactor_raw")
    )

    # Create a clamped PF for safe derived calculations
    long_df = long_df.with_columns(
        pl.when(pl.col("PowerFactor_raw").is_not_null())
          .then(pl.col("PowerFactor_raw").clip(0.0, 1.1))
          .otherwise(pl.col("PowerFactor_raw"))
          .alias("PowerFactor_clamped")
    )
    
    if "Apparent_kVA_from_raw" not in long_df.columns:
        long_df = long_df.with_columns(pl.lit(None, dtype=pl.Float64).alias("Apparent_kVA_from_raw"))

    # Apparent power (if provided)
    if "Apparent_raw" in long_df.columns:
        au = apparent_unit.upper()
        if au == "VA":
            long_df = long_df.with_columns((pl.col("Apparent_raw") / 1000.0).alias("Apparent_kVA_from_raw"))
        elif au == "KVA":
            long_df = long_df.with_columns(pl.col("Apparent_raw").alias("Apparent_kVA_from_raw"))
        else:
            raise SystemExit("--apparent-unit must be 'VA' or 'kVA'")

    # Apparent_kVA final
    if derive_apparent_if_missing:
        long_df = long_df.with_columns(
            pl.when(pl.col("Apparent_kVA_from_raw").is_not_null())
              .then(pl.col("Apparent_kVA_from_raw"))
              .otherwise(
                  pl.when((pl.col("Power_kW").is_not_null()) & (pl.col("PowerFactor_clamped") > 0))
                    .then(pl.col("Power_kW") / pl.col("PowerFactor_clamped"))
                    .otherwise(pl.lit(None))
              ).alias("Apparent_kVA")
        )
    else:
        if "Apparent_kVA_from_raw" in long_df.columns:
            long_df = long_df.with_columns(pl.col("Apparent_kVA_from_raw").alias("Apparent_kVA"))
        else:
            long_df = long_df.with_columns(pl.lit(None, dtype=pl.Float64).alias("Apparent_kVA"))

    # Sort + previous timestamp per (AssetId, Phase)
    long_df = long_df.sort(["AssetId", "Phase", "CreatedOnUtc"]).with_columns(
        pl.col("CreatedOnUtc").shift(1).over(["AssetId", "Phase"]).alias("PrevCreatedOnUtc")
    )

    # dt_seconds and QA flags
    long_df = long_df.with_columns([
        pl.when(pl.col("PrevCreatedOnUtc").is_null())
          .then(pl.lit(None, dtype=pl.Float64))
          .otherwise((pl.col("CreatedOnUtc").cast(pl.Datetime("ns"))
                      - pl.col("PrevCreatedOnUtc").cast(pl.Datetime("ns"))).dt.total_seconds().cast(pl.Float64))
          .alias("dt_seconds"),
    ]).with_columns([
        pl.when(pl.col("dt_seconds") < 0).then(pl.lit(None)).otherwise(pl.col("dt_seconds")).alias("dt_seconds"),
        pl.when(pl.col("dt_seconds").is_not_null() & (pl.col("dt_seconds") > gap_threshold_sec)).then(1).otherwise(0).alias("IsGap"),
        pl.when(pl.col("Power_kW").is_null()).then(1).otherwise(0).alias("IsMissing_Power"),
        pl.when(pl.col("Power_kW") < 0).then(1).otherwise(0).alias("IsNegative_Power"),
        # Outlier PF should be based on RAW reported PF
        pl.when(
            pl.col("PowerFactor_raw").is_not_null() &
            ((pl.col("PowerFactor_raw") < 0) | (pl.col("PowerFactor_raw") > 1.1))
        ).then(1).otherwise(0).alias("IsOutlier_PF"),
        pl.when((pl.col("Apparent_kVA") < 0)).then(1).otherwise(0).alias("IsOutlier_Apparent"),
    ])

    # Convenience flag + dt_utc partition key
    long_df = long_df.with_columns([
        (1 - pl.max_horizontal(
            pl.col("IsGap"),
            pl.col("IsMissing_Power"),
            pl.col("IsNegative_Power"),
            pl.col("IsOutlier_PF"),
            pl.col("IsOutlier_Apparent"),
        )).alias("IsReliableRow"),
        pl.col("CreatedOnUtc").dt.date().cast(pl.Utf8).alias("dt_utc"),
    ])

    # Final column order
    ordered = [
        "AssetId", "CreatedOnUtc", "Phase",
        "Current_A", "Power_kW",
        "PowerFactor_raw", "PowerFactor_clamped",
        "Apparent_kVA",
        "PrevCreatedOnUtc", "dt_seconds",
        "IsGap", "IsMissing_Power", "IsNegative_Power", "IsOutlier_PF", "IsOutlier_Apparent", "IsReliableRow",
        "dt_utc",
    ]
    return long_df.select([c for c in ordered if c in long_df.columns])

def write_partitioned_overwrite(df: pl.DataFrame, out_root: pathlib.Path) -> List[str]:
    if df.height == 0:
        return []
    out_root.mkdir(parents=True, exist_ok=True)
    touched_days: List[str] = []

    for (asset_id, dt_utc), part in df.group_by(["AssetId", "dt_utc"], maintain_order=True):
        subdir = out_root / f"asset_id={asset_id}" / f"dt_utc={dt_utc}"
        if subdir.exists():
            shutil.rmtree(subdir)
        subdir.mkdir(parents=True, exist_ok=True)

        max_ts = part.select(pl.max("CreatedOnUtc")).item()
        max_ts_iso = max_ts.isoformat() if max_ts else None

        fname = stable_part_name(asset_id, dt_utc, part.height, max_ts_iso)
        part.write_parquet(subdir / fname, compression="zstd", statistics=True)
        touched_days.append(f"{asset_id}|{dt_utc}")

    return touched_days

def scan_master_assets(src_root: pathlib.Path) -> List[pathlib.Path]:
    """Find master Parquet files (one or more per asset)."""
    return [p for p in src_root.rglob("*.parquet")
            if p.is_file() and not any(part.startswith((".", "_")) for part in p.parts)]

def stable_part_name(asset_id: str, dt_utc: str, nrows: int, max_ts_iso: str | None) -> str:
    key = f"{asset_id}|{dt_utc}|{nrows}|{max_ts_iso or ''}".encode("utf-8")
    h = hashlib.sha1(key).hexdigest()[:12]
    return f"part-{h}.parquet"

def ensure_created_on_utc(df: pl.DataFrame, default_local_tz: str, createdon_is_utc_naive: bool) -> pl.DataFrame:
    if "CreatedOnUtc" in df.columns:
        if df.schema.get("CreatedOnUtc") != pl.Datetime:
            df = df.with_columns(pl.col("CreatedOnUtc").str.strptime(pl.Datetime, strict=False))
        try:
            df = df.with_columns(pl.col("CreatedOnUtc").dt.convert_time_zone("UTC").alias("CreatedOnUtc"))
        except Exception:
            pass
        return df

    if "CreatedOn" not in df.columns:
        raise SystemExit("Source must contain either CreatedOnUtc or CreatedOn")

    if createdon_is_utc_naive:
        # Treat CreatedOn as already UTC (no DST conversion)
        if df.schema.get("CreatedOn") != pl.Datetime:
            df = df.with_columns(pl.col("CreatedOn").str.strptime(pl.Datetime, strict=False))
        return df.with_columns(pl.col("CreatedOn").alias("CreatedOnUtc"))

    # Otherwise, assume CreatedOn is local time with DST rules
    return coerce_to_created_on_utc(df, default_local_tz=default_local_tz)

# ---------------- Main incremental flow ----------------

def main():
    ap = argparse.ArgumentParser(description="Incremental import: master → Silver/telemetry_long (daily partitions, overlap replay)")
    ap.add_argument("--src-root", type=pathlib.Path, required=True, help="Folder with master Parquet(s) per asset")
    ap.add_argument("--out-root", type=pathlib.Path, required=True, help="2.Silver/telemetry_long")
    ap.add_argument("--default-local-tz", type=str, default="Europe/Dublin")
    ap.add_argument("--overlap-days", type=int, default=2)
    ap.add_argument("--power-unit", default="W", choices=["W", "kW"])
    ap.add_argument("--apparent-unit", default="VA", choices=["VA", "kVA"])
    ap.add_argument("--derive-apparent-if-missing", action="store_true")
    ap.add_argument("--gap-threshold-sec", type=int, default=300)
    ap.add_argument("--unit-map", type=pathlib.Path, default=None, help="JSON {asset_id: 'W'|'kW'} to override per-asset power units")
    ap.add_argument("--createdon-is-utc-naive", action="store_true", help="Treat CreatedOn as already-UTC (naive) with no DST conversion.")
    ap.add_argument("--full-rebuild", action="store_true", help="Rebuild Silver from scratch for all assets (ignore watermarks/overlap).")
    args = ap.parse_args()

    unit_map: dict[str, str] = {}
    if args.unit_map and args.unit_map.exists():
        try:
            unit_map_raw = json.loads(args.unit_map.read_text(encoding="utf-8"))
            # normalize keys to lowercase
            unit_map = {str(k).lower(): str(v) for k, v in unit_map_raw.items()}
            print(f"Loaded per-asset power unit map for {len(unit_map)} asset(s) from {args.unit_map}")
        except Exception as e:
            print(f"WARNING: failed to read --unit-map {args.unit_map}: {e}")

    src_files = scan_master_assets(args.src_root)
    if not src_files:
        raise SystemExit(f"No master Parquet found under {args.src_root}")

    print(f"Found {len(src_files)} master file(s) under {args.src_root}")

    state_dir = _state_dir(args.out_root)
    state_dir.mkdir(parents=True, exist_ok=True)
    
    # Process each master file independently
    for i, src in enumerate(sorted(src_files), start=1):
        asset_id = infer_asset_id_from_path(src)
        print(f"[{i}/{len(src_files)}] Asset {asset_id}: {src.name}")

        wm_file = _wm_path(args.out_root, asset_id)
        wm = _load_watermark(wm_file)
        
        current_sig = _filesig(src)
        if (not args.full_rebuild) and (wm.get("src_filesig") == current_sig) and wm.get("last_ingested_utc"):
            print("  unchanged file signature; skipping")
            continue

        # ---- Read source (always) ----
        lf = pl.scan_parquet(str(src))
        df = lf.collect()

        df = ensure_created_on_utc(
            df,
            default_local_tz=args.default_local_tz,
            createdon_is_utc_naive=args.createdon_is_utc_naive
        )

        # Ensure CreatedOnUtc is comparable (drop tz if present)
        try:
            df = df.with_columns(pl.col("CreatedOnUtc").dt.replace_time_zone(None).alias("CreatedOnUtc"))
        except Exception:
            pass

        # ---- Apply incremental cutoff (only if NOT full rebuild) ----
        if not args.full_rebuild:
            # Decide cutoff based on overlap vs watermark
            now_utc = dt.datetime.now(dt.timezone.utc)
            overlap_start = (now_utc - dt.timedelta(days=args.overlap_days)).date().isoformat()

            # Best-effort: if stored last_ingested_utc, start a bit earlier (by overlap)
            filter_from_iso = overlap_start
            if wm.get("last_ingested_utc"):
                try:
                    prev = dt.datetime.fromisoformat(wm["last_ingested_utc"].replace("Z","")).date()
                    earlier = (prev - dt.timedelta(days=args.overlap_days)).isoformat()
                    filter_from_iso = min(filter_from_iso, earlier)
                except Exception:
                    pass

            cutoff = dt.date.fromisoformat(filter_from_iso)
            cutoff_dt = dt.datetime.combine(cutoff, dt.time(0))  # naive datetime

            df = df.filter(pl.col("CreatedOnUtc") >= pl.lit(cutoff_dt))

            if df.height == 0:
                print("  nothing to import for this asset in the overlap window")
                continue
        else:
            # Full rebuild: process everything
            if df.height == 0:
                print("  empty source file; skipping")
                continue

        # ---- Reshape & compute fields (always) ----
        long_df = reshape_wide_to_long(df, asset_id=asset_id)
        if long_df.height == 0:
            print("  no per-phase columns present; skipping")
            continue

        # pick unit per asset (falls back to global --power-unit)
        pu_for_asset = unit_map.get(asset_id.lower(), args.power_unit)

        long_df = (
            compute_long_fields(
                long_df=long_df,
                power_unit=pu_for_asset,
                apparent_unit=args.apparent_unit,
                derive_apparent_if_missing=args.derive_apparent_if_missing,
                gap_threshold_sec=args.gap_threshold_sec,
            )
            .with_columns([
                pl.col("Power_kW").abs().alias("Power_kW_abs"),
            ])
            .unique(subset=["AssetId", "Phase", "CreatedOnUtc"], keep="last")
        )

        # Write (delete+rewrite touched partitions)
        touched = write_partitioned_overwrite(long_df, args.out_root)
        print(f"  wrote {len(touched)} day partition(s)")

        max_ts = long_df.select(pl.max("CreatedOnUtc")).item()
        max_ts_iso = max_ts.isoformat() + ("Z" if getattr(max_ts, "tzinfo", None) is None else "")

        sig = _filesig(src)
        _save_watermark(wm_file, {
            "asset_id": asset_id,
            "last_ingested_utc": max_ts_iso,
            "src_filesig": sig,
            "updated_utc": dt.datetime.utcnow().isoformat() + "Z",
            "overlap_days": args.overlap_days
        })
        
    print("Done.")
    
if __name__ == "__main__":
    pl.Config.set_tbl_rows(50)
    main()
