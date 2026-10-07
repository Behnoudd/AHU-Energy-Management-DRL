#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import argparse
import math
import pathlib
from typing import List, Dict, Any
import polars as pl

# -----------------------
# Helpers
# -----------------------

def scan_parquet_paths(root: pathlib.Path) -> List[pathlib.Path]:
    return [
        p for p in root.rglob("*.parquet")
        if p.is_file() and not any(part.startswith((".", "_")) for part in p.parts)
    ]


def mirrored_csv_path(src_path: pathlib.Path, src_root: pathlib.Path, dst_root: pathlib.Path) -> pathlib.Path:
    rel = src_path.relative_to(src_root)
    return (dst_root / rel).with_suffix(".csv")


def ensure_parent(path: pathlib.Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def dtype_family(dtype: pl.DataType) -> str:
    if dtype in (pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64):
        return "int"
    if dtype in (pl.Float32, pl.Float64):
        return "float"
    if dtype == pl.Boolean:
        return "bool"
    if dtype == pl.Date:
        return "date"
    if dtype in (pl.Datetime,):
        return "datetime"
    return "other"


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Convert Gold Parquet partitions to CSV and validate equivalence.")
    ap.add_argument("--parquet-root", type=pathlib.Path, required=True, help="Root folder of Gold parquet partitions")
    ap.add_argument("--csv-root", type=pathlib.Path, required=True, help="Root folder for mirrored CSV partitions")
    ap.add_argument("--report-dir", type=pathlib.Path, required=True, help="Folder for validation reports")
    ap.add_argument("--float-tol", type=float, default=1e-9, help="Absolute tolerance for float equivalence checks")
    ap.add_argument("--na-token", type=str, default="NaN", help="Null token written to CSV")
    ap.add_argument("--stop-after-first-fail", action="store_true")
    ap.add_argument("--max-files", type=int, default=None)
    return ap.parse_args()


# -----------------------
# Conversion
# -----------------------

def prepare_for_csv(df: pl.DataFrame) -> pl.DataFrame:
    out = df.clone()

    for col, dtp in out.schema.items():
        fam = dtype_family(dtp)
        if fam == "datetime":
            out = out.with_columns(
                pl.col(col).dt.strftime("%Y-%m-%d %H:%M:%S").alias(col)
            )
        elif fam == "date":
            out = out.with_columns(
                pl.col(col).dt.strftime("%Y-%m-%d").alias(col)
            )

    return out


def sort_for_release(df: pl.DataFrame) -> pl.DataFrame:
    sort_cols = []
    temp_cols = []

    if "window_start_utc" in df.columns:
        sort_cols.append("window_start_utc")

    if "Phase" in df.columns:
        phase_order = {"L1": 1, "L2": 2, "L3": 3, "ALL": 4}
        df = df.with_columns(
            pl.col("Phase").replace(phase_order, default=99).alias("_phase_order")
        )
        sort_cols.append("_phase_order")
        temp_cols.append("_phase_order")

    if "AssetId" in df.columns:
        sort_cols.append("AssetId")

    if sort_cols:
        df = df.sort(sort_cols)

    if temp_cols:
        df = df.drop(temp_cols)

    return df


def write_csv_partition(df: pl.DataFrame, out_path: pathlib.Path, na_token: str) -> None:
    ensure_parent(out_path)
    # Idempotent overwrite
    if out_path.exists():
        out_path.unlink()

    df.write_csv(
        out_path,
        null_value=na_token,
        float_precision=15,
    )


# -----------------------
# Validation
# -----------------------

def read_csv_back(csv_path: pathlib.Path, parquet_schema: Dict[str, pl.DataType], na_token: str) -> pl.DataFrame:
    # Read everything as strings first, then cast by source schema
    df = pl.read_csv(
        csv_path,
        null_values=[na_token],
        try_parse_dates=False,
        infer_schema_length=0,
    )

    # Reorder/add columns exactly as parquet
    parquet_cols = list(parquet_schema.keys())
    if df.columns != parquet_cols:
        missing = [c for c in parquet_cols if c not in df.columns]
        extra = [c for c in df.columns if c not in parquet_cols]
        if missing or extra:
            raise ValueError(f"CSV schema mismatch. Missing={missing}, Extra={extra}")
        df = df.select(parquet_cols)

    cast_exprs = []
    for col, dtp in parquet_schema.items():
        fam = dtype_family(dtp)

        if fam == "datetime":
            cast_exprs.append(
                pl.col(col).str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S", strict=False).alias(col)
            )
        elif fam == "date":
            cast_exprs.append(
                pl.col(col).str.strptime(pl.Date, format="%Y-%m-%d", strict=False).alias(col)
            )
        else:
            cast_exprs.append(pl.col(col).cast(dtp, strict=False).alias(col))

    return df.with_columns(cast_exprs).select(parquet_cols)


def validate_partition(
    pq_df: pl.DataFrame,
    csv_df: pl.DataFrame,
    float_tol: float,
) -> Dict[str, Any]:
    report: Dict[str, Any] = {
        "row_count_match": pq_df.height == csv_df.height,
        "column_count_match": len(pq_df.columns) == len(csv_df.columns),
        "column_order_match": pq_df.columns == csv_df.columns,
        "schema_match": True,
        "max_abs_diff_any_float": 0.0,
        "all_floats_within_tol": True,
        "all_nonfloat_equal": True,
        "partition_ok": True,
        "notes": "",
    }

    if not report["row_count_match"] or not report["column_order_match"]:
        report["partition_ok"] = False
        report["notes"] = "Basic row/column mismatch"
        return report

    for c in pq_df.columns:
        pq_type = pq_df.schema[c]
        csv_type = csv_df.schema[c]
        if dtype_family(pq_type) != dtype_family(csv_type):
            report["schema_match"] = False
            report["notes"] += f"[{c}: schema family mismatch {pq_type} vs {csv_type}] "

    for c in pq_df.columns:
        pq_col = pq_df.get_column(c)
        csv_col = csv_df.get_column(c)
        fam = dtype_family(pq_df.schema[c])

        # Null-mask check first
        pq_null = pq_col.is_null()
        csv_null = csv_col.is_null()
        if not pq_null.equals(csv_null):
            report["partition_ok"] = False
            report["notes"] += f"[{c}: null mask mismatch] "
            if fam == "float":
                report["all_floats_within_tol"] = False
            else:
                report["all_nonfloat_equal"] = False
            continue

        if fam == "float":
            both_not_null = ~(pq_null | csv_null)
            if both_not_null.sum() > 0:
                pq_vals = pq_df.filter(both_not_null).get_column(c).cast(pl.Float64)
                csv_vals = csv_df.filter(both_not_null).get_column(c).cast(pl.Float64)
                diffs = (pq_vals - csv_vals).abs()
                max_diff = float(diffs.max() or 0.0)
                report["max_abs_diff_any_float"] = max(report["max_abs_diff_any_float"], max_diff)
                if max_diff > float_tol:
                    report["all_floats_within_tol"] = False
                    report["partition_ok"] = False
                    report["notes"] += f"[{c}: max_diff={max_diff}] "

        elif fam == "datetime":
            if c == "WindowStartLocal":
                continue
            else:
                pq_i = pq_col.cast(pl.Int64)
                csv_i = csv_col.cast(pl.Int64)
                if not pq_i.equals(csv_i):
                    report["all_nonfloat_equal"] = False
                    report["partition_ok"] = False
                    report["notes"] += f"[{c}: datetime mismatch] "

        elif fam == "date":
            # Compare as days since epoch
            pq_i = pq_col.cast(pl.Int32)
            csv_i = csv_col.cast(pl.Int32)
            if not pq_i.equals(csv_i):
                report["all_nonfloat_equal"] = False
                report["partition_ok"] = False
                report["notes"] += f"[{c}: date mismatch] "

        else:
            if not pq_col.equals(csv_col):
                report["all_nonfloat_equal"] = False
                report["partition_ok"] = False
                report["notes"] += f"[{c}: exact mismatch] "

    if not report["schema_match"]:
        report["partition_ok"] = False

    return report


# -----------------------
# Main
# -----------------------

def main() -> None:
    args = parse_args()

    args.csv_root.mkdir(parents=True, exist_ok=True)
    args.report_dir.mkdir(parents=True, exist_ok=True)

    parquet_paths = scan_parquet_paths(args.parquet_root)
    print(f"Found {len(parquet_paths)} parquet partitions under {args.parquet_root}")
    
    if args.max_files is not None:
        parquet_paths = parquet_paths[:args.max_files]

    if not parquet_paths:
        print("No parquet files found.")
        return

    partition_reports: List[Dict[str, Any]] = []

    dataset_summary = {
        "partitions_scanned": 0,
        "partitions_passed": 0,
        "partitions_failed": 0,
        "total_rows_parquet": 0,
        "total_rows_csv": 0,
        "global_energy_kwh_parquet": 0.0,
        "global_energy_kwh_csv": 0.0,
        "max_abs_diff_any_float_any_partition": 0.0,
    }

    for pq_path in parquet_paths:
        csv_path = mirrored_csv_path(pq_path, args.parquet_root, args.csv_root)

        pq_df = pl.read_parquet(pq_path)
        pq_df = sort_for_release(pq_df)
        pq_schema = pq_df.schema

        # Export
        csv_export_df = prepare_for_csv(pq_df)
        write_csv_partition(csv_export_df, csv_path, args.na_token)

        # Read back and validate
        csv_df = read_csv_back(csv_path, pq_schema, args.na_token)
        csv_df = sort_for_release(csv_df)
        rep = validate_partition(pq_df, csv_df, args.float_tol)
        rel_partition = str(pq_path.relative_to(args.parquet_root))
        
        if not rep["partition_ok"] and args.stop_after_first_fail:
            print("\nStopping after first failure.")
            print(f"Partition: {rel_partition}")
            print(f"Notes: {rep['notes']}")
            return

        rel_partition = str(pq_path.relative_to(args.parquet_root))

        row_parquet = pq_df.height
        row_csv = csv_df.height

        energy_parquet = 0.0
        energy_csv = 0.0
        if "Energy_kWh_15m" in pq_df.columns:
            energy_parquet = float((pq_df.get_column("Energy_kWh_15m").sum() or 0.0))
            energy_csv = float((csv_df.get_column("Energy_kWh_15m").sum() or 0.0))

        dataset_summary["partitions_scanned"] += 1
        dataset_summary["total_rows_parquet"] += row_parquet
        dataset_summary["total_rows_csv"] += row_csv
        dataset_summary["global_energy_kwh_parquet"] += energy_parquet
        dataset_summary["global_energy_kwh_csv"] += energy_csv
        dataset_summary["max_abs_diff_any_float_any_partition"] = max(
            dataset_summary["max_abs_diff_any_float_any_partition"],
            rep["max_abs_diff_any_float"],
        )

        if rep["partition_ok"]:
            dataset_summary["partitions_passed"] += 1
        else:
            dataset_summary["partitions_failed"] += 1

        partition_reports.append({
            "partition": rel_partition,
            "csv_partition": str(csv_path.relative_to(args.csv_root)),
            "row_count_parquet": row_parquet,
            "row_count_csv": row_csv,
            "energy_kwh_parquet": energy_parquet,
            "energy_kwh_csv": energy_csv,
            **rep,
        })

        status = "PASS" if rep["partition_ok"] else "FAIL"
        print(f"[{status}] {rel_partition}")

    # Write detailed report
    report_df = pl.DataFrame(partition_reports)
    report_csv = args.report_dir / "csv_equivalence_report.csv"
    report_df.write_csv(report_csv, float_precision=10)

    # Write failures only
    failures = [r for r in partition_reports if not r["partition_ok"]]
    failures_path = args.report_dir / "csv_equivalence_failures.csv"
    if failures:
        pl.DataFrame(failures).write_csv(failures_path, float_precision=10)
    else:
        pl.DataFrame({
            "message": ["No failures detected"]
        }).write_csv(failures_path)

    # Write summary text
    summary_txt = args.report_dir / "csv_equivalence_summary.txt"
    energy_diff = dataset_summary["global_energy_kwh_parquet"] - dataset_summary["global_energy_kwh_csv"]

    lines = [
        "CSV EQUIVALENCE SUMMARY",
        "=======================",
        f"Parquet root: {args.parquet_root}",
        f"CSV root: {args.csv_root}",
        f"Partitions scanned: {dataset_summary['partitions_scanned']}",
        f"Partitions passed:  {dataset_summary['partitions_passed']}",
        f"Partitions failed:  {dataset_summary['partitions_failed']}",
        "",
        f"Total rows (Parquet): {dataset_summary['total_rows_parquet']}",
        f"Total rows (CSV):     {dataset_summary['total_rows_csv']}",
        "",
        f"Global Energy_kWh_15m sum (Parquet): {dataset_summary['global_energy_kwh_parquet']:.10f}",
        f"Global Energy_kWh_15m sum (CSV):     {dataset_summary['global_energy_kwh_csv']:.10f}",
        f"Global energy difference:            {energy_diff:.12f}",
        "",
        f"Max abs float diff across all partitions: {dataset_summary['max_abs_diff_any_float_any_partition']:.12f}",
        f"Float tolerance used: {args.float_tol}",
        "",
        "Interpretation:",
        "- CSV files are mirrored exports of the validated Gold Parquet partitions.",
        "- Partition-level checks compare row counts, column order, schema families, exact non-float equality, and float equivalence within tolerance.",
    ]
    summary_txt.write_text("\n".join(lines), encoding="utf-8")

    print("\nDone.")
    print(f"Detailed report: {report_csv}")
    print(f"Failures report: {failures_path}")
    print(f"Summary:         {summary_txt}")


if __name__ == "__main__":
    main()