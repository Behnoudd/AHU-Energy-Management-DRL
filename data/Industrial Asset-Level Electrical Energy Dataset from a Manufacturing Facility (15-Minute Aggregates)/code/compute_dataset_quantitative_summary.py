#!/usr/bin/env python3
"""
compute_dataset_quantitative_summary.py
-----------------------

Computes an objective quantitative summary of the published Gold-layer
15-minute industrial electricity dataset.

Expected Gold schema:
- AssetId
- Phase
- window_start_utc
- Energy_kWh_15m
- Demand_kW
- AvgPower_kW_15m
- SecondsObserved
- DataCoveragePct
- SecondsReliable
- ReliableCoveragePct
- IsReliableWindow

Expected partition layout:
    <gold_root>/asset_id=<AssetId>/dt_utc=<YYYY-MM-DD>/*.parquet

Outputs:
- summary.json
- summary.md
- per_asset_summary.csv
- monthly_summary.csv
- asset_type_summary.csv (optional, if metadata provided)

Optional metadata CSV columns:
- AssetId  
- AssetType
- MeteredAsset / AssetName
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Summarise Gold-layer Parquet dataset.")
    p.add_argument(
        "--gold-root",
        required=True,
        help="Root directory of the Gold-layer Parquet dataset.",
    )
    p.add_argument(
        "--outdir",
        required=True,
        help="Directory where outputs will be written.",
    )
    p.add_argument(
        "--metadata-csv",
        default=None,
        help="Optional asset metadata CSV with AssetId and AssetType columns.",
    )
    p.add_argument(
        "--table-glob",
        default="**/*.parquet",
        help="Glob under gold-root to locate parquet files (default: **/*.parquet).",
    )
    p.add_argument(
        "--reliability-threshold",
        type=float,
        default=80.0,
        help="Threshold used only for reporting if IsReliableWindow is absent. Default 80.0",
    )
    return p.parse_args()


def make_con() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(database=":memory:")
    con.execute("PRAGMA threads=4;")
    con.execute("PRAGMA enable_progress_bar;")
    return con


def detect_columns(con: duckdb.DuckDBPyConnection, parquet_glob: str) -> list[str]:
    q = f"""
    DESCRIBE SELECT * 
    FROM read_parquet('{parquet_glob}', hive_partitioning=1)
    """
    df = con.execute(q).fetchdf()
    return df["column_name"].tolist()


def choose_col(cols: list[str], *candidates: str) -> Optional[str]:
    lookup = {c.lower(): c for c in cols}
    for cand in candidates:
        if cand.lower() in lookup:
            return lookup[cand.lower()]
    return None


def sql_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def main() -> None:
    args = parse_args()

    gold_root = Path(args.gold_root)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    parquet_glob = str((gold_root / args.table_glob).as_posix())

    con = make_con()
    cols = detect_columns(con, parquet_glob)

    asset_col = choose_col(cols, "AssetId", "asset_id")
    phase_col = choose_col(cols, "Phase", "phase")
    ts_col = choose_col(cols, "window_start_utc", "WindowStartUtc", "window_start")
    energy_col = choose_col(cols, "Energy_kWh_15m", "energy_kwh_15m")
    demand_col = choose_col(cols, "Demand_kW", "demand_kw")
    avgp_col = choose_col(cols, "AvgPower_kW_15m", "avgpower_kw_15m")
    sec_obs_col = choose_col(cols, "SecondsObserved", "secondsobserved")
    cov_col = choose_col(cols, "DataCoveragePct", "datacoveragepct")
    sec_rel_col = choose_col(cols, "SecondsReliable", "secondsreliable")
    rel_cov_col = choose_col(cols, "ReliableCoveragePct", "reliablecoveragepct")
    rel_flag_col = choose_col(cols, "IsReliableWindow", "isreliablewindow")

    required = {
        "AssetId": asset_col,
        "Phase": phase_col,
        "window_start_utc": ts_col,
        "Energy_kWh_15m": energy_col,
    }
    missing = [k for k, v in required.items() if v is None]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # Build a unified base view.
    # If IsReliableWindow is absent, derive a reporting-only proxy from ReliableCoveragePct.
    derived_rel_expr = None
    if rel_flag_col is None:
        if rel_cov_col is None:
            derived_rel_expr = "NULL::INTEGER AS IsReliableWindowDerived"
        else:
            derived_rel_expr = (
                f"CASE WHEN {sql_ident(rel_cov_col)} >= {args.reliability_threshold} "
                f"THEN 1 ELSE 0 END AS IsReliableWindowDerived"
            )

    select_bits = [
        f"{sql_ident(asset_col)} AS AssetId",
        f"{sql_ident(phase_col)} AS Phase",
        f"{sql_ident(ts_col)} AS window_start_utc",
        f"{sql_ident(energy_col)} AS Energy_kWh_15m",
    ]

    if demand_col:
        select_bits.append(f"{sql_ident(demand_col)} AS Demand_kW")
    else:
        select_bits.append("NULL::DOUBLE AS Demand_kW")

    if avgp_col:
        select_bits.append(f"{sql_ident(avgp_col)} AS AvgPower_kW_15m")
    else:
        select_bits.append("NULL::DOUBLE AS AvgPower_kW_15m")

    if sec_obs_col:
        select_bits.append(f"{sql_ident(sec_obs_col)} AS SecondsObserved")
    else:
        select_bits.append("NULL::DOUBLE AS SecondsObserved")

    if cov_col:
        select_bits.append(f"{sql_ident(cov_col)} AS DataCoveragePct")
    else:
        select_bits.append("NULL::DOUBLE AS DataCoveragePct")

    if sec_rel_col:
        select_bits.append(f"{sql_ident(sec_rel_col)} AS SecondsReliable")
    else:
        select_bits.append("NULL::DOUBLE AS SecondsReliable")

    if rel_cov_col:
        select_bits.append(f"{sql_ident(rel_cov_col)} AS ReliableCoveragePct")
    else:
        select_bits.append("NULL::DOUBLE AS ReliableCoveragePct")

    if rel_flag_col:
        select_bits.append(f"{sql_ident(rel_flag_col)} AS IsReliableWindow")
    else:
        select_bits.append(derived_rel_expr)

    base_sql = f"""
    CREATE OR REPLACE VIEW gold AS
    SELECT
        {", ".join(select_bits)}
    FROM read_parquet('{parquet_glob}', hive_partitioning=1)
    """
    con.execute(base_sql)

    # Optional metadata
    metadata_available = False
    md = None

    if args.metadata_csv:
        metadata_path = Path(args.metadata_csv)
        if not metadata_path.exists():
            raise FileNotFoundError(f"Metadata CSV not found: {metadata_path}")

        md = pd.read_csv(metadata_path, dtype=str).fillna("")

        # Normalise headers
        md.columns = [c.strip() for c in md.columns]
        lower_cols = {c.lower(): c for c in md.columns}

        if "asset_id" not in lower_cols:
            raise ValueError("Metadata CSV must contain an AssetId column.")

        rename_map = {lower_cols["asset_id"]: "AssetId"}
        if "asset_type" in lower_cols:
            rename_map[lower_cols["asset_type"]] = "AssetType"
        if "metered_asset" in lower_cols:
            rename_map[lower_cols["metered_asset"]] = "MeteredAsset"
        elif "asset_name" in lower_cols:
            rename_map[lower_cols["asset_name"]] = "MeteredAsset"

        md = md.rename(columns=rename_map)

        # Keep only relevant columns if present
        keep_cols = [c for c in ["AssetId", "AssetType", "MeteredAsset"] if c in md.columns]
        md = md[keep_cols].copy()

        # Normalise join keys and text
        md["AssetId"] = md["AssetId"].astype(str).str.strip()
        if "AssetType" in md.columns:
            md["AssetType"] = md["AssetType"].astype(str).str.strip()
            md.loc[md["AssetType"].eq(""), "AssetType"] = pd.NA
        if "MeteredAsset" in md.columns:
            md["MeteredAsset"] = md["MeteredAsset"].astype(str).str.strip()
            md.loc[md["MeteredAsset"].eq(""), "MeteredAsset"] = pd.NA

        # Remove duplicate metadata rows on AssetId
        dup_md = md[md.duplicated(subset=["AssetId"], keep=False)]
        if not dup_md.empty:
            print("\nWarning: duplicate AssetId rows found in metadata; keeping first occurrence:")
            print(dup_md.to_string(index=False))
            md = md.drop_duplicates(subset=["AssetId"], keep="first")

        metadata_available = True

    # Overall summary across all rows
    overall = con.execute("""
        SELECT
            COUNT(*) AS total_rows,
            COUNT(DISTINCT AssetId) AS distinct_assets,
            COUNT(DISTINCT AssetId || '|' || Phase) AS distinct_asset_phase_streams,
            MIN(window_start_utc) AS dataset_start_utc,
            MAX(window_start_utc) AS dataset_end_utc,
            SUM(COALESCE(Energy_kWh_15m, 0)) AS total_energy_kwh,
            AVG(DataCoveragePct) AS avg_data_coverage_pct,
            MEDIAN(DataCoveragePct) AS median_data_coverage_pct,
            AVG(ReliableCoveragePct) AS avg_reliable_coverage_pct,
            MEDIAN(ReliableCoveragePct) AS median_reliable_coverage_pct,
            SUM(CASE WHEN IsReliableWindow = 1 THEN 1 ELSE 0 END) AS reliable_rows,
            SUM(CASE WHEN IsReliableWindow = 0 THEN 1 ELSE 0 END) AS nonreliable_rows
        FROM gold
    """).fetchdf()

    # ALL-phase summary, usually most paper-friendly
    all_phase = con.execute("""
        SELECT
            COUNT(*) AS total_all_phase_rows,
            COUNT(DISTINCT AssetId) AS assets_in_all_phase,
            MIN(window_start_utc) AS all_phase_start_utc,
            MAX(window_start_utc) AS all_phase_end_utc,
            SUM(COALESCE(Energy_kWh_15m, 0)) AS total_all_phase_energy_kwh,
            AVG(DataCoveragePct) AS avg_all_phase_data_coverage_pct,
            MEDIAN(DataCoveragePct) AS median_all_phase_data_coverage_pct,
            AVG(ReliableCoveragePct) AS avg_all_phase_reliable_coverage_pct,
            MEDIAN(ReliableCoveragePct) AS median_all_phase_reliable_coverage_pct,
            SUM(CASE WHEN IsReliableWindow = 1 THEN 1 ELSE 0 END) AS reliable_all_phase_rows
        FROM gold
        WHERE UPPER(Phase) = 'ALL'
    """).fetchdf()

    # Monthly summary on ALL phase
    monthly = con.execute("""
        SELECT
            DATE_TRUNC('month', window_start_utc) AS month_utc,
            COUNT(*) AS rows_all_phase,
            COUNT(DISTINCT AssetId) AS active_assets,
            SUM(COALESCE(Energy_kWh_15m, 0)) AS energy_kwh,
            AVG(DataCoveragePct) AS avg_data_coverage_pct,
            AVG(ReliableCoveragePct) AS avg_reliable_coverage_pct,
            SUM(CASE WHEN IsReliableWindow = 1 THEN 1 ELSE 0 END) AS reliable_rows
        FROM gold
        WHERE UPPER(Phase) = 'ALL'
        GROUP BY 1
        ORDER BY 1
    """).fetchdf()

    # Per-asset summary on ALL phase
    per_asset_sql = """
        SELECT
            g.AssetId,
            MIN(g.window_start_utc) AS start_utc,
            MAX(g.window_start_utc) AS end_utc,
            COUNT(*) AS rows_all_phase,
            SUM(COALESCE(g.Energy_kWh_15m, 0)) AS total_energy_kwh,
            AVG(g.DataCoveragePct) AS avg_data_coverage_pct,
            MEDIAN(g.DataCoveragePct) AS median_data_coverage_pct,
            AVG(g.ReliableCoveragePct) AS avg_reliable_coverage_pct,
            MEDIAN(g.ReliableCoveragePct) AS median_reliable_coverage_pct,
            SUM(CASE WHEN g.IsReliableWindow = 1 THEN 1 ELSE 0 END) AS reliable_rows,
            100.0 * SUM(CASE WHEN g.IsReliableWindow = 1 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) AS reliable_row_pct
        FROM gold g
        WHERE UPPER(g.Phase) = 'ALL'
        GROUP BY g.AssetId
        ORDER BY g.AssetId
    """
    per_asset = con.execute(per_asset_sql).fetchdf()

    if metadata_available and md is not None:
        per_asset["AssetId"] = per_asset["AssetId"].astype(str).str.strip()
        per_asset = per_asset.merge(md, on="AssetId", how="left")

        unmatched = per_asset["AssetType"].isna().sum() if "AssetType" in per_asset.columns else len(per_asset)
        print(f"\nMetadata merge complete. Unmatched AssetType rows: {unmatched} / {len(per_asset)}")

        if "AssetType" in per_asset.columns and unmatched > 0:
            print("Unmatched AssetIds:")
            print(per_asset.loc[per_asset["AssetType"].isna(), ["AssetId"]].to_string(index=False))

    asset_type = None
    if metadata_available and "AssetType" in per_asset.columns:
        asset_type_input = per_asset.copy()
        asset_type_input["AssetType"] = asset_type_input["AssetType"].fillna("Unclassified").astype(str).str.strip()

        asset_type = (
            asset_type_input.groupby("AssetType", dropna=False)
            .agg(
                assets=("AssetId", "nunique"),
                total_rows=("rows_all_phase", "sum"),
                total_energy_kwh=("total_energy_kwh", "sum"),
                avg_reliable_row_pct=("reliable_row_pct", "mean"),
                avg_data_coverage_pct=("avg_data_coverage_pct", "mean"),
            )
            .reset_index()
            .sort_values(["AssetType"])
        )

    # Expected 15-minute windows per asset from observed span
    # Useful but note this is span-based, not commissioning-calendar-based.
    if not per_asset.empty:
        per_asset["span_days"] = (
            pd.to_datetime(per_asset["end_utc"]) - pd.to_datetime(per_asset["start_utc"])
        ).dt.total_seconds() / 86400.0
        per_asset["expected_windows_from_span"] = (
            ((pd.to_datetime(per_asset["end_utc"]) - pd.to_datetime(per_asset["start_utc"])).dt.total_seconds() / 900.0) + 1
        ).round().astype("Int64")
        per_asset["row_completeness_vs_span_pct"] = (
            100.0 * per_asset["rows_all_phase"] / per_asset["expected_windows_from_span"]
        )

    # Compose JSON summary
    overall_row = overall.iloc[0].to_dict()
    all_phase_row = all_phase.iloc[0].to_dict()

    summary = {
        "dataset_summary_all_rows": overall_row,
        "dataset_summary_all_phase": all_phase_row,
        "files_scanned_glob": parquet_glob,
        "notes": {
            "all_rows_includes_phases": "Includes L1/L2/L3/ALL if present.",
            "all_phase_summary": "Usually most appropriate for manuscript-level dataset summary.",
            "reliable_window_definition_source": (
                "IsReliableWindow column from dataset"
                if rel_flag_col is not None
                else f"Derived from ReliableCoveragePct >= {args.reliability_threshold}"
            ),
        },
    }

    # Write outputs
    (outdir / "summary.json").write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    per_asset.to_csv(outdir / "per_asset_summary.csv", index=False)
    monthly.to_csv(outdir / "monthly_summary.csv", index=False)
    if asset_type is not None:
        asset_type.to_csv(outdir / "asset_type_summary.csv", index=False)

    # Build markdown block for easy paper pasting
    md_lines = []
    md_lines.append("# Gold-layer dataset quantitative summary")
    md_lines.append("")
    md_lines.append("## Overall released table")
    md_lines.append("")
    md_lines.append(f"- Total rows: **{int(overall_row['total_rows']):,}**")
    md_lines.append(f"- Distinct assets: **{int(overall_row['distinct_assets'])}**")
    md_lines.append(f"- Distinct asset-phase streams: **{int(overall_row['distinct_asset_phase_streams'])}**")
    md_lines.append(f"- Dataset start (UTC): **{overall_row['dataset_start_utc']}**")
    md_lines.append(f"- Dataset end (UTC): **{overall_row['dataset_end_utc']}**")
    md_lines.append(f"- Total energy (all rows): **{overall_row['total_energy_kwh']:,.3f} kWh**")
    if pd.notna(overall_row["avg_data_coverage_pct"]):
        md_lines.append(f"- Mean data coverage: **{overall_row['avg_data_coverage_pct']:.2f}%**")
    if pd.notna(overall_row["median_data_coverage_pct"]):
        md_lines.append(f"- Median data coverage: **{overall_row['median_data_coverage_pct']:.2f}%**")
    if pd.notna(overall_row["avg_reliable_coverage_pct"]):
        md_lines.append(f"- Mean reliable coverage: **{overall_row['avg_reliable_coverage_pct']:.2f}%**")
    if pd.notna(overall_row["median_reliable_coverage_pct"]):
        md_lines.append(f"- Median reliable coverage: **{overall_row['median_reliable_coverage_pct']:.2f}%**")
    md_lines.append("")

    md_lines.append("## ALL-phase manuscript summary")
    md_lines.append("")
    md_lines.append(f"- Total ALL-phase rows: **{int(all_phase_row['total_all_phase_rows']):,}**")
    md_lines.append(f"- Assets represented in ALL phase: **{int(all_phase_row['assets_in_all_phase'])}**")
    md_lines.append(f"- ALL-phase start (UTC): **{all_phase_row['all_phase_start_utc']}**")
    md_lines.append(f"- ALL-phase end (UTC): **{all_phase_row['all_phase_end_utc']}**")
    md_lines.append(f"- Total ALL-phase energy: **{all_phase_row['total_all_phase_energy_kwh']:,.3f} kWh**")
    if pd.notna(all_phase_row["avg_all_phase_data_coverage_pct"]):
        md_lines.append(f"- Mean ALL-phase data coverage: **{all_phase_row['avg_all_phase_data_coverage_pct']:.2f}%**")
    if pd.notna(all_phase_row["median_all_phase_data_coverage_pct"]):
        md_lines.append(f"- Median ALL-phase data coverage: **{all_phase_row['median_all_phase_data_coverage_pct']:.2f}%**")
    if pd.notna(all_phase_row["avg_all_phase_reliable_coverage_pct"]):
        md_lines.append(f"- Mean ALL-phase reliable coverage: **{all_phase_row['avg_all_phase_reliable_coverage_pct']:.2f}%**")
    if pd.notna(all_phase_row["median_all_phase_reliable_coverage_pct"]):
        md_lines.append(f"- Median ALL-phase reliable coverage: **{all_phase_row['median_all_phase_reliable_coverage_pct']:.2f}%**")
    if int(all_phase_row["total_all_phase_rows"]) > 0:
        reliable_pct = 100.0 * int(all_phase_row["reliable_all_phase_rows"]) / int(all_phase_row["total_all_phase_rows"])
        md_lines.append(
            f"- Reliable ALL-phase windows: **{int(all_phase_row['reliable_all_phase_rows']):,} "
            f"({reliable_pct:.2f}%)**"
        )
    md_lines.append("")

    md_lines.append("## Output files")
    md_lines.append("")
    md_lines.append("- `summary.json`")
    md_lines.append("- `summary.md`")
    md_lines.append("- `per_asset_summary.csv`")
    md_lines.append("- `monthly_summary.csv`")
    if asset_type is not None:
        md_lines.append("- `asset_type_summary.csv`")
    md_lines.append("")

    (outdir / "summary.md").write_text("\n".join(md_lines), encoding="utf-8")

    # Console preview
    print("\n=== DATASET SUMMARY (ALL ROWS) ===")
    print(overall.to_string(index=False))
    print("\n=== DATASET SUMMARY (ALL PHASE) ===")
    print(all_phase.to_string(index=False))
    print("\nWrote outputs to:", outdir)


if __name__ == "__main__":
    main()