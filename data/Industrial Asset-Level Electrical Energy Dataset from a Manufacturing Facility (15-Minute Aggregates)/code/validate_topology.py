#!/usr/bin/env python3
"""
validate_topology.py
-----------------------------

Whole-dataset topology verification for 15-minute industrial electricity data.

Checks:
1. Edge-derived parent-child containment:
   child_energy <= parent_energy per window
2. Edge-derived parent-group closure:
   sum(children) <= parent_energy per window
3. Correlation structure across all ALL-phase meters
4. Probable parent rankings inferred from data

Inputs:
- Gold-layer parquet root
- Edge list CSV with columns:
    Source, Target, RelationshipNote
- Optional asset table CSV with columns:
    AssetId, AssetType

Outputs:
- declared_parent_child_containment.csv
- declared_parent_group_closure.csv
- all_meter_correlations.csv
- probable_parent_rankings.csv
- topology_summary.md
"""

from __future__ import annotations

import argparse
from pathlib import Path
import itertools
import math

import duckdb
import numpy as np
import pandas as pd


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Verify electrical topology from Gold-layer dataset.")
    p.add_argument("--gold-root", required=True, help="Root folder of Gold-layer parquet dataset.")
    p.add_argument("--edges-csv", required=True, help="Edge list CSV with Source, Target, RelationshipNote.")
    p.add_argument("--assets-csv", default=None, help="Optional asset table CSV with AssetId, AssetType.")
    p.add_argument("--outdir", required=True, help="Output directory.")
    p.add_argument("--phase", default="ALL", help="Phase to use, default ALL.")
    p.add_argument("--table-glob", default="**/*.parquet", help="Glob under gold-root, default **/*.parquet")
    p.add_argument("--min_overlap_windows", type=int, default=96, help="Minimum overlapping windows for pairwise tests.")
    p.add_argument("--tiny_tol_kwh", type=float, default=1e-9, help="Tiny tolerance to ignore floating-point noise.")
    p.add_argument("--soft_margin_kwh", type=float, default=0.05, help="Soft margin for containment violations.")
    return p.parse_args()


def load_edges(edges_csv: Path) -> pd.DataFrame:
    edges = pd.read_csv(edges_csv)
    required_cols = {"Source", "Target", "RelationshipNote"}
    missing = required_cols - set(edges.columns)
    if missing:
        raise ValueError(f"Edge CSV missing required columns: {sorted(missing)}")

    edges["Source"] = edges["Source"].astype(str).str.strip()
    edges["Target"] = edges["Target"].astype(str).str.strip()
    edges["RelationshipNote"] = edges["RelationshipNote"].astype(str).str.strip()

    # Keep only electrical relationships
    electrical = edges[
        edges["RelationshipNote"].str.contains("supply|distribution", case=False, regex=True)
    ].copy()

    return electrical


def build_metadata_from_edges(edges: pd.DataFrame, assets_csv: Path | None = None) -> pd.DataFrame:
    # Parent lists by target
    parent_map = edges.groupby("Target")["Source"].apply(list).to_dict()

    # Child lists by source
    child_map = edges.groupby("Source")["Target"].apply(list).to_dict()

    asset_ids = sorted(set(edges["Source"]).union(set(edges["Target"])))

    md = pd.DataFrame({"AssetId": asset_ids})
    md["ParentList"] = md["AssetId"].map(lambda x: parent_map.get(x, []))
    md["ChildList"] = md["AssetId"].map(lambda x: child_map.get(x, []))

    if assets_csv:
        assets = pd.read_csv(assets_csv)
        if "AssetId" not in assets.columns:
            raise ValueError("Assets CSV must contain AssetId.")
        assets["AssetId"] = assets["AssetId"].astype(str).str.strip()

        if "AssetType" in assets.columns:
            assets["AssetType"] = assets["AssetType"].astype(str).str.strip()
            md = md.merge(assets[["AssetId", "AssetType"]], on="AssetId", how="left")
        else:
            md["AssetType"] = ""
    else:
        md["AssetType"] = ""

    return md

def safe_mkdir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def load_gold(gold_root: Path, table_glob: str, phase: str) -> pd.DataFrame:
    con = duckdb.connect(database=":memory:")
    q = f"""
    SELECT
        AssetId,
        Phase,
        window_start_utc,
        Energy_kWh_15m,
        Demand_kW,
        DataCoveragePct,
        ReliableCoveragePct,
        IsReliableWindow
    FROM read_parquet('{(gold_root / table_glob).as_posix()}', hive_partitioning=1)
    WHERE UPPER(Phase) = UPPER('{phase}')
    ORDER BY window_start_utc, AssetId
    """
    return con.execute(q).fetchdf()

def build_wide(df_gold: pd.DataFrame) -> pd.DataFrame:
    wide = df_gold.pivot_table(
        index="window_start_utc",
        columns="AssetId",
        values="Energy_kWh_15m",
        aggfunc="mean"
    ).sort_index()

    # Defensive collapse in case duplicate labels still exist
    if wide.columns.duplicated().any():
        print("Warning: duplicate AssetId columns detected after pivot; collapsing by column-wise mean.")
        wide = wide.T.groupby(level=0).mean().T

    return wide

def child_parent_containment(
    wide: pd.DataFrame,
    metadata: pd.DataFrame,
    tiny_tol_kwh: float,
    soft_margin_kwh: float,
    min_overlap_windows: int,
) -> pd.DataFrame:
    rows = []

    for _, r in metadata.iterrows():
        child = r["AssetId"]
        parents = r["ParentList"]

        if child not in wide.columns:
            continue

        for parent in parents:
            if parent not in wide.columns:
                rows.append({
                    "parent": parent,
                    "child": child,
                    "status": "parent_missing_in_gold",
                    "overlap_windows": 0,
                    "hard_violations": np.nan,
                    "hard_violation_rate_pct": np.nan,
                    "soft_violations": np.nan,
                    "soft_violation_rate_pct": np.nan,
                    "mean_child_minus_parent_kwh": np.nan,
                    "median_child_minus_parent_kwh": np.nan,
                    "p95_child_minus_parent_kwh": np.nan,
                    "corr": np.nan,
                })
                continue

            tmp = wide[[child, parent]].dropna()
            overlap = len(tmp)

            if overlap < min_overlap_windows:
                rows.append({
                    "parent": parent,
                    "child": child,
                    "status": "insufficient_overlap",
                    "overlap_windows": overlap,
                    "hard_violations": np.nan,
                    "hard_violation_rate_pct": np.nan,
                    "soft_violations": np.nan,
                    "soft_violation_rate_pct": np.nan,
                    "mean_child_minus_parent_kwh": np.nan,
                    "median_child_minus_parent_kwh": np.nan,
                    "p95_child_minus_parent_kwh": np.nan,
                    "corr": np.nan,
                })
                continue

            diff = tmp[child] - tmp[parent]
            hard = int((diff > tiny_tol_kwh).sum())
            soft = int((diff > soft_margin_kwh).sum())

            corr = tmp[child].corr(tmp[parent])

            rows.append({
                "parent": parent,
                "child": child,
                "status": "ok",
                "overlap_windows": overlap,
                "hard_violations": hard,
                "hard_violation_rate_pct": 100.0 * hard / overlap,
                "soft_violations": soft,
                "soft_violation_rate_pct": 100.0 * soft / overlap,
                "mean_child_minus_parent_kwh": diff.mean(),
                "median_child_minus_parent_kwh": diff.median(),
                "p95_child_minus_parent_kwh": diff.quantile(0.95),
                "corr": corr,
            })

    out = pd.DataFrame(rows).sort_values(["child", "parent"], kind="stable")
    return out


def parent_group_closure(
    wide: pd.DataFrame,
    metadata: pd.DataFrame,
    tiny_tol_kwh: float,
    soft_margin_kwh: float,
    min_overlap_windows: int,
) -> pd.DataFrame:
    rows = []

    for _, r in metadata.iterrows():
        parent = r["AssetId"]
        children = r["ChildList"]

        if len(children) == 0:
            continue

        if parent not in wide.columns:
            rows.append({
                "parent": parent,
                "status": "parent_missing_in_gold",
                "declared_children_count": len(children),
                "children_present_count": 0,
                "children_missing": "; ".join(children),
                "overlap_windows": 0,
                "hard_violations": np.nan,
                "hard_violation_rate_pct": np.nan,
                "soft_violations": np.nan,
                "soft_violation_rate_pct": np.nan,
                "mean_children_minus_parent_kwh": np.nan,
                "median_children_minus_parent_kwh": np.nan,
                "p95_children_minus_parent_kwh": np.nan,
            })
            continue

        present_children = [c for c in children if c in wide.columns]
        missing_children = [c for c in children if c not in wide.columns]

        if len(present_children) == 0:
            rows.append({
                "parent": parent,
                "status": "no_children_present_in_gold",
                "declared_children_count": len(children),
                "children_present_count": 0,
                "children_missing": "; ".join(missing_children),
                "overlap_windows": 0,
                "hard_violations": np.nan,
                "hard_violation_rate_pct": np.nan,
                "soft_violations": np.nan,
                "soft_violation_rate_pct": np.nan,
                "mean_children_minus_parent_kwh": np.nan,
                "median_children_minus_parent_kwh": np.nan,
                "p95_children_minus_parent_kwh": np.nan,
            })
            continue

        cols = [parent] + present_children
        tmp = wide[cols].dropna()
        overlap = len(tmp)

        if overlap < min_overlap_windows:
            rows.append({
                "parent": parent,
                "status": "insufficient_overlap",
                "declared_children_count": len(children),
                "children_present_count": len(present_children),
                "children_missing": "; ".join(missing_children),
                "overlap_windows": overlap,
                "hard_violations": np.nan,
                "hard_violation_rate_pct": np.nan,
                "soft_violations": np.nan,
                "soft_violation_rate_pct": np.nan,
                "mean_children_minus_parent_kwh": np.nan,
                "median_children_minus_parent_kwh": np.nan,
                "p95_children_minus_parent_kwh": np.nan,
            })
            continue

        child_sum = tmp[present_children].sum(axis=1)
        diff = child_sum - tmp[parent]

        hard = int((diff > tiny_tol_kwh).sum())
        soft = int((diff > soft_margin_kwh).sum())

        rows.append({
            "parent": parent,
            "status": "ok",
            "declared_children_count": len(children),
            "children_present_count": len(present_children),
            "children_missing": "; ".join(missing_children),
            "overlap_windows": overlap,
            "hard_violations": hard,
            "hard_violation_rate_pct": 100.0 * hard / overlap,
            "soft_violations": soft,
            "soft_violation_rate_pct": 100.0 * soft / overlap,
            "mean_children_minus_parent_kwh": diff.mean(),
            "median_children_minus_parent_kwh": diff.median(),
            "p95_children_minus_parent_kwh": diff.quantile(0.95),
        })

    out = pd.DataFrame(rows).sort_values(["parent"], kind="stable")
    return out


def all_meter_correlations(wide: pd.DataFrame, min_overlap_windows: int) -> pd.DataFrame:
    # Collapse duplicate meter columns if any exist
    if wide.columns.duplicated().any():
        print("Warning: duplicate AssetId columns detected in wide table; collapsing by column-wise mean.")
        wide = wide.T.groupby(level=0).mean().T

    cols = list(wide.columns)
    rows = []

    for a, b in itertools.product(cols, cols):
        tmp = wide[[a, b]].dropna()
        overlap = len(tmp)

        if overlap < min_overlap_windows:
            corr = np.nan
        else:
            s1 = tmp[a]
            s2 = tmp[b]

            # If duplicates somehow still survive, coerce to Series
            if isinstance(s1, pd.DataFrame):
                s1 = s1.mean(axis=1)
            if isinstance(s2, pd.DataFrame):
                s2 = s2.mean(axis=1)

            corr = s1.corr(s2)

        rows.append({
            "asset_a": a,
            "asset_b": b,
            "overlap_windows": overlap,
            "corr": corr,
        })

    return pd.DataFrame(rows)


def probable_parent_rankings(
    wide: pd.DataFrame,
    metadata: pd.DataFrame,
    min_overlap_windows: int,
    tiny_tol_kwh: float,
) -> pd.DataFrame:
    # Candidate parents = any asset that appears as a declared parent somewhere,
    # plus distribution/supply-type assets.
    declared_parents = set()
    for _, r in metadata.iterrows():
        declared_parents.update(r["ParentList"])

    type_like = metadata[
        metadata["AssetType"].fillna("").str.contains(
            "Distribution|Grid|Photovoltaic|Supply|Utilities Supply",
            case=False,
            regex=True,
        )
    ]["AssetId"].tolist()

    candidates = sorted((declared_parents.union(type_like)).intersection(set(wide.columns)))

    rows = []
    for child in sorted(wide.columns):
        for parent in candidates:
            if child == parent:
                continue

            tmp = wide[[child, parent]].dropna()
            overlap = len(tmp)
            if overlap < min_overlap_windows:
                continue

            diff = tmp[child] - tmp[parent]
            hard_violation_rate = float((diff > tiny_tol_kwh).sum()) / overlap
            corr = tmp[child].corr(tmp[parent])

            # heuristic score:
            # reward high corr, penalize violations, mildly reward parent usually larger
            parent_dominance = float((tmp[parent] >= tmp[child] - tiny_tol_kwh).sum()) / overlap
            score = (
                0.65 * (0 if pd.isna(corr) else corr)
                + 0.25 * parent_dominance
                - 0.60 * hard_violation_rate
            )

            rows.append({
                "child": child,
                "candidate_parent": parent,
                "overlap_windows": overlap,
                "corr": corr,
                "hard_violation_rate_pct": 100.0 * hard_violation_rate,
                "parent_dominance_pct": 100.0 * parent_dominance,
                "score": score,
            })

    out = pd.DataFrame(rows)
    if out.empty:
        return out

    out = out.sort_values(["child", "score", "corr"], ascending=[True, False, False], kind="stable")
    out["rank"] = out.groupby("child").cumcount() + 1
    return out


def build_summary_md(
    metadata: pd.DataFrame,
    df_gold: pd.DataFrame,
    containment: pd.DataFrame,
    group_closure: pd.DataFrame,
    rankings: pd.DataFrame,
) -> str:
    total_assets_in_metadata = metadata["AssetId"].nunique()
    total_assets_in_gold = df_gold["AssetId"].nunique()
    total_rows = len(df_gold)
    start_utc = df_gold["window_start_utc"].min()
    end_utc = df_gold["window_start_utc"].max()

    lines = []
    lines.append("# Topology verification summary")
    lines.append("")
    lines.append("## Dataset scope")
    lines.append("")
    lines.append(f"- Assets in metadata: **{total_assets_in_metadata}**")
    lines.append(f"- Assets present in Gold layer ({df_gold['Phase'].iloc[0]} phase): **{total_assets_in_gold}**")
    lines.append(f"- Rows analysed: **{total_rows:,}**")
    lines.append(f"- Window range: **{start_utc}** to **{end_utc}**")
    lines.append("")

    lines.append("## Declared parent-child containment")
    lines.append("")
    ok_pc = containment[containment["status"] == "ok"].copy()
    if not ok_pc.empty:
        zero_hard = int((ok_pc["hard_violations"] == 0).sum())
        low_hard = int((ok_pc["hard_violation_rate_pct"] <= 0.1).sum())
        total_ok = len(ok_pc)
        lines.append(f"- Relationships tested: **{total_ok}**")
        lines.append(f"- Zero hard-violation relationships: **{zero_hard}**")
        lines.append(f"- Relationships with hard-violation rate ≤ 0.1%: **{low_hard}**")
        worst = ok_pc.sort_values("hard_violation_rate_pct", ascending=False).head(10)
        lines.append("")
        lines.append("### Highest declared parent-child violation rates")
        lines.append("")
        lines.append("| Parent | Child | Overlap windows | Hard violations | Hard violation rate (%) | Corr |")
        lines.append("|---|---:|---:|---:|---:|---:|")
        for _, r in worst.iterrows():
            lines.append(
                f"| {r['parent']} | {r['child']} | {int(r['overlap_windows'])} | "
                f"{int(r['hard_violations'])} | {r['hard_violation_rate_pct']:.3f} | {r['corr']:.3f} |"
            )
    else:
        lines.append("- No declared parent-child relationships could be tested.")
    lines.append("")

    lines.append("## Declared parent-group closure")
    lines.append("")
    ok_pg = group_closure[group_closure["status"] == "ok"].copy()
    if not ok_pg.empty:
        zero_hard_pg = int((ok_pg["hard_violations"] == 0).sum())
        total_ok_pg = len(ok_pg)
        lines.append(f"- Parent groups tested: **{total_ok_pg}**")
        lines.append(f"- Parent groups with zero hard violations: **{zero_hard_pg}**")
        worst_pg = ok_pg.sort_values("hard_violation_rate_pct", ascending=False).head(10)
        lines.append("")
        lines.append("### Highest parent-group closure violation rates")
        lines.append("")
        lines.append("| Parent | Present children | Overlap windows | Hard violations | Hard violation rate (%) |")
        lines.append("|---|---:|---:|---:|---:|")
        for _, r in worst_pg.iterrows():
            lines.append(
                f"| {r['parent']} | {int(r['children_present_count'])} | {int(r['overlap_windows'])} | "
                f"{int(r['hard_violations'])} | {r['hard_violation_rate_pct']:.3f} |"
            )
    else:
        lines.append("- No parent-group closures could be tested.")
    lines.append("")

    lines.append("## Probable parent rankings")
    lines.append("")
    if not rankings.empty:
        top = rankings[rankings["rank"] == 1].copy().sort_values("score", ascending=False).head(20)
        lines.append("| Child | Best inferred parent | Corr | Hard violation rate (%) | Parent dominance (%) | Score |")
        lines.append("|---|---:|---:|---:|---:|---:|")
        for _, r in top.iterrows():
            lines.append(
                f"| {r['child']} | {r['candidate_parent']} | {r['corr']:.3f} | "
                f"{r['hard_violation_rate_pct']:.3f} | {r['parent_dominance_pct']:.2f} | {r['score']:.3f} |"
            )
    else:
        lines.append("- No inferred rankings available.")
    lines.append("")

    lines.append("## Interpretation note")
    lines.append("")
    lines.append(
        "These tests provide statistical support or contradiction for an assumed monitoring hierarchy, "
        "but they do not prove electrical topology in the strict sense. In particular, magnitude-only "
        "Gold-layer data cannot resolve reverse power flow or export direction."
    )
    lines.append("")

    return "\n".join(lines)


def main() -> None:
    args = parse_args()

    gold_root = Path(args.gold_root)
    outdir = Path(args.outdir)
    edges_csv = Path(args.edges_csv)
    assets_csv = Path(args.assets_csv) if args.assets_csv else None
    safe_mkdir(outdir)

    df_gold = load_gold(gold_root, args.table_glob, args.phase)

    if df_gold.empty:
        raise ValueError("No Gold-layer rows found for the requested phase.")
    
    dup_check = (
        df_gold.groupby(["window_start_utc", "AssetId", "Phase"])
        .size()
        .reset_index(name="n")
    )

    dups = dup_check[dup_check["n"] > 1]

    print(f"Duplicate asset-window-phase rows: {len(dups)}")
    if not dups.empty:
        print(dups.head(20).to_string(index=False))

    edges = load_edges(edges_csv)
    md = build_metadata_from_edges(edges, assets_csv=assets_csv)

    wide = build_wide(df_gold)

    containment = child_parent_containment(
        wide=wide,
        metadata=md,
        tiny_tol_kwh=args.tiny_tol_kwh,
        soft_margin_kwh=args.soft_margin_kwh,
        min_overlap_windows=args.min_overlap_windows,
    )

    group_closure = parent_group_closure(
        wide=wide,
        metadata=md,
        tiny_tol_kwh=args.tiny_tol_kwh,
        soft_margin_kwh=args.soft_margin_kwh,
        min_overlap_windows=args.min_overlap_windows,
    )

    corr_long = all_meter_correlations(wide, min_overlap_windows=args.min_overlap_windows)
    corr_matrix = corr_long.pivot(index="asset_a", columns="asset_b", values="corr").sort_index(axis=0).sort_index(axis=1)

    rankings = probable_parent_rankings(
        wide=wide,
        metadata=md,
        min_overlap_windows=args.min_overlap_windows,
        tiny_tol_kwh=args.tiny_tol_kwh,
    )

    containment.to_csv(outdir / "declared_parent_child_containment.csv", index=False)
    group_closure.to_csv(outdir / "declared_parent_group_closure.csv", index=False)
    corr_matrix.to_csv(outdir / "all_meter_correlations.csv")
    rankings.to_csv(outdir / "probable_parent_rankings.csv", index=False)

    summary_md = build_summary_md(
        metadata=md,
        df_gold=df_gold,
        containment=containment,
        group_closure=group_closure,
        rankings=rankings,
    )
    (outdir / "topology_summary.md").write_text(summary_md, encoding="utf-8")

    print("\nDone.")
    print("Outputs written to:", outdir)
    print(" - declared_parent_child_containment.csv")
    print(" - declared_parent_group_closure.csv")
    print(" - all_meter_correlations.csv")
    print(" - probable_parent_rankings.csv")
    print(" - topology_summary.md")


if __name__ == "__main__":
    main()