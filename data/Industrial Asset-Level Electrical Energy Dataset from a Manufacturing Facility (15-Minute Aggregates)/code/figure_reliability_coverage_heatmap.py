import duckdb
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib as mpl

mpl.rcParams['font.family'] = 'serif'
mpl.rcParams['font.serif'] = ['Palatino Linotype', 'Palatino', 'DejaVu Serif']

# ============================================================
# CONFIG
# ============================================================
GOLD_GLOB = r"data_parquet/**/**.parquet"
PHASE_FILTER = "ALL"   # set to None to skip Phase filtering, or keep "ALL"
OUT_PNG = "Fig_Reliability_Heatmap_Asset_by_Date.png"
OUT_SVG = "Fig_Reliability_Heatmap_Asset_by_Date.svg"

# If you want the heatmap limited to a particular date range, set these:
DATE_MIN = None  # e.g. "2025-01-01"
DATE_MAX = None  # e.g. "2025-12-31"

# Figure sizing
FIG_WIDTH_IN = 12
ROW_HEIGHT_IN = 0.42 # inches per asset row

# ============================================================
# LOAD + AGGREGATE (DuckDB)
# ============================================================
con = duckdb.connect()

where_clauses = []
if PHASE_FILTER is not None:
    where_clauses.append(f"Phase = '{PHASE_FILTER}'")

if DATE_MIN is not None:
    where_clauses.append(f"window_start_utc >= TIMESTAMP '{DATE_MIN}'")
if DATE_MAX is not None:
    where_clauses.append(f"window_start_utc < TIMESTAMP '{DATE_MAX}'")

where_sql = ""
if where_clauses:
    where_sql = "WHERE " + " AND ".join(where_clauses)

query = f"""
    WITH base AS (
        SELECT
            AssetId,
            window_start_utc,
            DataCoveragePct,
            IsReliableWindow
        FROM read_parquet('{GOLD_GLOB}')
        {where_sql}
    ),
    daily AS (
        SELECT
            AssetId,
            CAST(window_start_utc AS DATE) AS day,
            AVG(CASE WHEN IsReliableWindow THEN 1 ELSE 0 END) AS ReliableFrac,
            AVG(DataCoveragePct) AS CoveragePctMean
        FROM base
        GROUP BY 1, 2
    ),
    firsts AS (
        SELECT
            AssetId,
            MIN(day) AS first_day
        FROM daily
        GROUP BY 1
    )
    SELECT
        d.AssetId,
        d.day,
        d.ReliableFrac,
        d.CoveragePctMean,
        f.first_day
    FROM daily d
    JOIN firsts f USING (AssetId)
    ORDER BY f.first_day, d.AssetId, d.day
"""

df_day = con.execute(query).df()

if df_day.empty:
    raise SystemExit("No rows returned. Check GOLD_GLOB, Phase filter, and date bounds.")

df_day["day"] = pd.to_datetime(df_day["day"])
df_day["AssetId"] = df_day["AssetId"].astype(str).str.strip()

# ============================================================
# PIVOT TO MATRIX (asset x day)
# ============================================================
asset_order = (
    df_day[["AssetId", "first_day"]]
    .drop_duplicates()
    .sort_values(["first_day", "AssetId"])
    ["AssetId"]
    .tolist()
)

mat = (
    df_day.pivot(index="AssetId", columns="day", values="ReliableFrac")
    .reindex(asset_order)
)

mat = mat.reindex(sorted(mat.columns), axis=1)

assets = mat.index.tolist()
days = mat.columns.to_pydatetime()

# Mask NaNs so they render as white
Z = np.ma.masked_invalid(mat.values.astype(float))

# ============================================================
# PLOT
# ============================================================
fig_height = max(4, ROW_HEIGHT_IN * len(assets))
fig, ax = plt.subplots(figsize=(FIG_WIDTH_IN, fig_height))

cmap = plt.cm.viridis.copy()
cmap.set_bad(color="white")

im = ax.imshow(
    Z,
    aspect="auto",
    interpolation="nearest",
    vmin=0,
    vmax=1,
    cmap=cmap,
)

# ------------------------------------------------------------
# Horizontal leader lines: from left edge to first day with data
# ------------------------------------------------------------
for row_idx in range(Z.shape[0]):
    row = mat.iloc[row_idx].values
    valid = np.where(~pd.isna(row))[0]

    if len(valid) == 0:
        continue

    first_valid_col = valid[0]

    if first_valid_col > 0:
        ax.hlines(
            y=row_idx,
            xmin=-0.5,
            xmax=first_valid_col - 0.5,
            colors="black",
            linestyles=":",
            linewidth=0.5,
            alpha=0.5,
            zorder=3
        )

# Y axis
ax.set_yticks(np.arange(len(assets)))
ax.set_yticklabels(assets, fontsize=12)

# X axis: actual month starts
month_positions = [i for i, d in enumerate(days) if d.day == 1]
month_labels = [d.strftime("%Y-%m") for d in days if d.day == 1]
ax.set_xticks(month_positions)
ax.set_xticklabels(month_labels, rotation=45, ha="right", fontsize=12)

# ------------------------------------------------------------
# Vertical guide lines at month ticks
# ------------------------------------------------------------
for xpos in month_positions:
    ax.vlines(
        x=xpos - 0.5,
        ymin=-0.5,
        ymax=Z.shape[0] - 0.5,
        colors="black",
        linestyles=":",
        linewidth=0.4,
        alpha=0.18,
        zorder=2
    )

ax.set_xlabel("Calendar Date (UTC)", fontsize=16, labelpad=8)
ax.set_ylabel("Metered Industrial Asset", fontsize=16)
#ax.set_title("Daily fraction of reliable 15-minute windows by asset", pad=12)

# Colorbar
cbar = fig.colorbar(im, ax=ax, fraction=0.025, pad=0.03)
cbar.ax.tick_params(labelsize=12) 
cbar.set_label("Fraction of reliable 15-min windows", fontsize=14, labelpad=10)

fig.subplots_adjust(left=0.16, right=0.92, bottom=0.16, top=0.93)

fig.savefig(OUT_PNG, dpi=400, bbox_inches="tight", pad_inches=0.1)
plt.savefig(OUT_SVG)
print(f"Saved: {OUT_PNG}")

plt.show()