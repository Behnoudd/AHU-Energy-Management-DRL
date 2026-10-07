import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib as mpl

mpl.rcParams['font.family'] = 'serif'
mpl.rcParams['font.serif'] = ['Palatino Linotype', 'Palatino', 'DejaVu Serif']

# ============================================================
# CONFIG
# ============================================================
GOLD_GLOB = r"data_parquet/**/**.parquet"

PHASE_FILTER = "ALL"
RELIABLE_ONLY = True
LOCAL_TIMEZONE = "Europe/Dublin"

# Representative week (UTC)
WEEK_START = "2025-12-15"
WEEK_END   = "2025-12-22"

# Use actual AssetId values from the Parquet
SELECTED_ASSETS = [
    "p_k",
    "pr_k",
    "ahu_a",
    "comp_a",
    "mi_a",
]

ASSET_SEMANTIC_LABELS = {
    "p_k": "Press",
    "pr_k": "Robot",
    "ahu_a": "AHU",
    "comp_a": "Compressor",
    "mi_a": "Grid Supply",
}

OUT_PNG = r"Fig_Representative_Week_SelectedAssets.png"
OUT_SVG = r"Fig_Representative_Week_SelectedAssets.svg"

def make_display_label(asset_id: str) -> str:
    desc = ASSET_SEMANTIC_LABELS.get(asset_id.lower().strip(), "")
    if desc:
        return f"{desc}\n({asset_id})"
    return asset_id

# ============================================================
# LOAD DATA
# ============================================================
con = duckdb.connect()

where = [
    f"Phase = '{PHASE_FILTER}'",
    f"window_start_utc >= TIMESTAMP '{WEEK_START}'",
    f"window_start_utc <  TIMESTAMP '{WEEK_END}'"
]

if RELIABLE_ONLY:
    where.append("IsReliableWindow = TRUE")

where_sql = "WHERE " + " AND ".join(where)

df = con.execute(f"""
    SELECT
        AssetId,
        window_start_utc,
        Energy_kWh_15m
    FROM read_parquet('{GOLD_GLOB}')
    {where_sql}
""").df()

if df.empty:
    raise SystemExit("No data returned for selected week.")

df["AssetId"] = df["AssetId"].astype(str).str.strip().str.lower()
df["window_start_utc"] = pd.to_datetime(df["window_start_utc"], utc=True)
df["window_start_local"] = df["window_start_utc"].dt.tz_convert(LOCAL_TIMEZONE)

selected_assets = [a.strip().lower() for a in SELECTED_ASSETS]
df = df[df["AssetId"].isin(selected_assets)]

if df.empty:
    raise SystemExit("None of the selected assets were found in the returned data.")

# ============================================================
# PLOTTING
# ============================================================
n_assets = len(selected_assets)

fig, axes = plt.subplots(
    n_assets,
    1,
    figsize=(12, 2.2 * n_assets),
    sharex=True
)

if n_assets == 1:
    axes = [axes]

for ax, asset in zip(axes, selected_assets):
    sub = df[df["AssetId"] == asset].sort_values("window_start_local")

    ax.plot(
        sub["window_start_local"],
        sub["Energy_kWh_15m"],
        linewidth=0.8
    )

    display_label = make_display_label(asset)

    ax.set_ylabel(
        display_label,
        rotation=0,
        labelpad=42,
        fontsize=12,
        va="center"
    )

    ax.grid(axis="y", linestyle="--", alpha=0.25)
    ax.tick_params(axis="y", labelsize=10)
    
fig.text(
    0.04, 0.5,
    "Energy per 15-min interval (kWh)",
    va="center",
    rotation="vertical",
    fontsize=14
)

# ------------------------------------------------------------
# Format x-axis
# ------------------------------------------------------------
axes[-1].xaxis.set_major_locator(mdates.DayLocator())
axes[-1].xaxis.set_major_formatter(mdates.DateFormatter("%a\n%d-%b"))
plt.setp(axes[-1].get_xticklabels(), rotation=0)

#axes[-1].set_xlabel("Local time")

#fig.suptitle("Representative operational week — selected assets", y=0.995)
fig.subplots_adjust(left=0.2, right=0.98, bottom=0.08, top=0.96, hspace=0.25)

fig.savefig(
    OUT_PNG,
    dpi=400,
    bbox_inches="tight",
    pad_inches=0.1
)

fig.savefig(
    OUT_SVG
)

plt.show()
print(f"Saved: {OUT_PNG}")