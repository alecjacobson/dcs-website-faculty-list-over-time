#!/usr/bin/env python3
"""
visualize.py - Generate plots from parsed and matched faculty data.

Outputs to plots/:
  faculty_count_over_time.png  - line chart of faculty count by stream
  yearly_net_change.png        - grouped bar chart of net ±change per year
  faculty_tenure.png           - histogram of months each person stayed listed
  cumulative_arrivals.png      - cumulative new hires over time by stream

Usage:
    python visualize.py
    python visualize.py --output-dir results/plots
"""

import argparse
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import pandas as pd

PARSED_DIR   = Path("data/parsed")
TIMELINE_CSV = Path("data/faculty_timeline.csv")
DEFAULT_PLOT_DIR = Path("plots")

COLORS = {
    "research": "#1f77b4",   # blue
    "teaching": "#ff7f0e",   # orange
    "unknown":  "#aaaaaa",
}


def yyyymm_to_date(s: str) -> datetime:
    return datetime.strptime(str(s)[:6], "%Y%m")


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_monthly_counts() -> pd.DataFrame:
    """Build a monthly time series of faculty counts by stream."""
    rows = []
    for jf in sorted(PARSED_DIR.glob("*.json")):
        data = json.loads(jf.read_text())
        ts = data["timestamp"]
        counts: dict[str, int] = defaultdict(int)
        for f in data["faculty"]:
            counts[f.get("stream", "unknown")] += 1
        rows.append({
            "date":     yyyymm_to_date(ts),
            "research": counts.get("research", 0),
            "teaching": counts.get("teaching", 0),
            "unknown":  counts.get("unknown",  0),
            "total":    counts.get("research", 0) + counts.get("teaching", 0),
        })
    df = pd.DataFrame(rows).sort_values("date").reset_index(drop=True)
    return df


# ---------------------------------------------------------------------------
# Plot helpers
# ---------------------------------------------------------------------------

def _apply_year_xaxis(ax: plt.Axes, df: pd.DataFrame):
    span_years = (df["date"].max() - df["date"].min()).days / 365
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    if span_years < 8:
        ax.xaxis.set_minor_locator(mdates.MonthLocator(bymonth=[4, 7, 10]))
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right")
    ax.grid(True, which="major", axis="x", alpha=0.2, linestyle="--")
    ax.grid(True, axis="y", alpha=0.3)


# ---------------------------------------------------------------------------
# Plot 1 – Faculty count over time
# ---------------------------------------------------------------------------

def plot_count_over_time(df: pd.DataFrame, out: Path):
    fig, ax = plt.subplots(figsize=(14, 6))

    ax.fill_between(df["date"], df["research"], alpha=0.15, color=COLORS["research"])
    ax.fill_between(df["date"], df["teaching"], alpha=0.15, color=COLORS["teaching"])
    ax.plot(df["date"], df["research"], label="Research Stream",
            color=COLORS["research"], linewidth=2, marker="o", markersize=3)
    ax.plot(df["date"], df["teaching"], label="Teaching Stream",
            color=COLORS["teaching"], linewidth=2, marker="o", markersize=3)
    if df["unknown"].sum() > 0:
        ax.plot(df["date"], df["unknown"], label="Unclassified",
                color=COLORS["unknown"], linewidth=1, linestyle=":", marker=".", markersize=2)

    ax.set_title("UofT CS Faculty Count Over Time", fontsize=14, fontweight="bold")
    ax.set_xlabel("Year")
    ax.set_ylabel("Faculty on Directory Page")
    ax.legend(framealpha=0.9)
    ax.yaxis.set_major_locator(ticker.MaxNLocator(integer=True))
    _apply_year_xaxis(ax, df)
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"  {out}")


# ---------------------------------------------------------------------------
# Plot 2 – Yearly net change (±)
# ---------------------------------------------------------------------------

def plot_yearly_net_change(df: pd.DataFrame, out: Path):
    df = df.copy()
    df["year"] = df["date"].dt.year

    def net_per_year(col: str) -> pd.Series:
        g = df.groupby("year")[col]
        return g.last() - g.first()

    years   = sorted(df["year"].unique())
    net_r   = net_per_year("research").reindex(years, fill_value=0)
    net_t   = net_per_year("teaching").reindex(years, fill_value=0)
    net_all = net_per_year("total").reindex(years, fill_value=0)

    x = list(range(len(years)))
    w = 0.35

    fig, axes = plt.subplots(2, 1, figsize=(14, 10), sharex=True)

    # Top: combined net change
    ax = axes[0]
    bar_colors = [("#2ecc71" if v >= 0 else "#e74c3c") for v in net_all]
    ax.bar(x, net_all, color=bar_colors, edgecolor="white", linewidth=0.5)
    ax.axhline(0, color="black", linewidth=0.8)
    ax.set_title("Net Faculty Change Per Year (All Streams)", fontsize=12, fontweight="bold")
    ax.set_ylabel("Net Change")
    ax.yaxis.set_major_locator(ticker.MaxNLocator(integer=True))
    ax.grid(True, axis="y", alpha=0.3)

    # Bottom: by stream
    ax2 = axes[1]
    ax2.bar([xi - w/2 for xi in x], net_r, width=w, label="Research",
            color=COLORS["research"], alpha=0.85, edgecolor="white")
    ax2.bar([xi + w/2 for xi in x], net_t, width=w, label="Teaching",
            color=COLORS["teaching"], alpha=0.85, edgecolor="white")
    ax2.axhline(0, color="black", linewidth=0.8)
    ax2.set_title("Net Faculty Change Per Year (By Stream)", fontsize=12, fontweight="bold")
    ax2.set_ylabel("Net Change")
    ax2.set_xticks(x)
    ax2.set_xticklabels(years, rotation=45, ha="right")
    ax2.yaxis.set_major_locator(ticker.MaxNLocator(integer=True))
    ax2.legend()
    ax2.grid(True, axis="y", alpha=0.3)

    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"  {out}")


# ---------------------------------------------------------------------------
# Plot 3 – Faculty tenure histogram (requires timeline CSV)
# ---------------------------------------------------------------------------

def plot_tenure_histogram(timeline_csv: Path, out: Path):
    df = pd.read_csv(timeline_csv)
    df = df[df["stream"].isin(["research", "teaching"])]

    max_months = int(df["months_active"].max()) + 1
    bins = list(range(0, max_months + 12, 12))

    fig, ax = plt.subplots(figsize=(12, 5))
    for stream in ("research", "teaching"):
        sub = df[df["stream"] == stream]["months_active"]
        ax.hist(sub, bins=bins, alpha=0.75, label=stream.capitalize(),
                color=COLORS[stream], edgecolor="white", linewidth=0.5)

    ax.set_title("Faculty Listing Duration", fontsize=12, fontweight="bold")
    ax.set_xlabel("Months Appearing on Faculty Directory")
    ax.set_ylabel("Number of People")
    ax.xaxis.set_major_locator(ticker.MultipleLocator(12))
    ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda v, _: f"{int(v//12)}y"))
    ax.legend()
    ax.grid(True, axis="y", alpha=0.3)
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"  {out}")


# ---------------------------------------------------------------------------
# Plot 4 – Cumulative new arrivals over time
# ---------------------------------------------------------------------------

def plot_cumulative_arrivals(timeline_csv: Path, out: Path):
    df = pd.read_csv(timeline_csv)
    df = df[df["stream"].isin(["research", "teaching"])]
    df["first_date"] = df["first_seen"].apply(lambda s: yyyymm_to_date(str(s)))

    fig, ax = plt.subplots(figsize=(14, 6))
    for stream in ("research", "teaching"):
        sub = df[df["stream"] == stream].sort_values("first_date")
        sub = sub.groupby("first_date").size().reset_index(name="new")
        sub["cumulative"] = sub["new"].cumsum()
        ax.step(sub["first_date"], sub["cumulative"], where="post",
                label=f"{stream.capitalize()} (first appearance)",
                color=COLORS[stream], linewidth=2)

    ax.set_title("Cumulative New Faculty Appearances Over Time", fontsize=12, fontweight="bold")
    ax.set_xlabel("Year")
    ax.set_ylabel("Cumulative Count")
    ax.legend()
    _apply_year_xaxis(ax, pd.DataFrame({"date": df["first_date"]}))
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"  {out}")


# ---------------------------------------------------------------------------
# Text summary
# ---------------------------------------------------------------------------

def print_summary(df: pd.DataFrame, timeline_csv: Path):
    tl = pd.read_csv(timeline_csv)
    latest = df.iloc[-1]
    earliest = df.iloc[0]

    print("\n=== Summary ===")
    print(f"Date range : {earliest['date'].strftime('%b %Y')} → {latest['date'].strftime('%b %Y')}")
    print(f"Snapshots  : {len(df)}")
    print(f"\nLatest snapshot ({latest['date'].strftime('%b %Y')}):")
    print(f"  Research stream : {latest['research']}")
    print(f"  Teaching stream : {latest['teaching']}")
    print(f"  Total           : {int(latest['total'])}")
    print(f"\nAll-time unique faculty:")
    for stream in ("research", "teaching"):
        sub = tl[tl["stream"] == stream]
        current = sub["currently_listed"].sum()
        print(f"  {stream.capitalize():10s}: {len(sub):3d} individuals  ({current} currently listed)")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Generate plots from faculty data")
    parser.add_argument("--output-dir", default=str(DEFAULT_PLOT_DIR),
                        help=f"Directory to write plots (default: {DEFAULT_PLOT_DIR})")
    args = parser.parse_args()

    plot_dir = Path(args.output_dir)
    plot_dir.mkdir(parents=True, exist_ok=True)

    if not PARSED_DIR.exists() or not any(PARSED_DIR.glob("*.json")):
        print("No parsed data found. Run parse.py first.")
        return

    print("Loading monthly faculty counts...")
    df = load_monthly_counts()
    if df.empty:
        print("No data to plot.")
        return

    print(f"Generating plots from {len(df)} monthly snapshots...\n")
    plot_count_over_time(df,  plot_dir / "faculty_count_over_time.png")
    plot_yearly_net_change(df, plot_dir / "yearly_net_change.png")

    if TIMELINE_CSV.exists():
        plot_tenure_histogram(TIMELINE_CSV,    plot_dir / "faculty_tenure.png")
        plot_cumulative_arrivals(TIMELINE_CSV, plot_dir / "cumulative_arrivals.png")
        print_summary(df, TIMELINE_CSV)
    else:
        print("\nNote: Run match.py first to enable tenure and cumulative-arrivals plots.")

    print(f"\nPlots written to {plot_dir}/")


if __name__ == "__main__":
    main()
