"""
visualize.py — Matplotlib Performance Visualization for GFS Benchmark.

Reads results/benchmark_results.json and generates:
  • results/upload_time.png   — Upload time vs file size
  • results/download_time.png — Download time vs file size

Usage:
    python visualize.py
    (Run benchmark.py first to generate the JSON data)
"""

import json
import os
import sys

try:
    import matplotlib
    matplotlib.use("Agg")   # Non-interactive backend — safe on all platforms
    import matplotlib.pyplot as plt
    import matplotlib.ticker as ticker
except ImportError:
    print("ERROR: matplotlib is not installed.  Run: pip install matplotlib")
    sys.exit(1)

from config import RESULTS_DIR

# ── Aesthetics ────────────────────────────────────────────────────────────────
COLOURS = {
    "upload":   "#4E8FE8",   # blue
    "download": "#E86E4E",   # orange-red
    "grid":     "#E0E0E0",
    "bg":       "#FAFAFA",
}
FONT_FAMILY = "DejaVu Sans"


def load_results(json_path: str) -> dict:
    if not os.path.exists(json_path):
        print(f"ERROR: results file not found: {json_path}")
        print("Please run benchmark.py first.")
        sys.exit(1)
    with open(json_path) as fh:
        return json.load(fh)


def plot_metric(
    labels: list,
    values: list,
    title: str,
    ylabel: str,
    colour: str,
    save_path: str,
) -> None:
    """Generate and save a single bar+line chart."""
    fig, ax = plt.subplots(figsize=(9, 5))
    fig.patch.set_facecolor(COLOURS["bg"])
    ax.set_facecolor(COLOURS["bg"])

    x = range(len(labels))

    # Bar chart
    bars = ax.bar(
        x, values,
        color=colour,
        alpha=0.75,
        width=0.5,
        zorder=3,
        edgecolor="white",
        linewidth=0.8,
    )

    # Line overlay
    ax.plot(
        x, values,
        marker="o",
        color=colour,
        linewidth=2,
        markersize=8,
        markeredgecolor="white",
        markeredgewidth=1.5,
        zorder=4,
    )

    # Value annotations on each bar
    for bar, val in zip(bars, values):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + max(values) * 0.015,
            f"{val:.3f}s",
            ha="center",
            va="bottom",
            fontsize=9,
            color="#333333",
            fontweight="bold",
        )

    # Formatting
    ax.set_xticks(list(x))
    ax.set_xticklabels(labels, fontsize=11)
    ax.set_xlabel("File Size", fontsize=12, labelpad=8)
    ax.set_ylabel(ylabel, fontsize=12, labelpad=8)
    ax.set_title(title, fontsize=14, fontweight="bold", pad=14)
    ax.yaxis.set_major_formatter(ticker.FormatStrFormatter("%.3f s"))
    ax.grid(axis="y", color=COLOURS["grid"], linestyle="--", linewidth=0.8, zorder=0)
    ax.spines[["top", "right"]].set_visible(False)

    plt.tight_layout()
    plt.savefig(save_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {save_path}")


def plot_combined(
    labels: list,
    upload_vals: list,
    download_vals: list,
    save_path: str,
) -> None:
    """Generate a combined grouped-bar chart for upload vs download."""
    import numpy as np

    fig, ax = plt.subplots(figsize=(10, 5.5))
    fig.patch.set_facecolor(COLOURS["bg"])
    ax.set_facecolor(COLOURS["bg"])

    x = np.arange(len(labels))
    width = 0.35

    b1 = ax.bar(x - width / 2, upload_vals,   width, label="Upload",   color=COLOURS["upload"],   alpha=0.82, zorder=3, edgecolor="white")
    b2 = ax.bar(x + width / 2, download_vals, width, label="Download", color=COLOURS["download"], alpha=0.82, zorder=3, edgecolor="white")

    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=11)
    ax.set_xlabel("File Size", fontsize=12, labelpad=8)
    ax.set_ylabel("Time (s)", fontsize=12, labelpad=8)
    ax.set_title(
        "GFS Simulation — Upload vs Download Performance",
        fontsize=13, fontweight="bold", pad=14,
    )
    ax.legend(fontsize=11)
    ax.yaxis.set_major_formatter(ticker.FormatStrFormatter("%.3f s"))
    ax.grid(axis="y", color=COLOURS["grid"], linestyle="--", linewidth=0.8, zorder=0)
    ax.spines[["top", "right"]].set_visible(False)

    plt.tight_layout()
    plt.savefig(save_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {save_path}")


def main() -> None:
    print("\n" + "=" * 55)
    print("  GFS Visualizer — Generating Performance Graphs")
    print("=" * 55 + "\n")

    json_path = os.path.join(RESULTS_DIR, "benchmark_results.json")
    data = load_results(json_path)

    labels        = [r["size_label"] for r in data]
    upload_times  = [r["upload_time"] for r in data]
    download_times = [r["download_time"] for r in data]

    os.makedirs(RESULTS_DIR, exist_ok=True)

    plot_metric(
        labels, upload_times,
        title="GFS Simulation — Upload Time vs File Size",
        ylabel="Upload Time (s)",
        colour=COLOURS["upload"],
        save_path=os.path.join(RESULTS_DIR, "upload_time.png"),
    )

    plot_metric(
        labels, download_times,
        title="GFS Simulation — Download Time vs File Size",
        ylabel="Download Time (s)",
        colour=COLOURS["download"],
        save_path=os.path.join(RESULTS_DIR, "download_time.png"),
    )

    plot_combined(
        labels, upload_times, download_times,
        save_path=os.path.join(RESULTS_DIR, "combined_performance.png"),
    )

    print(f"\n  All graphs saved to: {RESULTS_DIR}\n")


if __name__ == "__main__":
    main()
