#!/usr/bin/env python3
"""
Plot NimbusVault benchmark results from bench/results/summary.tsv.

Generates:
  bench/results/plots/throughput_by_mode.png
  bench/results/plots/latency_by_mode.png
  bench/results/plots/latency_cdf_<label>.png  (one per CSV listed in summary)
  bench/results/plots/bandwidth_by_mode.png

Usage:
  python3 bench/analysis/plot_results.py [--summary PATH] [--out-dir DIR]
"""

import argparse
import os
import sys
import csv as csv_mod

def try_import():
    missing = []
    try:
        import pandas as pd        # noqa: F401
    except ImportError:
        missing.append("pandas")
    try:
        import matplotlib          # noqa: F401
    except ImportError:
        missing.append("matplotlib")
    if missing:
        print(f"ERROR: missing packages: {', '.join(missing)}")
        print("Install with: pip install pandas matplotlib")
        sys.exit(1)

try_import()

import pandas as pd               # noqa: E402
import matplotlib                  # noqa: E402
matplotlib.use("Agg")              # headless rendering
import matplotlib.pyplot as plt    # noqa: E402
import matplotlib.ticker as ticker # noqa: E402

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
DEFAULT_SUMMARY = os.path.join(REPO_ROOT, "bench/results/summary.tsv")
DEFAULT_OUT_DIR = os.path.join(REPO_ROOT, "bench/results/plots")

# ── helpers ──────────────────────────────────────────────────────────────────

def savefig(fig, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fig.savefig(path, bbox_inches="tight", dpi=150)
    plt.close(fig)
    print(f"  wrote {path}")


def bar_group(ax, df, x_col, y_col, hue_col, ylabel, title):
    groups = df[hue_col].unique()
    x_vals = df[x_col].unique()
    width = 0.8 / len(groups)
    for i, grp in enumerate(groups):
        sub = df[df[hue_col] == grp].set_index(x_col)[y_col]
        x_pos = range(len(x_vals))
        heights = [sub.get(xv, 0) for xv in x_vals]
        ax.bar([p + i * width for p in x_pos], heights, width=width, label=grp)
    ax.set_xticks([p + width * (len(groups) - 1) / 2 for p in range(len(x_vals))])
    ax.set_xticklabels(x_vals, rotation=30, ha="right")
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.legend()


# ── plot functions ────────────────────────────────────────────────────────────

def plot_throughput(df, out_dir):
    fig, ax = plt.subplots(figsize=(10, 5))
    label = df["mode"] + "/" + df["distribution"]
    df = df.copy()
    df["label"] = label
    bar_group(ax, df, x_col="chunk_kb", y_col="throughput_ops_s",
              hue_col="label", ylabel="Throughput (ops/s)",
              title="Throughput by chunk size, mode, and distribution")
    savefig(fig, os.path.join(out_dir, "throughput_by_mode.png"))


def plot_latency(df, out_dir):
    fig, axes = plt.subplots(1, 3, figsize=(15, 5), sharey=False)
    label = df["mode"] + "/" + df["distribution"]
    df = df.copy()
    df["label"] = label
    for ax, col, title in zip(axes,
                               ["p50_us", "p95_us", "p99_us"],
                               ["p50 latency", "p95 latency", "p99 latency"]):
        bar_group(ax, df, x_col="chunk_kb", y_col=col,
                  hue_col="label", ylabel="Latency (µs)", title=title)
    fig.tight_layout()
    savefig(fig, os.path.join(out_dir, "latency_by_mode.png"))


def plot_bandwidth(df, out_dir):
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    label = df["mode"] + "/" + df["distribution"]
    df = df.copy()
    df["label"] = label
    bar_group(axes[0], df, x_col="chunk_kb", y_col="put_mb_s",
              hue_col="label", ylabel="PUT MB/s", title="PUT bandwidth")
    bar_group(axes[1], df, x_col="chunk_kb", y_col="get_mb_s",
              hue_col="label", ylabel="GET MB/s", title="GET bandwidth")
    fig.tight_layout()
    savefig(fig, os.path.join(out_dir, "bandwidth_by_mode.png"))


def plot_latency_cdf(csv_path, label, out_dir):
    """Plot CDF of per-op latency from a raw CSV file."""
    latencies = []
    try:
        with open(csv_path, newline="") as f:
            reader = csv_mod.DictReader(f)
            for row in reader:
                if row.get("op_type") == "TIER_CHANGE":
                    continue
                try:
                    latencies.append(int(row["latency_us"]))
                except (KeyError, ValueError):
                    pass
    except FileNotFoundError:
        print(f"  WARNING: {csv_path} not found, skipping CDF")
        return

    if not latencies:
        return

    latencies.sort()
    n = len(latencies)
    y = [(i + 1) / n for i in range(n)]

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.plot(latencies, y, linewidth=1.2)
    ax.set_xlabel("Latency (µs)")
    ax.set_ylabel("CDF")
    ax.set_title(f"Latency CDF — {label}")
    ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
    ax.grid(True, alpha=0.3)
    fname = f"latency_cdf_{label}.png"
    savefig(fig, os.path.join(out_dir, fname))


def plot_rf_vs_throughput(df, out_dir):
    """Throughput vs desired_rf for adaptive mode runs."""
    adaptive = df[df["mode"] == "adaptive"].copy()
    if adaptive.empty:
        return
    fig, ax = plt.subplots(figsize=(8, 5))
    for dist, grp in adaptive.groupby("distribution"):
        grp_sorted = grp.sort_values("desired_rf")
        ax.plot(grp_sorted["desired_rf"], grp_sorted["throughput_ops_s"],
                marker="o", label=dist)
    ax.set_xlabel("Desired RF")
    ax.set_ylabel("Throughput (ops/s)")
    ax.set_title("Throughput vs Replication Factor (adaptive)")
    ax.legend()
    ax.grid(True, alpha=0.3)
    savefig(fig, os.path.join(out_dir, "rf_vs_throughput.png"))


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="Plot NimbusVault benchmark results")
    ap.add_argument("--summary", default=DEFAULT_SUMMARY,
                    help="Path to summary.tsv (default: bench/results/summary.tsv)")
    ap.add_argument("--out-dir", default=DEFAULT_OUT_DIR,
                    help="Output directory for PNGs (default: bench/results/plots)")
    args = ap.parse_args()

    if not os.path.exists(args.summary):
        print(f"ERROR: summary file not found: {args.summary}")
        print("Run bench/scripts/run_all.sh first.")
        sys.exit(1)

    df = pd.read_csv(args.summary, sep="\t")

    # Coerce numeric columns.
    for col in ["chunk_kb", "desired_rf", "ops", "errors",
                "throughput_ops_s", "p50_us", "p95_us", "p99_us",
                "put_mb_s", "get_mb_s"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df["chunk_kb"] = df["chunk_kb"].astype(int).astype(str) + " KB"

    os.makedirs(args.out_dir, exist_ok=True)
    print(f"Generating plots → {args.out_dir}/")

    plot_throughput(df, args.out_dir)
    plot_latency(df, args.out_dir)
    plot_bandwidth(df, args.out_dir)
    plot_rf_vs_throughput(df, args.out_dir)

    # Per-run CDF plots.
    if "csv" in df.columns:
        for _, row in df.iterrows():
            lbl = f"{row['mode']}_{row['distribution']}_{row['chunk_kb'].replace(' ','')}_rf{int(row['desired_rf'])}"
            plot_latency_cdf(str(row["csv"]), lbl, args.out_dir)

    print("Done.")


if __name__ == "__main__":
    main()
