#!/usr/bin/env python3
"""
Generate publication-quality benchmark comparison charts for LSM-KV Store vs. LevelDB.
Reads Google Benchmark JSON output and produces PNG figures in docs/benchmarks/.
"""

import json
import os
import sys
from pathlib import Path

# Use non-interactive Agg backend before importing pyplot
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np


def load_benchmark_data(json_path: Path):
    if not json_path.exists():
        raise FileNotFoundError(f"Benchmark results file not found: {json_path}")
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    benchmarks = {}
    for entry in data.get("benchmarks", []):
        raw_name = entry.get("run_name") or entry.get("name", "")
        benchmarks[raw_name] = entry
    return benchmarks


def find_entry(benchmarks, pattern):
    for name, entry in benchmarks.items():
        if pattern in name:
            return entry
    return None


def get_counter_or_field(entry, key, default=0.0):
    if not entry:
        return default
    if key in entry:
        return float(entry[key])
    counters = entry.get("counters", {})
    if key in counters:
        return float(counters[key])
    return default


def plot_throughput(benchmarks, out_dir: Path):
    """Plot 1: Large-Value Write Throughput (LSM-KV with vLog vs LevelDB)."""
    lsmkv_entry = find_entry(benchmarks, "BM_LargeValueWrite_LSMKV")
    leveldb_entry = find_entry(benchmarks, "BM_LargeValueWrite_LevelDB")

    if not lsmkv_entry or not leveldb_entry:
        print("[Warning] Missing large value benchmark data, skipping throughput plot.")
        return

    # Calculate throughput in MB/s
    lsmkv_mb_s = get_counter_or_field(lsmkv_entry, "bytes_per_second") / (1024 * 1024)
    leveldb_mb_s = get_counter_or_field(leveldb_entry, "bytes_per_second") / (1024 * 1024)

    # Calculate throughput in items/s
    lsmkv_ops_s = get_counter_or_field(lsmkv_entry, "items_per_second")
    leveldb_ops_s = get_counter_or_field(leveldb_entry, "items_per_second")

    fig, ax1 = plt.subplots(figsize=(8, 5.5), dpi=300)

    labels = ["LevelDB\n(Standard LSM)", "LSM-KV\n(WiscKey vLog)"]
    mb_values = [leveldb_mb_s, lsmkv_mb_s]
    colors = ["#4A90E2", "#2ECC71"]

    bars = ax1.bar(labels, mb_values, color=colors, width=0.45, edgecolor="#1B3B6F", linewidth=1.5, zorder=3)
    ax1.set_ylabel("Write Throughput (MB/s)", fontsize=12, fontweight="bold", color="#1B3B6F")
    ax1.set_title("Large-Value (4 KB) Write Throughput Comparison", fontsize=14, fontweight="bold", pad=15)
    ax1.grid(axis="y", linestyle="--", alpha=0.5, zorder=0)

    # Annotate values
    speedup = lsmkv_mb_s / max(leveldb_mb_s, 1e-6)
    for bar, mb, ops in zip(bars, mb_values, [leveldb_ops_s, lsmkv_ops_s]):
        height = bar.get_height()
        ax1.annotate(f"{mb:.1f} MB/s\n({int(ops):,} ops/s)",
                     xy=(bar.get_x() + bar.get_width() / 2, height),
                     xytext=(0, 6),
                     textcoords="offset points",
                     ha="center", va="bottom",
                     fontsize=11, fontweight="bold")

    ax1.set_ylim(0, max(mb_values) * 1.25)

    # Note highlighting durability trade-off
    if lsmkv_mb_s > leveldb_mb_s:
        ax1.text(0.5, 0.85, f"vLog Advantage: {speedup:.1f}x Faster Throughput",
                 transform=ax1.transAxes, ha="center", fontsize=11, fontweight="bold",
                 bbox=dict(boxstyle="round,pad=0.5", facecolor="#EAFAF1", edgecolor="#27AE60", linewidth=1.5))
    else:
        ax1.text(0.5, 0.85, "LSM-KV: Synchronous fsync() durability on every write\nLevelDB: In-memory async page cache (sync=false)",
                 transform=ax1.transAxes, ha="center", fontsize=9, fontweight="bold",
                 bbox=dict(boxstyle="round,pad=0.5", facecolor="#FEF9E7", edgecolor="#F39C12", linewidth=1.5))

    plt.tight_layout()
    out_path = out_dir / "throughput_comparison.png"
    plt.savefig(out_path)
    plt.close()
    print(f"[Generated] {out_path}")


def plot_burst_latency(benchmarks, out_dir: Path):
    """Plot 2: Tail Latency Under Compaction Bursts (SmartScheduler vs Standard Compaction)."""
    lsmkv_entry = find_entry(benchmarks, "BM_BurstP99Latency_LSMKV")
    leveldb_entry = find_entry(benchmarks, "BM_BurstP99Latency_LevelDB")

    if not lsmkv_entry or not leveldb_entry:
        print("[Warning] Missing burst latency benchmark data, skipping latency plot.")
        return

    metrics = ["p50_us", "p95_us", "p99_us"]
    metric_labels = ["p50 (Median)", "p95", "p99 (Tail)"]

    leveldb_vals = [get_counter_or_field(leveldb_entry, m) for m in metrics]
    lsmkv_vals = [get_counter_or_field(lsmkv_entry, m) for m in metrics]

    x = np.arange(len(metric_labels))
    width = 0.35

    fig, ax = plt.subplots(figsize=(9, 5.5), dpi=300)

    rects1 = ax.bar(x - width/2, leveldb_vals, width, label="LevelDB (Standard)",
                    color="#E74C3C", edgecolor="#922B21", linewidth=1.2, zorder=3)
    rects2 = ax.bar(x + width/2, lsmkv_vals, width, label="LSM-KV (SmartScheduler)",
                    color="#27AE60", edgecolor="#196F3D", linewidth=1.2, zorder=3)

    ax.set_ylabel("Latency (microseconds - µs)", fontsize=12, fontweight="bold")
    ax.set_title("Tail Latency Under Heavy Background Compaction Bursts", fontsize=14, fontweight="bold", pad=15)
    ax.set_xticks(x)
    ax.set_xticklabels(metric_labels, fontsize=11, fontweight="bold")
    ax.legend(frameon=True, facecolor="#F8F9F9", edgecolor="#BDC3C7", fontsize=11)
    ax.grid(axis="y", linestyle="--", alpha=0.5, zorder=0)

    # Value annotations on bars
    for rects in (rects1, rects2):
        for bar in rects:
            height = bar.get_height()
            ax.annotate(f"{height:.0f} µs",
                        xy=(bar.get_x() + bar.get_width() / 2, height),
                        xytext=(0, 4),
                        textcoords="offset points",
                        ha="center", va="bottom",
                        fontsize=10, fontweight="bold")

    all_vals = leveldb_vals + lsmkv_vals
    ax.set_ylim(0, max(all_vals) * 1.25)

    plt.tight_layout()
    out_path = out_dir / "burst_p99_latency.png"
    plt.savefig(out_path)
    plt.close()
    print(f"[Generated] {out_path}")


def plot_time_travel_overhead(benchmarks, out_dir: Path):
    """Plot 3: Point Read & Time-Travel Overhead (LevelDB Get vs LSM-KV Get vs GetAsOf)."""
    # Find entries matching PointGet / GetAsOf
    ldb_get = None
    lsm_get = None
    lsm_getasof = None

    for k, v in benchmarks.items():
        if "BM_PointGet_LevelDB" in k:
            ldb_get = v
        elif "BM_PointGet_LSMKV" in k:
            lsm_get = v
        elif "BM_GetAsOf_LSMKV" in k:
            lsm_getasof = v

    if not ldb_get or not lsm_get or not lsm_getasof:
        print("[Warning] Missing point get / time-travel benchmark data, skipping overhead plot.")
        return

    # Real time per operation in microseconds
    def get_lat_us(entry):
        time_unit = entry.get("time_unit", "us")
        t = float(entry.get("real_time", 0.0))
        if time_unit == "ns":
            return t / 1000.0
        elif time_unit == "ms":
            return t * 1000.0
        elif time_unit == "s":
            return t * 1000000.0
        return t

    ldb_lat = get_lat_us(ldb_get)
    lsm_lat = get_lat_us(lsm_get)
    asof_lat = get_lat_us(lsm_getasof)

    labels = ["LevelDB\nPoint Get", "LSM-KV\nLatest Get", "LSM-KV\nGetAsOf (Historical)"]
    latencies = [ldb_lat, lsm_lat, asof_lat]
    colors = ["#3498DB", "#2ECC71", "#9B59B6"]

    fig, ax = plt.subplots(figsize=(8.5, 5.5), dpi=300)

    bars = ax.bar(labels, latencies, color=colors, width=0.45, edgecolor="#2C3E50", linewidth=1.5, zorder=3)
    ax.set_ylabel("Per-Operation Latency (µs)", fontsize=12, fontweight="bold")
    ax.set_title("Read Path & Time-Travel Snapshot (AS OF) Overhead", fontsize=14, fontweight="bold", pad=15)
    ax.grid(axis="y", linestyle="--", alpha=0.5, zorder=0)

    for bar, val in zip(bars, latencies):
        ax.annotate(f"{val:.2f} µs",
                    xy=(bar.get_x() + bar.get_width() / 2, val),
                    xytext=(0, 6),
                    textcoords="offset points",
                    ha="center", va="bottom",
                    fontsize=11, fontweight="bold")

    ax.set_ylim(0, max(latencies) * 1.3)

    overhead_ratio = asof_lat / max(lsm_lat, 1e-6)
    ax.text(0.5, 0.86, f"Time-Travel (GetAsOf) Overhead vs Latest Get: ~{overhead_ratio:.1f}x",
            transform=ax.transAxes, ha="center", fontsize=11, fontweight="bold",
            bbox=dict(boxstyle="round,pad=0.5", facecolor="#F4ECF7", edgecolor="#8E44AD", linewidth=1.5))

    plt.tight_layout()
    out_path = out_dir / "time_travel_overhead.png"
    plt.savefig(out_path)
    plt.close()
    print(f"[Generated] {out_path}")


def main():
    repo_root = Path(__file__).resolve().parent.parent
    bench_json = repo_root / "docs" / "benchmarks" / "leveldb_results.json"
    out_dir = repo_root / "docs" / "benchmarks"
    out_dir.mkdir(parents=True, exist_ok=True)

    if len(sys.argv) > 1:
        bench_json = Path(sys.argv[1])

    print(f"[Plotting] Reading benchmark data from: {bench_json}")
    benchmarks = load_benchmark_data(bench_json)

    plot_throughput(benchmarks, out_dir)
    plot_burst_latency(benchmarks, out_dir)
    plot_time_travel_overhead(benchmarks, out_dir)
    print(f"[Success] All benchmark charts generated in: {out_dir}")


if __name__ == "__main__":
    main()
