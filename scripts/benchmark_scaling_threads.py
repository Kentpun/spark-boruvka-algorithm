#!/usr/bin/env python3
"""
Scaling benchmark for Borůvka MST.

Currently supports testing with different numbers of local threads
to simulate the effect of parallelism by scaling the number of cores
"""

import argparse
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import multiprocessing


def run_single(path: str, fmt: str, num_cores: int) -> tuple[float, int]:
    """Run Borůvka in a fresh SparkSession with `num_cores` local threads."""
    from pyspark.sql import SparkSession
    from src.boruvka_spark import run_boruvka_from_file

    master = f"local[{num_cores}]"
    spark = (
        SparkSession.builder.appName(f"BoruvkaMST-bench-{num_cores}")
        .master(master)
        # Suppress most Spark logs so timing output is readable
        .config("spark.ui.enabled", "false")
        .config("spark.log.level", "ERROR")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    try:
        t0 = time.perf_counter()
        result = run_boruvka_from_file(spark.sparkContext, path, fmt=fmt)
        elapsed = time.perf_counter() - t0
    finally:
        spark.stop()

    return elapsed, result.num_edges


def main() -> None:
    max_cores = multiprocessing.cpu_count()

    p = argparse.ArgumentParser(description="Borůvka MST scaling benchmark")
    p.add_argument("input", help="Path to graph file")
    p.add_argument(
        "--format",
        choices=("edge_list", "adjacency_list"),
        default=None,
    )
    p.add_argument(
        "--cores",
        nargs="+",
        type=int,
        default=None,
        help=(
            f"Core counts to test (default: powers of 2 up to {max_cores}). "
            "Example: --cores 1 2 4 8"
        ),
    )
    p.add_argument(
        "--runs",
        type=int,
        default=1,
        help="Repetitions per core count (median is used). Default: 1",
    )
    p.add_argument(
        "--plot",
        action="store_true",
        help="Show a speedup plot after benchmarking (requires matplotlib)",
    )
    p.add_argument(
        "--output",
        "-o",
        default="plots/benchmark_scaling.csv",
        help="Save results as CSV to this path",
    )
    args = p.parse_args()

    # Detect physical cores (more accurate than logical on hyperthreaded CPUs)
    try:
        import subprocess
        physical_cores = int(
            subprocess.check_output(["sysctl", "-n", "hw.physicalcpu"]).strip()
        )
    except Exception:
        physical_cores = max_cores  # fallback to logical count on non-macOS

    # Determine core counts to test
    if args.cores:
        core_counts = sorted(set(args.cores))
    else:
        core_counts = [1]

    # Warnings about core counts exceeding physical cores
    oversubscribed = [n for n in core_counts if n > physical_cores]
    if oversubscribed:
        print(
            f"""WARNING: core count(s) {oversubscribed} exceed physical cores
            {physical_cores}). Results for these may be skewed by thread
            contention and won't reflect true parallel scaling."""
        )

    # Infer format
    if args.format:
        fmt = args.format
    else:
        from src.graph_loader import detect_format_from_path
        fmt = detect_format_from_path(args.input)

    print(f"Graph : {args.input}")
    print(f"Format: {fmt}")
    print(f"Cores to test: {core_counts}")
    print(f"Runs per config: {args.runs}")
    print(f"Host CPUs: {physical_cores} physical / {max_cores} logical")

    times: list[tuple[int, float]] = []
    baseline: float | None = None

    for n in core_counts:
        run_times = []
        for r in range(args.runs):
            t, _ = run_single(args.input, fmt, n)
            run_times.append(t)
        elapsed = sorted(run_times)[len(run_times) // 2]  # median

        if baseline is None: # Set the single-core time as the baseline for speedup calculations
            baseline = elapsed
        speedup = baseline / elapsed # Speed is the ratio of single-core time to current time
        efficiency = speedup / n # Define as the ratio of speedup to the num cores

        times.append((n, elapsed))
        print(f"# Cores={n:>6}  Time={elapsed:>10.3f}s  Speedup={speedup:>8.2f}x  Efficiency={efficiency:>9.1%}")

    # Save CSV
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w") as f:
        f.write("cores,time_s,speedup,efficiency\n")
        base = times[0][1]
        for n, t in times:
            sp = base / t
            f.write(f"{n},{t:.4f},{sp:.4f},{sp/n:.4f}\n")
    print(f"\nResults saved to {out}")

    if args.plot:
        try:
            import matplotlib.pyplot as plt

            core_vals = [x[0] for x in times]
            speedups = [times[0][1] / x[1] for x in times]

            fig, axes = plt.subplots(1, 2, figsize=(10, 4))

            axes[0].plot(core_vals, speedups, "o-", label="Actual speedup")
            axes[0].plot(core_vals, core_vals, "--", color="gray", label="Ideal (linear)")
            axes[0].set_xlabel("Number of cores")
            axes[0].set_ylabel("Speedup")
            axes[0].set_title("Speedup vs. cores")
            axes[0].legend()
            axes[0].grid(True)

            wall_times = [x[1] for x in times]
            axes[1].plot(core_vals, wall_times, "s-", color="tab:orange")
            axes[1].set_xlabel("Number of cores")
            axes[1].set_ylabel("Wall-clock time (s)")
            axes[1].set_title("Wall-clock time vs. cores")
            axes[1].grid(True)

            plt.tight_layout()
            plot_path = out.with_suffix(".png")
            plt.savefig(plot_path, dpi=150)
            print(f"Plot saved to {plot_path}")
            plt.show()
        except ImportError:
            print("matplotlib not installed; skipping plot. Run: pip install matplotlib")


if __name__ == "__main__":
    main()
