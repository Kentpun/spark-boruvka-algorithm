#!/usr/bin/env python3
"""CLI: run Borůvka MST on a graph file with PySpark."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = os.path.dirname(os.path.abspath(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from pyspark.sql import SparkSession

from src.boruvka_spark import run_boruvka_from_file
from src.graph_loader import detect_format_from_path
from src.mst_output import write_mst_file


def main() -> None:
    p = argparse.ArgumentParser(description="Borůvka MST (PySpark)")
    p.add_argument("input", help="Path to graph text file (local or HDFS/S3 URI)")
    p.add_argument(
        "--format",
        choices=("edge_list", "adjacency_list"),
        default=None,
        help="Input format (default: infer from filename)",
    )
    p.add_argument(
        "--master",
        default=os.environ.get("SPARK_MASTER", "local[*]"),
        help="Spark master URL (default: local[*] or SPARK_MASTER env)",
    )
    p.add_argument(
        "--max-iterations",
        type=int,
        default=None,
        help="Safety cap on Borůvka rounds (default: |V|)",
    )
    p.add_argument(
        "--output",
        "-o",
        default=None,
        help="MST output file (default: output/mst_<input_basename>.txt under project root)",
    )
    p.add_argument(
        "--output-dir",
        default=None,
        help="Directory for default output filename (default: <project>/output)",
    )
    args = p.parse_args()

    fmt = args.format or detect_format_from_path(args.input)

    spark = (
        SparkSession.builder.appName("BoruvkaMST")
        .master(args.master)
        .getOrCreate()
    )
    try:
        res = run_boruvka_from_file(
            spark.sparkContext,
            args.input,
            fmt=fmt,
            max_iterations=args.max_iterations,
        )
    finally:
        spark.stop()

    out_dir = Path(args.output_dir) if args.output_dir else Path(ROOT) / "output"
    if args.output:
        out_path = Path(args.output)
        if not out_path.is_absolute():
            out_path = Path(ROOT) / out_path
    else:
        stem = Path(args.input).stem
        out_path = out_dir / f"mst_{stem}.txt"

    written = write_mst_file(res, out_path, source=args.input)
    print(f"Wrote MST/MSF to {written}")

    print("MST / MSF edges (u, v, weight), u < v:")
    for u, v, w in res.mst_edges:
        print(f"  {u} {v} {w}")
    print(f"total_weight={res.total_weight}")
    print(f"num_edges={res.num_edges}")
    print(f"num_iterations={res.num_iterations}")
    print(f"num_components={res.num_components}")


if __name__ == "__main__":
    main()
