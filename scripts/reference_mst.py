#!/usr/bin/env python3
"""Kruskal reference MSF for an edge-list file (compare to Spark output)."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.graph_loader import load_graph_from_text
from src.mst_reference import kruskal_msf


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("input", type=Path, help="Edge-list .txt file")
    args = p.parse_args()
    text = args.input.read_text(encoding="utf-8")
    edges = load_graph_from_text(text, fmt="edge_list")
    total, mst, n_comp = kruskal_msf(edges)
    print(f"reference_total_weight {total}")
    print(f"reference_mst_edges {len(mst)}")
    print(f"reference_components {n_comp}")
    print("edges (u v w):")
    for row in mst:
        print(f"  {row[0]} {row[1]} {row[2]}")


if __name__ == "__main__":
    main()
