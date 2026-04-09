#!/usr/bin/env python3
"""
Generate a larger random connected graph and optional .meta with Kruskal reference totals.

Usage (from project root):
  python scripts/generate_complex_graph.py --out data/complex_random.txt
  python run_mst.py data/complex_random.txt
  python scripts/reference_mst.py data/complex_random.txt
"""

from __future__ import annotations

import argparse
import random
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.mst_reference import kruskal_msf


def generate_connected(
    n: int, extra_edges: int, seed: int, w_max: int
) -> list[tuple[int, int, int]]:
    if n < 2:
        raise ValueError("n must be >= 2")
    rng = random.Random(seed)
    # (min,max) -> weight
    seen: dict[tuple[int, int], int] = {}

    def add_edge(u: int, v: int, w: int) -> None:
        if u == v:
            return
        a, b = (u, v) if u <= v else (v, u)
        if (a, b) not in seen:
            seen[(a, b)] = w

    # Random spanning tree (shuffle vertices, connect each new vertex to an earlier one)
    perm = list(range(n))
    rng.shuffle(perm)
    for i in range(1, n):
        u = perm[i]
        v = rng.choice(perm[:i])
        add_edge(u, v, rng.randint(1, w_max))

    target = (n - 1) + extra_edges
    guard = 0
    while len(seen) < target and guard < extra_edges * 50 + n * n:
        guard += 1
        u = rng.randrange(n)
        v = rng.randrange(n)
        if u == v:
            continue
        a, b = (u, v) if u <= v else (v, u)
        if (a, b) in seen:
            continue
        add_edge(u, v, rng.randint(1, w_max))

    out = [(a, b, w) for (a, b), w in seen.items()]
    rng.shuffle(out)
    return out


def main() -> None:
    p = argparse.ArgumentParser(description="Generate complex weighted graph for Borůvka tests")
    p.add_argument("--out", type=Path, default=ROOT / "data" / "complex_random.txt")
    p.add_argument("--vertices", type=int, default=120)
    p.add_argument("--extra-edges", type=int, default=500, help="Beyond a spanning tree")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--w-max", type=int, default=50_000)
    args = p.parse_args()

    edges = generate_connected(args.vertices, args.extra_edges, args.seed, args.w_max)
    total, mst, n_comp = kruskal_msf([(u, v, w) for u, v, w in edges])

    args.out.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        f"# Generated: n={args.vertices}, |E|={len(edges)}, seed={args.seed}",
        f"# Reference (Kruskal): total_weight={total} mst_edges={len(mst)} components={n_comp}",
    ]
    lines.extend(f"{u} {v} {w}" for u, v, w in edges)
    args.out.write_text("\n".join(lines) + "\n", encoding="utf-8")

    meta = args.out.with_name(args.out.stem + ".meta")
    meta.write_text(
        "\n".join(
            [
                f"vertices {args.vertices}",
                f"edges {len(edges)}",
                f"reference_total_weight {total}",
                f"reference_mst_edges {len(mst)}",
                f"reference_components {n_comp}",
                f"seed {args.seed}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    print(f"Wrote {args.out} ({len(edges)} edges)")
    print(f"Wrote {meta}")
    print(f"Kruskal reference: weight={total} |MST|={len(mst)} components={n_comp}")


if __name__ == "__main__":
    main()
