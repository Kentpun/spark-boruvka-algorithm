"""
Correctness test: compare Boruvka output file against Kruskal ground truth.
"""

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.graph_loader import load_graph_from_text
from src.mst_reference import kruskal_msf
from src.union_find import UnionFind


def _parse_boruvka_output(path: Path) -> tuple[int, int, int, list[tuple[int, int, int]]]:
    """
    Parse a Borůvka output file produced by mst_output.write_mst_file.

    Returns (total_weight, num_iterations, num_components, edges).
    """
    total_weight = num_iterations = num_components = None
    edges: list[tuple[int, int, int]] = []

    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("total_weight"):
            total_weight = int(line.split()[1])
        elif line.startswith("num_iterations"):
            num_iterations = int(line.split()[1])
        elif line.startswith("num_components"):
            num_components = int(line.split()[1])
        elif line.startswith("num_edges"):
            pass  # derived — we recount from the edge list
        else:
            parts = line.split()
            if len(parts) >= 3:
                u, v, w = int(parts[0]), int(parts[1]), int(parts[2])
                edges.append((u, v, w))

    missing = [
        k for k, v in {
            "total_weight": total_weight,
            "num_iterations": num_iterations,
            "num_components": num_components,
        }.items() if v is None
    ]
    if missing:
        raise ValueError(f"Borůvka output missing fields: {missing}")

    return total_weight, num_iterations, num_components, edges


def _check_no_cycles(edges: list[tuple[int, int, int]]) -> list[str]:
    """Return a list of error messages for any cycle detected in the edge set."""
    uf = UnionFind()
    errors = []
    for u, v, w in edges:
        if uf.find(u) == uf.find(v):
            errors.append(f"  cycle detected: edge ({u}, {v}, {w}) connects already-joined vertices")
        else:
            uf.union(u, v)
    return errors


def _check_edges_in_graph(
    boruvka_edges: list[tuple[int, int, int]],
    graph_edge_set: set[tuple[int, int, int]],
) -> list[str]:
    """Return errors for any Borůvka edge not present in the original graph."""
    errors = []
    for u, v, w in boruvka_edges:
        key = (min(u, v), max(u, v), w)
        if key not in graph_edge_set:
            errors.append(f"  edge ({u}, {v}, {w}) not found in input graph")
    return errors


def run_tests(boruvka_path: Path, graph_path: Path) -> bool:
    print(f"Borůvka output : {boruvka_path}")
    print(f"Input graph    : {graph_path}")
    print()

    # Load files
    boruvka_total, boruvka_iters, boruvka_ncomp, boruvka_edges = (
        _parse_boruvka_output(boruvka_path)
    )
    graph_edges = load_graph_from_text(
        graph_path.read_text(encoding="utf-8"), fmt="edge_list"
    )
    graph_edge_set = {(min(u, v), max(u, v), w) for u, v, w in graph_edges}

    # Kruskal ground truth
    ref_total, ref_mst, ref_ncomp = kruskal_msf(graph_edges)

    all_vertices = {v for u, v, _ in graph_edges} | {u for u, _, __ in graph_edges}
    expected_edge_count = len(all_vertices) - ref_ncomp

    print(f"{'':=<60}")
    print(f"  Kruskal total_weight  : {ref_total}")
    print(f"  Kruskal num_components: {ref_ncomp}")
    print(f"  Kruskal mst_edges     : {len(ref_mst)}")
    print(f"  Expected edge count   : {expected_edge_count}  (|V|={len(all_vertices)} - components={ref_ncomp})")
    print()
    print(f"  Borůvka total_weight  : {boruvka_total}")
    print(f"  Borůvka num_components: {boruvka_ncomp}")
    print(f"  Borůvka mst_edges     : {len(boruvka_edges)}")
    print(f"  Borůvka num_iterations: {boruvka_iters}")
    print(f"{'':=<60}")
    print()

    # Individual checks
    passed = True

    def check(name: str, ok: bool, detail: str = "") -> None:
        nonlocal passed
        status = "PASS" if ok else "FAIL"
        msg = f"[{status}] {name}"
        if detail:
            msg += f"\n       {detail}"
        print(msg)
        if not ok:
            passed = False

    # 1. Total weight
    check(
        "Total weight matches Kruskal",
        boruvka_total == ref_total,
        f"boruvka={boruvka_total}, kruskal={ref_total}, diff={boruvka_total - ref_total:+d}",
    )

    # 2. Edge count (detects both cycles and missing edges)
    check(
        "Edge count equals |V| - components",
        len(boruvka_edges) == expected_edge_count,
        f"boruvka={len(boruvka_edges)}, expected={expected_edge_count}, diff={len(boruvka_edges) - expected_edge_count:+d}",
    )

    # 3. Component count
    check(
        "Component count matches Kruskal",
        boruvka_ncomp == ref_ncomp,
        f"boruvka={boruvka_ncomp}, kruskal={ref_ncomp}",
    )

    # 4. All edges exist in the original graph
    invalid_edges = _check_edges_in_graph(boruvka_edges, graph_edge_set)
    check(
        "All Borůvka edges exist in input graph",
        len(invalid_edges) == 0,
        "\n       ".join(invalid_edges[:10])
        + ("  ..." if len(invalid_edges) > 10 else ""),
    )

    # 5. No cycles
    cycle_errors = _check_no_cycles(boruvka_edges)
    check(
        "Borůvka edge set is acyclic (no cycles)",
        len(cycle_errors) == 0,
        "\n       ".join(cycle_errors[:10])
        + ("  ..." if len(cycle_errors) > 10 else ""),
    )

    if passed:
        print("All checks passed — Borůvka output is a valid MSF.")
    else:
        print("One or more checks FAILED.")

    return passed

def main() -> None:
    p = argparse.ArgumentParser(
        description="Compare Borůvka MST output against Kruskal ground truth."
    )
    p.add_argument(
        "--boruvka_output",
        type=Path,
        required=True,
        help="Borůvka output .txt file (from run_mst.py)",
    )
    p.add_argument(
        "--graph",
        type=Path,
        required=True,
        help="Original input graph .txt file (edge list)",
    )
    args = p.parse_args()

    ok = run_tests(args.boruvka_output, args.graph)
    sys.exit(0 if ok else 1)

if __name__ == "__main__":
    main()