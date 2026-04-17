"""Reference MSF/MST via Kruskal (driver-only, for validating Spark Borůvka output)."""

from __future__ import annotations

from src.graph_loader import Weight
from src.union_find import UnionFind


def kruskal_msf(
    edges: list[tuple[int, int, Weight]],
) -> tuple[Weight, list[tuple[int, int, Weight]], int]:
    """
    Undirected multigraph allowed. Edges are (u, v, w); Kruskal picks lightest bridges first.

    Returns (total_weight, mst_edges_sorted, num_connected_components).
    """
    vertices: set[int] = set()
    for u, v, _ in edges:
        vertices.add(u)
        vertices.add(v)

    sorted_e = sorted(
        edges,
        key=lambda e: (e[2], min(e[0], e[1]), max(e[0], e[1])),
    )

    uf = UnionFind()
    picked: list[tuple[int, int, Weight]] = []
    for u, v, w in sorted_e:
        if uf.find(u) != uf.find(v):
            uf.union(u, v)
            a, b = (u, v) if u <= v else (v, u)
            picked.append((a, b, w))

    roots = {uf.find(v) for v in vertices}
    n_comp = len(roots)
    total = sum(w for _, _, w in picked)
    return total, sorted(picked), n_comp
