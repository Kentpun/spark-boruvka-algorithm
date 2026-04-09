"""
Distributed Borůvka minimum spanning forest using PySpark RDDs.

Each iteration:
- Map: attach current component labels to endpoints of every edge.
- Reduce: per component, take the minimum-weight outgoing edge (tie-break by endpoints).
- Driver: union selected component pairs and relabel all vertices.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional

from pyspark import RDD, SparkContext

from src.graph_loader import parse_adjacency_list_lines, parse_edge_list_lines
from src.union_find import UnionFind


def _norm_edge(u: int, v: int, w: int) -> tuple[int, int, int]:
    a, b = (u, v) if u <= v else (v, u)
    return (a, b, w)


def _min_edge_choice(
    a: tuple[int, int, int, int], b: tuple[int, int, int, int]
) -> tuple[int, int, int, int]:
    return a if a <= b else b


def _parse_edge_partition(
    fmt: str, lines: Iterable[str]
) -> Iterable[tuple[int, int, int]]:
    parsed = (
        parse_edge_list_lines(lines)
        if fmt == "edge_list"
        else parse_adjacency_list_lines(lines)
    )
    for t in parsed:
        yield t


@dataclass(frozen=True)
class BoruvkaResult:
    mst_edges: list[tuple[int, int, int]]
    total_weight: int
    num_iterations: int
    num_components: int


def boruvka_mst(
    sc: SparkContext,
    edges_rdd: RDD[tuple[int, int, int]],
    max_iterations: Optional[int] = None,
) -> BoruvkaResult:
    """
    Compute a minimum spanning forest of an undirected weighted graph.

    `edges_rdd` must list each undirected edge at least once as (u, v, weight).
    Self-loops are ignored. Parallel edges are allowed.
    """
    edges = (
        edges_rdd.map(lambda e: _norm_edge(int(e[0]), int(e[1]), int(e[2])))
        .filter(lambda e: e[0] != e[1])
        .distinct()
        .cache()
    )

    vertices = edges.flatMap(lambda e: (e[0], e[1])).distinct().cache()
    n_vertices = vertices.count()
    if n_vertices == 0:
        return BoruvkaResult([], 0, 0, 0)

    components: RDD[tuple[int, int]] = vertices.map(lambda v: (v, v)).cache()

    mst_acc: list[tuple[int, int, int]] = []
    iters = 0
    num_comp = 0
    cap = max_iterations if max_iterations is not None else n_vertices

    try:
        while iters < cap:
            comp_by_v = components

            # (u, ((v, w), comp_u))
            e_u = edges.map(lambda e: (e[0], (e[1], e[2]))).join(comp_by_v)
            # (v, (u, w, comp_u))
            keyed_by_v = e_u.map(
                lambda x: (x[1][0][0], (x[0], x[1][0][1], x[1][1]))
            )
            # (u, v, w, comp_u, comp_v)
            joined = keyed_by_v.join(comp_by_v).map(
                lambda x: (x[1][0][0], x[0], x[1][0][1], x[1][0][2], x[1][1])
            )

            cross = joined.filter(lambda t: t[3] != t[4])
            if cross.isEmpty():
                break

            candidates = cross.flatMap(
                lambda t: (
                    (t[3], (t[2], t[0], t[1], t[4])),
                    (t[4], (t[2], t[1], t[0], t[3])),
                )
            )
            best_per_comp = candidates.reduceByKey(_min_edge_choice)

            chosen = best_per_comp.collect()
            if not chosen:
                break

            merge_pairs: list[tuple[int, int]] = []
            mst_round: dict[tuple[int, int, int], tuple[int, int, int]] = {}

            for _comp_key, (w, u, v, other_comp) in chosen:
                merge_pairs.append((_comp_key, other_comp))
                mst_round[_norm_edge(u, v, w)] = (u, v, w)

            for e in mst_round.values():
                mst_acc.append(_norm_edge(e[0], e[1], e[2]))

            all_comp_ids = set(components.map(lambda x: x[1]).distinct().collect())
            for a, b in merge_pairs:
                all_comp_ids.add(a)
                all_comp_ids.add(b)

            uf = UnionFind()
            for a, b in merge_pairs:
                uf.union(a, b)
            comp_map = uf.component_map(all_comp_ids)

            components.unpersist()
            # Default-arg capture ships `comp_map` in the closure (no Broadcast
            # lifecycle issues on stage resubmission). For huge label maps, switch
            # to sc.broadcast(comp_map) and do not destroy until the job ends.
            components = comp_by_v.map(
                lambda x, m=comp_map: (x[0], m.get(x[1], x[1]))
            ).cache()
            components.count()
            iters += 1

        final_comps = components.values().distinct().collect()
        num_comp = len(final_comps)
    finally:
        edges.unpersist()
        vertices.unpersist()
        components.unpersist()

    # Dedupe MST edges (same edge might be added in theory in buggy cases; safe dedupe)
    seen: set[tuple[int, int, int]] = set()
    unique_mst: list[tuple[int, int, int]] = []
    tw = 0
    for u, v, w in mst_acc:
        k = _norm_edge(u, v, w)
        if k not in seen:
            seen.add(k)
            unique_mst.append(k)
            tw += k[2]

    return BoruvkaResult(
        mst_edges=sorted(unique_mst),
        total_weight=tw,
        num_iterations=iters,
        num_components=num_comp,
    )


def edges_rdd_from_text_file(
    sc: SparkContext, path: str, fmt: str = "edge_list"
) -> RDD[tuple[int, int, int]]:
    lines = sc.textFile(path)

    def part_parse(it):
        return _parse_edge_partition(fmt, it)

    return lines.mapPartitions(part_parse)


def run_boruvka_from_file(
    sc: SparkContext,
    path: str,
    fmt: str = "edge_list",
    max_iterations: Optional[int] = None,
) -> BoruvkaResult:
    edges = edges_rdd_from_text_file(sc, path, fmt=fmt)
    return boruvka_mst(sc, edges, max_iterations=max_iterations)
