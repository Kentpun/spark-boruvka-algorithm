"""
Distributed component merge utilities (RDD-based).

This module implements a practical "emit links, then reconcile globally" pattern:
1) Represent proposed component merges as undirected links (a, b).
2) Build a component-link graph in Spark.
3) Iteratively propagate minimum labels until convergence.
4) Use the converged labels to relabel vertex->component assignments.

It avoids collecting all merge pairs or all component ids to the driver.
"""

from __future__ import annotations

from typing import Optional

from pyspark import RDD


def _norm_pair(a: int, b: int) -> tuple[int, int]:
    return (a, b) if a <= b else (b, a)


def _propagate_min_labels(
    comp_ids: RDD[int],
    merge_links: RDD[tuple[int, int]],
    *,
    max_iterations: Optional[int] = None,
) -> RDD[tuple[int, int]]:
    """
    Compute connected-component roots for component ids via iterative min-label propagation.

    Returns RDD[(component_id, root_label)].
    """
    # Undirected adjacency list as (src, dst)
    adjacency = (
        merge_links.flatMap(lambda ab: ((ab[0], ab[1]), (ab[1], ab[0])))
        .distinct()
        .cache()
    )

    labels = comp_ids.map(lambda cid: (cid, cid)).cache()
    labels.count()

    cap = max_iterations if max_iterations is not None else 100
    for _ in range(cap):
        old_labels = labels

        # For each node, gather neighbor labels and keep the minimum.
        # (src, dst) join (src, label_src) -> (dst, label_src)
        neighbor_labels = (
            adjacency.join(old_labels)
            .map(lambda x: (x[1][0], x[1][1]))
            .reduceByKey(min)
        )

        labels = (
            old_labels.leftOuterJoin(neighbor_labels)
            .mapValues(lambda t: min(t[0], t[1]) if t[1] is not None else t[0])
            .cache()
        )
        labels.count()  # materialize this iteration

        changed = labels.join(old_labels).filter(lambda x: x[1][0] != x[1][1]).isEmpty()
        old_labels.unpersist()
        if changed:
            break

    adjacency.unpersist()
    return labels


def relabel_components_distributed(
    components: RDD[tuple[int, int]],
    merge_pairs: RDD[tuple[int, int]],
    *,
    max_iterations: Optional[int] = None,
) -> RDD[tuple[int, int]]:
    """
    Relabel (vertex, component) assignments using distributed merge links.

    Inputs:
      - components: RDD[(vertex_id, component_id)]
      - merge_pairs: RDD[(component_a, component_b)] from the current Boruvka round

    Output:
      - RDD[(vertex_id, new_component_root)]

    Notes:
      - If merge_pairs is empty, this returns components unchanged.
      - This is a practical distributed alternative to driver-side union-find.
    """
    links = (
        merge_pairs.filter(lambda ab: ab[0] != ab[1])
        .map(lambda ab: _norm_pair(int(ab[0]), int(ab[1])))
        .distinct()
        .cache()
    )

    if links.isEmpty():
        links.unpersist()
        return components

    # Include ids from current components and any merge endpoints.
    current_comp_ids = components.map(lambda vc: int(vc[1])).distinct()
    link_comp_ids = links.flatMap(lambda ab: (ab[0], ab[1])).distinct()
    all_comp_ids = current_comp_ids.union(link_comp_ids).distinct().cache()
    all_comp_ids.count()

    comp_to_root = _propagate_min_labels(
        all_comp_ids,
        links,
        max_iterations=max_iterations,
    ).cache()
    comp_to_root.count()

    # (vertex, comp) -> (comp, vertex) join (comp, root) -> (vertex, root)
    relabeled = (
        components.map(lambda vc: (int(vc[1]), int(vc[0])))
        .join(comp_to_root)
        .map(lambda x: (x[1][0], x[1][1]))
    )

    links.unpersist()
    all_comp_ids.unpersist()
    comp_to_root.unpersist()
    return relabeled

