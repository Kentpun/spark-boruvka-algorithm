"""
Load undirected weighted graphs from text files.

Supported formats:
- edge_list: one edge per line "u v weight" (whitespace-separated)
- adjacency_list: "u v1 w1 v2 w2 ..." or "u: v1 w1 v2 w2 ..."
"""

from __future__ import annotations

from typing import Iterator, Tuple

Edge = Tuple[int, int, int]


def _parse_ints(parts: list[str]) -> list[int]:
    return [int(x) for x in parts]


def parse_edge_list_lines(lines: Iterator[str]) -> list[Edge]:
    edges: list[Edge] = []
    for raw in lines:
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if len(parts) < 3:
            raise ValueError(f"Edge list line needs u v weight, got: {raw!r}")
        u, v, w = _parse_ints(parts[:3])
        edges.append((u, v, w))
    return edges


def parse_adjacency_list_lines(lines: Iterator[str]) -> list[Edge]:
    edges: list[Edge] = []
    seen: set[tuple[int, int, int]] = set()

    for raw in lines:
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if ":" in line:
            left, right = line.split(":", 1)
            u = int(left.strip())
            rest = right.split()
        else:
            parts = line.split()
            if len(parts) < 1:
                continue
            u = int(parts[0])
            rest = parts[1:]

        if len(rest) % 2 != 0:
            raise ValueError(
                f"Adjacency list must be pairs (neighbor weight); line: {raw!r}"
            )
        for i in range(0, len(rest), 2):
            v = int(rest[i])
            w = int(rest[i + 1])
            a, b = (u, v) if u <= v else (v, u)
            key = (a, b, w)
            if key in seen:
                continue
            seen.add(key)
            edges.append((u, v, w))

    return edges


def load_graph_from_text(
    text: str, fmt: str = "edge_list"
) -> list[Edge]:
    lines = text.splitlines()
    if fmt == "edge_list":
        return parse_edge_list_lines(iter(lines))
    if fmt == "adjacency_list":
        return parse_adjacency_list_lines(iter(lines))
    raise ValueError(f"Unknown format {fmt!r}; use 'edge_list' or 'adjacency_list'")


def detect_format_from_path(path: str) -> str:
    lower = path.lower()
    if "adjacency" in lower or lower.endswith(".adj.txt"):
        return "adjacency_list"
    return "edge_list"
