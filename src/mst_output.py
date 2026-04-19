"""Write Borůvka MST/MSF results to a text file."""

from __future__ import annotations

from pathlib import Path

from src.boruvka_spark import BoruvkaResult


def write_mst_file(
    result: BoruvkaResult,
    out_path: str | Path,
    *,
    source: str | None = None,
) -> Path:
    """
    Write MST/MSF edges and summary metadata to `out_path`.

    Creates parent directories if needed. Returns the resolved path.
    """
    path = Path(out_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    lines = [
        "# Borůvka minimum spanning forest (undirected edges, u <= v)",
    ]
    if source:
        lines.append(f"# input: {source}")
    lines.append(f"total_weight {result.total_weight}")
    lines.append(f"num_edges {result.num_edges}")
    lines.append(f"num_iterations {result.num_iterations}")
    lines.append(f"num_components {result.num_components}")
    lines.append("# edges: u v weight")
    for u, v, w in result.mst_edges:
        lines.append(f"{u} {v} {w}")

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path.resolve()
