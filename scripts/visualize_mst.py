#!/usr/bin/env python3
"""
Visualize the Borůvka MST overlaid on the Hong Kong road network map.

Produces an interactive HTML map (folium) with:
  - Grey lines: all original road edges (sampled for performance)
  - Red lines: MST edges
  - Optional district boundary and region filtering
"""

import argparse
import csv
import json
import random
import sys
from pathlib import Path

import folium

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def load_coords(path: str) -> dict[int, tuple[float, float]]:
    coords: dict[int, tuple[float, float]] = {}
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            coords[int(row["node_id"])] = (float(row["lat"]), float(row["lon"]))
    return coords


def load_edges(path: str) -> list[tuple[int, int, int]]:
    edges = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split()
            if len(parts) >= 3:
                try:
                    edges.append((int(parts[0]), int(parts[1]), int(parts[2])))
                except ValueError:
                    continue
    return edges


def load_mst_edges(path: str) -> list[tuple[int, int, int]]:
    return load_edges(path)


def load_district_polygon(geojson_path: str, district_name: str):
    """Return a shapely geometry for the named district, or raise ValueError."""
    try:
        from shapely.geometry import shape
    except ImportError:
        print("shapely not installed. Run: pip install shapely")
        sys.exit(1)

    with open(geojson_path) as f:
        fc = json.load(f)

    name_lower = district_name.strip().lower()
    for feature in fc.get("features", []):
        props = feature.get("properties", {})
        if props.get("district", "").lower() == name_lower:
            return shape(feature["geometry"])

    available = [f["properties"].get("district", "") for f in fc.get("features", [])]
    raise ValueError(
        f"District '{district_name}' not found in {geojson_path}.\n"
        f"Available: {available}"
    )


def filter_edges_by_region(
    edges: list[tuple[int, int, int]],
    coords: dict[int, tuple[float, float]],
    polygon,
) -> list[tuple[int, int, int]]:
    """Keep edges where both endpoints lie inside the polygon."""
    from shapely.geometry import Point

    bbox = polygon.bounds  # (minx, miny, maxx, maxy) = (minlon, minlat, maxlon, maxlat)
    minlon, minlat, maxlon, maxlat = bbox

    # Cache per-node membership (bounding box pre-filter, then exact polygon test)
    inside: dict[int, bool] = {}

    def node_inside(node_id: int) -> bool:
        if node_id in inside:
            return inside[node_id]
        if node_id not in coords:
            inside[node_id] = False
            return False
        lat, lon = coords[node_id]
        if not (minlat <= lat <= maxlat and minlon <= lon <= maxlon):
            inside[node_id] = False
            return False
        result = polygon.contains(Point(lon, lat))
        inside[node_id] = result
        return result

    return [(u, v, w) for u, v, w in edges if node_inside(u) and node_inside(v)]


def _edges_to_geojson(
    edges: list[tuple[int, int, int]],
    coords: dict[int, tuple[float, float]],
) -> tuple[dict, int]:
    features = []
    skipped = 0
    for u, v, w in edges:
        if u not in coords or v not in coords:
            skipped += 1
            continue
        lat_u, lon_u = coords[u]
        lat_v, lon_v = coords[v]
        features.append({
            "type": "Feature",
            "geometry": {
                "type": "LineString",
                "coordinates": [[lon_u, lat_u], [lon_v, lat_v]],
            },
            "properties": {"u": u, "v": v, "weight_m": w},
        })
    return {"type": "FeatureCollection", "features": features}, skipped


def _make_colormap(edges: list[tuple[int, int, int]], colors: list[str]):
    """Return a branca LinearColormap scaled to the weight range of `edges`."""
    import branca.colormap as cm

    weights = [w for _, _, w in edges]
    vmin, vmax = min(weights), max(weights)
    # Avoid degenerate single-value range
    if vmin == vmax:
        vmax = vmin + 1
    cmap = cm.LinearColormap(colors=colors, vmin=vmin, vmax=vmax)
    cmap.caption = "Edge weight (metres)"
    return cmap


def build_map(
    coords: dict[int, tuple[float, float]],
    all_edges: list[tuple[int, int, int]],
    mst_edges: list[tuple[int, int, int]],
    road_sample_rate: float,
    mst_sample_rate: float,
    output_path: str,
    district_name: str | None = None,
    district_polygon=None,
    color_by_weight: bool = True,
) -> None:
    # Determine map centre and zoom
    if district_polygon is not None:
        c = district_polygon.centroid
        map_center = [c.y, c.x]
        zoom = 13
    else:
        map_center = [22.3193, 114.1694]
        zoom = 11

    m = folium.Map(location=map_center, zoom_start=zoom, tiles="CartoDB positron")

    # Draw district boundary if filtering by region
    if district_polygon is not None:
        from shapely.geometry import mapping
        boundary_geojson = {"type": "Feature", "geometry": mapping(district_polygon), "properties": {}}
        folium.GeoJson(
            boundary_geojson,
            name=f"{district_name} boundary",
            style_function=lambda _: {
                "color": "#2196F3", "weight": 2.5, "fillOpacity": 0.05,
            },
        ).add_to(m)

    # Sampled road network (skipped if rate == 0)
    sampled_roads: list = []
    missing_road = 0
    if road_sample_rate > 0.0:
        sampled_roads = (
            all_edges if road_sample_rate >= 1.0
            else random.sample(all_edges, int(len(all_edges) * road_sample_rate))
        )
        road_geojson, missing_road = _edges_to_geojson(sampled_roads, coords)
        folium.GeoJson(
            road_geojson,
            name=f"Road network ({road_sample_rate:.0%} sample)",
            style_function=lambda _: {"color": "#aaaaaa", "weight": 0.8, "opacity": 0.4},
        ).add_to(m)

    # MST edges
    sampled_mst = (
        mst_edges if mst_sample_rate >= 1.0
        else random.sample(mst_edges, int(len(mst_edges) * mst_sample_rate))
    )
    mst_geojson, missing_mst = _edges_to_geojson(sampled_mst, coords)

    if color_by_weight and sampled_mst:
        colormap = _make_colormap(
            sampled_mst,
            colors=["#2166ac", "#fee08b", "#d73027"],  # blue → yellow → red
        )
        colormap.add_to(m)
        style_fn = lambda f: {
            "color": colormap(f["properties"]["weight_m"]),
            "weight": 1.8,
            "opacity": 0.85,
        }
    else:
        style_fn = lambda _: {"color": "#e63946", "weight": 1.8, "opacity": 0.85}

    folium.GeoJson(
        mst_geojson,
        name="MST edges (coloured by weight)",
        style_function=style_fn,
        tooltip=folium.GeoJsonTooltip(
            fields=["u", "v", "weight_m"],
            aliases=["Node u", "Node v", "Length (m)"],
        ),
    ).add_to(m)

    folium.LayerControl().add_to(m)

    total_mst_weight = sum(w for _, _, w in mst_edges)
    region_label = f"<br>Region: <b>{district_name}</b>" if district_name else ""
    html = f"""
    <div style="position:fixed;bottom:30px;left:30px;z-index:1000;
                background:white;padding:12px 16px;border-radius:8px;
                box-shadow:2px 2px 8px rgba(0,0,0,0.3);font-family:sans-serif;font-size:13px">
      <b>Borůvka MST — Hong Kong Road Network</b>{region_label}<br>
      Road edges shown: {len(sampled_roads):,}<br>
      MST edges shown: {len(sampled_mst):,} of {len(mst_edges):,}<br>
      MST total weight: {total_mst_weight:,} m ({total_mst_weight/1000:.1f} km)<br>
      <span style="color:#e63946">&#9644;</span> MST &nbsp;
      <span style="color:#aaaaaa">&#9644;</span> Roads &nbsp;
      <span style="color:#2196F3">&#9644;</span> District
    </div>
    """
    m.get_root().html.add_child(folium.Element(html))

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    m.save(output_path)

    if missing_mst:
        print(f"  Warning: {missing_mst} MST edges skipped (nodes missing from coords)")
    if missing_road:
        print(f"  Warning: {missing_road} road edges skipped (nodes missing from coords)")


def main() -> None:
    p = argparse.ArgumentParser(description="Visualize MST on Hong Kong road map")
    p.add_argument("--edges", required=True, help="Original edge list (.txt)")
    p.add_argument("--coords", required=True, help="Node coordinates CSV (node_id,lat,lon)")
    p.add_argument("--mst", required=True, help="MST output file from run_mst.py")
    p.add_argument("--output", "-o", default="visualizations/mst_map.html", help="Output HTML map path")
    p.add_argument(
        "--region",
        default=None,
        help='Filter to a single HK district, e.g. --region "Wan Chai". '
             'Use --districts to specify the boundary file.',
    )
    p.add_argument(
        "--districts",
        default=str(ROOT / "data" / "hk_districts.geojson"),
        help="District boundaries GeoJSON (default: data/hk_districts.geojson)",
    )
    p.add_argument(
        "--road-sample",
        type=float,
        default=0.0,
        help="Fraction of road edges to draw (default: 0 = skip, base tile shows roads). "
             "Set >0 to overlay sampled roads.",
    )
    p.add_argument(
        "--mst-sample",
        type=float,
        default=0.02,
        help="Fraction of MST edges to draw (default: 0.02). "
             "Ignored when --region is set (all region edges are shown).",
    )
    p.add_argument(
        "--no-color-weight",
        action="store_true",
        help="Disable weight-based colouring; draw all MST edges in a single red colour.",
    )
    args = p.parse_args()

    print("Loading coordinates...")
    coords = load_coords(args.coords)
    print(f"  {len(coords):,} nodes loaded")

    print("Loading road edges...")
    all_edges = load_edges(args.edges)
    print(f"  {len(all_edges):,} edges loaded")

    print("Loading MST edges...")
    mst_edges = load_mst_edges(args.mst)
    print(f"  {len(mst_edges):,} MST edges loaded")

    district_polygon = None
    if args.region:
        print(f"Filtering to district: {args.region} ...")
        district_polygon = load_district_polygon(args.districts, args.region)
        mst_edges = filter_edges_by_region(mst_edges, coords, district_polygon)
        all_edges = filter_edges_by_region(all_edges, coords, district_polygon)
        print(f"  {len(mst_edges):,} MST edges within region")
        print(f"  {len(all_edges):,} road edges within region")
        # Show all edges within the district — no sampling needed
        mst_sample = 1.0
        road_sample = args.road_sample
    else:
        mst_sample = args.mst_sample
        road_sample = args.road_sample

    print("Building map...")
    build_map(
        coords, all_edges, mst_edges,
        road_sample_rate=road_sample,
        mst_sample_rate=mst_sample,
        output_path=args.output,
        district_name=args.region,
        district_polygon=district_polygon,
        color_by_weight=not args.no_color_weight,
    )
    print(f"Map saved to {args.output}")
    print("Open in a browser to explore interactively.")


if __name__ == "__main__":
    main()
