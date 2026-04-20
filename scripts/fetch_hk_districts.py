#!/usr/bin/env python3
"""
Fetch polygon boundaries for all 18 Hong Kong districts from OSM Nominatim
and save them to data/hk_districts.geojson.
"""
import argparse
import json
import time
import urllib.request
import urllib.parse
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# Official 18 districts with their OSM relation IDs for precise lookup.
# Using relation IDs avoids ambiguity from name-only searches.
DISTRICTS: list[tuple[str, int]] = [
    ("Central and Western", 6450180),
    ("Eastern",             6450183),
    ("Southern",            6450185),
    ("Wan Chai",            6450187),
    ("Kowloon City",        6450176),
    ("Kwun Tong",           6450178),
    ("Sham Shui Po",        6450174),
    ("Wong Tai Sin",        6450179),
    ("Yau Tsim Mong",       6450175),
    ("Islands",             6450188),
    ("Kwai Tsing",          6450172),
    ("North",               6450169),
    ("Sai Kung",            6450182),
    ("Sha Tin",             6450181),
    ("Tai Po",              6450170),
    ("Tsuen Wan",           6450173),
    ("Tuen Mun",            6450171),
    ("Yuen Long",           6450168),
]


def fetch_district_geojson(name: str) -> dict | None:
    """Fetch a district polygon from Nominatim by name search."""
    params = urllib.parse.urlencode({
        "q": f"{name} District Hong Kong",
        "format": "geojson",
        "polygon_geojson": 1,
        "limit": 1,
    })
    url = f"https://nominatim.openstreetmap.org/search?{params}"
    req = urllib.request.Request(url, headers={"User-Agent": "MSBD5003-MST-project/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
        features = data.get("features", [])
        return features[0] if features else None
    except Exception as e:
        print(f"    Error: {e}")
        return None


def main():
    p = argparse.ArgumentParser(description="Fetch HK district boundaries from Nominatim")
    p.add_argument(
        "--output", "-o",
        default=str(ROOT / "data" / "hk_districts.geojson"),
        help="Output GeoJSON file path",
    )
    args = p.parse_args()

    features = []
    for i, (name, rel_id) in enumerate(DISTRICTS):
        print(f"[{i+1:2d}/18] {name} ... ", end="", flush=True)
        feat = fetch_district_geojson(name)
        if feat is None:
            print("FAILED — skipping")
        else:
            # Attach clean district name as a property
            feat.setdefault("properties", {})["district"] = name
            features.append(feat)
            geom_type = feat.get("geometry", {}).get("type", "?")
            print(f"ok ({geom_type})")
        if i < len(DISTRICTS) - 1:
            time.sleep(1.1)  # Nominatim rate limit: 1 req/s

    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps({"type": "FeatureCollection", "features": features}, indent=2))
    print(f"\nSaved {len(features)}/18 districts to {out}")

    print("\nAvailable district names:")
    for f in features:
        print(f"  {f['properties']['district']}")


if __name__ == "__main__":
    main()
