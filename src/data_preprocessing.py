import math
import argparse
import osmium

def haversine(lat1, lon1, lat2, lon2):
    R = 6371000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    d = int(2 * R * math.asin(math.sqrt(a)))
    return d

class RoadGraphHandler(osmium.SimpleHandler):
    def __init__(self):
        super().__init__()
        self.coords = {} # node_id: (lat,lon)
        self.edges = set() # (u, v, w)

    def node(self, n):
        self.coords[n.id] = (n.location.lat, n.location.lon)

    def way(self, w):
        if 'highway' not in w.tags:
            return
        nodes = list(w.nodes)
        for i in range(len(nodes) - 1):
            u, v = nodes[i].ref, nodes[i + 1].ref

            if u not in self.coords or v not in self.coords:
                continue

            lat1, lon1 = self.coords[u]
            lat2, lon2 = self.coords[v]
            w = haversine(lat1, lon1, lat2, lon2)

            if w == 0:
                continue
            # normalized representation
            a, b = (u, v) if u <= v else (v, u)
            self.edges.add((a, b, w))

    def remap(self):
        # Map the original OSM node ids to a compact ones
        all_nodes = sorted({u for u, _, _ in self.edges} | {v for _, v, _ in self.edges})
        id_map = {old: new for new, old in enumerate(all_nodes)}

        self.edges = {(id_map[u], id_map[v], w) for u, v, w in self.edges}
            
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Preprocess OSM data to build road network graph.")
    parser.add_argument("--input", type=str, required=True, help="Input OSM PBF file.")
    parser.add_argument("--output", type=str, required=True, help="Output file for edge list.")

    args = parser.parse_args()

    handler = RoadGraphHandler()
    handler.apply_file(args.input, locations=True)
    handler.remap()
    with open(args.output, "w") as f:
        f.write("# Hong Kong road network - edge list: u v weight_meters\n")
        for u, v, w in handler.edges:
            f.write(f"{u} {v} {w}\n")

    print(f"Extracted {len(handler.edges)} edges from OSM data to {args.output}")