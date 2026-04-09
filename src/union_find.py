"""Union–find for consolidating component IDs on the driver (one Borůvka round)."""


class UnionFind:
    def __init__(self) -> None:
        self._parent: dict[int, int] = {}

    def find(self, x: int) -> int:
        if x not in self._parent:
            self._parent[x] = x
        p = self._parent[x]
        if p != x:
            self._parent[x] = self.find(p)
        return self._parent[x]

    def union(self, a: int, b: int) -> int:
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return ra
        if ra < rb:
            self._parent[rb] = ra
            return ra
        self._parent[ra] = rb
        return rb

    def component_map(self, ids: set[int]) -> dict[int, int]:
        for i in ids:
            self.find(i)
        return {i: self.find(i) for i in ids}
