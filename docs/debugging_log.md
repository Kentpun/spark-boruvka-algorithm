
### Cycle creation in rounds with equal-weight edges

Original implementation add every edge selected by best_per_comp to mst_acc with no cycle check. In a round where components A→B→C→A each select a different minimum edge (possible when weights are tied), all 3 edges go into mst_round and mst_acc, but
only 2 are needed to connect 3 components. This adds a redundant edge and inflates total_weight. Road networks have many edges with identical integer weights (distances in metres), so ties are frequent.

**Fix**: use a local UnionFind when collecting mst_round to skip edges that would close a cycle:
```python
  round_uf = UnionFind()
  for _comp_key, (w, u, v, other_comp) in chosen:
      merge_pairs.append((_comp_key, other_comp))
      # Only add edge if it actually bridges two un-merged components
      if round_uf.find(_comp_key) != round_uf.find(other_comp):
          round_uf.union(_comp_key, other_comp)
          mst_round[_norm_edge(u, v, w)] = (u, v, w)
```