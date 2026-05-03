# Borůvka on Spark: driver-side union–find vs distributed contraction (detailed analysis)

This document explains **what the current implementation does on the driver**, **what data structures it uses**, and how that compares to a **distributed alternative** that replaces driver-side union–find for component contraction. It is written to support a project report section on **scalability trade-offs**.

Code references:

- Main loop: `src/boruvka_spark.py`
- Driver DSU: `src/union_find.py`
- Distributed relabel prototype: `src/distributed_component_merge.py`

---

## Part A — Current implementation: driver-side union–find and what it is for

### A.1 What problem union–find solves in *this* Borůvka loop

Each Borůvka **outer iteration** must:

1. For every current **component** \(C\), pick a minimum-weight edge leaving \(C\) toward a different component (distributed).
2. Turn those picks into **actual merges** of components for the next iteration (global connectivity problem).
3. Update every vertex’s **component label** so the next iteration’s joins are correct.

Union–find is used for step (2)+(3) on the **component-id graph** (supernodes), not on the original road graph directly.

---

### A.2 Spark side (distributed): min outgoing edge per component

The distributed portion builds rows `(u, v, w, comp_u, comp_v)` and filters crossing edges `comp_u != comp_v`, then emits two directed candidates per crossing edge and reduces by component id:

```87:108:/Users/kentpun/Documents/_04.studies/MSc BDT/MSBD 5003/_05. Project/Implementation/src/boruvka_spark.py
            e_u = edges.map(lambda e: (e[0], (e[1], e[2]))).join(comp_by_v)
            keyed_by_v = e_u.map(
                lambda x: (x[1][0][0], (x[0], x[1][0][1], x[1][1]))
            )
            joined = keyed_by_v.join(comp_by_v).map(
                lambda x: (x[1][0][0], x[0], x[1][0][1], x[1][0][2], x[1][1])
            )

            cross = joined.filter(lambda t: t[3] != t[4])
            ...
            candidates = cross.flatMap(
                lambda t: (
                    (t[3], (t[2], t[0], t[1], t[4])),
                    (t[4], (t[2], t[1], t[0], t[3])),
                )
            )
            best_per_comp = candidates.reduceByKey(_min_edge_choice)
```

**Spark operation types (typical):**

- `join`: **wide** (shuffle/hash exchange on join keys)
- `filter`, `map`, `flatMap`: **narrow**
- `reduceByKey`: **wide** (shuffle + per-key reduction)

This stage is usually where you get parallel speedup from Spark.

---

### A.3 Driver side: what is collected, and why it is not “all edges”

The per-component minima are brought to the driver:

```110:112:/Users/kentpun/Documents/_04.studies/MSc BDT/MSBD 5003/_05. Project/Implementation/src/boruvka_spark.py
            chosen = best_per_comp.collect()
            if not chosen:
                break
```

**Important precision for reports:**

- `best_per_comp` has **at most one record per distinct component key** after `reduceByKey`.
- So `len(chosen)` is **≤ current number of components**, not \(|E|\).

There is also a second per-iteration collect of distinct component labels:

```144:144:/Users/kentpun/Documents/_04.studies/MSc BDT/MSBD 5003/_05. Project/Implementation/src/boruvka_spark.py
            all_comp_ids = set(components.map(lambda x: x[1]).distinct().collect())
```

So each outer iteration can involve **two** driver collects whose sizes scale with **#components / distinct labels**, not the full edge list.

---

### A.4 Driver-side cycle scan + MST edge selection (round-local DSU)

After `chosen` is collected, the code builds a deduped set of component-component candidate links and runs a **round-local** union–find to avoid adding cyclic merges among the *round’s* chosen supernode edges:

```114:142:/Users/kentpun/Documents/_04.studies/MSc BDT/MSBD 5003/_05. Project/Implementation/src/boruvka_spark.py
            comp_link_best: dict[tuple[int, int], tuple[Weight, int, int]] = {}
            for comp_a, (w, u, v, comp_b) in chosen:
                ...
            round_uf = UnionFind()
            merge_pairs: list[tuple[int, int]] = []
            mst_round: dict[tuple[int, int, Weight], tuple[int, int, Weight]] = {}
            for (ca, cb), (w, u, v) in sorted(
                comp_link_best.items(), key=lambda item: (item[1][0], item[1][1], item[1][2])
            ):
                if round_uf.find(ca) == round_uf.find(cb):
                    continue
                round_uf.union(ca, cb)
                merge_pairs.append((ca, cb))
                mst_round[_norm_edge(u, v, w)] = (u, v, w)
```

**Data structures here:**

- `comp_link_best`: Python `dict` keyed by normalized component pair `(min,max)` mapping to best physical edge representation `(w, u, v)` (tuple ordering used for deterministic tie breaks).
- `round_uf`: a fresh `UnionFind` instance used only to filter redundant merges inside the round.
- `merge_pairs`: list of accepted component merges for the global contraction step.
- `mst_round`: dict of MST edges added this round.

---

### A.5 Global contraction: second union–find + `comp_map` + vertex relabel

The accepted `merge_pairs` are unioned again to compute final roots for relabeling:

```149:152:/Users/kentpun/Documents/_04.studies/MSc BDT/MSBD 5003/_05. Project/Implementation/src/boruvka_spark.py
            uf = UnionFind()
            for a, b in merge_pairs:
                uf.union(a, b)
            comp_map = uf.component_map(all_comp_ids)
```

`UnionFind` itself is a parent-pointer forest stored in a Python dict:

```4:29:/Users/kentpun/Documents/_04.studies/MSc BDT/MSBD 5003/_05. Project/Implementation/src/union_find.py
class UnionFind:
    def __init__(self) -> None:
        self._parent: dict[int, int] = {}
    ...
```

Then vertices are relabeled in Spark using a **narrow `map`**:

```158:160:/Users/kentpun/Documents/_04.studies/MSc BDT/MSBD 5003/_05. Project/Implementation/src/boruvka_spark.py
            components = comp_by_v.map(
                lambda x, m=comp_map: (x[0], m.get(x[1], x[1]))
            ).cache()
```

**Important implementation detail for reports:**

- `comp_map` is computed on the driver, but it is **used by Spark executors** because it is captured in the task closure (`m=comp_map`).
- This is not “Spark ignoring the map”; it is **serialization/shipping of the closure payload** to executors.

Optional improvement at large map sizes: `sc.broadcast(comp_map)` (same direction: driver → executors), with careful lifecycle management.

---

### A.6 Additional driver collect introduced for edge pruning (scalability note)

The file also contains a progressive pruning step that collects **all vertex→component** pairs to a Python dict:

```163:170:/Users/kentpun/Documents/_04.studies/MSc BDT/MSBD 5003/_05. Project/Implementation/src/boruvka_spark.py
            vertex_comp = dict(components.collect())
            prev_edges = edges
            edges = prev_edges.filter(
                lambda e, vc=vertex_comp: vc.get(e[0]) != vc.get(e[1])
            ).cache()
```

For reporting/scaling analysis, treat this separately:

- It is **\(O(|V|)\)** driver materialization per iteration (worst case), which can dominate driver memory for huge graphs.
- Its benefit is reducing future **join input size** for `edges ⋈ components` by removing intra-component edges early.

---

## Part B — Alternative: distributed contraction replacing driver union–find

### B.1 What is being replaced

Only the **global contraction / relabel** step:

- from: `uf.union` + `comp_map` + `map(m=comp_map)`
- to: distributed connected-components style propagation on **component-link edges**

The distributed min-edge selection (`join` + `reduceByKey`) can remain the same.

Prototype code: `relabel_components_distributed(...)` in `src/distributed_component_merge.py`.

---

### B.2 Mechanism: emit links + iterative min-label propagation

Given merge links `(a,b)` between component ids, build undirected adjacency and repeatedly update labels:

```35:67:/Users/kentpun/Documents/_04.studies/MSc BDT/MSBD 5003/_05. Project/Implementation/src/distributed_component_merge.py
    adjacency = (
        merge_links.flatMap(lambda ab: ((ab[0], ab[1]), (ab[1], ab[0])))
        .distinct()
        .cache()
    )
    ...
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
```

Then relabel vertices by joining `(vertex, old_comp)` with `(old_comp, root)`:

```117:122:/Users/kentpun/Documents/_04.studies/MSc BDT/MSBD 5003/_05. Project/Implementation/src/distributed_component_merge.py
    relabeled = (
        components.map(lambda vc: (int(vc[1]), int(vc[0])))
        .join(comp_to_root)
        .map(lambda x: (x[1][0], x[1][1]))
    )
```

**What problem this solves relative to driver UF:**

- avoids building `comp_map` entirely on the driver
- avoids needing a single Python dict to hold the entire parent forest for global merges (as graph grows)

**What problem it does *not* magically solve:**

- you still need **global synchronization**, now expressed as **multiple Spark jobs** (iterations) instead of one driver pass.

---

## Part C — Performance analysis (why distributed contraction often “feels slower”)

Below is a structured narrative aligned to your outline, but grounded in *this codebase’s* operations.

### C.1 High communication intensity (shuffle-heavy inner loop)

Distributed label propagation does repeated:

- `join` on adjacency (`adjacency.join(old_labels)`)
- `reduceByKey(min)` to aggregate neighbor minima

Each inner iteration is typically **at least one shuffle stage** (sometimes more depending on lineage caching).

By contrast, driver union–find does **pointer chasing in RAM** with very low constant factors.

References:

- Spark shuffle/RDD concepts: [RDD Programming Guide](https://spark.apache.org/docs/latest/rdd-programming-guide.html)
- Union–find asymptotics (amortized near-constant): [Wikipedia — Disjoint-set data structure](https://en.wikipedia.org/wiki/Disjoint-set_data_structure)

---

### C.2 Excessive iterations (outer Borůvka vs inner convergence)

You already pay **\(O(\log V)\)** Borůvka-style outer rounds in many graphs (empirically bounded by `max_iterations` too).

Distributed contraction adds an **inner loop** per outer round:

- worst-case many propagation rounds on long chains of component merges
- each round needs an action like `count()` / `isEmpty()` to materialize/check convergence in Spark

This is the classic trade-off: **replace one cheap global step with several expensive distributed steps**.

---

### C.3 Data skew and stragglers

Even if merge links are small, skew can appear because:

- some vertices/components touch many edges in the original join stages
- `reduceByKey` can create hot keys if many candidates map to same component id patterns (less common here, but possible with pathological graphs)

Spark performance is often limited by the **slowest task** in a stage.

---

### C.4 Memory and serialization (Python ↔ JVM)

PySpark UDF closures serialize Python objects to executors.

- `comp_map` dict captured in `map` is shipped to executors (closure serialization).
- Distributed iterative approach increases **number of serialized stages** and Python object churn.

This overhead matters most when the cluster is small or the graph is not huge.

---

## Part D — “When does distributed pay off?” (how to phrase it safely)

A defensible report statement:

- For **large enough graphs** where **driver collects / driver memory** become the dominant bottleneck, distributed contraction can be necessary even if it is slower per merge on small benchmarks.
- For **many coursework-sized graphs**, a hybrid **distributed joins + driver UF** is often faster end-to-end because it minimizes repeated shuffle iterations.

This is essentially **Amdahl’s law**: speeding up parallel stages does not help if a serial/global phase (or many extra distributed rounds) dominates.

Reference:

- [Amdahl’s law (Wikipedia)](https://en.wikipedia.org/wiki/Amdahl%27s_law)

---

## Part E — Suggested “report figure” ideas (optional)

1. **Per-iteration breakdown** (measured): time in `join` stages vs driver `collect`+UF vs relabel `map`.
2. **Sizes**: `|chosen|`, distinct `components` labels, `|E|` after pruning.
3. **Outer iterations** vs **inner propagation iterations** (distributed variant).

---

## Part F — References (non-paywalled)

1. Apache Spark Project. *RDD Programming Guide* — transformations/actions, shuffles.  
   https://spark.apache.org/docs/latest/rdd-programming-guide.html

2. Wikipedia contributors. *Disjoint-set data structure*.  
   https://en.wikipedia.org/wiki/Disjoint-set_data_structure

3. Wikipedia contributors. *Amdahl’s law*.  
   https://en.wikipedia.org/wiki/Amdahl%27s_law
