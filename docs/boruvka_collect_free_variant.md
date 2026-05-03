# A “less collect-heavy” Borůvka variant (design sketch)

This note answers: **what would Borůvka look like if we avoided pulling large structures to the driver with `collect()`?**

It is **not** a drop-in replacement of your current `boruvka_mst` (which intentionally uses small `collect()` calls for correctness and simplicity). It is a **design direction** you can compare in reports.

---

## What `collect()` is doing today (and why it exists)

In `src/boruvka_spark.py`, each outer Borůvka round ends with driver-side work:

- `chosen = best_per_comp.collect()` — size is **≤ current #components** (not |E|).
- `components.map(...).distinct().collect()` — also **≤ #components** distinct labels.

Then the driver runs **union–find** to compute new component labels and pushes them back with a `map`.

So the current bottleneck is **not “all edges”**, but **#components in early rounds** (can still be huge).

---

## What “without collect” usually means in practice

You cannot magically eliminate **global coordination** in Borůvka: you must communicate merge decisions across the cluster.

“No collect” usually means:

- replace **driver union–find + broadcast-like closure relabel**  
  with **fully distributed contraction / connected-components** on the **component graph** for that round.

You still need **actions** to terminate iterations (`count`, `isEmpty`, etc.), but you avoid **driver-sized** `collect()`.

---

## Proposed distributed round (high level)

Assume you already computed, in Spark, the set of **accepted merge links** between **current component ids**:

- input: `components: RDD[(vertex, comp_id)]`
- input: `merge_links: RDD[(comp_a, comp_b)]` (undirected, deduped)

Goal: compute `new_comp_id` for every vertex **without** collecting all components on the driver.

### Step A — build a component-link graph (distributed)

From `merge_links`, emit undirected adjacency:

- `(a, b)` and `(b, a)`

This is a **narrow** `flatMap` (plus `distinct` which is **wide**).

### Step B — distributed connected-components on component ids

Run iterative **label propagation** until labels stabilize:

1. Start labels: `(comp_id, comp_id)` (each node’s label is itself).
2. Repeat until no change:
   - join adjacency with labels
   - each node takes `min(self_label, neighbor_label)`
   - check convergence with a distributed emptiness test (`isEmpty` on diffs)

This is the pattern implemented in `src/distributed_component_merge.py` as `relabel_components_distributed(...)`.

**Termination:** worst-case rounds can be large for “chain” component graphs; you typically cap iterations or use smarter contraction (see limitations).

### Step C — relabel vertices (distributed join)

Join:

- `(vertex, old_comp)` with `(old_comp, new_root)` → `(vertex, new_root)`

This is a **wide** `join`, but it is standard Spark.

---

## Where the “min outgoing edge” part still lives

The expensive distributed part of Borůvka is still:

- attach component labels to edges (**join**)
- filter crossing edges (**narrow**)
- pick min per component (**wide** `reduceByKey`)

That part is already “Spark-native”.

The only part you are redesigning is **how to apply merges** after candidates are chosen.

---

## Can you remove *all* `collect()`?

**Fully removing every collect is hard** in classic RDD Borůvka unless you redesign how you represent chosen edges.

Reason: `reduceByKey` gives you **per-key minima in parallel**, but turning that into a **global acyclic merge set** without any central view usually requires either:

- multiple distributed iterations (label propagation / star contraction), or
- graph-system primitives (GraphX / Pregel-style messaging).

So “no collect” becomes “**no large collect**”, not literally zero actions.

---

## Trade-offs (why it can be slower in notebooks)

Distributed relabel often runs **multiple Spark jobs per Borůvka outer iteration**:

- each inner iteration: joins + `isEmpty` / `count`
- convergence can take many hops on long chains

Driver union–find is **one sequential pass** on a small list (fast until #components is enormous).

That matches what you observed: **distributed relabel can be slower** even though it avoids driver `collect()` for merges.

---

## Practical recommendation for your project

- **Default path:** hybrid Borůvka (distributed min-edge + small `collect` + driver UF) — simplest and usually fastest at coursework scale.
- **Experimental path:** `relabel_components_distributed(...)` to demonstrate a scalable idea and discuss Amdahl limits.
- **Report narrative:** compare **driver bottleneck** vs **extra shuffle/iteration overhead**.

---

## Related code in this repo

- Current MST implementation: `src/boruvka_spark.py`
- Distributed relabel helper: `src/distributed_component_merge.py`
- Notebook demo + experimental validation section: `boruvka_mst.ipynb`
