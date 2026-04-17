# Borůvka’s Algorithm with PySpark

This project implements **Borůvka’s algorithm** for a **minimum spanning forest (MSF)** of an **undirected, weighted** graph using **Apache Spark** (PySpark **RDD** API). The design follows a **MapReduce-style** pattern in each phase: parallel joins and reductions over edges, then a small **driver-side** consolidation of component labels for the next round.

If the graph is **connected**, the result is a **minimum spanning tree (MST)**. If it is **disconnected**, you get an MSF (one MST per connected component).

Use **`boruvka_mst.ipynb`** for a step-by-step run, pandas tables, **Kruskal validation** after each example, and the same **`output/`** reports as the CLI (see **Jupyter notebook** below). For a written walkthrough and **Mermaid flow diagrams** (narrow vs wide vs action per step), see **`docs/boruvka_spark_flow.md`**.

---

## Algorithm (high level)

Borůvka repeats until no edge connects two different components:

1. **Label vertices** with their current component id (initially each vertex is its own component).
2. **Map** each edge \((u, v, w)\) to the component ids of \(u\) and \(v\).
3. **Keep** only edges with **different** endpoints in component space (crossing edges).
4. **Reduce** by component: for each component \(C\), pick the **minimum-weight** edge leaving \(C\) (ties broken by \((u, v)\) for determinism).
5. **Merge** all selected component pairs on the driver with **union–find**, then **broadcast** the new labels and update every vertex.

There are at most **\(O(\log V)\)** rounds for \(V\) vertices (each round adds at least one edge per remaining tree in a connected component, and the number of trees shrinks). The implementation also supports a **`--max-iterations`** safety cap.

---

## Distributed vs driver work

| Step | Where it runs | Spark operation (conceptually) |
|------|----------------|--------------------------------|
| Attach `comp(u)`, `comp(v)` to edges | Executors | `join` (shuffle) |
| Filter cross-component edges | Executors | `filter` |
| Minimum outgoing edge per component | Executors | `flatMap` + `reduceByKey` |
| Union of chosen merges | Driver | Union–find on collected pairs |
| Relabel all vertices | Executors | `map` + **broadcast** lookup |

**Note:** Each iteration calls `collect()` on the small RDD of **one best edge per current component** (size \(\le\) current number of components), not on all edges. For very large graphs, the **union–find map** is broadcast once per iteration. The **vertex–component** table is still a full RDD join each round (scalable with partitioning).

---

## Input formats (text files)

The graph is **undirected**. Weights can be **int or float** (including scientific notation like `1e-3`).

### 1. Edge list (**recommended** for Spark)

One edge per line:

```text
u v weight
```

- Vertices are **non-negative integers** (any consistent integer ids work).
- **Comments:** lines starting with `#` are ignored.
- **Whitespace:** any spaces/tabs between fields.
- **Weight format:** accepts integers and floats, e.g. `7`, `2.5`, `1e-3`.
- Each undirected edge should appear **once** (or you may list both \((u,v)\) and \((v,u)\); the implementation **canonicalizes** to `(min(u,v), max(u,v), w)` and **deduplicates**).
- **Self-loops** (`u == v`) are ignored.

**Example** (`data/sample_edges.txt`):

```text
# Undirected edge list: u v weight
0 1 1
1 2 2
0 2 3
```

### 2. Adjacency list (“linked” view)

Each line describes one node and its neighbors:

```text
u: v1 w1 v2 w2 ...
```

or without colon:

```text
u v1 w1 v2 w2 ...
```

- Each pair `(neighbor, weight)` repeats the same undirected edge; the loader **deduplicates** identical `(min(u,v), max(u,v), w)` triples while parsing.

**Example** (`data/sample_adjacency.txt`):

```text
0: 1 1 2 3
1: 0 1 2 2
2: 1 2 0 3
```

### Isolated vertices

Vertices that **never appear** in any edge file are **not** included (the edge list defines the vertex set). To include isolates, add a **self-loop with weight 0** (will be filtered) — better: extend the loader with an optional vertex list file if you need that for your report.

---

## Project layout

```text
.
├── README.md
├── requirements.txt
├── boruvka_mst.ipynb       # interactive walkthrough + Kruskal validation
├── run_mst.py              # CLI entry point
├── output/                 # default MST report files (created at run time)
├── src/
│   ├── boruvka_spark.py    # Core algorithm + file → RDD
│   ├── graph_loader.py     # Parsers for edge / adjacency formats
│   ├── mst_output.py       # Write MST report files
│   ├── mst_reference.py    # Kruskal reference (validation)
│   └── union_find.py       # Driver-side union–find for one round
├── scripts/
│   ├── generate_complex_graph.py  # synthetic large graphs + .meta reference
│   └── reference_mst.py             # Kruskal totals for any edge-list file
├── docs/
│   └── boruvka_spark_flow.md   # steps + Spark narrow/wide/action flowcharts
└── data/
    ├── sample_edges.txt
    ├── sample_adjacency.txt
    ├── sample_disconnected.txt
    ├── complex_random.txt           # generated stress test (see header comments)
    └── complex_random.meta          # reference total_weight / |MST| / components
```

---

## Setup

1. **Java** (Spark needs a JDK; Spark 3.4+ commonly targets Java 8/11/17 depending on distribution).
2. **Apache Spark** with **PySpark**, or install PySpark via pip (bundles a Spark version).

```bash
cd "/path/to/Implementation"
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

`requirements.txt` includes **pandas** (used in the notebook for edge tables). For the notebook only, you also need **Jupyter** (or the Jupyter support in your IDE):

```bash
pip install jupyter ipykernel
```

---

## Jupyter notebook (`boruvka_mst.ipynb`)

The notebook is the main **interactive** entry point: it mirrors `run_mst.py`, adds **pandas** views of MST edges, writes the same **`output/mst_*.txt`** reports, and runs a **validation** step after each example.

### What it does

1. **Project root** — The first code cell searches upward from the kernel’s current working directory for `src/boruvka_spark.py`, adds that folder to `sys.path`, and `chdir`s there so `data/` and `output/` paths resolve correctly (even if the server was started elsewhere).
2. **Spark** — Builds a `SparkSession` (`local[*]` by default; override with env `SPARK_MASTER`).
3. **Validation (Kruskal)** — Defines `validate_vs_kruskal(...)`, which loads the **same** graph as Spark (from a file or an explicit edge list), runs **`kruskal_msf`** from `src/mst_reference.py` on the driver, and **asserts** that Spark’s result matches on:
   - **`total_weight`**
   - **`num_components`**
   - **`|MST|`** (number of edges in the spanning forest)  
   When weights tie, Borůvka and Kruskal may pick different MST **edge sets**; those three checks still certify a correct **minimum** spanning forest weight and structure.
4. **Examples (in order)**  
   - Edge list: `data/sample_edges.txt`  
   - Inline `parallelize` graph  
   - Adjacency list: `data/sample_adjacency.txt`  
   - Disconnected MSF: `data/sample_disconnected.txt`  
   - Larger stress test: `data/complex_random.txt`, plus an optional assert against **`data/complex_random.meta`** (`reference_total_weight`, etc.)
5. **Stop Spark** — Final cell calls `spark.stop()`. After that, **re-run the Spark session cell** before re-executing compute cells.

### How to run

```bash
cd "/path/to/Implementation"
source .venv/bin/activate
jupyter notebook boruvka_mst.ipynb
```

Or open `boruvka_mst.ipynb` in **VS Code / Cursor**, choose the venv’s Python interpreter, and **Run All**. On the first pass, run cells **top to bottom** so imports, `PROJ_ROOT`, Spark, and `validate_vs_kruskal` are defined before the examples.

---

## Run (CLI)

From the project root (so `src` is importable):

```bash
python run_mst.py data/sample_edges.txt
python run_mst.py data/sample_adjacency.txt --format adjacency_list
python run_mst.py data/sample_disconnected.txt
python run_mst.py data/complex_random.txt   # larger random graph (~120 vertices, ~620 edges)
```

**Larger / custom graphs:** `python scripts/generate_complex_graph.py --out data/mygraph.txt --vertices 200 --extra-edges 1000 --seed 1` writes an edge list plus `data/mygraph.meta` with Kruskal reference totals. Check Spark with `python scripts/reference_mst.py data/mygraph.txt` and compare `total_weight` to `run_mst.py`.

Optional:

```bash
python run_mst.py data/sample_edges.txt --master local[4]
python run_mst.py data/sample_edges.txt --max-iterations 50
```

**Remote / cluster:** pass a file path Spark can read (`hdfs://…`, `s3a://…`, etc.) and set `--master` to your cluster URL.

---

## Output

By default the **CLI** writes a text report under **`output/mst_<input_basename>.txt`** (project root). The **notebook** writes the same style of files (`output/mst_sample_edges.txt`, `mst_inline_example.txt`, etc.) from each section. Override CLI path with `--output` / `-o`, or the default directory with `--output-dir`. Each file lists `total_weight`, `num_iterations`, `num_components`, and one edge per line (`u v weight` with `u ≤ v`).

The **CLI** prints:

- **MST/MSF edges** as `(u, v, weight)` with **`u < v`**.
- **`total_weight`**: sum of weights (each undirected edge counted once).
- **`num_iterations`**: Borůvka rounds executed.
- **`num_components`**: number of connected components in the **input** graph (from final component labels).

**Expected** for `data/sample_edges.txt`: edges `(0, 1, 1)` and `(1, 2, 2)`, `total_weight=3`, `num_components=1`.

---

## API usage (in your own code)

```python
from pyspark.sql import SparkSession
from src.boruvka_spark import boruvka_mst, edges_rdd_from_text_file

spark = SparkSession.builder.appName("demo").master("local[*]").getOrCreate()
sc = spark.sparkContext

edges = edges_rdd_from_text_file(sc, "data/sample_edges.txt", fmt="edge_list")
result = boruvka_mst(sc, edges)
print(result.mst_edges, result.total_weight)
spark.stop()
```

---

## Correctness and limitations

- **Correct** for undirected graphs with **distinct** parallel edges allowed; minimum weight is chosen per component each round.
- **Weights** should be orderable; ties are broken by \((u, v)\).
- **Driver memory:** each iteration collects **at most one record per current component** (not the full edge list). Very large **numbers of components** still imply a large collect + union–find — acceptable for typical coursework sizes; for extreme scale, you would replace the driver union–find round with a fully distributed label propagation (more complex).
- **Directed graphs** are not supported as-is (treat edges as undirected by listing each logical edge once or twice).

---

## References

- Borůvka, O. *O jistém problému minimálním* (1926) — historical origin of the algorithm.
- Standard MST algorithm texts (Cormen et al.; Kleinberg & Tardos) for Borůvka’s rounds and \(O(\log V)\) depth intuition.
- Apache Spark RDD [Programming Guide](https://spark.apache.org/docs/latest/rdd-programming-guide.html) for `join`, `reduceByKey`, and broadcasts.

---

## License

Use and modify for your MSBD 5003 project as needed.
