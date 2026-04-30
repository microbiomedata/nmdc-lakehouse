# Graph traversal: Biosample ↔ DataObject paths

> **TL;DR for BERDL notebook users:** Use Trino (`get_trino_connection()`), not Spark.
> Spark 4.0.1 `WITH RECURSIVE` is broken (see below). The working solution uses
> four existing Silver side tables — no `nmdc_graph_edges` or alldocs required.
> See [`trino_recursive_graph_traversal.md`](trino_recursive_graph_traversal.md)
> for tested, production-ready code.
>
> **TL;DR for remote / API users:** The Trino two-step pattern requires Python
> orchestration between calls and does not translate to a single MCP API query.
> Use the precomputed `workflow_to_biosample` table instead — see
> [Precomputed closure for remote and API access](#precomputed-closure-for-remote-and-api-access).

## Spark 4.0.1 WITH RECURSIVE is broken

Both Spark transport modes fail with the same root cause:

```
assertion failed: No plan for UnionLoop 0
```

- **Spark Connect** (`get_spark_session()` default): surfaces as `No plan for UnionLoop`
- **Classic standalone** (`get_spark_session(use_spark_connect=False)`): surfaces as
  `INTERNAL_ERROR: transpose requires all collections have the same size`

This is a Spark planner bug, not a configuration problem. Setting
`spark.sql.ansi.enabled = true` or switching master URLs does not help.

**Use Trino instead.** Trino is available in every BERDL notebook via
`get_trino_connection()`, reads the same Delta tables from the Hive metastore,
and supports `WITH RECURSIVE` natively.

## The problem

NMDC metadata forms two interleaved bipartite chains between a Biosample and
the DataObjects that were produced from it:

**Material transformation chain** (variable depth):
```
Biosample → Extraction/Filtration/SubSampling/Pooling/LibraryPrep
          → ProcessedSample → more MaterialProcessing → ProcessedSample
          → DataGeneration (NucleotideSequencing or MassSpectrometry)
```

**Workflow / data object chain** (variable depth):
```
DataGeneration → DataObject (raw reads)
              → ReadQcAnalysis → DataObject
              → MetagenomeAssembly → DataObject
              → MetagenomeAnnotation → DataObject (annotation TSV)
```

The depth of each chain is not fixed. It depends on the number of sample
preparation steps and the workflow DAG for a given study. The Silver schema
tables (`data_generation_set_has_input`, `workflow_execution_set_has_input`,
etc.) are each one hop only and all share the same polymorphic `type` column —
writing a generic recursive CTE against them requires knowing which table to
join at each hop, which makes the query non-generic and fragile.

## Why `nmdc_graph_edges` solves this

`nmdc_graph_edges` (see issue #97) collapses all entity types and all
relationship types into a single directed edge table:

| Column | Meaning |
|---|---|
| `src_id` | upstream entity ID |
| `src_type` | upstream entity type (e.g. `nmdc:Biosample`) |
| `dst_id` | downstream entity ID |
| `dst_type` | downstream entity type (e.g. `nmdc:DataObject`) |

Every hop — whether it crosses a MaterialProcessing step, a DataGeneration,
or a WorkflowExecution — is a row in this table. A recursive CTE can traverse
any depth without knowing the intermediate entity types in advance.

**Prerequisite:** `nmdc_graph_edges` must be loaded into `nmdc_metadata`.
The ETL job exists (`nmdc-lakehouse run-job alldocs`) and a BERDL ingest
notebook is needed to register the three tables as Delta tables. Until that
is done, use the fixed-join patterns in `nmdc_metadata_tables.md`.

## Recursive CTE patterns (Spark 4.0+)

Spark 4.0 supports `WITH RECURSIVE` natively. A depth guard (`depth < N`) is
mandatory to prevent infinite loops on any cycles in the graph.

### Downstream: Biosample → all reachable DataObjects

```sql
WITH RECURSIVE downstream(id, type, depth) AS (
  -- anchor: the starting biosample
  SELECT 'nmdc:bsm-11-xyz123' AS id,
         'nmdc:Biosample'     AS type,
         0                    AS depth

  UNION ALL

  -- recursive: follow one edge downstream per iteration
  SELECT e.dst_id,
         e.dst_type,
         d.depth + 1
  FROM   downstream d
  JOIN   nmdc_metadata.nmdc_graph_edges e ON e.src_id = d.id
  WHERE  d.depth < 10            -- safety limit; NMDC paths rarely exceed 6
)
SELECT DISTINCT id AS data_object_id, depth
FROM   downstream
WHERE  type = 'nmdc:DataObject'
ORDER  BY depth
```

### Upstream: DataObject → all reachable Biosamples

Traverse in reverse by matching `e.dst_id = d.id` and collecting `e.src_id`:

```sql
WITH RECURSIVE upstream(id, type, depth) AS (
  SELECT 'nmdc:dobj-11-abc456' AS id,
         'nmdc:DataObject'     AS type,
         0                     AS depth

  UNION ALL

  SELECT e.src_id,
         e.src_type,
         u.depth + 1
  FROM   upstream u
  JOIN   nmdc_metadata.nmdc_graph_edges e ON e.dst_id = u.id
  WHERE  u.depth < 10
)
SELECT DISTINCT id AS biosample_id, depth
FROM   upstream
WHERE  type = 'nmdc:Biosample'
ORDER  BY depth
```

### Science query: KO annotation hits per biosample (any path length)

Filter early on the annotation table, then traverse upstream to find
biosamples regardless of how many material processing or workflow steps
intervene:

```sql
WITH

-- 1. Collect the distinct workflow run IDs that have the target KO
ko_runs AS (
  SELECT DISTINCT workflow_run_id
  FROM   nmdc_results.annotation_kegg_orthology
  WHERE  annotation_id = 'KO:K00001'
),

-- 2. Traverse upstream from each workflow run to find all Biosamples
RECURSIVE upstream(start_id, id, type, depth) AS (
  SELECT workflow_run_id AS start_id,
         workflow_run_id AS id,
         'nmdc:MetagenomeAnnotation' AS type,
         0 AS depth
  FROM   ko_runs

  UNION ALL

  SELECT u.start_id,
         e.src_id,
         e.src_type,
         u.depth + 1
  FROM   upstream u
  JOIN   nmdc_metadata.nmdc_graph_edges e ON e.dst_id = u.id
  WHERE  u.depth < 10
    AND  u.type != 'nmdc:Biosample'   -- prune: stop expanding once we reach a biosample
),

-- 3. Keep only the biosample endpoints
run_to_biosample AS (
  SELECT DISTINCT start_id AS workflow_run_id, id AS biosample_id
  FROM   upstream
  WHERE  type = 'nmdc:Biosample'
)

-- 4. Join biosample attributes and aggregate KO hit counts
SELECT bs.id                         AS biosample_id,
       bs.env_broad_scale_term_id,
       bs.env_medium_term_id,
       bs.geo_loc_name_has_raw_value,
       SUM(ko.n_hits)                AS total_ko_hits
FROM   run_to_biosample r2b
JOIN   nmdc_metadata.biosample_set bs
         ON bs.id = r2b.biosample_id
JOIN   (SELECT workflow_run_id, COUNT(*) AS n_hits
        FROM   nmdc_results.annotation_kegg_orthology
        WHERE  annotation_id = 'KO:K00001'
        GROUP  BY workflow_run_id) ko
         ON ko.workflow_run_id = r2b.workflow_run_id
GROUP  BY 1, 2, 3, 4
ORDER  BY total_ko_hits DESC
```

## Alternative approaches that don't require alldocs or the Linked Instances API

### Check whether recursion is needed at all

`WorkflowExecution.was_informed_by` already points directly to the
`DataGeneration` (NucleotideSequencing or MassSpectrometry), bypassing the
DataObject → WorkflowExecution → DataObject chain entirely. So the workflow
side of the bipartite graph is a single hop for any annotation workflow.

The only variable-depth problem is the **material processing chain** on the
other side: `DataGeneration.has_input` can point to a Biosample (`bsm-`), a
ProcessedSample (`procsm-`), or an OrganismSample (`osm-`). If all values in
the current BERDL data start with `bsm-`, no recursion is needed at all.

**Check this first before writing any recursive query:**

```sql
SELECT DISTINCT SUBSTRING(has_input, 1, INSTR(has_input, '-') - 1) AS id_prefix,
               COUNT(*) AS n
FROM nmdc_metadata.data_generation_set_has_input
GROUP BY 1
ORDER BY 2 DESC
```

If only `bsm-` appears, the fixed equi-join chain in `nmdc_metadata_tables.md`
is sufficient for all current data.

### Option A: Fixed-depth UNION for the material processing chain

No recursive CTE required. Unroll the material processing hops explicitly with
`UNION ALL`. Each additional `SELECT` handles one more hop. In practice NMDC
material processing chains are at most 3–4 steps deep.

This produces a `(data_generation_id, biosample_id)` mapping that can be used
as a replacement for `data_generation_set_has_input` when that table points to
`procsm-` or `osm-` IDs:

```sql
WITH dg_to_biosample AS (

  -- depth 0: DataGeneration.has_input → Biosample directly
  SELECT dhi.parent_id AS dg_id,
         dhi.has_input AS biosample_id,
         0             AS n_hops
  FROM   nmdc_metadata.data_generation_set_has_input dhi
  WHERE  dhi.has_input LIKE 'bsm-%'

  UNION ALL

  -- depth 1: DataGeneration → ProcessedSample → MaterialProcessing → Biosample
  SELECT dhi.parent_id,
         mpi.has_input,
         1
  FROM   nmdc_metadata.data_generation_set_has_input dhi
  JOIN   nmdc_metadata.material_processing_set_has_output mpho
           ON mpho.has_output = dhi.has_input
  JOIN   nmdc_metadata.material_processing_set_has_input mpi
           ON mpi.parent_id  = mpho.parent_id
  WHERE  mpi.has_input LIKE 'bsm-%'

  UNION ALL

  -- depth 2: one more material processing step back
  SELECT dhi.parent_id,
         mpi2.has_input,
         2
  FROM   nmdc_metadata.data_generation_set_has_input dhi
  JOIN   nmdc_metadata.material_processing_set_has_output mpho1
           ON mpho1.has_output = dhi.has_input
  JOIN   nmdc_metadata.material_processing_set_has_input mpi1
           ON mpi1.parent_id   = mpho1.parent_id
  JOIN   nmdc_metadata.material_processing_set_has_output mpho2
           ON mpho2.has_output = mpi1.has_input
  JOIN   nmdc_metadata.material_processing_set_has_input mpi2
           ON mpi2.parent_id   = mpho2.parent_id
  WHERE  mpi2.has_input LIKE 'bsm-%'
  -- add more UNION ALL blocks if deeper chains exist in the data

)
SELECT dg_id, biosample_id, MIN(n_hops) AS n_hops
FROM   dg_to_biosample
GROUP  BY 1, 2
```

Plug this CTE into the full annotation → biosample chain by replacing the
`data_generation_set_has_input` join with a join to `dg_to_biosample`.

### Option B: Precomputed `dg_to_biosample` Delta table

Run Option A once (or use DuckDB locally against the Silver Parquet exports,
where `WITH RECURSIVE` is fully supported), store the result as a small Delta
table `nmdc_metadata.dg_to_biosample`, and every subsequent Spark SQL query
is a plain equi-join. Needs refreshing when NMDC data is updated.

DuckDB against local Parquets:

```bash
duckdb -c "
COPY (
  WITH RECURSIVE dg_to_biosample(dg_id, id, depth) AS (
    SELECT parent_id, has_input, 0
    FROM   read_parquet('lakehouse/data_generation_set_has_input.parquet')
    UNION ALL
    SELECT t.dg_id, mpi.has_input, t.depth + 1
    FROM   dg_to_biosample t
    JOIN   read_parquet('lakehouse/material_processing_set_has_output.parquet') mpho
             ON mpho.has_output = t.id
    JOIN   read_parquet('lakehouse/material_processing_set_has_input.parquet') mpi
             ON mpi.parent_id   = mpho.parent_id
    WHERE  t.depth < 10
      AND  t.id NOT LIKE 'bsm-%'
  )
  SELECT dg_id, id AS biosample_id, MIN(depth) AS n_hops
  FROM   dg_to_biosample
  WHERE  id LIKE 'bsm-%'
  GROUP  BY 1, 2
) TO 'lakehouse/dg_to_biosample.parquet' (FORMAT PARQUET)
"
```

Upload `dg_to_biosample.parquet` to BERDL Bronze and register as a Delta
table. The recursive heavy lifting happens once outside Spark.

### Option C: `WITH RECURSIVE` in Trino on the four Silver side tables (recommended)

Trino supports `WITH RECURSIVE` and no new tables are needed. The four existing
Silver side tables form a complete edge set for upstream provenance traversal:

| Table | src col | dst col |
|---|---|---|
| `workflow_execution_set_was_informed_by` | `parent_id` | `was_informed_by` |
| `data_generation_set_has_input` | `parent_id` | `has_input` |
| `material_processing_set_has_output` | `has_output` | `parent_id` |
| `material_processing_set_has_input` | `parent_id` | `has_input` |

Tested against real NMDC data including 8-hop Pooling chains. See
[`trino_recursive_graph_traversal.md`](trino_recursive_graph_traversal.md)
for the full working code and the two-step pattern required to avoid
`TOO_MANY_REQUESTS_FAILED` on large annotation scans.

### Option D: `WITH RECURSIVE` on `nmdc_graph_edges` (requires table to be loaded)

A more general approach once `nmdc_graph_edges` is registered in `nmdc_metadata`
(issue #97). Not required for the biosample ↔ DataObject use case — Option C
using Trino and the existing Silver tables is sufficient.

---

## Why BERDL Claude's earlier attempts probably failed

Before `nmdc_graph_edges` existed in `nmdc_metadata`, recursive traversal
required joining against multiple polymorphic Silver tables at each hop
(`material_processing_set_has_input`, `data_generation_set_has_input`,
`workflow_execution_set_has_input`, etc.) with different column names and no
shared edge schema. A recursive CTE cannot switch join targets between
iterations — each iteration must be a structurally identical query. Without
a uniform edge table this is impossible to write generically.

The fix is `nmdc_graph_edges`: one table, one join, works for any entity type
at any hop depth.

## When you don't need recursion

If the data you are working with has NucleotideSequencing runs whose
`has_input` points directly to a Biosample (most metagenomics data in NMDC
today), the fixed-join pattern in `nmdc_metadata_tables.md` is simpler and
faster:

```
annotation.workflow_run_id
  → workflow_execution_set_was_informed_by
  → data_generation_set_has_input   (check: is has_input a bsm- ID or procsm-?)
  → biosample_set
```

Check the `has_input` prefix: `bsm-` = direct Biosample (no recursion needed),
`procsm-` = ProcessedSample (use recursive CTE or Option A above),
`osm-` = OrganismSample (same variable-depth problem as ProcessedSample).

## Depth limit guidance

| Path type | Typical depth | Recommended limit |
|---|---|---|
| Biosample → raw DataObject (no processing) | 2 | 5 |
| Biosample → annotation DataObject (metagenomics) | 5–7 | 10 |
| Biosample → DataObject (complex MS sample prep) | 8–12 | 15 |

Set the limit conservatively high rather than tight. The depth guard only
prevents runaway queries on unexpected cycles; it does not affect correctness
for well-formed data.

---

## Precomputed closure for remote and API access

### Why the Trino solution doesn't work remotely

The Trino two-step pattern (`trino_recursive_graph_traversal.md`) requires
Python orchestration: step 1 returns workflow run IDs to a Python variable,
which step 2 injects as `VALUES` literals into a new query. A remote caller
using the BERDL MCP REST API (`/delta/tables/query`) cannot hold state between
calls. Additionally:

- The MCP API routes queries through Spark, not Trino — `WITH RECURSIVE` fails
  with the same `No plan for UnionLoop` bug.
- Injecting thousands of IDs as `VALUES` literals does not scale and may exceed
  query size limits.

### The solution: `workflow_to_biosample` Delta table

Precompute the full workflow-run → biosample mapping once and store it as a
small Delta table in `nmdc_metadata`. Remote and API users then get the linkage
with a plain equi-join that works through any query interface — Spark, Trino,
MCP REST, or DuckDB against exported Parquet.

**Generate with the Trino recursive CTE** (run once in a BERDL notebook,
refresh whenever NMDC data is updated):

```python
from berdl_notebook_utils.setup_trino_session import get_trino_connection
conn = get_trino_connection()

_EDGES = """
    SELECT parent_id AS src, was_informed_by AS next_id
    FROM   nmdc_metadata.workflow_execution_set_was_informed_by
    UNION ALL
    SELECT parent_id AS src, has_input       AS next_id
    FROM   nmdc_metadata.data_generation_set_has_input
    UNION ALL
    SELECT has_output AS src, parent_id      AS next_id
    FROM   nmdc_metadata.material_processing_set_has_output
    UNION ALL
    SELECT parent_id AS src, has_input       AS next_id
    FROM   nmdc_metadata.material_processing_set_has_input
"""

cur = conn.cursor()
cur.execute(f"""
    WITH RECURSIVE upstream(origin, id, depth) AS (
        SELECT CAST(we.id AS VARCHAR), CAST(we.id AS VARCHAR), CAST(0 AS BIGINT)
        FROM   nmdc_metadata.workflow_execution_set we
        UNION ALL
        SELECT u.origin, e.next_id, u.depth + 1
        FROM   upstream u
        JOIN   ({_EDGES}) e ON e.src = u.id
        WHERE  u.depth < 15
          AND  u.id NOT LIKE 'nmdc:bsm%'
    )
    SELECT DISTINCT origin AS workflow_run_id,
                    id     AS biosample_id,
                    depth  AS n_hops
    FROM   upstream
    WHERE  id LIKE 'nmdc:bsm%'
""")
import pandas as pd
w2b = pd.DataFrame(cur.fetchall(), columns=["workflow_run_id", "biosample_id", "n_hops"])
```

Write to Parquet, upload to Bronze MinIO, and register as a Delta table in
`nmdc_metadata` following the same pattern as other ingest notebooks.

**Once loaded, any query interface can join on it:**

```sql
-- Works via MCP REST API, Spark, Trino, or DuckDB — no recursion needed
SELECT ko.annotation_id,
       w2b.biosample_id,
       w2b.n_hops,
       bs.env_broad_scale_term_id,
       bs.geo_loc_name_has_raw_value,
       COUNT(*) AS n_hits
FROM   nmdc_results.annotation_kegg_orthology ko
JOIN   nmdc_metadata.workflow_to_biosample w2b
         ON w2b.workflow_run_id = ko.workflow_run_id
JOIN   nmdc_metadata.biosample_set bs
         ON bs.id = w2b.biosample_id
WHERE  ko.annotation_id = 'KO:K00001'
GROUP  BY 1, 2, 3, 4, 5
ORDER  BY n_hits DESC
```

### Refresh strategy

`workflow_to_biosample` is a derived table — it must be rebuilt when the
underlying Silver tables change. NMDC data is loaded in batches (not
continuously), so a manual rebuild after each load cycle is sufficient for now.
A Dagster or notebook-triggered refresh can be added later.

The table is small: one row per (workflow run, biosample) pair. With ~30K
workflow runs and typically 1–3 biosamples each, the table will be well under
100K rows and fast to regenerate.

---

## Fully-normalized biosample_to_gene_entity table

### Motivation

`workflow_to_biosample` records (workflow_run_id, biosample_id, n_hops) but
drops provenance detail. For scientific use — answering "which slots connect
this biosample to that annotation workflow?" — a richer precomputed table is
more useful.

`biosample_to_gene_entity` adds a `slot_path` column: a dot-separated list of
schema slot names traversed from the **annotation workflow** back to the
**biosample**, written in forward (biosample → workflow) reading order.

### Table schema

| column | type | description |
|---|---|---|
| `biosample_id` | string | `nmdc:bsm-*` identifier |
| `entity_id` | string | WorkflowExecution `id` (the annotation workflow run) |
| `entity_type` | string | e.g. `nmdc:MetagenomeAnnotation` |
| `slot_path` | string | dot-separated slot names, biosample-to-entity order |
| `n_hops` | int | number of edges in the path |

**`slot_path` semantics:** each element names the schema slot traversed in the
upstream direction from the annotation workflow to the biosample. Because the
traversal walks *upstream* (workflow → biosample) but the path should read
*downstream* (biosample → workflow), each new slot name is **prepended** to the
accumulator during recursion.

**Example rows:**

| biosample_id | entity_id | entity_type | slot_path | n_hops |
|---|---|---|---|---|
| `nmdc:bsm-11-abc` | `nmdc:wfmgan-11-xyz` | `nmdc:MetagenomeAnnotation` | `has_input.was_informed_by` | 2 |
| `nmdc:bsm-11-def` | `nmdc:wfmgan-11-xyz` | `nmdc:MetagenomeAnnotation` | `has_input.has_output.has_input.was_informed_by` | 4 |

Row 1: `DataGeneration.has_input → Biosample` (direct biosample input).
Row 2: `MaterialProcessing.has_input → Biosample` via a ProcessedSample
intermediate (material transformation chain added 2 more hops).

### Trino CTE to generate it

Start from annotation workflow runs only (filter by type), walk upstream
accumulating slot names, stop when a `bsm-` node is reached.

```python
from berdl_notebook_utils.setup_trino_session import get_trino_connection
import pandas as pd

conn = get_trino_connection()
cur = conn.cursor()

# Step 1: collect annotation workflow run IDs and types
cur.execute("""
    SELECT id, type
    FROM   nmdc_metadata.workflow_execution_set
    WHERE  type IN (
        'nmdc:MetagenomeAnnotation',
        'nmdc:MetaproteomicsAnalysis',
        'nmdc:MetabolomicsAnalysis',
        'nmdc:NOMAnalysis',
        'nmdc:MetagenomeAssembly',
        'nmdc:MetatranscriptomeAnnotation'
    )
""")
annotation_workflows = pd.DataFrame(cur.fetchall(), columns=["id", "type"])
ids_csv = ", ".join(f"'{r}'" for r in annotation_workflows["id"])

# Step 2: recursive upstream walk with slot_path accumulation
# Each edge prepends the slot name so the final path reads biosample → workflow
cur.execute(f"""
    WITH RECURSIVE upstream(origin, origin_type, id, slot_path, depth) AS (
        -- seed: annotation workflows, path starts empty
        SELECT CAST(id AS VARCHAR),
               CAST(type AS VARCHAR),
               CAST(id AS VARCHAR),
               CAST('' AS VARCHAR),
               CAST(0 AS BIGINT)
        FROM   nmdc_metadata.workflow_execution_set
        WHERE  id IN ({ids_csv})

        UNION ALL

        -- hop via was_informed_by (WorkflowExecution → DataGeneration)
        SELECT u.origin,
               u.origin_type,
               CAST(wib.was_informed_by AS VARCHAR),
               CAST(
                 CASE WHEN u.slot_path = '' THEN 'was_informed_by'
                      ELSE 'was_informed_by.' || u.slot_path END
               AS VARCHAR),
               u.depth + 1
        FROM   upstream u
        JOIN   nmdc_metadata.workflow_execution_set_was_informed_by wib
                 ON wib.parent_id = u.id
        WHERE  u.depth < 15
          AND  u.id NOT LIKE 'nmdc:bsm%'

        UNION ALL

        -- hop via has_input (DataGeneration → Biosample or ProcessedSample)
        SELECT u.origin,
               u.origin_type,
               CAST(dgi.has_input AS VARCHAR),
               CAST(
                 CASE WHEN u.slot_path = '' THEN 'has_input'
                      ELSE 'has_input.' || u.slot_path END
               AS VARCHAR),
               u.depth + 1
        FROM   upstream u
        JOIN   nmdc_metadata.data_generation_set_has_input dgi
                 ON dgi.parent_id = u.id
        WHERE  u.depth < 15
          AND  u.id NOT LIKE 'nmdc:bsm%'

        UNION ALL

        -- hop via has_output reversed (ProcessedSample → MaterialProcessing)
        SELECT u.origin,
               u.origin_type,
               CAST(mpo.parent_id AS VARCHAR),
               CAST(
                 CASE WHEN u.slot_path = '' THEN 'has_output'
                      ELSE 'has_output.' || u.slot_path END
               AS VARCHAR),
               u.depth + 1
        FROM   upstream u
        JOIN   nmdc_metadata.material_processing_set_has_output mpo
                 ON mpo.has_output = u.id
        WHERE  u.depth < 15
          AND  u.id NOT LIKE 'nmdc:bsm%'

        UNION ALL

        -- hop via has_input (MaterialProcessing → Biosample or ProcessedSample)
        SELECT u.origin,
               u.origin_type,
               CAST(mpi.has_input AS VARCHAR),
               CAST(
                 CASE WHEN u.slot_path = '' THEN 'has_input'
                      ELSE 'has_input.' || u.slot_path END
               AS VARCHAR),
               u.depth + 1
        FROM   upstream u
        JOIN   nmdc_metadata.material_processing_set_has_input mpi
                 ON mpi.parent_id = u.id
        WHERE  u.depth < 15
          AND  u.id NOT LIKE 'nmdc:bsm%'
    )
    SELECT DISTINCT
        id            AS biosample_id,
        origin        AS entity_id,
        origin_type   AS entity_type,
        slot_path,
        depth         AS n_hops
    FROM   upstream
    WHERE  id LIKE 'nmdc:bsm%'
""")

b2ge = pd.DataFrame(cur.fetchall(),
                    columns=["biosample_id", "entity_id", "entity_type",
                             "slot_path", "n_hops"])
```

Write `b2ge` to Parquet, upload to Bronze MinIO, and register as
`nmdc_metadata.biosample_to_gene_entity` following the standard ingest pattern.

### Using the table in a science query

```sql
-- Genes (KO hits) grouped by biosample ecosystem — works via any query interface
SELECT ko.annotation_id,
       b2ge.slot_path,
       bs.env_broad_scale_term_id,
       bs.geo_loc_name_has_raw_value,
       COUNT(*) AS n_hits
FROM   nmdc_results.annotation_kegg_orthology ko
JOIN   nmdc_metadata.biosample_to_gene_entity b2ge
         ON b2ge.entity_id = ko.workflow_run_id
JOIN   nmdc_metadata.biosample_set bs
         ON bs.id = b2ge.biosample_id
WHERE  ko.annotation_id = 'KO:K00001'
GROUP  BY 1, 2, 3, 4
ORDER  BY n_hits DESC
```

The `slot_path` column lets you filter to specific traversal shapes:

```sql
-- Only biosamples that fed directly into DataGeneration (no material processing)
WHERE  b2ge.slot_path = 'has_input.was_informed_by'

-- Only biosamples that went through at least one material processing step
WHERE  b2ge.slot_path LIKE 'has_input.has_output.%'
```

### Relationship to workflow_to_biosample

`workflow_to_biosample` is the simpler version — no `slot_path`, covers all
workflow types. Use it when you don't need provenance detail or when joining
across all workflow types at once. `biosample_to_gene_entity` is scoped to
annotation workflows and adds the slot-path column for queries that care about
the shape of the processing chain.
