# Graph traversal via Trino WITH RECURSIVE (no alldocs required)

## Summary

Spark 4.0.1 cannot execute `WITH RECURSIVE` in either Spark Connect or classic
standalone mode — both fail with `No plan for UnionLoop` (a Spark planner bug).

Trino, which is available in every BERDL notebook via `get_trino_connection()`,
supports `WITH RECURSIVE` natively and reads the same Delta tables from the Hive
metastore. The annotation → biosample walk can be done in a single Trino query
using four Silver side tables that are already loaded.

## The four edge tables

These cover the full upstream provenance walk from a workflow run to its source
biosamples, regardless of chain depth:

| Table | src col | dst col | edge meaning |
|---|---|---|---|
| `workflow_execution_set_was_informed_by` | `parent_id` | `was_informed_by` | workflow run → data generation |
| `data_generation_set_has_input` | `parent_id` | `has_input` | data generation → biosample or procsm |
| `material_processing_set_has_output` | `has_output` | `parent_id` | procsm → the processing that produced it |
| `material_processing_set_has_input` | `parent_id` | `has_input` | processing → its input (biosample or procsm) |

## Helper pattern (used in peek_ko_ec_links.ipynb)

```python
from berdl_notebook_utils.setup_trino_session import get_trino_connection
import pandas as pd

conn = get_trino_connection()

_EDGES = """
    SELECT parent_id  AS src, was_informed_by AS next_id
    FROM   nmdc_metadata.workflow_execution_set_was_informed_by
    UNION ALL
    SELECT parent_id  AS src, has_input       AS next_id
    FROM   nmdc_metadata.data_generation_set_has_input
    UNION ALL
    SELECT has_output AS src, parent_id       AS next_id
    FROM   nmdc_metadata.material_processing_set_has_output
    UNION ALL
    SELECT parent_id  AS src, has_input       AS next_id
    FROM   nmdc_metadata.material_processing_set_has_input
"""

def workflows_to_biosamples_bulk(conn, workflow_run_ids, max_depth=15):
    ids = list(set(workflow_run_ids))
    values = ",\n        ".join(f"('{w}')" for w in ids)
    cur = conn.cursor()
    cur.execute(f"""
        WITH RECURSIVE upstream(origin, id, depth) AS (
            SELECT CAST(id AS VARCHAR), CAST(id AS VARCHAR), CAST(0 AS BIGINT)
            FROM (VALUES {values}) AS t(id)
            UNION ALL
            SELECT u.origin, e.next_id, u.depth + 1
            FROM   upstream u
            JOIN   ({_EDGES}) e ON e.src = u.id
            WHERE  u.depth < {max_depth}
              AND  u.id NOT LIKE 'nmdc:bsm%'
        )
        SELECT DISTINCT origin AS workflow_run_id, id AS biosample_id
        FROM   upstream
        WHERE  id LIKE 'nmdc:bsm%'
    """)
    rows = cur.fetchall()
    return pd.DataFrame(rows or [], columns=["workflow_run_id", "biosample_id"])
```

## End-to-end: KO hits per biosample (two-step approach)

Combining the annotation table scan and the recursive walk into a single
`WITH RECURSIVE` query causes `TOO_MANY_REQUESTS_FAILED` — the Trino worker
node crashes under the combined load. Split into two steps instead:

```python
import time, pandas as pd

TARGET_KO = 'KO:K00001'

# Step 1: flat scan — annotation table only, no recursion
t0 = time.monotonic()
cur = conn.cursor()
cur.execute(f"""
    SELECT workflow_run_id, COUNT(*) AS n_hits
    FROM   nmdc_results.annotation_kegg_orthology
    WHERE  annotation_id = '{TARGET_KO}'
    GROUP  BY workflow_run_id
""")
ko_hits = pd.DataFrame(cur.fetchall(), columns=["workflow_run_id", "n_hits"])
print(f"Step 1: {len(ko_hits)} workflow runs  ({time.monotonic() - t0:.1f}s)")

# Step 2: recursive walk those run IDs to biosamples, then join metadata
t1 = time.monotonic()
r2b = workflows_to_biosamples_bulk(conn, ko_hits["workflow_run_id"].tolist())

bsm_ids = r2b["biosample_id"].unique().tolist()
ids_sql = ", ".join(f"'{i}'" for i in bsm_ids)
cur.execute(f"""
    SELECT id AS biosample_id, env_broad_scale_term_id, env_medium_term_id,
           geo_loc_name_has_raw_value
    FROM   nmdc_metadata.biosample_set
    WHERE  id IN ({ids_sql})
""")
bsm_meta = pd.DataFrame(cur.fetchall(), columns=["biosample_id", "env_broad_scale_term_id",
                                                   "env_medium_term_id", "geo_loc_name_has_raw_value"])

result = (
    r2b.merge(ko_hits, on="workflow_run_id")
       .merge(bsm_meta, on="biosample_id")
       .groupby(["biosample_id", "env_broad_scale_term_id",
                 "env_medium_term_id", "geo_loc_name_has_raw_value"], as_index=False)["n_hits"]
       .sum()
       .rename(columns={"n_hits": "total_hits"})
       .sort_values("total_hits", ascending=False)
       .head(20)
       .reset_index(drop=True)
)
print(f"Total elapsed: {time.monotonic() - t0:.1f}s")
```

## Trino WITH RECURSIVE syntax rules

1. `RECURSIVE` goes on `WITH`, not on individual CTE names:
   `WITH RECURSIVE cte(cols) AS (...)` ✓  
   `WITH ... RECURSIVE cte AS (...)` ✗

2. Every CTE in a `WITH RECURSIVE` block needs explicit column aliases,
   not just the recursive one.

3. `VALUES` literals infer `varchar(N)` from the literal length. Cast the
   anchor columns: `CAST(id AS VARCHAR)` to get unbounded varchar, which
   is required for the recursive step to type-check.

## Spark WITH RECURSIVE status

Both transport modes fail with the same root cause in Spark 4.0.1:

```
assertion failed: No plan for UnionLoop 0
```

- **Spark Connect** (`get_spark_session()` default): surfaces as `No plan for UnionLoop`
- **Classic standalone** (`get_spark_session(use_spark_connect=False)`): surfaces as
  `INTERNAL_ERROR: transpose requires all collections have the same size`,
  which wraps the same underlying assertion

The classic Spark master (`spark://spark-master-mamillerpa.jupyterhub-prod:7077`)
is reachable from the pod (confirmed via socket), but switching to it does not help.

## Tested chain depths

| Example | Depth | Result |
|---|---|---|
| `nmdc:wfmgan-11-e42fem70.1` | 2 hops | 1 biosample (direct) |
| `nmdc:wfmgan-11-4h48ff64.1` | 8 hops | 3 biosamples (Pooling chain) |
| `nmdc:wfmgan-11-kepa2m52.1` | 8 hops | 3 biosamples |
