# Guide for BERDL Claude instances

This document is written for Claude Code agents running inside the BERDL
JupyterHub environment, working with `nmdc_metadata` and `nmdc_results`.

## What is available right now

Two Spark databases are registered:

**`nmdc_metadata`** — schema-driven Silver tables, one per NMDC MongoDB
collection plus side tables for multivalued slots. See
[`nmdc_metadata_tables.md`](nmdc_metadata_tables.md) for the full table list
and join patterns.

**`nmdc_results`** — per-gene annotation tables loaded from NERSC workflow
output files:
- `annotation_kegg_orthology` — one row per (gene, KO term)
- `annotation_enzyme_commission` — one row per (gene, EC number)
- `taxonomy_centrifuge_classification` — Centrifuge per-read classifications (if loaded)

## Anchor columns on annotation tables

Every row in `annotation_kegg_orthology` and `annotation_enzyme_commission` has:

| Column | Join target |
|---|---|
| `workflow_run_id` | `nmdc_metadata.workflow_execution_set_was_informed_by.parent_id` |
| `data_object_id` | `nmdc_metadata.data_object_set.id` |
| `gene_id` | local identifier; no Silver table joins on this column yet |
| `ncbi_taxid` | external; no BERDL taxonomy dimension table yet |
| `annotation_id` | `KO:Kxxxxx` or `EC:n.n.n.-` |

## Standard join: annotation row → biosample

```sql
SELECT ko.annotation_id,
       bs.id            AS biosample_id,
       bs.env_broad_scale_term_id,
       bs.geo_loc_name_has_raw_value,
       s.id             AS study_id,
       s.name           AS study_name
FROM nmdc_results.annotation_kegg_orthology ko
JOIN nmdc_metadata.workflow_execution_set_was_informed_by wib
  ON wib.parent_id = ko.workflow_run_id
JOIN nmdc_metadata.data_generation_set_has_input dhi
  ON dhi.parent_id = wib.was_informed_by
JOIN nmdc_metadata.biosample_set bs
  ON bs.id = dhi.has_input
JOIN nmdc_metadata.data_generation_set_associated_studies dgs
  ON dgs.parent_id = wib.was_informed_by
JOIN nmdc_metadata.study_set s
  ON s.id = dgs.associated_studies
WHERE ko.annotation_id = 'KO:K00001'
LIMIT 100
```

**No `LATERAL VIEW EXPLODE` is needed.** `workflow_execution_set_was_informed_by`
is a side table with one row per (workflow run, data generation) pair — it is
already flat.

The same pattern works for EC: replace `annotation_kegg_orthology` with
`annotation_enzyme_commission` and filter on `EC:` prefixed `annotation_id` values.

## The bipartite / polymorphic graph problem

NMDC metadata is a typed, directed provenance graph. The Silver schema tables
handle the most common traversals through explicit side tables. For paths that
cross multiple collection types or require arbitrary-depth traversal, the side
tables require knowing the sequence of hops in advance.

A `alldocs`-derived graph table is being built (issue #97) that will provide a
general edge table (`nmdc_graph_edges`) covering all entity types. Until it
lands, use the known join chains in `nmdc_metadata_tables.md`.

## Preflight check

Before running annotation queries, verify the tables are registered:

```python
for tbl in ("annotation_kegg_orthology", "annotation_enzyme_commission"):
    n = spark.sql(f"SHOW TABLES IN nmdc_results LIKE '{tbl}'").count()
    print(f"{'OK' if n else 'MISSING'}: nmdc_results.{tbl}")

for tbl in ("workflow_execution_set_was_informed_by",
            "data_generation_set_has_input",
            "data_generation_set_associated_studies",
            "biosample_set", "study_set"):
    n = spark.sql(f"SHOW TABLES IN nmdc_metadata LIKE '{tbl}'").count()
    print(f"{'OK' if n else 'MISSING'}: nmdc_metadata.{tbl}")
```

## KO prefix translation (annotation tables vs functional_annotation_agg)

The `functional_annotation_agg` table (also in `nmdc_metadata`) uses
`KEGG.ORTHOLOGY:K00001` while annotation tables use `KO:K00001`.
To cross-check counts:

```sql
'KEGG.ORTHOLOGY:' || SUBSTRING(annotation_id, 4)  AS faa_gene_function_id
```

EC terms have no equivalent in `functional_annotation_agg` — only in
`annotation_enzyme_commission`.
