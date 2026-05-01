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
- `centrifuge_output_report_file` — Centrifuge per-(taxon, workflow_run) summary report with fields `numReads`, `abundance`, etc. (see `peek_read_taxonomy_links.ipynb`)
- `annotation_statistics` — 17 per-run QC metrics: sequence counts/lengths, gene-type counts (CDS, tRNA, ncRNA, rRNA, CRISPR), coding density, genes-per-Mbp (see `peek_ko_ec_links.ipynb` §9)

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

For the most common multi-hop case — Biosample to / from any WorkflowExecution
that produced annotations, taxonomy, MAGs, or other results — use the
precomputed table `nmdc_metadata.biosample_to_workflow_run`. See
[`biosample_to_workflow_run.md`](biosample_to_workflow_run.md). It collapses the
variable-depth bipartite chain (Biosample → MaterialProcessing → ProcessedSample
→ DataGeneration → WorkflowExecution) into one row per (biosample, workflow run)
pair and works through any query interface (Spark, Trino, REST API) with a
plain equi-join.

Ingesting the runtime-maintained `alldocs` MongoDB collection was considered
and rejected — see [`decisions/alldocs-not-ingested.md`](decisions/alldocs-not-ingested.md).

## Loading a new data product into `nmdc_results`

If your task is to add a Silver table for a data product the loaders already
support but BERDL hasn't ingested yet (e.g. Centrifuge per issue #94, GTDBTK
Archaeal per issue #95), use the existing on-pod two-phase pattern. The
ingest notebook's default behavior is now agent-safe — it auto-discovers
parquets in `SOURCE_DIR` and skips any whose stem is already registered in
`nmdc_results`. Still, scope the **fetch** to only the missing types so you
don't re-parse parquets you don't need.

**Step 1 — preflight: list what's already in `nmdc_results`**

```python
existing = sorted(r.tableName for r in spark.sql("SHOW TABLES IN nmdc_results").collect())
print(existing)
```

If your target table is already there, stop — there is nothing to load.

**Step 2 — fetch only the missing types**

`fetch_taxonomy_summaries.ipynb` honors the `TAXONOMY_TYPES` env var
(comma-separated, exact match against entries in `_DEFAULT_TARGET_TYPES`):

```bash
export TAXONOMY_TYPES="Centrifuge output report file"
```

The on-disk raw cache (`loaded_taxonomy/raw_cache/`) means re-running with the
full default list is recoverable but wasteful — narrow the scope.

**Step 3 — ingest with the safety net engaged**

`ingest_taxonomy_summaries.ipynb` auto-discovers `*.parquet` files under
`SOURCE_DIR` and skips any whose stem already appears in
`SHOW TABLES IN nmdc_results`. To intentionally re-overwrite a specific
table, add its name to `FORCE_OVERWRITE` in the configuration cell:

```python
FORCE_OVERWRITE = {"gtdbtk_bacterial_summary"}
```

The default empty set is the agent-safe default.

**Step 4 — verify**

Re-run the preflight from Step 1 and confirm the new table is present.

For other on-pod loaders (`fetch_ko_ec_annotations.ipynb` /
`ingest_ko_ec_annotations.ipynb`), see [`FETCH_TAXONOMY_NOTES.md`](../notebooks/FETCH_TAXONOMY_NOTES.md)
for the full set of gotchas (placeholder files, duplicate URLs, broken
upstream URLs, kernel staleness).

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

## Other BERDL namespaces with NMDC-relevant data

`nmdc_arkin` is maintained by the Arkin group and is queryable via `spark.sql()` like any other registered namespace. **Do not write into it.** It contains:

- `annotation_terms_unified` — unified term lookup across GO (48K terms, names populated), EC (8.8K, populated), MetaCyc (1.5K, populated), COG (26, populated), and KEGG KO/Module/Pathway (names **not** populated — see below)
- `go_terms`, `ec_terms`, `cog_hierarchy_flat`, `metacyc_pathways` — source-specific term tables, all with names populated
- `study_table`, `omics_files_table`, `sample_file_lookup` — Arkin-curated NMDC study and file metadata
- Taxonomy tables: `centrifuge_gold`, `gottcha_gold`, `kraken_gold`, `taxonomy_dim`
- Omics result tables: `nom_gold`, `metabolomics_gold`, `proteomics_gold`, `metatranscriptomics_gold`, `lipidomics_gold`
- Embeddings: `abiotic_embeddings`, `taxonomy_embeddings`, `biochemical_embeddings`, `unified_embeddings`, and others

To look up GO or EC names for annotation IDs:
```sql
SELECT a.annotation_id, t.name, t.definition
FROM nmdc_results.annotation_enzyme_commission a
JOIN nmdc_arkin.ec_terms t ON t.ec_id = REPLACE(a.annotation_id, 'EC:', '')
LIMIT 10
```

## Known gaps

**KEGG term names are unavailable.** `nmdc_arkin.kegg_ko_terms` and the `kegg_ko` rows in `annotation_terms_unified` have IDs but empty `name`/`description` fields. KEGG's [redistribution license](https://www.kegg.jp/kegg/legal.html) prohibits republishing term names. Queries against `annotation_kegg_orthology` return bare `KO:Kxxxxx` identifiers only. If human-readable names are needed, hit the KEGG API at query time (subject to rate limiting). Do not write into `nmdc_arkin`.

## KO prefix translation (annotation tables vs functional_annotation_agg)

The `functional_annotation_agg` table (also in `nmdc_metadata`) uses
`KEGG.ORTHOLOGY:K00001` while annotation tables use `KO:K00001`.
To cross-check counts:

```sql
'KEGG.ORTHOLOGY:' || SUBSTRING(annotation_id, 4)  AS faa_gene_function_id
```

EC terms have no equivalent in `functional_annotation_agg` — only in
`annotation_enzyme_commission`.
