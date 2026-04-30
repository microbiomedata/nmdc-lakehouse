# Ingesting `alldocs` into Silver Delta Lake tables (issue #97)

## What `alldocs` is

`alldocs` is a MongoDB collection maintained by nmdc-runtime (~360 K documents).
It is a precomputed provenance graph: every NMDC entity has one document, and
that document carries the entity's immediate graph neighbors across all
collection types. It is used by the nmdc-runtime API for linked-record lookups.

**It is not part of the nmdc-schema `Database` class** — it is a runtime artifact,
not a submission or workflow record. That is why `nmdc-lakehouse`'s schema-driven
ETL does not pick it up automatically.

## Why we are loading it

The Silver schema side tables (`workflow_execution_set_was_informed_by`,
`data_generation_set_has_input`, etc.) handle the most common joins, but they
require knowing the collection sequence in advance. `alldocs` enables:

- Multi-hop traversal where intermediate collection types are unknown
  (e.g., MAGsAnalysis → MetagenomeAssembly → NucleotideSequencing → Biosample)
- Type-hierarchy-aware queries via the full LinkML ancestor chain
- A single join surface for polymorphic graph problems

## Silver tables being added

All three tables land in `nmdc_metadata`.

### `nmdc_graph_nodes`

One row per NMDC entity.

| Column | Type | Source |
|---|---|---|
| `id` | string | `alldocs.id` |
| `type` | string | `alldocs.type` |

### `nmdc_graph_nodes_type_ancestors` (side table)

One row per (entity, ancestor type). Enables `type_ancestor = 'nmdc:WorkflowExecution'`
filters without pattern-matching on `type`.

| Column | Type | Source |
|---|---|---|
| `parent_id` | string | `alldocs.id` |
| `type_ancestor` | string | each element of `alldocs._type_and_ancestors` |

### `nmdc_graph_edges`

One directed edge per row. Built from `_downstream` arrays only — each edge
appears exactly once (src precedes dst in the processing chain).

| Column | Type | Source |
|---|---|---|
| `src_id` | string | `alldocs.id` |
| `src_type` | string | `alldocs.type` |
| `dst_id` | string | each `_downstream[].id` |
| `dst_type` | string | each `_downstream[].type` |

**Note:** The MongoDB field names `_upstream` and `_downstream` do not appear
anywhere in the Silver tables. Edges are derived from `_downstream` to avoid
storing each edge twice.

## Implementation approach

- A local LinkML schema fragment (`schema/nmdc_runtime_collections.yaml`) defines
  `NmdcGraphNode` and `NmdcGraphEdge` so the ETL can derive a PyArrow schema
  the same way it does for other collections.
- A new job class `AlldocsToParquetJob`
  (`src/nmdc_lakehouse/jobs/alldocs_to_parquet.py`) follows the
  `DirectMongoToParquetJob` pattern: raw pymongo cursor, no linkml-store,
  streams 50 K docs per batch.
- The job writes three Parquet files, which are then registered as Delta tables
  in `nmdc_metadata` by the BERDL ingest notebook.

## What to do in the meantime

For annotation → biosample / study joins, use the existing Silver side tables.
The join chain is fully documented in [`nmdc_metadata_tables.md`](nmdc_metadata_tables.md)
and a working Spark SQL example is in [`for_berdl_claude.md`](for_berdl_claude.md).

No `LATERAL VIEW EXPLODE` is needed — `workflow_execution_set_was_informed_by`
is already a flat side table.
