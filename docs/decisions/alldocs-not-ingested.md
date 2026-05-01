# Decision: alldocs not ingested into the lakehouse

**Date:** 2026-05-01
**Status:** Decided
**Context:** Issue [#97](https://github.com/microbiomedata/nmdc-lakehouse/issues/97), PR #98 (closed without merge)

## Decision

The `alldocs` MongoDB collection maintained by nmdc-runtime will **not** be
ingested into BERDL Silver Delta tables. Graph traversal queries
(biosample → annotation, annotation → biosample, etc.) are served by a derived
table built from the existing schema-driven Silver side tables —
see [`biosample_to_workflow_run.md`](../biosample_to_workflow_run.md).

## What alldocs is

A runtime-maintained MongoDB collection (~368K documents on production) that
flattens every NMDC entity into one row carrying `_upstream` / `_downstream`
arrays of immediate graph neighbours. The Linked Instances API (`$graphLookup`)
uses it for transitive traversal.

## Why we considered it

A flat union of every entity with adjacency arrays looks attractive for a
lakehouse: one ingestion job, one wide table, "all" graph edges in one place.
The original plan (PR #98) was to flatten alldocs into three Silver Delta
tables — `nmdc_graph_nodes`, `nmdc_graph_nodes_type_ancestors`,
`nmdc_graph_edges` — and use them for arbitrary multi-hop queries.

## Why we rejected it

### 1. Undefined upstream/downstream semantics

`_upstream` and `_downstream` are computed by `nmdc-runtime` from a
domain-curated, hand-coded list of slots in `ops.py` — not from any property
in the NMDC schema. Whether a given relationship counts as "upstream" or
"downstream" is a runtime convention that was never formally specified or
versioned. `Study.associated_studies` ends up as upstream by convention, with
no way for a downstream consumer to verify what would happen if the convention
changed. Building lakehouse tables on top of this couples our schema to a
runtime implementation detail.

### 2. Edge collapse: many distinct slots become one undifferentiated edge

Every relationship between two entities, regardless of which schema slot it
travels through (`has_input`, `has_output`, `was_informed_by`,
`associated_studies`, `uses_calibration`, etc.), is reduced to a single edge
in `_downstream` / `_upstream`. The slot name does not survive. Two entities
linked by `has_input` are indistinguishable from two entities linked by
`associated_studies` once they're in the alldocs adjacency arrays.

This makes it impossible to distinguish material-flow edges from
metadata-association edges without consulting the entity types on both ends
and re-deriving the slot via schema knowledge — at which point we're better
off using the typed Silver side tables directly.

### 3. Multi-hop traversal still requires recursion

alldocs is a 1-hop adjacency index, not a transitive closure. A biosample →
annotation query still needs `WITH RECURSIVE` (or equivalent iterative walk).
The Linked Instances API hides this by running `$graphLookup` server-side,
but the underlying collection does not pre-materialize the closure. Putting
alldocs in Silver does not buy us multi-hop traversal — it just gives us a
larger, less-typed edge table than we already have.

## What we use instead

Four schema-driven Silver side tables already in `nmdc_metadata`:

- `workflow_execution_set_was_informed_by`
- `data_generation_set_has_input`
- `material_processing_set_has_output`
- `material_processing_set_has_input`

These preserve the slot name (each is named for the slot it materializes),
preserve the source class (each carries a `parent_id` typed to a known
collection), and together cover every edge needed to walk from a Biosample
to any WorkflowExecution and back.

A precomputed Delta table — `nmdc_metadata.biosample_to_workflow_run` —
collapses the variable-depth bipartite chain into one row per
(biosample, workflow run) pair, with boolean flags recording which
MaterialProcessing classes appeared in the path. This table works through
any query interface (Spark, Trino, REST API) without recursion at the
consumer side.

## Consequences

- No `nmdc_graph_*` tables in `nmdc_metadata`.
- No `alldocs_to_parquet.py` ingestion job.
- No local LinkML schema fragment for runtime-only collections (the
  `nmdc_runtime_collections.yaml` file proposed in PR #98 is not added).
- Use cases beyond biosample ↔ workflow run linkage that require full graph
  traversal across non-standard slots (e.g., calibration chains, study
  associations) will need their own targeted tables rather than relying on a
  generic edge index.

## References

- `nmdc-runtime` slot direction logic: `nmdc_runtime/api/endpoints/lib/ops.py`
- Linked Instances API: `nmdc_runtime/api/endpoints/lib/linked_instances.py`
- Replacement: see [`docs/biosample_to_workflow_run.md`](../biosample_to_workflow_run.md)
