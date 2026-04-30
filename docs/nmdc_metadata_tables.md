# `nmdc_metadata` Silver table reference

`nmdc_metadata` in BERDL contains the 17 schema-defined NMDC MongoDB collections,
flattened to Parquet and registered as Delta Lake tables. This document describes
the naming conventions, the side-table pattern, and the key join chains.

## Naming conventions

### Primary tables

One table per schema-defined MongoDB collection, named identically:
`biosample_set`, `data_generation_set`, `workflow_execution_set`, etc.

### Side tables

Every multivalued slot that holds references to other NMDC objects or inlined
sub-objects is flattened to a separate side table named:

```
{collection}_{slot_name}
```

Each side table has a `parent_id` column that is a foreign key back to the
`id` of the primary table row, plus one or more columns for the slot values.
Only slots with at least one populated record produce a side table.

Example: `data_generation_set_has_input` has columns `parent_id` and `has_input`.
Joining `parent_id = data_generation_set.id` gives you the biosample IDs for a
sequencing run.

**Do not use `LATERAL VIEW EXPLODE` on primary table array columns when a side
table exists — the side table is the correct relational form and avoids the
Spark overhead of exploding a repeated field.**

## Key tables

| Table | Description |
|---|---|
| `biosample_set` | Environmental samples with `env_*`, `geo_loc_name_*`, `depth_*` |
| `data_generation_set` | Sequencing runs (`NucleotideSequencing`, `MassSpectrometry`, …) |
| `data_generation_set_has_input` | `parent_id` → biosample ID |
| `data_generation_set_associated_studies` | `parent_id` → study ID |
| `workflow_execution_set` | All workflow runs (`MetagenomeAnnotation`, `MAGsAnalysis`, …) |
| `workflow_execution_set_was_informed_by` | `parent_id` → data generation ID |
| `workflow_execution_set_has_input` | `parent_id` → input data object ID |
| `workflow_execution_set_has_output` | `parent_id` → output data object ID |
| `data_object_set` | File records: URL, MD5, size, `data_object_type` |
| `study_set` | Studies with PI name, title, DOIs |
| `functional_annotation_agg` | Precomputed `(was_generated_by, gene_function_id, count)` — KEGG.ORTHOLOGY, PFAM, COG only; no EC |

## The annotation → biosample join chain

This is the standard path from a row in `nmdc_results.annotation_kegg_orthology`
(or `annotation_enzyme_commission`) to its originating biosample:

```sql
SELECT bs.id AS biosample_id,
       bs.env_broad_scale_term_id,
       bs.geo_loc_name_has_raw_value
FROM nmdc_results.annotation_kegg_orthology ko
JOIN nmdc_metadata.workflow_execution_set_was_informed_by wib
  ON wib.parent_id = ko.workflow_run_id
JOIN nmdc_metadata.data_generation_set_has_input dhi
  ON dhi.parent_id = wib.was_informed_by
JOIN nmdc_metadata.biosample_set bs
  ON bs.id = dhi.has_input
WHERE ko.annotation_id = 'KO:K00001'
```

No `EXPLODE` is needed. `workflow_execution_set_was_informed_by` already has one
row per (workflow run, data generation) pair.

### Extending to study

Add one more join:

```sql
JOIN nmdc_metadata.data_generation_set_associated_studies dgs
  ON dgs.parent_id = wib.was_informed_by
JOIN nmdc_metadata.study_set s
  ON s.id = dgs.associated_studies
```

## Equivalence with MongoDB `flattened_*` collections

The nmdc-runtime MongoDB instance maintains a set of `flattened_*` collections
(`flattened_workflow_execution`, `flattened_biosample`, etc.) that serve as
its own denormalized query layer. These are **not** loaded into `nmdc_metadata`.
The Silver side tables cover the same ground under different names:

| MongoDB collection | Equivalent in `nmdc_metadata` |
|---|---|
| `flattened_workflow_execution.was_informed_by` (scalar) | `workflow_execution_set_was_informed_by.was_informed_by` |
| `flattened_data_generation.has_input` (pipe-delimited) | `data_generation_set_has_input.has_input` |
| `flattened_biosample.*` (flat scalar fields) | `biosample_set.*` |
| `flattened_study.*` | `study_set.*` |

## `functional_annotation_agg` caveats

- **EC is absent.** The agg only carries `KEGG.ORTHOLOGY`, `PFAM`, and `COG`.
  `nmdc_results.annotation_enzyme_commission` is the only source of EC in BERDL.
- **KO prefix differs.** Annotation tables use `KO:K00001`;
  the agg uses `KEGG.ORTHOLOGY:K00001`. Translate with
  `'KEGG.ORTHOLOGY:' || SUBSTRING(annotation_id, 4)` before joining.

## Upcoming: graph traversal tables

`nmdc_metadata` will gain two additional tables built from the MongoDB `alldocs`
collection (see issue #97):

| Table | Description |
|---|---|
| `nmdc_graph_nodes` | One row per NMDC entity (`id`, `type`) |
| `nmdc_graph_nodes_type_ancestors` | Side table: `parent_id`, `type_ancestor` (full LinkML class hierarchy) |
| `nmdc_graph_edges` | Directed edges: `src_id`, `src_type`, `dst_id`, `dst_type` |

These enable arbitrary multi-hop traversal and type-hierarchy-aware queries
beyond what the schema side tables expose directly. Until they land, use the
join chain above for annotation → biosample navigation.
