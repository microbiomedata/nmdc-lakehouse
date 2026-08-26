# biosample_to_workflow_run: precomputed provenance table

> **This table disappears during a reload.**
>
> Promotion drops `biosample_to_workflow_run` and `graph_edges` **before** replacing the tables
> they are computed from, and rebuilds them afterwards. For the duration of a promotion both are
> absent: queries against them fail, and so do joins from `biosample_to_workflow_run` into
> `nmdc.results`.
>
> That is deliberate, decided 2026-08-26. The alternative was leaving them in place while their
> inputs were replaced underneath, which would have returned biosample-to-workflow mappings built
> from provenance that no longer existed. Those answers look correct, so nobody would notice.
> A failed query is noticed immediately; a wrong one is found out later, by somebody else, in
> results they had no reason to doubt.
>
> If you depend on these tables, expect a window of minutes during a reload rather than degraded
> answers. See https://github.com/microbiomedata/nmdc-lakehouse/issues/234.

## Purpose

`nmdc_metadata.biosample_to_workflow_run` maps every NMDC biosample to every
workflow run that produced results from it (or from derived material), across
**all workflow types**: annotation, read-based taxonomy, MAG analysis, and
others. It is the universal join bridge between biosample identity and any
`nmdc_results` table.

**The table does not store results.** It stores one row per (biosample, workflow
run) pair. Result queries join this table on `workflow_run_id`.

## Schema

| Column | Type | Description |
|---|---|---|
| `biosample_id` | string | `nmdc:bsm-*` identifier |
| `workflow_run_id` | string | Workflow run ID |
| `workflow_type` | string | NMDC class (e.g. `nmdc:MetagenomeAnnotation`, `nmdc:ReadBasedTaxonomyAnalysis`) |
| `n_hops` | int | Minimum graph edges from workflow run to biosample |
| `has_extraction` | boolean | `nmdc:Extraction` step in the provenance chain |
| `has_library_prep` | boolean | `nmdc:LibraryPreparation` step |
| `has_subsampling` | boolean | `nmdc:SubSamplingProcess` step |
| `has_pooling` | boolean | `nmdc:Pooling` step |
| `has_chromatographic_separation` | boolean | `nmdc:ChromatographicSeparationProcess` step |
| `has_dissolving` | boolean | `nmdc:DissolvingProcess` step |
| `has_chemical_conversion` | boolean | `nmdc:ChemicalConversionProcess` step |
| `has_filtration` | boolean | `nmdc:FiltrationProcess` step |

`n_hops = 2` means the biosample fed directly into DataGeneration. Larger
values indicate intermediate ProcessedSample / MaterialProcessing steps. The
boolean columns record which processing classes appeared anywhere in that chain,
regardless of workflow type.

## Workflow types covered

All types present in `nmdc_metadata.workflow_execution_set` at build time.
Run the preflight cell in `notebooks/build_biosample_to_workflow_run.ipynb`
to see the current breakdown. New workflow types are picked up automatically
on the next rebuild, with no config change required.

## Example queries

### All taxa detected in a biosample (Kraken2)

<!-- unverified: no run of this procedure is recorded. Declaring the 81 blocks
     that predate this rule is https://github.com/microbiomedata/nmdc-lakehouse/issues/291 -->
```sql
SELECT k.rank, k.name, k.taxid, k.pct_clade
FROM   nmdc_metadata.biosample_to_workflow_run b2wr
JOIN   nmdc_results.kraken2_classification_report k
         ON k.workflow_run_id = b2wr.workflow_run_id
WHERE  b2wr.biosample_id = 'nmdc:bsm-11-xyz'
  AND  k.rank = 'S'
ORDER BY k.pct_clade DESC
```

### All biosamples with a given taxon (GTDBTK)

<!-- unverified: no run of this procedure is recorded. Declaring the 81 blocks
     that predate this rule is https://github.com/microbiomedata/nmdc-lakehouse/issues/291 -->
```sql
SELECT DISTINCT b2wr.biosample_id
FROM   nmdc_results.gtdbtk_bacterial_summary g
JOIN   nmdc_metadata.biosample_to_workflow_run b2wr
         ON b2wr.workflow_run_id = g.workflow_run_id
WHERE  g.classification LIKE '%p__Bacteroidota%'
```

### All genes (KO annotations) for a biosample

<!-- unverified: no run of this procedure is recorded. Declaring the 81 blocks
     that predate this rule is https://github.com/microbiomedata/nmdc-lakehouse/issues/291 -->
```sql
SELECT ko.gene_id, ko.annotation_id, ko.ncbi_taxid
FROM   nmdc_metadata.biosample_to_workflow_run b2wr
JOIN   nmdc_results.annotation_kegg_orthology ko
         ON ko.workflow_run_id = b2wr.workflow_run_id
WHERE  b2wr.biosample_id = 'nmdc:bsm-11-xyz'
```

### Filter by workflow type when you only need one method

Add to any query:
<!-- unverified: no run of this procedure is recorded. Declaring the 81 blocks
     that predate this rule is https://github.com/microbiomedata/nmdc-lakehouse/issues/291 -->
```sql
AND  b2wr.workflow_type = 'nmdc:ReadBasedTaxonomyAnalysis'
```

## Generation

`nmdc_lakehouse.derived_tables` generates and registers both tables. See
[Maintenance](#maintenance) for the two calls and their order.

An iterative breadth-first walk over `graph_edges`, one join per hop level, which is what
avoids Trino's 150-stage `WITH RECURSIVE` limit. `graph_edges` is created or replaced from
the four provenance side tables and persists, so refresh it whenever those are reloaded.

Each hop is cached as it is built. A temp view is lazy, so without that each hop's plan
contains every earlier hop and the end-of-walk union re-executes all of them.

The result is written by `CREATE OR REPLACE TABLE ... AS SELECT`, in the catalog's own
format. No Bronze roundtrip, and no pinned table format: the notebook wrote
`spark.createDataFrame().write.saveAsTable()` into Delta, which was right for the Hive
namespace it targeted and wrong for an Iceberg one.

## Maintenance

### When NMDC data is reloaded

Both derived tables are rebuilt by `nmdc_lakehouse.derived_tables`. Call it yourself after a
reload: nothing calls it automatically yet, and wiring it into a promotion is
https://github.com/microbiomedata/nmdc-lakehouse/issues/234.

This matters because a reload replaces every table these two are derived from, so leaving
them alone leaves two populated tables describing data that no longer exists, and nothing
in the namespace says so.

**Pass the catalog-qualified name.** The examples elsewhere on this page say
`nmdc_metadata.biosample_to_workflow_run`, which is the legacy Hive address for the same
tables; `nmdc.metadata` is the Iceberg one. They are two addresses onto one dataset rather
than two copies, measured in
https://github.com/microbiomedata/nmdc-lakehouse/issues/248. The rebuild functions require
the qualified form and reject `nmdc_metadata`, because an unqualified name resolves in
whatever catalog the session happens to point at and these statements replace tables.

<!-- unverified: the module and its refusals are covered offline, but no rebuild has been run
     against a live catalog, so the SQL is not yet known to compute the right provenance.
     Tracked at https://github.com/microbiomedata/nmdc-lakehouse/issues/234 -->
```python
from nmdc_lakehouse.derived_tables import rebuild_biosample_to_workflow_run, rebuild_graph_edges

rebuild_graph_edges(spark, "nmdc.metadata")
rebuild_biosample_to_workflow_run(spark, "nmdc.metadata", progress=print)
```

Order matters: `graph_edges` is what the walk traverses, so it is rebuilt first from the
four provenance side tables.

`notebooks/build_biosample_to_workflow_run.ipynb` is the record of how this was worked out
and is no longer the way to run it. Three things changed in the move, and each was a reason
it could not run unattended: it used a Trino cursor for the walk and Spark for the writes, so
an automated run needed two connections; it accumulated every hop into pandas frames in the
driver; and each hop inlined every frontier identifier into an `IN (...)` clause, which at
33,234 workflow runs is a statement megabytes wide that grows with the data. The walk is the
same breadth-first algorithm.

It also wrote `USING DELTA` into the unqualified `nmdc_metadata`. That was right for the Hive
namespace it targeted and wrong for the Iceberg one, so the format is no longer pinned and the
namespace has to be catalog-qualified.

### When a new MaterialProcessing subclass is added to the NMDC schema

Add the type and a snake_case column name to `PROCESSING_TYPES` in
`src/nmdc_lakehouse/derived_tables.py`. The rebuild does not detect unknown types for you;
that preflight lived in the notebook and has not been ported, which is
https://github.com/microbiomedata/nmdc-lakehouse/issues/130.

### When a new workflow type is added to NMDC

No action required. The build notebook selects all workflow types without
filtering, so new types appear automatically in the rebuilt table.

### When a new nmdc_results table is ingested (e.g., Centrifuge)

No rebuild required. The new table joins to `biosample_to_workflow_run` on
`workflow_run_id` directly. Update the relevant peek notebook to un-skip
the new method.

## Downstream peek notebooks

| Notebook | What it demonstrates |
|---|---|
| `peek_ko_ec_links.ipynb` | KO/EC annotations ↔ biosample/study |
| `peek_read_taxonomy_links.ipynb` | Kraken2/GOTTCHA2/Centrifuge ↔ biosample (both directions) |
| `peek_mag_taxonomy_links.ipynb` | GTDBTK/CheckM ↔ biosample (both directions) |
