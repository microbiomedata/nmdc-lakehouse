# biosample_to_workflow_run: precomputed provenance table

Build `graph_edges` and `biosample_to_workflow_run` from the saved metadata
snapshot with the [local builder](local-provenance.md), then validate and stage
the resulting Parquet. The Spark rebuild that failed at production volume has
been retired. Older promotion plans that drop these tables for a subsequent
rebuild are refused before execution.

Promotion of the metadata snapshot together with its derived snapshot remains
[issue 234](https://github.com/microbiomedata/nmdc-lakehouse/issues/234).
The snapshots must share the recorded parent identity; staging alone does not
update the canonical tables.

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
boolean columns record which processing classes appeared on any upstream branch
of the workflow, regardless of workflow type. They are workflow-wide, not
specific to the biosample path on each row. Pooling can associate one workflow
with multiple biosamples; the mapping does not apportion results among them.

## Workflow types covered

All types present in `nmdc_metadata.workflow_execution_set` at build time. New
workflow types are picked up automatically on the next rebuild, with no config
change required.

MaterialProcessing types are different: each one needs an entry in
`PROCESSING_TYPES` to get a boolean column. A rebuild now refuses when the data
holds a type the mapping does not cover, naming the types, because without a
column every workflow that passed through one reads as false for the steps it
did take. Query the current breakdown with
`SELECT type, COUNT(*) FROM nmdc_metadata.material_processing_set GROUP BY type`.

## Rebuilding it

Use the [local builder and validation procedure](local-provenance.md#build-from-an-existing-snapshot).
It reads the existing Parquet snapshot without a MongoDB tunnel or Spark session
and writes a separate snapshot containing both derived tables. The original
dump does not need to run again. Rebuild whenever the source snapshot changes.

## Example queries

### All taxa detected in a biosample (Kraken2)

<!-- unverified: no run of this procedure is recorded, and no tracking issue is
     named here. -->
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

<!-- unverified: no run of this procedure is recorded, and no tracking issue is
     named here. -->
```sql
SELECT DISTINCT b2wr.biosample_id
FROM   nmdc_results.gtdbtk_bacterial_summary g
JOIN   nmdc_metadata.biosample_to_workflow_run b2wr
         ON b2wr.workflow_run_id = g.workflow_run_id
WHERE  g.classification LIKE '%p__Bacteroidota%'
```

### All genes (KO annotations) for a biosample

<!-- unverified: no run of this procedure is recorded, and no tracking issue is
     named here. -->
```sql
SELECT ko.gene_id, ko.annotation_id, ko.ncbi_taxid
FROM   nmdc_metadata.biosample_to_workflow_run b2wr
JOIN   nmdc_results.annotation_kegg_orthology ko
         ON ko.workflow_run_id = b2wr.workflow_run_id
WHERE  b2wr.biosample_id = 'nmdc:bsm-11-xyz'
```

### Filter by workflow type when you only need one method

Add to any query:
<!-- unverified: no run of this procedure is recorded, and no tracking issue is
     named here. -->
```sql
AND  b2wr.workflow_type = 'nmdc:ReadBasedTaxonomyAnalysis'
```

## Generation and maintenance

The local builder combines four provenance side tables into `graph_edges`, then
walks upstream from each workflow with a visited set. It records minimum hop
counts and workflow-wide processing flags. Input-reference, processing-type,
cycle, and depth checks run before completing the derived snapshot.

The two tables have their own schema,
`src/nmdc_lakehouse/schemas/provenance.yaml`. The local guide documents its
metadata, validation, and measured comparison with recursive queries. There is
one supported builder; the former notebook and Spark/catalog rebuild are retired.

The examples above use the legacy Hive address `nmdc_metadata`. The
catalog-qualified Iceberg address is `nmdc.metadata`; the relationship between
these names was measured in
[issue 248](https://github.com/microbiomedata/nmdc-lakehouse/issues/248).

### When a new MaterialProcessing subclass is added to the NMDC schema

Add the type and a snake_case column name to `PROCESSING_TYPES` in
`src/nmdc_lakehouse/derived_tables.py`, and add its described column to
`src/nmdc_lakehouse/schemas/provenance.yaml`, updating that schema's version.
The builder refuses unknown processing types. Keep the schema and mapping
consistent; tests check that every flag is represented.

### When a new workflow type is added to NMDC

No action required. The builder selects all workflow types without
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
