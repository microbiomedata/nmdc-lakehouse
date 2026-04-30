# biosample_to_workflow_run — precomputed provenance table

## Purpose

`nmdc_metadata.biosample_to_workflow_run` maps every NMDC biosample to every
workflow run that produced results from it (or from derived material), across
**all workflow types** — annotation, read-based taxonomy, MAG analysis, and
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
on the next rebuild — no config change required.

## Example queries

### All taxa detected in a biosample (Kraken2)

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

```sql
SELECT DISTINCT b2wr.biosample_id
FROM   nmdc_results.gtdbtk_bacterial_summary g
JOIN   nmdc_metadata.biosample_to_workflow_run b2wr
         ON b2wr.workflow_run_id = g.workflow_run_id
WHERE  g.classification LIKE '%p__Bacteroidota%'
```

### All genes (KO annotations) for a biosample

```sql
SELECT ko.gene_id, ko.annotation_id, ko.ncbi_taxid
FROM   nmdc_metadata.biosample_to_workflow_run b2wr
JOIN   nmdc_results.annotation_kegg_orthology ko
         ON ko.workflow_run_id = b2wr.workflow_run_id
WHERE  b2wr.biosample_id = 'nmdc:bsm-11-xyz'
```

### Filter by workflow type when you only need one method

Add to any query:
```sql
AND  b2wr.workflow_type = 'nmdc:ReadBasedTaxonomyAnalysis'
```

## Generation

`notebooks/build_biosample_to_workflow_run.ipynb` generates and registers this
table. Run it once after each NMDC data load. The notebook uses an iterative
BFS walk over `nmdc_metadata.graph_edges` — one flat JOIN per hop level,
avoiding Trino's 150-stage `WITH RECURSIVE` limit. `graph_edges` is built and
dropped within the same notebook run.

The result is written directly to Silver via
`spark.createDataFrame().write.saveAsTable()`. No Bronze roundtrip.

## Maintenance

### When NMDC data is reloaded

Re-run `notebooks/build_biosample_to_workflow_run.ipynb`. Fully derived — no
manual editing required.

### When a new MaterialProcessing subclass is added to the NMDC schema

The preflight cell detects unknown types automatically and prints a WARNING.
Add the new type and a snake_case column name to `PROCESSING_TYPES` in the
build notebook before rebuilding.

### When a new workflow type is added to NMDC

No action required. The build notebook selects all workflow types without
filtering — new types appear automatically in the rebuilt table.

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
