# biosample_to_gene — precomputed provenance table

## Purpose

`nmdc_metadata.biosample_to_gene` maps every NMDC biosample to the annotation
workflow runs that produced gene-level results from it (or from derived material).
It is the join bridge between the graph metadata (`nmdc_metadata.*`) and the
per-gene annotation results (`nmdc_results.annotation_kegg_orthology`,
`nmdc_results.annotation_enzyme_commission`, etc.).

**The table does not store individual gene hits.** It stores one row per
(biosample, workflow run) pair. Gene-level queries join this table to the live
`nmdc_results` annotation tables on `workflow_run_id`.

## Schema

| Column | Type | Description |
|---|---|---|
| `biosample_id` | string | `nmdc:bsm-*` identifier |
| `workflow_run_id` | string | Annotation workflow run ID (e.g. `nmdc:wfmgan-*`) |
| `workflow_type` | string | NMDC class of the workflow (e.g. `nmdc:MetagenomeAnnotation`) |
| `n_hops` | int | Minimum number of graph edges from workflow run to biosample |
| `has_extraction` | boolean | `nmdc:Extraction` step in the provenance chain |
| `has_library_prep` | boolean | `nmdc:LibraryPreparation` step in the provenance chain |
| `has_subsampling` | boolean | `nmdc:SubSamplingProcess` step in the provenance chain |
| `has_pooling` | boolean | `nmdc:Pooling` step in the provenance chain |
| `has_chromatographic_separation` | boolean | `nmdc:ChromatographicSeparationProcess` step |
| `has_dissolving` | boolean | `nmdc:DissolvingProcess` step |
| `has_chemical_conversion` | boolean | `nmdc:ChemicalConversionProcess` step |
| `has_filtration` | boolean | `nmdc:FiltrationProcess` step |

`n_hops = 2` means the biosample fed directly into DataGeneration (no material
processing). Larger values indicate one or more intermediate ProcessedSample /
MaterialProcessing steps. The boolean columns record which processing classes
appeared anywhere in that chain.

## Example queries

### All genes (KO annotations) for a biosample

```sql
SELECT ko.gene_id, ko.annotation_id, ko.ncbi_taxid
FROM   nmdc_metadata.biosample_to_gene b2g
JOIN   nmdc_results.annotation_kegg_orthology ko
         ON ko.workflow_run_id = b2g.workflow_run_id
WHERE  b2g.biosample_id = 'nmdc:bsm-11-xyz'
```

### KO hit counts per biosample, filtered to direct sequencing (no processing)

```sql
SELECT b2g.biosample_id, COUNT(*) AS n_hits
FROM   nmdc_metadata.biosample_to_gene b2g
JOIN   nmdc_results.annotation_kegg_orthology ko
         ON ko.workflow_run_id = b2g.workflow_run_id
WHERE  ko.annotation_id = 'KO:K00001'
  AND  b2g.n_hops = 2   -- direct: DataGeneration.has_input → Biosample
GROUP  BY 1
ORDER  BY 2 DESC
```

### Compare direct-sequenced vs. pooled biosamples

```sql
SELECT b2g.has_pooling, COUNT(DISTINCT b2g.biosample_id) AS n_biosamples,
       COUNT(*) AS n_ko_hits
FROM   nmdc_metadata.biosample_to_gene b2g
JOIN   nmdc_results.annotation_kegg_orthology ko
         ON ko.workflow_run_id = b2g.workflow_run_id
WHERE  ko.annotation_id = 'KO:K00001'
GROUP  BY 1
```

## Generation

`notebooks/build_biosample_to_gene.ipynb` generates and registers this table.
Run it once after each NMDC data load. The notebook uses the Trino two-step
pattern: one recursive CTE per batch of workflow runs (500 at a time to avoid
`TOO_MANY_REQUESTS_FAILED`), collecting both biosample endpoints and
intermediate MaterialProcessing node types in a single pass.

The resulting DataFrame is written directly to Silver as a Hive-registered
Delta table via `spark.createDataFrame(result).write.saveAsTable()`. No Bronze
intermediate — this is a derived table computed from existing Silver tables,
not ingested from raw external data.

## Maintenance

### When NMDC data is reloaded

Rebuild the table by re-running `notebooks/build_biosample_to_gene.ipynb`.
The table is fully derived — no manual editing required.

### When a new MaterialProcessing subclass is added to the NMDC schema

**This is the critical maintenance case.** The eight boolean flag columns are
hardcoded in `build_biosample_to_gene.ipynb` against the MaterialProcessing
types that existed in BERDL as of 2026-04-30:

```python
PROCESSING_TYPES = {
    'nmdc:Extraction':                        'has_extraction',
    'nmdc:LibraryPreparation':                'has_library_prep',
    'nmdc:SubSamplingProcess':                'has_subsampling',
    'nmdc:Pooling':                           'has_pooling',
    'nmdc:ChromatographicSeparationProcess':  'has_chromatographic_separation',
    'nmdc:DissolvingProcess':                 'has_dissolving',
    'nmdc:ChemicalConversionProcess':         'has_chemical_conversion',
    'nmdc:FiltrationProcess':                 'has_filtration',
}
```

If a new subclass is loaded into NMDC data **without updating this dict**, the
new type will be silently ignored — existing data is unaffected, but the new
processing class will not have a flag column, and any biosample whose chain
includes it will appear to have no special processing.

**Detection:** rerun this query after any NMDC schema or data update:

```python
cur.execute("""
    SELECT type, COUNT(*) AS n
    FROM   nmdc_metadata.material_processing_set
    GROUP  BY type
    ORDER  BY n DESC
""")
```

Compare the result to `PROCESSING_TYPES.keys()`. Any type that appears in the
query but not in the dict needs a new flag column.

**Remediation:**
1. Add the new type and a snake_case column name to `PROCESSING_TYPES` in the notebook
2. Re-run `build_biosample_to_gene.ipynb` to rebuild the table with the new column
3. Update any downstream queries or views that select specific boolean columns
   by name (wildcard `SELECT *` queries are unaffected)

### When a MaterialProcessing subclass is removed or renamed

The corresponding boolean column will be all-`False` after a rebuild. It can
be dropped from `PROCESSING_TYPES` and the table rebuilt. Any downstream query
that references the removed column name will need updating.

### Frequency

NMDC data is loaded in batches, not continuously. A manual rebuild after each
load cycle is sufficient. If load frequency increases, a Dagster asset or a
post-ingest hook on `nmdc_metadata.material_processing_set` can trigger an
automatic rebuild.

## Relationship to other tables

| Table | Scope | n_hops | Processing flags |
|---|---|---|---|
| `workflow_to_biosample` (NUC proposal) | all workflow types | yes | no |
| `biosample_to_gene` | annotation workflows only | yes | yes |

Use `biosample_to_gene` when you need to join to `nmdc_results` or filter by
processing chain shape. Use `workflow_to_biosample` (if built) for broader
coverage across all workflow types without the flag overhead.
