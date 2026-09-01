# pfam_annotation_gff: per-gene Pfam domain hits from NMDC workflows

## Purpose

`nmdc_results.pfam_annotation_gff` stores every Pfam domain hit emitted by NMDC
metagenome-annotation workflows. One row per (gene, domain) hit; ~3 billion
rows from ~4,800 source GFF files. Joins to `nmdc_ref_data.pfam_terms` for
human-readable names and descriptions, and to
`nmdc_metadata.biosample_to_workflow_run` to reach back to biosamples / studies.

## Schema

| Column | Type | Description |
|---|---|---|
| `workflow_run_id` | string | `was_generated_by` from the source data object. Joins to `nmdc_metadata.workflow_execution_set` and `biosample_to_workflow_run` |
| `gene_id` | string | Annotated gene / CDS identifier (NMDC `nmdc:wfmgan-*` style or JGI `Ga0500553_*` style) |
| `pfam_accession` | string | Version-less Pfam ID (e.g. `PF02992`). Joins to `nmdc_ref_data.pfam_terms.pfam_id` |
| `start` | int | Alignment start on the gene |
| `end` | int | Alignment end on the gene |
| `score` | float | HMMER bit score |
| `e_value` | float | HMMER e-value (from col-9 attribute) |
| `alignment_length` | int | Alignment length in residues (from col-9 attribute) |
| `model_start` | int | Alignment start on the Pfam HMM (from col-9 attribute) |
| `model_end` | int | Alignment end on the Pfam HMM (from col-9 attribute) |
| `data_object_id` | string | Source `nmdc:dobj-*` ID (one per file) |

`workflow_run_id` may be `NULL` when the source `data_object` has no
`was_generated_by` value upstream; the parse stage logs the count and those
rows cannot participate in `biosample_to_workflow_run` joins.

## Source

NMDC workflow output: `data_object_type = 'Pfam Annotation GFF'` rows in
`nmdc_metadata.data_object_set`. Files at `data.microbiomedata.org/...`.
Format is a 9-column tab-separated GFF-like file emitted by HMMER:

```
gene_id  source  pfam_accession  start  end  score  .  .  ID=...;Name=...;e-value=...;alignment_length=...;model_start=...;model_end=...
```

The constant `source` column ("HMMER 3.1b2 (February 2015)"), strand and phase
placeholders ("."), and the `ID=` / `Name=` / `fake_percent_id=` attributes are
not stored, being constants, derivable, or redundant with `pfam_terms`.

## Generation

Four-stage pipeline (matching the KO/EC pattern, since 650 GB of HTTP would
crash an in-kernel fetch):

1. `just data-object-manifest` (or `nmdc-lakehouse data-object-manifest`) queries
   `data_object_set` and writes the manifest. Drops zero-byte placeholder files,
   of which Pfam has 7, and drops repeated URLs. See
   [Building a download manifest](#building-a-download-manifest) below.
   `notebooks/fetch_pfam_gff.ipynb` did this and is replaced by it. The notebook
   still runs; nothing in it reads the manifest this writes, so the two are
   alternatives rather than a pipeline.
2. `scripts/download_to_cache.py` runs in a terminal under `nohup`, downloads
   all GFFs in parallel to `loaded_pfam_gff/raw_cache/` (~650 GB on disk,
   resumable).
3. `notebooks/parse_pfam_gff.ipynb` does a streaming parse with
   `pyarrow.ParquetWriter` (~500 MB raw text per RowGroup) → one Parquet at
   `loaded_pfam_gff/pfam_annotation_gff.parquet`.
4. `notebooks/ingest_pfam_gff.ipynb` uploads to MinIO Bronze and registers
   `nmdc_results.pfam_annotation_gff` as a managed table. Refuses to clobber an
   existing copy unless `FORCE_OVERWRITE = True` (a re-load takes hours).

## Building a download manifest

The first stage, for any data object type rather than Pfam alone.

<!-- verified: 2026-09-01 run against the 2026-08-21 snapshot on a workstation,
     no pod. Returned 4,882 objects and 611.6 GiB with 7 zero-byte placeholders
     dropped, which is the count fetch_pfam_gff.ipynb recorded for this type. -->
```bash
just data-object-manifest "Pfam Annotation GFF" \
    local/mongodb-metadata-20260821_104214/data_object_set.parquet \
    local/pfam/manifest.csv
```

**Prerequisites.** A snapshot Parquet of `data_object_set`, which
`just etl-collections` produces. Reading the live catalog instead needs a Spark
session and the command run directly rather than through the recipe, because the
recipe always supplies `--data-object-set` and the command refuses two sources:

<!-- unverified: no run of this form is recorded. Running it needs a pod, and no
     tracking issue is named here. -->
```bash
uv run nmdc-lakehouse data-object-manifest --type "Pfam Annotation GFF" \
    --ingest-checkout ~/gitrepos/BERIL-research-observatory \
    --namespace nmdc.metadata \
    --output local/pfam/manifest.csv
```

Exactly one source must be named; neither is a default, because which one was
read changes what the manifest describes.

**What it writes.** One CSV at the path given by `--output`, with the columns
`scripts/download_to_cache.py` needs plus the ones the parse and ingest stages
join on. Written to a temporary file and renamed, so an interrupted run leaves
the previous manifest intact rather than a partial one, which
`scripts/download_to_cache.py` would read as the whole set.

**Safety boundaries.** It reads and writes local files and downloads nothing.
It refuses rather than writing a manifest that would describe no objects, that
would overwrite the snapshot it just read, or that contains URLs sharing a
download-cache path. That last one is not hypothetical: every object of type
`LC-DDA-MS/MS Raw Data` shares the path `/ProteoSAFe/DownloadResultFile`, so
2,733 payloads would land on one cached file
([#325](https://github.com/microbiomedata/nmdc-lakehouse/issues/325)). Those
types cannot be fetched until the cache key is fixed.

**Recovery.** Nothing to undo. Rerun it; the manifest is rebuilt from the
source each time and the command is the only writer of its output path.

**Type names** are resolved against nmdc-schema's `FileTypeEnum`, so a typo
fails immediately with close matches rather than producing an empty manifest
that downloads nothing and reports success. Pass `--type` more than once to
fetch several types in one manifest, which is what the KO/EC pair needs.

## Example queries

### Look up domain hits by Pfam accession

<!-- unverified: no run of this procedure is recorded, and no tracking issue is
     named here. -->
```sql
SELECT p.gene_id, p.score, p.e_value, t.name, t.description
FROM   nmdc_results.pfam_annotation_gff p
JOIN   nmdc_ref_data.pfam_terms t ON t.pfam_id = p.pfam_accession
WHERE  p.pfam_accession = 'PF04183'
ORDER BY p.score DESC
LIMIT  20
```

### All Pfam domains in a biosample

<!-- unverified: no run of this procedure is recorded, and no tracking issue is
     named here. -->
```sql
SELECT p.pfam_accession, t.name, t.description, COUNT(*) AS n_hits
FROM   nmdc_metadata.biosample_to_workflow_run b2wr
JOIN   nmdc_results.pfam_annotation_gff p ON p.workflow_run_id = b2wr.workflow_run_id
JOIN   nmdc_ref_data.pfam_terms t          ON t.pfam_id        = p.pfam_accession
WHERE  b2wr.biosample_id = 'nmdc:bsm-11-xyz'
GROUP BY p.pfam_accession, t.name, t.description
ORDER BY n_hits DESC
```

### Co-occurrence demo: siderophore + iron-reductase in the same workflow run

<!-- unverified: no run of this procedure is recorded, and no tracking issue is
     named here. -->
```sql
SELECT DISTINCT a.workflow_run_id
FROM   nmdc_results.pfam_annotation_gff a
WHERE  a.pfam_accession = 'PF04183'
  AND  EXISTS (
         SELECT 1
         FROM   nmdc_results.pfam_annotation_gff b
         WHERE  b.workflow_run_id = a.workflow_run_id
           AND  b.pfam_accession = 'PF06276'
       )
```

## Maintenance

Re-run the four-stage pipeline after each NMDC data load. The download script
(`scripts/download_to_cache.py`) is resumable; the parse and ingest stages
re-build their outputs from scratch on each run. The ingest notebook refuses
to overwrite an existing managed table by default, so set `FORCE_OVERWRITE = True`
once you've decided to replace it.
