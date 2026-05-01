# pfam_annotation_gff — per-gene Pfam domain hits from NMDC workflows

## Purpose

`nmdc_results.pfam_annotation_gff` stores every Pfam domain hit emitted by NMDC
metagenome-annotation workflows. One row per (gene, domain) hit; ~3 billion
rows from ~4,800 source GFF files. Joins to `nmdc_ref_data.pfam_terms` for
human-readable names and descriptions, and to
`nmdc_metadata.biosample_to_workflow_run` to reach back to biosamples / studies.

## Schema

| Column | Type | Description |
|---|---|---|
| `workflow_run_id` | string | `was_generated_by` from the source data object — joins to `nmdc_metadata.workflow_execution_set` and `biosample_to_workflow_run` |
| `gene_id` | string | Annotated gene / CDS identifier (NMDC `nmdc:wfmgan-*` style or JGI `Ga0500553_*` style) |
| `pfam_accession` | string | Version-less Pfam ID (e.g. `PF02992`) — joins to `nmdc_ref_data.pfam_terms.pfam_id` |
| `start` | int | Alignment start on the gene |
| `end` | int | Alignment end on the gene |
| `score` | float | HMMER bit score |
| `e_value` | float | HMMER e-value (from col-9 attribute) |
| `alignment_length` | int | Alignment length in residues (from col-9 attribute) |
| `model_start` | int | Alignment start on the Pfam HMM (from col-9 attribute) |
| `model_end` | int | Alignment end on the Pfam HMM (from col-9 attribute) |
| `data_object_id` | string | Source `nmdc:dobj-*` ID (one per file) |

## Source

NMDC workflow output: `data_object_type = 'Pfam Annotation GFF'` rows in
`nmdc_metadata.data_object_set`. Files at `data.microbiomedata.org/...`.
Format is a 9-column tab-separated GFF-like file emitted by HMMER:

```
gene_id  source  pfam_accession  start  end  score  .  .  ID=...;Name=...;e-value=...;alignment_length=...;model_start=...;model_end=...
```

The constant `source` column ("HMMER 3.1b2 (February 2015)"), strand and phase
placeholders ("."), and the `ID=` / `Name=` / `fake_percent_id=` attributes are
not stored — they are constants, derivable, or redundant with `pfam_terms`.

## Generation

Three-stage pipeline (matching the KO/EC pattern, since 650 GB of HTTP would
crash an in-kernel fetch):

1. `notebooks/fetch_pfam_gff.ipynb` — queries `data_object_set` and writes
   `loaded_pfam_gff/manifest.csv`. Drops zero-byte placeholder files.
2. `scripts/download_to_cache.py` — runs in a terminal under `nohup`, downloads
   all GFFs in parallel to `loaded_pfam_gff/raw_cache/` (~650 GB on disk,
   resumable).
3. `notebooks/parse_pfam_gff.ipynb` — streaming parse with
   `pyarrow.ParquetWriter` (~500 MB raw text per RowGroup) → one Parquet at
   `loaded_pfam_gff/pfam_annotation_gff.parquet`.
4. `notebooks/ingest_pfam_gff.ipynb` — uploads to MinIO Bronze and registers
   `nmdc_results.pfam_annotation_gff` as a Delta table. Refuses to clobber an
   existing copy unless `FORCE_OVERWRITE = True` (a re-load takes hours).

## Example queries

### Look up domain hits by Pfam accession

```sql
SELECT p.gene_id, p.score, p.e_value, t.name, t.description
FROM   nmdc_results.pfam_annotation_gff p
JOIN   nmdc_ref_data.pfam_terms t ON t.pfam_id = p.pfam_accession
WHERE  p.pfam_accession = 'PF04183'
ORDER BY p.score DESC
LIMIT  20
```

### All Pfam domains in a biosample

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

Re-run the four-stage pipeline after each NMDC data load. The fetch and parse
notebooks are resumable; the ingest notebook refuses to overwrite by default,
so set `FORCE_OVERWRITE = True` once you've decided to replace the existing
table.
