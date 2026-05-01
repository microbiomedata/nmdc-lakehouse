# pfam_terms — Pfam family definitions reference table

## Purpose

`nmdc_ref_data.pfam_terms` maps every Pfam family accession to its short name and
human-readable description. It is the join target for any `nmdc_results` table that
contains a `pfam_accession` column (e.g. `nmdc_results.pfam_annotation_gff`, issue #88).

## Schema

| Column | Type | Description |
|---|---|---|
| `pfam_id` | string | Pfam accession without version (e.g. `PF00001`) — join key |
| `name` | string | Short name (e.g. `7tm_1`) |
| `description` | string | Human-readable description |
| `clan_id` | string (nullable) | Clan accession (e.g. `CL0192`); null if no clan |
| `clan_name` | string (nullable) | Clan short name (e.g. `GPCR_A`); null if no clan |

## Source

`Pfam-A.clans.tsv.gz` from the EBI/InterPro Pfam FTP site:
`https://ftp.ebi.ac.uk/pub/databases/Pfam/current_release/Pfam-A.clans.tsv.gz`

License: CC0 — openly redistributable. ~27,500 entries covering all active Pfam families,
with and without clan assignments.

## Namespace

`nmdc_ref_data` — the BERDL Silver namespace for reference/ontology tables owned by the
NMDC lakehouse. Distinct from `nmdc_metadata` (schema-driven NMDC MongoDB collections) and
`nmdc_results` (workflow output files). All term/ontology tables land here.

Do not read from or write to `nmdc_arkin` or any other tenant's namespace.

## Generation

`notebooks/load_pfam_terms.ipynb` generates and registers this table. Re-run after each
Pfam release to pick up new or updated family entries. The accession format is version-less
(`PF00001`, not `PF00001.23`) — this matches the format used by NMDC Pfam GFF files.

## Example queries

### Look up a domain by accession

```sql
SELECT pfam_id, name, description, clan_name
FROM   nmdc_ref_data.pfam_terms
WHERE  pfam_id = 'PF00072'
```

### All hits for a Pfam domain (requires nmdc_results.pfam_annotation_gff)

```sql
SELECT p.gene_id, p.score, p.e_value, t.name, t.description
FROM   nmdc_results.pfam_annotation_gff p
JOIN   nmdc_ref_data.pfam_terms t ON t.pfam_id = p.pfam_accession
WHERE  p.pfam_accession = 'PF04183'
ORDER BY p.score DESC
LIMIT  20
```

### Co-occurrence demo: siderophore + iron-reductase domains in the same workflow run

```sql
SELECT a.workflow_run_id
FROM   nmdc_results.pfam_annotation_gff a
JOIN   nmdc_results.pfam_annotation_gff b ON b.workflow_run_id = a.workflow_run_id
WHERE  a.pfam_accession = 'PF04183'
  AND  b.pfam_accession = 'PF06276'
```

## Maintenance

Re-run `notebooks/load_pfam_terms.ipynb` after each Pfam release. The notebook uses
`write_mode: overwrite` so the table is fully replaced on each run.
