# NMDC feature tables

**Status: prototype.** These commands plan, sample and check NMDC annotation feature files, to
measure which file types repeat each other before any of them is loaded. Nothing here converts or
uploads data to BERDL (the KBase lakehouse). Conversion to the draft BER feature model
(https://github.com/turbomam/feature-table-corpus/blob/main/model/schema/ber_feature_model.yaml)
is https://github.com/microbiomedata/nmdc-lakehouse/issues/362 .

## Why

An NMDC metagenome or metatranscriptome annotation run writes about twenty feature-like files.
Loading all of them would load most observations two or three times. The existing loaders take
one annotation system at a time (Pfam in `pfam_annotation_gff.md`), and the combined
Functional Annotation GFF is tracked in
https://github.com/microbiomedata/nmdc-lakehouse/issues/84 . This page records which files repeat
which, measured on a sample, and the commands that apply the result.

## Which files to load, and which to skip

Measured on a 50-run sample on 2026-09-23 (see [Sample results](#sample-results)).

| `data_object_type` | recommendation | reason |
|---|---|---|
| Functional Annotation GFF | load | the selected gene and RNA calls with every functional key |
| Pfam, COG, TIGRFam, SMART, CATH FunFams, SUPERFam, KO_EC Annotation GFF | load | only source of hit coordinates, scores, e-values and repeated hits; the Functional Annotation GFF lists only their accessions |
| Structural Annotation GFF | skip | the Functional Annotation GFF minus its functional keys, row for row |
| Annotation KEGG Orthology, Annotation Enzyme Commission | skip | the same (gene, accession) pairs as KO_EC Annotation GFF |
| Product Names | skip | `product` and `product_source` of the Functional Annotation GFF; see the RNA exception below |
| Prodigal, GeneMark, tRNA, RFAM, CRT Annotation GFF | skip, except unselected calls | selected calls repeat the Functional Annotation GFF, except in old versions, see below |
| Contig Mapping File, Scaffold Lineage tsv | load | assembly contig ID and per-contig lineage, one row per contig |
| Gene Phylogeny tsv, Crispr Terms | not decided | not checked |

One annotation run is kept per input assembly. Some assemblies were annotated more than once (a
`.1` run and a later `.2` run over the same `has_input`); the run with the highest suffix is kept
and the rest are recorded as superseded in `plan.json`. Files are attached to runs through the
run's `has_output`, because many data objects have no `was_generated_by`.

## Exceptions found in the sample

- **Old pipeline versions change coordinates after selection.** In v1.0.2 and v1.0.4 runs, some
  selected CDS rows have coordinates no caller file reports (a Prodigal call at 1 to 336 selected
  as 1 to 177, in `nmdc:wfmgan-11-24e11y58.1`). In those runs, a caller row missing from the
  Functional Annotation GFF is not necessarily an unselected call.
- **Old pipeline versions repeat IDs.** An RFAM hit over one interval on both strands gets one
  `ID` twice, and once a GeneMark CDS and an RFAM sRNA shared one (`Ga0495594_0816865_1_525` in
  `nmdc:wfmgan-12-2h43v434.1`). IDs are unique once strand is included.
- **Old pipeline versions pack KOs differently.** `KO:K02025_KO:K10118` (one underscore) rather
  than one KO per row; the parser splits both forms.
- **Product Names labels RNA rows the Functional Annotation GFF does not.** For rRNA, tRNA,
  tmRNA and ncRNA rows, in every pipeline version, its third column says `rRNA_23S`, `tRNA`,
  `tmRNA` or `ncRNA` where the Functional Annotation GFF has no `product_source`. The label restates
  the feature type and, for rRNA, the subunit named in `product`, so it is not loaded. The check
  fails if any other feature type lacks a source that only Product Names has.

## Run it

All four recipes write under `local/feature-tables/` (override with `FEATURE_DIR`).

<!-- verified: 2026-09-23 planned 4,607 runs, sampled 50 runs (1,024 files, 42.55 GiB), downloaded in 9.1 minutes, checked them locally -->
```bash
just feature-plan
just feature-sample 50 3
just feature-download
just feature-check
```

`feature-sample` takes runs from each (run type, pipeline version) group in turn, so small
versions are represented. The second argument skips runs over that many GiB, which biases the
sample toward small runs. `feature-check` verifies each file's MD5 against NMDC and exits
non-zero if any check fails or any planned file is missing from the cache; its report is
`check-report.json`.

## Sample results

Measured 2026-09-23. The plan from the public NMDC API kept 4,607 annotation runs and recorded 483
superseded reruns and 67 data objects no run lists. The sample was 50 runs, 8 or 9 from each of
MetagenomeAnnotation v1.0.2, v1.0.4, v1.0.5, v1.1.0, v1.1.5 and MetatranscriptomeAnnotation
v1.1.4, capped at 3 GiB per run: 1,024 files, 42.55 GiB, downloaded in 9.1 minutes. Every file's
MD5 matched NMDC's record. Twelve of the files are empty CRT and Crispr Terms outputs, which
`feature-sample` now leaves out of the manifest, so the same sample lists 1,012 files.

| check | runs passed | runs failed |
|---|---|---|
| Structural Annotation GFF is the Functional Annotation GFF minus keys | 50 | 0 |
| each hit file's accessions per gene equal the Functional Annotation GFF's (Pfam, COG, TIGRFam, SMART, CATH, SUPERFam, KO_EC) | 50 | 0 |
| KEGG Orthology TSV pairs equal KO_EC Annotation GFF pairs | 50 | 0 |
| Enzyme Commission TSV pairs equal KO_EC Annotation GFF pairs | 50 | 0 |
| Product Names equals Functional Annotation GFF product fields | 50 | 0 |
| feature IDs unique once strand is included | 50 | 0 |
| Functional Annotation GFF contigs all in Contig Mapping File (37 runs have the file) | 37 | 0 |
| every selected row appears in a caller file | 37 | 13 |

The 13 failures are every sampled v1.0.2 and v1.0.4 run; see the exceptions above. The KO_EC rows
first failed in those same runs because of the parser, which now splits both packing forms.

## Not done

- Conversion, https://github.com/microbiomedata/nmdc-lakehouse/issues/362 .
- Upload to BERDL.
- Checks on all 4,607 selected runs. The files recommended for loading total about 5,028 GB.
- The Gene Phylogeny and Crispr Terms files.
