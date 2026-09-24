# NMDC feature tables

**Status: prototype.** These commands plan, sample and check NMDC annotation feature files, to
measure which file types repeat each other, and convert the files worth loading into local Parquet
shaped like the draft BER feature model
(https://github.com/turbomam/feature-table-corpus/blob/main/model/schema/ber_feature_model.yaml).
Nothing here uploads to BERDL (the KBase lakehouse).

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
| Prodigal, GeneMark, tRNA, RFAM, CRT Annotation GFF | skip, except unselected calls | each selected row repeats the same caller's row; about half the caller rows are unselected, including the losing caller's call at the same interval |
| Contig Mapping File, Scaffold Lineage tsv | load | assembly contig ID and per-contig lineage, one row per contig |
| Gene Phylogeny tsv, Crispr Terms | not decided | not checked |

One annotation run is kept per input assembly. Some assemblies were annotated more than once (a
`.1` run and a later `.2` run over the same `has_input`); the run with the highest suffix is kept
and the rest are recorded as superseded in `plan.json`. Files are attached to runs through the
run's `has_output`, because many data objects have no `was_generated_by`.

`feature-convert` follows these recommendations. It drops an accession key (`pfam`, `cog`, `ko`,
`ec_number` and the rest) from a gene's `attributes` only when that run's hit file exists and
matched it exactly; otherwise the key stays.

## Exceptions found in the sample

- **Old pipeline versions change coordinates after selection.** In all 9 sampled v1.0.2 runs and 4
  of the 9 v1.0.4 runs, some selected CDS rows have coordinates no caller file reports (a Prodigal call at 1 to 336 selected
  as 1 to 177, in `nmdc:wfmgan-11-24e11y58.1`). In those runs, a caller row missing from the
  Functional Annotation GFF is not necessarily an unselected call, so `--include-unselected` is
  refused for such runs.
- **Old pipeline versions repeat IDs.** An RFAM hit over one interval on both strands gets one
  `ID` twice, and once a GeneMark CDS and an RFAM sRNA shared one (`Ga0495594_0816865_1_525` in
  `nmdc:wfmgan-12-2h43v434.1`). IDs are unique once strand is included. The converter appends the
  strand to those feature IDs, keeps the original `ID` in `attributes`, and points protein hits at
  the renamed CDS.
- **Old pipeline versions pack KOs differently.** `KO:K02025_KO:K10118` (one underscore) rather
  than one KO per row; the parser splits both forms.
- **Product Names labels RNA rows the Functional Annotation GFF does not.** For rRNA, tRNA,
  tmRNA and ncRNA rows, in every pipeline version, its third column says `rRNA_23S`, `tRNA`,
  `tmRNA` or `ncRNA` where the Functional Annotation GFF has no `product_source`. The label restates
  the feature type and, for rRNA, the subunit named in `product`, so it is not loaded. The check
  fails if any other feature type lacks a source that only Product Names has.

## Output shape

`features.parquet`, one row per feature, columns named after the model's `Feature` slots:
`feature_id`, `seqid`, `source`, `type`, `start`, `end`, `coordinate_system`, `score`, `strand`,
`phase`, `parent` (list), `attributes` (list of `{key, value}` in source order), `generated_by`,
`source_files` (list), `is_selected`, `product`, `product_source`, plus
`source_data_object_type`, which the model does not have. `translated_sequence` and `location` are
not filled.

Genome features come from the Functional Annotation GFF with `coordinate_system` `contig` and
`is_selected` true. Hits come from the seven hit GFFs with `coordinate_system` `protein`, `parent`
set to their gene, `seqid` set to that gene's contig, and `is_selected` null. A hit on a gene the
Functional Annotation GFF lacks is counted in the run summary and not written.

`contigs.parquet`: `contig_id`, `assembly_contig_id` (not in the model), `taxonomic_lineage`
(list), `lineage_confidence`, `generated_by`, `source_files`. The model's `length_bp` and
`topology` are not filled: length needs the assembly FASTA, and no NMDC annotation file states
topology.

Each run directory also holds `run_id.txt`. `feature-convert` replaces a run directory only if that
file names the same run, so two run IDs that map to one directory name cannot overwrite each other.

A hit's `feature_id` is its GFF `ID`, its annotation system and its column 3 joined with `|`,
because one gene can carry the same coordinates in several systems. If any `feature_id` still
repeats within a run, `feature-convert` stops before writing that run.

Features carry the annotation run as `generated_by`. Contigs carry the assembly run, found by
`feature-plan` as the run whose `has_output` includes the annotation run's input, and null when
none does.

## Run it

All five recipes write under `local/feature-tables/` (override with `FEATURE_DIR`).

<!-- verified: 2026-09-24 planned 4,607 runs, sampled 50 runs (1,024 files, 42.55 GiB), downloaded in 9.1 minutes, checked and converted them locally -->
```bash
just feature-plan
just feature-sample 50 3
just feature-download
just feature-check
just feature-convert
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
`feature-sample` now leaves out of the manifest, so the same sample lists 1,012 files. The sample
was drawn when the 3 GiB cap counted required files only; `feature-sample` now counts every file
it downloads, under which one of these runs is 3.04 GiB and the same seed picks a different set.

| check | runs passed | runs failed |
|---|---|---|
| Structural Annotation GFF is the Functional Annotation GFF minus keys | 50 | 0 |
| each hit file's accessions per gene equal the Functional Annotation GFF's (Pfam, COG, TIGRFam, SMART, CATH, SUPERFam, KO_EC) | 50 | 0 |
| KEGG Orthology TSV pairs equal KO_EC Annotation GFF pairs | 50 | 0 |
| Enzyme Commission TSV pairs equal KO_EC Annotation GFF pairs | 50 | 0 |
| Product Names equals Functional Annotation GFF product fields | 50 | 0 |
| feature IDs unique once strand is included | 50 | 0 |
| Functional Annotation GFF contigs all in Contig Mapping File (37 runs have the file) | 37 | 0 |
| every selected row repeats a row of the same caller (column 2, location, score, phase, attributes) | 37 | 13 |

The 13 failures are all 9 sampled v1.0.2 runs and 4 of the 9 v1.0.4 runs; see the exceptions
above. The check matches a selected row to a row of the same caller, because the losing caller's
call at the same interval is a separate observation with its own score. Across the 50 runs, 20,473,880
of 43,518,479 caller rows (47%) are unselected. The KO_EC rows
first failed in those same runs because of the parser, which now splits both packing forms.

Conversion of the 50 runs, rerun 2026-09-24 with the code in this change, took 25 minutes on one
core. It wrote 3.38 GiB of Parquet: 85,834,048 features, of which 23,107,955 are genome features
and 62,726,093 are hits, plus 16,757,384 contigs. No feature ID repeats within a run, 32 were
renamed for the strand, no hit lacked its gene, every hit's `parent` is a genome feature in the
same run, and every contig carries an assembly run. Every run's hit files matched, so every run
dropped all eight accession keys.

## Not done

- Upload to BERDL.
- A run over all 4,607 selected runs. The files recommended for loading total about 5,028 GB.
  `feature-convert` reads from a full download cache; a mode that downloads, converts and deletes
  one run at a time does not exist yet.
- Contig lengths.
- The Gene Phylogeny and Crispr Terms files.
