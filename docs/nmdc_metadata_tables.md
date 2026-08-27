# `nmdc_metadata` Silver table reference

`nmdc_metadata` in BERDL contains the 19 schema-defined NMDC MongoDB collections,
flattened to Parquet and registered as managed Iceberg tables. This document describes
the naming conventions, the side-table pattern, and the key join chains. The
[MongoDB connection guide](mongodb-connection.md#maintained-collection-baseline)
records the exact reviewed collection snapshot and selection policy.

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
table exists: the side table is the correct relational form and avoids the
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
| `functional_annotation_agg` | Precomputed `(was_generated_by, gene_function_id, count)`. KEGG.ORTHOLOGY, PFAM, COG only; no EC |

## The annotation → biosample join chain

This is the standard path from a row in `nmdc_results.annotation_kegg_orthology`
(or `annotation_enzyme_commission`) to its originating biosample:

<!-- unverified: no run of this procedure is recorded. Declaring the 81 blocks
     that predate this rule is https://github.com/microbiomedata/nmdc-lakehouse/issues/291 -->
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

<!-- unverified: no run of this procedure is recorded. Declaring the 81 blocks
     that predate this rule is https://github.com/microbiomedata/nmdc-lakehouse/issues/291 -->
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

## Column description coverage

Every column whose LinkML slot has a description carries that description as an
Iceberg column comment, so a data dictionary built from the catalog uses the
wording in the schema itself rather than a second copy that drifts.

A comment can have three provenances, and the difference matters when a column
reads oddly:

| what the comment is made of | columns |
| --- | ---: |
| upstream text only | 461 |
| upstream text with a flattening note appended | 1,527 |
| a flattening note only, because the slot has none | 14 |
| a synthetic `parent_id`, with no slot behind it | 34 |
| no comment at all | 23 |

A note is appended rather than substituted, so most comments mix the two. 1,988
columns carry some upstream text and 461 carry it untouched.

So 2,036 of 2,059 carry a comment and 1,988 carry text authored upstream. The 23
blanks are slots with no description and no flattening note, and none of them is
a description this pipeline lost. See
[`column-description-path.md`](column-description-path.md).

Coverage is **2,036 of 2,059 columns, 98.9%**, measured 2026-08-27 against
`nmdc-schema` 11.23.0. The 23 blanks are not losses in this pipeline. Each one
was checked against the induced slot on its source class, which is the lookup
the flattener performs, and in all 23 the source slot has no description either.
Adding one upstream propagates here on the next regeneration with no code change.

The blanks a consumer is most likely to meet first are `instrument_set.vendor`,
`instrument_set.model`, `data_object_set.url`,
`workflow_execution_set.started_at_time`, and
`workflow_execution_set.ended_at_time`. A blank there reads
as an oversight in the lakehouse rather than in the schema it came from, which is
why the number is stated here rather than rounded to "documented".

The full list, its evidence, and the upstream proposal live in
[#299](https://github.com/microbiomedata/nmdc-lakehouse/issues/299) and
[nmdc-schema #685](https://github.com/microbiomedata/nmdc-schema/issues/685).

To re-measure, read the `comment` of each field in the Spark schema JSON that
the Parquet footer carries under `org.apache.spark.sql.parquet.row.metadata`.
There is no separate Spark footer: that is one metadata key inside the ordinary
Parquet footer, described in
[the footer key reference](mongodb-connection.md).

<!-- verified: 2026-08-27 run against local/mongodb-metadata-20260821_104214,
printed "2036 of 2059 described, 23 blank", which is the figure above. -->

```bash
uv run python - /path/to/completed-snapshot <<'EOF'
import json, pathlib, sys, pyarrow.parquet as pq

KEY = b"org.apache.spark.sql.parquet.row.metadata"
total = blank = 0
without_footer = []
for path in sorted(pathlib.Path(sys.argv[1]).glob("*.parquet")):
    raw = (pq.read_schema(path).metadata or {}).get(KEY)
    if raw is None:
        # Not an error to report as a crash: a pre-footer snapshot, or a directory of
        # unrelated Parquet, is a plausible thing to point this at, and "0 described"
        # would be the wrong answer rather than a refusal.
        without_footer.append(path.name)
        continue
    for field in json.loads(raw)["fields"]:
        total += 1
        blank += not (field.get("metadata") or {}).get("comment")
if without_footer:
    print(f"{len(without_footer)} file(s) carry no {KEY.decode()} key:")
    for name in without_footer:
        print(f"  {name}")
if total:
    print(f"{total - blank} of {total} described, {blank} blank")
else:
    print("no described columns found; is this a completed snapshot?")
EOF
```

## Multi-hop traversal: biosample_to_workflow_run

For variable-depth queries (Biosample to / from any WorkflowExecution),
use the precomputed table `nmdc_metadata.biosample_to_workflow_run`;
see [`biosample_to_workflow_run.md`](biosample_to_workflow_run.md).
Plain equi-join, no recursion at the consumer side.

Ingesting the runtime-maintained `alldocs` MongoDB collection was considered
and rejected; see [`decisions/alldocs-not-ingested.md`](decisions/alldocs-not-ingested.md).
