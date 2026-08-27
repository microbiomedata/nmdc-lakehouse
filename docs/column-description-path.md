# How a LinkML description becomes an Iceberg column comment

A column comment in the lakehouse is the description written on a slot in
`nmdc-schema`, plus any note the flattener adds when flattening changes what the
column means. For 14 columns the slot has no description and the note is the
whole comment. Nobody types either a second time, and the last hop is what
removed the dominant cost of a reload.

This document describes the mechanism. Decisions about it live in issues.

## The path

It branches rather than running in a line, which matters when you are working
out where a description went missing.

1. **A LinkML slot description** in `nmdc-schema`, where most of the text is
   authored.
2. **The flattener** reads it with `class_induced_slots`, not `get_slot`, so a
   class-specific `slot_usage` description wins over the schema-level one. It
   also **writes text of its own**, appending a note when flattening changes what
   a column means. See `src/nmdc_lakehouse/transforms/schema_generator.py`:

   - `Reference by identifier; original range was class '<range>'.`
   - `Flattened from nested slot '<parent>.<inner>'.`
   - `Polymorphic subclass-specific slot (from '<subclass>').`

   Each is appended to the upstream description, so when the upstream slot has
   none the note becomes the whole comment. That is why a catalog comment can
   differ from its source slot with nothing wrong, and it is the first thing to
   check when one does.
3. **`class_def_to_arrow_schema()` writes it in two places at once**, in
   `src/nmdc_lakehouse/sinks/parquet_sink.py`:
   - as Arrow field metadata under the key `nmdc_lakehouse.description`, and
   - directly into the Spark schema JSON, as that field's `comment`.

   The second is not derived from the first. Both are read from the same
   flattened attribute, so they can only disagree if that function changes.
4. **The Parquet footer** carries that JSON under
   `org.apache.spark.sql.parquet.row.metadata`. This is one key inside the
   ordinary Parquet footer; there is no separate Spark footer.
5. **The Iceberg column comment**, created by Spark when the table is created.

Nobody applies step 5. Spark reads the footer key and creates an
already-described table in the commit it was making anyway.

The one place the JSON is rebuilt from the Arrow metadata is pruning:
`with_spark_schema()` regenerates the footer entry from the fields a schema
actually has, because a stale entry naming a dropped column is worse than none,
since Spark would ask for data the file does not contain. So a description that
survives in Arrow metadata but is missing from the footer points at that
rebuild, not at the flattener.

## What it costs, and what it replaced

Verified on BERDL 2026-08-24, in the pod, against `biosample_set.parquet` from
the 2026-08-21 snapshot:

```
BASELINE dataframe_columns=1402 with_comment_in_schema=1393
ANSWER-1 columns_described=1393 of 1402
ANSWER-2 metadata_commits=1
ANSWER-3 id_comment='An NMDC assigned unique identifier for a biosample submitted to NMDC.'
ANSWER-4 rowcount=27352
```

One commit. Not 1,393.

The path it replaced applied one `ALTER TABLE ... ALTER COLUMN ... COMMENT` per
column, and at `biosample_set`'s width that path does not merely run slowly, it
stops. Measured 2026-08-20: 560 columns applied, then 833 raised
`RESTException`, 560 + 833 = 1,393 exactly.

Batching the `ALTER` statements into one schema update is the obvious repair and
it does not work. A previous session measured it on 2026-08-20 in
`nmdc.commentbench_probe_20260820`:

| form | described | commits |
| --- | ---: | ---: |
| grouped | **0 of 120** | 1 |
| one at a time | 120 of 120 | 101 |

The grouped form produced one commit and applied no comments at all, silently.
Read that result before proposing a batching design; see
[#297](https://github.com/microbiomedata/nmdc-lakehouse/issues/297).

For the whole namespace the difference is the difference between hours and
seconds. A full 53-table run on 2026-08-24 cost **0 column writes and about 40
seconds**, because every description was already present and
`_read_column_descriptions` found it. The same step had run for 117 minutes and
failed on 2026-08-20.

## What is verified, and what is not

The footer key and the agreement between its comments and the slot descriptions
are covered by tests in this repository. Spark turning those comments into
catalog column comments was unobserved until the 2026-08-24 probe above, and the
documentation said so until 2026-08-27, in this file, in
[`mongodb-connection.md`](mongodb-connection.md), and in a comment beside the
footer key in `src/nmdc_lakehouse/sinks/parquet_sink.py`. All three now record
the measurement instead.

Changing one column description on a live table that is not being reloaded is
**not supported**, decided 2026-08-27 in
[#297](https://github.com/microbiomedata/nmdc-lakehouse/issues/297) and enforced
since: `berdl-apply-metadata` refuses a target that is not a staging namespace,
naming the 560-then-833 failure and pointing here. Reload into a fresh staging
namespace instead, where the descriptions arrive in the footer at no extra
cost.

## Coverage

2,036 of 2,059 columns, 98.9%, measured 2026-08-27 against `nmdc-schema`
11.23.0. All 23 blanks are slots with no description upstream, not losses in
this pipeline. The figure, the list, and the command that reproduces it are in
[`nmdc_metadata_tables.md`](nmdc_metadata_tables.md).

**14 of the 2,036 carry only a generated note**, with no upstream text behind
them, because the slot they came from has no description and the flattening note
is all that is left. `data_object_set.was_generated_by` reads as a reference
note, and `protocol_link_url` on three tables reads as a nesting note. So 2,022
columns carry authored text, and 98.9% measures whether a column has a comment
rather than whether the schema supplied one.

## Running any of this yourself

Checking the catalog side needs a Spark session in a BERDL pod, because reading
back a column comment means asking the catalog. Everything up to the Parquet
file, and the offline commands in the publication sequence, need nothing.
Staging a script and running it in the pod is covered in
[`berdl-upload.md`](berdl-upload.md), along with the traps that cost the most
time: a local path fails at the executor after the driver has already resolved
the schema, a bare namespace resolves to `spark_catalog` rather than to the
`nmdc` Iceberg catalog, and bulk data should go to object storage with `mc`
rather than through the pod at all.
