# How a LinkML description becomes an Iceberg column comment

A column comment in the lakehouse is the description written on a slot in
`nmdc-schema`. It travels through five hops without anyone typing it a second
time, and the last hop is what removed the dominant cost of a reload.

This document describes the mechanism. Decisions about it live in issues.

## The path

1. **A LinkML slot description** in `nmdc-schema`. This is the only place the
   text is authored.
2. **The flattener** reads it with `class_induced_slots`, not `get_slot`, so a
   class-specific `slot_usage` description wins over the schema-level one. See
   `src/nmdc_lakehouse/transforms/schema_generator.py`.
3. **Arrow field metadata** on each field of the table being written.
4. **The Parquet footer**, under the key
   `org.apache.spark.sql.parquet.row.metadata`, which holds Spark's schema as
   JSON with each description as a field `comment`. Written by
   `src/nmdc_lakehouse/sinks/parquet_sink.py`. This is one key inside the
   ordinary Parquet footer; there is no separate Spark footer.
5. **The Iceberg column comment**, created by Spark when the table is created.

Nobody applies step 5. Spark reads the footer key and creates an
already-described table in the commit it was making anyway.

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
documentation said so until 2026-08-27. It is now measured rather than inferred.

What has **not** been established is a supported way to change one column
description on a live table that is not being reloaded. That is the open half,
and it is [#297](https://github.com/microbiomedata/nmdc-lakehouse/issues/297).

## Coverage today

2,036 of 2,059 columns, 98.9%, against `nmdc-schema` 11.23.0. All 23 blanks are
slots with no description upstream, not losses in this pipeline. The figure, the
list, and the command that reproduces it are in
[`nmdc_metadata_tables.md`](nmdc_metadata_tables.md).

## Running any of this yourself

Every step past the Parquet file needs a Spark session in a BERDL pod. Staging a
script and running it there is covered in
[`berdl-upload.md`](berdl-upload.md), along with the two traps that cost the most
time: a local path fails at the executor after the driver has already resolved
the schema, and a bare namespace resolves to `spark_catalog` rather than to the
`nmdc` Iceberg catalog.
