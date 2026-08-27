# Dropping and loading collections selectively

A full NMDC metadata run rebuilds every collection and produces a complete
snapshot suitable for publication. Publishing it to a destination is a separate
operation. That is the right default, because a snapshot is the unit that carries
a manifest, a digest, and a validation result. It is the wrong tool when one
collection changed, when one collection is expensive, or when one collection must
be held back from a reload.

This page covers what is possible today with the maintained commands, and what
the deliberate design gap is. It does not describe incremental extraction or
corrective patches, which are separate and not implemented; see
[#147](https://github.com/microbiomedata/nmdc-lakehouse/issues/147).

## Today: producing Parquet selectively

Every schema collection is a registered job, so a single collection can be
produced on its own:

<!-- unverified: no run of this procedure is recorded. Declaring the blocks
     that predate this rule was
     https://github.com/microbiomedata/nmdc-lakehouse/issues/291, now closed;
     nothing tracks running them. -->
```bash
uv run nmdc-lakehouse list-jobs
just run-job biosample_set
```

To produce everything except named collections, use the aggregate job with
repeatable exclusions. This is how `functional_annotation_agg` is normally held
back, since it is 54.8 million records on a separate loader:

<!-- unverified: no run of this procedure is recorded. Declaring the blocks
     that predate this rule was
     https://github.com/microbiomedata/nmdc-lakehouse/issues/291, now closed;
     nothing tracks running them. -->
```bash
just run-job all-collections --skip functional_annotation_agg
```

`--dry-run` plans a job and writes nothing. `--metrics FILE` records an atomic
performance record. `LAKEHOUSE_ROOT` selects the output directory, so a selective
run can be directed somewhere other than a completed snapshot.

## Today: removing local Parquet selectively

<!-- unverified: no run of this procedure is recorded. Declaring the blocks
     that predate this rule was
     https://github.com/microbiomedata/nmdc-lakehouse/issues/291, now closed;
     nothing tracks running them. -->
```bash
just clean-parquet                 # preview, the default
just clean-parquet --delete        # remove the previewed files
just drop-empty-parquet            # remove zero-row outputs
```

Preview first. `clean-parquet` recognizes only maintained metadata products, so
it will not remove unrelated files, and previewing is how you confirm that before
deleting anything.

## The constraint that matters

**A selective run does not produce a publishable snapshot.** `create-snapshot-manifest`
and `validate-snapshot` describe a complete, self-consistent output set, and
`berdl-upload-plan` requires successful target-schema validation with exact
snapshot and table coverage. Adding one freshly produced table to an already
manifested snapshot directory invalidates it, because the manifest enumerates the
artifacts and their digests.

So selective production is a development and diagnosis tool. Publication remains
snapshot-shaped.

## Today: loading selectively into the destination

Selectivity at the destination is expressed as reviewed dispositions rather than
as a partial upload. The publication plan assigns every candidate and live table
one of `replace`, `add`, `preserve`, `rebuild`, or `retire`, and the staging
namespace is loaded in full before anything canonical is touched.

Two properties follow, and both are enforced rather than conventional:

- A table that exists at the destination but not in the candidate snapshot is a
  live-only table, and plan generation **fails closed** until it is given an
  explicit reviewed disposition. Nothing can be dropped by omission.
- Staging is a separate namespace. A load proves the candidate tables before any
  canonical object is considered.

This is why holding a collection back from a snapshot is safe. `functional_annotation_agg`
is absent from a standard snapshot, so it appears as a live-only table and forces
a `preserve` decision rather than disappearing.

## Short-term strategy

Use selective production for iteration, and full snapshots for publication.

1. Produce the single collection under a scratch `LAKEHOUSE_ROOT` and inspect it.
2. When it is right, run the full `just etl-collections` to get a manifested,
   validated snapshot.
3. Load that snapshot into a staging namespace.
4. Express what should change at the destination through dispositions, giving
   every live-only table an explicit `preserve` or `retire`.

The cost is honest: a one-collection fix still requires a full re-dump before
publication, which is roughly three and a half minutes for the standard set and
about seventeen more if `functional_annotation_agg` is included. That is
affordable now and will not stay affordable as collections grow.

## Long-term strategy

The durable answer is a lifecycle model in which a partial output is a
first-class, publishable object rather than an invalid snapshot. That requires
things this repository deliberately does not yet have:

- **Snapshot lineage**, so a partial output can name its parent and its scope.
- **Change identity**, so additions, updates, and deletions are representable.
  Selective reproduction cannot express a deletion today, which is the deeper
  reason a partial run is not publishable.
- **Idempotent application**, so replaying a patch twice yields the same state.
- **Schema-version compatibility checks** before a patch is applied.
- **Compaction**, so accumulated patches periodically become a new full snapshot.

[#147](https://github.com/microbiomedata/nmdc-lakehouse/issues/147) owns that
model and states its first closable slice. Until it lands, treat "incremental" as
undefined rather than assuming it means append.

One sequencing note. Selective *loading* also depends on the destination
supporting per-table replacement with tested recovery, which is the open question
in [#240](https://github.com/microbiomedata/nmdc-lakehouse/issues/240) and
[#234](https://github.com/microbiomedata/nmdc-lakehouse/issues/234). The first
live probe run against BERDL, reported in
[this evidence comment](https://github.com/microbiomedata/nmdc-lakehouse/issues/240#issuecomment-5358756245),
shows per-table replacement works and that a verified recovery operation exists,
and that promotion is not atomic across tables. A selective load is therefore closer to
reach than a selective snapshot.
