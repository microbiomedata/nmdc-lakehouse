# Build and query provenance locally

`graph_edges` and `biosample_to_workflow_run` are derived indexes over the
material provenance already present in the metadata tables. They contain no
annotation or taxonomy results.

| Table | Useful for | Work still needed by a query |
| --- | --- | --- |
| Four original provenance side tables | Recovering the source relationships | Combine relationship types, reverse material outputs, and traverse intermediate samples and processes |
| `graph_edges` | Inspecting upstream edges and reconstructing paths | Traverse the graph to connect workflows to biosamples |
| `biosample_to_workflow_run` | Joining biosamples to workflow results, in either direction | An ordinary join on `workflow_run_id` or `biosample_id` |

The four relationships are workflow `was_informed_by`, data-generation
`has_input`, material-processing `has_output` (traversed backwards), and
material-processing `has_input`. This is a material provenance graph, not a
graph of every NMDC relationship. Studies, calibrations, and other associations
remain in their own tables.

The saved mapping answers questions such as which workflows used a biosample,
which biosamples have results reporting a particular taxon or function, and
which associated workflows involved extraction or pooling. The
[mapping guide](biosample_to_workflow_run.md#example-queries) has examples that
join to KO annotations, Kraken2, and GTDBTK results. The three `peek_*_links`
notebooks listed there show related queries. These examples require result
tables to have been loaded as well as metadata.

## Meaning and limits

There is one mapping row per reachable biosample and workflow, with the
minimum number of edges between them. `workflow_type` retains the workflow's
source `type` value. Each processing flag describes **any upstream branch of
the workflow**, not specifically the path to the biosample on that row. This
preserves the aggregation contract of the retired Spark builder.

Pooling can associate one workflow with several biosamples. The mapping does
not allocate a pooled measurement among samples. Summing a workflow's result
after joining to all its biosamples can count that result more than once.
The mapping also does not retain the intermediate path; use `graph_edges`
and the primary tables when that explanation is needed.

## Build from an existing snapshot

Use a checkout with dependencies installed for the source snapshot, as in the
[development setup](development-setup.md) and
[staging runbook](berdl-staging-runbook.md). The following paths are examples;
substitute your snapshot and a new output directory. This step is local and
requires no MongoDB tunnel or BERDL credentials.

<!-- verified: 2026-09-23 ran the equivalent commands on the complete 11.23.0 snapshot and compared all 57,786 pairs -->
```bash
export NMDC_SCHEMA_VERSION=11.23.0
just derive-provenance /path/to/metadata-snapshot /path/to/new-provenance-snapshot
just compare-provenance-queries /path/to/metadata-snapshot /path/to/new-provenance-snapshot /path/to/new-comparison.json --repeats 6
```

The builder reads nine tables: the four edge sources plus `biosample_set`,
`workflow_execution_set`, `data_generation_set`, `processed_sample_set`, and
`material_processing_set`. It checks the input manifest and all artifact
checksums, verifies unique primary IDs and reference endpoints, checks
processing types, and rejects cycles. It walks distinct neighbors with a
visited set per workflow. The default depth bound is 15; `--max-depth` changes
it. Reaching a new node beyond the bound or a workflow with no reachable
biosample fails the build instead of silently losing that lineage.

The new directory must be outside the source snapshot and must not exist.
It contains two Parquet files, `derivation-metrics.json`, and a snapshot
manifest. A failed attempt can leave partial files without a manifest; inspect
the error and use a new output directory after correcting its cause. A directory
without a validated manifest is not a completed derived snapshot.

Both tables and every column have descriptions in Arrow metadata and Spark
comments. Footers record the input snapshot, source schema, derived target
schema version and digest, algorithm, and depth bound. The manifest records
software versions, Git revision/dirty state, checksums, schemas, and row counts;
the metrics record retains the nine input artifact records. The explicit
derived schema is `src/nmdc_lakehouse/schemas/provenance.yaml`, version 1.0.0.
It does not pretend these calculated tables are classes in the flattened
MongoDB schema.

The original snapshot is rechecked before the derived manifest is completed.
No MongoDB dump is repeated, and the original snapshot and staging plan are
unchanged. The Spark rebuild command is retired; use this builder for both
derived tables. Old promotion plans requiring a subsequent rebuild are refused.

## Validate and prepare separate staging evidence

The maintained `validate-target-rows` command selects the packaged provenance
schema for a `derived-provenance-snapshot`. Collection snapshots continue to
use the published flattened schema from `nmdc-lakehouse-schema`. The derived
schema does not depend on the installed source release; the report preserves
the source schema identity and producer package version recorded in the
manifest instead of assigning the validator's installed source version.

Write the report outside the immutable derived snapshot, to a new file:

<!-- verified: 2026-09-23 validated all 194,562 existing derived rows with this command; zero invalid rows -->
```bash
just validate-target-rows /path/to/new-provenance-snapshot /path/to/provenance-validation.json --mode full
```

Validation requires both declared table classes, target schema ID and version,
and the exact packaged schema digest recorded in each Parquet footer. It checks
that both footers name the manifest's parent snapshot, and rechecks snapshot
integrity after reading rows. Full mode validates every row against the LinkML
constraints. Bounded mode uses the existing deterministic sampling rules.
Neither mode recalculates graph reachability or confirms that the parent still
exists on disk; use `compare-provenance-queries` with the parent for the separate
pair-and-hop comparison. The report's snapshot ID binds the parent identity
through the manifest, but is not proof that a parent has been staged or promoted.

The staging planner accepts this report using the same provenance schema.
Follow the [staging runbook](berdl-staging-runbook.md) with the **derived snapshot**
as the input throughout: create a new metadata profile and bundle, inventory
and publication policy, publication plan, metadata application plan, and
staging command plan. Preserve descriptions from the Parquet footers. Choose a
new staging namespace and object prefix; do not append these files to the
already reviewed 46-artifact snapshot or reuse its evidence. If comparing with
an inventory of the full metadata namespace, explicitly preserve its tables
that are absent from this two-table snapshot in the publication policy.

Actual staging and catalog readback remain tracked in
[issue 341](https://github.com/microbiomedata/nmdc-lakehouse/issues/341).
Combined parent-and-derived promotion remains
[issue 234](https://github.com/microbiomedata/nmdc-lakehouse/issues/234).
Planning alone does not modify the lakehouse or authorize canonical promotion.

## Measured query comparison

On 2026-09-23, the complete production snapshot
`sha256:58277b412710e232a448e97cd208d277b1911a7578247891307807edb2c0c000`
produced 136,776 edges and 57,786 biosample/workflow pairs across all 34,821
workflows. The maximum shortest path was 14 edges.

`compare-provenance-queries` executes three equivalent queries over that
snapshot using DuckDB. The first builds an edges CTE from the four source
tables; the second traverses the stored `graph_edges`; the third reads the
saved mapping. It compares every sorted pair **and minimum hop count**, not
just the total number of rows. Processing flags are tested separately with
synthetic branching and pooling examples.

| Approach | Equivalent pairs | Median seconds |
| --- | ---: | ---: |
| Recursive query over four source tables | 57,786 | 0.156406 |
| Recursive query over `graph_edges` | 57,786 | 0.151528 |
| Read saved `biosample_to_workflow_run` | 57,786 | 0.013700 |

The saved mapping was about 11 times faster for this local query. Materializing
edges alone made little difference. A separate direct two-hop join found only
8,431 pairs, missing 49,355; it is not an equivalent shortcut through the
processing chains.

These are six runs with rotating query order, warm operating-system caches,
two DuckDB threads, and a 512 MiB engine memory limit. Timings include sorted
result fetching but exclude manifest checks and view setup. The
[recorded comparison](runs/2026-09-23-provenance-query-comparison.json) records
the engine version, exact SQL, snapshot identities, and every timing. It emits
no production identifiers. It measures complete relationship retrieval, not
a selective biosample lookup or a large annotation-table join, and is **not a
measurement of BERDL latency**. The report must be written outside both
snapshots, to a new file.

The historical
[recursive-query notes](trino_recursive_graph_traversal.md) record Trino
planner/worker failures and Spark recursive SQL limitations. Those reports
explain the motivation for precomputation, but are not a controlled benchmark
of today's platform.
