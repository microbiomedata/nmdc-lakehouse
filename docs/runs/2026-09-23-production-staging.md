# Production export and BERDL staging record: 2026-09-23

This records Mark's run and the evidence behind it. Use the
[staging runbook](../berdl-staging-runbook.md) for the workflow and
[issue #136](https://github.com/microbiomedata/nmdc-lakehouse/issues/136) for live
acceptance. The dated helper names below are run-specific files uploaded to
Mark's pod, not installed project commands or a portable bootstrap interface.

## Candidate identity

| Item | Recorded value |
| --- | --- |
| Snapshot | `sha256:58277b412710e232a448e97cd208d277b1911a7578247891307807edb2c0c000` |
| Source schema | `11.23.0` |
| Flat-schema package | `nmdc-lakehouse-schema==0.5.0` |
| Target schema | `11.23.0+flat.1.3.0` |
| Collections | All 19 eligible collections, none skipped |
| Parquet artifacts | 46: 19 primary tables and 27 helper tables |
| Rows | 52,206,530 primary rows; 53,217,239 including helper rows |
| Full target validation | 53,217,239 selected; zero invalid |
| Parquet bytes | 464,128,154 |
| Export code | `ab992739f71adcbd49cf69c9517e54d92aac5166` |
| Staging code | PR #352 merge, `23aa4819466effd15ab93e9c9c8876e0224f7e68` |
| Official ingest | v0.1.5, `a76bb7a24a42f0c9212fda8b9ab0bd3b637645d3` |
| Reviewed staging plan SHA-256 | `d400e2bd3715feefbbaf3bc7d5c66555f4c4301a56236a4ff16f38009c7d611d` |
| Staging namespace | `nmdc.nmdc_metadata_staging_20260923_58277b41` |
| Object destination | Bucket `cdm-lake`, prefix `tenant-general-warehouse/nmdc/staging/20260923_58277b41` |

The source was read sequentially, not at one database-wide transaction snapshot.
Schema conformance and staging verification do not replace the remaining focused
source preservation audit in
[issue #347](https://github.com/microbiomedata/nmdc-lakehouse/issues/347).

## Verified steps and the failure encountered

1. Production source preflight accepted the installed 11.23.0 source/flat pair.
   The merged compatibility check derives schema migrations that do not alter
   documents from the installed package; it does not maintain a separate version
   history in this repository.
2. `just etl-collections` completed in about 25 minutes. Snapshot integrity
   passed. The subsequent full target-row validation passed in about 9.6 minutes.
   LinkML's informational metamodel-inlining messages were not row failures.
3. The approved profile and schema-derived bundle described all 46 tables and
   1,994 of 2,017 columns. The 23 missing descriptions were retained as explicit
   gaps rather than filled with invented text.
4. A 367,364,258-byte archive was transferred through the Hub contents API in
   four parts. Its SHA-256 is
   `b385fdf82181138d438cc8bdf12294353b248f87727344340b9a63af74e6bb8f`.
   It contained no credentials or AppleDouble siblings. The pod helper checked
   each part, the complete archive, all 46 Parquet hashes, and the saved validation
   report. It did not rerun the dump or row validation.
5. The read-only inventory completed at `2026-09-23T23:04:01.685079+00:00` and
   observed 54 Iceberg tables in `nmdc.metadata`.
6. The first environment preparation attempt, logged as
   `planning-20260923T231542Z.log`, verified all eight input checksums, confirmed
   Python 3.13.9, and created both pinned worktrees. Installing uv then failed
   because pip inherited a user-install default: `User site-packages are not
   visible in this virtualenv`.
7. The helper was corrected to use `pip install --no-user`. Offline reproductions
   of both an environment-variable default and a pip config-file default failed
   before that option and passed a no-index dry run with it. Global pip
   configuration was not changed.
8. The retry, logged as `planning-20260923T232125Z.log`, installed uv 0.12.17 in
   an isolated tool environment, created a virtualenv with the pod runtime
   available, installed the locked 11.23.0 source pair, checked runtime imports,
   and passed snapshot validation and portable preflight. It generated the
   pod-bound plan and completed the data/metadata preview.
9. The retrieved plan matched the operator-reported SHA-256 above. Its 46
   artifact hashes and row counts matched the original manifest, and all six
   bound evidence-file hashes matched the reviewed copies.

These commands ran in Mark's BERDL JupyterHub pod terminal:

<!-- verified: 2026-09-23 pod transfer checks and 54-table inventory completed. -->
```bash
python3 ~/prepare-nmdc-20260923-58277b41.py
```

<!-- verified: 2026-09-23 the corrected helper completed environment setup and staging preview. -->
```bash
bash ~/plan-nmdc-20260923-58277b41.sh
```

The staging helper completed successfully. It rechecks the exact
plan, checks that the staging namespace and bronze prefix are unused, and calls
the maintained CLI with the recorded snapshot and plan authorization. It keeps
the outer Spark session alive, reports progress every 30 seconds, and preserves
stdout, diagnostics, and immutable outcomes. Its uploaded bytes and the returned
outcomes were checked against the reviewed local copies.

<!-- verified: 2026-09-24 the pod helper completed staging and data/table-metadata verification for all 46 tables. -->
```bash
ipython ~/stage-nmdc-20260923-58277b41.py
```

## Verified staging execution

A subsequent inspection through the operator's signed-in Chrome session found
the plan and preview but none of the three expected outcome files. That
inspection did not query the catalog or object prefix, so missing files alone
did not establish that the destination was empty. Command-line requests had
returned HTTP 403; the successful browser inspection showed why that response
was insufficient evidence of pod unavailability.

The pod helper matched SHA-256
`5b35e654e9832fb449d22b7d415eac8a91bc20384e5b3b15dbc853b9c7de9c72`,
and the pod plan matched the reviewed digest above. After the operator approved
execution, the browser operator reported launching the helper once in a new
JupyterLab terminal. The helper's destination checks run before any upload.
The retained destination check confirms that the namespace was absent and the
object prefix empty before execution.

The run started at `2026-09-24T01:31:52Z` and completed both phases at about
`01:34:54Z`. The upload pipeline reported 46 tables, zero errors, and about
109 seconds of pipeline time. The complete operation included destination
checks, source-versus-catalog verification, and metadata application/read-back.

| Verification | Result |
| --- | --- |
| Combined status | `data-and-table-metadata-verified` |
| Source and destination artifact rows | 53,217,239 across all 46 tables |
| Table descriptions | 46 verified |
| Column descriptions | 1,994 verified; all already correct from ingestion |
| Table schema/snapshot properties | Verified for all 46 tables |
| Missing column descriptions | 23, as recorded in the reviewed plan |
| Deferred namespace operations | Nine, as recorded in the reviewed plan |

All seven retrieved evidence files matched their reported pod SHA-256 digests.
A local check then reconstructed the expected data outcome from the reviewed
plan and upstream outcome, checked the data/metadata hash bindings, verified the
exact table and column coverage, and compared the combined result with both
immutable outcomes and the original preview. That local check validates the
returned evidence; the live catalog and object-store checks were performed by
the pod execution path.

The checked cross-file bindings are explicit: the data outcome's
`staging_plan_sha256` equals the reviewed plan digest in the candidate table,
and its `upstream_outcome_sha256` equals the upstream file digest below.
The metadata outcome's `staging_outcome_sha256` equals the data file digest
below. Its `metadata_plan_sha256` is
`9d4bf2db954744c36629a541bb33495b5cd3c2ee0566fb8d74d475a14397dd5c`,
matching both the retained `metadata-application-plan.json` and that file's
binding inside the reviewed staging plan.

| Retrieved evidence file | SHA-256 |
| --- | --- |
| `execution-preview-20260924T013152772832Z.json` | `fa894619c216887716d1bb83e7899639f218bf0cf66c302f26a478df3deaed88` |
| `staging-destination-check-20260924T013152772832Z.json` | `6c686c3d034449b57ff7ebe4fcbc898ecd6a1a4c9a6f7a1cb8469bd9cccce608` |
| `staging-20260924T013152772832Z.log` | `4d2d79187e0b7239ecf1057ade2ddf8619613c7688c4aab0f04108c4b70efb65` |
| `kbase-ingest-outcome.json` | `c39eaf5a4292430199e75eba1dbfc81d87288c7d73278aa97818e9e973371f27` |
| `nmdc-staging-outcome.json` | `d109783f1dcda2890c4f5f2529a601d0cbcc2b47477fd48cf90a7ae79bc7791f` |
| `nmdc-staging-metadata-outcome.json` | `eb083bf61ff231927897a2e2c8cc08ddf062dead1d84dd8c2dadc051ab061677` |
| `staging-result-20260924T013152772832Z.json` | `9a5a27a3b1cedaf09147b06c3a04e8feed6d89454b5bf4ccd0f8c00359be5484` |

The outcome copies, destination check, preview, and run log are retained in the
Mac evidence directory under `pod-execution-readback/`. The corresponding pod
evidence remains in its original run directory. A telemetry-upload warning did
not prevent data or metadata verification. No canonical promotion was performed.

Keep browser operation and API operation distinct when diagnosing access. Use
the authenticated interface that works, retain credentials in that interface,
and report the exact failed route. Do not treat an API error as proof that the
operator's browser session is unavailable.

## Separate derived provenance candidate

The two derived provenance tables use their own snapshot and the independent
schema documented in [local provenance](../local-provenance.md). They are not
additional files in the 46-table snapshot or its already reviewed staging plan.

| Item | Recorded value |
| --- | --- |
| Derived snapshot | `sha256:b79eb4208c3d6e2883223142e5ac1c7b753ce9c911f464e4cf7bc5a3290d9410` |
| Parent | The `58277b41` metadata snapshot identified above |
| `graph_edges` | 136,776 rows |
| `biosample_to_workflow_run` | 57,786 rows |
| Full target validation | 194,562 selected; zero invalid |
| Target schema | `https://w3id.org/nmdc/lakehouse/provenance`, version `1.0.0` |
| Description coverage | Both tables and all 15 columns |
| Preparation code | PR #356 merge, `2ed4b7c30a1a250f1314d59b69962ed164b8235e` |
| Proposed staging namespace | `nmdc.nmdc_provenance_staging_20260923_b79eb420` |

The derived transfer archive contains the unchanged snapshot, full validation
report, approved profile and metadata bundle, exact provenance schema, and
historical local planning evidence. It is 3,101,393 bytes with SHA-256
`b483cc0f37aeb61a8fd7bcd272ecdd2a2e51df884ef85fcccbaaa6fa60e8937f`.
A local extraction verified all 14 member hashes, snapshot integrity, and the
existing validation report. No repeat MongoDB dump or full row validation is
required to transfer those bytes.

The archive's `evidence/local-preview` files are historical observations and
plans. Before execution, collect a fresh destination inventory and construct a
separate pod-bound plan using the merged validator. Preserve every canonical
table outside the two-table candidate, and check that its staging namespace and
object prefix are unused. The original run's pinned checkout and immutable plan
remain separate. Namespace metadata operations are still deferred; the parent
identity is retained in the manifest and evidence rather than claimed as a
verified live namespace property.

## What the plan includes

The comparison to canonical data contains 44 shared table names, two added
nested mobile-phase substance helper tables, and ten preserved canonical-only
tables. The two additions are
`configuration_set_ordered_mobile_phases_substances_used` and
`material_processing_set_ordered_mobile_phases_substances_used`.

The preserved objects are nine older biosample helper tables (`agrochem_addition`,
`air_temp_regm`, `fertilizer_regm`, `gaseous_environment`, `host_diet`,
`humidity_regm`, `perturbation`, `phaeopigments`, and `watering_regm`, each prefixed
with `biosample_set_`) and `graph_edges`. Those dispositions do not execute a
canonical replacement. Review their eventual retirement, preservation, or
rebuild and consumer impact before any separately authorized promotion.

The preview requires 46 table descriptions and 1,994 column descriptions, plus
table snapshot and target-schema properties. It reports the 23 missing column
descriptions and nine deferred namespace operations. Registry and tenant writes
are not implemented. The metadata bundle retains the richer schema annotations.

## Evidence locations and remaining work

These absolute paths are specific to Mark's Mac and pod account:

| Location | Contents |
| --- | --- |
| Mac: `/Users/mam/gitrepos/nmdc-lakehouse-schema-adoption/local/nmdc-11.23.0-20260923_171151/` | Immutable snapshot |
| Mac: same checkout, `local/target-validation-11.23.0-20260923_174155.json` | Original full validation report |
| Mac: same checkout, `local/berdl-staging-evidence-20260923-58277b41/` | Profile, bundle, plans, logs, helper versions and read-back copies, transfer receipts |
| Mac: same checkout, `local/session-records/2026-09-23/` | 138 moved temporary records, two preserved coverage files, hash-verified relocation map |
| Pod: `/home/mamillerpa/nmdc-stage-20260923-58277b41/` | Snapshot, evidence, two pinned worktrees, isolated tool environment |

The clean merged development worktree under `/private/tmp` was retired after
preserving its evidence. The uploaded helper files are in the pod home. No
credential, production row, or private connection string belongs in the tracked
documentation or issue comments.

Still outstanding: separate derived-provenance staging; the source preservation
audit in issue #347; namespace metadata support in
[issue #114](https://github.com/microbiomedata/nmdc-lakehouse/issues/114); and any
separately authorized canonical promotion. The transport parts remain available
and should be accounted for during cleanup after the run.

[Issue #353](https://github.com/microbiomedata/nmdc-lakehouse/issues/353) records
the measured simplification target: replace these dated helpers and repeated
path/hash handoffs with a maintained prepare/execute/status interface that reuses
the current checks and can resume completed phases.
