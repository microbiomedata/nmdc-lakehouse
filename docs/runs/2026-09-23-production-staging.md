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

The actual staging helper has been prepared and uploaded. It rechecks the exact
plan, checks that the staging namespace and bronze prefix are unused, and calls
the maintained CLI with the recorded snapshot and plan authorization. It keeps
the outer Spark session alive, reports progress every 30 seconds, and preserves
stdout, diagnostics, and immutable outcomes. Its Python syntax and uploaded
bytes were checked; live execution remains pending in this record.

<!-- unverified: live staging execution pending; tracked in https://github.com/microbiomedata/nmdc-lakehouse/issues/136 -->
```bash
ipython ~/stage-nmdc-20260923-58277b41.py
```

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

Still outstanding: the live staging/data/metadata outcomes; the source preservation
audit in issue #347; namespace metadata support in
[issue #114](https://github.com/microbiomedata/nmdc-lakehouse/issues/114); and any
separately authorized canonical promotion. The transport parts remain available
and should be accounted for during cleanup after the run.

[Issue #353](https://github.com/microbiomedata/nmdc-lakehouse/issues/353) records
the measured simplification target: replace these dated helpers and repeated
path/hash handoffs with a maintained prepare/execute/status interface that reuses
the current checks and can resume completed phases.
