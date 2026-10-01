# Production promotion and cleanup record: 2026-09-28

This records the first combined promotion into `nmdc.metadata` and the cleanup that
followed. Use the [staging runbook](../berdl-staging-runbook.md) and the
[promotion procedure](../berdl-upload.md#plan-separately-authorized-canonical-promotion)
for the workflow. The sources were staged as described in the
[2026-09-23 record](2026-09-23-production-staging.md). Acceptance was tracked in
[Promote a verified BERDL staging namespace with tested recovery](https://github.com/microbiomedata/nmdc-lakehouse/issues/234),
closed with this outcome.

## Identity

| Item | Recorded value |
| --- | --- |
| Canonical namespace | `nmdc.metadata` |
| Destination ID | `nmdc-production` |
| Approved plan SHA-256 | `e33368dade560835a4c7fe4481d785d28ccb9c704575f2d465a962a6bb62f2eb` |
| Metadata source | `sha256:58277b412710e232a448e97cd208d277b1911a7578247891307807edb2c0c000` in `nmdc.nmdc_metadata_staging_20260923_58277b41` |
| Derived source | `sha256:b79eb4208c3d6e2883223142e5ac1c7b753ce9c911f464e4cf7bc5a3290d9410` in `nmdc.nmdc_provenance_staging_20260923_b79eb420` |
| Pod runtime | `25871c220cb5f2d95c4f887d4e23b756d090479d`, built with `scripts/python/setup_pod_runtime.py` |
| Official ingest | `a76bb7a24a42f0c9212fda8b9ab0bd3b637645d3` |
| Spark | 4.0.1 (Spark Connect in the BERDL pod) |
| Preview | 2026-09-28 22:24:38 to 22:28:52 UTC |
| Promotion | 2026-09-28 22:56:06 to 23:05:46 UTC, status `promotion-verified` |

## What changed in `nmdc.metadata`

The table count went from 54 to 48: 45 tables replaced, 3 added and 9 dropped.

- Added: `biosample_to_workflow_run` (57,786 rows),
  `configuration_set_ordered_mobile_phases_substances_used` (34) and
  `material_processing_set_ordered_mobile_phases_substances_used` (16,370).
- Dropped: the nine obsolete TextValue helpers `biosample_set_agrochem_addition`,
  `_air_temp_regm`, `_fertilizer_regm`, `_gaseous_environment`, `_host_diet`,
  `_humidity_regm`, `_perturbation`, `_phaeopigments` and `_watering_regm`
  (3,571 rows in total).
- 36 replaced tables kept the same row count. The others changed as follows.

| Table | Rows before | Rows after | Change |
| --- | --- | --- | --- |
| `data_object_set` | 291,980 | 302,053 | +10,073 |
| `workflow_execution_set_has_input` | 120,990 | 131,939 | +10,949 |
| `workflow_execution_set_has_output` | 259,244 | 269,050 | +9,806 |
| `workflow_execution_set_mags_list` | 73,558 | 75,594 | +2,036 |
| `workflow_execution_set_was_informed_by` | 33,675 | 35,112 | +1,437 |
| `graph_edges` | 135,339 | 136,776 | +1,437 |
| `workflow_execution_set` | 33,430 | 34,821 | +1,391 |
| `data_generation_set_has_output` | 26,373 | 26,640 | +267 |
| `functional_annotation_agg` | 53,182,890 | 51,744,347 | -1,438,543 |

`functional_annotation_agg` is the only table that shrank. A read-only comparison of
its previous and current Iceberg snapshots on 2026-09-29 accounts for all of it: the
1,438,543 missing rows belong to exactly 64 `nmdc:MetagenomeAnnotation` runs
(`was_generated_by`), and the other 4,190 runs have identical row counts. All 64 were
in the previous `workflow_execution_set` snapshot and are absent from the current one,
and none has another version under the same base identifier. So the rows were not
lost in promotion: both exported tables consistently lack these runs in the
2026-09-23 dump. Whether production MongoDB removed them, or the export missed them,
was not checked against MongoDB; that question belongs to the source-preservation
audit in [Match production exports to the deployed source schema version](https://github.com/microbiomedata/nmdc-lakehouse/issues/347).

## Steps and evidence

1. The derived pair was staged first, from runtime `f0cacb6`, with the reviewed
   `stage-publication` command and staging plan
   `fcb3a149e3665d0217932f50a1cf26518e0492a86d2d285238c6dabf309fba7d`. The upstream
   ingest ran from 18:49:13 to 18:49:32 UTC into
   `nmdc.nmdc_provenance_staging_20260923_b79eb420` (Parquet under
   `tenant-general-warehouse/nmdc/staging/20260923_b79eb420`). Its ingest outcome
   reports `verified` (SHA-256 `474f50661110d387d6e5f32cd94e8a5fb0cbdb03149568626b24144e2ab209a7`),
   the staging outcome `data-verified`
   (`fd2744faaa96b6ec1ecc765bef11f6a827ec86c1d5158bbdcdd15034d0d9234f`) and the
   metadata outcome `metadata-verified`
   (`46c4ba73ce236883c5b2d32d64938c558b1fca3a3dd02677ca988f8b09e85331`), with 4
   namespace operations deferred. The two tables read back as 136,776 `graph_edges`
   and 57,786 `biosample_to_workflow_run` rows, the counts the preview later planned.
2. Three earlier previews from runtime `f0cacb6` failed with Spark Connect errors,
   described in
   [Staged-content comparison fails on the first Spark Connect query, then passes on rerun](https://github.com/microbiomedata/nmdc-lakehouse/issues/375). Two
   causes were found in the server log: an intermittent `StackOverflowError` while
   comparing the 1,350-column `biosample_set`, and UNAUTHENTICATED rejections that
   coincided with dropped connections between the Spark server and KBase auth.
3. [Retry Spark calls the BERDL token check refused after losing KBase auth](https://github.com/microbiomedata/nmdc-lakehouse/pull/376) added a
   client-side retry for the second cause. The pod runtime was rebuilt at its merge
   commit with the runbook's setup script.
4. The preview succeeded. During it the server logged 8 token-check rejections at
   22:28:06 UTC; the client resumed the call 5 seconds later and the preview
   finished. No stack overflow occurred.
5. Mark reviewed the printed plan, including the row changes above and the stated
   recovery limits, and approved that exact plan.
6. Mark ran `berdl-promote` in a pod terminal with the three authorization flags
   printed by the preview (see the
   [promotion procedure](../berdl-upload.md#plan-separately-authorized-canonical-promotion)).
   The server logged token-check rejections at 22:58:54 (8), 23:02:33 (7) and
   23:05:01 (2) UTC. The client resumed the first two; the promotion finished
   regardless and its read-back verified all 48 tables.
7. Shortly after the cleanup below, a separate read-only check counted 48 tables in `nmdc.metadata`, matched the plan's counts for
   `biosample_set`, `graph_edges`, `biosample_to_workflow_run` and
   `functional_annotation_agg`, and found 3 snapshots on `biosample_set`.

The promotion's recovery files are retained in the pod at
`/home/mamillerpa/nmdc-promotion-20260928-4/`: the approved plan
`combined-promotion.json`, and under `combined-promotion.execution/` the saved before
state `before.json` (identical to the plan, SHA-256 `e33368dade560835a4c7fe4481d785d28ccb9c704575f2d465a962a6bb62f2eb`),
the journal of 57 numbered `attempt`/`verified` pairs (`000` to `056`) and
`outcome.json` (SHA-256 `c2a39e68170c5c4e0dc18fa912b839302ad97c00fd88672553764126bbf3125d`).
A checksum list of every file is `recovery-artifacts.txt` in the same folder. A copy
of the folder was also taken off the pod to a workstation (archive SHA-256
`b5b8c5533e9facc8c248b1e07724541dc5100def7e872df5e97ec20d0752f9ca`).

Recovery is manual, and its basis is the retained staging copies: the two staging
namespaces and their upload Parquet, kept until a newer promotion replaces them. Iceberg
snapshots of `nmdc.metadata` are not a reliable way back. No scheduled job on BERDL
expires them (KBase staff, NMDC Slack `#ber_lakehouse`, 2026-09-02), but a platform
operation that recreates a table discards its history, as the 2026-09-30 sync did for
32 tables (see below). Only a same-schema staging replacement had been restored in
practice before that reload; multi-table and dropped-table recovery from snapshots are
unproven.

## Cleanup of earlier copies

A read-only inventory listed every catalog, namespace and storage folder holding NMDC
metadata. A second read-only check listed the storage folder behind every file and
manifest, across all snapshots, of every remaining `nmdc` table. Each table referenced
only its own folder, so no remaining table depended on the items below. After a dry
run matched the approved list, the cleanup ran on 2026-09-28 from 23:59:06 to
23:59:19 UTC with no delete errors and no objects left in any target folder.

Deleted, with Mark's approval:

| Item | Objects | GB |
| --- | --- | --- |
| Namespace `nmdc.nmdc_metadata_staging_20260908` (53 tables) and its files | 331 | 0.279 |
| Namespace `nmdc.nmdc_metadata_staging_20260824` (53 tables) and its files | 325 | 0.281 |
| Files of the abandoned 2026-08-20 staging run, already absent from the catalog | 833 | 22.798 |
| Ten probe and scratch folders with no catalog namespace | 1,025 | 0.072 |
| Upload Parquet for the 20260820, 20260824 and 20260908 staging runs | 167 | 1.408 |

Kept, and why:

- Old snapshots of `nmdc.metadata` (about 0.58 GB). They are not a reliable way back:
  on 2026-09-30 a platform sync recreated 32 tables, which discarded their earlier
  snapshots (see below).
- `nmdc.nmdc_metadata_staging_20260923_58277b41`,
  `nmdc.nmdc_provenance_staging_20260923_b79eb420` and their upload Parquet under
  `tenant-general-warehouse/nmdc/staging/`: the named sources of this promotion, and
  the copy the 2026-09-30 reload came from. Keep them until a newer promotion replaces
  them.

Not decided:

- The legacy Delta copies registered in `spark_catalog`: `nmdc_metadata` (49 tables,
  0.473 GB, last written 2026-04-30), `nmdc_results` (137.471 GB),
  `nmdc_ncbi_biosamples` (9.058 GB) and `nmdc_ref_data`. They predate the May move to
  Iceberg. Who still reads them is unknown, so they stay until that is asked.

Not NMDC's to change: the KBase-owned subsets `kbase.nmdc_arkin`, `kbase.nmdc_mags`
and `kbase.nmdc_neon` (owner `tgu2`), the `globalusers.nmdc_core_test3` and
`nmdc_core_test4` tables, and another user's `u_user233__nmdc` namespace.

## 2026-09-30: a platform sync overwrote 32 tables, and the reload

On 2026-09-30 a KBase Delta-to-Iceberg sync recreated 32 `nmdc.metadata` tables from the
legacy `spark_catalog.nmdc_metadata` Delta copy, last written 2026-04-30. KBase reported it
in the NMDC `#ber_lakehouse` channel the same day.

Measured read-only at 19:02 UTC:

- Exactly 32 tables were recreated, between 17:41:59 and 17:44:30 UTC: 30 of the 46
  metadata tables plus `graph_edges` and `biosample_to_workflow_run`. The 16 unchanged
  tables were `biosample_set_chem_administration`, `biosample_set_misc_param`,
  `collecting_biosamples_from_site_set`, `configuration_set`,
  `configuration_set_ordered_mobile_phases`, both `*_ordered_mobile_phases_substances_used`
  tables, `field_research_site_set`, `functional_annotation_set`, `genome_feature_set`,
  `organism_sample_set`, `organism_set`, `storage_process_set`, `study_set_part_of`,
  `study_set_protocol_link` and `study_set_study_image`.
- Each recreated table held the April row counts (for example `biosample_set` 16,640
  instead of 27,352) and had a single snapshot, so its earlier Iceberg history was gone.
  Its table and column descriptions and the `nmdc_lakehouse.*` properties were gone, and
  its owner had changed.
- `nmdc.results`, `nmdc.ref_data`, `nmdc.ncbi_biosamples` and both staging namespaces
  were unchanged.

The approved plan could not be reused, because `berdl-promote` refuses a plan whose saved
before state no longer matches the live tables. A fresh preview from the same two staging
runs, on runtime `25871c2`, gave plan
`b5f0f7cae528fc1e5124d343e9205924ee1756007a6202130e182bea31b86e63`: 48 replacements, no
drops, every row count equal to the September 28 plan. Mark approved it and ran
`berdl-promote` from 21:17:54 to 21:27:10 UTC; status `promotion-verified`. The plan, journal
and outcome are retained in the pod under `/home/mamillerpa/nmdc-promotion-20260930-reload/`.

A separate read-only check at 21:27:58 UTC found all 48 tables matching the plan: row
counts, table descriptions, column-description counts and the `nmdc_lakehouse.*`
properties, and every table equal in content to its staging copy by a one-column row-hash
comparison in both directions. All 48 tables are now owned by the account that ran the
promotion: a replacement made by this tool resets the owner.

What could not be restored: the earlier snapshot history of the 32 recreated tables.
The legacy `spark_catalog` Delta copies that were the sync's source still exist; retiring
them is part of
[Plan retirement of historical NMDC lakehouse copies across catalog and storage](https://github.com/microbiomedata/nmdc-lakehouse/issues/367).

