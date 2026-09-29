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

`functional_annotation_agg` is the only table that shrank. The new count is what the
validated 2026-09-23 dump contains; why production MongoDB held fewer aggregate rows
than the May load has not been investigated.

## Steps and evidence

1. Three earlier previews from runtime `f0cacb6` failed with Spark Connect errors,
   described in
   [Staged-content comparison fails on the first Spark Connect query, then passes on rerun](https://github.com/microbiomedata/nmdc-lakehouse/issues/375). Two
   causes were found in the server log: an intermittent `StackOverflowError` while
   comparing the 1,350-column `biosample_set`, and UNAUTHENTICATED rejections that
   coincided with dropped connections between the Spark server and KBase auth.
2. [Retry Spark calls the BERDL token check refused after losing KBase auth](https://github.com/microbiomedata/nmdc-lakehouse/pull/376) added a
   client-side retry for the second cause. The pod runtime was rebuilt at its merge
   commit with the runbook's setup script.
3. The preview succeeded. During it the server logged 8 token-check rejections at
   22:28:06 UTC; the client resumed the call 5 seconds later and the preview
   finished. No stack overflow occurred.
4. Mark reviewed the printed plan, including the row changes above and the stated
   recovery limits, and approved that exact plan.
5. Mark ran `berdl-promote` in a pod terminal with the three authorization flags
   printed by the preview (see the
   [promotion procedure](../berdl-upload.md#plan-separately-authorized-canonical-promotion)).
   The server logged token-check rejections at 22:58:54 (8), 23:02:33 (7) and
   23:05:01 (2) UTC. The client resumed the first two; the promotion finished
   regardless and its read-back verified all 48 tables.
6. Shortly after the cleanup below, a separate read-only check counted 48 tables in `nmdc.metadata`, matched the plan's counts for
   `biosample_set`, `graph_edges`, `biosample_to_workflow_run` and
   `functional_annotation_agg`, and found 3 snapshots on `biosample_set`.

Recovery is manual. The previous table versions remain as Iceberg snapshots, which
nothing on BERDL expires. Only a same-schema staging replacement has been restored in
practice; multi-table and dropped-table recovery are unproven.

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

- Old snapshots of `nmdc.metadata` (about 0.58 GB): the only way back from this
  promotion.
- `nmdc.nmdc_metadata_staging_20260923_58277b41`,
  `nmdc.nmdc_provenance_staging_20260923_b79eb420` and their upload Parquet under
  `tenant-general-warehouse/nmdc/staging/`: the named sources of this promotion. Keep
  them until the promotion has been in use long enough to accept.

Not decided:

- The legacy Delta copies registered in `spark_catalog`: `nmdc_metadata` (49 tables,
  0.473 GB, last written 2026-04-30), `nmdc_results` (137.471 GB),
  `nmdc_ncbi_biosamples` (9.058 GB) and `nmdc_ref_data`. They predate the May move to
  Iceberg. Who still reads them is unknown, so they stay until that is asked.

Not NMDC's to change: the KBase-owned subsets `kbase.nmdc_arkin`, `kbase.nmdc_mags`
and `kbase.nmdc_neon` (owner `tgu2`), the `globalusers.nmdc_core_test3` and
`nmdc_core_test4` tables, and another user's `u_user233__nmdc` namespace.
