# Select the source schema and roll out the flat projection

The default source is the latest reviewed tagged release, **11.24.0**. Set
`NMDC_SCHEMA_VERSION=11.23.0` to select the production compatibility pair.
The same schema-owned projection engine supports both; each source has its own
generated artifact and recorded identity.

| Source selection | Target version | Tables |
| --- | --- | --- |
| 11.24.0 (default) | `11.24.0+flat.1.3.0` | 61: 19 primary, 42 helpers |
| 11.23.0 | `11.23.0+flat.1.3.0` | 60: 19 primary, 41 helpers |

The consumer pins the published `nmdc-lakehouse-schema==0.5.0` package.
Its [release](https://github.com/microbiomedata/nmdc-lakehouse-schema/releases/tag/v0.5.0)
contains both artifacts, including the nested-substance preservation and
source-selection changes. Installing this package does not migrate MongoDB
or create a production snapshot.

## Packaged target versus production source

On 2026-09-22, the
[OpenAPI metadata](https://api.microbiomedata.org/openapi.json) for production reports NMDC
Runtime **2.21.0** and NMDC Schema **11.23.0**. The populated investigator and
`applies_to_person` paths in the MongoDB audit are consistent with that older
source contract. They are not evidence of a failed migration. The API version
does not independently certify the migration state of every MongoDB record.

The installed `nmdc-schema` version selects its exact packaged target through
`flat_schema_resource(version("nmdc-schema"))`. For 11.23.0, this returns
`schema/compat/11.23.0/nmdc_schema_flattened.yaml`, with target version
`11.23.0+flat.1.3.0`; 11.24.0 selects the canonical artifact.
The package-alignment guard checks their agreement and any explicit
`NMDC_SCHEMA_VERSION` selection. Unsupported versions have no fallback.
The Just runtime and validation recipes use the selected source extra; a plain
`uv run` does not read `NMDC_SCHEMA_VERSION`, so use those recipes or specify
the matching uv extra explicitly. The `build`, `lock`, and `test-dist` recipes
operate on the package or lock file without selecting a source extra.
Source selection is checked when a source-aware command runs. Package-only
recipes remain available with an unsupported selection, and `just doctor`
uses the installed environment without syncing so it can report that problem.

If plain `uv run` selects 11.24.0 while the process environment requests
11.23.0, the package-alignment guard stops the export before connecting to
MongoDB. A selection present only in `.env` is loaded by Just, not by this
guard; plain `uv run` does not load it automatically. Without an exported
selection, the installed source is still checked against the packaged
artifact and MongoDB's recorded migration version. A database recorded as
11.23.0 is refused by an exporter using 11.24.0. These are refusal checks,
not automatic environment switching.

Before reading records and again before promoting a collection's staged output,
both maintained export jobs read the existing MongoDB
`_migration_latest_schema_version` view. NMDC migration bookkeeping returns a
version only after the latest migration event is completed. A missing,
inaccessible, ambiguous, null, or mismatched version stops the job. The check
creates no view and performs no database writes. Dry runs use the same checks.
The preflight honors an explicit database in the MongoDB URI and uses `nmdc`
when its database path is absent, matching the direct exporter's convention.
An end-of-read mismatch discards staged files and preserves that collection's
previous output. Collections completed earlier in an all-collections run can
already have been promoted; a failed run does not produce a successful manifest.

These checks do not make the live reads a point-in-time snapshot or validate
every record against the source schema. Schedule exports while migrations are
quiescent; a migration that begins and finishes between checks may escape
detection. Source selection does not convert investigator/PersonValue records
into newer credit/Agent records and does not migrate MongoDB.

## Select production now and switch after migration

With schema package 0.5.0 installed, configure the source selection in the
shell or the local `.env`. With the read-only GCP tunnel open:

<!-- unverified: production execution awaits source preflight and a fresh validated export, tracked in https://github.com/microbiomedata/nmdc-lakehouse/issues/347 -->
```bash
export NMDC_SCHEMA_VERSION=11.23.0
just install-all
just source-preflight
just etl-collections
```

The preflight reads only migration metadata. The export writes a fresh local
snapshot by default. Set the version once for all export, manifest, cleanup,
and target-validation commands; selecting an old artifact beside a newer source
package is refused. Validate the completed snapshot using the
[MongoDB guide](mongodb-connection.md#running-etl-jobs).

After production's migration is completed and verified, choose 11.24.0 and
create a new snapshot:

<!-- unverified: requires the production source migration, tracked in https://github.com/microbiomedata/nmdc-lakehouse/issues/347 -->
```bash
export NMDC_SCHEMA_VERSION=11.24.0
just install-all
just source-preflight
just etl-collections
```

Future tagged releases require a reviewed schema-package artifact and consumer
dependency update. The default never follows upstream main or automatically
changes because a tag appeared.

## Changed output

TextValue fields are strings or string arrays on the containing record.
There is no `biosample_set_host_diet` table; use the `host_diet` array in
`biosample_set`. Repeated strings retain order, duplicates, empty strings, and
null elements. Populated additional TextValue content raises an error.

Under source 11.24.0, credit associations use `applies_to_agent_*`, including Person email/ORCID
and Organization ROR. DataGeneration gains a credit-association helper table.
Principal-investigator columns and `collection_date_inc` disappear because
their source slots are absent in 11.24.0. Source collection and credit-record
types remain in the output. These changes require consumer query updates;
existing snapshots retain their original schema identities.
Under 11.23.0, investigator and `applies_to_person_*` columns remain populated
from their existing source fields. They are tested through Parquet and full
manifest-bound target validation in both selected source environments.

Both pairs preserve `ordered_mobile_phases[*].substances_used[*]` in nested
helper tables. Join a substance to its phase using `(parent_id, mobile_phase_index)`;
`substance_index` preserves order and duplicates within that phase. The old
projection 1.2.0 omitted these objects, so its snapshots require a fresh export.

Target validation requires every declared artifact target version to match the
installed target schema and checks the manifest's aggregate version list.
Older or mixed projection versions are rejected before row validation. Legacy
version 1 footers, which have no target version, retain their existing read
compatibility; their projection version cannot be verified by this check.

Tests run synthetic Study, MassSpectrometry, NucleotideSequencing, and Biosample
records through the real collection job, Parquet writing/reading, and credit-row
target validation. Primary, child, and direct-loader tests verify producer
metadata in the actual footer. This is integration evidence for the installed
package pair, not evidence that production data has been migrated.

## Production preflight: 2026-09-22

A read-only scan read 87,367 documents across nine affected production
collections. It projected only the release-migration and unsupported nested
paths. All cursors completed; estimated collection counts before and after
matched the exact read counts. Reads were sequential rather than a point-in-time
snapshot. No source record values, identifiers, credentials, or connection
strings were retained. The temporary GCP tunnel was closed after the scan.

| Populated path | Documents | Occurrences |
| --- | ---: | ---: |
| `configuration_set.ordered_mobile_phases.substances_used` | 7 | 14 |
| `material_processing_set.ordered_mobile_phases.substances_used` | 3,237 | 12,618 |
| `data_generation_set.principal_investigator` | 2,967 | 2,967 |
| `study_set.principal_investigator` | 85 | 85 |
| `study_set.has_credit_associations.applies_to_person` | 85 | 675 |

For `substances_used`, an occurrence means a populated inner list, not the number
of substance objects. No populated `applies_to_agent` was observed under Study
or DataGeneration credits; no populated `collection_date_inc` was observed in
Biosample. The other schema-derived nested relation paths in this bounded
inspection were unpopulated. This was not a complete audit of every possible
projection-loss shape or undeclared input key.

The remaining production gates are:

1. Review and merge the consumer's adoption of published schema package 0.5.0.
2. Verify MongoDB's recorded migration state and select its matching pair under
   [#347](https://github.com/microbiomedata/nmdc-lakehouse/issues/347).
3. Repeat the bounded source audit and validate a fresh complete export. The
   observed older fields are expected while production remains on 11.23.0.

Package adoption can be reviewed independently. Do not describe a current
production export as complete until these gates are resolved and the preflight
is repeated. General projection-loss detection remains in
[#129](https://github.com/microbiomedata/nmdc-lakehouse/issues/129).

Once the gates are resolved, use the maintained `just etl-collections` workflow
and the [MongoDB connection guide](mongodb-connection.md), followed by snapshot
and target-row validation. It reads MongoDB and writes a new local Parquet
snapshot. The older `flatten-nmdc` recipe writes derived collections into MongoDB
and is a separate pipeline. Publication to a destination is another step.
