# Source schema 11.24.0 rollout

The consumer pins `nmdc-schema==11.24.0` and `nmdc-lakehouse-schema==0.4.0`.
The source version is the latest tagged release selected for this upgrade,
not a moving dependency on upstream main. The schema package supplies both
the runtime projection and the canonical target artifact, version
`11.24.0+flat.1.2.0`, with 59 tables (19 primary and 40 helpers).

## Changed output

TextValue fields are strings or string arrays on the containing record.
There is no `biosample_set_host_diet` table; use the `host_diet` array in
`biosample_set`. Repeated strings retain order, duplicates, empty strings, and
null elements. Populated additional TextValue content raises an error.

Credit associations now use `applies_to_agent_*`, including Person email/ORCID
and Organization ROR. DataGeneration gains a credit-association helper table.
Principal-investigator columns and `collection_date_inc` disappear because
their source slots are absent in 11.24.0. Source collection and credit-record
types remain in the output. These changes require consumer query updates;
existing snapshots retain their original schema identities.

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

Two gates therefore remain before a complete production export:

1. Preserve substances associated with each mobile-phase occurrence, tracked in
   [schema #21](https://github.com/microbiomedata/nmdc-lakehouse-schema/issues/21).
   Projection 1.2.0 currently omits this content without a JSON fallback.
2. Resolve the old source fields through the source system's migration or a
   separately reviewed extraction compatibility transform, tracked in
   [#347](https://github.com/microbiomedata/nmdc-lakehouse/issues/347).
   Installing a new schema does not migrate MongoDB.

Package adoption can be reviewed independently. Do not describe a current
production export as complete until these gates are resolved and the preflight
is repeated. General projection-loss detection remains in
[#129](https://github.com/microbiomedata/nmdc-lakehouse/issues/129).

Once the gates are resolved, use the maintained `just etl-collections` workflow
and the [MongoDB connection guide](mongodb-connection.md), followed by snapshot
and target-row validation. It reads MongoDB and writes a new local Parquet
snapshot. The older `flatten-nmdc` recipe writes derived collections into MongoDB
and is a separate pipeline. Publication to a destination is another step.
