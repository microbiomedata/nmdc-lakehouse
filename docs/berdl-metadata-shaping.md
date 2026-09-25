# Shaping BERDL metadata: tenant, org, schema, dataset, table, column

A survey of what descriptive metadata this pipeline can set at each level of the
BERDL hierarchy, what's already done, and what's tracked but not yet built. Written
because the work is scattered across seven issues ([#114](https://github.com/microbiomedata/nmdc-lakehouse/issues/114)-[#120](https://github.com/microbiomedata/nmdc-lakehouse/issues/120)) with no single map.

For the maintained workflow as of 2026-09-23, use the
[staging runbook](berdl-staging-runbook.md#what-success-means). `stage-publication`
now requires data and planned table/column metadata verification in one execution.
Namespace operations remain deferred, and tenant/registry writes remain
unimplemented. The dated pilot sections below preserve earlier observations;
their bare namespace names and table counts are not current destination defaults.

> **Column comments are no longer reached this way at scale.** Everything below
> describes `apply_comments_from_table_schema()`, one
> `ALTER TABLE ... ALTER COLUMN ... COMMENT` per column, and treats scaling it as
> the remaining work. It does not scale: it stops at `biosample_set`'s width, and
> describing a canonical table that way is now refused. Descriptions ride in the
> Parquet footer instead and arrive when Spark creates the table. The per-column
> helper survives as the staging fallback for whatever the footer did not carry.
> [`column-description-path.md`](column-description-path.md) is the current
> account.
>
> **The table level below is stale too.** It says scaling beyond the
> `nmdc_ref_data.pfam_terms` pilot is future work in
> [#115](https://github.com/microbiomedata/nmdc-lakehouse/issues/115). That issue
> closed on 2026-08-20, and `stage-publication` now applies and verifies
> planned table descriptions across the namespace
> (`berdl_metadata.py:410-423`). Current versioned plans also apply and verify
> table snapshot and target-schema properties. Namespace application remains
> outside that executor.

## Summary table

| Level | Settable today? | Mechanism | State |
|---|---|---|---|
| Tenant / org | Unconfirmed | `berdl_notebook_utils.governance`'s `list_tenants()` / `get_tenant_detail()` have readable `description`/`website`/`organization`/`display_name` fields per tenant; no known write path from NMDC-side code | Not investigated, see below |
| Schema / database | Piloted; deferred by the current staging executor | `ALTER SCHEMA ... SET DBPROPERTIES (...)` | Earlier pilot on `nmdc_ref_data` ([#116](https://github.com/microbiomedata/nmdc-lakehouse/issues/116)); current namespace application remains [#114](https://github.com/microbiomedata/nmdc-lakehouse/issues/114) |
| Dataset / Bronze objects | Checksum metadata on objects; no registry write adapter | The NMDC adapter sets `nmdc-sha256` on each uploaded object and verifies the object bytes | Richer descriptive content remains in the portable bundle; registry support is separate |
| Table | Yes | `data_lakehouse_ingest.utils.delta_comments.apply_table_comment` | Done. Piloted on `nmdc_ref_data.pfam_terms` ([#117](https://github.com/microbiomedata/nmdc-lakehouse/pull/117)), then scaled: [#115](https://github.com/microbiomedata/nmdc-lakehouse/issues/115) closed 2026-08-20 and `stage-publication` applies and verifies planned table descriptions |
| Column | Yes | The Parquet footer key `org.apache.spark.sql.parquet.row.metadata`, applied by Spark at table creation | Done, by a different route than this page proposed. `apply_comments_from_table_schema` does not scale and survives only as the staging fallback; see the note at the top |

## Tenant / org: the level nobody has written to yet

`berdl_inventory.py` in BERIL-research-observatory reads tenant metadata via
`berdl_notebook_utils.list_tenants()` / `get_tenant_detail()`. The returned
`TenantInfo` has real descriptive fields: `display_name`, `description`, `website`,
`organization`, alongside the access-control fields (`stewards`, `members_rw`,
`members_ro`, `namespace_prefix`).

**Unconfirmed: whether these are settable, and by whom.** `berdl_notebook_utils`
is a JupyterHub-pod-only package (not installable off-cluster, its dependencies
assume the cluster environment), so its write-side API couldn't be checked from
here. The `nmdc` tenant's steward might carry update rights, but the tenant/org
model in BERDL looks platform-owned (KBase) rather than per-tenant-owned. Worth
a direct question to BERDL platform owners, same move already used for the
`docs_url` redaction question in [#118](https://github.com/microbiomedata/nmdc-lakehouse/issues/118).

If a write path exists, `nmdc` tenant's `description`/`website`/`organization`
would be the natural home for the top-level "what is this and who maintains it"
answer that `DBPROPERTIES.representative` currently only expresses per-schema.

## Schema / database: proposed and partially piloted

Spark treats `SCHEMA` and `DATABASE` as synonyms, so this is one level, not two.
`ALTER SCHEMA <name> SET DBPROPERTIES (...)` is the mechanism. [#114](https://github.com/microbiomedata/nmdc-lakehouse/issues/114) proposes a
convention (`comment`, `source`, `representative`, `collection`, `role`,
`docs_url`) and [#116](https://github.com/microbiomedata/nmdc-lakehouse/issues/116)/[#117](https://github.com/microbiomedata/nmdc-lakehouse/pull/117) piloted it end-to-end on `nmdc_ref_data` (1 table, 5
columns; closed, verified via `DESCRIBE DATABASE EXTENDED`).

Known issue: `docs_url` displays as `*********(redacted)` in
`DESCRIBE DATABASE EXTENDED`. Spark's redaction regex apparently matches URL-shaped
values. Tracked in [#118](https://github.com/microbiomedata/nmdc-lakehouse/issues/118), unresolved. Until that lands, prefer embedding doc links
inside the `comment` field rather than a separate `docs_url` property.

The earlier pilot account listed `nmdc_metadata` with 49 tables and
`nmdc_results` with nine. Those are historical names and counts. The
[September inventory](runs/2026-09-23-production-staging.md) observed 54 tables
in the catalog-qualified `nmdc.metadata`. It did not establish that namespace
metadata had been applied; the current executor reports those operations as
deferred under [#114](https://github.com/microbiomedata/nmdc-lakehouse/issues/114).

## Dataset and Bronze object metadata

The Bronze layer is plain Parquet objects in MinIO under
`cdm-lake/tenant-general-warehouse/nmdc/datasets/{metadata,results,ref_data,...}/`.
This is a distinct concept from the managed Silver Iceberg tables above it: "dataset" in
BERDL's own path convention refers to this raw-object layer, not a table.

S3-compatible stores support per-object user metadata (`x-amz-meta-*` headers).
The current NMDC adapter passes `metadata={"nmdc-sha256": digest}` to the object
upload and reads the stored bytes back to verify their digest. It does not expand
the profile or schema bundle into descriptive object metadata. Those richer
descriptions remain in the portable evidence and table/column catalog metadata.
Object metadata is distinct from a dataset registry entry; there is no maintained
registry write adapter here. This supersedes the earlier survey's claim that no
object metadata was set.

## Historical table and column pilots

*Historical, and the two levels went different ways. Table descriptions were
scaled through `apply_table_comment` and that is still how they are applied.
Column descriptions were not: `apply_comments_from_table_schema` is the helper
that stops at width, so they arrive in the Parquet footer instead and it survives
only as the staging fallback. See the note at the top. This section is kept
because it records what was known and proposed at the time.*

BERDL already ships a supported convention for this
(`data_lakehouse_ingest.utils.delta_comments`), documented in [#115](https://github.com/microbiomedata/nmdc-lakehouse/issues/115):
`apply_table_comment()` (falls back from `COMMENT ON TABLE` to
`ALTER TABLE ... SET TBLPROPERTIES` depending on catalog support) and
`apply_comments_from_table_schema()` (per-column `ALTER TABLE ... ALTER COLUMN
... COMMENT`, driven by a structured schema with `column`/`type`/`nullable`/`comment`
keys).

For `nmdc_metadata`, the content already exists as data:
`schema_generator.flatten_class_def()` / `side_table_class_defs()` produce
per-column `description` strings (the `DISPATCH_NOTE` / `NESTED_NOTE` / `REF_NOTE`
annotations from polymorphic dispatch and nested-slot flattening) and a
class-level `description` explaining the polymorphic union. Wiring those into
the ingest's structured-schema `comment` field is the unlock. [#114](https://github.com/microbiomedata/nmdc-lakehouse/issues/114) and [#115](https://github.com/microbiomedata/nmdc-lakehouse/issues/115)
both propose this; [#120](https://github.com/microbiomedata/nmdc-lakehouse/issues/120) additionally argues for keeping the generator code
separate from any hand-authored content (YAML/SQL under a `metadata/` directory)
so domain experts can review descriptions without reading Python.

Verification harness: [#119](https://github.com/microbiomedata/nmdc-lakehouse/pull/119) (`scripts/python/audit_database_metadata.py`, merged)
reports per-database coverage stats (tables/columns with a comment) so a partial
backfill (applying schema-level `DBPROPERTIES` and table/column comments
retroactively to objects that predate this convention, not a historical-data
reload) can be measured and re-run to completion.

## Historical proposed order

This list records the pilot-era plan. Use the current staging runbook above for
execution; table/column application and verification are now mandatory phases.

1. [#119](https://github.com/microbiomedata/nmdc-lakehouse/pull/119) is merged: run the audit script to see current coverage before scaling anything below.
2. [#118](https://github.com/microbiomedata/nmdc-lakehouse/issues/118): resolve or work around the `docs_url` redaction before standardizing that property across more schemas.
3. [#120](https://github.com/microbiomedata/nmdc-lakehouse/issues/120)'s separation principle, applied as `metadata/nmdc_ref_data.yaml` ported from the [#117](https://github.com/microbiomedata/nmdc-lakehouse/pull/117) pilot, before scaling to two more schemas with 10x the content.
4. [#114](https://github.com/microbiomedata/nmdc-lakehouse/issues/114)/[#115](https://github.com/microbiomedata/nmdc-lakehouse/issues/115) for `nmdc_metadata` and `nmdc_results`: the LinkML-driven case, since the content is already generated data, not hand-authored.
5. Tenant/org level: ask BERDL platform owners whether it's writable at all before scoping any work here.
6. Dataset/Bronze-object metadata: file an issue if this turns out to matter for discovery; no evidence yet that anyone's blocked on it.

## Related

- `docs/publication-contract.md`: how the metadata content bundle,
  portable Parquet metadata, catalog comments, snapshot manifest, and registry
  record fit into a staged replacement.
- `docs/berdl-upload.md`: getting Parquet into BERDL in the first place; this doc is what to set once it's there.
- `docs/architecture.md`: the three-namespace policy (`nmdc_metadata`/`nmdc_results`/`nmdc_ref_data`) these DBPROPERTIES attach to.
- [#114](https://github.com/microbiomedata/nmdc-lakehouse/issues/114), [#115](https://github.com/microbiomedata/nmdc-lakehouse/issues/115), [#116](https://github.com/microbiomedata/nmdc-lakehouse/issues/116), [#117](https://github.com/microbiomedata/nmdc-lakehouse/pull/117), [#118](https://github.com/microbiomedata/nmdc-lakehouse/issues/118), [#119](https://github.com/microbiomedata/nmdc-lakehouse/pull/119), [#120](https://github.com/microbiomedata/nmdc-lakehouse/issues/120)
