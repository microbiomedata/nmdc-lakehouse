# Shaping BERDL metadata: tenant, org, schema, dataset, table, column

A survey of what descriptive metadata this pipeline can set at each level of the
BERDL hierarchy, what's already done, and what's tracked but not yet built. Written
because the work is scattered across seven issues (#114–#120) with no single map.

## Summary table

| Level | Settable today? | Mechanism | State |
|---|---|---|---|
| Tenant / org | Unconfirmed | `berdl_notebook_utils.governance` has readable `description`/`website`/`organization`/`display_name` fields per tenant; no known write path from NMDC-side code | Not investigated — see below |
| Schema / database | Yes | `ALTER SCHEMA ... SET DBPROPERTIES (...)` | Piloted on `nmdc_ref_data` only (#116, closed). `nmdc_metadata`/`nmdc_results` not yet done (#114) |
| Dataset (Bronze/MinIO object path) | No known mechanism | S3 supports object metadata (`x-amz-meta-*`) via `mc`; nothing in this pipeline sets it | Open gap, not filed |
| Table | Yes | `data_lakehouse_ingest.utils.delta_comments.apply_table_comment` | Piloted on `nmdc_ref_data.pfam_terms` only (#117). Scaling to 49+9 tables is #115 |
| Column | Yes | `data_lakehouse_ingest.utils.delta_comments.apply_comments_from_table_schema` | Same as table — piloted, not scaled |

## Tenant / org — the level nobody has written to yet

`berdl_inventory.py` in BERIL-research-observatory reads tenant metadata via
`berdl_notebook_utils.list_tenants()` / `get_tenant_detail()`. The returned
`TenantInfo` has real descriptive fields: `display_name`, `description`, `website`,
`organization`, alongside the access-control fields (`stewards`, `members_rw`,
`members_ro`, `namespace_prefix`) documented in Claude memory
`reference_berdl_tenant_access`.

**Unconfirmed: whether these are settable, and by whom.** `berdl_notebook_utils`
is a JupyterHub-pod-only package (not installable off-cluster — its dependencies
assume the cluster environment), so its write-side API couldn't be checked from
here. Mark is steward of the `nmdc` tenant, which might carry update rights, but
the tenant/org model in BERDL looks platform-owned (KBase) rather than
per-tenant-owned. Worth a direct question to BERDL platform owners — same move
already used for the `docs_url` redaction question in #118.

If a write path exists, `nmdc` tenant's `description`/`website`/`organization`
would be the natural home for the top-level "what is this and who maintains it"
answer that `DBPROPERTIES.representative` currently only expresses per-schema.

## Schema / database — proposed and partially piloted

Spark treats `SCHEMA` and `DATABASE` as synonyms, so this is one level, not two.
`ALTER SCHEMA <name> SET DBPROPERTIES (...)` is the mechanism; #114 proposes a
convention (`comment`, `source`, `representative`, `collection`, `role`,
`docs_url`) and #116/#117 piloted it end-to-end on `nmdc_ref_data` (1 table, 5
columns — closed, verified via `DESCRIBE DATABASE EXTENDED`).

Known issue: `docs_url` displays as `*********(redacted)` in
`DESCRIBE DATABASE EXTENDED` — Spark's redaction regex apparently matches URL-shaped
values. Tracked in #118, unresolved. Until that lands, prefer embedding doc links
inside the `comment` field rather than a separate `docs_url` property.

`nmdc_metadata` (49 tables) and `nmdc_results` (9 tables) don't have this yet —
that's the remaining scope of #114.

## Dataset (Bronze layer) — no mechanism identified

The Bronze layer is plain Parquet objects in MinIO under
`cdm-lake/tenant-general-warehouse/nmdc/datasets/{metadata,results,ref_data,...}/`.
This is a distinct concept from the Silver Delta tables above it — "dataset" in
BERDL's own path convention refers to this raw-object layer, not a table.

S3-compatible object stores support per-object user metadata
(`x-amz-meta-*` headers), settable via `mc cp --attr` or `mc tag`. Nothing in
this pipeline or in BERIL-research-observatory's ingest scripts currently sets
any such metadata on the Bronze objects — checked `ingest_lib.py` and found no
reference. This is a genuine gap, not yet filed as an issue. Lower priority than
the Silver-layer work above, since Silver is what users actually query.

## Table and column — proposed, piloted, not yet scaled

BERDL already ships a supported convention for this
(`data_lakehouse_ingest.utils.delta_comments`), documented in #115:
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
the ingest's structured-schema `comment` field is the unlock — #114 and #115
both propose this; #120 additionally argues for keeping the generator code
separate from any hand-authored content (YAML/SQL under a `metadata/` directory)
so domain experts can review descriptions without reading Python.

Verification harness: #119 (`scripts/python/audit_database_metadata.py`, open —
adds a PR, not yet merged) reports per-database coverage stats (tables/columns
with a comment) so a partial backfill can be measured and re-run to completion.

## Suggested order, if picking this up

1. #119 first — merge the audit script so progress on everything below is measurable.
2. #118 — resolve or work around the `docs_url` redaction before standardizing that property across more schemas.
3. #120's separation principle, applied as `metadata/nmdc_ref_data.yaml` ported from the #117 pilot, before scaling to two more schemas with 10x the content.
4. #114/#115 for `nmdc_metadata` and `nmdc_results` — the LinkML-driven case, since the content is already generated data, not hand-authored.
5. Tenant/org level — ask BERDL platform owners whether it's writable at all before scoping any work here.
6. Dataset/Bronze-object metadata — file an issue if this turns out to matter for discovery; no evidence yet that anyone's blocked on it.

## Related

- `docs/berdl-upload.md` — getting Parquet into BERDL in the first place; this doc is what to set once it's there.
- `docs/architecture.md` — the three-namespace policy (`nmdc_metadata`/`nmdc_results`/`nmdc_ref_data`) these DBPROPERTIES attach to.
- microbiomedata/nmdc-lakehouse#114, #115, #116, #117, #118, #119, #120
