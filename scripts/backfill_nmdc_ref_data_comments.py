#!/usr/bin/env python3
"""Backfill Delta comments and DBPROPERTIES on nmdc_ref_data (issue #116).

Pilot end-to-end backfill of:
  - DBPROPERTIES on the ``nmdc_ref_data`` schema (new convention being piloted —
    see microbiomedata/nmdc-lakehouse#114).
  - Table-level COMMENT on ``nmdc_ref_data.pfam_terms`` via the BERDL helper
    ``data_lakehouse_ingest.utils.delta_comments.apply_table_comment``.
  - Column-level COMMENTs on each ``pfam_terms`` column via
    ``apply_comments_from_table_schema`` (see microbiomedata/nmdc-lakehouse#115).

Source-of-truth for descriptions: ``docs/pfam_terms.md``.

Idempotent — re-running rewrites the same values. Run on-cluster (JupyterHub
CLI) where ``berdl_notebook_utils`` and ``data_lakehouse_ingest`` are
importable.

Usage:
    python scripts/backfill_nmdc_ref_data_comments.py --dry-run
    python scripts/backfill_nmdc_ref_data_comments.py
    python scripts/backfill_nmdc_ref_data_comments.py --skip-verify
"""

from __future__ import annotations

import argparse
import logging
import sys

DB = "nmdc_ref_data"
TABLE = "pfam_terms"

# Database-level: comment + descriptive properties. No BERDL helper for this
# yet — the key/value shape is the precedent this pilot establishes (#114).
DB_PROPERTIES: dict[str, str] = {
    "comment": (
        "Reference annotation tables built and maintained by nmdc-lakehouse "
        "for use across NMDC analyses. Pfam term definitions today; intended "
        "home for other freely redistributable ontology / vocabulary tables."
    ),
    "collection": "nmdc",
    "role": "ref_data",
    "source": "nmdc-lakehouse",
    "representative": "turbomam",
    "docs_url": (
        "https://github.com/microbiomedata/nmdc-lakehouse/blob/main/"
        "docs/architecture.md#namespace-policy"
    ),
}

TABLE_COMMENT = (
    "Pfam family definitions: maps every Pfam family accession to its short "
    "name and human-readable description. Join target for any nmdc_results "
    "table that contains a pfam_accession column (e.g. "
    "nmdc_results.pfam_annotation_gff). Source: Pfam-A.clans.tsv.gz "
    "(EBI/InterPro, CC0). Re-run notebooks/load_pfam_terms.ipynb after each "
    "Pfam release. See docs/pfam_terms.md."
)

# Structured schema accepted by data_lakehouse_ingest.utils.delta_comments.
# Allowed keys per orchestrator/schema_utils.py: column, type, nullable, comment.
COLUMN_SCHEMA: list[dict[str, object]] = [
    {
        "column": "pfam_id",
        "type": "string",
        "nullable": False,
        "comment": "Pfam accession without version (e.g. PF00001) — join key.",
    },
    {
        "column": "name",
        "type": "string",
        "nullable": True,
        "comment": "Short name (e.g. 7tm_1).",
    },
    {
        "column": "description",
        "type": "string",
        "nullable": True,
        "comment": "Human-readable description of the Pfam family.",
    },
    {
        "column": "clan_id",
        "type": "string",
        "nullable": True,
        "comment": (
            "Clan accession (e.g. CL0192). Nullable; null when the family is "
            "not assigned to a clan."
        ),
    },
    {
        "column": "clan_name",
        "type": "string",
        "nullable": True,
        "comment": (
            "Clan short name (e.g. GPCR_A). Nullable; null when the family "
            "is not assigned to a clan."
        ),
    },
]


def get_spark():
    try:
        from berdl_notebook_utils.setup_spark_session import get_spark_session
    except ImportError as e:
        sys.exit(
            f"ERROR: berdl_notebook_utils not importable ({e}). "
            "Run on-cluster (JupyterHub CLI / notebook)."
        )
    return get_spark_session(
        app_name="backfill_nmdc_ref_data_comments",
        tenant_name="nmdc",
    )


def _quote_dbproperty_value(s: str) -> str:
    """Escape for a single-quoted Spark SQL literal (DBPROPERTIES syntax).

    DBPROPERTIES uses ``'key' = 'value'``, so single quotes inside the value
    must be doubled. Backslashes are not interpreted in standard SQL string
    literals — leave them alone.
    """
    return s.replace("'", "''")


def apply_db_properties(spark, dry_run: bool) -> None:
    pairs = ", ".join(
        f"'{k}' = '{_quote_dbproperty_value(v)}'"
        for k, v in DB_PROPERTIES.items()
    )
    sql = f"ALTER SCHEMA {DB} SET DBPROPERTIES ({pairs})"
    print(f"[db]  ALTER SCHEMA {DB} SET DBPROPERTIES ({len(DB_PROPERTIES)} keys)")
    for k, v in DB_PROPERTIES.items():
        snippet = v if len(v) <= 80 else v[:77] + "..."
        print(f"        {k}: {snippet}")
    if not dry_run:
        spark.sql(sql)


def apply_table_comment_(spark, dry_run: bool, logger: logging.Logger) -> None:
    from data_lakehouse_ingest.utils.delta_comments import apply_table_comment

    full = f"{DB}.{TABLE}"
    print(f"[tbl] apply_table_comment {full}")
    snippet = TABLE_COMMENT if len(TABLE_COMMENT) <= 120 else TABLE_COMMENT[:117] + "..."
    print(f"        {snippet}")
    if dry_run:
        return
    report = apply_table_comment(spark, full, TABLE_COMMENT, logger=logger)
    print(f"        → status={report.get('status')} applied={report.get('applied')}")


def apply_column_comments(spark, dry_run: bool, logger: logging.Logger) -> None:
    from data_lakehouse_ingest.utils.delta_comments import (
        apply_comments_from_table_schema,
    )

    full = f"{DB}.{TABLE}"
    print(f"[col] apply_comments_from_table_schema {full} ({len(COLUMN_SCHEMA)} cols)")
    for c in COLUMN_SCHEMA:
        comment = str(c.get("comment", ""))
        snippet = comment if len(comment) <= 90 else comment[:87] + "..."
        print(f"        {c['column']}: {snippet}")
    if dry_run:
        return
    report = apply_comments_from_table_schema(
        spark, full, COLUMN_SCHEMA, logger=logger
    )
    print(
        f"        → status={report.get('status')} "
        f"applied={report.get('applied')} "
        f"skipped={report.get('skipped')} "
        f"failed={report.get('failed')}"
    )


def verify(spark) -> None:
    print()
    print("=" * 78)
    print("Verification")
    print("=" * 78)

    print(f"\n[db] DESCRIBE DATABASE EXTENDED {DB}:")
    for r in spark.sql(f"DESCRIBE DATABASE EXTENDED {DB}").collect():
        k = (r["info_name"] or "").strip()
        v = (r["info_value"] or "").strip()
        if k in ("Comment", "Properties"):
            print(f"  {k}: {v}")

    print(f"\n[tbl] DESCRIBE EXTENDED {DB}.{TABLE} — Comment row:")
    in_detail = False
    for r in spark.sql(f"DESCRIBE EXTENDED {DB}.{TABLE}").collect():
        col = (r["col_name"] or "").strip()
        if col.startswith("# Detailed"):
            in_detail = True
            continue
        if in_detail and col == "Comment":
            print(f"  Comment: {(r['data_type'] or '').strip()}")
            break

    print(f"\n[cols] berdl_notebook_utils.get_table_schema({DB!r}, {TABLE!r}):")
    import berdl_notebook_utils

    cols = berdl_notebook_utils.get_table_schema(
        DB, TABLE, detailed=True, return_json=False
    )
    for c in cols:
        desc = c.get("description") or "(none)"
        print(f"  - {c.get('name')}: {desc}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be applied without executing any ALTER",
    )
    ap.add_argument(
        "--skip-verify",
        action="store_true",
        help="Skip post-write DESCRIBE / get_table_schema verification",
    )
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    logger = logging.getLogger("backfill_nmdc_ref_data_comments")

    spark = get_spark()

    apply_db_properties(spark, args.dry_run)
    apply_table_comment_(spark, args.dry_run, logger)
    apply_column_comments(spark, args.dry_run, logger)

    if args.dry_run:
        print("\n(dry-run — no changes written)")
    elif not args.skip_verify:
        verify(spark)

    return 0


if __name__ == "__main__":
    sys.exit(main())
