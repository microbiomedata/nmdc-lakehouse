#!/usr/bin/env python3
"""Audit current database / table / column metadata in BERDL.

Shows what is already set today (DB-level COMMENT and DBPROPERTIES, table-level
COMMENT and TBLPROPERTIES, and optionally per-column COMMENTs) for the
requested databases. Run this before any metadata backfill to see the baseline.

Defaults to the three NMDC-representative databases. Pass database names as
positional args to override.

Requires `berdl_notebook_utils` — intended for on-cluster (JupyterHub CLI or
notebook). Detects on-cluster vs off-cluster via the standard env check.

Usage:
    python scripts/audit_database_metadata.py
    python scripts/audit_database_metadata.py nmdc_metadata nmdc_results
    python scripts/audit_database_metadata.py --columns --json data/audit.json
    python scripts/audit_database_metadata.py --columns --tables-with-issues
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

DEFAULT_DBS = ["nmdc_metadata", "nmdc_results", "nmdc_ref_data"]


def get_spark():
    try:
        from berdl_notebook_utils.setup_spark_session import get_spark_session
    except ImportError as e:
        sys.exit(
            f"ERROR: berdl_notebook_utils not importable ({e}). "
            "Run on-cluster (JupyterHub) — this script does not support the "
            "off-cluster Spark Connect path."
        )
    return get_spark_session(app_name="audit_database_metadata", tenant_name="nmdc")


def describe_database(spark, db: str) -> dict[str, str]:
    """Return {info_name: info_value} from DESCRIBE DATABASE EXTENDED."""
    rows = spark.sql(f"DESCRIBE DATABASE EXTENDED {db}").collect()
    return {(r["info_name"] or "").strip(): (r["info_value"] or "") for r in rows}


def list_tables(spark, db: str) -> list[str]:
    return [r["tableName"] for r in spark.sql(f"SHOW TABLES IN {db}").collect()]


def describe_table(spark, db: str, table: str) -> dict[str, Any]:
    """Pull selected fields out of DESCRIBE EXTENDED <db>.<table>.

    Spark 3.x lays this out as column rows, a "# Detailed Table Information"
    separator, then key/value rows where col_name carries the key and
    data_type carries the value. We only care about the detail block.
    """
    out = {
        "comment": "",
        "properties": "",
        "location": "",
        "owner": "",
        "provider": "",
    }
    rows = spark.sql(f"DESCRIBE EXTENDED {db}.{table}").collect()
    in_detail = False
    for r in rows:
        col = (r["col_name"] or "").strip()
        val = (r["data_type"] or "").strip() if r["data_type"] is not None else ""
        if col.startswith("# Detailed"):
            in_detail = True
            continue
        if not in_detail:
            continue
        if col == "Comment":
            out["comment"] = val
        elif col == "Table Properties":
            out["properties"] = val
        elif col == "Location":
            out["location"] = val
        elif col == "Owner":
            out["owner"] = val
        elif col == "Provider":
            out["provider"] = val
    return out


def get_columns(db: str, table: str) -> list[dict[str, Any]]:
    """Access-aware column-level schema with descriptions (column COMMENTs)."""
    import berdl_notebook_utils

    return berdl_notebook_utils.get_table_schema(
        db, table, detailed=True, return_json=False
    )


def _has_props(props: str) -> bool:
    """TBLPROPERTIES is rendered as a string like '[k=v, k=v]'. Empty looks like '' or '[]'."""
    s = (props or "").strip()
    return bool(s) and s not in ("[]", "{}")


def audit(databases: list[str], with_columns: bool) -> dict[str, Any]:
    spark = get_spark()
    out: dict[str, Any] = {"databases": {}}

    for db in databases:
        entry: dict[str, Any] = {}
        try:
            entry["db_info"] = describe_database(spark, db)
        except Exception as e:
            entry["error"] = f"DESCRIBE DATABASE EXTENDED failed: {e}"
            out["databases"][db] = entry
            continue

        try:
            tables = list_tables(spark, db)
        except Exception as e:
            entry["error"] = f"SHOW TABLES failed: {e}"
            out["databases"][db] = entry
            continue

        entry["n_tables"] = len(tables)
        entry["tables"] = {}

        for t in tables:
            t_entry: dict[str, Any] = {}
            try:
                t_entry["table_info"] = describe_table(spark, db, t)
            except Exception as e:
                t_entry["table_info_error"] = str(e)

            if with_columns:
                try:
                    cols = get_columns(db, t)
                    t_entry["columns"] = cols
                    t_entry["n_columns"] = len(cols)
                    t_entry["n_cols_with_description"] = sum(
                        1 for c in cols if (c.get("description") or "").strip()
                    )
                except Exception as e:
                    t_entry["columns_error"] = str(e)

            entry["tables"][t] = t_entry

        out["databases"][db] = entry

    return out


def render_summary(report: dict[str, Any], with_columns: bool, show_issues: bool) -> None:
    print()
    print("=" * 78)
    print("BERDL metadata audit")
    print("=" * 78)

    for db, entry in report["databases"].items():
        print()
        print(f"## {db}")
        if "error" in entry:
            print(f"  ERROR: {entry['error']}")
            continue

        info = entry.get("db_info", {})
        comment = info.get("Comment", "") or ""
        # Spark uses "Properties" (Hive metastore) — accept "DBProperties" too just in case.
        props = info.get("Properties", "") or info.get("DBProperties", "") or ""
        print(f"  Location:    {info.get('Location', '')}")
        print(f"  Owner:       {info.get('Owner', '')}")
        print(f"  Comment:     {comment if comment else '(none)'}")
        print(f"  Properties:  {props if props else '(none)'}")

        tables = entry.get("tables", {})
        n_tables = len(tables)
        n_with_comment = sum(
            1 for v in tables.values()
            if (v.get("table_info", {}).get("comment") or "").strip()
        )
        n_with_props = sum(
            1 for v in tables.values()
            if _has_props(v.get("table_info", {}).get("properties", ""))
        )
        print(f"  Tables:                  {n_tables}")
        print(f"    with comment:          {n_with_comment} / {n_tables}")
        print(f"    with TBLPROPERTIES:    {n_with_props} / {n_tables}")

        if with_columns:
            total_cols = sum(v.get("n_columns", 0) for v in tables.values())
            cols_with_desc = sum(
                v.get("n_cols_with_description", 0) for v in tables.values()
            )
            pct = (100.0 * cols_with_desc / total_cols) if total_cols else 0.0
            print(f"  Columns total:                {total_cols}")
            print(f"    with description (COMMENT): {cols_with_desc} ({pct:.1f}%)")

        # Tables that DO have a comment — likely a short list, useful to surface.
        with_comment = [
            (t, v["table_info"].get("comment", ""))
            for t, v in tables.items()
            if (v.get("table_info", {}).get("comment") or "").strip()
        ]
        if with_comment:
            print(f"  Tables with comment:")
            for t, c in with_comment:
                snippet = c if len(c) <= 70 else c[:67] + "..."
                print(f"    - {t}: {snippet}")

        if show_issues:
            problems = [
                (t, v.get("table_info_error") or v.get("columns_error"))
                for t, v in tables.items()
                if v.get("table_info_error") or v.get("columns_error")
            ]
            if problems:
                print(f"  Tables with audit errors:")
                for t, err in problems:
                    print(f"    - {t}: {err}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "databases", nargs="*", default=DEFAULT_DBS,
        help=f"Databases to audit (default: {' '.join(DEFAULT_DBS)})",
    )
    ap.add_argument(
        "--columns", action="store_true",
        help="Include column-level description coverage (uses berdl_notebook_utils.get_table_schema)",
    )
    ap.add_argument(
        "--json", type=Path, default=None,
        help="Write full audit (incl. raw rows) as JSON to this path",
    )
    ap.add_argument(
        "--tables-with-issues", dest="show_issues", action="store_true",
        help="Also list tables whose DESCRIBE / schema lookup raised an error",
    )
    args = ap.parse_args()

    report = audit(args.databases, with_columns=args.columns)
    render_summary(report, with_columns=args.columns, show_issues=args.show_issues)

    if args.json:
        args.json.parent.mkdir(parents=True, exist_ok=True)
        with args.json.open("w") as f:
            json.dump(report, f, indent=2, default=str)
        print(f"\nFull audit written to {args.json}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
