#!/usr/bin/env python3
"""Audit current database / table / column metadata in BERDL.

Shows what is already set today (DB-level COMMENT and DBPROPERTIES, table-level
COMMENT and TBLPROPERTIES, and optionally per-column COMMENTs) for the
requested databases. Run this before any metadata backfill to see the baseline.

Defaults to the three NMDC-representative databases. Pass database names as
positional args to override.

Requires `berdl_notebook_utils` — intended for on-cluster (JupyterHub CLI or
notebook) only. Exits with an error if that package isn't importable; there
is no off-cluster fallback path.

Usage:
    python scripts/python/audit_database_metadata.py
    python scripts/python/audit_database_metadata.py nmdc_metadata nmdc_results
    python scripts/python/audit_database_metadata.py --columns --json data/audit.json
    python scripts/python/audit_database_metadata.py --columns --tables-with-issues
    python scripts/python/audit_database_metadata.py nmdc_metadata \
        --publication-inventory data/nmdc-metadata-inventory.json \
        --destination-id nmdc-production \
        --provider spark_catalog \
        --table-format delta \
        --metadata-capability namespace \
        --metadata-capability table \
        --metadata-capability column
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

DEFAULT_DBS = ["nmdc_metadata", "nmdc_results", "nmdc_ref_data"]

# Database/table names are interpolated into SQL strings below (Spark's SQL
# parser has no parameterized-identifier support for DESCRIBE/SHOW TABLES).
# Restrict to what a Hive-metastore identifier can legally be so nothing
# unexpected — spaces, backticks, semicolons — reaches the query string.
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_PUBLICATION_TABLE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_]*$")
_SAFE_LABEL_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_METADATA_CAPABILITIES = {"tenant", "dataset", "namespace", "table", "column", "snapshot", "file"}

# Catalog-internal property key prefixes that Delta/Spark set automatically
# and that don't indicate a user has set any descriptive metadata.
_INTERNAL_PROPERTY_PREFIXES = ("delta.", "spark.", "option.")


def _validate_identifier(name: str, kind: str) -> str:
    """Return ``name`` unchanged if it's a safe bare SQL identifier, else raise."""
    if not _IDENTIFIER_RE.match(name):
        raise ValueError(f"Refusing to interpolate unsafe {kind} name into SQL: {name!r}")
    return name


def _validate_qualified_identifier(name: str, kind: str) -> str:
    """Validate a dot-qualified SQL identifier without accepting SQL syntax."""
    parts = name.split(".")
    if not parts or any(not _IDENTIFIER_RE.fullmatch(part) for part in parts):
        raise ValueError(f"Refusing to interpolate unsafe {kind} name into SQL: {name!r}")
    return name


def _quoted_identifier(name: str, kind: str) -> str:
    """Return a validated qualified identifier with every segment quoted."""
    return ".".join(f"`{part}`" for part in _validate_qualified_identifier(name, kind).split("."))


def _validate_label(value: str, kind: str) -> str:
    """Validate a credential-free logical label copied into an inventory."""
    if not _SAFE_LABEL_RE.fullmatch(value):
        raise ValueError(f"{kind} must be a sanitized logical label.")
    return value


def get_spark():
    """Return a Spark session for the ``nmdc`` tenant, or exit if off-cluster."""
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
    db = _quoted_identifier(db, "database")
    rows = spark.sql(f"DESCRIBE DATABASE EXTENDED {db}").collect()
    return {(r["info_name"] or "").strip(): (r["info_value"] or "") for r in rows}


def list_tables(spark, db: str) -> list[str]:
    """Return table names in ``db``, sorted for deterministic before/after diffs."""
    db = _quoted_identifier(db, "database")
    return sorted(r["tableName"] for r in spark.sql(f"SHOW TABLES IN {db}").collect())


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
    db = _quoted_identifier(db, "database")
    table = _validate_identifier(table, "table")
    rows = spark.sql(f"DESCRIBE EXTENDED {db}.`{table}`").collect()
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

    db = _validate_qualified_identifier(db, "database")
    table = _validate_identifier(table, "table")
    return berdl_notebook_utils.get_table_schema(db, table, detailed=True, return_json=False)


class PublicationInventoryError(ValueError):
    """Raised when a complete credential-free inventory cannot be generated."""


def _physical_schema_sha256(spark_schema: Any) -> str:
    """Hash a Spark schema with the same metadata-free Arrow representation as snapshots."""
    try:
        from pyspark.sql.pandas.types import to_arrow_schema

        return _arrow_physical_schema_sha256(to_arrow_schema(spark_schema))
    except Exception as error:
        raise PublicationInventoryError("Cannot convert a destination schema to canonical Arrow form.") from error


def _arrow_physical_schema_sha256(arrow_schema: Any) -> str:
    """Hash an Arrow schema after stripping schema and field metadata."""
    import pyarrow as pa

    fields = [pa.field(field.name, field.type, field.nullable) for field in arrow_schema]
    return hashlib.sha256(pa.schema(fields).serialize().to_pybytes()).hexdigest()


def build_publication_inventory(
    spark: Any,
    database: str,
    *,
    destination_id: str,
    provider: str,
    table_format: str,
    metadata_capabilities: list[str],
    observed_at: str | None = None,
) -> dict[str, Any]:
    """Build a complete planner inventory through read-only catalog queries."""
    database = _validate_qualified_identifier(database, "database")
    destination_id = _validate_label(destination_id, "Destination ID")
    provider = _validate_label(provider, "Provider")
    table_format = _validate_label(table_format, "Table format")
    if not metadata_capabilities or len(metadata_capabilities) != len(set(metadata_capabilities)):
        raise PublicationInventoryError("Metadata capabilities must be a nonempty list without duplicates.")
    unknown_capabilities = set(metadata_capabilities).difference(_METADATA_CAPABILITIES)
    if unknown_capabilities:
        raise PublicationInventoryError(
            "Unknown metadata capabilities: " + ", ".join(sorted(unknown_capabilities)) + "."
        )

    tables = list_tables(spark, database)
    if not tables:
        raise PublicationInventoryError("The selected destination namespace contains no visible tables.")
    if len(tables) != len(set(tables)):
        raise PublicationInventoryError("Destination discovery returned duplicate table names.")

    entries: list[dict[str, Any]] = []
    quoted_database = _quoted_identifier(database, "database")
    for table in tables:
        table = _validate_identifier(table, "table")
        if not _PUBLICATION_TABLE_RE.fullmatch(table):
            raise PublicationInventoryError(f"Table {table!r} cannot be represented by the planner contract.")
        try:
            table_info = describe_table(spark, database, table)
            discovered_format = str(table_info.get("provider", "")).strip()
            if not _SAFE_LABEL_RE.fullmatch(discovered_format):
                raise PublicationInventoryError(f"Table {table!r} reports an unsafe or blank provider label.")
            if discovered_format.casefold() != table_format.casefold():
                raise PublicationInventoryError(
                    f"Table {table!r} reports provider {discovered_format or '(blank)'!r}, "
                    f"not reviewed table format {table_format!r}."
                )
            qualified_table = f"{quoted_database}.`{table}`"
            schema_frame = spark.sql(f"SELECT * FROM {qualified_table} LIMIT 0")
            count_result = spark.sql(f"SELECT COUNT(*) AS row_count FROM {qualified_table}").collect()
            if len(count_result) != 1:
                raise PublicationInventoryError(f"Table {table!r} returned an invalid count result.")
            rows = count_result[0]["row_count"]
            if isinstance(rows, bool) or not isinstance(rows, int) or rows < 0:
                raise PublicationInventoryError(f"Table {table!r} returned an invalid row count.")
            schema_hash = _physical_schema_sha256(schema_frame.schema)
        except PublicationInventoryError:
            raise
        except Exception as error:
            raise PublicationInventoryError(f"Cannot inventory destination table {table!r} completely.") from error
        entries.append({"name": table, "rows": rows, "physical_schema_sha256": schema_hash})

    return {
        "inventory_format_version": 1,
        "destination_id": destination_id,
        "observed_at": observed_at or datetime.now(UTC).isoformat(),
        "provider": provider,
        "table_format": table_format,
        "metadata_capabilities": sorted(metadata_capabilities),
        "tables": entries,
    }


def write_publication_inventory(path: Path, inventory: dict[str, Any]) -> Path:
    """Write an inventory atomically without replacing non-files or symlinks."""
    temporary: Path | None = None
    published = False
    failure: BaseException | None = None
    try:
        destination = path.expanduser()
        if destination.is_symlink() or (destination.exists() and not destination.is_file()):
            raise PublicationInventoryError("Publication inventory output must be an ordinary file path.")
        destination = destination.resolve()
        destination.parent.mkdir(parents=True, exist_ok=True)
        fd, temporary_name = tempfile.mkstemp(prefix=f".{destination.name}.", suffix=".tmp", dir=destination.parent)
        temporary = Path(temporary_name)
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(inventory, stream, indent=2, sort_keys=True)
            stream.write("\n")
        temporary.replace(destination)
        published = True
    except PublicationInventoryError as error:
        failure = error
        raise
    except OSError as error:
        failure = PublicationInventoryError("Cannot write the publication inventory.")
        raise failure from error
    except Exception as error:
        failure = error
        raise
    finally:
        if temporary is not None:
            try:
                temporary.unlink(missing_ok=True)
            except OSError as error:
                if failure is not None:
                    failure.add_note("Temporary publication-inventory cleanup also failed.")
                elif not published:
                    raise PublicationInventoryError("Cannot finish writing the publication inventory.") from error
    return destination


def _parse_properties(props: str) -> dict[str, str]:
    """Best-effort parse of Spark's ``[k=v, k=v]`` TBLPROPERTIES rendering.

    Splits between tokens that look like the start of a new ``key=value``
    pair. Can misparse a property value that itself contains ``", word="`` —
    acceptable for a coverage estimate, not intended for exact extraction.
    """
    s = (props or "").strip()
    if (s.startswith("[") and s.endswith("]")) or (s.startswith("{") and s.endswith("}")):
        s = s[1:-1]
    if not s:
        return {}
    out: dict[str, str] = {}
    for part in re.split(r",\s*(?=[\w.-]+=)", s):
        if "=" not in part:
            continue
        k, _, v = part.partition("=")
        out[k.strip()] = v.strip()
    return out


def _has_user_properties(props: str) -> bool:
    """True if TBLPROPERTIES has any key beyond catalog-internal Delta/Spark ones.

    Delta tables always carry internal keys like ``delta.minReaderVersion``,
    so "has TBLPROPERTIES at all" is nearly always true and not a useful
    signal for whether anyone has set descriptive metadata.
    """
    parsed = _parse_properties(props)
    return any(not k.startswith(_INTERNAL_PROPERTY_PREFIXES) for k in parsed)


def audit(databases: list[str], with_columns: bool) -> dict[str, Any]:
    """Snapshot DB/table/(optionally column) metadata for each of ``databases``."""
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
                    t_entry["n_cols_with_description"] = sum(1 for c in cols if (c.get("description") or "").strip())
                except Exception as e:
                    t_entry["columns_error"] = str(e)

            entry["tables"][t] = t_entry

        out["databases"][db] = entry

    return out


def render_summary(report: dict[str, Any], with_columns: bool, show_issues: bool) -> None:
    """Print a human-readable coverage summary of an ``audit()`` report."""
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
        n_with_comment = sum(1 for v in tables.values() if (v.get("table_info", {}).get("comment") or "").strip())
        n_with_props = sum(
            1 for v in tables.values() if _has_user_properties(v.get("table_info", {}).get("properties", ""))
        )
        print(f"  Tables:                  {n_tables}")
        print(f"    with comment:          {n_with_comment} / {n_tables}")
        print(f"    with user TBLPROPERTIES: {n_with_props} / {n_tables} (excludes delta.*/spark.* internal keys)")

        if with_columns:
            total_cols = sum(v.get("n_columns", 0) for v in tables.values())
            cols_with_desc = sum(v.get("n_cols_with_description", 0) for v in tables.values())
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
            print("  Tables with comment:")
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
                print("  Tables with audit errors:")
                for t, err in problems:
                    print(f"    - {t}: {err}")


def main() -> int:
    """CLI entry point: parse args, run the audit, print a summary, optionally write JSON."""
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument(
        "databases",
        nargs="*",
        default=DEFAULT_DBS,
        help=f"Databases to audit (default: {' '.join(DEFAULT_DBS)})",
    )
    ap.add_argument(
        "--columns",
        action="store_true",
        help="Include column-level description coverage (uses berdl_notebook_utils.get_table_schema)",
    )
    ap.add_argument(
        "--json",
        type=Path,
        default=None,
        help="Write full audit (incl. raw rows) as JSON to this path",
    )
    ap.add_argument(
        "--tables-with-issues",
        dest="show_issues",
        action="store_true",
        help="Also list tables whose DESCRIBE / schema lookup raised an error",
    )
    ap.add_argument(
        "--publication-inventory",
        type=Path,
        default=None,
        help="Write a complete credential-free destination inventory for the publication planner",
    )
    ap.add_argument("--destination-id", help="Logical destination identity for publication inventory output")
    ap.add_argument("--provider", help="Reviewed destination catalog/provider label")
    ap.add_argument("--table-format", help="Reviewed table format; checked against every discovered table")
    ap.add_argument(
        "--metadata-capability",
        action="append",
        default=[],
        choices=sorted(_METADATA_CAPABILITIES),
        help="Destination metadata level supported by the selected target; repeat as needed",
    )
    args = ap.parse_args()

    if args.publication_inventory is not None:
        if len(args.databases) != 1:
            ap.error("--publication-inventory requires exactly one selected database or namespace")
        if args.columns or args.json or args.show_issues:
            ap.error("publication inventory output cannot be combined with audit report options")
        missing = [
            option
            for option, value in (
                ("--destination-id", args.destination_id),
                ("--provider", args.provider),
                ("--table-format", args.table_format),
                ("--metadata-capability", args.metadata_capability),
            )
            if not value
        ]
        if missing:
            ap.error("publication inventory output also requires " + ", ".join(missing))
        try:
            inventory = build_publication_inventory(
                get_spark(),
                args.databases[0],
                destination_id=args.destination_id,
                provider=args.provider,
                table_format=args.table_format,
                metadata_capabilities=args.metadata_capability,
            )
            output = write_publication_inventory(args.publication_inventory, inventory)
        except (PublicationInventoryError, ValueError) as error:
            print(f"ERROR: {error}", file=sys.stderr)
            return 1
        print(f"Publication inventory written to {output} ({len(inventory['tables'])} tables).")
        return 0

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
