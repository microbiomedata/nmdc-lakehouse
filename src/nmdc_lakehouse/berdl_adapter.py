"""Render the NMDC-owned adapter plan for the official KBase ingestion API."""

from __future__ import annotations

import argparse
import hashlib
import importlib
import io
import ipaddress
import json
import os
import re
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Callable, cast

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_BUCKET = re.compile(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]\Z")
_OBJECT_SEGMENT = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*\Z")
_OTHER_SOURCE_SUFFIXES = {".csv", ".db", ".sqlite", ".sqlite3", ".tsv"}


class AdapterConfigurationError(ValueError):
    """Raised before the adapter performs any external operation."""


class AdapterExecutionError(RuntimeError):
    """Raised when upload, ingest, or verification does not complete safely."""


def _object_key(value: str, label: str) -> str:
    path = PurePosixPath(value)
    if path.is_absolute() or not value or value.endswith("/") or str(path) != value:
        raise AdapterConfigurationError(f"{label} must be a relative object key")
    if any(part in {"", ".", ".."} or not _OBJECT_SEGMENT.fullmatch(part) for part in path.parts):
        raise AdapterConfigurationError(f"{label} contains an unsafe path segment")
    return value


def _validated_plan(args: argparse.Namespace) -> dict[str, object]:
    data_dir = args.data_dir.expanduser()
    if not data_dir.is_dir() or data_dir.is_symlink():
        raise AdapterConfigurationError("data directory must be an ordinary directory")
    if not _IDENTIFIER.fullmatch(args.tenant) or not _IDENTIFIER.fullmatch(args.dataset):
        raise AdapterConfigurationError("tenant and dataset must be safe SQL identifiers")
    try:
        ipaddress.ip_address(args.bucket)
    except ValueError:
        pass
    else:
        raise AdapterConfigurationError("bucket must not be an IP address")
    if not _BUCKET.fullmatch(args.bucket) or ".." in args.bucket or ".-" in args.bucket or "-." in args.bucket:
        raise AdapterConfigurationError("bucket must be a safe S3 bucket name")
    namespace = f"{args.tenant}.{args.dataset}"
    if args.staging_namespace != namespace:
        raise AdapterConfigurationError("staging namespace must exactly match <tenant>.<dataset>")
    if args.destination_provider != args.tenant or args.destination_table_format != "iceberg":
        raise AdapterConfigurationError("destination provider must name the tenant catalog, in the Iceberg format")
    prefix = _object_key(args.bronze_prefix, "bronze prefix") + "/"
    progress_key = _object_key(args.progress_key, "progress key")
    config_key = _object_key(args.config_key, "config key")
    if not progress_key.startswith(prefix) or not config_key.startswith(prefix):
        raise AdapterConfigurationError("progress and config keys must be children of the bronze prefix")
    if progress_key == config_key:
        raise AdapterConfigurationError("progress and config keys must be distinct")
    checkout = args.ingest_checkout.expanduser()
    package_init = checkout / "src" / "data_lakehouse_ingest" / "__init__.py"
    ingest_core = checkout / "src" / "data_lakehouse_ingest" / "core.py"
    if (
        not checkout.is_dir()
        or checkout.is_symlink()
        or not package_init.is_file()
        or package_init.is_symlink()
        or not ingest_core.is_file()
        or ingest_core.is_symlink()
    ):
        raise AdapterConfigurationError("KBase ingest checkout must contain its ordinary package entry points")
    children = list(data_dir.iterdir())
    mixed = sorted(path.name for path in children if path.is_file() and path.suffix.lower() in _OTHER_SOURCE_SUFFIXES)
    if mixed:
        raise AdapterConfigurationError("Parquet staging does not accept mixed tabular source formats")
    files = sorted(path for path in children if path.suffix == ".parquet")
    if not files:
        raise AdapterConfigurationError("data directory contains no lowercase .parquet files")
    if any(not path.is_file() or path.is_symlink() or not _IDENTIFIER.fullmatch(path.stem) for path in files):
        raise AdapterConfigurationError("Parquet inputs must be ordinary files with safe table names")
    artifact_keys = {f"{args.bronze_prefix}/{path.name}" for path in files}
    if progress_key in artifact_keys or config_key in artifact_keys:
        raise AdapterConfigurationError("progress and config keys must not collide with Parquet object keys")
    return {
        "status": "plan-only",
        "data_dir": str(data_dir.resolve()),
        "ingest": {
            "repository": "https://github.com/kbase/data-lakehouse-ingest",
            "checkout": str(checkout.resolve()),
            "api": "data_lakehouse_ingest.ingest",
        },
        "destination": {
            "provider": args.destination_provider,
            "table_format": args.destination_table_format,
            "bucket": args.bucket,
            "bronze_prefix": args.bronze_prefix,
            "namespace": namespace,
            "mode": "overwrite",
        },
        "progress_key": progress_key,
        "config_key": config_key,
        "tables": [path.stem for path in files],
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Plan NMDC-owned Parquet staging through BERDL resources.")
    parser.add_argument("--data-dir", required=True, type=Path)
    parser.add_argument("--ingest-checkout", required=True, type=Path)
    parser.add_argument("--tenant", required=True)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--staging-namespace", required=True)
    parser.add_argument("--destination-provider", required=True)
    parser.add_argument("--destination-table-format", choices=("iceberg",), required=True)
    parser.add_argument("--mode", choices=("overwrite",), required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--bronze-prefix", required=True)
    parser.add_argument("--progress-key", required=True)
    parser.add_argument("--config-key", required=True)
    parser.add_argument("--outcome", type=Path)
    parser.add_argument("--execute-staging", action="store_true")
    return parser


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _remote_sha256(client: Any, bucket: str, key: str) -> str:
    response = client.get_object(bucket, key)
    digest = hashlib.sha256()
    try:
        for block in iter(lambda: response.read(1024 * 1024), b""):
            digest.update(block)
    finally:
        response.close()
        response.release_conn()
    return digest.hexdigest()


def _runtime(checkout: Path) -> tuple[Callable[..., dict[str, Any]], Any]:
    source_root = (checkout / "src").resolve()
    package_root = source_root / "data_lakehouse_ingest"
    source = str(source_root)
    sys.path.insert(0, source)
    try:
        from berdl_notebook_utils.clients import get_s3_client

        package = importlib.import_module("data_lakehouse_ingest")
    except ImportError as error:
        raise AdapterExecutionError("the selected KBase ingest runtime is not importable") from error
    finally:
        sys.path.remove(source)
    package_file = getattr(package, "__file__", None)
    if package_file is None or not Path(package_file).resolve().is_relative_to(package_root):
        raise AdapterExecutionError("the KBase ingest runtime was not imported from the selected checkout")
    ingest = getattr(package, "ingest", None)
    if not callable(ingest):
        raise AdapterExecutionError("the selected KBase ingest runtime does not expose ingest")
    try:
        client = get_s3_client()
    except Exception as error:
        raise AdapterExecutionError("cannot initialize the BERDL object-store client") from error
    return ingest, client


def _report_value(record: Any, name: str) -> Any:
    value = record.get(name) if isinstance(record, dict) else getattr(record, name, None)
    return getattr(value, "value", value)


def _catalog_row_count(table: str) -> int:
    try:
        from berdl_notebook_utils.setup_spark_session import get_spark_session
    except ImportError as error:
        raise AdapterExecutionError("the BERDL Spark runtime is not importable") from error
    try:
        count = get_spark_session().table(table).count()
    except Exception as error:
        raise AdapterExecutionError(f"cannot count destination table '{table}'") from error
    if not isinstance(count, int) or isinstance(count, bool) or count < 0:
        raise AdapterExecutionError(f"destination table '{table}' returned an invalid row count")
    return count


def _write_outcome(path: Path, document: dict[str, object]) -> None:
    """Publish a complete outcome once, without exposing a partial final file."""
    descriptor: int | None = None
    temporary: Path | None = None
    try:
        descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
        temporary = Path(temporary_name)
        stream = os.fdopen(descriptor, "w", encoding="utf-8")
        descriptor = None
        with stream:
            json.dump(document, stream, indent=2, sort_keys=True)
            stream.write("\n")
        try:
            os.link(temporary, path)
        except FileExistsError as error:
            raise AdapterExecutionError("refusing to replace an existing staging outcome") from error
        except OSError as error:
            raise AdapterExecutionError("cannot publish the staging outcome atomically") from error
    except OSError as error:
        raise AdapterExecutionError("cannot write the staging outcome") from error
    finally:
        if descriptor is not None:
            try:
                os.close(descriptor)
            except OSError:
                pass
        if temporary is not None:
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass


def _execute(plan: dict[str, object], args: argparse.Namespace) -> dict[str, object]:
    if args.outcome is None:
        raise AdapterConfigurationError("--outcome is required with --execute-staging")
    outcome = args.outcome.expanduser()
    if outcome.exists() or outcome.is_symlink() or not outcome.parent.is_dir() or outcome.parent.is_symlink():
        raise AdapterConfigurationError("outcome must be a new file in an ordinary directory")
    started_at = datetime.now(timezone.utc)
    ingest_plan = cast(dict[str, object], plan["ingest"])
    checkout = Path(str(ingest_plan["checkout"]))
    ingest, client = _runtime(checkout)
    data_dir = Path(str(plan["data_dir"]))
    destination = cast(dict[str, object], plan["destination"])
    bucket = str(destination["bucket"])
    bronze_prefix = str(destination["bronze_prefix"])
    files = sorted(data_dir.glob("*.parquet"))
    source_hashes: dict[str, str] = {}
    for path in files:
        key = f"{bronze_prefix}/{path.name}"
        digest = _sha256(path)
        try:
            client.fput_object(bucket, key, str(path), metadata={"nmdc-sha256": digest})
            remote_digest = _remote_sha256(client, bucket, key)
        except Exception as error:
            raise AdapterExecutionError(f"object-store transfer failed for table '{path.stem}'") from error
        if remote_digest != digest:
            raise AdapterExecutionError(f"uploaded Parquet digest does not match table '{path.stem}'")
        source_hashes[path.stem] = digest
    config = {
        "tenant": args.tenant,
        "dataset": args.dataset,
        "paths": {"bronze_base": f"s3a://{bucket}/{bronze_prefix}"},
        "tables": [
            {
                "name": path.stem,
                "enabled": True,
                "format": "parquet",
                "mode": "overwrite",
                "bronze_path": f"s3a://{bucket}/{bronze_prefix}/{path.name}",
            }
            for path in files
        ],
    }
    config_bytes = json.dumps(config, indent=2, sort_keys=True).encode()
    try:
        client.put_object(bucket, str(plan["config_key"]), io.BytesIO(config_bytes), len(config_bytes))
    except Exception as error:
        raise AdapterExecutionError("cannot store the reviewed ingest configuration") from error
    try:
        report = ingest(config, minio_client=client)
    except Exception as error:
        raise AdapterExecutionError("KBase ingest did not complete successfully") from error
    if not isinstance(report, dict):
        raise AdapterExecutionError("KBase ingest did not return a supported report")
    records = report.get("tables", [])
    observed = {_report_value(record, "name"): record for record in records}
    if report.get("success") is not True or set(observed) != set(source_hashes):
        raise AdapterExecutionError("KBase ingest did not report success for the exact table set")
    tables = []
    for name in sorted(source_hashes):
        record = observed[name]
        status = _report_value(record, "status")
        rows_in = _report_value(record, "rows_in")
        rows_written = _report_value(record, "rows_written")
        target_table = _report_value(record, "target_table")
        expected_target = f"{args.staging_namespace}.{name}"
        if target_table != expected_target:
            raise AdapterExecutionError(f"KBase ingest reported an unexpected target for table '{name}'")
        if status != "success" or not isinstance(rows_in, int) or isinstance(rows_in, bool) or rows_written != rows_in:
            raise AdapterExecutionError(f"KBase ingest did not verify matching row counts for table '{name}'")
        destination_rows = _catalog_row_count(expected_target)
        if destination_rows != rows_in:
            raise AdapterExecutionError(f"destination row count does not match source table '{name}'")
        tables.append(
            {
                "table": name,
                "status": "verified",
                "source_rows": rows_in,
                "destination_rows": destination_rows,
                "source_basis": "source parquet",
                "source_sha256": source_hashes[name],
            }
        )
    document: dict[str, object] = {
        "schema_version": "1.0.0",
        "status": "verified",
        "started_at": started_at.isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "destination": destination,
        "verification": {"verified": True, "namespace": args.staging_namespace, "tables": tables},
    }
    progress = json.dumps({"status": "verified", "tables": len(tables)}, sort_keys=True).encode()
    try:
        client.put_object(bucket, str(plan["progress_key"]), io.BytesIO(progress), len(progress))
    except Exception as error:
        raise AdapterExecutionError("cannot store the verified staging progress") from error
    _write_outcome(outcome, document)
    return document


def main(argv: list[str] | None = None) -> int:
    """Preview or execute the reviewed adapter through the official KBase API."""
    parser = _parser()
    args = parser.parse_args(argv)
    try:
        plan = _validated_plan(args)
        document = _execute(plan, args) if args.execute_staging else plan
    except (AdapterConfigurationError, AdapterExecutionError, OSError) as error:
        parser.error(str(error))
    print(json.dumps(document, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
