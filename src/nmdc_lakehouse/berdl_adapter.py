"""Render the NMDC-owned adapter plan for the official KBase ingestion API."""

from __future__ import annotations

import argparse
import ipaddress
import json
import re
from pathlib import Path, PurePosixPath

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_BUCKET = re.compile(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]\Z")
_OBJECT_SEGMENT = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*\Z")
_OTHER_SOURCE_SUFFIXES = {".csv", ".db", ".sqlite", ".sqlite3", ".tsv"}


class AdapterConfigurationError(ValueError):
    """Raised before the adapter performs any external operation."""


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
    parser.add_argument("--mode", choices=("overwrite",), required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--bronze-prefix", required=True)
    parser.add_argument("--progress-key", required=True)
    parser.add_argument("--config-key", required=True)
    parser.add_argument("--outcome", type=Path)
    parser.add_argument("--execute-staging", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    """Print a credential-free plan and refuse live execution in this slice."""
    parser = _parser()
    args = parser.parse_args(argv)
    try:
        plan = _validated_plan(args)
        if args.execute_staging:
            raise AdapterConfigurationError("live staging execution is not available in this planner slice")
    except AdapterConfigurationError as error:
        parser.error(str(error))
    print(json.dumps(plan, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
