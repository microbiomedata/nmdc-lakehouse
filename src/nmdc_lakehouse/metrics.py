"""Lightweight, local ETL run measurements."""

from __future__ import annotations

import json
import os
import platform
import sys
import tempfile
from dataclasses import asdict
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any

from nmdc_lakehouse.jobs.base import JobResult, OutputResult

FORMAT_VERSION = 1


def peak_rss_bytes() -> int | None:
    """Return the process peak resident set size in bytes when supported."""
    try:
        import resource

        raw = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    except (ImportError, OSError, ValueError):
        return None
    return int(raw if sys.platform == "darwin" else raw * 1024)


def stamp_result(
    result: JobResult,
    *,
    output_root: Path,
    started_at: str,
    finished_at: str,
    elapsed_seconds: float,
) -> JobResult:
    """Attach timing and existing output-file measurements to ``result``."""
    outputs: list[OutputResult] = []
    for table, rows in result.table_rows:
        path = output_root / f"{table}.parquet"
        if path.is_file() and not path.is_symlink():
            outputs.append(OutputResult(table=table, path=path.name, rows=rows, bytes=path.stat().st_size))
    outputs.sort(key=lambda output: output.table)
    result.started_at = started_at
    result.finished_at = finished_at
    result.elapsed_seconds = max(0.0, elapsed_seconds)
    result.output_root = str(output_root.expanduser().resolve())
    result.outputs = tuple(outputs)
    return result


def _package_version(package: str) -> str | None:
    try:
        return version(package)
    except PackageNotFoundError:
        return None


def _environment_record() -> dict[str, Any]:
    """Describe the measured process and the peak-memory unit/source."""
    peak = peak_rss_bytes()
    return {
        "platform": platform.platform(),
        "python_version": platform.python_version(),
        "nmdc_schema_version": _package_version("nmdc-schema"),
        "peak_rss_bytes": peak,
        "peak_rss_unit": "bytes" if peak is not None else None,
        "peak_rss_source": "resource.getrusage(RUSAGE_SELF)" if peak is not None else None,
    }


def result_record(result: JobResult) -> dict[str, Any]:
    """Convert a measured job result into JSON-compatible data."""
    elapsed = result.elapsed_seconds or 0.0
    rate = result.rows_read / elapsed if elapsed > 0 else 0.0
    return {
        "job_name": result.job_name,
        "status": "success",
        "started_at": result.started_at,
        "finished_at": result.finished_at,
        "elapsed_seconds": result.elapsed_seconds,
        "rows_read": result.rows_read,
        "rows_written": result.rows_written,
        "rows_per_second": rate,
        "output_root": result.output_root,
        "outputs": [asdict(output) for output in result.outputs],
        "children": [result_record(child) for child in result.children],
    }


def success_record(result: JobResult, *, skipped_collections: tuple[str, ...], dry_run: bool) -> dict[str, Any]:
    """Build a complete run record from a successful outer job result."""
    return {
        "format_version": FORMAT_VERSION,
        **result_record(result),
        "dry_run": dry_run,
        "skipped_collections": list(skipped_collections),
        "environment": _environment_record(),
    }


def failure_record(
    *,
    job_name: str,
    started_at: str,
    finished_at: str,
    elapsed_seconds: float,
    error: BaseException,
    skipped_collections: tuple[str, ...],
    dry_run: bool,
) -> dict[str, Any]:
    """Build a sanitized failed-run record without exception text."""
    return {
        "format_version": FORMAT_VERSION,
        "job_name": job_name,
        "status": "failed",
        "started_at": started_at,
        "finished_at": finished_at,
        "elapsed_seconds": max(0.0, elapsed_seconds),
        "error_type": type(error).__name__,
        "dry_run": dry_run,
        "skipped_collections": list(skipped_collections),
        "environment": _environment_record(),
    }


def write_record(path: Path, record: dict[str, Any]) -> None:
    """Atomically write one local JSON run record."""
    destination = path.expanduser().resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(prefix=f".{destination.name}.", suffix=".tmp", dir=destination.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(record, stream, indent=2, sort_keys=True)
            stream.write("\n")
        temporary.replace(destination)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
