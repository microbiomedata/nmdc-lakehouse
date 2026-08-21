"""Create and validate portable, immutable Parquet snapshot manifests."""

from __future__ import annotations

import hashlib
import json
import os
import platform
import re
import subprocess
import tempfile
from collections.abc import Sequence
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import BaseModel, ConfigDict, Field

from nmdc_lakehouse.sinks.parquet_sink import FOOTER_METADATA_FORMAT_VERSION

MANIFEST_FORMAT_VERSION = 1
MANIFEST_NAME = "snapshot-manifest.json"
_PREFIX = b"nmdc_lakehouse."
_SOURCE_LABEL = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}\Z")
_ARTIFACT_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_]*\.parquet\Z")


class SnapshotManifestError(ValueError):
    """Raised when a snapshot cannot be manifested or validated."""


class ArtifactRecord(BaseModel):
    """Integrity, schema, and contract identity for one Parquet table."""

    model_config = ConfigDict(extra="forbid")

    path: str
    table: str
    rows: int = Field(ge=0)
    bytes: int = Field(ge=0)
    sha256: str
    physical_schema_sha256: str
    footer_schema_sha256: str
    source_schema_id: str
    source_schema_version: str
    source_class: str
    target_schema_id: str
    target_class: str
    mapping: str


class PerformanceRecord(BaseModel):
    """Portable link to the run measurement used to authorize the manifest."""

    model_config = ConfigDict(extra="forbid")

    path: str
    sha256: str


class SoftwareRecord(BaseModel):
    """Non-secret producer identities needed to reproduce the snapshot."""

    model_config = ConfigDict(extra="forbid")

    nmdc_lakehouse_version: str
    git_commit: str | None
    git_dirty: bool | None
    nmdc_schema_version: str
    python_version: str


class SnapshotManifest(BaseModel):
    """Versioned completion and integrity record for one full snapshot."""

    model_config = ConfigDict(extra="forbid")

    manifest_format_version: int
    snapshot_id: str
    generated_at: str
    scope: str
    parent_snapshot_id: str | None = None
    source_label: str
    included_collections: list[str]
    skipped_collections: list[str]
    footer_metadata_format_version: str
    target_schema_ids: list[str]
    mapping_ids: list[str]
    software: SoftwareRecord
    performance_record: PerformanceRecord
    artifacts: list[ArtifactRecord]


def manifest_json_schema() -> dict[str, Any]:
    """Return the machine-readable schema for the current manifest format."""
    schema = SnapshotManifest.model_json_schema()
    schema["x-manifest-format-version"] = MANIFEST_FORMAT_VERSION
    return schema


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _json_sha256(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    return hashlib.sha256(encoded).hexdigest()


def _physical_schema_sha256(schema: pa.Schema) -> str:
    fields = [pa.field(field.name, field.type, field.nullable) for field in schema]
    return hashlib.sha256(pa.schema(fields).serialize().to_pybytes()).hexdigest()


_MISMATCH_NAME_LIMIT = 10


def _describe_names(names: Sequence[str]) -> str:
    """Name the offending files, capped, with a count of any remainder."""
    shown = sorted(names)[:_MISMATCH_NAME_LIMIT]
    listed = ", ".join(repr(name) for name in shown)
    remainder = len(names) - len(shown)
    return f"{listed} and {remainder} more" if remainder else listed


def _contents_mismatch(expected: set[str], actual: set[str]) -> str:
    """Say which files are missing and which are unexpected, not merely that one of those happened.

    The two categories have different causes and different fixes. A missing file means an
    incomplete transfer. An unexpected file usually means the archiving step added something, and
    AppleDouble `._*` siblings from a macOS `tar` are the case that has actually happened. Reporting
    only the category sent an operator looking at the manifest when the problem was the copy.
    """
    missing = expected - actual
    unexpected = actual - expected
    parts = []
    if missing:
        parts.append(f"missing {len(missing)}: {_describe_names(sorted(missing))}")
    if unexpected:
        appledouble = sorted(name for name in unexpected if name.startswith("._"))
        parts.append(f"unexpected {len(unexpected)}: {_describe_names(sorted(unexpected))}")
        if appledouble:
            parts.append(
                f"{len(appledouble)} of the unexpected files start with '._', which is what extracting a "
                "macOS tar archive on Linux produces; re-archive with COPYFILE_DISABLE=1 or delete them"
            )
    return "Snapshot contents do not match the manifest: " + "; ".join(parts) + "."


def _footer_schema_sha256(schema: pa.Schema) -> str:
    def decoded(metadata: dict[bytes, bytes] | None) -> dict[str, str]:
        return {key.decode(): value.decode() for key, value in sorted((metadata or {}).items())}

    value = {
        "schema": decoded(schema.metadata),
        "fields": [
            {
                "name": field.name,
                "type": str(field.type),
                "nullable": field.nullable,
                "metadata": decoded(field.metadata),
            }
            for field in schema
        ],
    }
    return _json_sha256(value)


def _footer_value(schema: pa.Schema, key: str) -> str:
    value = (schema.metadata or {}).get(_PREFIX + key.encode())
    if value is None:
        raise SnapshotManifestError(f"Parquet footer is missing required metadata: {key}")
    return value.decode()


def _artifact(path: Path) -> ArtifactRecord:
    try:
        parquet = pq.ParquetFile(path)
        schema = parquet.schema_arrow
        footer_version = _footer_value(schema, "footer_metadata_format_version")
        if footer_version != FOOTER_METADATA_FORMAT_VERSION:
            raise SnapshotManifestError(f"Unsupported footer metadata format in {path.name}: {footer_version}")
        return ArtifactRecord(
            path=path.name,
            table=path.stem,
            rows=parquet.metadata.num_rows,
            bytes=path.stat().st_size,
            sha256=_sha256(path),
            physical_schema_sha256=_physical_schema_sha256(schema),
            footer_schema_sha256=_footer_schema_sha256(schema),
            source_schema_id=_footer_value(schema, "source_schema_id"),
            source_schema_version=_footer_value(schema, "source_schema_version"),
            source_class=_footer_value(schema, "source_class"),
            target_schema_id=_footer_value(schema, "target_schema_id"),
            target_class=_footer_value(schema, "target_class"),
            mapping=_footer_value(schema, "mapping"),
        )
    except SnapshotManifestError:
        raise
    except Exception as error:
        raise SnapshotManifestError(f"Cannot inspect Parquet artifact: {path.name}") from error


def _package_version(name: str) -> str:
    try:
        return version(name)
    except PackageNotFoundError as error:
        raise SnapshotManifestError(f"Required package metadata is unavailable: {name}") from error


def _git_state(root: Path) -> tuple[str | None, bool | None]:
    package_file = root / "src" / "nmdc_lakehouse" / "snapshot_manifest.py"
    if not (root / "pyproject.toml").is_file() or not package_file.is_file():
        return None, None
    try:
        commit = subprocess.run(
            ["git", "-C", str(root), "rev-parse", "HEAD"],
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None, None
    value = commit.stdout.strip()
    if commit.returncode != 0 or re.fullmatch(r"[0-9a-f]{40}", value) is None:
        return None, None
    try:
        status = subprocess.run(
            ["git", "-C", str(root), "status", "--porcelain"],
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired):
        return value, None
    return value, bool(status.stdout.strip()) if status.returncode == 0 else None


def _snapshot_identity(manifest: SnapshotManifest) -> str:
    identity = manifest.model_dump(
        exclude={"snapshot_id", "generated_at", "performance_record"},
        mode="json",
    )
    return f"sha256:{_json_sha256(identity)}"


def _load_metrics(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise SnapshotManifestError(f"Cannot read the metrics record: {path}") from error
    if not isinstance(value, dict):
        raise SnapshotManifestError("The metrics record must be a JSON object.")
    return value


def build_manifest(root: Path, metrics_path: Path, source_label: str) -> SnapshotManifest:
    """Build a manifest from one successful full-snapshot metrics record."""
    root = root.expanduser()
    metrics_path = metrics_path.expanduser()
    if not root.is_dir() or root.is_symlink():
        raise SnapshotManifestError("Snapshot root must be an existing ordinary directory.")
    if not metrics_path.is_file() or metrics_path.is_symlink():
        raise SnapshotManifestError("The metrics record must be an ordinary file directly inside the snapshot root.")
    root = root.resolve()
    metrics_path = metrics_path.resolve()
    if not _SOURCE_LABEL.fullmatch(source_label):
        raise SnapshotManifestError(
            "Source label must be 1–64 characters, start with a letter or digit, and otherwise use only "
            "letters, digits, dot, underscore, or hyphen."
        )
    if metrics_path.parent != root or not metrics_path.is_file() or metrics_path.is_symlink():
        raise SnapshotManifestError("The metrics record must be an ordinary file directly inside the snapshot root.")

    metrics = _load_metrics(metrics_path)
    if metrics.get("status") != "success" or metrics.get("job_name") != "all-collections":
        raise SnapshotManifestError("Metrics must describe a successful all-collections run.")
    if metrics.get("dry_run") is not False:
        raise SnapshotManifestError("A dry run cannot produce a snapshot manifest.")
    if Path(str(metrics.get("output_root", ""))).expanduser().resolve() != root:
        raise SnapshotManifestError("Metrics output_root does not match the snapshot root.")
    finished_at = metrics.get("finished_at")
    environment = metrics.get("environment")
    if not isinstance(finished_at, str) or not finished_at:
        raise SnapshotManifestError("Metrics do not contain a completion timestamp.")
    if not isinstance(environment, dict):
        raise SnapshotManifestError("Metrics do not contain an environment record.")
    required_environment_versions = ("nmdc_schema_version", "nmdc_lakehouse_version", "python_version")
    if any(
        not isinstance(environment.get(name), str) or not environment[name] for name in required_environment_versions
    ):
        raise SnapshotManifestError("Metrics do not contain complete producer version metadata.")

    expected_outputs = metrics.get("outputs")
    children = metrics.get("children")
    skipped = metrics.get("skipped_collections")
    if not isinstance(expected_outputs, list) or not isinstance(children, list) or not children:
        raise SnapshotManifestError("Metrics do not contain a complete output and collection inventory.")
    if not isinstance(skipped, list) or not all(isinstance(item, str) for item in skipped):
        raise SnapshotManifestError("Metrics contain a malformed skipped-collection inventory.")
    if not all(isinstance(child, dict) and isinstance(child.get("job_name"), str) for child in children):
        raise SnapshotManifestError("Metrics contain a malformed included-collection inventory.")
    included = [child["job_name"] for child in children]
    from nmdc_lakehouse.jobs.collection_to_parquet import _db_collection_map

    schema_collections = set(_db_collection_map())
    if not schema_collections:
        raise SnapshotManifestError("The installed NMDC schema does not expose any MongoDB collections.")
    if (
        len(included) != len(set(included))
        or len(skipped) != len(set(skipped))
        or set(included).intersection(skipped)
        or set(included).union(skipped) != schema_collections
    ):
        raise SnapshotManifestError(
            "Included and skipped collections do not cover the installed NMDC schema exactly once."
        )
    expected: dict[str, dict[str, Any]] = {}
    for item in expected_outputs:
        path = item.get("path") if isinstance(item, dict) else None
        if not isinstance(path, str) or not _ARTIFACT_NAME.fullmatch(path) or path in expected:
            raise SnapshotManifestError("Metrics contain duplicate or malformed output paths.")
        expected[path] = item

    parquet_paths = sorted(root.glob("*.parquet"))
    if any(path.is_symlink() or not path.is_file() for path in parquet_paths):
        raise SnapshotManifestError("Snapshot Parquet artifacts must be ordinary files, not symlinks.")
    if set(expected) != {path.name for path in parquet_paths}:
        raise SnapshotManifestError("Metrics and snapshot Parquet file sets do not agree.")
    allowed_names = {metrics_path.name, *(path.name for path in parquet_paths)}
    root_entries = list(root.iterdir())
    actual_names = {path.name for path in root_entries}
    if actual_names != allowed_names:
        raise SnapshotManifestError("Snapshot contains extra files before manifest creation.")
    if any(path.is_symlink() or not path.is_file() for path in root_entries):
        raise SnapshotManifestError("Snapshot entries must be ordinary files, not symlinks or directories.")

    artifacts = [_artifact(path) for path in parquet_paths]
    for artifact in artifacts:
        measured = expected[artifact.path]
        if measured.get("table") != artifact.table or measured.get("rows") != artifact.rows:
            raise SnapshotManifestError(f"Metrics and Parquet footer disagree for {artifact.path}.")
        if measured.get("bytes") != artifact.bytes:
            raise SnapshotManifestError(f"Metrics and file size disagree for {artifact.path}.")

    git_commit, git_dirty = _git_state(Path(__file__).resolve().parents[2])
    software = SoftwareRecord(
        nmdc_lakehouse_version=_package_version("nmdc-lakehouse"),
        git_commit=git_commit,
        git_dirty=git_dirty,
        nmdc_schema_version=_package_version("nmdc-schema"),
        python_version=platform.python_version(),
    )
    if environment.get("nmdc_schema_version") != software.nmdc_schema_version:
        raise SnapshotManifestError("Metrics and manifest generation used different nmdc-schema versions.")
    if environment.get("nmdc_lakehouse_version") != software.nmdc_lakehouse_version:
        raise SnapshotManifestError("Metrics and manifest generation used different nmdc-lakehouse versions.")
    if environment.get("python_version") != software.python_version:
        raise SnapshotManifestError("Metrics and manifest generation used different Python versions.")
    manifest = SnapshotManifest(
        manifest_format_version=MANIFEST_FORMAT_VERSION,
        snapshot_id="pending",
        generated_at=finished_at,
        scope="full-mongodb-metadata-snapshot",
        parent_snapshot_id=None,
        source_label=source_label,
        included_collections=sorted(included),
        skipped_collections=sorted(skipped),
        footer_metadata_format_version=FOOTER_METADATA_FORMAT_VERSION,
        target_schema_ids=sorted({artifact.target_schema_id for artifact in artifacts}),
        mapping_ids=sorted({artifact.mapping for artifact in artifacts}),
        software=software,
        performance_record=PerformanceRecord(path=metrics_path.name, sha256=_sha256(metrics_path)),
        artifacts=artifacts,
    )
    manifest.snapshot_id = _snapshot_identity(manifest)
    return manifest


def write_manifest(root: Path, manifest: SnapshotManifest) -> Path:
    """Atomically write the snapshot completion marker inside ``root``."""
    root = root.expanduser()
    if not root.is_dir() or root.is_symlink():
        raise SnapshotManifestError("Snapshot root must be an existing ordinary directory.")
    destination = root.resolve() / MANIFEST_NAME
    if destination.exists() or destination.is_symlink():
        raise SnapshotManifestError(f"Refusing to replace existing {MANIFEST_NAME}.")
    fd, temporary_name = tempfile.mkstemp(prefix=f".{MANIFEST_NAME}.", suffix=".tmp", dir=destination.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(manifest.model_dump_json(indent=2))
            stream.write("\n")
        try:
            os.link(temporary, destination)
        except FileExistsError as error:
            raise SnapshotManifestError(f"Refusing to replace existing {MANIFEST_NAME}.") from error
        except OSError as error:
            raise SnapshotManifestError(f"Cannot publish {MANIFEST_NAME} atomically.") from error
    finally:
        temporary.unlink(missing_ok=True)
    return destination


def validate_snapshot(root: Path) -> SnapshotManifest:
    """Validate a manifested snapshot without network or destination access."""
    root = root.expanduser()
    if not root.is_dir() or root.is_symlink():
        raise SnapshotManifestError("Snapshot root must be an existing ordinary directory.")
    root = root.resolve()
    manifest_path = root / MANIFEST_NAME
    if not manifest_path.is_file() or manifest_path.is_symlink():
        raise SnapshotManifestError(f"Cannot read a valid {MANIFEST_NAME}.")
    try:
        manifest = SnapshotManifest.model_validate_json(manifest_path.read_text(encoding="utf-8"))
    except Exception as error:
        raise SnapshotManifestError(f"Cannot read a valid {MANIFEST_NAME}.") from error
    if manifest.manifest_format_version != MANIFEST_FORMAT_VERSION:
        raise SnapshotManifestError("Unsupported snapshot manifest format version.")
    if manifest.footer_metadata_format_version != FOOTER_METADATA_FORMAT_VERSION:
        raise SnapshotManifestError("Unsupported Parquet footer metadata format version.")
    artifact_paths = [item.path for item in manifest.artifacts]
    owned_paths = [manifest.performance_record.path, *artifact_paths]
    if (
        len(owned_paths) != len(set(owned_paths))
        or MANIFEST_NAME in owned_paths
        or any("/" in path or "\\" in path or path in {"", ".", ".."} for path in owned_paths)
        or any(not _ARTIFACT_NAME.fullmatch(path) for path in artifact_paths)
    ):
        raise SnapshotManifestError("Snapshot manifest must contain a one-to-one inventory of safe owned paths.")
    if manifest.snapshot_id != _snapshot_identity(manifest):
        raise SnapshotManifestError("Snapshot identity does not match the manifest content.")

    expected_names = {MANIFEST_NAME, manifest.performance_record.path, *(item.path for item in manifest.artifacts)}
    root_entries = list(root.iterdir())
    actual_names = {path.name for path in root_entries}
    if actual_names != expected_names:
        raise SnapshotManifestError(_contents_mismatch(expected_names, actual_names))
    if any(path.is_symlink() or not path.is_file() for path in root_entries):
        raise SnapshotManifestError("Snapshot entries must be ordinary files, not symlinks or directories.")

    performance_path = root / manifest.performance_record.path
    if _sha256(performance_path) != manifest.performance_record.sha256:
        raise SnapshotManifestError("Performance record checksum does not match the manifest.")
    rebuilt = [_artifact(root / item.path) for item in manifest.artifacts]
    if rebuilt != manifest.artifacts:
        raise SnapshotManifestError(
            "Parquet content, rows, schemas, or footer contracts changed after manifest creation."
        )
    return manifest
