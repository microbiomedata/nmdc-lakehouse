"""Plan, execute, and independently verify guarded BERDL data staging."""

from __future__ import annotations

import hashlib
import ipaddress
import json
import os
import re
import subprocess
import sys
import tempfile
from collections.abc import Callable, Sequence
from pathlib import Path, PurePosixPath
from typing import Final, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from nmdc_lakehouse.metadata_application import (
    MetadataApplicationPlan,
    build_metadata_application_plan,
    load_metadata_application_plan,
)
from nmdc_lakehouse.metadata_bundle import MetadataBundle, load_metadata_bundle
from nmdc_lakehouse.publication_plan import (
    DestinationInventory,
    Disposition,
    PublicationPlan,
    load_destination_inventory,
    load_publication_plan,
)
from nmdc_lakehouse.publication_preflight import build_publication_preflight
from nmdc_lakehouse.snapshot_manifest import MANIFEST_NAME, SnapshotManifest, validate_snapshot
from nmdc_lakehouse.target_validation import (
    SAMPLING_ALGORITHM,
    TargetValidationReport,
    load_target_validation_report,
    packaged_target_schema_sha256,
    packaged_target_selection_bases,
)

PLAN_FORMAT_VERSION: Literal[1] = 1
_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_STAGING_DATASET = re.compile(r"[A-Za-z_][A-Za-z0-9_]*_staging_[A-Za-z0-9_]+\Z")
_BUCKET = re.compile(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]\Z")
_BUCKET_RESERVED_PREFIXES = ("xn--", "sthree-", "amzn-s3-demo-")
_BUCKET_RESERVED_SUFFIXES = ("-s3alias", "--ol-s3", ".mrap", "--x-s3", "--table-s3")
_OBJECT_SEGMENT = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*\Z")
_REVISION = re.compile(r"[0-9a-f]{40}\Z")
_SUPPORTED_DESTINATION_PROVIDER: Final[Literal["spark_catalog"]] = "spark_catalog"
_SUPPORTED_TABLE_FORMAT: Final[Literal["iceberg"]] = "iceberg"
_SUPPORTED_INGEST_REVISIONS: Final = frozenset({"a76bb7a24a42f0c9212fda8b9ab0bd3b637645d3"})

CommandRunner = Callable[[Sequence[str]], subprocess.CompletedProcess[str]]
OUTCOME_FORMAT_VERSION: Literal[1] = 1


class BerdlStagingPlanError(ValueError):
    """Raised when reviewed evidence cannot produce a safe staging plan."""


class EvidenceDigest(BaseModel):
    """Content identity for one reviewed JSON input."""

    model_config = ConfigDict(extra="forbid", strict=True)

    name: str
    path: str
    sha256: str


class IngestRevision(BaseModel):
    """Exact NMDC adapter and official KBase ingest source selected for execution."""

    model_config = ConfigDict(extra="forbid", strict=True)

    repository: Literal["https://github.com/kbase/data-lakehouse-ingest"]
    checkout: str
    checkout_remote: str
    revision: str
    adapter_sha256: str
    package_tree_git_oid: str
    package_init_sha256: str
    ingest_core_sha256: str


class StagingArtifact(BaseModel):
    """Manifest-owned candidate table selected for staging."""

    model_config = ConfigDict(extra="forbid", strict=True)

    table: str
    path: str
    rows: int
    bytes: int
    sha256: str
    physical_schema_sha256: str
    target_schema_id: str
    mapping_id: str


class TargetValidationEvidence(BaseModel):
    """Logical schema identity and bounded or full validation coverage."""

    model_config = ConfigDict(extra="forbid", strict=True)

    target_schema_id: str
    target_schema_sha256: str
    requested_mode: Literal["bounded", "full"]
    eligible_rows: int
    selected_rows: int
    tables: int


class BerdlStagingPlan(BaseModel):
    """Credential-free plan for invoking BERIL without executing it."""

    model_config = ConfigDict(extra="forbid", strict=True)

    plan_format_version: Literal[1]
    status: Literal["plan-only"]
    snapshot_id: str
    destination_id: str
    destination_observed_at: str
    destination_provider: Literal["spark_catalog"]
    destination_table_format: Literal["iceberg"]
    staging_namespace: str
    tenant: str
    dataset: str
    bucket: str
    bronze_prefix: str
    progress_key: str
    config_key: str
    evidence: list[EvidenceDigest]
    target_validation: TargetValidationEvidence
    ingest: IngestRevision
    artifacts: list[StagingArtifact]
    command: list[str]


class UpstreamDestination(BaseModel):
    """Destination identity reported by the BERIL command."""

    model_config = ConfigDict(extra="forbid", strict=True)

    bucket: str
    bronze_prefix: str
    namespace: str
    mode: Literal["overwrite"]


class UpstreamTableVerification(BaseModel):
    """One BERIL source-to-catalog row-count comparison."""

    model_config = ConfigDict(extra="forbid", strict=True)

    table: str
    status: Literal["verified"]
    source_rows: int = Field(ge=0)
    destination_rows: int = Field(ge=0)
    source_basis: str


class UpstreamVerification(BaseModel):
    """Structured verification emitted by the BERIL command."""

    model_config = ConfigDict(extra="forbid", strict=True)

    verified: Literal[True]
    namespace: str
    tables: list[UpstreamTableVerification]


class UpstreamStagingOutcome(BaseModel):
    """Successful BERIL staging outcome accepted by this repository."""

    model_config = ConfigDict(extra="forbid", strict=True)

    schema_version: Literal["1.0.0"]
    status: Literal["verified"]
    started_at: str
    finished_at: str
    destination: UpstreamDestination
    verification: UpstreamVerification


class StagedTable(BaseModel):
    """Independently checked table recorded in the NMDC outcome."""

    model_config = ConfigDict(extra="forbid", strict=True)

    table: str
    artifact_sha256: str
    rows: int = Field(ge=0)
    destination_rows: int = Field(ge=0)
    source_basis: str


class BerdlStagingOutcome(BaseModel):
    """Credential-free evidence that one immutable plan staged verified data."""

    model_config = ConfigDict(extra="forbid", strict=True)

    outcome_format_version: Literal[1]
    status: Literal["data-verified"]
    snapshot_id: str
    staging_namespace: str
    destination_id: str
    bucket: str
    bronze_prefix: str
    progress_key: str
    config_key: str
    beril_revision: str
    staging_plan_sha256: str
    upstream_outcome_sha256: str
    upstream_started_at: str
    upstream_finished_at: str
    tables: list[StagedTable]


def _run_command(args: Sequence[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, text=True, capture_output=True, check=False, timeout=10)  # noqa: S603


def _run_staging_command(args: Sequence[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, text=True, check=False, shell=False)  # noqa: S603


def _sha256(path: Path, label: str) -> str:
    document = path.expanduser()
    if not document.is_file() or document.is_symlink():
        raise BerdlStagingPlanError(f"The {label} must be an ordinary file.")
    digest = hashlib.sha256()
    try:
        with document.open("rb") as stream:
            for block in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(block)
    except OSError as error:
        raise BerdlStagingPlanError(f"Cannot read the {label}.") from error
    return digest.hexdigest()


def _validate_object_key(value: str, label: str) -> None:
    path = PurePosixPath(value)
    if path.is_absolute() or not value or value.endswith("/") or str(path) != value:
        raise BerdlStagingPlanError(f"The {label} must be a relative object key.")
    if any(part in {"", ".", ".."} or not _OBJECT_SEGMENT.fullmatch(part) for part in path.parts):
        raise BerdlStagingPlanError(f"The {label} contains an unsafe path segment.")


def _is_valid_s3_bucket(value: str) -> bool:
    if (
        not _BUCKET.fullmatch(value)
        or ".." in value
        or ".-" in value
        or "-." in value
        or value.startswith(_BUCKET_RESERVED_PREFIXES)
        or value.endswith(_BUCKET_RESERVED_SUFFIXES)
    ):
        return False
    try:
        ipaddress.ip_address(value)
    except ValueError:
        return True
    return False


def _validate_destination(
    *,
    tenant: str,
    dataset: str,
    staging_namespace: str,
    bucket: str,
    bronze_prefix: str,
    progress_key: str,
    config_key: str,
) -> None:
    if not _IDENTIFIER.fullmatch(tenant):
        raise BerdlStagingPlanError("The tenant must be a safe identifier.")
    if not _STAGING_DATASET.fullmatch(dataset):
        raise BerdlStagingPlanError("The dataset must use a unique <name>_staging_<suffix> identifier.")
    if staging_namespace != f"{tenant}.{dataset}":
        raise BerdlStagingPlanError("The staging namespace must exactly match <tenant>.<dataset>.")
    if not _is_valid_s3_bucket(bucket):
        raise BerdlStagingPlanError("The bucket must be a safe S3 bucket name.")
    for value, label in (
        (bronze_prefix, "bronze prefix"),
        (progress_key, "progress key"),
        (config_key, "config key"),
    ):
        _validate_object_key(value, label)
    required_prefix = f"tenant-general-warehouse/{tenant}/staging/"
    if not bronze_prefix.startswith(required_prefix):
        raise BerdlStagingPlanError("The bronze prefix must be inside the tenant staging area.")
    child_prefix = f"{bronze_prefix}/"
    if not progress_key.startswith(child_prefix) or not config_key.startswith(child_prefix):
        raise BerdlStagingPlanError("The progress and config keys must be children of the bronze prefix.")
    if progress_key == config_key:
        raise BerdlStagingPlanError("The progress and config keys must be distinct.")


def _require_metadata_agreement(
    manifest: SnapshotManifest,
    bundle: MetadataBundle,
    inventory: DestinationInventory,
    metadata_plan: MetadataApplicationPlan,
    staging_namespace: str,
) -> None:
    expected = build_metadata_application_plan(bundle, inventory, staging_namespace)
    if metadata_plan != expected or metadata_plan.snapshot_id != manifest.snapshot_id:
        raise BerdlStagingPlanError("The metadata application plan does not match the reviewed publication evidence.")


def _require_target_validation(manifest: SnapshotManifest, report: TargetValidationReport) -> None:
    artifacts = {artifact.table: artifact for artifact in manifest.artifacts}
    tables = {table.table: table for table in report.tables}
    has_error_issues = any(
        issue.severity.strip().upper() in {"ERROR", "FATAL"}
        for table in report.tables
        for issue in table.issue_categories
    )
    if report.status != "success" or report.invalid_rows != 0 or has_error_issues:
        raise BerdlStagingPlanError("The target validation report is not successful.")
    if report.snapshot_id != manifest.snapshot_id:
        raise BerdlStagingPlanError("The target validation report does not match the snapshot identity.")
    if set(tables) != set(artifacts) or len(tables) != len(report.tables):
        raise BerdlStagingPlanError("The target validation report table coverage does not match the snapshot.")
    if set(manifest.target_schema_ids) != {report.target_schema_id}:
        raise BerdlStagingPlanError("The target validation report schema does not match the snapshot.")
    if report.target_schema_sha256 != packaged_target_schema_sha256():
        raise BerdlStagingPlanError("The target validation report does not match the packaged target schema.")
    if (
        {artifact.source_schema_id for artifact in manifest.artifacts} != {report.target_schema_source_id}
        or {artifact.source_schema_version for artifact in manifest.artifacts} != {report.target_schema_source_version}
        or report.target_schema_source_package_version != manifest.software.nmdc_schema_version
    ):
        raise BerdlStagingPlanError("The target validation report source schema does not match the snapshot.")
    selection_bases = packaged_target_selection_bases({artifact.target_class for artifact in manifest.artifacts})
    for name, artifact in artifacts.items():
        table = tables[name]
        full = report.requested_mode == "full" or artifact.rows <= report.full_table_max_rows
        expected_mode = "full" if full else "sampled"
        expected_selected = artifact.rows if full else min(artifact.rows, report.sample_rows)
        if (
            table.artifact_path != artifact.path
            or table.target_class != artifact.target_class
            or table.selection_basis != selection_bases[artifact.target_class]
            or table.eligible_rows != artifact.rows
            or table.mode != expected_mode
            or table.selected_rows != expected_selected
            or table.selected_rows != table.valid_rows
            or table.invalid_rows != 0
        ):
            raise BerdlStagingPlanError(f"Target validation evidence does not match table '{name}'.")
    if (
        report.sampling_algorithm != SAMPLING_ALGORITHM
        or report.eligible_rows != sum(artifact.rows for artifact in manifest.artifacts)
        or report.selected_rows != sum(table.selected_rows for table in report.tables)
        or report.valid_rows != report.selected_rows
    ):
        raise BerdlStagingPlanError("The target validation report aggregate counts are inconsistent.")


def _select_artifacts(manifest: SnapshotManifest, publication_plan: PublicationPlan) -> list[StagingArtifact]:
    candidate_entries = {entry.table: entry for entry in publication_plan.tables if entry.candidate_path is not None}
    artifacts = {artifact.table: artifact for artifact in manifest.artifacts}
    if set(candidate_entries) != set(artifacts):
        raise BerdlStagingPlanError("The publication plan candidate table set does not match the snapshot manifest.")
    selected: list[StagingArtifact] = []
    for table, artifact in sorted(artifacts.items()):
        entry = candidate_entries[table]
        if entry.disposition not in {Disposition.ADD, Disposition.REPLACE}:
            raise BerdlStagingPlanError(f"Candidate table '{table}' does not have an add or replace disposition.")
        selected.append(
            StagingArtifact(
                table=table,
                path=artifact.path,
                rows=artifact.rows,
                bytes=artifact.bytes,
                sha256=artifact.sha256,
                physical_schema_sha256=artifact.physical_schema_sha256,
                target_schema_id=artifact.target_schema_id,
                mapping_id=artifact.mapping,
            )
        )
    return selected


_INGEST_PACKAGE = "src/data_lakehouse_ingest"
_INGEST_ENTRY_SOURCES = (
    "src/data_lakehouse_ingest/__init__.py",
    "src/data_lakehouse_ingest/core.py",
)
_OFFICIAL_INGEST_REMOTES = {
    "https://github.com/kbase/data-lakehouse-ingest",
    "https://github.com/kbase/data-lakehouse-ingest.git",
    "git@github.com:kbase/data-lakehouse-ingest.git",
}


def _require_revision_package(checkout: Path, revision: str, runner: CommandRunner) -> tuple[str, str, tuple[str, ...]]:
    """Bind the complete imported package and official checkout provenance."""
    try:
        remote = runner(("git", "-C", str(checkout), "remote", "get-url", "origin"))
        tracked = runner(("git", "-C", str(checkout), "ls-files", "--", _INGEST_PACKAGE))
        tree = runner(("git", "-C", str(checkout), "rev-parse", f"{revision}:{_INGEST_PACKAGE}"))
    except (OSError, subprocess.TimeoutExpired) as error:
        raise BerdlStagingPlanError("Cannot verify the KBase ingest package against the revision.") from error
    remote_url = remote.stdout.strip()
    sources = tuple(sorted(filter(None, tracked.stdout.splitlines())))
    tree_oid = tree.stdout.strip()
    if remote.returncode != 0 or remote_url not in _OFFICIAL_INGEST_REMOTES:
        raise BerdlStagingPlanError("The KBase ingest checkout must identify the official GitHub repository.")
    if tracked.returncode != 0 or not sources or not set(_INGEST_ENTRY_SOURCES).issubset(sources):
        raise BerdlStagingPlanError("The complete KBase ingest package must be tracked by the selected revision.")
    if tree.returncode != 0 or not re.fullmatch(r"[0-9a-f]{40,64}", tree_oid):
        raise BerdlStagingPlanError("Cannot identify the selected KBase ingest package tree.")
    try:
        flags = runner(("git", "-C", str(checkout), "ls-files", "-v", "--", _INGEST_PACKAGE))
    except (OSError, subprocess.TimeoutExpired) as error:
        raise BerdlStagingPlanError("Cannot verify the KBase ingest package against the revision.") from error
    if flags.returncode != 0 or set(flags.stdout.splitlines()) != {f"H {source}" for source in sources}:
        raise BerdlStagingPlanError("The KBase ingest package must not use special Git index flags.")
    for source in sources:
        path = checkout / source
        if not path.is_file() or path.is_symlink():
            raise BerdlStagingPlanError("The KBase ingest package must contain only ordinary tracked source files.")
        try:
            expected = runner(("git", "-C", str(checkout), "rev-parse", f"{revision}:{source}"))
            observed = runner(("git", "-C", str(checkout), "hash-object", f"--path={source}", source))
        except (OSError, subprocess.TimeoutExpired) as error:
            raise BerdlStagingPlanError("Cannot verify the KBase ingest package against the revision.") from error
        if (
            expected.returncode != 0
            or observed.returncode != 0
            or not expected.stdout.strip()
            or expected.stdout.strip() != observed.stdout.strip()
        ):
            raise BerdlStagingPlanError("The KBase ingest package bytes do not match the selected revision.")
    return remote_url, tree_oid, sources


def _inspect_ingest_checkout(
    checkout: Path,
    expected_revision: str,
    runner: CommandRunner,
) -> tuple[Path, Path, IngestRevision]:
    if not _REVISION.fullmatch(expected_revision):
        raise BerdlStagingPlanError("The KBase ingest revision must be a full lowercase Git commit.")
    if expected_revision not in _SUPPORTED_INGEST_REVISIONS:
        raise BerdlStagingPlanError("The KBase ingest revision is not an approved Iceberg-compatible stock release.")
    checkout = checkout.expanduser()
    if not checkout.is_dir() or checkout.is_symlink():
        raise BerdlStagingPlanError("The KBase ingest checkout must be an ordinary directory.")
    checkout = checkout.resolve()
    adapter = Path(__file__).with_name("berdl_adapter.py")
    package_init = checkout / _INGEST_ENTRY_SOURCES[0]
    ingest_core = checkout / _INGEST_ENTRY_SOURCES[1]
    for path, label in (
        (adapter, "NMDC BERDL adapter"),
        (package_init, "KBase ingest package initializer"),
        (ingest_core, "KBase ingest core"),
    ):
        if not path.is_file() or path.is_symlink():
            raise BerdlStagingPlanError(f"The {label} must be an ordinary file.")
    try:
        revision = runner(("git", "-C", str(checkout), "rev-parse", "--verify", "HEAD"))
        dirty = runner(("git", "-C", str(checkout), "status", "--porcelain", "--untracked-files=all"))
    except (OSError, subprocess.TimeoutExpired) as error:
        raise BerdlStagingPlanError("Cannot inspect the KBase ingest checkout revision.") from error
    if revision.returncode != 0 or revision.stdout.strip() != expected_revision:
        raise BerdlStagingPlanError("The KBase ingest checkout does not match the requested revision.")
    if dirty.returncode != 0 or dirty.stdout.strip():
        raise BerdlStagingPlanError("The KBase ingest checkout must have no tracked or untracked changes.")
    remote_url, tree_oid, sources = _require_revision_package(checkout, expected_revision, runner)
    evidence = IngestRevision(
        repository="https://github.com/kbase/data-lakehouse-ingest",
        checkout=str(checkout),
        checkout_remote=remote_url,
        revision=expected_revision,
        adapter_sha256=_sha256(adapter, "NMDC BERDL adapter"),
        package_tree_git_oid=tree_oid,
        package_init_sha256=_sha256(package_init, "KBase ingest package initializer"),
        ingest_core_sha256=_sha256(ingest_core, "KBase ingest core"),
    )
    try:
        final_revision = runner(("git", "-C", str(checkout), "rev-parse", "--verify", "HEAD"))
        final_dirty = runner(("git", "-C", str(checkout), "status", "--porcelain", "--untracked-files=all"))
        final_remote_url, final_tree_oid, final_sources = _require_revision_package(checkout, expected_revision, runner)
    except (OSError, subprocess.TimeoutExpired) as error:
        raise BerdlStagingPlanError("Cannot recheck the KBase ingest checkout after hashing.") from error
    if (
        final_revision.returncode != 0
        or final_revision.stdout.strip() != expected_revision
        or final_dirty.returncode != 0
        or final_dirty.stdout.strip()
    ):
        raise BerdlStagingPlanError("The KBase ingest checkout changed while its sources were hashed.")
    final_evidence = IngestRevision(
        repository="https://github.com/kbase/data-lakehouse-ingest",
        checkout=str(checkout),
        checkout_remote=final_remote_url,
        revision=expected_revision,
        adapter_sha256=_sha256(adapter, "NMDC BERDL adapter"),
        package_tree_git_oid=final_tree_oid,
        package_init_sha256=_sha256(package_init, "KBase ingest package initializer"),
        ingest_core_sha256=_sha256(ingest_core, "KBase ingest core"),
    )
    if final_sources != sources or final_evidence != evidence:
        raise BerdlStagingPlanError("The NMDC adapter or KBase ingest sources changed while being hashed.")
    return checkout, adapter, evidence


def build_berdl_staging_plan(
    *,
    snapshot_root: Path,
    manifest: SnapshotManifest,
    bundle: MetadataBundle,
    inventory: DestinationInventory,
    publication_plan: PublicationPlan,
    metadata_plan: MetadataApplicationPlan,
    target_validation: TargetValidationReport,
    evidence: list[EvidenceDigest],
    ingest_checkout: Path,
    ingest_revision: str,
    tenant: str,
    dataset: str,
    bucket: str,
    bronze_prefix: str,
    progress_key: str,
    config_key: str,
    runner: CommandRunner = _run_command,
) -> BerdlStagingPlan:
    """Cross-check loaded evidence and build the exact plan-only command."""
    staging_namespace = f"{tenant}.{dataset}"
    _validate_destination(
        tenant=tenant,
        dataset=dataset,
        staging_namespace=staging_namespace,
        bucket=bucket,
        bronze_prefix=bronze_prefix,
        progress_key=progress_key,
        config_key=config_key,
    )
    build_publication_preflight(manifest, bundle, inventory, publication_plan)
    if inventory.provider != _SUPPORTED_DESTINATION_PROVIDER or inventory.table_format != _SUPPORTED_TABLE_FORMAT:
        raise BerdlStagingPlanError(
            "BERDL staging requires a reviewed spark_catalog destination using the Iceberg table format."
        )
    _require_target_validation(manifest, target_validation)
    _require_metadata_agreement(manifest, bundle, inventory, metadata_plan, staging_namespace)
    artifacts = _select_artifacts(manifest, publication_plan)
    artifact_keys = {f"{bronze_prefix}/{artifact.path}" for artifact in artifacts}
    if progress_key in artifact_keys or config_key in artifact_keys:
        raise BerdlStagingPlanError("The progress and config keys must not collide with staged artifact keys.")
    checkout, adapter, ingest = _inspect_ingest_checkout(ingest_checkout, ingest_revision, runner)
    root = snapshot_root.resolve()
    command = [
        sys.executable,
        str(adapter),
        "--data-dir",
        str(root),
        "--ingest-checkout",
        str(checkout),
        "--tenant",
        tenant,
        "--dataset",
        dataset,
        "--staging-namespace",
        staging_namespace,
        "--destination-provider",
        inventory.provider,
        "--destination-table-format",
        inventory.table_format,
        "--mode",
        "overwrite",
        "--bucket",
        bucket,
        "--bronze-prefix",
        bronze_prefix,
        "--progress-key",
        progress_key,
        "--config-key",
        config_key,
    ]
    return BerdlStagingPlan(
        plan_format_version=PLAN_FORMAT_VERSION,
        status="plan-only",
        snapshot_id=manifest.snapshot_id,
        destination_id=inventory.destination_id,
        destination_observed_at=inventory.observed_at,
        destination_provider=_SUPPORTED_DESTINATION_PROVIDER,
        destination_table_format=_SUPPORTED_TABLE_FORMAT,
        staging_namespace=staging_namespace,
        tenant=tenant,
        dataset=dataset,
        bucket=bucket,
        bronze_prefix=bronze_prefix,
        progress_key=progress_key,
        config_key=config_key,
        evidence=evidence,
        target_validation=TargetValidationEvidence(
            target_schema_id=target_validation.target_schema_id,
            target_schema_sha256=target_validation.target_schema_sha256,
            requested_mode=target_validation.requested_mode,
            eligible_rows=target_validation.eligible_rows,
            selected_rows=target_validation.selected_rows,
            tables=len(target_validation.tables),
        ),
        ingest=ingest,
        artifacts=artifacts,
        command=command,
    )


def plan_berdl_staging(
    snapshot_root: Path,
    *,
    bundle_path: Path,
    inventory_path: Path,
    publication_plan_path: Path,
    metadata_plan_path: Path,
    target_validation_path: Path,
    ingest_checkout: Path,
    ingest_revision: str,
    tenant: str,
    dataset: str,
    bucket: str,
    bronze_prefix: str,
    progress_key: str,
    config_key: str,
    runner: CommandRunner = _run_command,
) -> BerdlStagingPlan:
    """Load and bind all reviewed inputs without contacting BERDL."""
    root = snapshot_root.expanduser()
    paths = (
        (root / MANIFEST_NAME, "snapshot-manifest.json", "snapshot manifest"),
        (bundle_path, "metadata-bundle.json", "metadata bundle"),
        (inventory_path, "destination-inventory.json", "destination inventory"),
        (publication_plan_path, "publication-plan.json", "publication plan"),
        (metadata_plan_path, "metadata-application-plan.json", "metadata application plan"),
        (target_validation_path, "target-validation-report.json", "target validation report"),
    )
    digests = [_sha256(path, label) for path, _name, label in paths]
    manifest = validate_snapshot(root)
    bundle = load_metadata_bundle(bundle_path)
    inventory = load_destination_inventory(inventory_path)
    publication_plan = load_publication_plan(publication_plan_path)
    metadata_plan = load_metadata_application_plan(metadata_plan_path)
    target_validation = load_target_validation_report(target_validation_path)
    evidence = [
        EvidenceDigest(name=name, path=str(path.expanduser().resolve()), sha256=digest)
        for (path, name, _label), digest in zip(paths, digests, strict=True)
    ]
    plan = build_berdl_staging_plan(
        snapshot_root=root,
        manifest=manifest,
        bundle=bundle,
        inventory=inventory,
        publication_plan=publication_plan,
        metadata_plan=metadata_plan,
        target_validation=target_validation,
        evidence=evidence,
        ingest_checkout=ingest_checkout,
        ingest_revision=ingest_revision,
        tenant=tenant,
        dataset=dataset,
        bucket=bucket,
        bronze_prefix=bronze_prefix,
        progress_key=progress_key,
        config_key=config_key,
        runner=runner,
    )
    final_manifest = validate_snapshot(root)
    if final_manifest != manifest:
        raise BerdlStagingPlanError("The manifested snapshot changed while the BERDL staging plan was built.")
    final_models = (
        load_metadata_bundle(bundle_path),
        load_destination_inventory(inventory_path),
        load_publication_plan(publication_plan_path),
        load_metadata_application_plan(metadata_plan_path),
        load_target_validation_report(target_validation_path),
    )
    if final_models != (bundle, inventory, publication_plan, metadata_plan, target_validation):
        raise BerdlStagingPlanError("Reviewed evidence changed while the BERDL staging plan was built.")
    if digests != [_sha256(path, label) for path, _name, label in paths]:
        raise BerdlStagingPlanError("A reviewed input changed while the BERDL staging plan was built.")
    return plan


def render_berdl_staging_plan(plan: BerdlStagingPlan) -> str:
    """Render stable, credential-free plan JSON."""
    return json.dumps(plan.model_dump(mode="json"), indent=2, sort_keys=True)


def write_berdl_staging_plan(path: Path, plan: BerdlStagingPlan) -> Path:
    """Atomically create a plan without replacing prior authorization evidence."""
    destination = path.expanduser()
    if destination.exists() or destination.is_symlink():
        raise BerdlStagingPlanError("Refusing to replace an existing BERDL staging plan.")
    parent = destination.parent
    if not parent.is_dir() or parent.is_symlink():
        raise BerdlStagingPlanError("The BERDL staging plan parent must be an ordinary directory.")
    destination = parent.resolve() / destination.name
    manifest_evidence = [item for item in plan.evidence if item.name == "snapshot-manifest.json"]
    if len(manifest_evidence) != 1:
        raise BerdlStagingPlanError("The BERDL staging plan must identify one snapshot manifest.")
    snapshot_root = Path(manifest_evidence[0].path).expanduser().resolve().parent
    if destination.is_relative_to(snapshot_root):
        raise BerdlStagingPlanError("The BERDL staging plan must be written outside the immutable snapshot.")
    ingest_checkout = Path(plan.ingest.checkout).expanduser().resolve()
    if destination.is_relative_to(ingest_checkout):
        raise BerdlStagingPlanError("The BERDL staging plan must be written outside the KBase ingest checkout.")
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{destination.name}.", suffix=".tmp", dir=parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(render_berdl_staging_plan(plan))
            stream.write("\n")
        try:
            os.link(temporary, destination)
        except FileExistsError as error:
            raise BerdlStagingPlanError("Refusing to replace an existing BERDL staging plan.") from error
        except OSError as error:
            raise BerdlStagingPlanError("Cannot publish the BERDL staging plan atomically.") from error
    finally:
        temporary.unlink(missing_ok=True)
    return destination


def load_berdl_staging_plan(path: Path) -> BerdlStagingPlan:
    """Load one immutable plan with strict contract validation."""
    document = path.expanduser()
    if not document.is_file() or document.is_symlink():
        raise BerdlStagingPlanError("The BERDL staging plan must be an ordinary file.")
    try:
        return BerdlStagingPlan.model_validate_json(document.read_text(encoding="utf-8"), strict=True)
    except (OSError, ValidationError) as error:
        raise BerdlStagingPlanError("The BERDL staging plan is not valid.") from error


def _evidence_paths(plan: BerdlStagingPlan) -> dict[str, Path]:
    expected = {
        "snapshot-manifest.json",
        "metadata-bundle.json",
        "destination-inventory.json",
        "publication-plan.json",
        "metadata-application-plan.json",
        "target-validation-report.json",
    }
    paths = {item.name: Path(item.path) for item in plan.evidence}
    if set(paths) != expected or len(paths) != len(plan.evidence):
        raise BerdlStagingPlanError("The staging plan evidence set is not complete and unique.")
    return paths


def revalidate_berdl_staging_plan(
    plan: BerdlStagingPlan,
    *,
    runner: CommandRunner = _run_command,
) -> BerdlStagingPlan:
    """Rebuild a plan from its evidence instead of trusting stored arguments."""
    paths = _evidence_paths(plan)
    if len(plan.command) < 2:
        raise BerdlStagingPlanError("The staging plan command is incomplete.")
    script = Path(plan.command[1])
    if script.name != "ingest_dataset.py" or script.parent.name != "scripts":
        raise BerdlStagingPlanError("The staging plan command does not identify the BERIL staging script.")
    rebuilt = plan_berdl_staging(
        paths["snapshot-manifest.json"].parent,
        bundle_path=paths["metadata-bundle.json"],
        inventory_path=paths["destination-inventory.json"],
        publication_plan_path=paths["publication-plan.json"],
        metadata_plan_path=paths["metadata-application-plan.json"],
        target_validation_path=paths["target-validation-report.json"],
        beril_checkout=script.parent.parent,
        beril_revision=plan.beril.revision,
        tenant=plan.tenant,
        dataset=plan.dataset,
        bucket=plan.bucket,
        bronze_prefix=plan.bronze_prefix,
        progress_key=plan.progress_key,
        config_key=plan.config_key,
        runner=runner,
    )
    if rebuilt != plan:
        raise BerdlStagingPlanError("The staging plan no longer matches its reviewed evidence.")
    return rebuilt


def build_berdl_execution_command(
    plan: BerdlStagingPlan,
    upstream_outcome_path: Path,
) -> list[str]:
    """Add only BERIL's explicit staging gate and immutable outcome path."""
    outcome = upstream_outcome_path.expanduser()
    if outcome.exists() or outcome.is_symlink():
        raise BerdlStagingPlanError("Refusing to replace an existing BERIL upstream outcome.")
    if not outcome.parent.is_dir() or outcome.parent.is_symlink():
        raise BerdlStagingPlanError("The BERIL upstream outcome parent must be an ordinary directory.")
    return [*plan.command, "--outcome", str(outcome.resolve()), "--execute-staging"]


def load_upstream_staging_outcome(path: Path) -> UpstreamStagingOutcome:
    """Load the successful, credential-free outcome emitted by BERIL."""
    document = path.expanduser()
    if not document.is_file() or document.is_symlink():
        raise BerdlStagingPlanError("The BERIL upstream outcome must be an ordinary file.")
    try:
        return UpstreamStagingOutcome.model_validate_json(document.read_text(encoding="utf-8"), strict=True)
    except (OSError, ValidationError) as error:
        raise BerdlStagingPlanError("The BERIL upstream outcome is not a supported verified outcome.") from error


def build_berdl_staging_outcome(
    plan: BerdlStagingPlan,
    upstream: UpstreamStagingOutcome,
    *,
    staging_plan_sha256: str,
    upstream_outcome_sha256: str,
) -> BerdlStagingOutcome:
    """Independently bind BERIL's verified counts to the manifested snapshot."""
    if (
        upstream.destination.bucket != plan.bucket
        or upstream.destination.bronze_prefix != plan.bronze_prefix
        or upstream.destination.namespace != plan.staging_namespace
        or upstream.verification.namespace != plan.staging_namespace
    ):
        raise BerdlStagingPlanError("The BERIL outcome destination does not match the staging plan.")
    expected = {artifact.table: artifact for artifact in plan.artifacts}
    observed = {table.table: table for table in upstream.verification.tables}
    if set(observed) != set(expected) or len(observed) != len(upstream.verification.tables):
        raise BerdlStagingPlanError("The BERIL outcome table set does not match the staging plan.")
    tables: list[StagedTable] = []
    for name, artifact in sorted(expected.items()):
        table = observed[name]
        if table.source_rows != artifact.rows or table.destination_rows != artifact.rows:
            raise BerdlStagingPlanError(f"The BERIL outcome row counts do not match table '{name}'.")
        if table.source_basis != "source parquet":
            raise BerdlStagingPlanError(f"The BERIL outcome did not verify table '{name}' from Parquet.")
        tables.append(
            StagedTable(
                table=name,
                artifact_sha256=artifact.sha256,
                rows=table.source_rows,
                destination_rows=table.destination_rows,
                source_basis=table.source_basis,
            )
        )
    return BerdlStagingOutcome(
        outcome_format_version=OUTCOME_FORMAT_VERSION,
        status="data-verified",
        snapshot_id=plan.snapshot_id,
        staging_namespace=plan.staging_namespace,
        destination_id=plan.destination_id,
        bucket=plan.bucket,
        bronze_prefix=plan.bronze_prefix,
        progress_key=plan.progress_key,
        config_key=plan.config_key,
        beril_revision=plan.beril.revision,
        staging_plan_sha256=staging_plan_sha256,
        upstream_outcome_sha256=upstream_outcome_sha256,
        upstream_started_at=upstream.started_at,
        upstream_finished_at=upstream.finished_at,
        tables=tables,
    )


def render_berdl_staging_outcome(outcome: BerdlStagingOutcome) -> str:
    """Render stable, credential-free staging outcome JSON."""
    return json.dumps(outcome.model_dump(mode="json"), indent=2, sort_keys=True)


def write_berdl_staging_outcome(path: Path, outcome: BerdlStagingOutcome) -> Path:
    """Atomically create an NMDC outcome without replacing prior evidence."""
    destination = path.expanduser()
    if destination.exists() or destination.is_symlink():
        raise BerdlStagingPlanError("Refusing to replace an existing NMDC staging outcome.")
    parent = destination.parent
    if not parent.is_dir() or parent.is_symlink():
        raise BerdlStagingPlanError("The NMDC staging outcome parent must be an ordinary directory.")
    destination = destination.resolve()
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{destination.name}.", suffix=".tmp", dir=parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(render_berdl_staging_outcome(outcome))
            stream.write("\n")
        try:
            os.link(temporary, destination)
        except FileExistsError as error:
            raise BerdlStagingPlanError("Refusing to replace an existing NMDC staging outcome.") from error
        except OSError as error:
            raise BerdlStagingPlanError("Cannot publish the NMDC staging outcome atomically.") from error
    finally:
        temporary.unlink(missing_ok=True)
    return destination


def execute_berdl_staging(
    plan_path: Path,
    *,
    upstream_outcome_path: Path,
    output_path: Path,
    authorize_snapshot: str | None,
    execute_staging: bool,
    checkout_runner: CommandRunner = _run_command,
    staging_runner: CommandRunner = _run_staging_command,
) -> tuple[list[str], BerdlStagingOutcome | None]:
    """Preview or execute one revalidated, snapshot-authorized staging plan."""
    plan_sha256 = _sha256(plan_path, "BERDL staging plan")
    plan = revalidate_berdl_staging_plan(load_berdl_staging_plan(plan_path), runner=checkout_runner)
    command = build_berdl_execution_command(plan, upstream_outcome_path)
    output = output_path.expanduser()
    if output.exists() or output.is_symlink():
        raise BerdlStagingPlanError("Refusing to replace an existing NMDC staging outcome.")
    if not output.parent.is_dir() or output.parent.is_symlink():
        raise BerdlStagingPlanError("The NMDC staging outcome parent must be an ordinary directory.")
    if not execute_staging:
        return command, None
    if authorize_snapshot != plan.snapshot_id:
        raise BerdlStagingPlanError("Execution requires --authorize-snapshot with the exact snapshot ID.")
    result = staging_runner(command)
    if result.returncode != 0:
        raise BerdlStagingPlanError("BERIL staging did not complete successfully; retain its staging keys for review.")
    final_plan = revalidate_berdl_staging_plan(load_berdl_staging_plan(plan_path), runner=checkout_runner)
    if final_plan != plan or _sha256(plan_path, "BERDL staging plan") != plan_sha256:
        raise BerdlStagingPlanError("The staging plan or its evidence changed during BERIL execution.")
    upstream = load_upstream_staging_outcome(upstream_outcome_path)
    outcome = build_berdl_staging_outcome(
        plan,
        upstream,
        staging_plan_sha256=plan_sha256,
        upstream_outcome_sha256=_sha256(upstream_outcome_path, "BERIL upstream outcome"),
    )
    write_berdl_staging_outcome(output, outcome)
    return command, outcome
