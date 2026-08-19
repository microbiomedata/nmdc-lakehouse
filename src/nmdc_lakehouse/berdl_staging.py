"""Build an immutable, non-mutating BERDL staging command plan."""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import tempfile
from collections.abc import Callable, Sequence
from pathlib import Path, PurePosixPath
from typing import Literal

from pydantic import BaseModel, ConfigDict

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
)

PLAN_FORMAT_VERSION: Literal[1] = 1
_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_STAGING_DATASET = re.compile(r"[A-Za-z_][A-Za-z0-9_]*_staging_[A-Za-z0-9_]+\Z")
_BUCKET = re.compile(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]\Z")
_OBJECT_SEGMENT = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*\Z")
_REVISION = re.compile(r"[0-9a-f]{40}\Z")

CommandRunner = Callable[[Sequence[str]], subprocess.CompletedProcess[str]]


class BerdlStagingPlanError(ValueError):
    """Raised when reviewed evidence cannot produce a safe staging plan."""


class EvidenceDigest(BaseModel):
    """Content identity for one reviewed JSON input."""

    model_config = ConfigDict(extra="forbid", strict=True)

    name: str
    path: str
    sha256: str


class BerilRevision(BaseModel):
    """Exact external implementation selected for later execution."""

    model_config = ConfigDict(extra="forbid", strict=True)

    revision: str
    ingest_script_sha256: str
    ingest_library_sha256: str


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
    staging_namespace: str
    tenant: str
    dataset: str
    bucket: str
    bronze_prefix: str
    progress_key: str
    config_key: str
    evidence: list[EvidenceDigest]
    target_validation: TargetValidationEvidence
    beril: BerilRevision
    artifacts: list[StagingArtifact]
    command: list[str]


def _run_command(args: Sequence[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, text=True, capture_output=True, check=False, timeout=10)  # noqa: S603


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


def _is_executable_file(path: Path) -> bool:
    try:
        resolved = path.resolve(strict=True)
    except (OSError, RuntimeError):
        return False
    return resolved.is_file() and not resolved.is_symlink() and os.access(resolved, os.X_OK)


def _validate_object_key(value: str, label: str) -> None:
    path = PurePosixPath(value)
    if path.is_absolute() or not value or value.endswith("/") or str(path) != value:
        raise BerdlStagingPlanError(f"The {label} must be a relative object key.")
    if any(part in {"", ".", ".."} or not _OBJECT_SEGMENT.fullmatch(part) for part in path.parts):
        raise BerdlStagingPlanError(f"The {label} contains an unsafe path segment.")


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
    if not _BUCKET.fullmatch(bucket):
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
    if report.status != "success" or report.invalid_rows != 0:
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
    for name, artifact in artifacts.items():
        table = tables[name]
        full = report.requested_mode == "full" or artifact.rows <= report.full_table_max_rows
        expected_mode = "full" if full else "sampled"
        expected_selected = artifact.rows if full else min(artifact.rows, report.sample_rows)
        if (
            table.artifact_path != artifact.path
            or table.target_class != artifact.target_class
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


def _inspect_beril_checkout(
    checkout: Path,
    expected_revision: str,
    runner: CommandRunner,
) -> tuple[Path, Path, Path, BerilRevision]:
    if not _REVISION.fullmatch(expected_revision):
        raise BerdlStagingPlanError("The BERIL revision must be a full lowercase Git commit.")
    checkout = checkout.expanduser()
    if not checkout.is_dir() or checkout.is_symlink():
        raise BerdlStagingPlanError("The BERIL checkout must be an ordinary directory.")
    checkout = checkout.resolve()
    script = checkout / "scripts" / "ingest_dataset.py"
    library = checkout / "scripts" / "ingest_lib.py"
    python_candidates = (
        checkout / ".venv-berdl" / "bin" / "python",
        checkout / ".venv-berdl" / "Scripts" / "python.exe",
    )
    python = next(
        (candidate for candidate in python_candidates if _is_executable_file(candidate)),
        None,
    )
    for path, label in ((script, "BERIL staging command"), (library, "BERIL ingest library")):
        if not path.is_file() or path.is_symlink():
            raise BerdlStagingPlanError(f"The {label} must be an ordinary file.")
    if python is None:
        raise BerdlStagingPlanError("The BERIL checkout has no .venv-berdl Python interpreter.")
    try:
        revision = runner(("git", "-C", str(checkout), "rev-parse", "--verify", "HEAD"))
        dirty = runner(("git", "-C", str(checkout), "status", "--porcelain", "--untracked-files=no"))
        tracked = runner(
            (
                "git",
                "-C",
                str(checkout),
                "ls-files",
                "--error-unmatch",
                "--",
                "scripts/ingest_dataset.py",
                "scripts/ingest_lib.py",
            )
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise BerdlStagingPlanError("Cannot inspect the BERIL checkout revision.") from error
    if revision.returncode != 0 or revision.stdout.strip() != expected_revision:
        raise BerdlStagingPlanError("The BERIL checkout does not match the requested revision.")
    if dirty.returncode != 0 or dirty.stdout.strip():
        raise BerdlStagingPlanError("The BERIL checkout must have no tracked changes.")
    if tracked.returncode != 0 or set(tracked.stdout.splitlines()) != {
        "scripts/ingest_dataset.py",
        "scripts/ingest_lib.py",
    }:
        raise BerdlStagingPlanError("The BERIL staging sources must be tracked by the selected revision.")
    evidence = BerilRevision(
        revision=expected_revision,
        ingest_script_sha256=_sha256(script, "BERIL staging command"),
        ingest_library_sha256=_sha256(library, "BERIL ingest library"),
    )
    return checkout, python, script, evidence


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
    beril_checkout: Path,
    beril_revision: str,
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
    _require_target_validation(manifest, target_validation)
    _require_metadata_agreement(manifest, bundle, inventory, metadata_plan, staging_namespace)
    artifacts = _select_artifacts(manifest, publication_plan)
    _checkout, python, script, beril = _inspect_beril_checkout(beril_checkout, beril_revision, runner)
    root = snapshot_root.resolve()
    command = [
        str(python),
        str(script),
        "--data-dir",
        str(root),
        "--tenant",
        tenant,
        "--dataset",
        dataset,
        "--staging-namespace",
        staging_namespace,
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
        beril=beril,
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
    beril_checkout: Path,
    beril_revision: str,
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
    if digests != [_sha256(path, label) for path, _name, label in paths]:
        raise BerdlStagingPlanError("A reviewed input changed while the BERDL staging plan was built.")
    evidence = [
        EvidenceDigest(name=name, path=str(path.expanduser().resolve()), sha256=digest)
        for (path, name, _label), digest in zip(paths, digests, strict=True)
    ]
    return build_berdl_staging_plan(
        snapshot_root=root,
        manifest=manifest,
        bundle=bundle,
        inventory=inventory,
        publication_plan=publication_plan,
        metadata_plan=metadata_plan,
        target_validation=target_validation,
        evidence=evidence,
        beril_checkout=beril_checkout,
        beril_revision=beril_revision,
        tenant=tenant,
        dataset=dataset,
        bucket=bucket,
        bronze_prefix=bronze_prefix,
        progress_key=progress_key,
        config_key=config_key,
        runner=runner,
    )


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
