"""Prepare reusable publication evidence from one configuration."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
from contextlib import contextmanager
from datetime import UTC, datetime
from importlib.metadata import version
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator

from nmdc_lakehouse.metadata_bundle import (
    DescriptionOverride,
    MetadataProfile,
    NamespaceProfile,
    build_metadata_bundle,
    load_metadata_profile,
)
from nmdc_lakehouse.snapshot_manifest import validate_snapshot


class PreparationError(ValueError):
    """Preparation cannot proceed without changing or repairing its inputs."""


class PreparationConfig(BaseModel):
    """Local inputs and reviewed namespace content; paths are relative to this file."""

    model_config = ConfigDict(extra="forbid")

    source_version: str = Field(pattern=r"^\d+\.\d+\.\d+$")
    snapshot: Path | None = None
    target_validation: Path | None = None
    profile: Path | None = None
    namespace: NamespaceProfile | None = None
    overrides: list[DescriptionOverride] = Field(default_factory=list)
    source_label: str = Field(default="nmdc-production", pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")

    @model_validator(mode="after")
    def reviewed_metadata(self) -> PreparationConfig:
        """Require one source of reviewed descriptions, without competing overrides."""
        if (self.profile is None) == (self.namespace is None):
            raise ValueError("Supply either a reviewed profile path or namespace metadata.")
        if self.profile is not None and self.overrides:
            raise ValueError("Put description overrides in the reviewed profile when using one.")
        if self.snapshot is None and (self.target_validation is not None or self.profile is not None):
            raise ValueError("Saved validation and profiles require the existing snapshot they describe.")
        return self


def file_digest(path: Path) -> str:
    """Hash an ordinary file without loading it into memory."""
    if path.is_symlink() or not path.is_file():
        raise PreparationError(f"Expected an ordinary file: {path.name}")
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def save_json(path: Path, value: Any) -> None:
    """Publish one complete document, allowing only an identical existing result."""
    raw = json.dumps(value, sort_keys=True, indent=2) + "\n"
    if path.exists() or path.is_symlink():
        file_digest(path)
        if path.read_text() != raw:
            raise PreparationError(f"Existing {path.name} differs; use a new preparation directory.")
        return
    fd, name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w") as stream:
            stream.write(raw)
        os.link(name, path)
    finally:
        Path(name).unlink(missing_ok=True)


@contextmanager
def progress(label: str):
    """Keep long validation or export phases visible without exposing runtime diagnostics."""
    print(f"Starting {label}", file=sys.stderr, flush=True)
    done = threading.Event()

    def heartbeat():
        while not done.wait(30):
            print(f"Still running: {label}", file=sys.stderr, flush=True)

    worker = threading.Thread(target=heartbeat, daemon=True)
    worker.start()
    try:
        yield
    finally:
        done.set()
        worker.join()


def _copy(source: Path, destination: Path) -> None:
    checksum = file_digest(source)
    fd, name = tempfile.mkstemp(prefix=f".{destination.name}.", dir=destination.parent)
    os.close(fd)
    try:
        shutil.copyfile(source, name)
        if file_digest(Path(name)) != checksum or file_digest(source) != checksum:
            raise PreparationError("An input changed while being copied.")
        os.link(name, destination)
    finally:
        Path(name).unlink(missing_ok=True)


def _export(root: Path, source_label: str) -> None:
    snapshot = root / "snapshot"
    if snapshot.exists():
        raise PreparationError(
            "An export exists; reuse it as an explicit snapshot input in a new directory if its manifest validates."
        )
    env = dict(
        os.environ, LAKEHOUSE_ROOT=str(snapshot), LAKEHOUSE_SKIP_COLLECTIONS="", LAKEHOUSE_DROP_EMPTY_COLS="false"
    )
    command = [sys.executable, "-c", "from nmdc_lakehouse.cli import cli; cli()"]
    log_path = root / "export.log"
    if log_path.is_symlink():
        raise PreparationError("The export log cannot be a symlink.")
    fd = os.open(log_path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o600)
    with os.fdopen(fd, "a") as log, progress("all-collections export"):
        for args in (
            ["run-job", "all-collections", "--metrics", str(snapshot / "etl-metrics.json")],
            [
                "create-snapshot-manifest",
                str(snapshot),
                "--metrics",
                str(snapshot / "etl-metrics.json"),
                "--source-label",
                source_label,
            ],
        ):
            result = subprocess.run([*command, *args], env=env, stdout=log, stderr=log, check=False)  # noqa: S603
            if result.returncode:
                raise PreparationError(
                    f"{args[0]} failed; inspect the private export.log. Completed files are retained."
                )


def prepare_publication(config_path: Path, root: Path) -> dict[str, Any]:
    """Export or reuse a snapshot and bind full validation and metadata in one durable directory."""
    from nmdc_lakehouse.berdl_staging import _require_target_validation
    from nmdc_lakehouse.target_validation import (
        assert_source_schema_aligned,
        load_target_validation_report,
        validate_target_snapshot,
        write_target_validation_report,
    )

    config_path = config_path.expanduser().absolute()
    configuration_digest = file_digest(config_path)
    config = PreparationConfig.model_validate_json(config_path.read_bytes())
    for field in ("snapshot", "target_validation", "profile"):
        value = getattr(config, field)
        if value is not None:
            path = config_path.parent / value.expanduser()
            if path.is_symlink():
                raise PreparationError(f"The {field} input cannot be a symlink.")
            setattr(config, field, path.resolve())
    if version("nmdc-schema") != config.source_version:
        raise PreparationError("Installed nmdc-schema differs from source_version; use just prepare-publication.")
    assert_source_schema_aligned()
    root = root.expanduser().absolute()
    if root.is_symlink() or (root / "evidence").is_symlink() or (root / "snapshot").is_symlink():
        raise PreparationError("Preparation directories cannot be symlinks.")
    root = root.resolve()
    if config.snapshot is not None and (root.is_relative_to(config.snapshot) or config.snapshot.is_relative_to(root)):
        raise PreparationError("The input snapshot and preparation directory must be disjoint.")
    if root.exists() and root.stat().st_mode & 0o077:
        raise PreparationError("The existing preparation directory must be private (mode 0700).")
    if root.exists() and any(root.iterdir()):
        raise PreparationError(
            "The preparation directory is not empty; use a new directory with completed snapshot/report inputs. "
            "An existing .prepare.lock also prevents reuse after interruption."
        )
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    lock = root / ".prepare.lock"
    try:
        stream = lock.open("x")
    except FileExistsError as error:
        raise PreparationError("Another preparation has claimed this directory; use a new one.") from error
    with stream:
        input_paths = {
            name: path
            for name, path in (
                ("validation", config.target_validation),
                ("profile", config.profile),
                ("manifest", config.snapshot / "snapshot-manifest.json" if config.snapshot is not None else None),
            )
            if path is not None
        }
        inputs = {
            "config": config.model_dump(mode="json"),
            "input_hashes": {n: file_digest(p) for n, p in input_paths.items()},
        }
        evidence = root / "evidence"
        evidence.mkdir(exist_ok=True)
        snapshot = root / "snapshot"
        if config.snapshot is None:
            _export(root, config.source_label)
        else:
            with progress("snapshot verification and copy"):
                original = validate_snapshot(config.snapshot)
                if original.software.nmdc_schema_version != config.source_version:
                    raise PreparationError("The snapshot does not describe the configured source version.")
                snapshot.mkdir(exist_ok=True)
                for name in [
                    "snapshot-manifest.json",
                    original.performance_record.path,
                    *(a.path for a in original.artifacts),
                ]:
                    _copy(config.snapshot / name, snapshot / name)
        manifest = validate_snapshot(snapshot)
        if manifest.software.nmdc_schema_version != config.source_version:
            raise PreparationError("The snapshot does not describe the configured source version.")
        report_path = evidence / "target-validation.json"
        if config.target_validation is not None:
            report = load_target_validation_report(config.target_validation)
        else:
            with progress("full target row validation"):
                report = validate_target_snapshot(snapshot, requested_mode="full")
        _require_target_validation(manifest, report)
        if report.requested_mode != "full":
            raise PreparationError("Publication preparation requires a full target validation report.")
        if config.target_validation is not None:
            _copy(config.target_validation, report_path)
        else:
            write_target_validation_report(report_path, report, snapshot_root=snapshot)
        if config.profile is not None:
            profile = load_metadata_profile(config.profile)
        else:
            assert config.namespace is not None
            profile = MetadataProfile(
                profile_format_version=1,
                profile_id="nmdc-" + manifest.snapshot_id.removeprefix("sha256:"),
                snapshot_id=manifest.snapshot_id,
                namespace=config.namespace,
                overrides=config.overrides,
            )
        save_json(evidence / "metadata-profile.json", profile.model_dump(mode="json"))
        bundle_path = evidence / "metadata-bundle.json"
        bundle = build_metadata_bundle(
            snapshot,
            manifest,
            profile,
            generated_at=datetime.now(UTC).isoformat(),
        )
        save_json(bundle_path, bundle.model_dump(mode="json"))
        if validate_snapshot(snapshot) != manifest:
            raise PreparationError("The snapshot changed during preparation.")
        if file_digest(config_path) != configuration_digest or inputs["input_hashes"] != {
            name: file_digest(path) for name, path in input_paths.items()
        }:
            raise PreparationError("Preparation inputs changed during the run; no completion receipt was published.")
        receipt = {
            "status": "prepared",
            "snapshot_id": manifest.snapshot_id,
            "source_version": config.source_version,
            "parent_snapshot_id": manifest.parent_snapshot_id,
            "tables": len(manifest.artifacts),
            "rows": sum(a.rows for a in manifest.artifacts),
            "evidence": {
                p.name: file_digest(p) for p in (report_path, evidence / "metadata-profile.json", bundle_path)
            },
        }
        save_json(root / "preparation.json", receipt)
        return receipt
