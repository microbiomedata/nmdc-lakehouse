"""Stage or resume one reviewed publication using its existing evidence formats."""

from __future__ import annotations

import fcntl
import json
import os
import shlex
import sys
import tempfile
import traceback
from contextlib import ExitStack, redirect_stderr, redirect_stdout
from pathlib import Path
from typing import Any, cast

from nmdc_lakehouse import berdl_adapter, berdl_metadata, berdl_staging
from nmdc_lakehouse.metadata_application import MetadataApplicationPlan, load_metadata_application_plan
from nmdc_lakehouse.publication_prepare import PreparationError, file_digest, progress, save_json
from nmdc_lakehouse.snapshot_manifest import validate_snapshot


def _evidence(root: Path) -> Path:
    root = root.expanduser().absolute()
    if root.is_symlink() or (root / "evidence").is_symlink() or not (root / "evidence").is_dir():
        raise PreparationError("Use an ordinary publication directory with an evidence directory.")
    return root.resolve() / "evidence"


def _verified_data(evidence: Path, plan: berdl_staging.BerdlStagingPlan) -> berdl_staging.BerdlStagingOutcome:
    data, _ = berdl_metadata._read_model(
        evidence / "nmdc-staging-outcome.json", berdl_staging.BerdlStagingOutcome, "data outcome"
    )
    upstream, upstream_digest = berdl_staging._read_upstream_staging_outcome(evidence / "kbase-ingest-outcome.json")
    expected = berdl_staging.build_berdl_staging_outcome(
        plan,
        upstream,
        staging_plan_sha256=file_digest(evidence / "berdl-staging-plan.json"),
        upstream_outcome_sha256=upstream_digest,
    )
    if data != expected:
        raise PreparationError("The data outcome differs from the reviewed plan or upstream verification.")
    return expected


def publication_status(root: Path) -> dict[str, Any]:
    """Check recorded evidence locally; this is not a fresh live catalog audit."""
    evidence = _evidence(root)
    root = evidence.parent
    paths = {
        "evidence_paths": {
            label: str(evidence / name)
            for label, name in (
                ("plan", "berdl-staging-plan.json"),
                ("data_outcome", "nmdc-staging-outcome.json"),
                ("metadata_outcome", "nmdc-staging-metadata-outcome.json"),
            )
        },
        "log_paths": [str(p) for p in sorted(evidence.glob("staging-*.log")) if p.is_file() and not p.is_symlink()],
    }
    plan_path = evidence / "berdl-staging-plan.json"
    for name in (
        "berdl-staging-plan.json",
        "nmdc-staging-outcome.json",
        "kbase-ingest-outcome.json",
        "nmdc-staging-metadata-outcome.json",
        "staging-attempt.json",
    ):
        if (evidence / name).is_symlink():
            raise PreparationError("Publication evidence must use ordinary files.")
    if not plan_path.exists():
        file_digest(root / "preparation.json")
        receipt = json.loads((root / "preparation.json").read_bytes())
        manifest = validate_snapshot(root / "snapshot")
        if (
            receipt.get("status") != "prepared"
            or receipt.get("snapshot_id") != manifest.snapshot_id
            or receipt.get("parent_snapshot_id") != manifest.parent_snapshot_id
        ):
            raise PreparationError("Preparation is incomplete or belongs to another snapshot.")
        for name in ("metadata-profile.json", "metadata-bundle.json", "target-validation.json"):
            if receipt.get("evidence", {}).get(name) != file_digest(evidence / name):
                raise PreparationError("Prepared evidence changed.")
        return {
            **paths,
            "status": "prepared",
            "snapshot_id": manifest.snapshot_id,
            "next_action": "Send to the pod and run plan-publication with the destination configuration.",
        }
    plan = berdl_staging.revalidate_berdl_staging_plan(berdl_staging.load_berdl_staging_plan(plan_path))
    digest = file_digest(plan_path)
    result: dict[str, Any] = {
        **paths,
        "status": "planned",
        "snapshot_id": plan.snapshot_id,
        "namespace": plan.staging_namespace,
        "plan_sha256": digest,
        "tables": len(plan.artifacts),
        "rows": sum(a.rows for a in plan.artifacts),
        "basis": "recorded evidence; no live catalog audit",
        "next_command": shlex.join(
            [
                "nmdc-lakehouse",
                "stage-publication",
                str(root),
                "--authorize-snapshot",
                plan.snapshot_id,
                "--authorize-plan-sha256",
                digest,
                "--execute",
            ]
        ),
    }
    data_path = evidence / "nmdc-staging-outcome.json"
    metadata_path = evidence / "nmdc-staging-metadata-outcome.json"
    if not data_path.exists():
        if (
            metadata_path.exists()
            or (evidence / "staging-attempt.json").exists()
            or (evidence / "kbase-ingest-outcome.json").exists()
        ):
            result.update(
                status="partial-staging",
                next_command=None,
                next_action=(
                    "Retain the evidence and inspect the private staging log before choosing a new staging destination."
                ),
            )
        return result
    if not metadata_path.exists():
        _verified_data(evidence, plan)
        result["status"] = "data-verified-metadata-pending"
        return result
    metadata_plan, metadata = verified_staging_metadata(evidence, plan)
    result.update(
        status="data-and-table-metadata-verified",
        next_command=None,
        columns_verified=sum(len(t.columns_verified) for t in metadata.targets),
        missing_descriptions=len(metadata_plan.missing_descriptions),
        deferred_namespace_operations=metadata.deferred_namespace_operations,
    )
    return result


def verified_staging_metadata(
    evidence: Path, plan: berdl_staging.BerdlStagingPlan
) -> tuple[MetadataApplicationPlan, berdl_metadata.BerdlMetadataOutcome]:
    """Verify data and metadata outcome bindings without requiring the old staging runtime."""
    data_path = evidence / "nmdc-staging-outcome.json"
    metadata_path = evidence / "nmdc-staging-metadata-outcome.json"
    data = _verified_data(evidence, plan)
    metadata_plan = load_metadata_application_plan(evidence / "metadata-application-plan.json")
    preview = berdl_metadata.build_berdl_metadata_preview(
        metadata_plan,
        data,
        metadata_plan_sha256=file_digest(evidence / "metadata-application-plan.json"),
        staging_outcome_sha256=file_digest(data_path),
    )
    metadata, _ = berdl_metadata._read_model(metadata_path, berdl_metadata.BerdlMetadataOutcome, "metadata outcome")
    metadata = cast(berdl_metadata.BerdlMetadataOutcome, metadata)
    for field in (
        "snapshot_id",
        "destination_id",
        "staging_namespace",
        "staging_outcome_sha256",
        "metadata_plan_sha256",
        "deferred_namespace_operations",
    ):
        if getattr(metadata, field) != getattr(preview, field):
            raise PreparationError("The metadata outcome does not match the verified data and metadata plan.")
    table_ops, column_ops, _ = berdl_metadata._description_operations(metadata_plan)
    if sorted(t.table for t in metadata.targets) != metadata_plan.tables:
        raise PreparationError("Metadata verification does not cover the exact planned table set.")
    for target in metadata.targets:
        if (
            sorted(target.columns_verified) != sorted(column for column, _ in column_ops[target.table])
            or target.table_description_status != ("verified" if target.table in table_ops else "not-planned")
            or target.schema_properties_status
            != ("verified" if berdl_metadata._schema_properties(metadata_plan) else "not-planned")
        ):
            raise PreparationError(f"Metadata verification is incomplete for {target.table}.")
    return metadata_plan, metadata


def _fresh_destination(plan: berdl_staging.BerdlStagingPlan) -> object:
    """Refuse occupied staging names and keep a Spark reference through the upload."""
    spark, _, _ = berdl_metadata._runtime(Path(plan.ingest.checkout))
    _, client = berdl_adapter._runtime(Path(plan.ingest.checkout))
    namespaces = spark.sql(f"SHOW NAMESPACES IN `{plan.tenant}`").collect()
    if plan.dataset in {row["namespace"] for row in namespaces}:
        raise PreparationError("The staging namespace already exists; select a new destination.")
    if next(iter(client.list_objects(plan.bucket, prefix=plan.bronze_prefix + "/", recursive=True)), None) is not None:
        raise PreparationError("The staging object prefix is occupied; select a new destination.")
    return spark


def stage_publication(
    root: Path,
    *,
    authorize_snapshot: str | None = None,
    authorize_plan_sha256: str | None = None,
    execute: bool = False,
) -> dict[str, Any]:
    """Preview, stage, or retry metadata alone without regenerating the reviewed plan."""
    evidence = _evidence(root)
    with ExitStack() as stack:
        if execute:
            lock = evidence / ".stage.lock"
            if lock.is_symlink():
                raise PreparationError("The staging lock cannot be a symlink.")
            stream = stack.enter_context(lock.open("a"))
            try:
                fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as error:
                raise PreparationError("Another staging command is using this publication.") from error
        state = publication_status(root)
        if state["status"] in {"prepared", "partial-staging"}:
            raise PreparationError(str(state["next_action"]))
        if state["status"] == "data-and-table-metadata-verified":
            return state
        plan_path = evidence / "berdl-staging-plan.json"
        plan = berdl_staging.load_berdl_staging_plan(plan_path)
        data_path = evidence / "nmdc-staging-outcome.json"
        metadata_path = evidence / "nmdc-staging-metadata-outcome.json"
        retry = state["status"] == "data-verified-metadata-pending"
        if retry:
            metadata_plan, data, preview = berdl_metadata.load_berdl_metadata_preview(
                evidence / "metadata-application-plan.json",
                data_path,
                staging_plan_path=plan_path,
                output_path=metadata_path,
                ingest_checkout=Path(plan.ingest.checkout),
            )
            if not execute:
                return {**preview.model_dump(mode="json"), "phase": "metadata-only"}
        elif not execute:
            return berdl_metadata.execute_berdl_staging_with_metadata(
                plan_path,
                upstream_outcome_path=evidence / "kbase-ingest-outcome.json",
                output_path=data_path,
                metadata_output_path=metadata_path,
                authorize_snapshot=None,
                authorize_plan_sha256=None,
                execute_staging=False,
            )
        if authorize_snapshot != plan.snapshot_id or authorize_plan_sha256 != state["plan_sha256"]:
            raise PreparationError("Execution requires the exact reviewed snapshot ID and staging plan SHA-256.")
        fd, name = tempfile.mkstemp(prefix="staging-", suffix=".log", dir=evidence)
        print(f"Private staging log: {name}", file=sys.stderr, flush=True)
        with progress("metadata retry" if retry else "data and metadata staging"), os.fdopen(fd, "w") as log:
            with redirect_stdout(log), redirect_stderr(log):
                try:
                    if retry:
                        outcome = berdl_metadata.apply_berdl_staging_metadata(
                            metadata_plan, data, preview, ingest_checkout=Path(plan.ingest.checkout)
                        )
                        berdl_metadata.write_berdl_metadata_outcome(metadata_path, outcome)
                    else:
                        spark = _fresh_destination(plan)
                        save_json(
                            evidence / "staging-attempt.json",
                            {"snapshot_id": plan.snapshot_id, "plan_sha256": state["plan_sha256"]},
                        )
                        berdl_metadata.execute_berdl_staging_with_metadata(
                            plan_path,
                            upstream_outcome_path=evidence / "kbase-ingest-outcome.json",
                            output_path=data_path,
                            metadata_output_path=metadata_path,
                            authorize_snapshot=authorize_snapshot,
                            authorize_plan_sha256=authorize_plan_sha256,
                            execute_staging=True,
                        )
                        del spark
                except (Exception, KeyboardInterrupt) as error:
                    traceback.print_exc(file=log)
                    raise PreparationError(f"Staging stopped; retain the evidence and inspect {name}.") from error
        return publication_status(root)
