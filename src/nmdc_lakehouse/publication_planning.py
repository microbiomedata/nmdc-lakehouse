"""Build the existing staging evidence together from a prepared publication."""

from __future__ import annotations

import json
from pathlib import Path

from pydantic import BaseModel, ConfigDict

from nmdc_lakehouse.berdl_staging import (
    BerdlStagingPlan,
    CommandRunner,
    _run_command,
    plan_berdl_staging,
    revalidate_berdl_staging_plan,
    write_berdl_staging_plan,
)
from nmdc_lakehouse.metadata_application import build_metadata_application_plan
from nmdc_lakehouse.metadata_bundle import load_metadata_bundle
from nmdc_lakehouse.publication_plan import (
    Disposition,
    PolicyRule,
    PublicationPolicy,
    build_publication_plan,
    load_destination_inventory,
)
from nmdc_lakehouse.publication_preflight import build_publication_preflight
from nmdc_lakehouse.publication_prepare import PreparationError, _copy, file_digest, save_json
from nmdc_lakehouse.snapshot_manifest import validate_snapshot


class PlanningConfig(BaseModel):
    """Pod-local destination inputs; relative paths use the configuration directory."""

    model_config = ConfigDict(extra="forbid")

    inventory: Path
    ingest_checkout: Path
    ingest_revision: str
    staging_namespace: str
    bucket: str
    bronze_prefix: str


def plan_publication(root: Path, configuration: Path, *, runner: CommandRunner = _run_command) -> BerdlStagingPlan:
    """Rebuild and bind all plans without service access or changing reviewed evidence."""
    root = root.expanduser().absolute()
    if any(p.is_symlink() or not p.is_dir() for p in (root, root / "snapshot", root / "evidence")):
        raise PreparationError("Use an ordinary prepared publication directory.")
    root = root.resolve()
    configuration = configuration.expanduser().absolute()
    file_digest(configuration)
    config = PlanningConfig.model_validate_json(configuration.read_bytes())
    for name in ("inventory", "ingest_checkout"):
        path = configuration.parent / getattr(config, name).expanduser()
        if path.is_symlink():
            raise PreparationError(f"The {name} input cannot be a symlink.")
        setattr(config, name, path.resolve())
    parts = config.staging_namespace.split(".")
    if len(parts) != 2:
        raise PreparationError("Use a catalog-qualified staging namespace: tenant.dataset.")
    tenant, dataset = parts
    evidence = root / "evidence"
    snapshot = root / "snapshot"
    receipt_path = root / "preparation.json"
    file_digest(receipt_path)
    receipt = json.loads(receipt_path.read_bytes())
    expected = {"metadata-profile.json", "metadata-bundle.json", "target-validation.json"}
    if receipt.get("status") != "prepared" or set(receipt.get("evidence", {})) != expected:
        raise PreparationError("The preparation receipt is incomplete.")
    for name, checksum in receipt["evidence"].items():
        if file_digest(evidence / name) != checksum:
            raise PreparationError(f"Prepared evidence changed: {name}")
    manifest = validate_snapshot(snapshot)
    if (
        receipt.get("snapshot_id") != manifest.snapshot_id
        or receipt.get("parent_snapshot_id") != manifest.parent_snapshot_id
    ):
        raise PreparationError("Preparation and snapshot identities differ.")
    bundle = load_metadata_bundle(evidence / "metadata-bundle.json")
    inventory = load_destination_inventory(config.inventory)
    policy = PublicationPolicy(
        policy_format_version=1,
        rules=[
            PolicyRule(
                table=table.name,
                disposition=Disposition.PRESERVE,
                rationale="Outside this staging snapshot; staging does not change canonical tables.",
            )
            for table in inventory.tables
            if table.name not in {artifact.table for artifact in manifest.artifacts}
        ],
    )
    publication = build_publication_plan(manifest, inventory, policy)
    preflight = build_publication_preflight(manifest, bundle, inventory, publication)
    metadata = build_metadata_application_plan(bundle, inventory, config.staging_namespace)
    save_json(
        evidence / "planning-inputs.json",
        {
            "config": config.model_dump(mode="json"),
            "preparation_sha256": file_digest(receipt_path),
            "inventory_sha256": file_digest(config.inventory),
        },
    )
    inventory_path = evidence / "destination-inventory.json"
    if config.inventory != inventory_path:
        _copy(config.inventory, inventory_path)
    for name, model in (
        ("publication-policy.json", policy),
        ("publication-plan.json", publication),
        ("publication-preflight.json", preflight),
        ("metadata-application-plan.json", metadata),
    ):
        save_json(evidence / name, model.model_dump(mode="json"))
    plan = plan_berdl_staging(
        snapshot,
        bundle_path=evidence / "metadata-bundle.json",
        inventory_path=inventory_path,
        publication_plan_path=evidence / "publication-plan.json",
        metadata_plan_path=evidence / "metadata-application-plan.json",
        target_validation_path=evidence / "target-validation.json",
        ingest_checkout=config.ingest_checkout,
        ingest_revision=config.ingest_revision,
        tenant=tenant,
        dataset=dataset,
        bucket=config.bucket,
        bronze_prefix=config.bronze_prefix,
        progress_key=f"{config.bronze_prefix}/progress.jsonl",
        config_key=f"{config.bronze_prefix}/config.json",
        runner=runner,
    )
    output = evidence / "berdl-staging-plan.json"
    if output.exists() or output.is_symlink():
        save_json(output, plan.model_dump(mode="json"))
    else:
        write_berdl_staging_plan(output, plan)
    return revalidate_berdl_staging_plan(plan, runner=runner)
