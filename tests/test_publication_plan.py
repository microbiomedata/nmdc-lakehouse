"""Tests for destination-neutral offline publication planning."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from nmdc_lakehouse.cli import cli
from nmdc_lakehouse.publication_plan import (
    DestinationInventory,
    DestinationTable,
    Disposition,
    MetadataCapability,
    PolicyRule,
    PublicationPlanError,
    PublicationPolicy,
    build_publication_plan,
    load_destination_inventory,
    load_publication_plan,
    load_publication_policy,
    publication_json_schema,
    render_publication_plan,
    write_publication_plan,
)
from nmdc_lakehouse.snapshot_manifest import (
    ArtifactRecord,
    PerformanceRecord,
    SnapshotManifest,
    SoftwareRecord,
)


def _artifact(table: str, rows: int, fingerprint: str) -> ArtifactRecord:
    return ArtifactRecord(
        path=f"{table}.parquet",
        table=table,
        rows=rows,
        bytes=100,
        sha256="f" * 64,
        physical_schema_sha256=fingerprint,
        footer_schema_sha256="e" * 64,
        source_schema_id="https://w3id.org/nmdc/nmdc",
        source_schema_version="11.10.0",
        source_class=table,
        target_schema_id="https://w3id.org/nmdc/nmdc-schema-flattened",
        target_class=table,
        mapping="nmdc_lakehouse.transforms.flatteners.SchemaDrivenFlattener",
    )


def _manifest() -> SnapshotManifest:
    return SnapshotManifest(
        manifest_format_version=1,
        snapshot_id="sha256:" + "1" * 64,
        generated_at="2026-08-18T16:00:00+00:00",
        scope="full-mongodb-metadata-snapshot",
        parent_snapshot_id=None,
        source_label="nmdc-production",
        included_collections=["biosample_set", "study_set"],
        skipped_collections=[],
        footer_metadata_format_version="1",
        target_schema_ids=["https://w3id.org/nmdc/nmdc-schema-flattened"],
        mapping_ids=["nmdc_lakehouse.transforms.flatteners.SchemaDrivenFlattener"],
        software=SoftwareRecord(
            nmdc_lakehouse_version="0.2.0.dev0",
            git_commit=None,
            git_dirty=None,
            nmdc_schema_version="11.10.0",
            python_version="3.13.13",
        ),
        performance_record=PerformanceRecord(path="etl-metrics.json", sha256="d" * 64),
        artifacts=[
            _artifact("biosample_set", 10, "a" * 64),
            _artifact("study_set", 5, "b" * 64),
        ],
    )


def _inventory() -> DestinationInventory:
    return DestinationInventory(
        inventory_format_version=1,
        destination_id="example-lakehouse",
        observed_at="2026-08-18T17:00:00+00:00",
        provider="example",
        table_format="iceberg",
        metadata_capabilities=[MetadataCapability.TABLE, MetadataCapability.COLUMN],
        tables=[
            DestinationTable(name="biosample_set", rows=9, physical_schema_sha256="c" * 64),
            DestinationTable(name="functional_annotation_agg", rows=100, physical_schema_sha256="d" * 64),
        ],
    )


def _policy(*rules: PolicyRule) -> PublicationPolicy:
    return PublicationPolicy(policy_format_version=1, rules=list(rules))


def test_plan_covers_union_with_defaults_and_reviewed_live_only_policy() -> None:
    policy = _policy(
        PolicyRule(
            table="functional_annotation_agg",
            disposition=Disposition.PRESERVE,
            rationale="No verified candidate replacement exists.",
        )
    )

    plan = build_publication_plan(_manifest(), _inventory(), policy)

    assert plan.candidate_snapshot_id == "sha256:" + "1" * 64
    assert plan.destination_id == "example-lakehouse"
    assert [entry.table for entry in plan.tables] == ["biosample_set", "functional_annotation_agg", "study_set"]
    dispositions = {entry.table: entry.disposition for entry in plan.tables}
    assert dispositions == {
        "biosample_set": Disposition.REPLACE,
        "functional_annotation_agg": Disposition.PRESERVE,
        "study_set": Disposition.ADD,
    }
    biosample = plan.tables[0]
    assert biosample.candidate_rows == 10
    assert biosample.destination_rows == 9
    assert biosample.candidate_physical_schema_sha256 == "a" * 64
    assert biosample.destination_physical_schema_sha256 == "c" * 64


def test_live_only_table_requires_reviewed_policy() -> None:
    with pytest.raises(PublicationPlanError, match="Live-only tables require.*functional_annotation_agg"):
        build_publication_plan(_manifest(), _inventory(), _policy())


@pytest.mark.parametrize(
    ("table", "disposition"),
    [
        ("biosample_set", Disposition.RETIRE),
        ("study_set", Disposition.PRESERVE),
        ("functional_annotation_agg", Disposition.REPLACE),
    ],
)
def test_plan_rejects_dispositions_incompatible_with_table_presence(table: str, disposition: Disposition) -> None:
    rules = [
        PolicyRule(
            table="functional_annotation_agg",
            disposition=Disposition.PRESERVE,
            rationale="Preserve the live-only table.",
        )
    ]
    rules.append(PolicyRule(table=table, disposition=disposition, rationale="Injected unsafe rule."))
    if table == "functional_annotation_agg":
        rules.pop(0)

    with pytest.raises(PublicationPlanError, match="unsafe"):
        build_publication_plan(_manifest(), _inventory(), _policy(*rules))


def test_plan_rejects_unknown_policy_table() -> None:
    policy = _policy(PolicyRule(table="unknown_table", disposition=Disposition.PRESERVE, rationale="Unknown."))

    with pytest.raises(PublicationPlanError, match="unknown table.*unknown_table"):
        build_publication_plan(_manifest(), _inventory(), policy)


def test_loaders_reject_duplicate_inventory_and_policy_entries(tmp_path: Path) -> None:
    inventory_path = tmp_path / "inventory.json"
    inventory = _inventory()
    inventory.tables.append(inventory.tables[0].model_copy(deep=True))
    inventory_path.write_text(inventory.model_dump_json(), encoding="utf-8")
    with pytest.raises(PublicationPlanError, match="duplicate table"):
        load_destination_inventory(inventory_path)

    policy_path = tmp_path / "policy.json"
    rule = PolicyRule(table="biosample_set", disposition=Disposition.REPLACE, rationale="Reviewed.")
    policy_path.write_text(_policy(rule, rule.model_copy(deep=True)).model_dump_json(), encoding="utf-8")
    with pytest.raises(PublicationPlanError, match="duplicate table"):
        load_publication_policy(policy_path)


@pytest.mark.parametrize("source", ["manifest", "inventory", "policy"])
def test_builder_rejects_programmatic_duplicate_table_entries(source: str) -> None:
    manifest = _manifest()
    inventory = _inventory()
    policy = _policy(
        PolicyRule(
            table="functional_annotation_agg",
            disposition=Disposition.PRESERVE,
            rationale="Preserve the live-only table.",
        )
    )
    if source == "manifest":
        manifest.artifacts.append(manifest.artifacts[0].model_copy(deep=True))
    elif source == "inventory":
        inventory.tables.append(inventory.tables[0].model_copy(deep=True))
    else:
        policy.rules.append(policy.rules[0].model_copy(deep=True))

    with pytest.raises(PublicationPlanError, match="duplicate table names"):
        build_publication_plan(manifest, inventory, policy)


@pytest.mark.parametrize(
    ("field", "value"),
    [("rows", -1), ("rows", "10"), ("physical_schema_sha256", "not-a-sha256")],
)
def test_inventory_rejects_malformed_counts_and_schemas(tmp_path: Path, field: str, value: object) -> None:
    inventory = json.loads(_inventory().model_dump_json())
    inventory["tables"][0][field] = value
    path = tmp_path / "inventory.json"
    path.write_text(json.dumps(inventory), encoding="utf-8")

    with pytest.raises(PublicationPlanError, match="Cannot read a valid destination inventory"):
        load_destination_inventory(path)


def test_publication_json_schemas_are_versioned() -> None:
    # Asserted per document rather than as one number, because they version independently:
    # the inventory moved to 2 when DestinationTable gained observed_table_format.
    expected = {"inventory": 2, "policy": 1, "plan": 1}
    for document, version in expected.items():
        schema = publication_json_schema(document)  # type: ignore[arg-type]
        assert schema["x-format-version"] == version
        assert schema["additionalProperties"] is False


def test_cli_schema_and_invalid_candidate_are_offline(tmp_path: Path) -> None:
    schema = CliRunner().invoke(cli, ["publication-plan-schema", "inventory"])
    invalid = CliRunner().invoke(
        cli,
        [
            "publication-plan",
            str(tmp_path / "missing-snapshot"),
            "--inventory",
            str(tmp_path / "missing-inventory.json"),
            "--policy",
            str(tmp_path / "missing-policy.json"),
        ],
    )

    assert schema.exit_code == 0
    # This asks for the inventory schema specifically, which is at version 2.
    assert json.loads(schema.output)["x-format-version"] == 2
    assert invalid.exit_code != 0
    assert "Snapshot root must be an existing ordinary directory" in invalid.output


def test_write_plan_is_atomic_and_rejects_symlink_output(tmp_path: Path) -> None:
    policy = _policy(
        PolicyRule(
            table="functional_annotation_agg",
            disposition=Disposition.PRESERVE,
            rationale="No verified replacement.",
        )
    )
    plan = build_publication_plan(_manifest(), _inventory(), policy)
    destination = tmp_path / "plan.json"

    assert write_publication_plan(destination, plan) == destination
    assert destination.read_text(encoding="utf-8") == render_publication_plan(plan) + "\n"
    assert json.loads(destination.read_text(encoding="utf-8"))["candidate_snapshot_id"] == plan.candidate_snapshot_id
    assert not list(tmp_path.glob(".plan.json.*.tmp"))

    linked = tmp_path / "linked-plan.json"
    linked.symlink_to(destination)
    with pytest.raises(PublicationPlanError, match="ordinary file path"):
        write_publication_plan(linked, plan)


def test_plan_loader_sanitizes_encoding_failure_and_rejects_duplicate_capabilities(tmp_path: Path) -> None:
    path = tmp_path / "plan.json"
    path.write_bytes(b"\xff")
    with pytest.raises(PublicationPlanError, match="Cannot read a valid publication plan"):
        load_publication_plan(path)

    plan = build_publication_plan(
        _manifest(),
        _inventory(),
        _policy(
            PolicyRule(
                table="functional_annotation_agg",
                disposition=Disposition.PRESERVE,
                rationale="No candidate replacement.",
            )
        ),
    )
    plan.destination_metadata_capabilities.append(plan.destination_metadata_capabilities[0])
    path.write_text(plan.model_dump_json(), encoding="utf-8")
    with pytest.raises(PublicationPlanError, match="duplicate metadata capabilities"):
        load_publication_plan(path)
