"""Tests for provider-neutral metadata application planning."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from nmdc_lakehouse.cli import cli
from nmdc_lakehouse.metadata_application import (
    MetadataApplicationError,
    MetadataOperationKind,
    build_metadata_application_plan,
    load_metadata_application_plan,
    metadata_application_json_schema,
    plan_metadata_application,
    render_metadata_application_plan,
    write_metadata_application_plan,
)
from nmdc_lakehouse.metadata_bundle import (
    ColumnMetadata,
    DescriptionRecord,
    MetadataBundle,
    NamespaceProfile,
    SchemaIdentity,
    TableMetadata,
)
from nmdc_lakehouse.publication_plan import DestinationInventory, MetadataCapability


def _description(value: str | None, *, origin: str = "footer") -> DescriptionRecord:
    if value is None:
        return DescriptionRecord(value=None, origin="none")
    if origin == "profile":
        return DescriptionRecord(value=value, origin="profile", rationale="Reviewed wording.", source="NMDC review.")
    return DescriptionRecord(value=value, origin="footer")


def _table(name: str, table_description: str | None, column_description: str | None) -> TableMetadata:
    return TableMetadata(
        name=name,
        source_class=name,
        target_schema_id="https://w3id.org/nmdc/nmdc-schema-flattened",
        target_class=name,
        mapping_id="nmdc_lakehouse.transforms.flatteners.SchemaDrivenFlattener",
        physical_schema_sha256="a" * 64,
        footer_schema_sha256="b" * 64,
        description=_description(table_description, origin="profile"),
        columns=[
            ColumnMetadata(
                name="id",
                arrow_type="string",
                nullable=False,
                linkml_range="string",
                identifier=True,
                designates_type=False,
                description=_description(column_description),
            )
        ],
    )


def _bundle(*tables: TableMetadata) -> MetadataBundle:
    return MetadataBundle(
        bundle_format_version=1,
        generated_at="2026-08-18T18:00:00+00:00",
        snapshot_id="sha256:" + "1" * 64,
        profile_id="nmdc-metadata-reviewed",
        source_schemas=[SchemaIdentity(schema_id="https://w3id.org/nmdc/nmdc", version="11.23.0")],
        target_schema_ids=["https://w3id.org/nmdc/nmdc-schema-flattened"],
        mapping_ids=["nmdc_lakehouse.transforms.flatteners.SchemaDrivenFlattener"],
        namespace=NamespaceProfile(
            name="nmdc_metadata",
            title="NMDC metadata",
            description="Flattened NMDC metadata tables.",
            documentation_url="https://microbiomedata.org/",
            properties={"role": "metadata"},
        ),
        tables=list(tables),
    )


def _inventory(*capabilities: MetadataCapability) -> DestinationInventory:
    return DestinationInventory(
        inventory_format_version=1,
        destination_id="nmdc-production",
        observed_at="2026-08-18T17:00:00+00:00",
        provider="spark_catalog",
        table_format="delta",
        metadata_capabilities=list(capabilities),
        tables=[],
    )


def test_plan_separates_supported_and_unsupported_operations() -> None:
    bundle = _bundle(_table("biosample_set", "Reviewed table's purpose.", "Stable identifier."))

    plan = build_metadata_application_plan(
        bundle,
        _inventory(MetadataCapability.NAMESPACE, MetadataCapability.TABLE),
        "spark_catalog.nmdc_metadata_staging",
    )

    supported_kinds = [operation.kind for operation in plan.supported_operations]
    assert supported_kinds == [
        MetadataOperationKind.NAMESPACE_TITLE,
        MetadataOperationKind.NAMESPACE_DESCRIPTION,
        MetadataOperationKind.NAMESPACE_DOCUMENTATION,
        MetadataOperationKind.NAMESPACE_PROPERTY,
        MetadataOperationKind.TABLE_DESCRIPTION,
    ]
    assert [operation.kind for operation in plan.unsupported_operations] == [MetadataOperationKind.COLUMN_DESCRIPTION]
    table = plan.supported_operations[-1]
    assert table.value == "Reviewed table's purpose."
    assert table.rationale == "Reviewed wording."
    assert table.source == "NMDC review."
    assert plan.tables == ["biosample_set"]
    assert plan.source_namespace == "nmdc_metadata"
    assert plan.bundle_generated_at == bundle.generated_at
    assert plan.destination_metadata_capabilities == [MetadataCapability.NAMESPACE, MetadataCapability.TABLE]
    assert "COMMENT ON" not in render_metadata_application_plan(plan)


def test_missing_descriptions_are_explicit_and_not_operations() -> None:
    plan = build_metadata_application_plan(
        _bundle(_table("biosample_set", None, None)),
        _inventory(MetadataCapability.NAMESPACE, MetadataCapability.TABLE, MetadataCapability.COLUMN),
        "nmdc_metadata_staging",
    )

    assert [item.model_dump() for item in plan.missing_descriptions] == [
        {"table": "biosample_set", "column": None},
        {"table": "biosample_set", "column": "id"},
    ]
    assert all(operation.table is None for operation in plan.supported_operations)


def test_rendering_is_deterministic_by_table_name() -> None:
    first = build_metadata_application_plan(
        _bundle(_table("study_set", "Study.", "Identifier."), _table("biosample_set", "Sample.", "ID.")),
        _inventory(MetadataCapability.TABLE, MetadataCapability.COLUMN),
        "nmdc_metadata_staging",
    )
    second = build_metadata_application_plan(
        _bundle(_table("biosample_set", "Sample.", "ID."), _table("study_set", "Study.", "Identifier.")),
        _inventory(MetadataCapability.TABLE, MetadataCapability.COLUMN),
        "nmdc_metadata_staging",
    )

    assert json.loads(render_metadata_application_plan(first)) == json.loads(render_metadata_application_plan(second))


def test_escaping_sensitive_content_remains_inert_data() -> None:
    value = "Purpose includes apostrophe'; DROP TABLE biosample_set; --"
    plan = build_metadata_application_plan(
        _bundle(_table("biosample_set", value, "Identifier.")),
        _inventory(MetadataCapability.TABLE),
        "nmdc_metadata_staging",
    )

    operation = next(item for item in plan.supported_operations if item.kind == MetadataOperationKind.TABLE_DESCRIPTION)
    assert operation.value == value
    assert json.loads(render_metadata_application_plan(plan))["supported_operations"][-1]["value"] == value


def test_duplicate_inputs_are_rejected() -> None:
    table = _table("biosample_set", "Sample.", "Identifier.")
    duplicate_bundle = _bundle(table, table)
    duplicate_capabilities = _inventory(MetadataCapability.TABLE, MetadataCapability.TABLE)

    with pytest.raises(MetadataApplicationError, match="duplicate table"):
        build_metadata_application_plan(duplicate_bundle, _inventory(), "nmdc_metadata_staging")
    with pytest.raises(MetadataApplicationError, match="duplicate metadata capabilities"):
        build_metadata_application_plan(_bundle(table), duplicate_capabilities, "nmdc_metadata_staging")


def test_copied_inventory_evidence_validation_is_sanitized() -> None:
    inventory = _inventory()
    inventory.observed_at = "not-a-timestamp"

    with pytest.raises(MetadataApplicationError, match="Cannot build a valid metadata application plan"):
        build_metadata_application_plan(_bundle(), inventory, "nmdc_metadata_staging")


def test_plan_loader_rejects_incomplete_table_coverage(tmp_path: Path) -> None:
    plan = build_metadata_application_plan(
        _bundle(_table("biosample_set", "Sample.", "Identifier.")),
        _inventory(MetadataCapability.TABLE, MetadataCapability.COLUMN),
        "nmdc_metadata_staging",
    )
    document = plan.model_dump(mode="json")
    document["tables"] = []
    path = tmp_path / "plan.json"
    path.write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(MetadataApplicationError, match="valid metadata application plan"):
        load_metadata_application_plan(path)


def test_schema_and_atomic_output_are_versioned(tmp_path: Path) -> None:
    plan = build_metadata_application_plan(_bundle(), _inventory(), "nmdc_metadata_staging")
    output = tmp_path / "output" / "metadata-application-plan.json"

    assert metadata_application_json_schema()["x-format-version"] == 1
    assert write_metadata_application_plan(output, plan) == output.resolve()
    assert output.read_text(encoding="utf-8") == render_metadata_application_plan(plan) + "\n"
    assert load_metadata_application_plan(output) == plan

    linked = tmp_path / "linked.json"
    linked.symlink_to(output)
    with pytest.raises(MetadataApplicationError, match="ordinary file path"):
        write_metadata_application_plan(linked, plan)


def test_offline_command_prints_and_writes_the_same_plan(tmp_path: Path) -> None:
    bundle = _bundle(_table("biosample_set", "Sample.", "Identifier."))
    inventory = _inventory(MetadataCapability.TABLE, MetadataCapability.COLUMN)
    bundle_path = tmp_path / "bundle.json"
    inventory_path = tmp_path / "inventory.json"
    output = tmp_path / "plan.json"
    bundle_path.write_text(bundle.model_dump_json(), encoding="utf-8")
    inventory_path.write_text(inventory.model_dump_json(), encoding="utf-8")

    expected = plan_metadata_application(bundle_path, inventory_path, "nmdc_metadata_staging")
    result = CliRunner().invoke(
        cli,
        [
            "metadata-application-plan",
            str(bundle_path),
            "--inventory",
            str(inventory_path),
            "--staging-namespace",
            "nmdc_metadata_staging",
            "--output",
            str(output),
        ],
    )

    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == expected.model_dump(mode="json")
    assert output.read_text(encoding="utf-8") == result.output


def test_command_sanitizes_invalid_input(tmp_path: Path) -> None:
    bundle_path = tmp_path / "bundle.json"
    bundle_path.write_bytes(b"\xff\xfe")
    inventory_path = tmp_path / "inventory.json"
    inventory_path.write_text(_inventory().model_dump_json(), encoding="utf-8")

    result = CliRunner().invoke(
        cli,
        [
            "metadata-application-plan",
            str(bundle_path),
            "--inventory",
            str(inventory_path),
            "--staging-namespace",
            "nmdc_metadata_staging",
        ],
    )

    assert result.exit_code != 0
    assert "Cannot read a valid metadata bundle" in result.output
    assert "UnicodeDecodeError" not in result.output


def test_command_sanitizes_output_directory_failure(tmp_path: Path) -> None:
    bundle_path = tmp_path / "bundle.json"
    inventory_path = tmp_path / "inventory.json"
    bundle_path.write_text(_bundle().model_dump_json(), encoding="utf-8")
    inventory_path.write_text(_inventory().model_dump_json(), encoding="utf-8")
    blocked_parent = tmp_path / "not-a-directory"
    blocked_parent.write_text("ordinary file", encoding="utf-8")

    result = CliRunner().invoke(
        cli,
        [
            "metadata-application-plan",
            str(bundle_path),
            "--inventory",
            str(inventory_path),
            "--staging-namespace",
            "nmdc_metadata_staging",
            "--output",
            str(blocked_parent / "plan.json"),
        ],
    )

    assert result.exit_code != 0
    assert "Cannot write the metadata application plan" in result.output
    assert "FileExistsError" not in result.output


def test_schema_command_emits_plan_contract() -> None:
    result = CliRunner().invoke(cli, ["metadata-application-plan-schema"])

    assert result.exit_code == 0, result.output
    assert json.loads(result.output)["x-format-version"] == 1


@pytest.mark.parametrize("namespace", ["nmdc metadata", "nmdc_metadata;DROP", "nmdc_metadata\n"])
def test_unsafe_staging_namespace_is_rejected(namespace: str) -> None:
    with pytest.raises(MetadataApplicationError, match="safe qualified identifier"):
        build_metadata_application_plan(_bundle(), _inventory(), namespace)
