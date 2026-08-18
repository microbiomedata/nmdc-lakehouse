"""Plan approved metadata operations without importing a destination client."""

from __future__ import annotations

import json
import re
import tempfile
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from nmdc_lakehouse.metadata_bundle import DescriptionRecord, MetadataBundle, load_metadata_bundle
from nmdc_lakehouse.publication_plan import (
    DestinationInventory,
    MetadataCapability,
    load_destination_inventory,
)

PLAN_FORMAT_VERSION: Literal[1] = 1
_SAFE_NAMESPACE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_]*(?:\.[A-Za-z0-9][A-Za-z0-9_]*)*\Z")
_SAFE_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_]*\Z")
_SAFE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
_SAFE_PROPERTY = re.compile(r"[A-Za-z][A-Za-z0-9._-]{0,127}\Z")


class MetadataApplicationError(ValueError):
    """Raised when approved metadata cannot produce a safe operation plan."""


class MetadataOperationKind(StrEnum):
    """Provider-neutral metadata changes understood by destination adapters."""

    NAMESPACE_TITLE = "namespace-title"
    NAMESPACE_DESCRIPTION = "namespace-description"
    NAMESPACE_DOCUMENTATION = "namespace-documentation"
    NAMESPACE_PROPERTY = "namespace-property"
    TABLE_DESCRIPTION = "table-description"
    COLUMN_DESCRIPTION = "column-description"


class MetadataOperation(BaseModel):
    """One approved semantic metadata change or unsupported change."""

    model_config = ConfigDict(extra="forbid", strict=True)

    kind: MetadataOperationKind
    namespace: str
    table: str | None = None
    column: str | None = None
    property: str | None = None
    value: str = Field(min_length=1, max_length=10_000)
    origin: Literal["profile", "footer"]
    rationale: str | None = None
    source: str | None = None

    @field_validator("namespace")
    @classmethod
    def validate_namespace(cls, value: str) -> str:
        """Require a qualified staging identifier safe for later adapters."""
        if not _SAFE_NAMESPACE.fullmatch(value):
            raise ValueError("Operation namespaces must be safe qualified identifiers.")
        return value

    @field_validator("table", "column")
    @classmethod
    def validate_target_name(cls, value: str | None) -> str | None:
        """Reject ambiguous or executable-looking table and column targets."""
        if value is not None and not _SAFE_NAME.fullmatch(value):
            raise ValueError("Operation table and column names must be safe identifiers.")
        return value

    @field_validator("property")
    @classmethod
    def validate_property(cls, value: str | None) -> str | None:
        """Require portable property names when an operation carries one."""
        if value is not None and not _SAFE_PROPERTY.fullmatch(value):
            raise ValueError("Operation property names must be safe identifiers.")
        return value

    @field_validator("value")
    @classmethod
    def validate_value(cls, value: str) -> str:
        """Reject blank approved content while preserving it as inert data."""
        if not value.strip():
            raise ValueError("Operation values must be nonblank.")
        return value

    @model_validator(mode="after")
    def validate_shape(self) -> MetadataOperation:
        """Keep each operation kind's target fields unambiguous."""
        namespace_kinds = {
            MetadataOperationKind.NAMESPACE_TITLE,
            MetadataOperationKind.NAMESPACE_DESCRIPTION,
            MetadataOperationKind.NAMESPACE_DOCUMENTATION,
        }
        if self.kind in namespace_kinds and any((self.table, self.column, self.property)):
            raise ValueError("Namespace metadata operations cannot name table, column, or property targets.")
        if self.kind == MetadataOperationKind.NAMESPACE_PROPERTY and (
            self.property is None or self.table is not None or self.column is not None
        ):
            raise ValueError("Namespace property operations require only a property target.")
        if self.kind == MetadataOperationKind.TABLE_DESCRIPTION and (
            self.table is None or self.column is not None or self.property is not None
        ):
            raise ValueError("Table description operations require only a table target.")
        if self.kind == MetadataOperationKind.COLUMN_DESCRIPTION and (
            self.table is None or self.column is None or self.property is not None
        ):
            raise ValueError("Column description operations require table and column targets.")
        if (self.rationale is None) != (self.source is None):
            raise ValueError("Description rationale and source must be supplied together.")
        if self.origin == "footer" and (self.rationale is not None or self.source is not None):
            raise ValueError("Footer descriptions cannot carry profile override evidence.")
        return self


class MissingDescription(BaseModel):
    """A table or column whose approved bundle has no description."""

    model_config = ConfigDict(extra="forbid", strict=True)

    table: str
    column: str | None = None

    @field_validator("table", "column")
    @classmethod
    def validate_name(cls, value: str | None) -> str | None:
        """Require safe table and optional column identifiers."""
        if value is not None and not _SAFE_NAME.fullmatch(value):
            raise ValueError("Missing-description targets must be safe identifiers.")
        return value


class MetadataApplicationPlan(BaseModel):
    """Versioned review artifact consumed later by a provider adapter."""

    model_config = ConfigDict(extra="forbid", strict=True)

    plan_format_version: Literal[1]
    snapshot_id: str
    profile_id: str
    bundle_generated_at: str
    source_namespace: str
    destination_id: str
    destination_observed_at: str
    destination_provider: str | None
    destination_table_format: str | None
    destination_metadata_capabilities: list[MetadataCapability]
    staging_namespace: str
    tables: list[str]
    supported_operations: list[MetadataOperation]
    unsupported_operations: list[MetadataOperation]
    missing_descriptions: list[MissingDescription]

    @field_validator("bundle_generated_at", "destination_observed_at")
    @classmethod
    def validate_timestamp(cls, value: str) -> str:
        """Require copied evidence timestamps to remain timezone-aware ISO 8601."""
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError as error:
            raise ValueError("Metadata application timestamps must use ISO 8601.") from error
        if parsed.tzinfo is None:
            raise ValueError("Metadata application timestamps must include a timezone.")
        return value

    @field_validator("snapshot_id", "profile_id", "source_namespace", "destination_id")
    @classmethod
    def validate_identity(cls, value: str) -> str:
        """Keep copied logical identities safe for review and logs."""
        if value.startswith("sha256:"):
            if not re.fullmatch(r"sha256:[0-9a-f]{64}", value):
                raise ValueError("Snapshot identities must be lowercase SHA-256 values.")
        elif not _SAFE_ID.fullmatch(value):
            raise ValueError("Metadata application identities must be sanitized logical labels.")
        return value

    @field_validator("destination_provider", "destination_table_format")
    @classmethod
    def validate_optional_identity(cls, value: str | None) -> str | None:
        """Keep optional provider observations credential-free and sanitized."""
        if value is not None and not _SAFE_ID.fullmatch(value):
            raise ValueError("Destination provider identities must be sanitized logical labels.")
        return value

    @field_validator("staging_namespace")
    @classmethod
    def validate_staging_namespace(cls, value: str) -> str:
        """Require an explicit qualified identifier, never a destination default."""
        if not _SAFE_NAMESPACE.fullmatch(value):
            raise ValueError("Staging namespace must be a safe qualified identifier.")
        return value

    @field_validator("tables")
    @classmethod
    def validate_tables(cls, value: list[str]) -> list[str]:
        """Require a duplicate-free, deterministically ordered table inventory."""
        if any(not _SAFE_NAME.fullmatch(name) for name in value):
            raise ValueError("Metadata application tables must be safe identifiers.")
        if value != sorted(value) or len(value) != len(set(value)):
            raise ValueError("Metadata application tables must be unique and sorted.")
        return value

    @model_validator(mode="after")
    def validate_coverage(self) -> MetadataApplicationPlan:
        """Bind operation classification and target coverage to declared evidence."""
        capabilities = self.destination_metadata_capabilities
        if len(capabilities) != len(set(capabilities)):
            raise ValueError("Destination metadata capabilities must be unique.")
        operations = [*self.supported_operations, *self.unsupported_operations]
        operation_keys = [(item.kind, item.namespace, item.table, item.column, item.property) for item in operations]
        if len(operation_keys) != len(set(operation_keys)):
            raise ValueError("Metadata application operations must be unique.")
        missing_keys = [(item.table, item.column) for item in self.missing_descriptions]
        if len(missing_keys) != len(set(missing_keys)):
            raise ValueError("Missing-description targets must be unique.")
        table_set = set(self.tables)
        referenced = {item.table for item in operations if item.table is not None}
        referenced.update(item.table for item in self.missing_descriptions)
        if referenced != table_set:
            raise ValueError("Metadata application targets must cover the exact bundle table set.")
        if any(item.namespace != self.staging_namespace for item in operations):
            raise ValueError("Metadata operation namespaces must match the plan staging namespace.")
        for supported, items in (
            (True, self.supported_operations),
            (False, self.unsupported_operations),
        ):
            for item in items:
                capability = _required_capability(item.kind)
                if (capability in capabilities) != supported:
                    raise ValueError("Metadata operations do not match declared destination capabilities.")
        return self


def _required_capability(kind: MetadataOperationKind) -> MetadataCapability:
    if kind in {MetadataOperationKind.TABLE_DESCRIPTION}:
        return MetadataCapability.TABLE
    if kind in {MetadataOperationKind.COLUMN_DESCRIPTION}:
        return MetadataCapability.COLUMN
    return MetadataCapability.NAMESPACE


def _operation(
    kind: MetadataOperationKind,
    namespace: str,
    value: str,
    *,
    description: DescriptionRecord | None = None,
    table: str | None = None,
    column: str | None = None,
    property_name: str | None = None,
) -> MetadataOperation:
    origin: Literal["profile", "footer"] = "profile"
    if description is not None:
        if description.origin == "none":
            raise MetadataApplicationError("Cannot plan an operation without approved metadata content.")
        origin = description.origin
    return MetadataOperation(
        kind=kind,
        namespace=namespace,
        table=table,
        column=column,
        property=property_name,
        value=value,
        origin=origin,
        rationale=description.rationale if description else None,
        source=description.source if description else None,
    )


def build_metadata_application_plan(
    bundle: MetadataBundle,
    inventory: DestinationInventory,
    staging_namespace: str,
) -> MetadataApplicationPlan:
    """Map approved bundle content to declared destination capabilities."""
    if not _SAFE_NAMESPACE.fullmatch(staging_namespace):
        raise MetadataApplicationError("Staging namespace must be a safe qualified identifier.")
    table_names = [table.name for table in bundle.tables]
    if len(table_names) != len(set(table_names)):
        raise MetadataApplicationError("Metadata bundle contains duplicate table names.")
    for table in bundle.tables:
        column_names = [column.name for column in table.columns]
        if len(column_names) != len(set(column_names)):
            raise MetadataApplicationError(f"Metadata bundle table '{table.name}' contains duplicate columns.")
    if len(inventory.metadata_capabilities) != len(set(inventory.metadata_capabilities)):
        raise MetadataApplicationError("Destination inventory contains duplicate metadata capabilities.")

    capabilities = set(inventory.metadata_capabilities)
    supported: list[MetadataOperation] = []
    unsupported: list[MetadataOperation] = []
    missing: list[MissingDescription] = []

    def classify(operation: MetadataOperation, capability: MetadataCapability) -> None:
        (supported if capability in capabilities else unsupported).append(operation)

    namespace = bundle.namespace
    for kind, value in (
        (MetadataOperationKind.NAMESPACE_TITLE, namespace.title),
        (MetadataOperationKind.NAMESPACE_DESCRIPTION, namespace.description),
    ):
        classify(_operation(kind, staging_namespace, value), MetadataCapability.NAMESPACE)
    if namespace.documentation_url:
        classify(
            _operation(MetadataOperationKind.NAMESPACE_DOCUMENTATION, staging_namespace, namespace.documentation_url),
            MetadataCapability.NAMESPACE,
        )
    for key, value in sorted(namespace.properties.items()):
        classify(
            _operation(
                MetadataOperationKind.NAMESPACE_PROPERTY,
                staging_namespace,
                value,
                property_name=key,
            ),
            MetadataCapability.NAMESPACE,
        )

    for table in sorted(bundle.tables, key=lambda item: item.name):
        if table.description.value is None:
            missing.append(MissingDescription(table=table.name))
        else:
            classify(
                _operation(
                    MetadataOperationKind.TABLE_DESCRIPTION,
                    staging_namespace,
                    table.description.value,
                    description=table.description,
                    table=table.name,
                ),
                MetadataCapability.TABLE,
            )
        for column in sorted(table.columns, key=lambda item: item.name):
            if column.description.value is None:
                missing.append(MissingDescription(table=table.name, column=column.name))
            else:
                classify(
                    _operation(
                        MetadataOperationKind.COLUMN_DESCRIPTION,
                        staging_namespace,
                        column.description.value,
                        description=column.description,
                        table=table.name,
                        column=column.name,
                    ),
                    MetadataCapability.COLUMN,
                )

    return MetadataApplicationPlan(
        plan_format_version=PLAN_FORMAT_VERSION,
        snapshot_id=bundle.snapshot_id,
        profile_id=bundle.profile_id,
        bundle_generated_at=bundle.generated_at,
        source_namespace=bundle.namespace.name,
        destination_id=inventory.destination_id,
        destination_observed_at=inventory.observed_at,
        destination_provider=inventory.provider,
        destination_table_format=inventory.table_format,
        destination_metadata_capabilities=sorted(inventory.metadata_capabilities),
        staging_namespace=staging_namespace,
        tables=sorted(table_names),
        supported_operations=supported,
        unsupported_operations=unsupported,
        missing_descriptions=missing,
    )


def render_metadata_application_plan(plan: MetadataApplicationPlan) -> str:
    """Render stable reviewable JSON without destination commands or credentials."""
    return json.dumps(plan.model_dump(mode="json"), indent=2, sort_keys=True)


def load_metadata_application_plan(path: Path) -> MetadataApplicationPlan:
    """Load a strict reviewed operation plan without contacting a destination."""
    document = path.expanduser()
    if not document.is_file() or document.is_symlink():
        raise MetadataApplicationError("The metadata application plan must be an ordinary JSON file.")
    try:
        return MetadataApplicationPlan.model_validate_json(document.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValidationError) as error:
        raise MetadataApplicationError("Cannot read a valid metadata application plan.") from error


def plan_metadata_application(
    bundle_path: Path,
    inventory_path: Path,
    staging_namespace: str,
) -> MetadataApplicationPlan:
    """Load reviewed offline inputs and map their metadata capabilities."""
    bundle = load_metadata_bundle(bundle_path)
    inventory = load_destination_inventory(inventory_path)
    return build_metadata_application_plan(bundle, inventory, staging_namespace)


def metadata_application_json_schema() -> dict[str, Any]:
    """Return the versioned metadata application plan JSON Schema."""
    schema = MetadataApplicationPlan.model_json_schema()
    schema["x-format-version"] = PLAN_FORMAT_VERSION
    return schema


def write_metadata_application_plan(path: Path, plan: MetadataApplicationPlan) -> Path:
    """Atomically write a generated plan to an ordinary local path."""
    destination = path.expanduser()
    if destination.is_symlink():
        raise MetadataApplicationError("Metadata application plan output must be an ordinary file path.")
    destination = destination.resolve()
    if destination.exists() and (destination.is_symlink() or not destination.is_file()):
        raise MetadataApplicationError("Metadata application plan output must be an ordinary file path.")
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(prefix=f".{destination.name}.", suffix=".tmp", dir=destination.parent)
    temporary = Path(temporary_name)
    try:
        with open(fd, "w", encoding="utf-8", closefd=True) as stream:
            stream.write(render_metadata_application_plan(plan))
            stream.write("\n")
        temporary.replace(destination)
    except OSError as error:
        raise MetadataApplicationError("Cannot write the metadata application plan.") from error
    finally:
        temporary.unlink(missing_ok=True)
    return destination
