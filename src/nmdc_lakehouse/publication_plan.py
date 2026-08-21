"""Build destination-neutral publication disposition plans offline."""

from __future__ import annotations

import json
import re
import tempfile
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal, TypeVar

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from nmdc_lakehouse.snapshot_manifest import SnapshotManifest, validate_snapshot

# Raised from 1 when DestinationTable gained observed_table_format. Every model here forbids
# extra fields, so any added key is a format change whether or not it has a default.
INVENTORY_FORMAT_VERSION: Literal[2] = 2
POLICY_FORMAT_VERSION: Literal[1] = 1
PLAN_FORMAT_VERSION: Literal[1] = 1
_SAFE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
_SAFE_TABLE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_]*\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


class PublicationPlanError(ValueError):
    """Raised when publication evidence cannot produce a safe complete plan."""


class Disposition(StrEnum):
    """Allowed treatment of one candidate or destination table."""

    REPLACE = "replace"
    ADD = "add"
    PRESERVE = "preserve"
    REBUILD = "rebuild"
    RETIRE = "retire"


class MetadataCapability(StrEnum):
    """Metadata levels a destination reports that it can preserve."""

    TENANT = "tenant"
    DATASET = "dataset"
    NAMESPACE = "namespace"
    TABLE = "table"
    COLUMN = "column"
    SNAPSHOT = "snapshot"
    FILE = "file"


class DestinationTable(BaseModel):
    """Observed non-secret evidence for one destination table."""

    model_config = ConfigDict(extra="forbid", strict=True)

    name: str
    rows: int = Field(ge=0)
    physical_schema_sha256: str
    # The format observed on this table, as opposed to the reviewed namespace label above.
    # The producer already reads it per table and fails closed on a mismatch; recording it turns
    # "every table was checked" from a claim about how the command works into evidence in the
    # artifact. Absent from a version 1 inventory, hence the default.
    observed_table_format: str | None = None

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        """Reject table names that cannot map to portable local artifacts."""
        if not _SAFE_TABLE.fullmatch(value):
            raise ValueError("Table names must be safe local identifiers.")
        return value

    @field_validator("observed_table_format")
    @classmethod
    def validate_observed_format(cls, value: str | None) -> str | None:
        """Hold the observed format to the same contract as the reviewed labels above it.

        This is read from a file and copied into plans and log output, so it gets the same
        sanitization as `provider` and `table_format` rather than being trusted because the
        producer happened to validate it before writing.
        """
        if value is not None and not _SAFE_ID.fullmatch(value):
            raise ValueError("An observed table format must be a sanitized logical label.")
        return value

    @field_validator("physical_schema_sha256")
    @classmethod
    def validate_schema_hash(cls, value: str) -> str:
        """Require the same lowercase SHA-256 representation as the manifest."""
        if not _SHA256.fullmatch(value):
            raise ValueError("Physical schema fingerprints must be lowercase SHA-256 values.")
        return value


class DestinationInventory(BaseModel):
    """Versioned, credential-free observation of one destination."""

    model_config = ConfigDict(extra="forbid", strict=True)

    # Both accepted on read. Version 2 added observed_table_format to each table, which is
    # optional, so a version 1 document is still valid here. Reading is widened rather than the
    # version being left alone because these models forbid extra fields: a reader pinned to
    # version 1 rejects a version 2 document outright, and a default does nothing to prevent that.
    inventory_format_version: Literal[1, 2]
    destination_id: str
    observed_at: str = Field(min_length=1)
    provider: str | None = None
    table_format: str | None = None
    metadata_capabilities: list[MetadataCapability] = Field(default_factory=list)
    tables: list[DestinationTable]

    @field_validator("destination_id", "provider", "table_format")
    @classmethod
    def validate_safe_identity(cls, value: str | None) -> str | None:
        """Keep inventory identities logical and safe to copy into logs and plans."""
        if value is not None and not _SAFE_ID.fullmatch(value):
            raise ValueError("Destination identities must be sanitized logical labels.")
        return value


class PolicyRule(BaseModel):
    """Reviewed exception or live-only disposition for one table."""

    model_config = ConfigDict(extra="forbid", strict=True)

    table: str
    disposition: Disposition
    rationale: str = Field(min_length=1, max_length=1000)

    @field_validator("table")
    @classmethod
    def validate_table(cls, value: str) -> str:
        """Reject ambiguous or path-like policy table names."""
        if not _SAFE_TABLE.fullmatch(value):
            raise ValueError("Policy table names must be safe local identifiers.")
        return value


class PublicationPolicy(BaseModel):
    """Versioned reviewed exceptions to deterministic planning defaults."""

    model_config = ConfigDict(extra="forbid", strict=True)

    policy_format_version: Literal[1]
    rules: list[PolicyRule] = Field(default_factory=list)


class PlanEntry(BaseModel):
    """Complete disposition and comparison evidence for one union table."""

    model_config = ConfigDict(extra="forbid", strict=True)

    table: str
    disposition: Disposition
    rationale: str
    decision_source: Literal["generated", "policy"]
    candidate_path: str | None
    candidate_rows: int | None
    candidate_physical_schema_sha256: str | None
    candidate_target_schema_id: str | None
    candidate_mapping_id: str | None
    destination_rows: int | None
    destination_physical_schema_sha256: str | None


class PublicationPlan(BaseModel):
    """Versioned offline checkpoint before provider-specific publication."""

    model_config = ConfigDict(extra="forbid", strict=True)

    plan_format_version: Literal[1]
    candidate_snapshot_id: str
    destination_id: str
    destination_observed_at: str
    destination_provider: str | None
    destination_table_format: str | None
    destination_metadata_capabilities: list[MetadataCapability]
    tables: list[PlanEntry]


ModelType = TypeVar("ModelType", bound=BaseModel)


def _load_document(path: Path, model: type[ModelType], label: str) -> ModelType:
    path = path.expanduser()
    if not path.is_file() or path.is_symlink():
        raise PublicationPlanError(f"The {label} must be an ordinary JSON file.")
    try:
        return model.model_validate_json(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValidationError) as error:
        raise PublicationPlanError(f"Cannot read a valid {label}.") from error


def load_destination_inventory(path: Path) -> DestinationInventory:
    """Load a versioned destination inventory without contacting its provider."""
    inventory = _load_document(path, DestinationInventory, "destination inventory")
    table_names = [table.name for table in inventory.tables]
    if len(table_names) != len(set(table_names)):
        raise PublicationPlanError("Destination inventory contains duplicate table names.")
    if len(inventory.metadata_capabilities) != len(set(inventory.metadata_capabilities)):
        raise PublicationPlanError("Destination inventory contains duplicate metadata capabilities.")
    return inventory


def load_publication_policy(path: Path) -> PublicationPolicy:
    """Load a versioned reviewed publication policy."""
    policy = _load_document(path, PublicationPolicy, "publication policy")
    table_names = [rule.table for rule in policy.rules]
    if len(table_names) != len(set(table_names)):
        raise PublicationPlanError("Publication policy contains duplicate table rules.")
    return policy


def load_publication_plan(path: Path) -> PublicationPlan:
    """Load a versioned approved publication plan without contacting its destination."""
    plan = _load_document(path, PublicationPlan, "publication plan")
    table_names = [entry.table for entry in plan.tables]
    if len(table_names) != len(set(table_names)):
        raise PublicationPlanError("Publication plan contains duplicate table names.")
    capabilities = plan.destination_metadata_capabilities
    if len(capabilities) != len(set(capabilities)):
        raise PublicationPlanError("Publication plan contains duplicate metadata capabilities.")
    return plan


def disposition_is_compatible(disposition: Disposition, *, candidate: bool, destination: bool) -> bool:
    """Return whether a disposition is safe for the observed table presence."""
    return {
        Disposition.ADD: candidate and not destination,
        Disposition.REPLACE: candidate and destination,
        Disposition.PRESERVE: destination,
        Disposition.REBUILD: destination,
        Disposition.RETIRE: destination and not candidate,
    }[disposition]


def _validate_disposition(disposition: Disposition, *, candidate: bool, destination: bool, table: str) -> None:
    if not disposition_is_compatible(disposition, candidate=candidate, destination=destination):
        raise PublicationPlanError(
            f"Disposition '{disposition}' is unsafe for table '{table}' with its candidate/destination presence."
        )


def build_publication_plan(
    manifest: SnapshotManifest,
    inventory: DestinationInventory,
    policy: PublicationPolicy,
) -> PublicationPlan:
    """Join validated candidate and destination evidence into one total plan."""
    named_inputs = (
        ("Candidate manifest", [artifact.table for artifact in manifest.artifacts]),
        ("Destination inventory", [table.name for table in inventory.tables]),
        ("Publication policy", [rule.table for rule in policy.rules]),
    )
    for label, names in named_inputs:
        if len(names) != len(set(names)):
            raise PublicationPlanError(f"{label} contains duplicate table names.")

    candidate = {artifact.table: artifact for artifact in manifest.artifacts}
    destination = {table.name: table for table in inventory.tables}
    union = set(candidate).union(destination)
    rules = {rule.table: rule for rule in policy.rules}
    unknown_rules = set(rules).difference(union)
    if unknown_rules:
        raise PublicationPlanError(f"Publication policy names unknown table(s): {', '.join(sorted(unknown_rules))}.")
    missing_live_only = set(destination).difference(candidate, rules)
    if missing_live_only:
        raise PublicationPlanError(
            "Live-only tables require an explicit reviewed disposition: " + ", ".join(sorted(missing_live_only)) + "."
        )

    entries: list[PlanEntry] = []
    for table in sorted(union):
        candidate_artifact = candidate.get(table)
        destination_table = destination.get(table)
        rule = rules.get(table)
        if rule is not None:
            disposition = rule.disposition
            rationale = rule.rationale
            source: Literal["generated", "policy"] = "policy"
        elif candidate_artifact is not None and destination_table is not None:
            disposition = Disposition.REPLACE
            rationale = "Candidate and destination both contain the table."
            source = "generated"
        else:
            disposition = Disposition.ADD
            rationale = "Candidate contains a table that is absent from the destination."
            source = "generated"
        _validate_disposition(
            disposition,
            candidate=candidate_artifact is not None,
            destination=destination_table is not None,
            table=table,
        )
        entries.append(
            PlanEntry(
                table=table,
                disposition=disposition,
                rationale=rationale,
                decision_source=source,
                candidate_path=candidate_artifact.path if candidate_artifact else None,
                candidate_rows=candidate_artifact.rows if candidate_artifact else None,
                candidate_physical_schema_sha256=(
                    candidate_artifact.physical_schema_sha256 if candidate_artifact else None
                ),
                candidate_target_schema_id=candidate_artifact.target_schema_id if candidate_artifact else None,
                candidate_mapping_id=candidate_artifact.mapping if candidate_artifact else None,
                destination_rows=destination_table.rows if destination_table else None,
                destination_physical_schema_sha256=(
                    destination_table.physical_schema_sha256 if destination_table else None
                ),
            )
        )
    return PublicationPlan(
        plan_format_version=PLAN_FORMAT_VERSION,
        candidate_snapshot_id=manifest.snapshot_id,
        destination_id=inventory.destination_id,
        destination_observed_at=inventory.observed_at,
        destination_provider=inventory.provider,
        destination_table_format=inventory.table_format,
        destination_metadata_capabilities=inventory.metadata_capabilities,
        tables=entries,
    )


def plan_snapshot_publication(
    snapshot_root: Path,
    inventory_path: Path,
    policy_path: Path,
) -> PublicationPlan:
    """Validate all offline inputs and generate a destination-neutral plan."""
    manifest = validate_snapshot(snapshot_root)
    inventory = load_destination_inventory(inventory_path)
    policy = load_publication_policy(policy_path)
    return build_publication_plan(manifest, inventory, policy)


def publication_json_schema(document: Literal["inventory", "policy", "plan"]) -> dict[str, Any]:
    """Return the selected publication document's versioned JSON Schema."""
    models: dict[str, tuple[type[BaseModel], int]] = {
        "inventory": (DestinationInventory, INVENTORY_FORMAT_VERSION),
        "policy": (PublicationPolicy, POLICY_FORMAT_VERSION),
        "plan": (PublicationPlan, PLAN_FORMAT_VERSION),
    }
    model, format_version = models[document]
    schema = model.model_json_schema()
    schema["x-format-version"] = format_version
    return schema


def write_publication_plan(path: Path, plan: PublicationPlan) -> Path:
    """Atomically write a generated plan to an ordinary local path."""
    destination = path.expanduser()
    if destination.is_symlink():
        raise PublicationPlanError("Publication plan output must be an ordinary file path.")
    destination = destination.resolve()
    if destination.exists() and (destination.is_symlink() or not destination.is_file()):
        raise PublicationPlanError("Publication plan output must be an ordinary file path.")
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(prefix=f".{destination.name}.", suffix=".tmp", dir=destination.parent)
    temporary = Path(temporary_name)
    try:
        with open(fd, "w", encoding="utf-8", closefd=True) as stream:
            stream.write(render_publication_plan(plan))
            stream.write("\n")
        temporary.replace(destination)
    except OSError as error:
        raise PublicationPlanError("Cannot write the publication plan.") from error
    finally:
        temporary.unlink(missing_ok=True)
    return destination


def render_publication_plan(plan: PublicationPlan) -> str:
    """Render stable, reviewable JSON for stdout or a file."""
    return json.dumps(plan.model_dump(mode="json"), indent=2, sort_keys=True)
