"""Generate reviewable metadata bundles from validated Parquet snapshots."""

from __future__ import annotations

import json
import re
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, TypeVar

import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from nmdc_lakehouse.snapshot_manifest import ArtifactRecord, SnapshotManifest, validate_snapshot

PROFILE_FORMAT_VERSION: Literal[1] = 1
BUNDLE_FORMAT_VERSION: Literal[1] = 1
_PREFIX = b"nmdc_lakehouse."
_SAFE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
_SAFE_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_]*\Z")
_SAFE_PROPERTY = re.compile(r"[A-Za-z][A-Za-z0-9._-]{0,127}\Z")
_SENSITIVE_PROPERTY = re.compile(r"(?:password|secret|token|credential|access[_.-]?key)", re.IGNORECASE)
_SNAPSHOT_ID = re.compile(r"sha256:[0-9a-f]{64}\Z")


class MetadataBundleError(ValueError):
    """Raised when metadata inputs cannot produce a safe complete bundle."""


class NamespaceProfile(BaseModel):
    """Reviewed provider-neutral metadata for one logical namespace."""

    model_config = ConfigDict(extra="forbid", strict=True)

    name: str
    title: str = Field(min_length=1, max_length=500)
    description: str = Field(min_length=1, max_length=10_000)
    documentation_url: str | None = Field(default=None, min_length=1, max_length=2_000)
    properties: dict[str, str] = Field(default_factory=dict)

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        """Require a portable logical namespace identifier."""
        if not _SAFE_ID.fullmatch(value):
            raise ValueError("Namespace names must be sanitized logical identifiers.")
        return value

    @field_validator("title", "description")
    @classmethod
    def validate_text(cls, value: str) -> str:
        """Reject whitespace-only reviewed namespace content."""
        if not value.strip():
            raise ValueError("Namespace title and description must be nonblank.")
        return value

    @field_validator("documentation_url")
    @classmethod
    def validate_documentation_url(cls, value: str | None) -> str | None:
        """Require an explicit HTTPS documentation location when supplied."""
        if value is not None and (not value.strip() or not value.startswith("https://")):
            raise ValueError("Documentation URLs must be nonblank and use HTTPS.")
        return value

    @field_validator("properties")
    @classmethod
    def validate_properties(cls, value: dict[str, str]) -> dict[str, str]:
        """Reject unsafe, blank, or apparently credential-bearing properties."""
        for key, item in value.items():
            if not _SAFE_PROPERTY.fullmatch(key) or _SENSITIVE_PROPERTY.search(key):
                raise ValueError("Namespace property names must be safe and credential-free.")
            if not item.strip() or len(item) > 2_000:
                raise ValueError("Namespace property values must be nonblank and at most 2,000 characters.")
        return value


class DescriptionOverride(BaseModel):
    """Reviewed replacement for one generated table or column description."""

    model_config = ConfigDict(extra="forbid", strict=True)

    table: str
    column: str | None = None
    description: str = Field(min_length=1, max_length=10_000)
    rationale: str = Field(min_length=1, max_length=2_000)
    source: str = Field(min_length=1, max_length=2_000)

    @field_validator("table", "column")
    @classmethod
    def validate_name(cls, value: str | None) -> str | None:
        """Require table and column names that map unambiguously to the snapshot."""
        if value is not None and not _SAFE_NAME.fullmatch(value):
            raise ValueError("Override table and column names must be safe identifiers.")
        return value

    @field_validator("description", "rationale", "source")
    @classmethod
    def validate_text(cls, value: str) -> str:
        """Reject whitespace-only reviewed override content."""
        if not value.strip():
            raise ValueError("Override description, rationale, and source must be nonblank.")
        return value


class MetadataProfile(BaseModel):
    """Version-controlled namespace content and reviewed description overrides."""

    model_config = ConfigDict(extra="forbid", strict=True)

    profile_format_version: Literal[1]
    profile_id: str
    snapshot_id: str
    namespace: NamespaceProfile
    overrides: list[DescriptionOverride] = Field(default_factory=list)

    @field_validator("profile_id")
    @classmethod
    def validate_profile_id(cls, value: str) -> str:
        """Require a stable credential-free profile identity."""
        if not _SAFE_ID.fullmatch(value):
            raise ValueError("Profile identifiers must be sanitized logical identifiers.")
        return value

    @field_validator("snapshot_id")
    @classmethod
    def validate_snapshot_id(cls, value: str) -> str:
        """Bind reviewed content to one immutable candidate snapshot."""
        if not _SNAPSHOT_ID.fullmatch(value):
            raise ValueError("Profile snapshot identities must be lowercase SHA-256 values.")
        return value


class DescriptionRecord(BaseModel):
    """One final description and the evidence for its provenance."""

    model_config = ConfigDict(extra="forbid", strict=True)

    value: str | None
    origin: Literal["footer", "profile", "none"]
    rationale: str | None = None
    source: str | None = None

    @model_validator(mode="after")
    def validate_provenance(self) -> DescriptionRecord:
        """Keep the description value and provenance fields internally consistent."""
        if self.origin == "profile":
            if not self.value or not self.rationale or not self.source:
                raise ValueError("Profile descriptions require value, rationale, and source.")
        elif self.rationale is not None or self.source is not None:
            raise ValueError("Only profile descriptions may carry override evidence.")
        if self.origin == "none" and self.value is not None:
            raise ValueError("Descriptions with origin 'none' cannot carry a value.")
        if self.origin == "footer" and not self.value:
            raise ValueError("Footer descriptions require a value.")
        return self


class ColumnMetadata(BaseModel):
    """Portable metadata for one physical Parquet field."""

    model_config = ConfigDict(extra="forbid", strict=True)

    name: str
    arrow_type: str
    nullable: bool
    linkml_range: str | None
    identifier: bool
    designates_type: bool
    description: DescriptionRecord

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        """Require a portable unambiguous column name."""
        if not _SAFE_NAME.fullmatch(value):
            raise ValueError("Column names must be safe identifiers.")
        return value


class TableMetadata(BaseModel):
    """Portable metadata and schema lineage for one manifested table."""

    model_config = ConfigDict(extra="forbid", strict=True)

    name: str
    source_class: str
    target_schema_id: str
    target_class: str
    mapping_id: str
    physical_schema_sha256: str
    footer_schema_sha256: str
    description: DescriptionRecord
    columns: list[ColumnMetadata]

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        """Require a portable unambiguous table name."""
        if not _SAFE_NAME.fullmatch(value):
            raise ValueError("Table names must be safe identifiers.")
        return value


class SchemaIdentity(BaseModel):
    """One source schema identifier/version pair present in the snapshot."""

    model_config = ConfigDict(extra="forbid", strict=True)

    schema_id: str
    version: str


class MetadataBundle(BaseModel):
    """Versioned destination-neutral namespace, table, and column metadata."""

    model_config = ConfigDict(extra="forbid", strict=True)

    bundle_format_version: Literal[1]
    generated_at: str = Field(min_length=1)
    snapshot_id: str
    profile_id: str
    source_schemas: list[SchemaIdentity]
    target_schema_ids: list[str]
    mapping_ids: list[str]
    namespace: NamespaceProfile
    tables: list[TableMetadata]

    @field_validator("generated_at")
    @classmethod
    def validate_generated_at(cls, value: str) -> str:
        """Require a timezone-aware ISO 8601 generation timestamp."""
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError as error:
            raise ValueError("Bundle generation timestamps must use ISO 8601.") from error
        if parsed.tzinfo is None:
            raise ValueError("Bundle generation timestamps must include a timezone.")
        return value


ModelType = TypeVar("ModelType", bound=BaseModel)


def _load_document(path: Path, model: type[ModelType], label: str) -> ModelType:
    path = path.expanduser()
    if not path.is_file() or path.is_symlink():
        raise MetadataBundleError(f"The {label} must be an ordinary JSON file.")
    try:
        return model.model_validate_json(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValidationError) as error:
        raise MetadataBundleError(f"Cannot read a valid {label}.") from error


def _override_key(override: DescriptionOverride) -> tuple[str, str | None]:
    return override.table, override.column


def _validate_overrides(profile: MetadataProfile) -> dict[tuple[str, str | None], DescriptionOverride]:
    keys = [_override_key(override) for override in profile.overrides]
    if len(keys) != len(set(keys)):
        raise MetadataBundleError("Metadata profile contains duplicate description overrides.")
    return {_override_key(override): override for override in profile.overrides}


def load_metadata_profile(path: Path) -> MetadataProfile:
    """Load reviewed metadata content without contacting a destination."""
    profile = _load_document(path, MetadataProfile, "metadata profile")
    _validate_overrides(profile)
    return profile


def _metadata_value(metadata: dict[bytes, bytes] | None, key: str, *, table: str) -> str | None:
    value = (metadata or {}).get(_PREFIX + key.encode())
    if value is None:
        return None
    try:
        return value.decode()
    except UnicodeDecodeError as error:
        raise MetadataBundleError(f"Table '{table}' contains invalid portable metadata.") from error


def _metadata_bool(metadata: dict[bytes, bytes] | None, key: str, *, table: str) -> bool:
    value = _metadata_value(metadata, key, table=table)
    if value is None:
        return False
    if value != "true":
        raise MetadataBundleError(f"Table '{table}' contains invalid portable metadata.")
    return True


def _description(value: str | None, override: DescriptionOverride | None) -> DescriptionRecord:
    if override is not None:
        return DescriptionRecord(
            value=override.description,
            origin="profile",
            rationale=override.rationale,
            source=override.source,
        )
    if value:
        return DescriptionRecord(value=value, origin="footer")
    return DescriptionRecord(value=None, origin="none")


def _table_metadata(
    root: Path,
    artifact: ArtifactRecord,
    overrides: dict[tuple[str, str | None], DescriptionOverride],
) -> TableMetadata:
    try:
        schema = pq.ParquetFile(root / artifact.path).schema_arrow
    except (OSError, pa.ArrowException) as error:
        raise MetadataBundleError(f"Cannot read portable metadata for table '{artifact.table}'.") from error
    field_names = [field.name for field in schema]
    if len(field_names) != len(set(field_names)) or any(not _SAFE_NAME.fullmatch(name) for name in field_names):
        raise MetadataBundleError(f"Table '{artifact.table}' contains unsafe or duplicate column names.")
    columns = []
    for field in schema:
        columns.append(
            ColumnMetadata(
                name=field.name,
                arrow_type=str(field.type),
                nullable=field.nullable,
                linkml_range=_metadata_value(field.metadata, "linkml_range", table=artifact.table),
                identifier=_metadata_bool(field.metadata, "identifier", table=artifact.table),
                designates_type=_metadata_bool(field.metadata, "designates_type", table=artifact.table),
                description=_description(
                    _metadata_value(field.metadata, "description", table=artifact.table),
                    overrides.get((artifact.table, field.name)),
                ),
            )
        )
    return TableMetadata(
        name=artifact.table,
        source_class=artifact.source_class,
        target_schema_id=artifact.target_schema_id,
        target_class=artifact.target_class,
        mapping_id=artifact.mapping,
        physical_schema_sha256=artifact.physical_schema_sha256,
        footer_schema_sha256=artifact.footer_schema_sha256,
        description=_description(
            _metadata_value(schema.metadata, "table_description", table=artifact.table),
            overrides.get((artifact.table, None)),
        ),
        columns=columns,
    )


def build_metadata_bundle(
    root: Path,
    manifest: SnapshotManifest,
    profile: MetadataProfile,
    *,
    generated_at: str,
) -> MetadataBundle:
    """Build a complete bundle from already validated snapshot evidence."""
    overrides = _validate_overrides(profile)
    if profile.snapshot_id != manifest.snapshot_id:
        raise MetadataBundleError("Metadata profile does not match the validated snapshot identity.")
    if any(not _SAFE_NAME.fullmatch(artifact.table) for artifact in manifest.artifacts):
        raise MetadataBundleError("Snapshot manifest contains an unsafe table name.")
    artifacts = {artifact.table: artifact for artifact in manifest.artifacts}
    if len(artifacts) != len(manifest.artifacts):
        raise MetadataBundleError("Snapshot manifest contains duplicate table names.")
    tables = [_table_metadata(root, artifacts[name], overrides) for name in sorted(artifacts)]
    columns = {table.name: {column.name for column in table.columns} for table in tables}
    unknown = [
        override
        for override in profile.overrides
        if override.table not in columns
        or (override.column is not None and override.column not in columns[override.table])
    ]
    if unknown:
        names = sorted(
            f"{override.table}.{override.column}" if override.column else override.table for override in unknown
        )
        raise MetadataBundleError(
            "Metadata profile names unknown table or column override(s): " + ", ".join(names) + "."
        )
    source_schemas = sorted({(item.source_schema_id, item.source_schema_version) for item in manifest.artifacts})
    return MetadataBundle(
        bundle_format_version=BUNDLE_FORMAT_VERSION,
        generated_at=generated_at,
        snapshot_id=manifest.snapshot_id,
        profile_id=profile.profile_id,
        source_schemas=[SchemaIdentity(schema_id=schema_id, version=version) for schema_id, version in source_schemas],
        target_schema_ids=sorted(manifest.target_schema_ids),
        mapping_ids=sorted(manifest.mapping_ids),
        namespace=profile.namespace,
        tables=tables,
    )


def generate_metadata_bundle(snapshot_root: Path, profile_path: Path) -> MetadataBundle:
    """Validate all offline inputs and generate their provider-neutral bundle."""
    snapshot = snapshot_root.expanduser()
    manifest = validate_snapshot(snapshot)
    profile = load_metadata_profile(profile_path)
    return build_metadata_bundle(snapshot.resolve(), manifest, profile, generated_at=datetime.now(UTC).isoformat())


def metadata_json_schema(document: Literal["profile", "bundle"]) -> dict[str, Any]:
    """Return the selected metadata document's versioned JSON Schema."""
    models: dict[str, tuple[type[BaseModel], int]] = {
        "profile": (MetadataProfile, PROFILE_FORMAT_VERSION),
        "bundle": (MetadataBundle, BUNDLE_FORMAT_VERSION),
    }
    model, format_version = models[document]
    schema = model.model_json_schema()
    schema["x-format-version"] = format_version
    return schema


def render_metadata_bundle(bundle: MetadataBundle) -> str:
    """Render canonical reviewable JSON for stdout or a file."""
    return json.dumps(bundle.model_dump(mode="json"), indent=2, sort_keys=True)


def write_metadata_bundle(path: Path, bundle: MetadataBundle) -> Path:
    """Atomically write a generated bundle to an ordinary local path."""
    destination = path.expanduser()
    if destination.is_symlink():
        raise MetadataBundleError("Metadata bundle output must be an ordinary file path.")
    destination = destination.resolve()
    if destination.exists() and (destination.is_symlink() or not destination.is_file()):
        raise MetadataBundleError("Metadata bundle output must be an ordinary file path.")
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(prefix=f".{destination.name}.", suffix=".tmp", dir=destination.parent)
    temporary = Path(temporary_name)
    try:
        with open(fd, "w", encoding="utf-8", closefd=True) as stream:
            stream.write(render_metadata_bundle(bundle))
            stream.write("\n")
        temporary.replace(destination)
    except OSError as error:
        raise MetadataBundleError("Cannot write the metadata bundle.") from error
    finally:
        temporary.unlink(missing_ok=True)
    return destination
