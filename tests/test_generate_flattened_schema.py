"""Tests for the canonical flattened LinkML schema artifact command."""

from pathlib import Path

import pytest

from scripts.python.generate_flattened_schema import (
    SchemaArtifactError,
    check_schema_artifact,
    write_schema_artifact,
)


def test_schema_artifact_check_detects_stale_content(tmp_path: Path) -> None:
    artifact = tmp_path / "nmdc_metadata.yaml"
    write_schema_artifact(artifact, "current\n")

    check_schema_artifact(artifact, "current\n")
    with pytest.raises(SchemaArtifactError, match="stale"):
        check_schema_artifact(artifact, "replacement\n")


def test_schema_artifact_check_rejects_missing_file(tmp_path: Path) -> None:
    with pytest.raises(SchemaArtifactError, match="Cannot read"):
        check_schema_artifact(tmp_path / "missing.yaml", "expected\n")
