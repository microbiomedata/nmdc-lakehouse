"""Tests for the canonical flattened LinkML schema artifact command."""

import subprocess
import sys
from pathlib import Path

import pytest

from scripts.python.generate_flattened_schema import (
    CANONICAL_OUTPUT,
    SchemaArtifactError,
    check_schema_artifact,
    main,
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


def test_generation_entrypoint_warns_without_changing_the_canonical_artifact() -> None:
    before = CANONICAL_OUTPUT.read_bytes()
    with pytest.warns(FutureWarning, match="nmdc-lakehouse-schema"):
        main(["--check"])
    assert CANONICAL_OUTPUT.read_bytes() == before


def test_legacy_generation_warns_and_still_generates_and_checks(tmp_path: Path) -> None:
    """Both CLI modes remain usable and display the replacement command by default."""
    script = Path(__file__).resolve().parents[1] / "scripts/python/generate_flattened_schema.py"
    artifact = tmp_path / "legacy.yaml"
    expected = CANONICAL_OUTPUT.read_bytes()
    for args in ([str(artifact)], ["--check", str(artifact)]):
        result = subprocess.run([sys.executable, str(script), *args], capture_output=True, text=True)
        assert result.returncode == 0, result.stderr
        assert "FutureWarning" in result.stderr
        assert "Schema generation in nmdc-lakehouse is deprecated" in result.stderr
        assert "'just generate-flat-schema' in nmdc-lakehouse-schema" in result.stderr
        assert artifact.read_bytes() == expected
