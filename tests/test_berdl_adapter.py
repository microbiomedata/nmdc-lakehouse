"""Tests for the repository-owned BERDL adapter boundary."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from nmdc_lakehouse import berdl_adapter


def _arguments(tmp_path: Path) -> list[str]:
    data = tmp_path / "snapshot"
    data.mkdir()
    (data / "biosample_set.parquet").write_bytes(b"PAR1")
    checkout = tmp_path / "data-lakehouse-ingest"
    package = checkout / "src" / "data_lakehouse_ingest"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("from .core import ingest\n", encoding="utf-8")
    (package / "core.py").write_text("def ingest(config): ...\n", encoding="utf-8")
    return [
        "--data-dir",
        str(data),
        "--ingest-checkout",
        str(checkout),
        "--tenant",
        "nmdc",
        "--dataset",
        "nmdc_metadata_staging_20260819",
        "--staging-namespace",
        "nmdc.nmdc_metadata_staging_20260819",
        "--mode",
        "overwrite",
        "--bucket",
        "cdm-lake",
        "--bronze-prefix",
        "tenant-general-warehouse/nmdc/staging/20260819",
        "--progress-key",
        "tenant-general-warehouse/nmdc/staging/20260819/progress.jsonl",
        "--config-key",
        "tenant-general-warehouse/nmdc/staging/20260819/config.json",
    ]


def test_plan_only_reports_owned_adapter_inputs(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert berdl_adapter.main(_arguments(tmp_path)) == 0

    document = json.loads(capsys.readouterr().out)
    assert document["status"] == "plan-only"
    assert document["tables"] == ["biosample_set"]
    assert document["destination"]["namespace"] == "nmdc.nmdc_metadata_staging_20260819"
    assert document["ingest"]["api"] == "data_lakehouse_ingest.ingest"


def test_planner_slice_refuses_live_execution(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit, match="2"):
        berdl_adapter.main([*_arguments(tmp_path), "--execute-staging"])

    assert "live staging execution is not available" in capsys.readouterr().err


@pytest.mark.parametrize(
    ("option", "value", "message"),
    [
        ("--staging-namespace", "nmdc.other", "exactly match"),
        ("--bucket", "192.168.1.1", "IP address"),
        ("--progress-key", "elsewhere/progress.jsonl", "children"),
    ],
)
def test_adapter_rejects_unsafe_destinations(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    option: str,
    value: str,
    message: str,
) -> None:
    arguments = _arguments(tmp_path)
    arguments[arguments.index(option) + 1] = value

    with pytest.raises(SystemExit, match="2"):
        berdl_adapter.main(arguments)

    assert message in capsys.readouterr().err
