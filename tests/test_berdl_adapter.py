"""Tests for the repository-owned BERDL adapter boundary."""

from __future__ import annotations

import json
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace

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
        "--destination-provider",
        "spark_catalog",
        "--destination-table-format",
        "iceberg",
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
    assert document["destination"]["provider"] == "spark_catalog"
    assert document["destination"]["table_format"] == "iceberg"
    assert document["ingest"]["api"] == "data_lakehouse_ingest.ingest"


class _Response(BytesIO):
    def release_conn(self) -> None:
        pass


class _Client:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}

    def fput_object(self, bucket, key, path, metadata=None):
        self.objects[(bucket, key)] = Path(path).read_bytes()

    def get_object(self, bucket, key):
        return _Response(self.objects[(bucket, key)])

    def put_object(self, bucket, key, stream, length):
        self.objects[(bucket, key)] = stream.read(length)


def test_execute_uploads_verifies_and_calls_official_ingest(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    client = _Client()
    observed = {}

    def ingest(config, *, minio_client):
        observed["config"] = config
        assert minio_client is client
        return {
            "success": True,
            "tables": [
                SimpleNamespace(
                    name="biosample_set",
                    status=SimpleNamespace(value="success"),
                    rows_in=1,
                    rows_written=1,
                )
            ],
        }

    monkeypatch.setattr(berdl_adapter, "_runtime", lambda _checkout: (ingest, client))
    outcome = tmp_path / "upstream-outcome.json"
    assert berdl_adapter.main([*_arguments(tmp_path), "--outcome", str(outcome), "--execute-staging"]) == 0

    document = json.loads(capsys.readouterr().out)
    assert document["status"] == "verified"
    assert document["verification"]["tables"][0]["source_rows"] == 1
    assert observed["config"]["tables"][0]["format"] == "parquet"
    assert outcome.is_file()
    assert ("cdm-lake", "tenant-general-warehouse/nmdc/staging/20260819/config.json") in client.objects


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
