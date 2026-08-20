"""Tests for the repository-owned BERDL adapter boundary."""

from __future__ import annotations

import json
import os
import sys
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
                    target_table="nmdc.nmdc_metadata_staging_20260819.biosample_set",
                )
            ],
        }

    monkeypatch.setattr(berdl_adapter, "_runtime", lambda _checkout: (ingest, client))
    monkeypatch.setattr(
        berdl_adapter,
        "_catalog_row_count",
        lambda table: observed.setdefault("counted_table", table) and 1,
    )
    outcome = tmp_path / "upstream-outcome.json"
    assert berdl_adapter.main([*_arguments(tmp_path), "--outcome", str(outcome), "--execute-staging"]) == 0

    document = json.loads(capsys.readouterr().out)
    assert document["status"] == "verified"
    assert document["destination"]["provider"] == "spark_catalog"
    assert document["destination"]["table_format"] == "iceberg"
    assert document["verification"]["tables"][0]["source_rows"] == 1
    assert observed["config"]["tables"][0]["format"] == "parquet"
    assert observed["counted_table"] == "nmdc.nmdc_metadata_staging_20260819.biosample_set"
    assert outcome.is_file()
    assert ("cdm-lake", "tenant-general-warehouse/nmdc/staging/20260819/config.json") in client.objects


def test_execute_rejects_destination_count_that_differs_from_source(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    client = _Client()

    def ingest(_config, *, minio_client):
        assert minio_client is client
        return {
            "success": True,
            "tables": [
                SimpleNamespace(
                    name="biosample_set",
                    status=SimpleNamespace(value="success"),
                    rows_in=1,
                    rows_written=1,
                    target_table="nmdc.nmdc_metadata_staging_20260819.biosample_set",
                )
            ],
        }

    monkeypatch.setattr(berdl_adapter, "_runtime", lambda _checkout: (ingest, client))
    monkeypatch.setattr(berdl_adapter, "_catalog_row_count", lambda _table: 2)

    with pytest.raises(SystemExit, match="2"):
        berdl_adapter.main([*_arguments(tmp_path), "--outcome", str(tmp_path / "outcome.json"), "--execute-staging"])

    assert "destination row count does not match" in capsys.readouterr().err


def test_execute_sanitizes_object_store_failures(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    class FailingClient(_Client):
        def fput_object(self, *_args, **_kwargs):
            raise RuntimeError("sensitive backend detail")

    monkeypatch.setattr(berdl_adapter, "_runtime", lambda _checkout: (pytest.fail, FailingClient()))

    with pytest.raises(SystemExit, match="2"):
        berdl_adapter.main([*_arguments(tmp_path), "--outcome", str(tmp_path / "outcome.json"), "--execute-staging"])

    message = capsys.readouterr().err
    assert "object-store transfer failed for table 'biosample_set'" in message
    assert "sensitive backend detail" not in message


def test_outcome_publication_does_not_leave_a_partial_final_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    outcome = tmp_path / "outcome.json"

    def fail_link(_source, _destination):
        raise OSError("injected publication failure")

    monkeypatch.setattr(os, "link", fail_link)

    with pytest.raises(berdl_adapter.AdapterExecutionError, match="publish.*atomically"):
        berdl_adapter._write_outcome(outcome, {"status": "verified"})

    assert not outcome.exists()
    assert list(tmp_path.iterdir()) == []


def test_runtime_rejects_package_imported_outside_selected_checkout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    checkout = tmp_path / "selected"
    package = checkout / "src" / "data_lakehouse_ingest"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("def ingest(config, *, minio_client): ...\n", encoding="utf-8")
    other = tmp_path / "other" / "data_lakehouse_ingest" / "__init__.py"
    other.parent.mkdir(parents=True)
    other.write_text("def ingest(config, *, minio_client): ...\n", encoding="utf-8")
    stale = SimpleNamespace(__file__=str(other), ingest=lambda *_args, **_kwargs: None)
    monkeypatch.setitem(sys.modules, "data_lakehouse_ingest", stale)
    clients = SimpleNamespace(get_s3_client=lambda: object())
    monkeypatch.setitem(sys.modules, "berdl_notebook_utils", SimpleNamespace())
    monkeypatch.setitem(sys.modules, "berdl_notebook_utils.clients", clients)

    with pytest.raises(berdl_adapter.AdapterExecutionError, match="selected checkout"):
        berdl_adapter._runtime(checkout)


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
