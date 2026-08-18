"""Tests for lightweight local ETL run measurements."""

import json
from pathlib import Path

from click.testing import CliRunner

from nmdc_lakehouse.cli import cli
from nmdc_lakehouse.jobs.base import JobResult
from nmdc_lakehouse.metrics import failure_record, stamp_result, success_record, write_record


def test_stamp_and_success_record_include_rows_rate_bytes_and_children(tmp_path: Path, monkeypatch) -> None:
    output = tmp_path / "biosample_set.parquet"
    output.write_bytes(b"parquet-bytes")
    child = JobResult(
        job_name="biosample_set",
        rows_read=20,
        rows_written=20,
        tables_written=("biosample_set",),
        table_rows=(("biosample_set", 20),),
    )
    stamp_result(
        child,
        output_root=tmp_path,
        started_at="2026-08-18T00:00:00+00:00",
        finished_at="2026-08-18T00:00:04+00:00",
        elapsed_seconds=4.0,
    )
    run = JobResult(
        job_name="all-collections",
        rows_read=20,
        rows_written=20,
        tables_written=("biosample_set",),
        table_rows=(("biosample_set", 20),),
        children=(child,),
    )
    stamp_result(
        run,
        output_root=tmp_path,
        started_at="2026-08-18T00:00:00+00:00",
        finished_at="2026-08-18T00:00:05+00:00",
        elapsed_seconds=5.0,
    )
    monkeypatch.setattr("nmdc_lakehouse.metrics.peak_rss_bytes", lambda: 123456)

    record = success_record(run, skipped_collections=("functional_annotation_agg",), dry_run=False)

    assert record["status"] == "success"
    assert record["dry_run"] is False
    assert record["rows_per_second"] == 4.0
    assert record["children"][0]["rows_per_second"] == 5.0
    assert record["children"][0]["outputs"] == [
        {
            "table": "biosample_set",
            "path": "biosample_set.parquet",
            "rows": 20,
            "bytes": len(b"parquet-bytes"),
        }
    ]
    assert record["output_root"] == str(tmp_path.resolve())
    assert record["skipped_collections"] == ["functional_annotation_agg"]
    assert record["environment"]["peak_rss_bytes"] == 123456
    assert record["environment"]["peak_rss_unit"] == "bytes"
    assert record["environment"]["nmdc_schema_version"]


def test_stamp_result_sorts_outputs_by_table(tmp_path: Path) -> None:
    (tmp_path / "alpha.parquet").write_bytes(b"a")
    (tmp_path / "zeta.parquet").write_bytes(b"z")
    result = JobResult(
        job_name="fixture",
        table_rows=(("zeta", 2), ("alpha", 1)),
    )

    stamp_result(
        result,
        output_root=tmp_path,
        started_at="start",
        finished_at="finish",
        elapsed_seconds=1.0,
    )

    assert [output.table for output in result.outputs] == ["alpha", "zeta"]


def test_failure_record_is_sanitized(monkeypatch) -> None:
    monkeypatch.setattr("nmdc_lakehouse.metrics.peak_rss_bytes", lambda: None)
    secret = "TOP-SECRET-SENTINEL"

    record = failure_record(
        job_name="all-collections",
        started_at="start",
        finished_at="finish",
        elapsed_seconds=2.0,
        error=RuntimeError(secret),
        skipped_collections=("functional_annotation_agg",),
        dry_run=False,
    )

    assert record["status"] == "failed"
    assert record["dry_run"] is False
    assert record["error_type"] == "RuntimeError"
    assert secret not in repr(record)
    assert record["environment"]["peak_rss_bytes"] is None
    assert record["environment"]["peak_rss_source"] is None
    assert record["environment"]["peak_rss_unit"] is None


def test_write_record_is_atomic_json(tmp_path: Path) -> None:
    destination = tmp_path / "metrics" / "run.json"

    write_record(destination, {"status": "success", "rows": 3})

    assert json.loads(destination.read_text(encoding="utf-8")) == {"rows": 3, "status": "success"}
    assert not list(destination.parent.glob("*.tmp"))


def test_run_job_cli_writes_success_and_sanitized_failure_records(tmp_path: Path, monkeypatch) -> None:
    class SuccessfulJob:
        out_root = tmp_path

        def run(self, *, dry_run: bool = False) -> JobResult:
            assert not dry_run
            return JobResult(job_name="fixture", rows_read=3, rows_written=3)

    class FailingJob:
        def run(self, *, dry_run: bool = False) -> JobResult:
            raise RuntimeError("TOP-SECRET-SENTINEL")

    class InterruptingJob:
        def run(self, *, dry_run: bool = False) -> JobResult:
            raise KeyboardInterrupt

    metrics_path = tmp_path / "run.json"
    monkeypatch.setattr("nmdc_lakehouse.jobs.registry.get", lambda _name: SuccessfulJob())
    success = CliRunner().invoke(cli, ["run-job", "fixture", "--metrics", str(metrics_path)])
    success_record_data = json.loads(metrics_path.read_text(encoding="utf-8"))

    assert success.exit_code == 0
    assert success_record_data["status"] == "success"
    assert success_record_data["rows_read"] == 3

    monkeypatch.setattr("nmdc_lakehouse.jobs.registry.get", lambda _name: FailingJob())
    failure = CliRunner().invoke(cli, ["run-job", "fixture", "--metrics", str(metrics_path)])
    failure_record_data = json.loads(metrics_path.read_text(encoding="utf-8"))

    assert failure.exit_code != 0
    assert failure_record_data["status"] == "failed"
    assert failure_record_data["error_type"] == "RuntimeError"
    assert "TOP-SECRET-SENTINEL" not in repr(failure_record_data)

    monkeypatch.setattr("nmdc_lakehouse.jobs.registry.get", lambda _name: InterruptingJob())
    interrupted = CliRunner().invoke(cli, ["run-job", "fixture", "--metrics", str(metrics_path)])
    interrupted_record_data = json.loads(metrics_path.read_text(encoding="utf-8"))

    assert interrupted.exit_code != 0
    assert interrupted_record_data["status"] == "failed"
    assert interrupted_record_data["error_type"] == "KeyboardInterrupt"


def test_run_job_cli_rejects_metrics_directory(tmp_path: Path) -> None:
    result = CliRunner().invoke(cli, ["run-job", "fixture", "--metrics", str(tmp_path)])

    assert result.exit_code == 2
    assert "is a directory" in result.output
