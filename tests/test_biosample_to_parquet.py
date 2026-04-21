"""Tests for the biosamples-to-parquet job."""

from __future__ import annotations

import os

import pyarrow.parquet as pq
import pytest

from nmdc_lakehouse.jobs.biosample_to_parquet import BiosampleToParquetJob
from nmdc_lakehouse.jobs.registry import get, list_names


def test_job_is_registered():
    """The job is registered under 'biosamples-to-parquet'."""
    import nmdc_lakehouse.jobs.biosample_to_parquet  # noqa: F401 — triggers registration

    assert "biosamples-to-parquet" in list_names()


def test_job_can_be_retrieved_from_registry():
    """registry.get() returns a BiosampleToParquetJob instance."""
    import nmdc_lakehouse.jobs.biosample_to_parquet  # noqa: F401

    job = get("biosamples-to-parquet")
    assert isinstance(job, BiosampleToParquetJob)


@pytest.mark.integration
@pytest.mark.skipif(
    os.environ.get("ENABLE_DB_TESTS") != "true",
    reason="Requires a local MongoDB and nmdc_lakehouse_prep (set ENABLE_DB_TESTS=true)",
)
def test_dry_run_counts_but_writes_nothing(tmp_path):
    """dry_run=True counts records without writing a Parquet file."""
    mongo_uri = os.environ.get("MONGO_URI", "mongodb://localhost:27017/nmdc_lakehouse_prep")
    job = BiosampleToParquetJob(mongo_uri=mongo_uri, out_root=tmp_path)
    result = job.run(dry_run=True)

    assert result.rows_read > 0
    assert result.rows_written == 0
    assert result.tables_written == ()
    assert not list(tmp_path.glob("*.parquet"))


@pytest.mark.integration
@pytest.mark.skipif(
    os.environ.get("ENABLE_DB_TESTS") != "true",
    reason="Requires a local MongoDB and nmdc_lakehouse_prep (set ENABLE_DB_TESTS=true)",
)
def test_full_run_writes_parquet(tmp_path):
    """End-to-end: fetches biosamples, flattens, writes Parquet."""
    mongo_uri = os.environ.get("MONGO_URI", "mongodb://localhost:27017/nmdc_lakehouse_prep")
    job = BiosampleToParquetJob(mongo_uri=mongo_uri, out_root=tmp_path)
    result = job.run(dry_run=False)

    out = tmp_path / "biosample_set.parquet"
    assert out.exists()
    tbl = pq.read_table(out)
    assert tbl.num_rows == result.rows_written
    assert tbl.num_rows > 0
    assert "id" in tbl.schema.names
    assert result.tables_written == ("biosample_set",)
