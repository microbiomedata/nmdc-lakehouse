"""Tests for the generic collection-to-parquet job registration."""

from __future__ import annotations

import pyarrow.parquet as pq

import nmdc_lakehouse.jobs  # noqa: F401 — registers all built-in jobs including direct ones
from nmdc_lakehouse.jobs import collection_to_parquet as collection_module
from nmdc_lakehouse.jobs.collection_to_parquet import (
    REVIEWED_SCHEMA_COLLECTIONS,
    AllCollectionsToParquetJob,
    CollectionToParquetJob,
    _db_collection_map,
)
from nmdc_lakehouse.jobs.registry import get, list_names


def test_schema_collection_baseline_matches_reviewed_snapshot():
    """Schema scope changes require an explicit snapshot update."""
    assert set(_db_collection_map().keys()) == REVIEWED_SCHEMA_COLLECTIONS


def test_all_schema_collections_registered():
    """One job is registered per reviewed Database slot (19 total)."""
    assert REVIEWED_SCHEMA_COLLECTIONS <= set(list_names())


def test_all_collections_job_registered():
    """'all-collections' job is registered."""
    assert "all-collections" in list_names()


def test_collection_job_instance():
    """registry.get('study_set') returns a CollectionToParquetJob."""
    job = get("study_set")
    assert isinstance(job, CollectionToParquetJob)
    assert job.collection == "study_set"
    assert job.root_class == "Study"


def test_direct_collections_not_collection_to_parquet():
    """DIRECT_COLLECTIONS are registered as DirectMongoToParquetJob, not CollectionToParquetJob."""
    from nmdc_lakehouse.jobs.direct_mongo_to_parquet import DIRECT_COLLECTIONS, DirectMongoToParquetJob

    for name in DIRECT_COLLECTIONS:
        job = get(name)
        assert isinstance(job, DirectMongoToParquetJob), f"{name} should use DirectMongoToParquetJob"
        assert not isinstance(job, CollectionToParquetJob)


def test_all_collections_job_instance():
    """registry.get('all-collections') returns an AllCollectionsToParquetJob."""
    job = get("all-collections")
    assert isinstance(job, AllCollectionsToParquetJob)


def test_all_collections_skip_via_env(monkeypatch):
    """LAKEHOUSE_SKIP_COLLECTIONS populates the skip set."""
    monkeypatch.setenv("LAKEHOUSE_SKIP_COLLECTIONS", "functional_annotation_agg, study_set")
    job = get("all-collections")
    assert isinstance(job, AllCollectionsToParquetJob)
    assert job.skip == {"functional_annotation_agg", "study_set"}


def test_all_collections_skip_default_empty(monkeypatch):
    """Unset LAKEHOUSE_SKIP_COLLECTIONS yields an empty skip set."""
    monkeypatch.delenv("LAKEHOUSE_SKIP_COLLECTIONS", raising=False)
    job = get("all-collections")
    assert isinstance(job, AllCollectionsToParquetJob)
    assert job.skip == set()


def test_collection_run_promotes_primary_and_removes_stale_side_table(tmp_path, monkeypatch):
    """A no-side-row rerun reconciles stale owned side-table output."""

    class TestSource:
        def __init__(self, _uri):
            pass

        def iter_records(self, _collection, page_size):
            del page_size
            yield {"id": "nmdc:sty-11-test", "name": "Test study", "type": "nmdc:Study"}

        def estimated_count(self, _collection):
            return 1

    stale_side = tmp_path / "study_set_associated_dois.parquet"
    stale_side.write_bytes(b"stale")
    unrelated = tmp_path / "notes.txt"
    unrelated.write_text("preserve", encoding="utf-8")
    monkeypatch.setattr(collection_module, "MongoSource", TestSource)

    result = CollectionToParquetJob("study_set", "Study", "mongodb://localhost/nmdc", tmp_path).run()

    assert result.table_rows == (("study_set", 1),)
    assert pq.read_metadata(tmp_path / "study_set.parquet").num_rows == 1
    assert not stale_side.exists()
    assert unrelated.read_text(encoding="utf-8") == "preserve"
    assert not (tmp_path / ".staging").exists()
