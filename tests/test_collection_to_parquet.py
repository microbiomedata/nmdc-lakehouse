"""Tests for the generic collection-to-parquet job registration."""

from __future__ import annotations

import nmdc_lakehouse.jobs  # noqa: F401 — registers all built-in jobs including direct ones
from nmdc_lakehouse.jobs.collection_to_parquet import (
    AllCollectionsToParquetJob,
    CollectionToParquetJob,
)
from nmdc_lakehouse.jobs.registry import get, list_names


def test_all_17_collections_registered():
    """One job is registered per Database slot (17 total)."""
    names = list_names()
    # The 17 schema-specified collections
    expected = {
        "biosample_set",
        "calibration_set",
        "collecting_biosamples_from_site_set",
        "configuration_set",
        "data_generation_set",
        "data_object_set",
        "field_research_site_set",
        "functional_annotation_agg",
        "functional_annotation_set",
        "genome_feature_set",
        "instrument_set",
        "manifest_set",
        "material_processing_set",
        "processed_sample_set",
        "storage_process_set",
        "study_set",
        "workflow_execution_set",
    }
    assert expected.issubset(set(names))


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
