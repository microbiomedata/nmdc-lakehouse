"""Tests for MongoSource."""

from __future__ import annotations

import os

import pytest

from nmdc_lakehouse.sources.mongo_source import MongoSource


@pytest.mark.integration
@pytest.mark.skipif(
    os.environ.get("ENABLE_DB_TESTS") != "true",
    reason="Requires a local MongoDB (set ENABLE_DB_TESTS=true)",
)
def test_iter_records_against_local_mongo():
    """Streaming iteration works against a local nmdc MongoDB."""
    handle = os.environ.get("MONGO_URI", "mongodb://localhost:27017/nmdc")
    source = MongoSource(handle)

    rows = list(source.iter_records("biosample_set", page_size=500))
    assert len(rows) > 0
    assert all(isinstance(r, dict) for r in rows)
    assert all("_id" not in r for r in rows)


@pytest.mark.integration
@pytest.mark.skipif(
    os.environ.get("ENABLE_DB_TESTS") != "true",
    reason="Requires a local MongoDB (set ENABLE_DB_TESTS=true)",
)
def test_iter_records_pages_do_not_duplicate():
    """Paginated iteration returns each record exactly once."""
    handle = os.environ.get("MONGO_URI", "mongodb://localhost:27017/nmdc")
    source = MongoSource(handle)

    ids = [r["id"] for r in source.iter_records("biosample_set", page_size=100) if "id" in r]
    assert len(ids) == len(set(ids)), "pagination produced duplicate records"
