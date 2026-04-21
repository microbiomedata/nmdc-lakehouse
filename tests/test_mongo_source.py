"""Tests for MongoSource."""

from __future__ import annotations

import os
from itertools import islice

import pytest

from nmdc_lakehouse.sources.mongo_source import MongoSource


@pytest.mark.integration
@pytest.mark.skipif(
    os.environ.get("ENABLE_DB_TESTS") != "true",
    reason="Requires a local MongoDB (set ENABLE_DB_TESTS=true)",
)
def test_iter_records_streams_prefix():
    """Streaming iteration yields dicts with ``_id`` stripped."""
    handle = os.environ.get("MONGO_URI", "mongodb://localhost:27017/nmdc")
    source = MongoSource(handle)

    rows = list(islice(source.iter_records("biosample_set", page_size=500), 10))
    assert len(rows) == 10
    assert all(isinstance(r, dict) for r in rows)
    assert all("_id" not in r for r in rows)


@pytest.mark.integration
@pytest.mark.skipif(
    os.environ.get("ENABLE_DB_TESTS") != "true",
    reason="Requires a local MongoDB (set ENABLE_DB_TESTS=true)",
)
def test_iter_records_pages_do_not_duplicate():
    """Paginated iteration returns each record exactly once across page boundaries."""
    handle = os.environ.get("MONGO_URI", "mongodb://localhost:27017/nmdc")
    source = MongoSource(handle)

    # 1000 is > 5 pages at page_size=100, so this exercises pagination stitching
    # without materialising the full collection.
    ids = [r["id"] for r in islice(source.iter_records("biosample_set", page_size=100), 1000) if "id" in r]
    assert len(ids) == len(set(ids)), "pagination produced duplicate records"
