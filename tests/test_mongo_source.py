"""Tests for MongoSource."""

from __future__ import annotations

import os
from itertools import islice

import pytest

from nmdc_lakehouse.config import MongoSettings
from nmdc_lakehouse.sources.mongo_source import MongoSource

# ---------- Unit tests (no MongoDB required) ----------


def test_mongo_settings_uri_without_auth():
    """Default settings with empty password produce an auth-less URI."""
    s = MongoSettings(host="localhost", port=27017, db="nmdc_test", password="")
    assert s.uri == "mongodb://localhost:27017/nmdc_test"


def test_mongo_settings_uri_with_auth():
    """Password-bearing settings produce a URI with percent-escaped credentials."""
    s = MongoSettings(host="h", port=27018, db="nmdc", username="adm in", password="p@ss")
    assert s.uri == "mongodb://adm%20in:p%40ss@h:27018/nmdc"


def test_mongo_settings_uri_replica_set():
    """Replica set shows up as a query parameter."""
    s = MongoSettings(host="rs0", port=27017, db="nmdc", password="", replica_set="rs0")
    assert s.uri == "mongodb://rs0:27017/nmdc?replicaSet=rs0"


def test_mongo_source_from_settings_uses_uri():
    """``from_settings`` propagates the URI and alias without connecting."""
    s = MongoSettings(host="localhost", port=27017, db="nmdc_lakehouse_prep", password="")
    src = MongoSource.from_settings(s, alias="nmdc-lh")
    assert src.handle == "mongodb://localhost:27017/nmdc_lakehouse_prep"
    assert src.alias == "nmdc-lh"


# ---------- Integration tests (require local MongoDB) ----------


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
