"""Direct pymongo → Parquet job for flat, high-volume collections.

linkml-store's ``find_iter`` calls ``count_documents`` on every page, which
takes ~25 s per call on ``functional_annotation_agg`` (54.8 M records) and
produces a projected runtime of ~15 days. See linkml-store#69.

This module bypasses linkml-store entirely: a raw pymongo cursor streams
records in large batches, the pyarrow schema is derived from the installed
nmdc-schema, and rows are written via the existing ``ParquetSink``.

Throughput on the 2026-04-24 benchmark: ~30,000 rows/s vs ~34 rows/s through
the linkml-store path (~900x faster). ``functional_annotation_agg`` (54.8 M
records) completes in ~17 minutes instead of ~15 days.
"""

from __future__ import annotations

import logging
import os
import time
from importlib.util import find_spec
from pathlib import Path
from typing import Iterator
from urllib.parse import urlparse

import pymongo

from nmdc_lakehouse.config import LakehouseSettings, MongoSettings
from nmdc_lakehouse.jobs.base import Job, JobResult
from nmdc_lakehouse.jobs.registry import register
from nmdc_lakehouse.sinks.parquet_sink import ParquetSink

logger = logging.getLogger(__name__)

_BATCH_SIZE = 50_000

# Collections handled by this module. Excluded from CollectionToParquetJob's
# auto-registration loop so each name is registered exactly once.
DIRECT_COLLECTIONS: frozenset[str] = frozenset({"functional_annotation_agg"})


def _schema_path() -> str:
    spec = find_spec("nmdc_schema")
    if spec is None or not spec.submodule_search_locations:
        raise RuntimeError("nmdc_schema package is not installed")
    return str(Path(spec.submodule_search_locations[0]) / "nmdc_materialized_patterns.yaml")


def _dbname_from_uri(uri: str) -> str:
    parsed = urlparse(uri)
    return parsed.path.lstrip("/").split("?")[0] or "nmdc"


class DirectMongoToParquetJob(Job):
    """Flatten a flat NMDC collection to Parquet via a raw pymongo cursor.

    Use instead of ``CollectionToParquetJob`` for collections where linkml-store
    ``find_iter`` is prohibitively slow due to per-page ``count_documents`` calls.
    """

    def __init__(
        self,
        collection: str,
        root_class: str,
        mongo_uri: str,
        out_root: Path,
        batch_size: int = _BATCH_SIZE,
    ) -> None:
        """Construct the job for a single collection."""
        self.collection = collection
        self.root_class = root_class
        self.mongo_uri = mongo_uri
        self.out_root = out_root
        self.batch_size = batch_size
        self.name = collection

    def run(self, *, dry_run: bool = False) -> JobResult:
        """Stream records from MongoDB through a raw cursor into Parquet."""
        from linkml_runtime import SchemaView

        from nmdc_lakehouse.transforms.schema_generator import flatten_class_def

        schema_view = SchemaView(_schema_path())
        flat_class = flatten_class_def(schema_view, self.root_class)

        client: pymongo.MongoClient = pymongo.MongoClient(self.mongo_uri)
        try:
            dbname = _dbname_from_uri(self.mongo_uri)
            mongo_coll = client[dbname][self.collection]

            total = mongo_coll.estimated_document_count()
            total_str = f"~{total:,}" if total else "?"
            logger.info("%s: starting (%s records)", self.collection, total_str)

            log_interval = int(os.environ.get("LAKEHOUSE_LOG_INTERVAL", "1000000"))
            heartbeat_secs = int(os.environ.get("LAKEHOUSE_HEARTBEAT_SECS", "60"))

            cursor = mongo_coll.find({}, {"_id": 0}).batch_size(self.batch_size)
            rows_read = 0

            def _stream() -> Iterator[dict]:
                nonlocal rows_read
                first_logged = False
                t0 = time.monotonic()
                last_log_t = t0
                for doc in cursor:
                    rows_read += 1
                    now = time.monotonic()
                    if not first_logged:
                        first_logged = True
                        logger.info("%s: first row received", self.collection)
                        last_log_t = now
                    elapsed = now - t0
                    rate = rows_read / elapsed if elapsed > 0 else 0
                    if log_interval > 0 and rows_read % log_interval == 0:
                        pct = rows_read / total * 100 if total else 0
                        eta_h = (total - rows_read) / rate / 3600 if rate > 0 and total else 0
                        logger.info(
                            "%s: %d / %s rows (%.0f rows/s, %.1f%%, ETA %.1fh)",
                            self.collection,
                            rows_read,
                            total_str,
                            rate,
                            pct,
                            eta_h,
                        )
                        last_log_t = now
                    elif heartbeat_secs > 0 and (now - last_log_t) >= heartbeat_secs:
                        logger.info(
                            "%s: heartbeat — %d / %s rows (%.0f rows/s, %.1f min elapsed)",
                            self.collection,
                            rows_read,
                            total_str,
                            rate,
                            elapsed / 60,
                        )
                        last_log_t = now
                    yield doc

            if dry_run:
                for _ in _stream():
                    pass
                return JobResult(
                    job_name=self.name,
                    rows_read=rows_read,
                    rows_written=0,
                    tables_written=(),
                )

            sink = ParquetSink(self.out_root, class_def=flat_class, batch_size=self.batch_size)
            rows_written = sink.write(_stream(), table=self.collection)
        finally:
            client.close()

        return JobResult(
            job_name=self.name,
            rows_read=rows_read,
            rows_written=rows_written,
            tables_written=(self.collection,),
        )


def _make_direct_factory(collection: str, root_class: str):
    def _factory():
        mongo_uri = MongoSettings().uri
        out_root = LakehouseSettings().root
        return DirectMongoToParquetJob(collection, root_class, mongo_uri, out_root)

    return _factory


def _root_class_for(collection: str) -> str:
    import yaml

    spec = find_spec("nmdc_schema")
    if spec is None or not spec.submodule_search_locations:
        return collection
    schema_path = Path(spec.submodule_search_locations[0]) / "nmdc_materialized_patterns.yaml"
    schema = yaml.safe_load(schema_path.read_text())
    top_slots = schema.get("slots", {})
    return top_slots.get(collection, {}).get("range", collection)


for _coll in DIRECT_COLLECTIONS:
    register(_coll)(_make_direct_factory(_coll, _root_class_for(_coll)))
