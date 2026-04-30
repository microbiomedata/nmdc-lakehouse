"""ETL job: flatten the MongoDB alldocs graph index into three Parquet tables.

alldocs is a runtime-maintained collection (~360 K documents) that is not part
of the nmdc-schema Database class. Each document represents one NMDC entity and
carries its immediate graph neighbours across all collection types.

Three output tables (all registered in nmdc_metadata):

  nmdc_graph_nodes             — one row per entity (id, type)
  nmdc_graph_nodes_type_ancestors — side table (parent_id, type_ancestor)
  nmdc_graph_edges             — one directed edge per row (src_id, src_type,
                                  dst_id, dst_type), built from _downstream
                                  arrays only so each edge appears exactly once.

The MongoDB field names _upstream and _downstream do not appear in any output.
"""

from __future__ import annotations

import logging
import os
import time
from pathlib import Path
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.parquet as pq
import pymongo

from nmdc_lakehouse.config import LakehouseSettings, MongoSettings
from nmdc_lakehouse.jobs.base import Job, JobResult
from nmdc_lakehouse.jobs.registry import register

logger = logging.getLogger(__name__)

COLLECTION = "alldocs"
_BATCH_SIZE = 50_000

_NODE_SCHEMA = pa.schema(
    [
        pa.field("id", pa.string()),
        pa.field("type", pa.string()),
    ]
)

_ANCESTOR_SCHEMA = pa.schema(
    [
        pa.field("parent_id", pa.string()),
        pa.field("type_ancestor", pa.string()),
    ]
)

_EDGE_SCHEMA = pa.schema(
    [
        pa.field("src_id", pa.string()),
        pa.field("src_type", pa.string()),
        pa.field("dst_id", pa.string()),
        pa.field("dst_type", pa.string()),
    ]
)


def _dbname_from_uri(uri: str) -> str:
    parsed = urlparse(uri)
    return parsed.path.lstrip("/").split("?")[0] or "nmdc"


class _BatchWriter:
    """Accumulate rows and flush to a Parquet file in fixed-size batches."""

    def __init__(self, path: Path, schema: pa.Schema, batch_size: int = _BATCH_SIZE) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._writer = pq.ParquetWriter(str(path), schema)
        self._schema = schema
        self._batch_size = batch_size
        self._buf: list[dict] = []
        self.rows_written = 0

    def append(self, row: dict) -> None:
        self._buf.append(row)
        if len(self._buf) >= self._batch_size:
            self._flush()

    def _flush(self) -> None:
        if not self._buf:
            return
        batch = pa.RecordBatch.from_pylist(self._buf, schema=self._schema)
        self._writer.write_batch(batch)
        self.rows_written += len(self._buf)
        self._buf = []

    def close(self) -> int:
        self._flush()
        self._writer.close()
        return self.rows_written


class AlldocsToParquetJob(Job):
    """Flatten the MongoDB alldocs collection into three graph Parquet tables."""

    name = COLLECTION

    def __init__(self, mongo_uri: str, out_root: Path, batch_size: int = _BATCH_SIZE) -> None:
        """Construct the job."""
        self.mongo_uri = mongo_uri
        self.out_root = out_root
        self.batch_size = batch_size

    def run(self, *, dry_run: bool = False) -> JobResult:
        """Stream alldocs from MongoDB and write graph tables to Parquet."""
        node_path = self.out_root / "nmdc_graph_nodes.parquet"
        ancestor_path = self.out_root / "nmdc_graph_nodes_type_ancestors.parquet"
        edge_path = self.out_root / "nmdc_graph_edges.parquet"

        client: pymongo.MongoClient = pymongo.MongoClient(self.mongo_uri)
        try:
            dbname = _dbname_from_uri(self.mongo_uri)
            coll = client[dbname][COLLECTION]

            total = coll.estimated_document_count()
            total_str = f"~{total:,}" if total else "?"
            logger.info("%s: starting (%s documents)", COLLECTION, total_str)

            log_interval = int(os.environ.get("LAKEHOUSE_LOG_INTERVAL", "10000"))
            heartbeat_secs = int(os.environ.get("LAKEHOUSE_HEARTBEAT_SECS", "60"))

            if dry_run:
                rows_read = sum(1 for _ in coll.find({}, {"_id": 0}).batch_size(self.batch_size))
                return JobResult(
                    job_name=self.name,
                    rows_read=rows_read,
                    rows_written=0,
                    tables_written=(),
                )

            nodes = _BatchWriter(node_path, _NODE_SCHEMA, self.batch_size)
            ancestors = _BatchWriter(ancestor_path, _ANCESTOR_SCHEMA, self.batch_size)
            edges = _BatchWriter(edge_path, _EDGE_SCHEMA, self.batch_size)

            rows_read = 0
            first_logged = False
            t0 = time.monotonic()
            last_log_t = t0

            projection = {"_id": 0, "id": 1, "type": 1, "_type_and_ancestors": 1, "_downstream": 1}
            cursor = coll.find({}, projection).batch_size(self.batch_size)

            for doc in cursor:
                rows_read += 1
                entity_id = doc.get("id", "")
                entity_type = doc.get("type", "")

                nodes.append({"id": entity_id, "type": entity_type})

                for ancestor in doc.get("_type_and_ancestors") or []:
                    ancestors.append({"parent_id": entity_id, "type_ancestor": ancestor})

                for neighbor in doc.get("_downstream") or []:
                    edges.append(
                        {
                            "src_id": entity_id,
                            "src_type": entity_type,
                            "dst_id": neighbor.get("id", ""),
                            "dst_type": neighbor.get("type", ""),
                        }
                    )

                now = time.monotonic()
                if not first_logged:
                    first_logged = True
                    logger.info("%s: first document received", COLLECTION)
                    last_log_t = now
                elapsed = now - t0
                rate = rows_read / elapsed if elapsed > 0 else 0
                if log_interval > 0 and rows_read % log_interval == 0:
                    logger.info(
                        "%s: %d / %s docs (%.0f docs/s)",
                        COLLECTION,
                        rows_read,
                        total_str,
                        rate,
                    )
                    last_log_t = now
                elif heartbeat_secs > 0 and (now - last_log_t) >= heartbeat_secs:
                    logger.info(
                        "%s: heartbeat — %d / %s docs (%.0f docs/s, %.1f min elapsed)",
                        COLLECTION,
                        rows_read,
                        total_str,
                        rate,
                        elapsed / 60,
                    )
                    last_log_t = now

        finally:
            client.close()

        node_rows = nodes.close()
        ancestor_rows = ancestors.close()
        edge_rows = edges.close()

        logger.info("%s: %d node rows, %d ancestor rows, %d edge rows", COLLECTION, node_rows, ancestor_rows, edge_rows)

        return JobResult(
            job_name=self.name,
            rows_read=rows_read,
            rows_written=node_rows + ancestor_rows + edge_rows,
            tables_written=("nmdc_graph_nodes", "nmdc_graph_nodes_type_ancestors", "nmdc_graph_edges"),
        )


@register(COLLECTION)
def _factory() -> AlldocsToParquetJob:
    mongo_uri = MongoSettings().uri
    out_root = LakehouseSettings().root
    return AlldocsToParquetJob(mongo_uri, out_root)
