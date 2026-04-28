"""Generic ETL job: flatten any schema-specified NMDC collection to Parquet.

Registers one named job per ``Database`` slot (e.g. ``biosample_set``,
``study_set``, ...) plus ``all-collections`` which runs all of them in
sequence. Collection→root-class mapping is derived from the installed
``nmdc-schema`` at import time using the same pyyaml approach as
``scripts/python/schema_collections.py`` — no linkml import at module load.

Usage::

    nmdc-lakehouse run-job biosample_set
    nmdc-lakehouse run-job study_set
    nmdc-lakehouse run-job all-collections [--drop-empty-cols]
"""

from __future__ import annotations

import logging
import os
import time
from pathlib import Path

from nmdc_lakehouse.config import LakehouseSettings, MongoSettings
from nmdc_lakehouse.jobs.base import Job, JobResult
from nmdc_lakehouse.jobs.registry import register
from nmdc_lakehouse.sinks.parquet_sink import ParquetSink
from nmdc_lakehouse.sources.mongo_source import MongoSource
from nmdc_lakehouse.transforms.flatteners import SchemaDrivenFlattener

logger = logging.getLogger(__name__)


def _db_collection_map() -> dict[str, str]:
    """Return {collection_name: root_class} from the installed nmdc-schema.

    Uses pyyaml only (no linkml import chain) so this is fast at module load.
    """
    from importlib.util import find_spec

    import yaml

    spec = find_spec("nmdc_schema")
    if spec is None or not spec.submodule_search_locations:
        return {}
    schema_path = Path(spec.submodule_search_locations[0]) / "nmdc_materialized_patterns.yaml"
    schema = yaml.safe_load(schema_path.read_text())
    db_slots = schema["classes"]["Database"].get("slots", []) or []
    top_slots = schema.get("slots", {})
    return {name: top_slots[name]["range"] for name in db_slots if name in top_slots and "range" in top_slots[name]}


class CollectionToParquetJob(Job):
    """Flatten one schema-specified NMDC collection and write to Parquet."""

    def __init__(self, collection: str, root_class: str, mongo_uri: str, out_root: Path) -> None:
        """Construct the job for a single collection."""
        self.collection = collection
        self.root_class = root_class
        self.mongo_uri = mongo_uri
        self.out_root = out_root
        self.name = collection

    def run(self, *, dry_run: bool = False) -> JobResult:
        """Stream records from MongoDB through the flattener into Parquet.

        Side tables (for ref-class and inlined-class multivalued slots) are
        buffered in memory during the primary stream and written after the
        primary table is complete. Scalar multivalued slots are stored as
        native Parquet ARRAY columns in the primary table and do not produce
        side tables.
        Memory usage scales with the total number of side-table rows, not the
        number of primary records. ``functional_annotation_agg`` uses
        ``DirectMongoToParquetJob`` because it is too large for this path.
        """
        from importlib.util import find_spec

        from linkml_runtime import SchemaView

        from nmdc_lakehouse.transforms.flatteners import side_table_rows
        from nmdc_lakehouse.transforms.schema_generator import (
            flatten_class_def,
            side_table_class_defs,
        )

        spec = find_spec("nmdc_schema")
        if spec is None or not spec.submodule_search_locations:
            raise RuntimeError("nmdc_schema package is not installed")
        schema_path = f"{spec.submodule_search_locations[0]}/nmdc_materialized_patterns.yaml"
        schema_view = SchemaView(schema_path)

        source = MongoSource(self.mongo_uri)
        flattener = SchemaDrivenFlattener(schema_view, self.root_class)
        flat_class = flatten_class_def(schema_view, self.root_class)
        sink = ParquetSink(self.out_root, class_def=flat_class)

        # Buffer for side table rows accumulated during the primary stream.
        side_buffer: dict[str, list[dict]] = {}

        def _tee_side_tables(raw_records):
            for record in raw_records:
                for table_name, row in side_table_rows(record, schema_view, self.root_class, self.collection):
                    side_buffer.setdefault(table_name, []).append(row)
                yield record

        records = _tee_side_tables(source.iter_records(self.collection))
        flat_rows = flattener.apply(records)

        if dry_run:
            rows_read = sum(1 for _ in flat_rows)
            return JobResult(job_name=self.name, rows_read=rows_read, rows_written=0, tables_written=())

        drop_empty = os.environ.get("LAKEHOUSE_DROP_EMPTY_COLS", "").lower() in ("1", "true", "yes")
        log_interval = int(os.environ.get("LAKEHOUSE_LOG_INTERVAL", "10000"))
        heartbeat_secs = int(os.environ.get("LAKEHOUSE_HEARTBEAT_SECS", "60"))
        total = source.estimated_count(self.collection)
        total_str = f"~{total:,}" if total else "?"
        logger.info("%s: starting (~%s records)", self.collection, total_str)
        rows_read = 0
        first_row_logged = False
        t0 = time.monotonic()
        last_log_t = t0

        def _counted(rows):
            nonlocal rows_read, first_row_logged, last_log_t
            for row in rows:
                rows_read += 1
                now = time.monotonic()
                if not first_row_logged:
                    first_row_logged = True
                    logger.info("%s: first row received", self.collection)
                    last_log_t = now
                elapsed = now - t0
                rate = rows_read / elapsed if elapsed > 0 else 0
                if log_interval > 0 and rows_read % log_interval == 0:
                    logger.info(
                        "%s: %s / %s rows (%.0f rows/s)",
                        self.collection,
                        f"{rows_read:,}",
                        total_str,
                        rate,
                    )
                    last_log_t = now
                elif heartbeat_secs > 0 and (now - last_log_t) >= heartbeat_secs:
                    logger.info(
                        "%s: heartbeat — %s / %s rows (%.0f rows/s, %.1f min elapsed)",
                        self.collection,
                        f"{rows_read:,}",
                        total_str,
                        rate,
                        elapsed / 60,
                    )
                    last_log_t = now
                yield row

        rows_written: int = sink.write(_counted(flat_rows), table=self.collection, drop_empty_cols=drop_empty) or 0

        # Write side tables.
        side_defs = dict(side_table_class_defs(schema_view, self.root_class, self.collection))
        side_tables: list[str] = []
        for table_name, side_rows in side_buffer.items():
            if not side_rows:
                continue
            cd = side_defs.get(table_name)
            if cd is None:
                logger.warning("%s: no ClassDef for side table %s — skipping", self.collection, table_name)
                continue
            side_sink = ParquetSink(self.out_root, class_def=cd)
            n = side_sink.write(iter(side_rows), table=table_name) or 0
            logger.info("%s: wrote %d rows to side table %s", self.collection, n, table_name)
            side_tables.append(table_name)

        side_tables.sort()
        return JobResult(
            job_name=self.name,
            rows_read=rows_read,
            rows_written=rows_written,
            tables_written=(self.collection, *side_tables),
        )


class AllCollectionsToParquetJob(Job):
    """Run CollectionToParquetJob for every schema-specified collection."""

    name = "all-collections"

    def __init__(self, mongo_uri: str, out_root: Path, skip: set[str] | None = None) -> None:
        """Construct the job.

        Args:
            mongo_uri: MongoDB connection URI including database name.
            out_root: Directory to write Parquet files into.
            skip: Collection names to exclude from the run.
        """
        self.mongo_uri = mongo_uri
        self.out_root = out_root
        self.skip = skip or set()

    def run(self, *, dry_run: bool = False) -> JobResult:
        """Run each collection job in sequence and aggregate results."""
        from nmdc_lakehouse.jobs.direct_mongo_to_parquet import DIRECT_COLLECTIONS, DirectMongoToParquetJob

        total_read = total_written = 0
        tables: list[str] = []
        for name, root_class in _db_collection_map().items():
            if name in self.skip:
                continue
            job: Job
            if name in DIRECT_COLLECTIONS:
                job = DirectMongoToParquetJob(name, root_class, self.mongo_uri, self.out_root)
            else:
                job = CollectionToParquetJob(name, root_class, self.mongo_uri, self.out_root)
            result = job.run(dry_run=dry_run)
            total_read += result.rows_read
            total_written += result.rows_written
            tables.extend(result.tables_written)
        return JobResult(
            job_name=self.name,
            rows_read=total_read,
            rows_written=total_written,
            tables_written=tuple(tables),
        )


def _make_factory(collection: str, root_class: str):
    def _factory():
        mongo_uri = MongoSettings().uri
        out_root = LakehouseSettings().root
        return CollectionToParquetJob(collection, root_class, mongo_uri, out_root)

    return _factory


# Import triggers registration of DIRECT_COLLECTIONS and is the single source
# of truth for which collections use the direct path.
from nmdc_lakehouse.jobs.direct_mongo_to_parquet import DIRECT_COLLECTIONS as _DIRECT_COLLECTIONS  # noqa: E402

# Register one job per Database slot at import time.
for _collection, _root_class in _db_collection_map().items():
    if _collection in _DIRECT_COLLECTIONS:
        continue
    register(_collection)(_make_factory(_collection, _root_class))


@register("all-collections")
def _all_factory():
    mongo_uri = MongoSettings().uri
    out_root = LakehouseSettings().root
    skip_raw = os.environ.get("LAKEHOUSE_SKIP_COLLECTIONS", "")
    skip = {s.strip() for s in skip_raw.split(",") if s.strip()}
    return AllCollectionsToParquetJob(mongo_uri, out_root, skip=skip)
