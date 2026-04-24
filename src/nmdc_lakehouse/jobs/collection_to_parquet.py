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

import os
from pathlib import Path

from nmdc_lakehouse.jobs.base import Job, JobResult
from nmdc_lakehouse.jobs.registry import register
from nmdc_lakehouse.sinks.parquet_sink import ParquetSink
from nmdc_lakehouse.sources.mongo_source import MongoSource
from nmdc_lakehouse.transforms.flatteners import SchemaDrivenFlattener


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
        """Stream records from MongoDB through the flattener into Parquet."""
        from importlib.util import find_spec

        from linkml_runtime import SchemaView

        from nmdc_lakehouse.transforms.schema_generator import flatten_class_def

        spec = find_spec("nmdc_schema")
        if spec is None or not spec.submodule_search_locations:
            raise RuntimeError("nmdc_schema package is not installed")
        schema_path = f"{spec.submodule_search_locations[0]}/nmdc_materialized_patterns.yaml"
        schema_view = SchemaView(schema_path)

        source = MongoSource(self.mongo_uri)
        flattener = SchemaDrivenFlattener(schema_view, self.root_class)
        flat_class = flatten_class_def(schema_view, self.root_class)
        sink = ParquetSink(self.out_root, class_def=flat_class)

        records = source.iter_records(self.collection)
        flat_rows = flattener.apply(records)

        if dry_run:
            rows_read = sum(1 for _ in flat_rows)
            return JobResult(job_name=self.name, rows_read=rows_read, rows_written=0, tables_written=())

        drop_empty = os.environ.get("LAKEHOUSE_DROP_EMPTY_COLS", "").lower() in ("1", "true", "yes")
        rows_read = 0

        def _counted(rows):
            nonlocal rows_read
            for row in rows:
                rows_read += 1
                yield row

        rows_written: int = sink.write(_counted(flat_rows), table=self.collection, drop_empty_cols=drop_empty) or 0
        return JobResult(
            job_name=self.name,
            rows_read=rows_read,
            rows_written=rows_written,
            tables_written=(self.collection,),
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
        total_read = total_written = 0
        tables: list[str] = []
        for name, root_class in _db_collection_map().items():
            if name in self.skip:
                continue
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
        mongo_uri = os.environ.get("MONGO_URI", "mongodb://localhost:27017/nmdc_lakehouse_prep")
        out_root = Path(os.environ.get("LAKEHOUSE_ROOT", "./local/parquet"))
        return CollectionToParquetJob(collection, root_class, mongo_uri, out_root)

    return _factory


# Register one job per Database slot at import time.
for _collection, _root_class in _db_collection_map().items():
    register(_collection)(_make_factory(_collection, _root_class))


@register("all-collections")
def _all_factory():
    mongo_uri = os.environ.get("MONGO_URI", "mongodb://localhost:27017/nmdc_lakehouse_prep")
    out_root = Path(os.environ.get("LAKEHOUSE_ROOT", "./local/parquet"))
    skip_raw = os.environ.get("LAKEHOUSE_SKIP_COLLECTIONS", "")
    skip = {s.strip() for s in skip_raw.split(",") if s.strip()}
    return AllCollectionsToParquetJob(mongo_uri, out_root, skip=skip)
