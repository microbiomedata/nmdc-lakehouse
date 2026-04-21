"""ETL job: flatten NMDC biosample_set and write to Parquet.

This is the first concrete job in the nmdc-lakehouse pipeline.
It wires together the three core components:

  MongoSource  →  SchemaDrivenFlattener  →  ParquetSink

and is registered under the name ``"biosamples-to-parquet"`` so it can be
dispatched via ``nmdc-lakehouse run-job biosamples-to-parquet``.
"""

from __future__ import annotations

import os
from pathlib import Path

from nmdc_lakehouse.jobs.base import Job, JobResult
from nmdc_lakehouse.jobs.registry import register
from nmdc_lakehouse.sinks.parquet_sink import ParquetSink
from nmdc_lakehouse.sources.mongo_source import MongoSource
from nmdc_lakehouse.transforms.flatteners import SchemaDrivenFlattener

_COLLECTION = "biosample_set"
_ROOT_CLASS = "Biosample"


@register("biosamples-to-parquet")
def _factory() -> "BiosampleToParquetJob":
    """Construct a BiosampleToParquetJob from environment variables."""
    mongo_uri = os.environ.get("MONGO_URI", "mongodb://localhost:27017/nmdc_lakehouse_prep")
    out_root = Path(os.environ.get("LAKEHOUSE_ROOT", "./local/parquet"))
    return BiosampleToParquetJob(mongo_uri=mongo_uri, out_root=out_root)


class BiosampleToParquetJob(Job):
    """Flatten all records in ``biosample_set`` and write them to Parquet.

    Reads from a local MongoDB instance (configured via ``MONGO_URI``),
    flattens with :class:`~nmdc_lakehouse.transforms.flatteners.SchemaDrivenFlattener`
    using the installed ``nmdc-schema``, and writes the result to
    ``{LAKEHOUSE_ROOT}/biosample_set.parquet``.

    The Arrow schema is derived from the nmdc-schema at job construction
    time so column types are stable across all batched writes and columns
    absent from individual records are written as null.
    """

    name = "biosamples-to-parquet"

    def __init__(self, mongo_uri: str, out_root: Path) -> None:
        """Construct the job.

        Args:
            mongo_uri: MongoDB connection URI including database name.
            out_root: Directory to write Parquet files into.
        """
        self.mongo_uri = mongo_uri
        self.out_root = out_root

    def run(self, *, dry_run: bool = False) -> JobResult:
        """Execute the job.

        Streams records from MongoDB through the flattener into Parquet.
        In dry-run mode, counts records but writes nothing.

        Args:
            dry_run: If True, measure without writing.

        Returns:
            A :class:`~nmdc_lakehouse.jobs.base.JobResult` with row counts.
        """
        from importlib.util import find_spec

        from linkml_runtime import SchemaView

        from nmdc_lakehouse.transforms.schema_generator import flatten_class_def

        spec = find_spec("nmdc_schema")
        if spec is None or not spec.submodule_search_locations:
            raise RuntimeError("nmdc_schema package is not installed")
        schema_path = f"{spec.submodule_search_locations[0]}/nmdc_materialized_patterns.yaml"
        schema_view = SchemaView(schema_path)

        # Stubs on main have different signatures; real implementations are in
        # PRs #4 (MongoSource), #7 (ParquetSink), #13 (SchemaDrivenFlattener).
        # mypy: type-ignore below silences stub mismatches until dependencies are merged.
        source = MongoSource(self.mongo_uri)  # type: ignore
        flattener = SchemaDrivenFlattener(schema_view, _ROOT_CLASS)
        flat_class = flatten_class_def(schema_view, _ROOT_CLASS)
        sink = ParquetSink(self.out_root, class_def=flat_class)  # type: ignore

        records = source.iter_records(_COLLECTION)
        flat_rows = flattener.apply(records)

        if dry_run:
            rows_read = sum(1 for _ in flat_rows)
            return JobResult(
                job_name=self.name,
                rows_read=rows_read,
                rows_written=0,
                tables_written=(),
            )

        drop_empty = os.environ.get("LAKEHOUSE_DROP_EMPTY_COLS", "").lower() in ("1", "true", "yes")
        rows_read = 0

        def _counted(rows):  # type: ignore[no-untyped-def]
            nonlocal rows_read
            for row in rows:
                rows_read += 1
                yield row

        rows_written: int = sink.write(_counted(flat_rows), table=_COLLECTION, drop_empty_cols=drop_empty) or 0  # type: ignore
        return JobResult(
            job_name=self.name,
            rows_read=rows_read,
            rows_written=rows_written,
            tables_written=(_COLLECTION,),
        )
