"""Parquet sink — write flat row dictionaries to local Parquet files."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable, Iterator

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from linkml_runtime.linkml_model import ClassDefinition, SchemaDefinition

DEFAULT_BATCH_SIZE = 10_000

# Mapping from LinkML / XSD range names to Arrow types.
# Anything not listed defaults to string.
_RANGE_TO_ARROW: dict[str, pa.DataType] = {
    "integer": pa.int64(),
    "int": pa.int64(),
    "long": pa.int64(),
    "float": pa.float64(),
    "double": pa.float64(),
    "decimal": pa.float64(),
    "boolean": pa.bool_(),
    "string": pa.string(),
    "uriorcurie": pa.string(),
    "uri": pa.string(),
    "ncname": pa.string(),
    "date": pa.string(),
    "datetime": pa.string(),
}

_METADATA_PREFIX = "nmdc_lakehouse."
FOOTER_METADATA_FORMAT_VERSION = "1"


def _encoded_metadata(values: dict[str, str | None]) -> dict[bytes, bytes]:
    """Encode non-empty metadata values for Arrow and Parquet schemas."""
    return {f"{_METADATA_PREFIX}{key}".encode(): value.encode() for key, value in values.items() if value}


def class_def_to_arrow_schema(
    class_def: ClassDefinition,
    *,
    source_schema: SchemaDefinition | None = None,
    source_class: str | None = None,
    target_schema_id: str | None = None,
    mapping: str | None = None,
) -> pa.Schema:
    """Derive a PyArrow schema from a LinkML ClassDefinition.

    Each attribute becomes a nullable Arrow field. The range is mapped via
    ``_RANGE_TO_ARROW``; unknown ranges default to string. Multivalued slots
    become ``pa.list_(element_type)``.

    Args:
        class_def: A ``ClassDefinition`` produced by
            :func:`nmdc_lakehouse.transforms.schema_generator.flatten_class_def`.
        source_schema: LinkML schema from which the projection was generated.
        source_class: Root LinkML class projected into this table.
        target_schema_id: Stable identifier for the generated target schema.
        mapping: Stable identity of the mapping used to produce table rows.

    Returns:
        A ``pa.Schema`` with ``id`` first (if present), then remaining fields
        in alphabetical order.
    """
    fields = []
    # id first, then alphabetical — makes row browsers easier to use.
    names = sorted(class_def.attributes)
    if "id" in names:
        names = ["id"] + [n for n in names if n != "id"]
    for name in names:
        slot = class_def.attributes[name]
        range_name = slot.range or "string"
        element_type = _RANGE_TO_ARROW.get(range_name, pa.string())
        arrow_type = pa.list_(element_type) if slot.multivalued else element_type
        field_metadata = _encoded_metadata(
            {
                "description": slot.description,
                "linkml_range": range_name,
                "identifier": "true" if slot.identifier else None,
                "designates_type": "true" if slot.designates_type else None,
            }
        )
        fields.append(pa.field(name, arrow_type, nullable=True, metadata=field_metadata))
    schema_metadata = _encoded_metadata(
        {
            "footer_metadata_format_version": FOOTER_METADATA_FORMAT_VERSION,
            "table_description": class_def.description,
            "source_schema_id": source_schema.id if source_schema else None,
            "source_schema_version": source_schema.version if source_schema else None,
            "source_class": source_class,
            "target_schema_id": target_schema_id,
            "target_class": class_def.name,
            "mapping": mapping,
        }
    )
    return pa.schema(fields, metadata=schema_metadata)


class StreamingWriter:
    """Batch-buffered incremental Parquet writer for a single table.

    Unlike :class:`ParquetSink`, which consumes a complete iterable, this
    class accepts rows one at a time via :meth:`append` and flushes row groups
    as the buffer fills. Call :meth:`close` to flush the remainder and
    finalise the file. Intended for side tables that are populated
    concurrently with the primary stream so their rows can be written to disk
    as they arrive rather than buffered in memory for the entire run.
    """

    def __init__(self, path: Path, schema: pa.Schema, batch_size: int = DEFAULT_BATCH_SIZE) -> None:
        """Open a streaming Parquet writer at ``path`` using ``schema``."""
        self._path = path
        self._schema = schema
        self._batch_size = batch_size
        self._buffer: list[dict] = []
        self._writer: pq.ParquetWriter | None = None
        self._total = 0

    def append(self, row: dict) -> None:
        """Buffer one row; flush a row group to disk when the batch is full."""
        self._buffer.append(row)
        if len(self._buffer) >= self._batch_size:
            self._flush()

    def close(self) -> int:
        """Flush remaining rows and close the file. Returns total rows written."""
        self._flush()
        if self._writer is not None:
            self._writer.close()
            self._writer = None
        return self._total

    def _flush(self) -> None:
        if not self._buffer:
            return
        arrow_table = _rows_to_arrow_table(self._buffer, self._schema)
        if self._writer is None:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            self._writer = pq.ParquetWriter(self._path, arrow_table.schema)
        self._writer.write_table(arrow_table)
        self._total += len(self._buffer)
        self._buffer = []


class ParquetSink:
    """Write flat row dicts to Parquet files under a root directory.

    Matches the ``Sink`` protocol: ``write(rows, table=...)`` consumes
    an iterator of flat dicts and writes ``{root}/{table}.parquet``.

    Rows are buffered in batches of ``batch_size`` before each write so memory
    use stays bounded regardless of collection size. An Arrow schema is derived
    from a ``ClassDefinition`` at construction time so column types are stable
    across all batches; a column absent from a row is written as null.
    """

    def __init__(
        self,
        root: str | Path,
        class_def: ClassDefinition | None = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
        *,
        source_schema: SchemaDefinition | None = None,
        source_class: str | None = None,
        target_schema_id: str | None = None,
        mapping: str | None = None,
    ) -> None:
        """Construct a ParquetSink.

        Args:
            root: Local filesystem directory to write into. Object-store URIs
                (e.g. ``s3://...``) are not supported by this implementation.
            class_def: Optional flat ``ClassDefinition`` used to derive the
                Arrow schema. When provided, column types are stable and
                columns absent from a row are written as null. When ``None``,
                the schema is inferred from the first batch (simpler but types
                may vary across collections).
            batch_size: Number of rows to buffer before flushing a row group.
            source_schema: LinkML schema from which the projection was generated.
            source_class: Root LinkML class projected into this table.
            target_schema_id: Stable identifier for the generated target schema.
            mapping: Stable identity of the mapping used to produce table rows.
        """
        self.root = Path(root)
        self.class_def = class_def
        self.batch_size = batch_size
        self._arrow_schema: pa.Schema | None = (
            class_def_to_arrow_schema(
                class_def,
                source_schema=source_schema,
                source_class=source_class,
                target_schema_id=target_schema_id,
                mapping=mapping,
            )
            if class_def is not None
            else None
        )

    def write(self, rows: Iterable[dict], *, table: str, drop_empty_cols: bool = False) -> int:
        """Write ``rows`` to ``{root}/{table}.parquet``.

        Streams rows through in batches; the file is finalised and closed when
        the iterator is exhausted.

        Args:
            rows: Iterable of flat dicts (as produced by
                :meth:`nmdc_lakehouse.transforms.flatteners.SchemaDrivenFlattener.apply`).
            table: Logical table name; becomes the parquet filename stem.
            drop_empty_cols: When True, rewrite the file after writing to
                remove columns that are entirely null. Useful for wide sparse
                schemas (e.g. BiosampleFlat with 1,398 columns) where most
                columns are empty for a given dataset. Columns backed by a
                required slot, `identifier: true`, or `designates_type: true` are
                never dropped, even when empty in this dataset — their
                presence is part of the schema's contract, not a property
                of this run. See microbiomedata/nmdc-lakehouse#123.

        Returns:
            Total number of rows written.
        """
        self.root.mkdir(parents=True, exist_ok=True)
        out_path = self.root / f"{table}.parquet"
        total = 0
        writer: pq.ParquetWriter | None = None

        try:
            for batch in _batched(rows, self.batch_size):
                arrow_table = self._to_arrow_table(batch)
                if writer is None:
                    writer = pq.ParquetWriter(out_path, arrow_table.schema)
                writer.write_table(arrow_table)
                total += len(batch)
            if writer is None and self._arrow_schema is not None:
                pq.write_table(
                    pa.table(
                        {f.name: pa.array([], type=f.type) for f in self._arrow_schema}, schema=self._arrow_schema
                    ),
                    out_path,
                )
        finally:
            if writer is not None:
                writer.close()

        if drop_empty_cols and out_path.exists() and total > 0:
            tbl = pq.read_table(out_path)
            protected = self._protected_columns()
            keep = [name for name in tbl.schema.names if name in protected or _col_has_data(tbl.column(name))]
            if len(keep) < len(tbl.schema.names):
                pq.write_table(tbl.select(keep), out_path)

        return total

    def _to_arrow_table(self, rows: list[dict]) -> pa.Table:
        return _rows_to_arrow_table(rows, self._arrow_schema)

    def _protected_columns(self) -> set[str]:
        """Column names that must survive `drop_empty_cols` even when entirely null.

        A column backed by a required slot, `identifier: true`, or
        `designates_type: true` is part of the schema's contract, not just data
        that happened to be present in this run.
        """
        if self.class_def is None:
            return set()
        return {
            name
            for name, slot in self.class_def.attributes.items()
            if slot.required or slot.identifier or slot.designates_type
        }


def _rows_to_arrow_table(rows: list[dict], schema: pa.Schema | None) -> pa.Table:
    """Convert a list of flat dicts to a PyArrow Table.

    When ``schema`` is provided, missing columns are filled with nulls so every
    row group has the same shape. Without a schema the type is inferred from
    the batch (column types may differ across batches).
    """
    if schema is not None:
        columns: dict[str, list] = {name: [] for name in schema.names}
        for row in rows:
            for name in schema.names:
                val = row.get(name)
                field_type = schema.field(name).type
                columns[name].append(_coerce(val, field_type))
        arrays = [pa.array(columns[name], type=schema.field(name).type) for name in schema.names]
        return pa.table(dict(zip(schema.names, arrays, strict=True)), schema=schema)
    return pa.Table.from_pylist(rows)


def _col_has_data(col: pa.ChunkedArray) -> bool:
    """Return True if the column has at least one non-null, non-empty value.

    For list columns a row whose value is [] has null_count==0 but carries no
    data; uses list_value_length to avoid materialising all elements.
    """
    if pa.types.is_list(col.type):
        lengths = pc.list_value_length(col)
        return pc.any(pc.greater(pc.fill_null(lengths, 0), 0)).as_py()
    return col.null_count < len(col)


def _coerce(value: object, arrow_type: pa.DataType) -> object:
    """Coerce a Python value to be compatible with ``arrow_type``.

    Arrow is strict about types; real NMDC data sometimes has numeric values
    in slots declared as string (e.g. ``depth_has_raw_value = 0.5``). When
    the value is incompatible with a string column, stringify it. For list
    columns, wrap non-list values and recursively coerce elements.
    """
    if value is None:
        return None
    if pa.types.is_list(arrow_type):
        if not isinstance(value, list):
            value = [value]
        elem_type = arrow_type.value_type
        return [_coerce(v, elem_type) for v in value]
    if arrow_type == pa.string() and not isinstance(value, str):
        return str(value)
    return value


def _batched(iterable: Iterable[dict], size: int) -> Iterator[list[dict]]:
    """Yield successive lists of up to ``size`` items from ``iterable``."""
    batch: list[dict] = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch
