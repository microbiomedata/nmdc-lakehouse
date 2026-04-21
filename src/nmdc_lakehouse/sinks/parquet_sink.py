"""Parquet sink — write flat row dicts to partitioned Parquet files."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable, Iterator

import pyarrow as pa
import pyarrow.parquet as pq
from linkml_runtime.linkml_model import ClassDefinition

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


def class_def_to_arrow_schema(class_def: ClassDefinition) -> pa.Schema:
    """Derive a PyArrow schema from a (flat) LinkML ClassDefinition.

    Each attribute becomes a nullable Arrow field. The range is mapped
    via ``_RANGE_TO_ARROW``; unknown ranges default to string.

    Args:
        class_def: A flat ``ClassDefinition`` whose attributes have scalar
            ranges (as produced by :func:`nmdc_lakehouse.transforms.schema_generator.flatten_class_def`).

    Returns:
        A ``pa.Schema`` with one field per attribute, in alphabetical order.
    """
    fields = []
    for name in sorted(class_def.attributes):
        slot = class_def.attributes[name]
        range_name = slot.range or "string"
        arrow_type = _RANGE_TO_ARROW.get(range_name, pa.string())
        fields.append(pa.field(name, arrow_type, nullable=True))
    return pa.schema(fields)


class ParquetSink:
    """Write flat row dicts to Parquet files under a root directory.

    Matches Sierra's ``Sink`` protocol: ``write(rows, table=...)`` consumes
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
    ) -> None:
        """Construct a ParquetSink.

        Args:
            root: Directory (or object-store URI prefix) to write into.
            class_def: Optional flat ``ClassDefinition`` used to derive the
                Arrow schema. When provided, column types are stable and
                columns absent from a row are written as null. When ``None``,
                the schema is inferred from the first batch (simpler but types
                may vary across collections).
            batch_size: Number of rows to buffer before flushing a row group.
        """
        self.root = Path(root)
        self.class_def = class_def
        self.batch_size = batch_size
        self._arrow_schema: pa.Schema | None = class_def_to_arrow_schema(class_def) if class_def is not None else None

    def write(self, rows: Iterable[dict], *, table: str) -> int:
        """Write ``rows`` to ``{root}/{table}.parquet``.

        Streams rows through in batches; the file is finalised and closed when
        the iterator is exhausted.

        Args:
            rows: Iterable of flat dicts (as produced by
                :func:`nmdc_lakehouse.transforms.flatteners.flatten_record`).
            table: Logical table name; becomes the parquet filename stem.

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
        finally:
            if writer is not None:
                writer.close()

        return total

    def _to_arrow_table(self, rows: list[dict]) -> pa.Table:
        """Convert a list of flat dicts to a PyArrow Table.

        When a fixed schema is available, missing columns are filled with nulls
        so every row group has the same shape. Without a fixed schema, the schema
        is inferred from the batch (column types may differ across batches).
        """
        if self._arrow_schema is not None:
            columns: dict[str, list] = {name: [] for name in self._arrow_schema.names}
            for row in rows:
                for name in self._arrow_schema.names:
                    val = row.get(name)
                    field_type = self._arrow_schema.field(name).type
                    columns[name].append(_coerce(val, field_type))
            arrays = [
                pa.array(columns[name], type=self._arrow_schema.field(name).type) for name in self._arrow_schema.names
            ]
            return pa.table(dict(zip(self._arrow_schema.names, arrays, strict=True)), schema=self._arrow_schema)
        return pa.Table.from_pylist(rows)


def _coerce(value: object, arrow_type: pa.DataType) -> object:
    """Coerce a value to be compatible with ``arrow_type``.

    Real NMDC data sometimes has numeric values in slots declared as string
    (e.g. ``depth_has_raw_value = 0.5``). Stringify non-string values for
    string columns; leave everything else to Arrow's native coercion.
    """
    if value is None:
        return None
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
