"""Tests for ParquetSink and class_def_to_arrow_schema."""

from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from linkml_runtime import SchemaView
from linkml_runtime.linkml_model import ClassDefinition

from nmdc_lakehouse.sinks.parquet_sink import ParquetSink, class_def_to_arrow_schema


@pytest.fixture
def flat_class() -> ClassDefinition:
    """A minimal flat ClassDefinition with mixed ranges."""
    sv = SchemaView("""
id: https://example.org/test
name: test
prefixes:
  linkml: https://w3id.org/linkml/
imports:
  - linkml:types
classes:
  FlatRecord:
    attributes:
      id:
        range: string
        required: true
      depth_has_numeric_value:
        range: float
      depth_has_unit:
        range: string
      count:
        range: integer
      active:
        range: boolean
""")
    return sv.get_class("FlatRecord")


def test_arrow_schema_types(flat_class, tmp_path):
    """class_def_to_arrow_schema maps ranges to Arrow types correctly."""
    schema = class_def_to_arrow_schema(flat_class)
    field_map = {f.name: f.type for f in schema}
    assert field_map["id"] == pa.string()
    assert field_map["depth_has_numeric_value"] == pa.float64()
    assert field_map["depth_has_unit"] == pa.string()
    assert field_map["count"] == pa.int64()
    assert field_map["active"] == pa.bool_()


def test_arrow_schema_all_nullable(flat_class):
    """All fields are nullable (sparse columns for polymorphic data)."""
    schema = class_def_to_arrow_schema(flat_class)
    assert all(f.nullable for f in schema)


def test_write_produces_parquet_file(flat_class, tmp_path):
    """write() creates a parquet file at {root}/{table}.parquet."""
    sink = ParquetSink(tmp_path, class_def=flat_class)
    rows = [{"id": f"r{i}", "depth_has_numeric_value": float(i)} for i in range(5)]
    total = sink.write(iter(rows), table="flat_record")
    assert total == 5
    assert (tmp_path / "flat_record.parquet").exists()


def test_write_roundtrip(flat_class, tmp_path):
    """Rows written can be read back with correct values."""
    sink = ParquetSink(tmp_path, class_def=flat_class)
    rows = [
        {"id": "r1", "depth_has_numeric_value": 0.5, "depth_has_unit": "m", "count": 3},
        {"id": "r2", "depth_has_unit": "cm"},
    ]
    sink.write(iter(rows), table="flat_record")
    tbl = pq.read_table(tmp_path / "flat_record.parquet")
    assert tbl.num_rows == 2
    assert tbl.schema.field("id").type == pa.string()
    assert tbl.schema.field("depth_has_numeric_value").type == pa.float64()
    assert tbl.column("id").to_pylist() == ["r1", "r2"]
    assert tbl.column("depth_has_unit").to_pylist() == ["m", "cm"]


def test_missing_columns_written_as_null(flat_class, tmp_path):
    """Columns absent from a row are written as null when a schema is set."""
    sink = ParquetSink(tmp_path, class_def=flat_class)
    sink.write(iter([{"id": "r1"}]), table="flat_record")
    tbl = pq.read_table(tmp_path / "flat_record.parquet")
    assert tbl.column("depth_has_numeric_value")[0].as_py() is None
    assert tbl.column("count")[0].as_py() is None


def test_write_batches_large_input(flat_class, tmp_path):
    """Input larger than batch_size is written in multiple row groups."""
    sink = ParquetSink(tmp_path, class_def=flat_class, batch_size=10)
    rows = [{"id": f"r{i}"} for i in range(35)]
    total = sink.write(iter(rows), table="flat_record")
    assert total == 35
    tbl = pq.read_table(tmp_path / "flat_record.parquet")
    assert tbl.num_rows == 35


def test_write_without_class_def_infers_schema(tmp_path):
    """Without a class_def, schema is inferred from data."""
    sink = ParquetSink(tmp_path)
    rows = [{"id": "r1", "name": "biosample"}]
    sink.write(iter(rows), table="inferred")
    tbl = pq.read_table(tmp_path / "inferred.parquet")
    assert tbl.num_rows == 1
    assert set(tbl.schema.names) == {"id", "name"}


def test_write_empty_input(flat_class, tmp_path):
    """Writing zero rows produces no file."""
    sink = ParquetSink(tmp_path, class_def=flat_class)
    total = sink.write(iter([]), table="flat_record")
    assert total == 0
    assert not (tmp_path / "flat_record.parquet").exists()


def test_drop_empty_cols_removes_all_null_columns(flat_class, tmp_path):
    """drop_empty_cols=True strips columns that are null in every row."""
    sink = ParquetSink(tmp_path, class_def=flat_class)
    rows = [{"id": "r1", "depth_has_unit": "m"}, {"id": "r2", "depth_has_unit": "cm"}]
    sink.write(iter(rows), table="flat_record", drop_empty_cols=True)
    tbl = pq.read_table(tmp_path / "flat_record.parquet")
    assert "id" in tbl.schema.names
    assert "depth_has_unit" in tbl.schema.names
    # depth_has_numeric_value, count, active were never set — should be dropped
    assert "depth_has_numeric_value" not in tbl.schema.names
    assert "count" not in tbl.schema.names
    assert "active" not in tbl.schema.names
