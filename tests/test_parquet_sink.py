"""Tests for ParquetSink and class_def_to_arrow_schema."""

from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from linkml_runtime import SchemaView
from linkml_runtime.linkml_model import ClassDefinition, SlotDefinition

from nmdc_lakehouse.sinks.parquet_sink import (
    _SPARK_SCHEMA_KEY,
    ParquetSink,
    StreamingWriter,
    class_def_to_arrow_schema,
)
from nmdc_lakehouse.transforms.schema_generator import (
    DEFAULT_FLATTENED_SCHEMA_ID,
    flatten_class_def,
    side_table_class_defs,
)

TARGET_SCHEMA_ID = DEFAULT_FLATTENED_SCHEMA_ID
PRIMARY_MAPPING = "nmdc_lakehouse.transforms.flatteners.SchemaDrivenFlattener"


@pytest.fixture
def flat_schema_view() -> SchemaView:
    """A minimal source schema with table and column descriptions."""
    return SchemaView("""
id: https://example.org/test
name: test
version: 1.2.3
prefixes:
  linkml: https://w3id.org/linkml/
imports:
  - linkml:types
classes:
  FlatRecord:
    description: A flattened test record.
    attributes:
      id:
        range: string
        identifier: true
        description: Stable record identifier.
      depth_has_numeric_value:
        range: float
        description: Numeric depth value.
      depth_has_unit:
        range: string
      count:
        range: integer
      active:
        range: boolean
""")


@pytest.fixture
def flat_class(flat_schema_view: SchemaView) -> ClassDefinition:
    """A minimal flat ClassDefinition with mixed ranges."""
    return flat_schema_view.get_class("FlatRecord")


@pytest.fixture
def array_class() -> ClassDefinition:
    """A ClassDefinition with multivalued (ARRAY) slots of various element types."""
    sv = SchemaView("""
id: https://example.org/test
name: test
prefixes:
  linkml: https://w3id.org/linkml/
imports:
  - linkml:types
classes:
  ArrayRecord:
    attributes:
      id:
        range: string
        required: true
      tags:
        range: string
        multivalued: true
      scores:
        range: integer
        multivalued: true
      associated_studies:
        range: string
        multivalued: true
""")
    return sv.get_class("ArrayRecord")


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


def test_write_persists_schema_metadata_in_parquet_footer(flat_schema_view, flat_class, tmp_path):
    """The file footer carries stable table, schema, mapping, and field metadata."""
    sink = ParquetSink(
        tmp_path,
        class_def=flat_class,
        source_schema=flat_schema_view.schema,
        source_class="FlatRecord",
        target_schema_id=TARGET_SCHEMA_ID,
        target_schema_version="1.2.3+flat.1.0.0",
        mapping=PRIMARY_MAPPING,
    )
    sink.write(iter([{"id": "r1", "depth_has_numeric_value": 1.5}]), table="flat_record")

    schema = pq.ParquetFile(tmp_path / "flat_record.parquet").schema_arrow
    # `schema.metadata` builds a fresh dict on every access, so take one copy and edit that.
    footer = dict(schema.metadata)
    # Checked separately, because its value is a whole rendered schema rather than a label.
    assert footer.pop(_SPARK_SCHEMA_KEY, None) is not None, "the Spark schema must reach the footer"
    assert footer == {
        b"nmdc_lakehouse.footer_metadata_format_version": b"2",
        b"nmdc_lakehouse.table_description": b"A flattened test record.",
        b"nmdc_lakehouse.source_schema_id": b"https://example.org/test",
        b"nmdc_lakehouse.source_schema_version": b"1.2.3",
        b"nmdc_lakehouse.source_class": b"FlatRecord",
        b"nmdc_lakehouse.target_schema_id": TARGET_SCHEMA_ID.encode(),
        # An artifact that cannot name the flat schema that produced it is what issue 293 is
        # about. Two schemas from the same nmdc-schema release but different flattener code are
        # indistinguishable without this.
        b"nmdc_lakehouse.target_schema_version": b"1.2.3+flat.1.0.0",
        b"nmdc_lakehouse.target_class": b"FlatRecord",
        b"nmdc_lakehouse.mapping": PRIMARY_MAPPING.encode(),
    }
    assert schema.field("depth_has_numeric_value").metadata == {
        b"nmdc_lakehouse.description": b"Numeric depth value.",
        b"nmdc_lakehouse.linkml_range": b"float",
    }
    assert schema.field("id").metadata[b"nmdc_lakehouse.identifier"] == b"true"


def test_primary_footer_retains_generated_flattening_description(tmp_path):
    """Generated nested-slot provenance reaches a primary Parquet footer."""
    sv = SchemaView("""
id: https://example.org/nested
name: nested
version: 2.0.0
prefixes:
  linkml: https://w3id.org/linkml/
imports:
  - linkml:types
classes:
  Record:
    description: A source record.
    attributes:
      id:
        identifier: true
        range: string
      depth:
        range: Quantity
        inlined: true
  Quantity:
    attributes:
      has_raw_value:
        range: float
        description: Original numeric value.
""")
    flat_class = flatten_class_def(sv, "Record")
    sink = ParquetSink(
        tmp_path,
        class_def=flat_class,
        source_schema=sv.schema,
        source_class="Record",
        target_schema_id=TARGET_SCHEMA_ID,
        mapping=PRIMARY_MAPPING,
    )
    sink.write(iter([{"id": "r1", "depth_has_raw_value": 3.5}]), table="record_set")

    schema = pq.ParquetFile(tmp_path / "record_set.parquet").schema_arrow
    assert schema.metadata[b"nmdc_lakehouse.table_description"].startswith(b"A source record.")
    description = schema.field("depth_has_raw_value").metadata[b"nmdc_lakehouse.description"]
    assert b"Original numeric value" in description
    assert b"Flattened from nested slot 'depth.has_raw_value'" in description


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
    """Writing zero rows with a known schema writes a zero-row Parquet file."""
    sink = ParquetSink(tmp_path, class_def=flat_class)
    total = sink.write(iter([]), table="flat_record")
    assert total == 0
    out = tmp_path / "flat_record.parquet"
    assert out.exists()
    import pyarrow.parquet as _pq

    tbl = _pq.read_table(out)
    assert len(tbl) == 0
    assert "id" in tbl.schema.names


def test_write_empty_input_preserves_footer_metadata(flat_schema_view, flat_class, tmp_path):
    """A schema-only Parquet file retains table and column descriptions."""
    sink = ParquetSink(
        tmp_path,
        class_def=flat_class,
        source_schema=flat_schema_view.schema,
        source_class="FlatRecord",
        target_schema_id=TARGET_SCHEMA_ID,
        mapping=PRIMARY_MAPPING,
    )
    sink.write(iter([]), table="flat_record")

    schema = pq.ParquetFile(tmp_path / "flat_record.parquet").schema_arrow
    assert schema.metadata[b"nmdc_lakehouse.source_schema_version"] == b"1.2.3"
    assert schema.field("id").metadata[b"nmdc_lakehouse.description"] == b"Stable record identifier."


def test_write_empty_input_no_schema(tmp_path):
    """Writing zero rows without a schema produces no file (schema unknown)."""
    sink = ParquetSink(tmp_path)
    total = sink.write(iter([]), table="flat_record")
    assert total == 0
    assert not (tmp_path / "flat_record.parquet").exists()


def test_arrow_schema_multivalued_becomes_list_type(array_class):
    """Multivalued slots map to pa.list_(element_type) in the Arrow schema."""
    schema = class_def_to_arrow_schema(array_class)
    field_map = {f.name: f.type for f in schema}
    assert field_map["tags"] == pa.list_(pa.string())
    assert field_map["scores"] == pa.list_(pa.int64())
    assert field_map["associated_studies"] == pa.list_(pa.string())
    assert field_map["id"] == pa.string()  # single-valued — not a list


def test_write_array_columns_roundtrip(array_class, tmp_path):
    """ARRAY columns survive a write/read roundtrip with correct values."""
    sink = ParquetSink(tmp_path, class_def=array_class)
    rows = [
        {"id": "r1", "tags": ["a", "b"], "scores": [1, 2, 3]},
        {"id": "r2", "tags": ["x"]},
        {"id": "r3"},  # all array cols null
    ]
    total = sink.write(iter(rows), table="array_record")
    assert total == 3
    tbl = pq.read_table(tmp_path / "array_record.parquet")
    assert tbl.schema.field("tags").type == pa.list_(pa.string())
    assert tbl.schema.field("scores").type == pa.list_(pa.int64())
    assert tbl.column("tags").to_pylist() == [["a", "b"], ["x"], None]
    assert tbl.column("scores").to_pylist() == [[1, 2, 3], None, None]


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


def test_drop_empty_cols_preserves_footer_metadata(flat_schema_view, flat_class, tmp_path):
    """Column pruning retains metadata on the table and retained fields."""
    sink = ParquetSink(
        tmp_path,
        class_def=flat_class,
        source_schema=flat_schema_view.schema,
        source_class="FlatRecord",
        target_schema_id=TARGET_SCHEMA_ID,
        mapping=PRIMARY_MAPPING,
    )
    sink.write(iter([{"id": "r1"}]), table="flat_record", drop_empty_cols=True)

    schema = pq.ParquetFile(tmp_path / "flat_record.parquet").schema_arrow
    assert schema.metadata[b"nmdc_lakehouse.target_schema_id"] == TARGET_SCHEMA_ID.encode()
    assert schema.field("id").metadata[b"nmdc_lakehouse.description"] == b"Stable record identifier."


def test_side_table_writers_persist_reference_and_child_metadata(tmp_path):
    """Reference and inlined-child side-table footers retain generated descriptions."""
    sv = SchemaView("""
id: https://example.org/side-tables
name: side_tables
version: 4.5.6
prefixes:
  linkml: https://w3id.org/linkml/
imports:
  - linkml:types
classes:
  Record:
    attributes:
      id:
        identifier: true
        range: string
      related:
        range: Related
        multivalued: true
        description: Related record identifiers.
      children:
        range: Child
        multivalued: true
        inlined: true
  Related:
    attributes:
      id:
        identifier: true
        range: string
  Child:
    description: A nested child record.
    attributes:
      label:
        range: string
        description: Child label.
""")
    defs = dict(side_table_class_defs(sv, "Record", "record_set"))
    rows = {
        "record_set_related": {"parent_id": "r1", "related": "r2"},
        "record_set_children": {"parent_id": "r1", "label": "child"},
    }

    for table_name, row in rows.items():
        schema = class_def_to_arrow_schema(
            defs[table_name],
            source_schema=sv.schema,
            source_class="Record",
            target_schema_id=TARGET_SCHEMA_ID,
            mapping="nmdc_lakehouse.transforms.flatteners.side_table_rows",
        )
        writer = StreamingWriter(tmp_path / f"{table_name}.parquet", schema)
        writer.append(row)
        assert writer.close() == 1

    reference_schema = pq.ParquetFile(tmp_path / "record_set_related.parquet").schema_arrow
    assert reference_schema.metadata[b"nmdc_lakehouse.table_description"].startswith(b"References from")
    assert b"Related record identifiers" in reference_schema.field("related").metadata[b"nmdc_lakehouse.description"]

    child_schema = pq.ParquetFile(tmp_path / "record_set_children.parquet").schema_arrow
    assert child_schema.metadata[b"nmdc_lakehouse.table_description"].startswith(b"A nested child record.")
    assert child_schema.field("label").metadata[b"nmdc_lakehouse.description"] == b"Child label."
    assert (
        child_schema.field("parent_id").metadata[b"nmdc_lakehouse.description"].startswith(b"Identifier of the parent")
    )


def test_drop_empty_cols_removes_all_empty_array_columns(array_class, tmp_path):
    """drop_empty_cols=True strips ARRAY columns where every row has [] or null."""
    sink = ParquetSink(tmp_path, class_def=array_class)
    rows = [{"id": "r1", "tags": ["a"]}, {"id": "r2"}]
    sink.write(iter(rows), table="array_record", drop_empty_cols=True)
    tbl = pq.read_table(tmp_path / "array_record.parquet")
    assert "id" in tbl.schema.names
    assert "tags" in tbl.schema.names  # has data
    assert "scores" not in tbl.schema.names  # all null/empty
    assert "associated_studies" not in tbl.schema.names  # all null/empty


@pytest.fixture
def flat_class_with_designators() -> ClassDefinition:
    """A flat ClassDefinition with an identifier slot and a designates_type slot."""
    sv = SchemaView("""
id: https://example.org/test
name: test
prefixes:
  linkml: https://w3id.org/linkml/
imports:
  - linkml:types
classes:
  DesignatorRecord:
    attributes:
      id:
        range: string
        identifier: true
      type:
        range: string
        designates_type: true
      required_value:
        range: string
        required: true
      has_raw_value:
        range: string
""")
    return sv.get_class("DesignatorRecord")


def test_drop_empty_cols_keeps_required_identifier_and_designates_type_columns(flat_class_with_designators, tmp_path):
    """Contract columns survive even when null in every row of this run.

    Regression test for microbiomedata/nmdc-lakehouse#123: these columns are
    part of the schema's contract (e.g. the polymorphic dispatch key), not a
    property of what happened to be populated in one dataset.
    """
    sink = ParquetSink(tmp_path, class_def=flat_class_with_designators)
    rows = [{"has_raw_value": "x"}, {"has_raw_value": "y"}]  # id and type both entirely null
    sink.write(iter(rows), table="designator_record", drop_empty_cols=True)
    tbl = pq.read_table(tmp_path / "designator_record.parquet")
    assert "id" in tbl.schema.names
    assert "type" in tbl.schema.names
    assert "required_value" in tbl.schema.names
    assert "has_raw_value" in tbl.schema.names


def test_custom_linkml_types_resolve_to_their_base_arrow_type() -> None:
    """A range naming a custom type must not silently fall back to string."""
    from linkml_runtime.linkml_model import SchemaDefinition, TypeDefinition

    from nmdc_lakehouse.sinks.parquet_sink import _arrow_type_for_range

    schema = SchemaDefinition(id="https://example.org/t", name="t")
    schema.types["decimal_degree"] = TypeDefinition(name="decimal_degree", base="float", uri="xsd:decimal")
    schema.types["bytes"] = TypeDefinition(name="bytes", base="int", uri="xsd:long")
    schema.types["external_identifier"] = TypeDefinition(name="external_identifier", typeof="uriorcurie")

    assert _arrow_type_for_range("decimal_degree", schema) == pa.float64()
    assert _arrow_type_for_range("bytes", schema) == pa.int64()
    assert _arrow_type_for_range("external_identifier", schema) == pa.string()
    assert _arrow_type_for_range("string", schema) == pa.string()


def test_enum_ranges_and_unknown_ranges_remain_strings() -> None:
    from linkml_runtime.linkml_model import SchemaDefinition

    from nmdc_lakehouse.sinks.parquet_sink import _arrow_type_for_range

    schema = SchemaDefinition(id="https://example.org/t", name="t")

    assert _arrow_type_for_range("FileTypeEnum", schema) == pa.string()
    assert _arrow_type_for_range("SomethingUndefined", schema) == pa.string()
    assert _arrow_type_for_range("decimal_degree", None) == pa.string()


def test_the_footer_carries_a_spark_schema_whose_comments_are_the_slot_descriptions() -> None:
    """Spark reads its own schema from this key, so descriptions can arrive with the data; #258."""
    import json

    class_def = ClassDefinition(
        name="biosample_set",
        description="A biosample table.",
        attributes={
            "id": SlotDefinition(name="id", range="string", description="Stable identifier", identifier=True),
            "depth": SlotDefinition(name="depth", range="double", description="Depth in metres"),
            "flags": SlotDefinition(name="flags", range="string", multivalued=True, description="Free-text flags"),
            "undocumented": SlotDefinition(name="undocumented", range="integer"),
        },
    )

    schema = class_def_to_arrow_schema(class_def)
    spark = json.loads(schema.metadata[_SPARK_SCHEMA_KEY].decode())

    assert spark["type"] == "struct"
    by_name = {field["name"]: field for field in spark["fields"]}
    assert by_name["id"]["metadata"]["comment"] == "Stable identifier"
    assert by_name["depth"]["type"] == "double"
    assert by_name["flags"]["type"] == {"type": "array", "elementType": "string", "containsNull": True}
    assert by_name["flags"]["metadata"]["comment"] == "Free-text flags"
    # A slot with no description carries no comment rather than an empty one, so Spark does not
    # create a column described as "".
    assert by_name["undocumented"]["metadata"] == {}
    assert [field["name"] for field in spark["fields"]] == [field.name for field in schema]


def test_the_spark_schema_survives_the_parquet_writer_into_the_footer(tmp_path) -> None:
    """The Arrow schema is not the contract; what lands in the Parquet footer is."""
    import json

    class_def = ClassDefinition(
        name="study_set",
        attributes={"id": SlotDefinition(name="id", range="string", description="Stable identifier")},
    )
    schema = class_def_to_arrow_schema(class_def)
    target = tmp_path / "study_set.parquet"
    pq.write_table(pa.table({"id": pa.array(["nmdc:sty-1"], type=pa.string())}, schema=schema), target)

    footer = pq.ParquetFile(target).metadata.metadata

    assert _SPARK_SCHEMA_KEY in footer, "Spark reads the footer, so an Arrow-only schema would be inert"
    parsed = json.loads(footer[_SPARK_SCHEMA_KEY].decode())
    assert parsed["fields"][0]["metadata"]["comment"] == "Stable identifier"


def test_an_unmapped_arrow_type_raises_rather_than_emitting_a_schema_spark_cannot_read() -> None:
    """The mapping is exhaustive on purpose; a silent default would produce a wrong Spark schema."""
    import pyarrow as pa_local
    import pytest

    from nmdc_lakehouse.sinks.parquet_sink import _spark_type

    with pytest.raises(ValueError, match="No Spark type mapping"):
        _spark_type(pa_local.timestamp("us"))


def test_dropping_empty_columns_rebuilds_the_spark_schema(flat_schema_view, flat_class, tmp_path) -> None:
    """Spark trusts this entry as the file schema, so it must never name a dropped column."""
    import json

    sink = ParquetSink(
        tmp_path,
        class_def=flat_class,
        source_schema=flat_schema_view.schema,
        source_class="FlatRecord",
        target_schema_id=TARGET_SCHEMA_ID,
        mapping=PRIMARY_MAPPING,
    )
    sink.write(iter([{"id": "r1", "depth_has_numeric_value": 1.5}]), table="flat_record", drop_empty_cols=True)

    written = pq.ParquetFile(tmp_path / "flat_record.parquet")
    columns = written.schema_arrow.names
    spark = json.loads(written.metadata.metadata[_SPARK_SCHEMA_KEY].decode())

    assert [field["name"] for field in spark["fields"]] == columns, (
        "the Spark schema must describe the columns the file actually holds"
    )
    # And the surviving columns keep their descriptions, which is the point of the entry.
    described = {field["name"]: field["metadata"].get("comment") for field in spark["fields"]}
    assert described["id"] == "Stable record identifier."
