"""Tests for the schema_generator.

Uses a hand-crafted LinkML schema that exercises the shapes needed to
validate generated flattened column names and related slot behavior.
"""

from __future__ import annotations

import pytest
from linkml_runtime import SchemaView

from nmdc_lakehouse.transforms.flatteners import side_table_rows
from nmdc_lakehouse.transforms.schema_generator import (
    flatten_class_def,
    flatten_database_schema,
    side_table_class_defs,
)

_SCHEMA_YAML = """
id: https://example.org/test
name: test_schema
prefixes:
  linkml: https://w3id.org/linkml/
imports:
  - linkml:types
default_range: string

classes:
  Term:
    attributes:
      id:
        identifier: true
        required: true
      name:

  ControlledTermValue:
    attributes:
      has_raw_value:
      term:
        range: Term

  TextValue:
    attributes:
      has_raw_value:

  Process:
    attributes:
      id:
        required: true
      type:
  Pooling:
    is_a: Process
    attributes:
      pooling_method:
  Extraction:
    is_a: Process
    attributes:
      extraction_targets:
        multivalued: true

  Record:
    attributes:
      id:
        required: true
      name:
      depth:
      tags:
        multivalued: true
      env_broad_scale:
        range: ControlledTermValue
        inlined: true
      description:
        range: TextValue
        inlined: true
      chem_admin:
        range: ControlledTermValue
        multivalued: true
        inlined: true
      associated_studies:
        range: Term
        multivalued: true
      scores:
        range: integer
        multivalued: true
      parent:
        range: Term

  Database:
    tree_root: true
    attributes:
      record_set:
        range: Record
        multivalued: true
      process_set:
        range: Process
        multivalued: true
"""


@pytest.fixture
def sv() -> SchemaView:
    return SchemaView(_SCHEMA_YAML)


def test_flat_class_includes_scalar_slots(sv):
    """Scalar slots become flat slots with the same range."""
    flat = flatten_class_def(sv, "Record")
    assert "id" in flat.attributes
    assert flat.attributes["id"].range == "string"
    assert "name" in flat.attributes


def test_flat_class_multivalued_scalar_is_array(sv):
    """Multivalued scalar slots have multivalued=True and retain their declared range."""
    flat = flatten_class_def(sv, "Record")
    tags = flat.attributes["tags"]
    assert tags.multivalued is True
    assert tags.range == "string"  # default_range in test schema
    assert "pipe-separated" not in (tags.description or "")


def test_flat_class_treats_class_ref_as_scalar(sv):
    """Slot with class range and an identifier is a string ID column."""
    flat = flatten_class_def(sv, "Record")
    parent = flat.attributes["parent"]
    assert parent.range == "string"
    assert parent.multivalued is False
    assert "Reference by identifier" in (parent.description or "")


def test_flat_class_multivalued_ref_is_array_of_strings(sv):
    """Multivalued ref-class slot becomes multivalued=True with string range."""
    flat = flatten_class_def(sv, "Record")
    assoc = flat.attributes["associated_studies"]
    assert assoc.range == "string"
    assert assoc.multivalued is True


def test_flat_class_expands_inlined_object(sv):
    """Single-valued inlined class slot expands to <slot>_<subslot>."""
    flat = flatten_class_def(sv, "Record")
    assert "description_has_raw_value" in flat.attributes
    assert "env_broad_scale_has_raw_value" in flat.attributes
    # Two-level expansion through controlled term's term.id
    assert "env_broad_scale_term_id" in flat.attributes


def test_flat_class_unions_subclass_slots(sv):
    """Polymorphic dispatch: subclass slots appear on the base flat class."""
    flat = flatten_class_def(sv, "Process")
    # Pooling-only and Extraction-only slots both present
    assert "pooling_method" in flat.attributes
    assert "extraction_targets" in flat.attributes
    # Subclass slots are non-required (sparse columns)
    assert flat.attributes["pooling_method"].required is False


def test_flat_class_subclass_slots_carry_dispatch_note(sv):
    """Subclass slots are tagged with the source subclass in description."""
    flat = flatten_class_def(sv, "Process")
    desc = flat.attributes["pooling_method"].description or ""
    assert "Pooling" in desc


def test_flatten_database_schema_yields_one_class_per_collection(sv):
    """Walking Database produces one flat class per multivalued slot."""
    out = flatten_database_schema(sv, database_class="Database")
    assert "RecordFlat" in out.classes
    assert "ProcessFlat" in out.classes


# Note: a generator/runtime consistency test ("every column flatten_record
# emits exists in the generated class") will be added in a follow-up once
# both #6 (flattener) and #12 (this) have landed and both helpers are in
# the same source tree.


# ── side_table_class_defs ─────────────────────────────────────────────────────


def test_side_table_scalar_no_classdef(sv):
    """Scalar multivalued slots produce no side table ClassDef (ARRAY in primary)."""
    defs = dict(side_table_class_defs(sv, "Record", "record_set"))
    assert "record_set_tags" not in defs


def test_side_table_ref_class_junction(sv):
    """Ref-class multivalued slot produces a two-column junction ClassDef."""
    defs = dict(side_table_class_defs(sv, "Record", "record_set"))
    assert "record_set_associated_studies" in defs
    cls = defs["record_set_associated_studies"]
    assert "parent_id" in cls.attributes
    assert "associated_studies" in cls.attributes
    # ref type: value column is string (ID)
    assert cls.attributes["associated_studies"].range == "string"


def test_side_table_inlined_class_child(sv):
    """Inlined-class multivalued slot produces child-object ClassDef with parent_id."""
    defs = dict(side_table_class_defs(sv, "Record", "record_set"))
    assert "record_set_chem_admin" in defs
    cls = defs["record_set_chem_admin"]
    assert "parent_id" in cls.attributes
    # ControlledTermValue slots appear in the child schema
    assert "has_raw_value" in cls.attributes


def test_side_table_single_valued_slots_excluded(sv):
    """Single-valued slots do not produce side tables."""
    defs = dict(side_table_class_defs(sv, "Record", "record_set"))
    assert "record_set_name" not in defs
    assert "record_set_depth" not in defs
    assert "record_set_parent" not in defs


def test_side_table_excludes_descendant_scalar_slots(sv):
    """Subclass-specific scalar multivalued slots have no side table (ARRAY in primary)."""
    defs = dict(side_table_class_defs(sv, "Process", "process_set"))
    # extraction_targets is scalar multivalued on Extraction — no side table
    assert "process_set_extraction_targets" not in defs


def test_side_table_output_sorted(sv):
    """Output is sorted by table name for deterministic ordering."""
    defs = side_table_class_defs(sv, "Record", "record_set")
    names = [t for t, _ in defs]
    assert names == sorted(names)


def test_side_table_scalar_integer_no_classdef(sv):
    """Integer scalar multivalued slots produce no side table ClassDef (ARRAY in primary)."""
    defs = dict(side_table_class_defs(sv, "Record", "record_set"))
    assert "record_set_scores" not in defs


def test_side_table_schema_covers_runtime_row_keys(sv):
    """Every key emitted by side_table_rows must appear in the ClassDef from side_table_class_defs.

    Regression guard: if side_table_rows emits a key that has no matching
    attribute in the ClassDef, ParquetSink will silently drop or mistype that
    column. (The reverse direction — schema columns never emitted at runtime —
    is not checked here; those become nullable nulls, which is harmless.)
    """
    defs = dict(side_table_class_defs(sv, "Record", "record_set"))

    # Omit the nested term dict inside chem_admin — non-inlined class-range
    # slots inside a side-table child use the ref-as-string path in the schema
    # but the two-level expansion path in the runtime (a known open gap, not
    # tested here).
    record = {
        "id": "r1",
        "chem_admin": [
            {"has_raw_value": "NaCl"},
        ],
        "associated_studies": ["study:1", "study:2"],
    }

    for table_name, row in side_table_rows(record, sv, "Record", "record_set"):
        assert table_name in defs, f"side_table_rows emitted unknown table {table_name!r}"
        schema_cols = {attr for attr in defs[table_name].attributes}
        extra = set(row.keys()) - schema_cols
        assert not extra, f"side_table_rows emitted keys {extra} not in ClassDef for {table_name!r}"
