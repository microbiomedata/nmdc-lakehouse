"""Tests for the schema_generator.

Uses the same hand-crafted LinkML schema shape as test_flatteners — keeps
the generator and the runtime flattener honest about producing the same
column names.
"""

from __future__ import annotations

import pytest
from linkml_runtime import SchemaView

from nmdc_lakehouse.transforms.schema_generator import (
    flatten_class_def,
    flatten_database_schema,
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
      associated_studies:
        range: Term
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


def test_flat_class_pipe_joins_multivalued_scalar(sv):
    """Multivalued scalar slots become a single string slot, with note."""
    flat = flatten_class_def(sv, "Record")
    tags = flat.attributes["tags"]
    assert tags.range == "string"
    assert "pipe-separated" in (tags.description or "")


def test_flat_class_treats_class_ref_as_scalar(sv):
    """Slot with class range and an identifier is a string ID column."""
    flat = flatten_class_def(sv, "Record")
    parent = flat.attributes["parent"]
    assert parent.range == "string"
    assert "Reference by identifier" in (parent.description or "")


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
