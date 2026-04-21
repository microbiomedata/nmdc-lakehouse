"""Tests for the SchemaDrivenFlattener and flatten_record function.

Uses a small hand-crafted LinkML schema so the tests run without any
external dependency on nmdc-schema or a live MongoDB. Covers each branch
of the decision tree documented in ``flatten_record``.
"""

from __future__ import annotations

import pytest
from linkml_runtime import SchemaView

from nmdc_lakehouse.transforms.flatteners import (
    SchemaDrivenFlattener,
    flatten_record,
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
      parent:
        range: Term
"""


@pytest.fixture
def sv() -> SchemaView:
    """SchemaView over the test schema."""
    return SchemaView(_SCHEMA_YAML)


def test_scalar_passthrough(sv):
    """Scalar slots pass their value through unchanged."""
    out = flatten_record({"id": "r1", "name": "first", "depth": 3.5}, sv, "Record")
    assert out == {"id": "r1", "name": "first", "depth": 3.5}


def test_multivalued_scalar_pipe_joined(sv):
    """Multivalued scalar slots are joined with '|'."""
    out = flatten_record({"id": "r1", "tags": ["a", "b", "c"]}, sv, "Record")
    assert out["tags"] == "a|b|c"


def test_single_valued_scalar_not_wrapped_in_list(sv):
    """A non-list value for a multivalued slot still works (wrapped into one)."""
    out = flatten_record({"id": "r1", "tags": "lonely"}, sv, "Record")
    assert out["tags"] == "lonely"


def test_inlined_object_expanded_one_level(sv):
    """Single-valued inlined object expands to <slot>_<subslot> columns."""
    out = flatten_record(
        {"id": "r1", "description": {"has_raw_value": "notes here"}},
        sv,
        "Record",
    )
    assert out["description_has_raw_value"] == "notes here"
    assert "description" not in out


def test_inlined_object_expanded_two_levels(sv):
    """Two-level nesting (env_broad_scale.term.id) flattens to underscore-joined."""
    out = flatten_record(
        {
            "id": "r1",
            "env_broad_scale": {
                "has_raw_value": "ENVO:01000253",
                "term": {"id": "ENVO:01000253", "name": "freshwater river biome"},
            },
        },
        sv,
        "Record",
    )
    assert out["env_broad_scale_has_raw_value"] == "ENVO:01000253"
    assert out["env_broad_scale_term_id"] == "ENVO:01000253"
    assert out["env_broad_scale_term_name"] == "freshwater river biome"


def test_multivalued_class_ref_pipe_joined(sv):
    """Multivalued class-range slot without inlined=True treats values as ID refs."""
    out = flatten_record(
        {"id": "r1", "associated_studies": ["nmdc:sty-11-a", "nmdc:sty-11-b"]},
        sv,
        "Record",
    )
    assert out["associated_studies"] == "nmdc:sty-11-a|nmdc:sty-11-b"


def test_single_class_ref_passthrough(sv):
    """Single-valued class-range slot without inlined=True is a scalar ID."""
    out = flatten_record({"id": "r1", "parent": "nmdc:t-xyz"}, sv, "Record")
    assert out["parent"] == "nmdc:t-xyz"


def test_multivalued_inlined_object_dropped(sv):
    """Multivalued inlined objects are skipped (helper tables are a follow-up)."""
    out = flatten_record(
        {
            "id": "r1",
            "chem_admin": [
                {"has_raw_value": "formaldehyde [CHEBI:16842];2022-01-01"},
                {"has_raw_value": "methanol [CHEBI:17790];2022-01-02"},
            ],
        },
        sv,
        "Record",
    )
    assert "chem_admin" not in out
    assert out["id"] == "r1"


def test_missing_slots_omitted(sv):
    """Slots not present on the input are omitted from the output (no None padding)."""
    out = flatten_record({"id": "r1"}, sv, "Record")
    assert out == {"id": "r1"}


def test_none_value_omitted(sv):
    """Explicit None values are treated as absent."""
    out = flatten_record({"id": "r1", "name": None, "depth": 1.0}, sv, "Record")
    assert "name" not in out
    assert out["depth"] == 1.0


def test_flattener_apply_yields_one_per_record(sv):
    """SchemaDrivenFlattener.apply yields one flat dict per input record."""
    flattener = SchemaDrivenFlattener(sv, "Record")
    records = [
        {"id": "r1", "name": "first"},
        {"id": "r2", "name": "second"},
    ]
    out = list(flattener.apply(records))
    assert out == [{"id": "r1", "name": "first"}, {"id": "r2", "name": "second"}]


def test_boolean_scalars_stringified_in_list(sv):
    """Booleans inside multivalued scalar lists are rendered as 'true'/'false'."""
    # Not directly supported by the test schema, but _stringify is called for
    # multivalued scalars — add a minimal case using tags.
    out = flatten_record({"id": "r1", "tags": [True, False]}, sv, "Record")
    assert out["tags"] == "true|false"
