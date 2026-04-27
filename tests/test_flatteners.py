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
    side_table_rows,
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
      type:

  # Process hierarchy for testing polymorphic dispatch, modeled on
  # nmdc-schema's MaterialProcessing / Pooling pattern: a base class with
  # concrete subclasses that add their own distinct slots.
  Process:
    attributes:
      id:
        required: true
      type:
  Pooling:
    is_a: Process
    attributes:
      pooling_method:

  # A class that embeds a Process inline, for testing polymorphic dispatch
  # inside an inlined-object range.
  RecordWithInlinedProcess:
    attributes:
      id:
        required: true
      performed_step:
        range: Process
        inlined: true
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


def test_polymorphic_dispatch_root_class(sv):
    """A record's ``type`` field picks up subclass-specific slots."""
    out = flatten_record(
        {"id": "p1", "type": "test:Pooling", "pooling_method": "serial"},
        sv,
        "Process",
    )
    # pooling_method is defined on Pooling, not Process — dispatch picks it up.
    assert out["pooling_method"] == "serial"
    assert out["id"] == "p1"


def test_polymorphic_dispatch_bare_type_name(sv):
    """``type`` values without a colon prefix still resolve."""
    out = flatten_record(
        {"id": "p1", "type": "Pooling", "pooling_method": "parallel"},
        sv,
        "Process",
    )
    assert out["pooling_method"] == "parallel"


def test_polymorphic_dispatch_unknown_type_falls_back(sv):
    """Unknown type values fall back to the declared root class."""
    out = flatten_record(
        {"id": "p1", "type": "nmdc:MysteriousSubclass", "pooling_method": "serial"},
        sv,
        "Process",
    )
    # Pooling-specific slot is dropped because we fell back to Process.
    assert "pooling_method" not in out
    assert out["id"] == "p1"


def test_polymorphic_dispatch_in_inlined_object(sv):
    """A subclass declared via ``type`` on an inlined object is dispatched.

    Mirrors the nmdc-schema pattern of an inlined slot whose declared range
    is a base class (e.g. MaterialProcessing) but whose value is a concrete
    subclass (e.g. Pooling) that defines its own slots. The subclass slot
    should appear in the flat output as ``<slot>_<subclass_slot>``.
    """
    out = flatten_record(
        {
            "id": "r1",
            "performed_step": {
                "id": "step-1",
                "type": "test:Pooling",
                "pooling_method": "serial",
            },
        },
        sv,
        "RecordWithInlinedProcess",
    )
    # pooling_method is on the subclass only — dispatch picks it up via
    # the inlined expansion, producing performed_step_pooling_method.
    assert out["performed_step_id"] == "step-1"
    assert out["performed_step_pooling_method"] == "serial"


# ── side_table_rows ────────────────────────────────────────────────────────────


def test_side_table_scalar_multivalued(sv):
    """Scalar multivalued slots produce one junction row per element."""
    rows = list(side_table_rows({"id": "r1", "tags": ["a", "b"]}, sv, "Record", "record_set"))
    assert rows == [
        ("record_set_tags", {"parent_id": "r1", "tags": "a"}),
        ("record_set_tags", {"parent_id": "r1", "tags": "b"}),
    ]


def test_side_table_ref_class_multivalued(sv):
    """Ref-class multivalued slots produce one junction row per referenced ID."""
    rows = list(
        side_table_rows(
            {"id": "r1", "associated_studies": ["nmdc:sty-11-a", "nmdc:sty-11-b"]},
            sv,
            "Record",
            "record_set",
        )
    )
    assert rows == [
        ("record_set_associated_studies", {"parent_id": "r1", "associated_studies": "nmdc:sty-11-a"}),
        ("record_set_associated_studies", {"parent_id": "r1", "associated_studies": "nmdc:sty-11-b"}),
    ]


def test_side_table_inlined_class_multivalued(sv):
    """Inlined-class multivalued slots produce one flattened child row per element."""
    rows = list(
        side_table_rows(
            {
                "id": "r1",
                "chem_admin": [
                    {"has_raw_value": "formaldehyde"},
                    {"has_raw_value": "methanol"},
                ],
            },
            sv,
            "Record",
            "record_set",
        )
    )
    tables = [t for t, _ in rows]
    assert all(t == "record_set_chem_admin" for t in tables)
    child_rows = [r for _, r in rows]
    assert child_rows[0] == {"has_raw_value": "formaldehyde", "parent_id": "r1"}
    assert child_rows[1] == {"has_raw_value": "methanol", "parent_id": "r1"}


def test_side_table_no_multivalued_slots(sv):
    """Records with no multivalued slots emit no side table rows."""
    rows = list(side_table_rows({"id": "r1", "name": "x"}, sv, "Record", "record_set"))
    assert rows == []


def test_side_table_missing_slot_skipped(sv):
    """Slots absent from the record are silently skipped."""
    rows = list(side_table_rows({"id": "r1"}, sv, "Record", "record_set"))
    assert rows == []


def test_side_table_empty_list_skipped(sv):
    """Empty list values produce no rows."""
    rows = list(side_table_rows({"id": "r1", "tags": []}, sv, "Record", "record_set"))
    assert rows == []


def test_side_table_preserves_non_string_types(sv):
    """Integer values in side table rows are not cast to string."""
    rows = list(side_table_rows({"id": "r1", "scores": [1, 2, 3]}, sv, "Record", "record_set"))
    assert rows == [
        ("record_set_scores", {"parent_id": "r1", "scores": 1}),
        ("record_set_scores", {"parent_id": "r1", "scores": 2}),
        ("record_set_scores", {"parent_id": "r1", "scores": 3}),
    ]


def test_side_table_dispatch_outside_hierarchy_falls_back(sv):
    """A record whose type is outside the root_class hierarchy falls back to root_class slots."""
    # Process is not a descendant of Record — Extraction has extraction_targets (multivalued).
    # Without the hierarchy guard, side_table_rows would iterate Extraction slots and emit
    # record_set_extraction_targets rows that have no ClassDef in side_table_class_defs.
    rows = list(
        side_table_rows(
            {"id": "r1", "type": "test:Extraction", "extraction_targets": ["x"]},
            sv,
            "Record",
            "record_set",
        )
    )
    table_names = {t for t, _ in rows}
    assert "record_set_extraction_targets" not in table_names
