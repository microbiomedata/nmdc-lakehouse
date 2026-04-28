"""Flatteners that convert nested LinkML objects into tabular rows."""

from __future__ import annotations

from typing import Any, Iterable, Iterator

from linkml_runtime import SchemaView
from linkml_runtime.linkml_model import ClassDefinition, SlotDefinition


def flatten_record(record: dict, schema_view: SchemaView, root_class: str) -> dict:
    """Flatten a single nested record into a flat dict.

    Decision tree, schema-driven (each slot of the effective class):

    - scalar slot, not multivalued: pass the value through unchanged
    - scalar slot, multivalued: emit a native Python list
    - class range, not inlined (reference by ID): treat the value(s) as
      scalar identifiers — pass through or emit as list
    - class range, inlined, not multivalued: expand the nested object's
      scalar slots as ``<slot>_<subslot>`` columns (one level deep)
    - class range, inlined, multivalued: SKIPPED in this pass (child
      side tables capture these rows)

    Polymorphism: when a record has a ``type`` field naming a subclass of
    ``root_class`` (or of an inlined slot's declared range), the concrete
    subclass's induced slots are used instead. So a record with
    ``type: "nmdc:Pooling"`` flattened against ``MaterialProcessing``
    picks up Pooling-specific slots automatically.

    Values not present on the input record are omitted from the output
    (no None-padding) — consumers such as pyarrow will still see consistent
    column sets across rows because the schema is the source of truth.

    Args:
        record: A dict representation of a ``root_class`` instance.
        schema_view: Loaded LinkML SchemaView for the schema defining ``root_class``.
        root_class: Name of the root class to flatten against. If the
            record declares a concrete subclass via its ``type`` field,
            that subclass's slots are used.

    Returns:
        A flat dict. Keys are slot names (or ``<slot>_<subslot>`` for
        expanded inlined objects). Multivalued slots are Python lists.
    """
    out: dict[str, Any] = {}
    effective_class = _dispatch_class(record, root_class, schema_view)
    slots = schema_view.class_induced_slots(effective_class)
    for slot in slots:
        if slot.name not in record:
            continue
        value = record[slot.name]
        if value is None:
            continue

        range_class = _range_class(slot, schema_view)

        # Class ranges that aren't inlined are references — IDs as scalars or lists.
        if range_class is not None and not _is_inlined(slot, schema_view):
            if slot.multivalued and not isinstance(value, list):
                value = [value]
            out[slot.name] = value
            continue

        if range_class is None:
            # Scalar range.
            if slot.multivalued and not isinstance(value, list):
                value = [value]
            out[slot.name] = value
            continue

        # Class range, inlined.
        if slot.multivalued:
            # Helper-table case; deferred.
            continue

        # Single-valued inlined object: expand to <slot>_<subslot>.
        if isinstance(value, dict):
            for sub_key, sub_value in _expand_inlined(value, range_class, schema_view).items():
                out[f"{slot.name}_{sub_key}"] = sub_value

    return out


def _range_class(slot: SlotDefinition, schema_view: SchemaView) -> ClassDefinition | None:
    """Return the class range of a slot, or None if the range is a type/enum."""
    if not slot.range:
        return None
    cls = schema_view.get_class(slot.range)
    return cls


def _is_inlined(slot: SlotDefinition, schema_view: SchemaView) -> bool:
    """Decide whether a class-range slot should be treated as inlined.

    LinkML's default when ``slot.inlined`` is unset: ranges with an
    ``identifier`` slot are referenced by ID (not inlined); ranges without
    one are embedded (inlined). Most NMDC ``*Value`` classes (PersonValue,
    QuantityValue, ControlledIdentifiedTermValue, GeolocationValue, ...)
    have no identifier and are therefore inlined by this default, even
    though the materialized schema leaves ``slot.inlined`` as ``None``.
    """
    if slot.inlined is not None:
        return slot.inlined
    if not slot.range:
        return False
    if schema_view.get_class(slot.range) is None:
        return False
    return schema_view.get_identifier_slot(slot.range) is None


def _dispatch_class(record: dict, declared_class: str, schema_view: SchemaView) -> str:
    """Resolve the concrete class name for a record via its ``type`` field.

    Accepts LinkML-style class URIs (``nmdc:Pooling``) or bare names
    (``Pooling``). Falls back to ``declared_class`` if the record has no
    ``type`` field or the type doesn't resolve to a known class.

    This intentionally does NOT verify that the resolved class is a
    descendant of ``declared_class`` — if the record's ``type`` points
    at a known class, we use it. Data-quality issues (wrong type) are
    reported by a separate QC job.
    """
    type_uri = record.get("type")
    if not isinstance(type_uri, str) or not type_uri:
        return declared_class
    name = type_uri.rsplit(":", 1)[-1]
    if schema_view.get_class(name) is not None:
        return name
    return declared_class


def _expand_inlined(value: dict, class_def: ClassDefinition, schema_view: SchemaView) -> dict[str, Any]:
    """Flatten a single-valued inlined object one level deep.

    Scalar subslots pass through; multivalued scalars become lists. Nested
    classes inside the inlined object are expanded one more level (so
    ``env_broad_scale.term.id`` becomes ``env_broad_scale_term_id``), then
    anything deeper is dropped. This matches the common NMDC shape where
    controlled terms are exactly two levels deep (slot → term → id/name).
    """
    out: dict[str, Any] = {}
    effective_class = _dispatch_class(value, class_def.name, schema_view)
    induced = schema_view.class_induced_slots(effective_class)
    for sub_slot in induced:
        if sub_slot.name == "type":
            continue
        if sub_slot.name not in value:
            continue
        sub_value = value[sub_slot.name]
        if sub_value is None:
            continue
        sub_range = _range_class(sub_slot, schema_view)
        if sub_range is None:
            # Scalar.
            if sub_slot.multivalued and not isinstance(sub_value, list):
                sub_value = [sub_value]
            out[sub_slot.name] = sub_value
            continue
        # One more level of nesting allowed: expand scalar subslots of this inner class.
        if not sub_slot.multivalued and isinstance(sub_value, dict):
            inner_class = _dispatch_class(sub_value, sub_range.name, schema_view)
            inner_induced = schema_view.class_induced_slots(inner_class)
            for inner_slot in inner_induced:
                if inner_slot.name == "type" or inner_slot.name not in sub_value:
                    continue
                inner_value = sub_value[inner_slot.name]
                if inner_value is None:
                    continue
                if _range_class(inner_slot, schema_view) is not None:
                    # Three levels deep — dropped.
                    continue
                if inner_slot.multivalued and not isinstance(inner_value, list):
                    inner_value = [inner_value]
                out[f"{sub_slot.name}_{inner_slot.name}"] = inner_value
    return out


def side_table_rows(
    record: dict,
    schema_view: SchemaView,
    root_class: str,
    collection: str,
) -> Iterator[tuple[str, dict]]:
    """Yield ``(table_name, row_dict)`` for every side table row from one record.

    Two slot types produce side table rows:

    - **ref_class multivalued** (class range, not inlined) — one
      ``(parent_id, <slot_name>)`` junction row per referenced ID.
    - **inlined_class multivalued** — one flattened child-object row per element,
      with ``parent_id`` prepended.

    Scalar multivalued slots are stored as native Parquet ARRAY columns in the
    primary table and do NOT produce side table rows.

    ``table_name`` is ``{collection}_{slot_name}`` in all cases.

    Args:
        record: A dict representation of a ``root_class`` instance.
        schema_view: Loaded LinkML SchemaView.
        root_class: Declared class for ``record`` (polymorphic dispatch is
            applied via the ``type`` field).
        collection: Collection name (e.g. ``biosample_set``) — used as the
            table name prefix.
    """
    effective_class = _dispatch_class(record, root_class, schema_view)
    # Constrain dispatch to root_class hierarchy; an out-of-hierarchy type
    # would produce table names that have no ClassDef in side_table_class_defs.
    if effective_class != root_class:
        ancestors = schema_view.class_ancestors(effective_class, mixins=True) or []
        if root_class not in ancestors:
            effective_class = root_class
    parent_id = record.get("id", "")

    for slot in schema_view.class_induced_slots(effective_class):
        if not slot.multivalued:
            continue
        if slot.name not in record:
            continue
        value = record[slot.name]
        if value is None:
            continue
        if not isinstance(value, list):
            value = [value]
        if not value:
            continue

        table_name = f"{collection}_{slot.name}"
        range_class = _range_class(slot, schema_view)

        if range_class is not None and _is_inlined(slot, schema_view):
            # Inlined multivalued → child side table
            for child in value:
                if not isinstance(child, dict):
                    continue
                row = _expand_inlined(child, range_class, schema_view)
                row["parent_id"] = parent_id
                yield table_name, row
        elif range_class is not None:
            # Ref-class multivalued → junction table (ARRAY also in primary)
            for v in value:
                if v is not None:
                    yield table_name, {"parent_id": parent_id, slot.name: v}
        # Scalar multivalued: ARRAY column in primary table, no side table row


class SchemaDrivenFlattener:
    """Flatten NMDC objects using a LinkML SchemaView.

    Matches the ``Transform`` protocol: ``apply(records)`` yields flat
    dicts. Constructed once per root class; per-record slot induction cost
    is paid inside ``flatten_record`` on each call (not amortised).
    """

    def __init__(self, schema_view: SchemaView, root_class: str) -> None:
        """Construct a flattener for ``root_class`` under ``schema_view``."""
        self.schema_view = schema_view
        self.root_class = root_class

    def apply(self, records: Iterable[dict]) -> Iterator[dict]:
        """Yield one flat dict per input record."""
        for record in records:
            yield flatten_record(record, self.schema_view, self.root_class)
