"""Generate a LinkML schema describing the flat shape produced by the flattener.

Given a source LinkML schema (e.g. nmdc-schema) and a root class, emit a new
:class:`ClassDefinition` whose attributes mirror the flat output produced by
:meth:`nmdc_lakehouse.transforms.flatteners.SchemaDrivenFlattener.apply`. The
decision tree matches the runtime flattener one-to-one, so the generated
schema and the runtime output can't drift.

For polymorphic base classes (where records may declare a concrete subclass
via ``type``), the generated flat class contains the **union of all subclass
slots**. Subclass-specific slots are emitted as non-required so parquet can
hold them sparsely.
"""

from __future__ import annotations

from typing import Iterable

from linkml_runtime import SchemaView
from linkml_runtime.linkml_model import (
    ClassDefinition,
    Prefix,
    SchemaDefinition,
    SlotDefinition,
)


def _is_inlined(slot: SlotDefinition, schema_view: SchemaView) -> bool:
    """Apply LinkML's identifier-based default for class-range slot inlining.

    Keeps schema generation aligned with the runtime flattener's
    expanded-vs-reference decision. Both modules implement the same rule
    independently rather than sharing a helper.
    """
    if slot.inlined is not None:
        return slot.inlined
    if not slot.range:
        return False
    if schema_view.get_class(slot.range) is None:
        return False
    return schema_view.get_identifier_slot(slot.range) is None


LIST_JOIN_NOTE = "Multivalued slot flattened to a pipe-separated string (values joined with '|')."
REF_NOTE = "Reference by identifier; original range was class '{range}'."
NESTED_NOTE = "Flattened from nested slot '{parent}.{inner}'."
DISPATCH_NOTE = "Polymorphic subclass-specific slot (from '{subclass}')."


def flatten_class_def(schema_view: SchemaView, root_class: str, target_name: str | None = None) -> ClassDefinition:
    """Return a new ClassDefinition describing the flat form of ``root_class``.

    Walks the same decision tree as ``flatten_record``:

    - scalar → flat slot with same range
    - multivalued scalar → flat slot with string range (pipe-joined)
    - class range, not inlined → flat slot with string range (ID reference)
    - single-valued inlined class → one flat slot per subclass scalar slot,
      named ``<parent>_<inner>`` (one or two levels deep)
    - multivalued inlined class → **skipped** (helper-table follow-up)

    Union polymorphism: slots on concrete subclasses of ``root_class`` are
    unioned in, annotated with ``DISPATCH_NOTE`` so downstream consumers can
    tell base-class from subclass-specific columns.

    Args:
        schema_view: Loaded SchemaView over the source schema.
        root_class: Name of the class to flatten.
        target_name: Name for the generated class. Defaults to ``<root_class>Flat``.

    Returns:
        A new ``ClassDefinition``. Attributes are flat ``SlotDefinition``s.
    """
    target_name = target_name or f"{root_class}Flat"
    cls = ClassDefinition(name=target_name)
    cls.description = (
        f"Flattened tabular form of '{root_class}' produced by "
        f"`SchemaDrivenFlattener`. Attributes are the union of base-class "
        f"slots and slots from concrete subclasses of '{root_class}' that "
        f"may appear via the 'type' field."
    )

    attrs: dict[str, SlotDefinition] = {}

    # Base class slots — faithful to induced_slots(root_class)
    for slot in schema_view.class_induced_slots(root_class):
        for flat in _flatten_slot(slot, schema_view):
            attrs.setdefault(flat.name, flat)

    # Union in slots from concrete subclasses (polymorphic dispatch)
    for descendant in _proper_descendants(schema_view, root_class):
        for slot in schema_view.class_induced_slots(descendant):
            for flat in _flatten_slot(slot, schema_view, dispatch_subclass=descendant):
                # Keep the first-seen slot (base class takes precedence); add
                # a dispatch annotation if only a subclass contributed it.
                attrs.setdefault(flat.name, flat)

    # Deterministic ordering for reproducible output
    for name in sorted(attrs):
        cls.attributes[name] = attrs[name]

    return cls


def flatten_database_schema(
    schema_view: SchemaView,
    database_class: str = "Database",
    schema_id: str = "https://w3id.org/nmdc/nmdc-schema-flattened",
    schema_name: str = "nmdc_schema_flattened",
) -> SchemaDefinition:
    """Emit a full SchemaDefinition with one flat class per Database slot.

    Walks each multivalued slot on ``database_class`` (the 17 NMDC collections),
    resolves its range, and calls :func:`flatten_class_def` to produce a flat
    class for each.
    """
    out = SchemaDefinition(
        id=schema_id,
        name=schema_name,
        description=(
            "Flattened LinkML schema describing the tabular output of "
            "`SchemaDrivenFlattener` applied to each schema-specified "
            "collection. Generated — do not edit by hand."
        ),
        prefixes={
            "linkml": Prefix(
                prefix_prefix="linkml",
                prefix_reference="https://w3id.org/linkml/",
            ),
        },
        imports=["linkml:types"],
        default_range="string",
    )

    db_slots = schema_view.class_induced_slots(database_class)
    for slot in db_slots:
        if not slot.multivalued or not slot.range:
            continue
        range_class = schema_view.get_class(slot.range)
        if range_class is None:
            continue
        flat = flatten_class_def(schema_view, slot.range)
        out.classes[flat.name] = flat

    return out


def side_table_class_defs(
    schema_view: SchemaView,
    root_class: str,
    collection: str,
) -> list[tuple[str, ClassDefinition]]:
    """Return ``(table_name, ClassDefinition)`` pairs for all side tables of ``root_class``.

    Mirrors the decision tree in :func:`nmdc_lakehouse.transforms.flatteners.side_table_rows`:

    - **scalar** or **ref_class** multivalued: junction table with two slots,
      ``parent_id`` (string) and ``<slot_name>`` (slot's range or string for refs).
    - **inlined_class** multivalued: child-class flat schema (via
      :func:`flatten_class_def`) plus a ``parent_id`` slot.

    Scans ``root_class`` and all its proper descendants so polymorphic
    subclass-specific slots (e.g. ``mags_list`` on ``MagsAnalysis``) are
    included even when the collection is declared against the abstract base.

    Args:
        schema_view: Loaded LinkML SchemaView.
        root_class: Root class for this collection.
        collection: Collection name — used as the table name prefix.

    Returns:
        List of ``(table_name, ClassDefinition)`` pairs, one per distinct
        multivalued slot. Ordered by table name for deterministic output.
    """
    result: list[tuple[str, ClassDefinition]] = []
    seen: set[str] = set()

    for class_name in [root_class] + _proper_descendants(schema_view, root_class):
        for slot in schema_view.class_induced_slots(class_name):
            if not slot.multivalued:
                continue
            table_name = f"{collection}_{slot.name}"
            if table_name in seen:
                continue
            seen.add(table_name)

            range_class = _range_class(slot, schema_view)

            if range_class is not None and _is_inlined(slot, schema_view):
                child_flat = flatten_class_def(schema_view, range_class.name, target_name=table_name)
                child_flat.attributes["parent_id"] = SlotDefinition(name="parent_id", range="string")
                result.append((table_name, child_flat))
            else:
                cls = ClassDefinition(name=table_name)
                cls.attributes["parent_id"] = SlotDefinition(name="parent_id", range="string")
                value_range = "string" if range_class is not None else (slot.range or "string")
                cls.attributes[slot.name] = SlotDefinition(name=slot.name, range=value_range)
                result.append((table_name, cls))

    result.sort(key=lambda x: x[0])
    return result


def _flatten_slot(
    slot: SlotDefinition,
    schema_view: SchemaView,
    dispatch_subclass: str | None = None,
) -> Iterable[SlotDefinition]:
    """Yield one or more flat SlotDefinitions for a single source slot.

    ``dispatch_subclass`` marks the resulting slot(s) as coming from a
    polymorphic-dispatch path so the output schema can distinguish them.
    """
    range_class = _range_class(slot, schema_view)
    notes: list[str] = []
    if dispatch_subclass:
        notes.append(DISPATCH_NOTE.format(subclass=dispatch_subclass))

    # Class range, not inlined → reference (string)
    if range_class is not None and not _is_inlined(slot, schema_view):
        yield _flat_string_slot(
            slot.name,
            slot,
            description=(slot.description or "")
            + (" " if slot.description else "")
            + REF_NOTE.format(range=slot.range)
            + (" " + LIST_JOIN_NOTE if slot.multivalued else ""),
            required_override=(slot.required if not dispatch_subclass else False),
            notes=notes,
        )
        return

    # Scalar range
    if range_class is None:
        description = slot.description or ""
        if slot.multivalued:
            description = (description + " " + LIST_JOIN_NOTE).strip()
            yield _flat_string_slot(
                slot.name,
                slot,
                description=description,
                required_override=(slot.required if not dispatch_subclass else False),
                notes=notes,
            )
        else:
            new_slot = SlotDefinition(
                name=slot.name,
                range=slot.range,
                description=description or None,
                required=(slot.required if not dispatch_subclass else False),
            )
            _attach_notes(new_slot, notes)
            yield new_slot
        return

    # Class range, inlined
    if slot.multivalued:
        # Helper-table case — deferred.
        return

    # Single-valued inlined class → expand one level (and one more for
    # nested controlled terms), producing <parent>_<inner> slots.
    for inner_slot in schema_view.class_induced_slots(range_class.name):
        if inner_slot.name == "type":
            continue
        inner_range = _range_class(inner_slot, schema_view)
        if inner_range is None:
            flat_name = f"{slot.name}_{inner_slot.name}"
            inner_description = inner_slot.description or ""
            if inner_slot.multivalued:
                description = (
                    inner_description
                    + " "
                    + NESTED_NOTE.format(parent=slot.name, inner=inner_slot.name)
                    + " "
                    + LIST_JOIN_NOTE
                ).strip()
                yield _flat_string_slot(
                    flat_name,
                    inner_slot,
                    description=description,
                    required_override=False,
                    notes=notes,
                )
            else:
                new_slot = SlotDefinition(
                    name=flat_name,
                    range=inner_slot.range,
                    description=(
                        (inner_description + " " + NESTED_NOTE.format(parent=slot.name, inner=inner_slot.name)).strip()
                    ),
                    required=False,
                )
                _attach_notes(new_slot, notes)
                yield new_slot
            continue
        # One more level of nesting (term → id/name)
        if not inner_slot.multivalued:
            for deepest in schema_view.class_induced_slots(inner_range.name):
                if deepest.name == "type":
                    continue
                if _range_class(deepest, schema_view) is not None:
                    continue  # Three levels deep is out of scope
                flat_name = f"{slot.name}_{inner_slot.name}_{deepest.name}"
                deep_description = deepest.description or ""
                if deepest.multivalued:
                    description = (
                        deep_description
                        + " "
                        + NESTED_NOTE.format(
                            parent=f"{slot.name}.{inner_slot.name}",
                            inner=deepest.name,
                        )
                        + " "
                        + LIST_JOIN_NOTE
                    ).strip()
                    yield _flat_string_slot(
                        flat_name,
                        deepest,
                        description=description,
                        required_override=False,
                        notes=notes,
                    )
                else:
                    new_slot = SlotDefinition(
                        name=flat_name,
                        range=deepest.range,
                        description=(
                            (
                                deep_description
                                + " "
                                + NESTED_NOTE.format(
                                    parent=f"{slot.name}.{inner_slot.name}",
                                    inner=deepest.name,
                                )
                            ).strip()
                        ),
                        required=False,
                    )
                    _attach_notes(new_slot, notes)
                    yield new_slot


def _range_class(slot: SlotDefinition, schema_view: SchemaView):
    """Return the class range of a slot, or None if the range is a type/enum."""
    if not slot.range:
        return None
    return schema_view.get_class(slot.range)


def _flat_string_slot(
    name: str,
    source: SlotDefinition,
    description: str,
    required_override: bool | None,
    notes: list[str],
) -> SlotDefinition:
    """Construct a flat SlotDefinition with a string range."""
    slot = SlotDefinition(
        name=name,
        range="string",
        description=description or None,
        required=required_override,
    )
    _attach_notes(slot, notes)
    return slot


def _attach_notes(slot: SlotDefinition, notes: list[str]) -> None:
    """Append notes to the slot's description (semicolon-separated)."""
    if not notes:
        return
    extra = "; ".join(notes)
    slot.description = f"{slot.description}. {extra}" if slot.description else extra


def _proper_descendants(schema_view: SchemaView, class_name: str) -> list[str]:
    """Return descendants of ``class_name`` excluding itself.

    Uses ``class_descendants`` which LinkML returns inclusive of the root.
    """
    try:
        descendants = schema_view.class_descendants(class_name)
    except Exception:
        return []
    return [d for d in descendants if d != class_name]
