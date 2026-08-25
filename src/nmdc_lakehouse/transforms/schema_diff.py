"""Compare two generated flat schemas and report what changed between them.

The point of giving the flat schema a version is being able to answer "what changed in the
lakehouse and why", and a version nobody can diff answers nothing. Three committed revisions of
the artifact all declared 11.23.0 while differing in content, so until now no such report was
possible even in principle.

Reads two rendered artifacts. It does not reach the network or the installed package, so it can
compare a release against any revision materialised from git.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from pathlib import Path

from linkml_runtime import SchemaView


class SchemaDiffError(ValueError):
    """Raised when a schema cannot be read for comparison."""


@dataclass
class AttributeChange:
    """One attribute that exists in both schemas but does not describe the same thing."""

    table: str
    attribute: str
    what: str
    before: str
    after: str


@dataclass
class SchemaDiff:
    """Everything that differs between two flat schemas."""

    before_version: str
    after_version: str
    tables_added: list[str] = field(default_factory=list)
    tables_removed: list[str] = field(default_factory=list)
    attributes_added: list[tuple[str, str]] = field(default_factory=list)
    attributes_removed: list[tuple[str, str]] = field(default_factory=list)
    attributes_changed: list[AttributeChange] = field(default_factory=list)
    tables_changed: list[AttributeChange] = field(default_factory=list)
    # Set when the two files are not byte-identical. Kept separate from the modelled findings so
    # the report can distinguish "nothing changed" from "something changed that this report does
    # not look at", which is the difference between a clean result and a blind one.
    documents_differ: bool = False

    @property
    def is_empty(self) -> bool:
        """True when nothing this report models differs between the two schemas.

        That is: the same tables, the same attributes on each, the same range, multivalued and
        required flags, and the same attribute and table descriptions. It says nothing about
        types, enums, prefixes, imports or top-level annotations, which is why `documents_differ`
        is reported alongside it rather than folded into it.
        """
        return not any(
            (
                self.tables_added,
                self.tables_removed,
                self.attributes_added,
                self.attributes_removed,
                self.attributes_changed,
                self.tables_changed,
            )
        )


def _sha256(path: str) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _load(path: str) -> SchemaView:
    try:
        return SchemaView(path)
    except Exception as error:  # linkml raises a variety of parse errors
        raise SchemaDiffError(f"Cannot read schema for comparison: {path}") from error


def _attributes(view: SchemaView, table: str) -> dict[str, object]:
    definition = view.get_class(table)
    return dict(definition.attributes or {}) if definition is not None else {}


def _describe(attribute: object) -> dict[str, str]:
    """Reduce one attribute to the properties a consumer would notice changing."""
    return {
        "range": str(getattr(attribute, "range", "") or ""),
        "multivalued": str(bool(getattr(attribute, "multivalued", False))),
        "required": str(bool(getattr(attribute, "required", False))),
        "description": str(getattr(attribute, "description", "") or ""),
    }


def diff_schemas(before_path: str, after_path: str) -> SchemaDiff:
    """Compare two rendered flat schemas by table, attribute and attribute properties."""
    before, after = _load(before_path), _load(after_path)
    result = SchemaDiff(
        before_version=before.schema.version or "unversioned",
        after_version=after.schema.version or "unversioned",
    )

    try:
        result.documents_differ = _sha256(before_path) != _sha256(after_path)
    except OSError as error:
        raise SchemaDiffError(f"Cannot read schema for comparison: {error}") from error

    before_tables, after_tables = set(before.all_classes()), set(after.all_classes())
    result.tables_added = sorted(after_tables - before_tables)
    result.tables_removed = sorted(before_tables - after_tables)

    for table in sorted(before_tables & after_tables):
        # Class-level properties, which an attribute-only comparison misses entirely. Between
        # c4d6ceb and 9073b67 every difference was a class description, so the first version of
        # this function reported "no differences" for two schemas whose contents hash differently
        # and whose line counts differ by 55.
        old_class, new_class = before.get_class(table), after.get_class(table)
        old_description = str(getattr(old_class, "description", "") or "")
        new_description = str(getattr(new_class, "description", "") or "")
        if old_description != new_description:
            result.tables_changed.append(
                AttributeChange(
                    table=table,
                    attribute="(class)",
                    what="description",
                    before=old_description,
                    after=new_description,
                )
            )

        old, new = _attributes(before, table), _attributes(after, table)
        result.attributes_added.extend((table, name) for name in sorted(set(new) - set(old)))
        result.attributes_removed.extend((table, name) for name in sorted(set(old) - set(new)))
        for name in sorted(set(old) & set(new)):
            old_shape, new_shape = _describe(old[name]), _describe(new[name])
            for what, old_value in old_shape.items():
                new_value = new_shape[what]
                if old_value != new_value:
                    result.attributes_changed.append(
                        AttributeChange(table=table, attribute=name, what=what, before=old_value, after=new_value)
                    )
    return result


def _differing_part(before: str, after: str, context: int = 24, width: int = 70) -> tuple[str, str]:
    """Return the two values trimmed to where they actually diverge.

    Truncating from the start hides the difference whenever a long shared prefix precedes it,
    which is the common case for descriptions: the first attempt printed 79 rows where most
    showed the same 60 characters twice and told the reader nothing.
    """
    shared = min(len(before), len(after))
    for index, (left, right) in enumerate(zip(before, after, strict=False)):
        if left != right:
            shared = index
            break
    start = max(0, shared - context)
    lead = "..." if start else ""

    def trim(value: str) -> str:
        piece = value[start : start + width]
        tail = "..." if start + width < len(value) else ""
        return f"{lead}{piece}{tail}"

    return trim(before), trim(after)


def render_diff(diff: SchemaDiff, limit: int = 40) -> str:
    """Render a diff as markdown, saying plainly when a section was truncated.

    A report that silently shows the first forty of six hundred reads as "forty changes", which
    is the sort of quiet truncation this repository has been bitten by before.
    """
    lines = ["# Flat schema diff", "", f"- before: `{diff.before_version}`", f"- after: `{diff.after_version}`", ""]
    if diff.is_empty:
        if diff.documents_differ:
            lines.append(
                "**The two documents are not identical, but nothing this report models differs.** "
                "Something changed outside tables, attributes, ranges and descriptions: types, "
                "enums, prefixes, imports or top-level annotations. Compare the files directly."
            )
        else:
            lines.append("The two documents are byte-identical.")
        return "\n".join(lines) + "\n"

    def section(title: str, items: list[str]) -> None:
        if not items:
            return
        lines.append(f"## {title} ({len(items)})")
        lines.append("")
        for item in items[:limit]:
            lines.append(f"- {item}")
        if len(items) > limit:
            lines.append(f"- ... and {len(items) - limit} more, not shown")
        lines.append("")

    section("Tables added", [f"`{t}`" for t in diff.tables_added])
    section("Tables removed", [f"`{t}`" for t in diff.tables_removed])
    section("Attributes added", [f"`{t}.{a}`" for t, a in diff.attributes_added])
    section("Attributes removed", [f"`{t}.{a}`" for t, a in diff.attributes_removed])
    section(
        "Table descriptions changed",
        ["`{}`: `{}` -> `{}`".format(c.table, *_differing_part(c.before, c.after)) for c in diff.tables_changed],
    )
    section(
        "Attributes changed",
        [
            f"`{c.table}.{c.attribute}` {c.what}: `{c.before or '(none)'}` -> `{c.after or '(none)'}`"
            for c in diff.attributes_changed
        ],
    )
    return "\n".join(lines).rstrip() + "\n"
