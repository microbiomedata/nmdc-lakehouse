"""Flatteners that convert nested LinkML objects into tabular rows.

Stub module — actual implementations will use the LinkML ``SchemaView`` to
unroll nested slots (multivalued / inlined) into one or more output tables,
optionally emitting join tables for many-to-many relationships.
"""

from __future__ import annotations

from typing import Iterable, Iterator


class SchemaDrivenFlattener:
    """Flatten NMDC objects using a LinkML SchemaView."""

    def __init__(self, schema_view: object, root_class: str) -> None:
        """Construct a flattener for ``root_class`` under ``schema_view``."""
        self.schema_view = schema_view
        self.root_class = root_class

    def apply(self, records: Iterable[dict]) -> Iterator[dict]:
        """Yield one flat row per (logical) leaf derived from each record."""
        raise NotImplementedError
