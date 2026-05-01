"""Resolve `data_object_type` strings against nmdc-schema's `FileTypeEnum`.

Loaders that filter `data_object_set` by `data_object_type` should pass
their target strings through `resolve_file_type` rather than embedding
literals. A typo or schema rename then fails fast at notebook startup
instead of producing a silently empty manifest at SQL time.
"""

from __future__ import annotations

import difflib
from functools import cache

from nmdc_schema.nmdc import FileTypeEnum


@cache
def _permissible_values() -> tuple[frozenset[str], tuple[str, ...]]:
    # nmdc-schema's FileTypeEnum is a LinkML EnumDefinitionImpl, not a stdlib
    # Enum: dir() exposes the human-readable permissible-value strings as
    # attribute names ("Pfam Annotation GFF", not "PFAM_ANNOTATION_GFF").
    values = sorted(a for a in dir(FileTypeEnum) if not a.startswith("_"))
    return frozenset(values), tuple(values)


def resolve_file_type(value: str) -> str:
    """Return ``value`` if it's a permissible value of FileTypeEnum.

    Raises ValueError with up to three close matches if not.
    """
    membership, ordered = _permissible_values()
    if value in membership:
        return value
    suggestions = difflib.get_close_matches(value, ordered, n=3, cutoff=0.6)
    hint = f" Did you mean: {suggestions}?" if suggestions else ""
    raise ValueError(f"{value!r} is not a permissible value of nmdc-schema FileTypeEnum.{hint}")


def resolve_file_types(values: list[str]) -> list[str]:
    """Validate every entry in ``values`` against FileTypeEnum, preserving order."""
    return [resolve_file_type(v) for v in values]
