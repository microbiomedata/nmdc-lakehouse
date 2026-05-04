"""Tests for nmdc_lakehouse.file_types."""

from __future__ import annotations

import pytest

from nmdc_lakehouse.file_types import resolve_file_type, resolve_file_types


def test_known_value_round_trips() -> None:
    assert resolve_file_type("Pfam Annotation GFF") == "Pfam Annotation GFF"


def test_unknown_value_raises_with_suggestion() -> None:
    with pytest.raises(ValueError, match="Did you mean"):
        resolve_file_type("Pfam Annotation GFF3")


def test_unknown_value_raises_without_suggestion_when_no_close_match() -> None:
    with pytest.raises(ValueError) as exc:
        resolve_file_type("xxxxxxxxxxxxxxxxxxxx")
    assert "FileTypeEnum" in str(exc.value)


def test_resolve_list_preserves_order() -> None:
    inputs = ["Annotation KEGG Orthology", "Annotation Enzyme Commission"]
    assert resolve_file_types(inputs) == inputs


def test_resolve_list_fails_on_first_bad_entry() -> None:
    with pytest.raises(ValueError):
        resolve_file_types(["Annotation KEGG Orthology", "Not A Real Type"])
