"""Tests for flat-schema identity and the diff report built on it."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

from nmdc_lakehouse.transforms.schema_diff import SchemaDiffError, diff_schemas, render_diff
from nmdc_lakehouse.transforms.schema_generator import (
    FLATTENER_VERSION,
    UNRESOLVED_CONTENT_SHA256,
    flat_schema_version,
)


def _load_generator_script():
    """Load the generator script without putting scripts/python on sys.path.

    Inserting into sys.path at import time changes import resolution for every other test in the
    session, not just this file, and the effect outlives the module that caused it.
    """
    path = Path(__file__).resolve().parents[1] / "scripts/python/generate_flattened_schema.py"
    spec = importlib.util.spec_from_file_location("generate_flattened_schema", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_generator = _load_generator_script()
SchemaArtifactError = _generator.SchemaArtifactError
declared_content_digest = _generator.declared_content_digest
resolve_content_digest = _generator.resolve_content_digest
verify_content_digest = _generator.verify_content_digest

_HEADER = """id: https://example.org/flat
name: flat
version: {version}
default_range: string
classes:
"""


def _schema(tmp_path: Path, name: str, body: str, version: str = "1.0.0+flat.1.0.0") -> str:
    path = tmp_path / f"{name}.yaml"
    path.write_text(_HEADER.format(version=version) + body, encoding="utf-8")
    return str(path)


def test_the_flat_version_names_both_halves_of_what_produced_it() -> None:
    """A flattener change must be visible without also changing the upstream release."""
    assert flat_schema_version("11.23.0") == f"11.23.0+flat.{FLATTENER_VERSION}"
    assert flat_schema_version("11.23.0") != "11.23.0"


def test_a_resolved_digest_describes_the_document_that_carries_it() -> None:
    rendered = f"name: x\nvalue: '{UNRESOLVED_CONTENT_SHA256}'\n"
    resolved = resolve_content_digest(rendered)
    assert UNRESOLVED_CONTENT_SHA256 not in resolved
    verify_content_digest(resolved)


def test_a_hand_edited_artifact_is_refused() -> None:
    """The guard has to be able to fail, or a clean run proves nothing."""
    resolved = resolve_content_digest(f"name: x\nvalue: '{UNRESOLVED_CONTENT_SHA256}'\n")
    tampered = resolved.replace("name: x", "name: edited-by-hand")

    with pytest.raises(SchemaArtifactError, match="does not match its content"):
        verify_content_digest(tampered)


def test_an_artifact_with_no_digest_is_refused() -> None:
    with pytest.raises(SchemaArtifactError, match="declares no content digest"):
        verify_content_digest("name: x\n")


def test_a_quoted_digest_is_found() -> None:
    """The dumper quotes a hex string, and an unquoted-only pattern found nothing."""
    digest = "a" * 64
    assert declared_content_digest(f"    value: '{digest}'\n") == digest
    assert declared_content_digest(f"    value: {digest}\n") == digest


def test_a_class_description_change_is_reported(tmp_path: Path) -> None:
    """The regression this diff was rewritten for.

    Between c4d6ceb and 9073b67 every difference was a class description, and an
    attribute-only comparison called those two schemas identical.
    """
    before = _schema(tmp_path, "before", "  T:\n    description: made by the old flattener\n")
    after = _schema(tmp_path, "after", "  T:\n    description: made by the new flattener\n")

    diff = diff_schemas(before, after)

    assert not diff.is_empty
    assert [change.table for change in diff.tables_changed] == ["T"]
    assert "Table descriptions changed" in render_diff(diff)


def test_identical_documents_say_so(tmp_path: Path) -> None:
    body = "  T:\n    description: same\n"
    path = _schema(tmp_path, "same", body)

    report = render_diff(diff_schemas(path, path))

    assert "byte-identical" in report


def test_documents_that_differ_outside_the_model_are_not_called_clean(tmp_path: Path) -> None:
    """A report that models nothing relevant must say so rather than say "no differences"."""
    before = _schema(tmp_path, "b", "  T:\n    description: same\n")
    after = _schema(tmp_path, "a", "  T:\n    description: same\n")
    extra = "\nenums:\n  E:\n    description: new\n"
    Path(after).write_text(Path(after).read_text(encoding="utf-8") + extra, encoding="utf-8")

    diff = diff_schemas(before, after)
    report = render_diff(diff)

    assert diff.documents_differ
    assert "nothing this report models differs" in report


def test_added_and_removed_tables_and_attributes(tmp_path: Path) -> None:
    before = _schema(tmp_path, "before", "  Gone:\n    attributes:\n      a:\n        range: string\n")
    after = _schema(
        tmp_path,
        "after",
        "  New:\n    attributes:\n      b:\n        range: integer\n",
    )

    diff = diff_schemas(before, after)

    assert diff.tables_added == ["New"]
    assert diff.tables_removed == ["Gone"]


def test_an_attribute_range_change_is_reported(tmp_path: Path) -> None:
    before = _schema(tmp_path, "before", "  T:\n    attributes:\n      a:\n        range: string\n")
    after = _schema(tmp_path, "after", "  T:\n    attributes:\n      a:\n        range: integer\n")

    changes = diff_schemas(before, after).attributes_changed

    assert [(c.attribute, c.what, c.before, c.after) for c in changes] == [("a", "range", "string", "integer")]


def test_truncation_is_reported_rather_than_silent(tmp_path: Path) -> None:
    """A report showing the first N of many must not read as if N were the total."""
    attributes = "".join(f"      a{i}:\n        range: string\n" for i in range(10))
    before = _schema(tmp_path, "before", "  T:\n    attributes:\n" + attributes)
    after = _schema(tmp_path, "after", "  T:\n    description: added\n    attributes:\n" + attributes)
    widened = Path(after).read_text(encoding="utf-8").replace("range: string", "range: integer")
    Path(after).write_text(widened, encoding="utf-8")

    report = render_diff(diff_schemas(before, after), limit=3)

    assert "and 7 more, not shown" in report


def test_an_unreadable_schema_is_refused(tmp_path: Path) -> None:
    with pytest.raises(SchemaDiffError, match="Cannot read schema"):
        diff_schemas(str(tmp_path / "missing.yaml"), str(tmp_path / "also-missing.yaml"))


def test_cli_prints_a_report(tmp_path: Path) -> None:
    """The command path itself, not just the function underneath it."""
    from click.testing import CliRunner

    from nmdc_lakehouse.cli import cli

    before = _schema(tmp_path, "before", "  T:\n    description: old\n")
    after = _schema(tmp_path, "after", "  T:\n    description: new\n")

    result = CliRunner().invoke(cli, ["schema-diff", before, after])

    assert result.exit_code == 0, result.output
    assert "Table descriptions changed" in result.output


def test_cli_writes_a_report_to_a_file(tmp_path: Path) -> None:
    from click.testing import CliRunner

    from nmdc_lakehouse.cli import cli

    before = _schema(tmp_path, "before", "  T:\n    description: old\n")
    after = _schema(tmp_path, "after", "  T:\n    description: new\n")
    destination = tmp_path / "report.md"

    result = CliRunner().invoke(cli, ["schema-diff", before, after, "--output", str(destination)])

    assert result.exit_code == 0, result.output
    assert "Table descriptions changed" in destination.read_text(encoding="utf-8")


def test_cli_reports_an_unreadable_schema_as_a_clean_failure(tmp_path: Path) -> None:
    """A parse failure must be a message, not a traceback."""
    from click.testing import CliRunner

    from nmdc_lakehouse.cli import cli

    good = _schema(tmp_path, "good", "  T:\n    description: fine\n")
    broken = tmp_path / "broken.yaml"
    broken.write_text("id: [unclosed\n", encoding="utf-8")

    result = CliRunner().invoke(cli, ["schema-diff", good, str(broken)])

    assert result.exit_code != 0
    assert "Cannot read schema" in result.output


def test_the_digest_is_read_from_its_own_annotation() -> None:
    """A generic scan takes the first hex value in the document, which need not be ours.

    The snapshot manifest already records several sha256 values, so an artifact that grew a
    similar field would have verification checking the wrong string and still passing.
    """
    decoy = "b" * 64
    real = "c" * 64
    document = (
        "annotations:\n"
        "  some_other_digest:\n"
        "    tag: some_other_digest\n"
        f"    value: '{decoy}'\n"
        "  flat_schema_sha256:\n"
        "    tag: flat_schema_sha256\n"
        f"    value: '{real}'\n"
    )

    assert declared_content_digest(document) == real


def test_a_bare_annotation_still_works() -> None:
    """The fallback keeps small fixtures readable rather than forcing the full artifact shape."""
    digest = "d" * 64

    assert declared_content_digest(f"    value: '{digest}'\n") == digest
