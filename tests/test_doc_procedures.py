"""Tests for the runnable-procedure verification-marker check.

The first test is the one that matters. A check that cannot fail reports every
run as clean, and this repository has shipped that mistake more than once: a
prose-lint invocation that could not match Vale's singular "1 error", and a
verification snippet that iterated its destination so a wholly absent table
passed. So the guard is tested on what it must reject before it is tested on
what it must accept.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from nmdc_lakehouse.doc_procedures import (
    BASELINE_FORMAT_VERSION,
    MARKER_LOOKBACK,
    ProcedureBlock,
    fingerprint,
    iter_blocks,
    load_baseline,
    main,
    offending,
    report,
    scan,
    write_baseline,
)

UNMARKED = "intro\n\n```bash\necho hi\n```\n"
VERIFIED = "intro\n\n<!-- verified: 2026-08-24 ran it, printed hi -->\n```bash\necho hi\n```\n"
UNVERIFIED = "intro\n\n<!-- unverified: needs a pod terminal -->\n```bash\necho hi\n```\n"


def _write(directory: Path, text: str, name: str = "a.md") -> Path:
    path = directory / name
    path.write_text(text, encoding="utf-8")
    return path


def test_rejects_an_undeclared_runnable_block(tmp_path: Path) -> None:
    """The guard must fail on the thing it exists to catch."""
    _write(tmp_path, UNMARKED)
    blocks = scan([tmp_path])
    assert len(blocks) == 1
    assert offending(blocks, set()) == blocks
    assert main([str(tmp_path), "--baseline", str(tmp_path / "absent.json")]) == 1


def test_rejects_every_undeclared_block_not_just_the_first(tmp_path: Path) -> None:
    """A second offender in the same file is reported too."""
    _write(tmp_path, UNMARKED + "\nmore\n\n```python\nprint(1)\n```\n")
    blocks = scan([tmp_path])
    assert len(blocks) == 2
    assert len(offending(blocks, set())) == 2


@pytest.mark.parametrize("text,marker", [(VERIFIED, "verified"), (UNVERIFIED, "unverified")])
def test_either_marker_declares_a_block(tmp_path: Path, text: str, marker: str) -> None:
    """Both markers pass: the rule is that the document says which, not that it was run."""
    _write(tmp_path, text)
    blocks = scan([tmp_path])
    assert [block.marker for block in blocks] == [marker]
    assert offending(blocks, set()) == []
    assert main([str(tmp_path), "--baseline", str(tmp_path / "absent.json")]) == 0


def test_a_marker_too_far_above_does_not_count(tmp_path: Path) -> None:
    """A marker separated by more than the lookback belongs to something else."""
    filler = "\n".join(f"line {index}" for index in range(MARKER_LOOKBACK + 1))
    _write(tmp_path, f"<!-- verified: 2026-08-24 -->\n{filler}\n```bash\necho hi\n```\n")
    assert offending(scan([tmp_path]), set()) != []


def test_blank_lines_do_not_consume_the_lookback(tmp_path: Path) -> None:
    """Markdown separates a comment from a fence by a blank line; that must still count."""
    _write(tmp_path, "<!-- verified: 2026-08-24 ran it -->\n\n\n```bash\necho hi\n```\n")
    assert offending(scan([tmp_path]), set()) == []


def test_a_block_with_no_language_is_inert(tmp_path: Path) -> None:
    """Pasted output is written as a bare fence throughout these documents."""
    _write(tmp_path, "intro\n\n```\n30GiB  annotation_kegg_orthology.parquet\n```\n")
    assert scan([tmp_path]) == []


def test_a_data_language_is_inert(tmp_path: Path) -> None:
    """A JSON or YAML excerpt is not a procedure."""
    _write(tmp_path, 'intro\n\n```json\n{"rows": 4815}\n```\n')
    assert scan([tmp_path]) == []


def test_tilde_fences_and_indented_fences_are_found(tmp_path: Path) -> None:
    """Both fence characters occur in these documents, and fences nest in lists."""
    _write(tmp_path, "- step\n\n  ```bash\n  echo hi\n  ```\n")
    _write(tmp_path, "~~~bash\necho hi\n~~~\n", name="b.md")
    assert len(scan([tmp_path])) == 2


def test_a_shorter_fence_at_line_start_does_not_close_a_longer_block(tmp_path: Path) -> None:
    """A block quoting a fence on its own line must not be split into two.

    The fixture puts the inner backticks at the start of a line on purpose. An
    earlier version wrote them inside `echo \'```\'`, which cannot match the fence
    pattern, so the test passed without exercising anything.
    """
    _write(tmp_path, "````bash\n```\necho hi\n````\n")
    blocks = scan([tmp_path])
    assert len(blocks) == 1
    assert blocks[0].language == "bash"


def test_the_baseline_grandfathers_and_an_edit_revokes_it(tmp_path: Path) -> None:
    """The blocks being changed are the blocks being claimed about."""
    path = _write(tmp_path, UNMARKED)
    baseline_path = tmp_path / "baseline.json"
    write_baseline(baseline_path, scan([tmp_path]))
    baseline = load_baseline(baseline_path)
    assert offending(scan([tmp_path]), baseline) == []

    path.write_text(UNMARKED.replace("echo hi", "echo hi there"), encoding="utf-8")
    assert offending(scan([tmp_path]), baseline) != []


def test_the_baseline_records_its_format_version(tmp_path: Path) -> None:
    """A future reader needs to know which shape they are looking at."""
    _write(tmp_path, UNMARKED)
    baseline_path = tmp_path / "baseline.json"
    write_baseline(baseline_path, scan([tmp_path]))
    document = json.loads(baseline_path.read_text(encoding="utf-8"))
    assert document["baseline_format_version"] == BASELINE_FORMAT_VERSION
    assert len(document["occurrences"]) == 1


def test_an_absent_baseline_grandfathers_nothing(tmp_path: Path) -> None:
    """A missing file must not read as permission."""
    assert load_baseline(tmp_path / "nope.json") == set()


def test_fingerprint_ignores_trailing_whitespace_only() -> None:
    """Reindenting a command changes it; a stray trailing space does not."""
    assert fingerprint("echo hi  \n") == fingerprint("echo hi\n")
    assert fingerprint("echo hi") != fingerprint("  echo hi")


def test_fingerprint_covers_the_language() -> None:
    """Retagging a grandfathered block is a change to what it claims to be."""
    assert fingerprint("echo hi", "bash") != fingerprint("echo hi", "python")


def test_retagging_a_grandfathered_block_revokes_it(tmp_path: Path) -> None:
    """The hole this closes: bash to python kept the hash and passed unmarked."""
    path = _write(tmp_path, UNMARKED)
    baseline_path = tmp_path / "baseline.json"
    write_baseline(baseline_path, scan([tmp_path]))
    baseline = load_baseline(baseline_path)
    assert offending(scan([tmp_path]), baseline) == []

    path.write_text(UNMARKED.replace("```bash", "```python"), encoding="utf-8")
    assert offending(scan([tmp_path]), baseline) != []


def test_a_marker_does_not_carry_to_the_next_block(tmp_path: Path) -> None:
    """The scan stops at a fence, so one block's marker never covers the block after it."""
    _write(tmp_path, VERIFIED + "```bash\necho two\n```\n")
    blocks = scan([tmp_path])
    assert [block.marker for block in blocks] == ["verified", None]
    assert len(offending(blocks, set())) == 1


def test_a_new_copy_of_a_grandfathered_block_still_fails(tmp_path: Path) -> None:
    """docs/berdl-upload.md already repeats a block, so copying one must not be a way in."""
    path = _write(tmp_path, UNMARKED)
    baseline_path = tmp_path / "baseline.json"
    write_baseline(baseline_path, scan([tmp_path]))
    baseline = load_baseline(baseline_path)

    path.write_text(UNMARKED + "\n" + UNMARKED, encoding="utf-8")
    bad = offending(scan([tmp_path]), baseline)
    assert len(bad) == 1, "the second occurrence is new and must be caught"


def test_a_copy_in_another_file_is_not_grandfathered(tmp_path: Path) -> None:
    """A baseline entry grandfathers one occurrence in one file, not the content."""
    _write(tmp_path, UNMARKED)
    baseline_path = tmp_path / "baseline.json"
    write_baseline(baseline_path, scan([tmp_path]))
    baseline = load_baseline(baseline_path)

    _write(tmp_path, UNMARKED, name="elsewhere.md")
    assert len(offending(scan([tmp_path]), baseline)) == 1


def test_sql_is_runnable(tmp_path: Path) -> None:
    """Maintained docs run SELECT statements from sql fences, e.g. docs/pfam_annotation_gff.md."""
    _write(tmp_path, "intro\n\n```sql\nSELECT count(*) FROM t\n```\n")
    assert [block.language for block in scan([tmp_path])] == ["sql"]


def test_the_report_says_what_was_measured(tmp_path: Path) -> None:
    """A failure naming one presumed cause misleads when the count moves the other way."""
    _write(tmp_path, UNMARKED + "\n" + VERIFIED)
    blocks = scan([tmp_path])
    message = report(blocks, set())
    assert "2 runnable blocks" in message
    assert "1 verified" in message
    assert "1 undeclared" in message
    assert "a.md:3" in message


def test_a_clean_report_still_states_the_counts(tmp_path: Path) -> None:
    """Silence on success hides a check that stopped finding anything."""
    _write(tmp_path, VERIFIED)
    message = report(scan([tmp_path]), set())
    assert "1 runnable blocks" in message
    assert "declares neither" not in message


def test_write_baseline_mode_exits_zero_and_creates_the_file(tmp_path: Path) -> None:
    """Adoption is a single deliberate command."""
    _write(tmp_path, UNMARKED)
    baseline_path = tmp_path / "baseline.json"
    assert main([str(tmp_path), "--baseline", str(baseline_path), "--write-baseline"]) == 0
    assert baseline_path.exists()
    assert main([str(tmp_path), "--baseline", str(baseline_path)]) == 0


def test_location_is_a_clickable_reference() -> None:
    """Reviewers act on path:line, not on a bare filename."""
    block = ProcedureBlock(Path("docs/berdl-upload.md"), 317, "python", "abc", None, 0)
    assert block.location == "docs/berdl-upload.md:317"


def test_scan_accepts_a_file_as_well_as_a_directory(tmp_path: Path) -> None:
    """The recipe passes a directory; a reviewer checking one file passes a file."""
    path = _write(tmp_path, UNMARKED)
    assert len(scan([path])) == 1


def test_iter_blocks_reports_the_fence_line(tmp_path: Path) -> None:
    """The reported line must land on the fence, not on the body."""
    blocks = iter_blocks(UNMARKED, Path("a.md"))
    assert [block.line for block in blocks] == [3]
