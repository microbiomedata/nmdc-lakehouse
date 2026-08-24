"""Tests for the runnable-procedure verification-marker check.

The first test is the one that matters. A check that cannot fail reports every
run as clean, and this repository has shipped that mistake more than once: a
prose-lint invocation that could not match Vale's singular "1 error", and a
verification snippet that iterated its destination so a wholly absent table
passed. So the guard is tested on what it must reject before it is tested on
what it must accept.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from scripts.python.doc_procedures import (
    ProcedureBlock,
    iter_blocks,
    main,
    malformed_markers,
    marker_fault,
    report,
    scan,
    scan_malformed,
    undeclared,
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
    assert undeclared(blocks) == blocks
    assert main([str(tmp_path)]) == 1


def test_rejects_every_undeclared_block_not_just_the_first(tmp_path: Path) -> None:
    """A second offender in the same file is reported too."""
    _write(tmp_path, UNMARKED + "\nmore\n\n```python\nprint(1)\n```\n")
    blocks = scan([tmp_path])
    assert len(blocks) == 2
    assert len(undeclared(blocks)) == 2


@pytest.mark.parametrize("text,marker", [(VERIFIED, "verified"), (UNVERIFIED, "unverified")])
def test_either_marker_declares_a_block(tmp_path: Path, text: str, marker: str) -> None:
    """Both markers pass: the rule is that the document says which, not that it was run."""
    _write(tmp_path, text)
    blocks = scan([tmp_path])
    assert [block.marker for block in blocks] == [marker]
    assert undeclared(blocks) == []
    assert main([str(tmp_path)]) == 0


def test_a_marker_separated_by_prose_does_not_count(tmp_path: Path) -> None:
    """A marker with something else between it and the fence belongs to that."""
    _write(
        tmp_path,
        "<!-- verified: 2026-08-24 ran it -->\n\nA paragraph in between.\n\n```bash\necho hi\n```\n",
    )
    assert undeclared(scan([tmp_path])) != []


def test_an_over_indented_line_cannot_close_a_block(tmp_path: Path) -> None:
    """Markdown allows at most three spaces before a fence.

    A scanner that allowed any indentation stopped at the indented line, so the
    commands after it fell outside the block. The parser keeps them inside it,
    which is what a reader sees.
    """
    _write(tmp_path, "```bash\necho one\n    ```\necho INJECTED\n```\n")
    blocks = scan([tmp_path])
    assert len(blocks) == 1
    assert len(undeclared(blocks)) == 1


def test_a_varying_quote_prefix_does_not_swallow_the_next_block(tmp_path: Path) -> None:
    """The space after ``>`` is optional per line, so requiring it to match exactly
    missed a valid close and let a marked block absorb an undeclared one."""
    _write(
        tmp_path,
        "> <!-- verified: 2026-08-24 ran it -->\n> ```bash\n> echo hi\n>```\n\n```bash\necho UNDECLARED\n```\n",
    )
    blocks = scan([tmp_path])
    assert len(blocks) == 2
    assert [block.marker for block in blocks] == ["verified", None]


def test_a_longer_fence_may_quote_a_shorter_one(tmp_path: Path) -> None:
    """A block that contains a fence is one block, not two."""
    _write(tmp_path, "````bash\n```\necho hi\n````\n")
    assert len(scan([tmp_path])) == 1


def test_blank_lines_do_not_consume_the_lookback(tmp_path: Path) -> None:
    """Markdown separates a comment from a fence by a blank line; that must still count."""
    _write(tmp_path, "<!-- verified: 2026-08-24 ran it -->\n\n\n```bash\necho hi\n```\n")
    assert undeclared(scan([tmp_path])) == []


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


def test_an_unknown_language_is_runnable(tmp_path: Path) -> None:
    """An allowlist made every unlisted language invisible rather than checked."""
    _write(tmp_path, "```javascript\nrmEverything()\n```\n")
    assert len(undeclared(scan([tmp_path]))) == 1


def test_a_marker_does_not_carry_to_the_next_block(tmp_path: Path) -> None:
    """The scan stops at a fence, so one block's marker never covers the block after it."""
    _write(tmp_path, VERIFIED + "```bash\necho two\n```\n")
    blocks = scan([tmp_path])
    assert [block.marker for block in blocks] == ["verified", None]
    assert len(undeclared(blocks)) == 1


def test_sql_is_runnable(tmp_path: Path) -> None:
    """Maintained docs run SELECT statements from sql fences, e.g. docs/pfam_annotation_gff.md."""
    _write(tmp_path, "intro\n\n```sql\nSELECT count(*) FROM t\n```\n")
    assert [block.language for block in scan([tmp_path])] == ["sql"]


def test_the_report_says_what_was_measured(tmp_path: Path) -> None:
    """A failure naming one presumed cause misleads when the count moves the other way."""
    _write(tmp_path, UNMARKED + "\n" + VERIFIED)
    blocks = scan([tmp_path])
    message = report(blocks)
    assert "2 runnable blocks" in message
    assert "1 verified" in message
    assert "1 undeclared" in message
    assert "a.md:3" in message


def test_a_clean_report_still_states_the_counts(tmp_path: Path) -> None:
    """Silence on success hides a check that stopped finding anything."""
    _write(tmp_path, VERIFIED)
    message = report(scan([tmp_path]))
    assert "1 runnable block," in message
    assert "declares neither" not in message


def test_location_is_a_clickable_reference() -> None:
    """Reviewers act on path:line, not on a bare filename."""
    block = ProcedureBlock(Path("docs/berdl-upload.md"), 317, "python", None)
    assert block.location == "docs/berdl-upload.md:317"


def test_scan_accepts_a_file_as_well_as_a_directory(tmp_path: Path) -> None:
    """The recipe passes a directory; a reviewer checking one file passes a file."""
    path = _write(tmp_path, UNMARKED)
    assert len(scan([path])) == 1


def test_iter_blocks_reports_the_fence_line(tmp_path: Path) -> None:
    """The reported line must land on the fence, not on the body."""
    blocks = iter_blocks(UNMARKED, Path("a.md"))
    assert [block.line for block in blocks] == [3]


def test_prose_mentioning_a_marker_is_not_a_declaration(tmp_path: Path) -> None:
    """A document explaining the convention must not thereby satisfy it."""
    _write(tmp_path, "Write <!-- verified: date and result --> above the fence.\n\n" + UNMARKED)
    assert len(undeclared(scan([tmp_path]))) == 1


def test_a_marker_wrapped_over_several_lines_still_declares(tmp_path: Path) -> None:
    """Real markers cite a URL and wrap; the whole paragraph is read."""
    marker = "<!-- unverified: needs a pod terminal,\n     tracked in issue 136 -->\n"
    _write(tmp_path, marker + "```bash\necho hi\n```\n")
    assert undeclared(scan([tmp_path])) == []


def test_a_single_block_is_not_reported_in_the_plural(tmp_path: Path) -> None:
    """CI and operators read this line; "1 runnable blocks" is what it used to say."""
    _write(tmp_path, UNMARKED)
    message = report(scan([tmp_path]))
    assert "1 runnable block," in message
    assert "1 runnable blocks" not in message


@pytest.mark.parametrize(
    "marker",
    [
        "<!-- unverified: -->",
        "<!-- verified:    -->",
        "<!-- verified: ok --> and then some prose",
        "some prose and then <!-- verified: ok -->",
    ],
)
def test_a_malformed_marker_declares_nothing(tmp_path: Path, marker: str) -> None:
    """A declaration is a complete comment with something said in it."""
    _write(tmp_path, marker + "\n" + UNMARKED)
    assert len(undeclared(scan([tmp_path]))) == 1


def test_a_fence_inside_a_block_quote_is_found(tmp_path: Path) -> None:
    """A quoted fence is a real fence, and was invisible to the gate."""
    _write(tmp_path, "intro\n\n> ```bash\n> rm -rf /important\n> ```\n")
    blocks = scan([tmp_path])
    assert [block.language for block in blocks] == ["bash"]
    assert len(undeclared(blocks)) == 1


def test_a_marker_inside_a_block_quote_declares_its_block(tmp_path: Path) -> None:
    """Reading the fence but not the marker would fail every quoted block."""
    _write(
        tmp_path,
        "> <!-- verified: 2026-08-24 ran it -->\n> ```bash\n> echo hi\n> ```\n",
    )
    assert undeclared(scan([tmp_path])) == []


def test_a_nested_block_quote_is_found(tmp_path: Path) -> None:
    """One level of quoting is not a special case worth hard-coding."""
    _write(tmp_path, "> > ```bash\n> > echo hi\n> > ```\n")
    assert len(undeclared(scan([tmp_path]))) == 1


def test_an_unclosed_marker_is_reported_rather_than_silently_hiding_a_block(
    tmp_path: Path,
) -> None:
    """CommonMark reads an unclosed comment as running to the next close.

    So `<!-- verified:` with no `-->` turns the fence below it into comment text.
    The block stops being a block, in the rendered document as well as here, and
    a typo would hide a procedure rather than fail it. That is reported on its own
    terms instead of being counted as an undeclared block, because it is not one.
    """
    _write(tmp_path, "<!-- verified:\n```bash\necho hi\n```\n")
    assert scan([tmp_path]) == []
    assert len(scan_malformed([tmp_path])) == 1
    assert main([str(tmp_path)]) == 1


def test_a_well_formed_marker_is_not_reported_as_malformed(tmp_path: Path) -> None:
    """The guard must not fire on the thing it is meant to allow."""
    _write(tmp_path, VERIFIED)
    assert scan_malformed([tmp_path]) == []


def test_an_ordinary_html_comment_is_not_a_malformed_marker(tmp_path: Path) -> None:
    """Documents contain comments that have nothing to do with this rule."""
    _write(tmp_path, "<!-- a note to the reader -->\n" + UNMARKED)
    assert scan_malformed([tmp_path]) == []


@pytest.mark.parametrize(
    "marker,reason",
    [
        ("<!-- verified:", "encloses a fenced block"),
        ("<!-- unverified: -->", "says nothing after the colon"),
        ("<!-- verified: ok --> and prose", "followed by more text"),
    ],
)
def test_a_malformed_marker_is_diagnosed_by_its_actual_fault(tmp_path: Path, marker: str, reason: str) -> None:
    """One message for three faults told two thirds of readers the wrong thing."""
    found = malformed_markers(marker + "\n" + UNMARKED, Path("a.md"))
    assert len(found) == 1
    assert reason in found[0][1]


def test_an_unclosed_marker_with_no_block_below_says_it_is_unclosed(tmp_path: Path) -> None:
    """Without a fence to swallow, the fault is simply that it never closes."""
    found = malformed_markers("<!-- verified: ran it\n\nordinary prose\n", Path("a.md"))
    assert len(found) == 1
    assert "never closed" in found[0][1]


def test_a_marker_enclosing_a_block_hides_it_and_is_reported(tmp_path: Path) -> None:
    """The dangerous case: the command is inside the comment, so no fence exists.

    Matching the marker across newlines made this a well-formed declaration with a
    long description, so the block was invisible and nothing complained.
    """
    text = "<!-- verified: ok\n```bash\nrm -rf /important\n```\n-->\n"
    _write(tmp_path, text)
    assert scan([tmp_path]) == []
    assert len(scan_malformed([tmp_path])) == 1
    assert main([str(tmp_path)]) == 1


@pytest.mark.parametrize(
    "container,body",
    [
        ("block quote", "> ```bash\n> rm -rf /important\n> ```"),
        ("list item", "  - step\n\n    ```bash\n    rm -rf /important\n    ```"),
        ("top level", "```bash\nrm -rf /important\n```"),
    ],
)
def test_a_marker_enclosing_a_fence_in_any_container_is_caught(tmp_path: Path, container: str, body: str) -> None:
    """Detecting the enclosed fence with a pattern found only top-level ones.

    A fence inside a block quote or a list has lines starting with ">" or with
    spaces, so a delimiter match missed it and the marker was accepted as a
    declaration with a long description, hiding the block. The comment's inner
    text goes through the same parser now, which is the rule this repository
    just wrote down.
    """
    _write(tmp_path, f"<!-- verified: ok\n{body}\n-->\n" + UNMARKED)
    assert len(scan_malformed([tmp_path])) == 1
    assert main([str(tmp_path)]) == 1


def test_a_verified_marker_without_a_date_is_rejected(tmp_path: Path) -> None:
    """ "verified: ok" asserts something with nothing behind it.

    That is the shape of the failure this check exists to prevent, so the claim
    that matters is the one held to a format. An unverified marker concedes rather
    than claims, so it needs only a reason.
    """
    assert marker_fault("<!-- verified: ok -->") is not None
    assert marker_fault("<!-- verified: 2026-08-24 ran it, 53 tables -->") is None
    assert marker_fault("<!-- unverified: needs a pod terminal -->") is None

    _write(tmp_path, "<!-- verified: ok -->\n```bash\necho hi\n```\n")
    blocks = scan([tmp_path])
    assert [block.marker for block in blocks] == [None], "a faulty marker declares nothing"
    assert len(undeclared(blocks)) == 1
    assert len(scan_malformed([tmp_path])) == 1
    assert main([str(tmp_path)]) == 1
