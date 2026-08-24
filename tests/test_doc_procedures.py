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

from scripts.python.doc_procedures import (
    BASELINE_FORMAT_VERSION,
    BaselineFormatError,
    ProcedureBlock,
    added_by,
    fingerprint,
    iter_blocks,
    load_baseline,
    main,
    malformed_markers,
    offending,
    pruned_baseline,
    report,
    scan,
    scan_malformed,
    stale_allowances,
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
    assert offending(blocks, {}) == blocks
    assert main([str(tmp_path), "--baseline", str(tmp_path / "absent.json")]) == 1


def test_rejects_every_undeclared_block_not_just_the_first(tmp_path: Path) -> None:
    """A second offender in the same file is reported too."""
    _write(tmp_path, UNMARKED + "\nmore\n\n```python\nprint(1)\n```\n")
    blocks = scan([tmp_path])
    assert len(blocks) == 2
    assert len(offending(blocks, {})) == 2


@pytest.mark.parametrize("text,marker", [(VERIFIED, "verified"), (UNVERIFIED, "unverified")])
def test_either_marker_declares_a_block(tmp_path: Path, text: str, marker: str) -> None:
    """Both markers pass: the rule is that the document says which, not that it was run."""
    _write(tmp_path, text)
    blocks = scan([tmp_path])
    assert [block.marker for block in blocks] == [marker]
    assert offending(blocks, {}) == []
    assert main([str(tmp_path), "--baseline", str(tmp_path / "absent.json")]) == 0


def test_a_marker_separated_by_prose_does_not_count(tmp_path: Path) -> None:
    """A marker with something else between it and the fence belongs to that."""
    _write(
        tmp_path,
        "<!-- verified: 2026-08-24 ran it -->\n\nA paragraph in between.\n\n```bash\necho hi\n```\n",
    )
    assert offending(scan([tmp_path]), {}) != []


def test_an_over_indented_line_cannot_close_a_block(tmp_path: Path) -> None:
    """Markdown allows at most three spaces before a fence; a scanner that allowed
    any indentation stopped early, so commands after it fell outside the hash."""
    plain = iter_blocks("```bash\necho one\n```\n", Path("a.md"))[0]
    injected = iter_blocks("```bash\necho one\n    ```\necho INJECTED\n```\n", Path("a.md"))[0]
    assert plain.fingerprint != injected.fingerprint


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


def test_an_allowance_whose_body_vanished_is_reported(tmp_path: Path) -> None:
    """An exemption must be attached to something that exists.

    Editing a grandfathered block into a new marked body left its entry behind
    with nothing to spend it on, and a later unmarked copy of the old body could
    spend it. The stale entry is now an error telling you to regenerate.
    """
    path = _write(tmp_path, "intro\n\n" + UNMARKED)
    baseline_path = tmp_path / "baseline.json"
    write_baseline(baseline_path, scan([tmp_path]))
    baseline = load_baseline(baseline_path)
    assert stale_allowances(scan([tmp_path]), baseline) == []

    path.write_text(
        "<!-- verified: 2026-08-24 ran it -->\n```bash\necho replacement\n```\n",
        encoding="utf-8",
    )
    assert len(stale_allowances(scan([tmp_path]), baseline)) == 1
    assert main([str(tmp_path), "--baseline", str(baseline_path)]) == 1


def test_blank_lines_do_not_consume_the_lookback(tmp_path: Path) -> None:
    """Markdown separates a comment from a fence by a blank line; that must still count."""
    _write(tmp_path, "<!-- verified: 2026-08-24 ran it -->\n\n\n```bash\necho hi\n```\n")
    assert offending(scan([tmp_path]), {}) == []


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
    block = scan([tmp_path])[0]
    assert document["allowances"] == {block.allowance_key: 1}
    assert block.allowance_key.endswith(f"a.md::{block.fingerprint}")


def test_an_absent_baseline_grandfathers_nothing(tmp_path: Path) -> None:
    """A missing file must not read as permission."""
    assert load_baseline(tmp_path / "nope.json") == {}


def test_fingerprint_treats_trailing_whitespace_as_significant() -> None:
    """A stray trailing space is not noise in a shell.

    This test asserted the opposite until review pointed out why that was wrong:
    a backslash followed by a space stops escaping the newline, so appending a
    space to a continuation line changes what the command does while the text
    looks identical. docs/berdl-upload.md alone has 52 continuation lines.
    """
    assert fingerprint("echo hi  \n") != fingerprint("echo hi\n")
    assert fingerprint("echo hi") != fingerprint("  echo hi")


def test_a_trailing_space_after_a_continuation_changes_the_hash() -> None:
    """The concrete case, spelled out, because the abstract one reads as pedantry."""
    joined = "cmd \\\n  arg"
    broken = "cmd \\   \n  arg"
    assert fingerprint(joined, "bash") != fingerprint(broken, "bash")


def test_an_unknown_language_is_runnable(tmp_path: Path) -> None:
    """An allowlist made every unlisted language invisible rather than checked."""
    _write(tmp_path, "```javascript\nrmEverything()\n```\n")
    assert len(offending(scan([tmp_path]), {})) == 1


def test_a_body_line_starting_with_a_redirect_is_not_a_quote(tmp_path: Path) -> None:
    """Stripping '>' from every line made two different commands hash the same."""
    plain = iter_blocks("```bash\ncat a\noutput.txt\n```\n", Path("a.md"))[0]
    redirect = iter_blocks("```bash\ncat a\n> output.txt\n```\n", Path("a.md"))[0]
    assert plain.fingerprint != redirect.fingerprint


def test_offending_accepts_a_generator(tmp_path: Path) -> None:
    """Two passes over an Iterable argument silently reported nothing to fix."""
    _write(tmp_path, UNMARKED)
    assert len(offending(iter(scan([tmp_path])), {})) == 1


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
    assert len(offending(blocks, {})) == 1


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
    message = report(blocks, {})
    assert "2 runnable blocks" in message
    assert "1 verified" in message
    assert "1 undeclared" in message
    assert "a.md:3" in message


def test_a_clean_report_still_states_the_counts(tmp_path: Path) -> None:
    """Silence on success hides a check that stopped finding anything."""
    _write(tmp_path, VERIFIED)
    message = report(scan([tmp_path]), {})
    assert "1 runnable block," in message
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
    block = ProcedureBlock(Path("docs/berdl-upload.md"), 317, "python", "abc", None)
    assert block.location == "docs/berdl-upload.md:317"


def test_scan_accepts_a_file_as_well_as_a_directory(tmp_path: Path) -> None:
    """The recipe passes a directory; a reviewer checking one file passes a file."""
    path = _write(tmp_path, UNMARKED)
    assert len(scan([path])) == 1


def test_iter_blocks_reports_the_fence_line(tmp_path: Path) -> None:
    """The reported line must land on the fence, not on the body."""
    blocks = iter_blocks(UNMARKED, Path("a.md"))
    assert [block.line for block in blocks] == [3]


def test_a_copy_prepended_above_the_original_is_still_caught(tmp_path: Path) -> None:
    """The hole a repeat-index baseline had: the copy inherited the exemption.

    Keying on file and body with a count carries no position, so it does not
    matter whether the copy lands above or below the block it was copied from.
    """
    path = _write(tmp_path, "intro\n\n" + UNMARKED)
    baseline_path = tmp_path / "baseline.json"
    write_baseline(baseline_path, scan([tmp_path]))
    baseline = load_baseline(baseline_path)

    path.write_text("intro\n\n" + UNMARKED + "\nmiddle\n\n" + UNMARKED, encoding="utf-8")
    assert len(offending(scan([tmp_path]), baseline)) == 1


def test_prose_mentioning_a_marker_is_not_a_declaration(tmp_path: Path) -> None:
    """A document explaining the convention must not thereby satisfy it."""
    _write(tmp_path, "Write <!-- verified: date and result --> above the fence.\n\n" + UNMARKED)
    assert len(offending(scan([tmp_path]), {})) == 1


def test_a_marker_wrapped_over_several_lines_still_declares(tmp_path: Path) -> None:
    """Real markers cite a URL and wrap; the whole paragraph is read."""
    marker = "<!-- unverified: needs a pod terminal,\n     tracked in issue 136 -->\n"
    _write(tmp_path, marker + "```bash\necho hi\n```\n")
    assert offending(scan([tmp_path]), {}) == []


def test_an_unknown_baseline_version_raises_rather_than_being_trusted(tmp_path: Path) -> None:
    """Silently reading an unrecognised format is the failure this check exists to catch."""
    stale = tmp_path / "stale.json"
    stale.write_text(
        json.dumps({"baseline_format_version": BASELINE_FORMAT_VERSION - 1, "occurrences": []}),
        encoding="utf-8",
    )
    with pytest.raises(BaselineFormatError):
        load_baseline(stale)


def test_a_baseline_with_no_version_raises(tmp_path: Path) -> None:
    """A hand-written file with no version is not a format this build understands."""
    stale = tmp_path / "stale.json"
    stale.write_text(json.dumps({"allowances": {}}), encoding="utf-8")
    with pytest.raises(BaselineFormatError):
        load_baseline(stale)


def test_paths_under_the_working_directory_are_recorded_relative(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The committed baseline holds relative paths, so that branch needs a test.

    Every other test writes under `tmp_path`, which is outside the working
    directory, so `_relative` always raised and returned the path unchanged. Line
    coverage read 100% while the branch that actually produces baseline keys had
    never returned.
    """
    (tmp_path / "docs").mkdir()
    (tmp_path / "docs" / "a.md").write_text(UNMARKED, encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    blocks = scan([Path("docs")])
    assert [block.path.as_posix() for block in blocks] == ["docs/a.md"]
    assert blocks[0].allowance_key.startswith("docs/a.md::")


def test_a_single_block_is_not_reported_in_the_plural(tmp_path: Path) -> None:
    """CI and operators read this line; "1 runnable blocks" is what it used to say."""
    _write(tmp_path, UNMARKED)
    message = report(scan([tmp_path]), {})
    assert "1 runnable block," in message
    assert "1 runnable blocks" not in message


def test_marking_one_copy_does_not_free_the_allowance_for_another(tmp_path: Path) -> None:
    """Marking spends the allowance rather than stepping aside from it.

    Without that, the exemption was transferable: mark the grandfathered block,
    prepend an identical unmarked copy, and the undeclared count was unchanged
    while the exemption had moved onto new text. Byte-identical blocks cannot be
    told apart, so copies of one body in one file are declared together or not
    at all.
    """
    path = _write(tmp_path, "intro\n\n" + UNMARKED)
    baseline_path = tmp_path / "baseline.json"
    write_baseline(baseline_path, scan([tmp_path]))
    baseline = load_baseline(baseline_path)

    path.write_text(
        "intro\n\n" + UNMARKED + "\n<!-- verified: 2026-08-24 ran it -->\n" + UNMARKED,
        encoding="utf-8",
    )
    assert len(offending(scan([tmp_path]), baseline)) == 1


def test_marking_the_only_grandfathered_copy_is_still_fine(tmp_path: Path) -> None:
    """Declaring a grandfathered block must not be punished."""
    path = _write(tmp_path, "intro\n\n" + UNMARKED)
    baseline_path = tmp_path / "baseline.json"
    write_baseline(baseline_path, scan([tmp_path]))
    baseline = load_baseline(baseline_path)

    path.write_text("<!-- verified: 2026-08-24 ran it -->\n" + UNMARKED, encoding="utf-8")
    assert offending(scan([tmp_path]), baseline) == []


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
    assert len(offending(scan([tmp_path]), {})) == 1


def test_a_fence_inside_a_block_quote_is_found(tmp_path: Path) -> None:
    """A quoted fence is a real fence, and was invisible to the gate."""
    _write(tmp_path, "intro\n\n> ```bash\n> rm -rf /important\n> ```\n")
    blocks = scan([tmp_path])
    assert [block.language for block in blocks] == ["bash"]
    assert len(offending(blocks, {})) == 1


def test_a_marker_inside_a_block_quote_declares_its_block(tmp_path: Path) -> None:
    """Reading the fence but not the marker would fail every quoted block."""
    _write(
        tmp_path,
        "> <!-- verified: 2026-08-24 ran it -->\n> ```bash\n> echo hi\n> ```\n",
    )
    assert offending(scan([tmp_path]), {}) == []


def test_a_nested_block_quote_is_found(tmp_path: Path) -> None:
    """One level of quoting is not a special case worth hard-coding."""
    _write(tmp_path, "> > ```bash\n> > echo hi\n> > ```\n")
    assert len(offending(scan([tmp_path]), {})) == 1


def test_quoting_a_block_does_not_change_its_fingerprint(tmp_path: Path) -> None:
    """The command is the same command; the quote is presentation."""
    plain = iter_blocks("```bash\necho hi\n```\n", Path("a.md"))[0]
    quoted = iter_blocks("> ```bash\n> echo hi\n> ```\n", Path("a.md"))[0]
    assert plain.fingerprint == quoted.fingerprint


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
    assert main([str(tmp_path), "--baseline", str(tmp_path / "absent.json")]) == 1


def test_a_well_formed_marker_is_not_reported_as_malformed(tmp_path: Path) -> None:
    """The guard must not fire on the thing it is meant to allow."""
    _write(tmp_path, VERIFIED)
    assert scan_malformed([tmp_path]) == []


def test_an_ordinary_html_comment_is_not_a_malformed_marker(tmp_path: Path) -> None:
    """Documents contain comments that have nothing to do with this rule."""
    _write(tmp_path, "<!-- a note to the reader -->\n" + UNMARKED)
    assert scan_malformed([tmp_path]) == []


def test_deleting_one_of_two_grandfathered_copies_is_reported(tmp_path: Path) -> None:
    """Presence is not enough when a key is budgeted for more than one copy.

    The committed baseline has exactly one entry with an allowance of two, the
    repeated validate-snapshot block in docs/berdl-upload.md. Comparing membership
    rather than counts left that entry whole when one of its two copies went, so a
    spare exemption survived for a body that no longer had two copies to spend it.
    """
    block = "```bash\necho twice\n```\n"
    path = _write(tmp_path, block + "\ntext\n\n" + block)
    baseline_path = tmp_path / "baseline.json"
    write_baseline(baseline_path, scan([tmp_path]))
    baseline = load_baseline(baseline_path)
    assert list(baseline.values()) == [2]
    assert stale_allowances(scan([tmp_path]), baseline) == []

    path.write_text(block, encoding="utf-8")
    assert len(stale_allowances(scan([tmp_path]), baseline)) == 1


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
    assert main([str(tmp_path), "--baseline", str(tmp_path / "absent.json")]) == 1


def test_a_marker_that_swallowed_a_block_declares_nothing_after_it(tmp_path: Path) -> None:
    """The comment closes, so the next fence is real. It must not inherit the marker.

    Otherwise hiding one command inside a marker would also silently declare the
    command after it, which turns one concealed block into two.
    """
    _write(
        tmp_path,
        "<!-- verified: ok\n```bash\nrm -rf /important\n```\n-->\n\n```bash\necho real\n```\n",
    )
    blocks = scan([tmp_path])
    assert [block.marker for block in blocks] == [None]
    assert len(offending(blocks, {})) == 1


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
    assert main([str(tmp_path), "--baseline", str(tmp_path / "absent.json")]) == 1


def test_pruning_clears_a_stale_entry_without_exempting_new_work(tmp_path: Path) -> None:
    """The recovery path a contributor is told to use must not disable the gate.

    Marking a grandfathered block leaves its old entry stale. The obvious recovery,
    regenerating, grandfathers every undeclared block in the tree, so following it
    after adding a new block exempts that new block too. Pruning cannot.
    """
    path = _write(tmp_path, "```bash\necho original\n```\n")
    baseline_path = tmp_path / "baseline.json"
    write_baseline(baseline_path, scan([tmp_path]))

    path.write_text(
        "<!-- verified: 2026-08-24 ran it -->\n```bash\necho original\n```\n\n```bash\nrm -rf /brand-new\n```\n",
        encoding="utf-8",
    )
    assert main([str(tmp_path), "--baseline", str(baseline_path), "--prune-baseline"]) == 0
    assert load_baseline(baseline_path) == {}
    assert main([str(tmp_path), "--baseline", str(baseline_path)]) == 1


def test_pruning_never_adds_or_raises_an_allowance(tmp_path: Path) -> None:
    """Its only safety property, asserted rather than described."""
    _write(tmp_path, UNMARKED + "\ntext\n\n" + UNMARKED)
    blocks = scan([tmp_path])
    key = blocks[0].allowance_key
    assert pruned_baseline(blocks, {key: 1}) == {key: 1}
    assert pruned_baseline(blocks, {}) == {}
    assert pruned_baseline(blocks, {key: 5}) == {key: 2}


def test_regenerating_refuses_to_exempt_something_new(tmp_path: Path) -> None:
    """The old advice is now an error naming what it would have hidden."""
    path = _write(tmp_path, "```bash\necho original\n```\n")
    baseline_path = tmp_path / "baseline.json"
    write_baseline(baseline_path, scan([tmp_path]))
    before = load_baseline(baseline_path)

    path.write_text("```bash\necho original\n```\n\n" + UNMARKED, encoding="utf-8")
    assert main([str(tmp_path), "--baseline", str(baseline_path), "--write-baseline"]) == 1
    assert load_baseline(baseline_path) == before

    assert main([str(tmp_path), "--baseline", str(baseline_path), "--write-baseline", "--force"]) == 0
    assert load_baseline(baseline_path) != before


def test_added_by_names_only_what_grows(tmp_path: Path) -> None:
    """Used to decide whether a regenerate is a widening; must not fire on a shrink."""
    assert added_by({"a": 1}, {"a": 1}) == []
    assert added_by({"a": 1}, {"a": 2}) == ["a"]
    assert added_by({"a": 2}, {"a": 1}) == []
    assert added_by({}, {"b": 1}) == ["b"]
