"""Tests for the check that references in docs resolve for a reader.

The rejections come first, as in `test_doc_procedures`, for the same reason: a check that cannot
fail reports every run as clean. Both of this checker's rules shipped in a form that could not
fail, and review found both. The declaration exempted a whole file from a line two thirds of the
way down it, and an unreadable issue state printed a note and exited zero, so an unreachable
GitHub was indistinguishable from a clean run.
"""

from __future__ import annotations

import json
import subprocess
import types
from pathlib import Path

import pytest

from scripts.python import doc_references as dr


def _write(tmp_path: Path, name: str, text: str) -> Path:
    path = tmp_path / name
    path.write_text(text, encoding="utf-8")
    return path


def _states(mapping: dict[str, str]):
    """A `gh issue view` stand-in returning the states given, and failing for anything else."""

    def run(args, **_kwargs):
        number = args[3]
        if number not in mapping:
            return types.SimpleNamespace(returncode=1, stdout="")
        return types.SimpleNamespace(returncode=0, stdout=json.dumps({"state": mapping[number]}))

    return run


def test_a_cited_script_that_does_not_exist_is_reported(tmp_path: Path) -> None:
    """The rule that would have caught naming the wrong bootstrap script."""
    document = _write(tmp_path, "d.md", "run `scripts/nope.py` first\n")

    assert dr.unresolvable_scripts([document], tmp_path) == [(document, 1, "scripts/nope.py")]


def test_a_declaration_exempts_only_what_follows_it(tmp_path: Path) -> None:
    """It was file-wide, and berdl-upload.md declares two thirds of the way down.

    That exempted 895 lines of maintained runbook on the strength of a declaration introducing a
    section they are not part of, so a genuinely broken path in the current path would have passed.
    """
    document = _write(
        tmp_path,
        "d.md",
        "cites scripts/above.py\n<!-- external-scripts: other/repo -->\ncites scripts/below.py\n",
    )

    found = dr.unresolvable_scripts([document], tmp_path)

    assert found == [(document, 1, "scripts/above.py")], "above is checked, below is exempt"


def test_a_declaration_can_be_ended_so_it_does_not_run_to_the_end_of_the_file(tmp_path: Path) -> None:
    """berdl-upload.md needs this: it names two external scripts, and then cites one of its own.

    Without an end marker the declaration would run to the end of the file and stop checking that
    citation, and a maintained citation that stops being checked is the thing this check is for.
    """
    document = _write(
        tmp_path,
        "d.md",
        "<!-- external-scripts: other/repo -->\n"
        "cites scripts/theirs.py\n"
        "<!-- /external-scripts -->\n"
        "cites scripts/ours.py\n",
    )

    found = dr.unresolvable_scripts([document], tmp_path)

    assert found == [(document, 4, "scripts/ours.py")], "theirs is exempt, ours is checked again"


def test_an_end_marker_without_a_declaration_changes_nothing(tmp_path: Path) -> None:
    """A stray end marker must not turn checking off, or accidentally on."""
    document = _write(tmp_path, "d.md", "<!-- /external-scripts -->\ncites scripts/ours.py\n")

    assert dr.unresolvable_scripts([document], tmp_path) == [(document, 2, "scripts/ours.py")]


def test_a_marker_naming_a_closed_issue_is_reported(tmp_path: Path, monkeypatch) -> None:
    document = _write(
        tmp_path,
        "d.md",
        "<!-- unverified: x, tracked in https://github.com/microbiomedata/nmdc-lakehouse/issues/1 -->\n",
    )
    monkeypatch.setattr(subprocess, "run", _states({"1": "CLOSED"}))

    problems, unreadable = dr.markers_citing_closed_issues([document])

    assert problems == [(document, 1, "1")]
    assert not unreadable


def test_an_open_issue_is_not_reported(tmp_path: Path, monkeypatch) -> None:
    """The control. A rule that reports every marker is not a rule."""
    document = _write(
        tmp_path,
        "d.md",
        "<!-- unverified: x, tracked in https://github.com/microbiomedata/nmdc-lakehouse/issues/1 -->\n",
    )
    monkeypatch.setattr(subprocess, "run", _states({"1": "OPEN"}))

    problems, _ = dr.markers_citing_closed_issues([document])

    assert problems == []


def test_a_bare_issue_reference_is_queried_too(tmp_path: Path, monkeypatch) -> None:
    """doc_procedures accepts `#136`, so matching only URLs let a closed issue evade this rule."""
    document = _write(tmp_path, "d.md", "<!-- unverified: x, see #1 -->\n")
    monkeypatch.setattr(subprocess, "run", _states({"1": "CLOSED"}))

    problems, _ = dr.markers_citing_closed_issues([document])

    assert problems == [(document, 1, "1")]


def test_a_marker_wrapped_across_lines_is_read_whole(tmp_path: Path, monkeypatch) -> None:
    """The issue is usually not on the line the marker opens, because markers wrap."""
    document = _write(
        tmp_path,
        "d.md",
        "<!-- unverified: a long reason that runs on\n"
        "     and names https://github.com/microbiomedata/nmdc-lakehouse/issues/1 -->\n",
    )
    monkeypatch.setattr(subprocess, "run", _states({"1": "CLOSED"}))

    problems, _ = dr.markers_citing_closed_issues([document])

    assert problems == [(document, 1, "1")], "reported against the line the marker opens"


def test_a_verified_marker_is_not_checked(tmp_path: Path, monkeypatch) -> None:
    """A verified marker naming a closed issue is correct: it closed because verification worked."""
    document = _write(
        tmp_path,
        "d.md",
        "<!-- verified: 2026-08-24 ran it while verifying"
        " https://github.com/microbiomedata/nmdc-lakehouse/issues/1 -->\n",
    )
    monkeypatch.setattr(subprocess, "run", _states({"1": "CLOSED"}))

    problems, unreadable = dr.markers_citing_closed_issues([document])

    assert problems == []
    assert not unreadable, "and it is not even queried"


@pytest.mark.parametrize(
    "response",
    [
        types.SimpleNamespace(returncode=1, stdout=""),
        types.SimpleNamespace(returncode=0, stdout="not json"),
        types.SimpleNamespace(returncode=0, stdout='{"unexpected": "shape"}'),
        types.SimpleNamespace(returncode=0, stdout='{"state": null}'),
        types.SimpleNamespace(returncode=0, stdout='{"state": "TRIAGED"}'),
        types.SimpleNamespace(returncode=0, stdout='{"state": []}'),
    ],
    ids=[
        "non-zero exit",
        "unparseable output",
        "wrong shape",
        "null state",
        "state this checker does not know",
        "unhashable state",
    ],
)
def test_a_state_that_cannot_be_read_is_unreadable_rather_than_absent(tmp_path: Path, monkeypatch, response) -> None:
    """Every one of these must reach the caller as unreadable, which the caller fails on.

    Two of these are valid JSON. `{"state": null}` and an unknown enum parse fine and mean nothing
    to this checker, so recording them would take the number out of `unreadable` and pass it
    silently: the unparseable hole wearing valid JSON.

    The unparseable case used to raise out of `_issue_states`, so one malformed response abandoned
    every remaining issue: the check stopped instead of reporting one unread reference.
    """
    document = _write(tmp_path, "d.md", "<!-- unverified: x, see #1 -->\n")
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: response)

    problems, unreadable = dr.markers_citing_closed_issues([document])

    assert problems == []
    assert unreadable == {"1"}


def test_a_missing_gh_is_unreadable_rather_than_a_crash(tmp_path: Path, monkeypatch) -> None:
    """Both exits are non-zero, so neither reads as a pass, but they want different responses."""

    def missing(*_args, **_kwargs):
        raise FileNotFoundError("gh")

    document = _write(tmp_path, "d.md", "<!-- unverified: x, see #1 -->\n")
    monkeypatch.setattr(subprocess, "run", missing)

    _problems, unreadable = dr.markers_citing_closed_issues([document])

    assert unreadable == {"1"}


def test_one_marker_naming_an_issue_twice_is_reported_once(tmp_path: Path, monkeypatch) -> None:
    """A markdown link matches for the label and for the URL, and it is still one marker."""
    document = _write(
        tmp_path,
        "d.md",
        "<!-- unverified: x, see [#1](https://github.com/microbiomedata/nmdc-lakehouse/issues/1) -->\n",
    )
    monkeypatch.setattr(subprocess, "run", _states({"1": "CLOSED"}))

    problems, _ = dr.markers_citing_closed_issues([document])

    assert problems == [(document, 1, "1")], "one marker, one finding"


def test_an_issue_in_another_repository_is_not_checked_against_this_one(tmp_path: Path, monkeypatch) -> None:
    """Every query goes to one repository, so matching a fork URL would ask about the wrong issue."""
    document = _write(
        tmp_path,
        "d.md",
        "<!-- unverified: x, tracked in https://github.com/a-fork/nmdc-lakehouse/issues/1 -->\n",
    )
    monkeypatch.setattr(subprocess, "run", _states({"1": "CLOSED"}))

    problems, unreadable = dr.markers_citing_closed_issues([document])

    assert problems == []
    assert not unreadable, "and it is not queried at all"


def test_a_dotted_script_name_is_matched(tmp_path: Path) -> None:
    """`scripts/migrate.v2.py` is a legal name, and excluding dots let it pass while missing."""
    document = _write(tmp_path, "d.md", "run `scripts/migrate.v2.py`\n")

    assert dr.unresolvable_scripts([document], tmp_path) == [(document, 1, "scripts/migrate.v2.py")]


def test_a_traversal_component_is_not_read_as_a_filename(tmp_path: Path) -> None:
    """Allowing dots must not turn `..` into a path this reports on."""
    document = _write(tmp_path, "d.md", "run `scripts/../elsewhere.py`\n")

    assert dr.unresolvable_scripts([document], tmp_path) == []


def test_a_markdown_link_to_another_repository_is_not_queried(tmp_path: Path, monkeypatch) -> None:
    """The owner exclusion was bypassed by the ordinary link form.

    The URL alternative correctly ignored a foreign owner, and the link's `#1` label did not, so
    `[#1](https://github.com/a-fork/...)` was still answered with this repository's issue 1.
    """
    document = _write(
        tmp_path,
        "d.md",
        "<!-- unverified: x, see [#1](https://github.com/a-fork/nmdc-lakehouse/issues/1) -->\n",
    )
    monkeypatch.setattr(subprocess, "run", _states({"1": "CLOSED"}))

    problems, unreadable = dr.markers_citing_closed_issues([document])

    assert problems == []
    assert not unreadable


def test_a_markdown_link_to_this_repository_is_still_queried(tmp_path: Path, monkeypatch) -> None:
    """The control. Stripping foreign links must not strip ours."""
    document = _write(
        tmp_path,
        "d.md",
        "<!-- unverified: x, see [#1](https://github.com/microbiomedata/nmdc-lakehouse/issues/1) -->\n",
    )
    monkeypatch.setattr(subprocess, "run", _states({"1": "CLOSED"}))

    problems, _ = dr.markers_citing_closed_issues([document])

    assert problems == [(document, 1, "1")]


def test_a_longer_filename_is_not_matched_by_its_prefix(tmp_path: Path) -> None:
    """`scripts/tool.py.bak` yielded `scripts/tool.py`, which exists, so a missing file passed."""
    (tmp_path / "scripts").mkdir()
    (tmp_path / "scripts" / "tool.py").write_text("", encoding="utf-8")
    document = _write(tmp_path, "d.md", "run `scripts/tool.py.bak` and `scripts/tool.pyc`\n")

    assert dr.unresolvable_scripts([document], tmp_path) == [], "neither is extracted at all"


def test_a_dot_slash_path_is_matched_but_a_nested_one_is_not() -> None:
    """`./scripts/x.py` is the same path; `a/scripts/x.py` and `../scripts/x.py` are not."""
    assert dr.SCRIPT_REFERENCE.findall("run ./scripts/nope.py") == ["scripts/nope.py"]
    assert dr.SCRIPT_REFERENCE.findall("run ../scripts/nope.py") == []
    assert dr.SCRIPT_REFERENCE.findall("see a/scripts/nope.py") == []


def test_saying_the_issue_closed_does_not_excuse_naming_it(tmp_path: Path, monkeypatch) -> None:
    """There is deliberately no settlement escape, and this is the case that removed it.

    Three attempts at reading settlement from prose each produced a narrower wrong answer, and the
    last accepted "#1 is still open and expected to be closed later": a check reporting success
    for something that had not happened, inside a checker built to catch exactly that. The remedy
    is to stop naming the closed issue, which needs no judgement.
    """
    document = _write(tmp_path, "d.md", "<!-- unverified: x, tracked in #1, now closed -->\n")
    monkeypatch.setattr(subprocess, "run", _states({"1": "CLOSED"}))

    problems, _ = dr.markers_citing_closed_issues([document])

    assert problems == [(document, 1, "1")]


def test_a_future_tense_cannot_slip_past(tmp_path: Path, monkeypatch) -> None:
    """The input no regular expression was going to read correctly."""
    document = _write(
        tmp_path,
        "d.md",
        "<!-- unverified: #1 is still open and expected to be closed later -->\n",
    )
    monkeypatch.setattr(subprocess, "run", _states({"1": "CLOSED"}))

    problems, _ = dr.markers_citing_closed_issues([document])

    assert problems == [(document, 1, "1")], "reported, because nothing here judges the sentence"


def test_both_patterns_follow_the_repository_constant(tmp_path: Path, monkeypatch) -> None:
    """They were anchored separately, one through the constant and one by hand.

    A checker that queries one repository and matches another answers a different issue's number
    with confidence, which is the failure the constant exists to prevent. Rebuilding both from a
    changed constant is the only way to see that they still agree.
    """
    import re

    other = "someone/other-repo"
    reference = re.compile(rf"github\.com/{re.escape(other)}/issues/(\d+)|(?<![\w/])#(\d+)\b")
    foreign = re.compile(rf"\[[^\]]*\]\(https?://(?!github\.com/{re.escape(other)}/)[^)]*?/issues/\d+[^)]*\)")

    assert dr.ISSUE_REFERENCE.pattern == reference.pattern.replace(re.escape(other), re.escape(dr.REPOSITORY))
    assert dr.FOREIGN_LINK.pattern == foreign.pattern.replace(re.escape(other), re.escape(dr.REPOSITORY))
