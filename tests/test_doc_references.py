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
import sys
import types
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts" / "python"))

import doc_references as dr  # noqa: E402


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


def test_a_marker_naming_a_closed_issue_is_reported(tmp_path: Path, monkeypatch) -> None:
    document = _write(
        tmp_path,
        "d.md",
        "<!-- unverified: x, tracked in https://github.com/microbiomedata/nmdc-lakehouse/issues/1 -->\n",
    )
    monkeypatch.setattr(subprocess, "run", _states({"1": "CLOSED"}))

    problems, unreadable = dr.markers_citing_closed_issues([document], "o/r")

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

    problems, _ = dr.markers_citing_closed_issues([document], "o/r")

    assert problems == []


def test_saying_the_issue_closed_is_the_remedy_not_the_defect(tmp_path: Path, monkeypatch) -> None:
    """Reporting a marker that already says "now closed" would report the fix."""
    document = _write(
        tmp_path,
        "d.md",
        "<!-- unverified: x, was https://github.com/microbiomedata/nmdc-lakehouse/issues/1, now closed -->\n",
    )
    monkeypatch.setattr(subprocess, "run", _states({"1": "CLOSED"}))

    problems, _ = dr.markers_citing_closed_issues([document], "o/r")

    assert problems == []


def test_a_bare_issue_reference_is_queried_too(tmp_path: Path, monkeypatch) -> None:
    """doc_procedures accepts `#136`, so matching only URLs let a closed issue evade this rule."""
    document = _write(tmp_path, "d.md", "<!-- unverified: x, see #1 -->\n")
    monkeypatch.setattr(subprocess, "run", _states({"1": "CLOSED"}))

    problems, _ = dr.markers_citing_closed_issues([document], "o/r")

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

    problems, _ = dr.markers_citing_closed_issues([document], "o/r")

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

    problems, unreadable = dr.markers_citing_closed_issues([document], "o/r")

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

    problems, unreadable = dr.markers_citing_closed_issues([document], "o/r")

    assert problems == []
    assert unreadable == {"1"}


def test_a_missing_gh_is_unreadable_rather_than_a_crash(tmp_path: Path, monkeypatch) -> None:
    """Both exits are non-zero, so neither reads as a pass, but they want different responses."""

    def missing(*_args, **_kwargs):
        raise FileNotFoundError("gh")

    document = _write(tmp_path, "d.md", "<!-- unverified: x, see #1 -->\n")
    monkeypatch.setattr(subprocess, "run", missing)

    _problems, unreadable = dr.markers_citing_closed_issues([document], "o/r")

    assert unreadable == {"1"}


def test_a_settled_word_inside_a_negation_does_not_suppress(tmp_path: Path, monkeypatch) -> None:
    """ "not done" contains "done", and the marker presents the work as unfinished.

    `done`, `complete` and `fixed` were in the settled set and are gone, because each reads as
    ordinary English rather than as a statement about issue state. Negation is guarded directly
    rather than by hoping those words never appear inside one.
    """
    document = _write(tmp_path, "d.md", "<!-- unverified: the export is not done; tracked in #1 -->\n")
    monkeypatch.setattr(subprocess, "run", _states({"1": "CLOSED"}))

    problems, _ = dr.markers_citing_closed_issues([document], "o/r")

    assert problems == [(document, 1, "1")], "a live finding must not be suppressed by 'not done'"


def test_saying_it_is_not_closed_yet_does_not_suppress(tmp_path: Path, monkeypatch) -> None:
    """The same trap with the word the rule is actually about."""
    document = _write(tmp_path, "d.md", "<!-- unverified: this is not closed yet, see #1 -->\n")
    monkeypatch.setattr(subprocess, "run", _states({"1": "CLOSED"}))

    problems, _ = dr.markers_citing_closed_issues([document], "o/r")

    assert problems == [(document, 1, "1")]
