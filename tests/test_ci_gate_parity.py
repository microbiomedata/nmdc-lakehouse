"""Tests for the check that CI runs every gate `just check` runs.

What these cover is the comparison and the parsing of both sides. What they do not cover is
whether a workflow step succeeds, which only a CI run can say.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from scripts.python import ci_gate_parity as parity


def _workflow(tmp_path: Path, script: str) -> Path:
    path = tmp_path / "w.yml"
    path.write_text(f"jobs:\n  check:\n    steps:\n      - run: |\n{script}\n", encoding="utf-8")
    return path


def test_a_recipe_invoked_by_a_run_step_is_found(tmp_path: Path) -> None:
    assert parity.recipes_invoked_by(_workflow(tmp_path, "          just typecheck")) == {"typecheck"}


def test_every_line_of_a_multi_line_run_step_is_read(tmp_path: Path) -> None:
    """A step that runs two recipes was a single string, and reading only the first missed one."""
    found = parity.recipes_invoked_by(_workflow(tmp_path, "          just bootstrap\n          just lint"))

    assert found == {"bootstrap", "lint"}


def test_a_flag_is_not_mistaken_for_a_recipe(tmp_path: Path) -> None:
    """`just --summary` invokes nothing, and counting `--summary` as a gate would satisfy the
    comparison with a recipe nobody runs."""
    assert parity.recipes_invoked_by(_workflow(tmp_path, "          just --summary")) == set()


def test_a_step_with_no_run_is_skipped(tmp_path: Path) -> None:
    """An `uses:` step has no script, and reading one as an empty string used to raise."""
    path = tmp_path / "w.yml"
    path.write_text("jobs:\n  check:\n    steps:\n      - uses: actions/checkout@v4\n", encoding="utf-8")

    assert parity.recipes_invoked_by(path) == set()


def test_a_workflow_with_no_jobs_is_not_an_error(tmp_path: Path) -> None:
    path = tmp_path / "w.yml"
    path.write_text("name: nothing\n", encoding="utf-8")

    assert parity.recipes_invoked_by(path) == set()


def test_an_unbalanced_quote_does_not_stop_the_scan(tmp_path: Path) -> None:
    """actionlint owns shell syntax. This one must not fail closed on a line it cannot tokenise,
    because that would report every later recipe as missing."""
    found = parity.recipes_invoked_by(_workflow(tmp_path, '          echo "unbalanced\n          just lint'))

    assert found == {"lint"}


def test_this_repository_passes() -> None:
    """The check running against the real pair, which is what CI runs.

    A failure here is not a test problem: it means a gate in `just check` is not run by CI.
    """
    root = Path(__file__).resolve().parents[1]

    assert parity.missing_from_ci(root) == []


def test_check_has_dependencies_to_compare() -> None:
    """A comparison against an empty list passes for the wrong reason.

    If `just --dump` ever changes shape, `check_dependencies` could return nothing and every
    parity run would report success while comparing two empty sets.
    """
    root = Path(__file__).resolve().parents[1]

    assert len(parity.check_dependencies(root)) > 5


@pytest.mark.parametrize("name", ["prose-lint", "test-prose-lint-exit", "check-flat-schema"])
def test_the_three_gates_this_check_was_written_for_are_in_ci(name: str) -> None:
    """The three the issue measured. Named so a regression says which one came back."""
    root = Path(__file__).resolve().parents[1]
    invoked: set[str] = set()
    for workflow in sorted((root / ".github" / "workflows").glob("*.yml")):
        invoked |= parity.recipes_invoked_by(workflow)

    assert name in invoked
