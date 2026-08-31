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


def test_a_recipe_run_only_by_an_unrelated_workflow_does_not_satisfy_parity(tmp_path: Path) -> None:
    """This globbed every workflow, so any file mentioning a recipe made it look covered.

    A gate that looks covered and is not is the exact failure this check exists to catch, and
    issue 290 asks for every `check` dependency to appear in `ci.yml` specifically.
    """
    workflows = tmp_path / ".github" / "workflows"
    workflows.mkdir(parents=True)
    (workflows / "ci.yml").write_text("jobs:\n  check:\n    steps:\n      - run: just lint\n", encoding="utf-8")
    (workflows / "unrelated.yml").write_text(
        "jobs:\n  other:\n    steps:\n      - run: just typecheck\n", encoding="utf-8"
    )
    (tmp_path / "justfile").write_text(
        "check: lint typecheck\n\nlint:\n    @true\n\ntypecheck:\n    @true\n", encoding="utf-8"
    )

    assert parity.missing_from_ci(tmp_path) == ["typecheck"]


def test_a_missing_gate_workflow_is_an_error_rather_than_a_pass(tmp_path: Path) -> None:
    """An absent ci.yml would otherwise mean nothing is invoked, so every recipe reports missing,
    or worse, a future refactor makes the empty set read as agreement. Say which it is."""
    (tmp_path / ".github" / "workflows").mkdir(parents=True)
    (tmp_path / "justfile").write_text("check: lint\n\nlint:\n    @true\n", encoding="utf-8")

    with pytest.raises(FileNotFoundError, match="gate parity cannot be established"):
        parity.missing_from_ci(tmp_path)


@pytest.mark.parametrize(
    "line",
    [
        "          just lint || true",
        "          just lint ; echo done",
        "          just lint | tee out.txt",
        "          just lint &",
    ],
)
def test_a_gate_whose_verdict_is_discarded_does_not_count(tmp_path: Path, line: str) -> None:
    """`just lint || true` runs the gate and throws away the answer.

    Counting it would let parity pass while a green build says nothing about that gate, which is
    the silent-gate condition this check exists to prevent rather than a technicality.
    """
    assert parity.recipes_invoked_by(_workflow(tmp_path, line)) == set()


def test_an_and_list_still_counts(tmp_path: Path) -> None:
    """`&&` was rejected and should not have been.

    An AND-list exits with the failing command's status, so `just lint && echo ok` short-circuits
    when the gate fails and the step fails with it. That is an enforced gate, and reporting it as
    missing is a false alarm on a working setup.
    """
    assert parity.recipes_invoked_by(_workflow(tmp_path, "          just lint && echo ok")) == {"lint"}


def test_a_gate_after_an_and_still_counts(tmp_path: Path) -> None:
    """`echo starting && just lint` takes the gate's status too."""
    assert parity.recipes_invoked_by(_workflow(tmp_path, "          echo go && just lint")) == {"lint"}


@pytest.mark.parametrize("ignored", [True, "true", "${{ true }}"])
def test_a_job_whose_result_is_ignored_contains_no_gates(tmp_path: Path, ignored: object) -> None:
    """Checked at the job, not only the step. Looking only at steps left the job-level hole open."""
    path = tmp_path / "w.yml"
    path.write_text(
        f"jobs:\n  check:\n    continue-on-error: {ignored}\n    steps:\n      - run: just lint\n",
        encoding="utf-8",
    )

    assert parity.recipes_invoked_by(path) == set()


@pytest.mark.parametrize("never", ["false", "${{ false }}"])
def test_a_step_that_never_runs_contains_no_gates(tmp_path: Path, never: str) -> None:
    path = tmp_path / "w.yml"
    path.write_text(
        f"jobs:\n  check:\n    steps:\n      - run: just lint\n        if: {never}\n",
        encoding="utf-8",
    )

    assert parity.recipes_invoked_by(path) == set()


def test_a_conditional_step_that_might_run_still_counts(tmp_path: Path) -> None:
    """An expression is not evaluated, and the step is counted.

    `diff-cover` in this repository runs under `if: github.event_name == 'pull_request'`. A gate
    that runs on every pull request is a gate, and guessing at expressions would trade a real hole
    for a bigger one.
    """
    path = tmp_path / "w.yml"
    path.write_text(
        "jobs:\n  check:\n    steps:\n      - run: just lint\n        if: github.event_name == 'pull_request'\n",
        encoding="utf-8",
    )

    assert parity.recipes_invoked_by(path) == {"lint"}


def test_a_continue_on_error_step_does_not_count(tmp_path: Path) -> None:
    """GitHub is told to ignore the result, so the step cannot fail the build."""
    path = tmp_path / "w.yml"
    path.write_text(
        "jobs:\n  check:\n    steps:\n      - run: just lint\n        continue-on-error: true\n",
        encoding="utf-8",
    )

    assert parity.recipes_invoked_by(path) == set()


def test_a_blocking_step_still_counts(tmp_path: Path) -> None:
    """The rejections above must not reject everything, or parity fails for any input."""
    assert parity.recipes_invoked_by(_workflow(tmp_path, "          just lint")) == {"lint"}


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


def test_a_commented_out_gate_does_not_count(tmp_path: Path) -> None:
    """A commented gate counting as a gate is a false pass, which is the direction that matters."""
    assert parity.recipes_invoked_by(_workflow(tmp_path, "          # just lint")) == set()
    assert parity.recipes_invoked_by(_workflow(tmp_path, "          just lint # just typecheck")) == {"lint"}


def _tiny_repo(tmp_path: Path) -> Path:
    workflows = tmp_path / ".github" / "workflows"
    workflows.mkdir(parents=True)
    (workflows / "ci.yml").write_text("jobs:\n  check:\n    steps:\n      - run: just lint\n", encoding="utf-8")
    (tmp_path / "justfile").write_text(
        "check: lint typecheck\n\nlint:\n    @true\n\ntypecheck:\n    @true\n", encoding="utf-8"
    )
    return tmp_path


def test_an_exemption_without_a_reason_is_refused(tmp_path: Path, monkeypatch) -> None:
    """The comment says an exemption carries its reason and nothing enforced it.

    An unexplained exemption is how a gate goes missing on purpose and then stays missing by
    accident, which is the thing the mapping was chosen over a list to prevent.
    """
    monkeypatch.setattr(parity, "EXEMPT", {"typecheck": "   "})

    with pytest.raises(ValueError, match="must carry the reason"):
        parity.missing_from_ci(_tiny_repo(tmp_path))


def test_an_exemption_for_a_recipe_check_no_longer_runs_is_refused(tmp_path: Path, monkeypatch) -> None:
    """A stale exemption explains nothing and hides that the gate it named is gone."""
    monkeypatch.setattr(parity, "EXEMPT", {"a-recipe-that-left": "it went away"})

    with pytest.raises(ValueError, match="no longer runs"):
        parity.missing_from_ci(_tiny_repo(tmp_path))


def test_an_explained_exemption_is_honoured(tmp_path: Path, monkeypatch) -> None:
    """The rejections above must not reject every exemption, or the mechanism is unusable."""
    monkeypatch.setattr(parity, "EXEMPT", {"typecheck": "run by a separate scheduled workflow"})

    assert parity.missing_from_ci(_tiny_repo(tmp_path)) == []


@pytest.mark.parametrize(
    "line",
    [
        "          echo just new-gate",
        "          echo 'run just new-gate first'",
        "          printf '%s' just new-gate",
    ],
)
def test_a_mention_of_a_recipe_is_not_an_invocation(tmp_path: Path, line: str) -> None:
    """`echo just new-gate` names a recipe and runs nothing.

    Counting it would let adding that recipe to `check` pass parity while CI never executes it,
    which is a false pass and the direction that matters.
    """
    assert parity.recipes_invoked_by(_workflow(tmp_path, line)) == set()


def test_a_heredoc_body_is_data_rather_than_commands(tmp_path: Path) -> None:
    """What a heredoc writes is text. A gate named inside one is not run by naming it."""
    script = "          cat > note.md <<'EOF'\n          just new-gate\n          EOF\n          just lint"

    assert parity.recipes_invoked_by(_workflow(tmp_path, script)) == {"lint"}


def test_the_two_command_positions_still_count(tmp_path: Path) -> None:
    """The rejections must not reject real invocations, or parity fails for any input."""
    assert parity.recipes_invoked_by(_workflow(tmp_path, "          just lint")) == {"lint"}
    assert parity.recipes_invoked_by(_workflow(tmp_path, "          echo go && just lint")) == {"lint"}
