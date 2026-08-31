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


def test_a_recipe_run_only_by_an_unrelated_workflow_does_not_satisfy_parity(tmp_path: Path, monkeypatch) -> None:
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
    # This repository's real exemption names a recipe this synthetic `check` does not have, and
    # the staleness rule would fire on it first. That rule has its own tests.
    monkeypatch.setattr(parity, "EXEMPT", {})

    assert parity.missing_from_ci(tmp_path) == ["typecheck"]


def test_a_missing_gate_workflow_is_an_error_rather_than_a_pass(tmp_path: Path) -> None:
    """An absent ci.yml would otherwise mean nothing is invoked, so every recipe reports missing,
    or worse, a future refactor makes the empty set read as agreement. Say which it is."""
    (tmp_path / ".github" / "workflows").mkdir(parents=True)
    (tmp_path / "justfile").write_text("check: lint\n\nlint:\n    @true\n", encoding="utf-8")

    with pytest.raises(FileNotFoundError, match="gate parity cannot be established"):
        parity.missing_from_ci(tmp_path)


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

    # The gate workflow specifically. Aggregating every workflow here would let this pass because
    # a gate appears somewhere unrelated, which is the hole `missing_from_ci` was changed to close.
    assert name in parity.recipes_invoked_by(root / parity.GATE_WORKFLOW)


def _tiny_repo(tmp_path: Path) -> Path:
    workflows = tmp_path / ".github" / "workflows"
    workflows.mkdir(parents=True)
    # `typecheck` is present but conditional, so an exemption for it can be backed by a real step.
    (workflows / "ci.yml").write_text(
        "jobs:\n  check:\n    steps:\n      - run: just lint\n"
        "      - run: just typecheck\n        if: github.event_name == 'pull_request'\n",
        encoding="utf-8",
    )
    (tmp_path / "justfile").write_text(
        "check: lint typecheck\n\nlint:\n    @true\n\ntypecheck:\n    @true\n", encoding="utf-8"
    )
    return tmp_path


def test_an_exemption_without_a_reason_is_refused(tmp_path: Path, monkeypatch) -> None:
    """The comment says an exemption carries its reason and nothing enforced it.

    An unexplained exemption is how a gate goes missing on purpose and then stays missing by
    accident, which is the thing the mapping was chosen over a list to prevent.
    """
    monkeypatch.setattr(parity, "EXEMPT", {"typecheck": parity.Exemption("github.event_name == 'pull_request'", "   ")})

    with pytest.raises(ValueError, match="must carry the reason"):
        parity.missing_from_ci(_tiny_repo(tmp_path))


def test_an_exemption_for_a_recipe_check_no_longer_runs_is_refused(tmp_path: Path, monkeypatch) -> None:
    """A stale exemption explains nothing and hides that the gate it named is gone."""
    monkeypatch.setattr(parity, "EXEMPT", {"a-recipe-that-left": parity.Exemption("x", "it went away")})

    with pytest.raises(ValueError, match="no longer runs"):
        parity.missing_from_ci(_tiny_repo(tmp_path))


def test_an_explained_exemption_is_honoured(tmp_path: Path, monkeypatch) -> None:
    """The rejections above must not reject every exemption, or the mechanism is unusable."""
    monkeypatch.setattr(
        parity,
        "EXEMPT",
        {"typecheck": parity.Exemption("github.event_name == 'pull_request'", "runs on pull requests only")},
    )

    assert parity.missing_from_ci(_tiny_repo(tmp_path)) == []


@pytest.mark.parametrize(
    ("script", "expected"),
    [
        ("just lint", {"lint"}),
        ('just diff-cover "origin/${GITHUB_BASE_REF}"', {"diff-cover"}),
        ("just lint || true", set()),
        # `shlex.split` split on whitespace, so this hid the operator inside a token. A character
        # test cannot miss it, which is why the rule tests characters rather than tokens.
        ("just lint ||true", set()),
        ("just lint&&echo ok", set()),
        ("just lint ; echo done", set()),
        ("just lint | tee out.txt", set()),
        ("just lint &", set()),
        ("echo just lint", set()),
        ("just lint && echo ok\necho later", set()),
        ("just --summary", set()),
        ("# just lint", set()),
    ],
)
def test_only_a_dedicated_blocking_gate_step_counts(tmp_path: Path, script: str, expected: set[str]) -> None:
    """One rule, replacing a scanner that read shell and was wrong four times in four rounds.

    Every case here is one that scanner got wrong or had to grow a rule for. A gate step is one
    line that runs `just <recipe>` and nothing that can put another command's status in its place.
    Arguments are fine; operators are not. Anything cleverer is reported missing, which someone
    sees and fixes, rather than counted, which nobody sees.
    """
    body = "\n".join("          " + line for line in script.split("\n"))
    path = tmp_path / "w.yml"
    path.write_text(f"jobs:\n  check:\n    steps:\n      - run: |\n{body}\n", encoding="utf-8")

    assert parity.recipes_invoked_by(path) == expected


def test_an_exemption_for_a_gate_ci_already_runs_is_refused(tmp_path: Path, monkeypatch) -> None:
    """Stale in the other direction, and the worse one.

    An exemption naming a gate CI runs keeps it exempt, so deleting that CI step later is masked
    by an exemption nobody remembers writing.
    """
    monkeypatch.setattr(parity, "EXEMPT", {"lint": parity.Exemption("x", "it is run elsewhere")})

    with pytest.raises(ValueError, match="stale and would hide"):
        parity.missing_from_ci(_tiny_repo(tmp_path))


@pytest.mark.parametrize(
    "control",
    [
        "    continue-on-error: true",
        "    continue-on-error: ${{ true }}",
        # The case the literal sets missed: an expression that is true and is not the word `true`.
        "    continue-on-error: ${{ 1 == 1 }}",
        "    if: false",
        "    if: ${{ false }}",
        "    if: ${{ 1 == 2 }}",
        "    if: github.event_name == 'pull_request'",
    ],
)
def test_a_job_that_cannot_be_proved_to_gate_does_not_count(tmp_path: Path, control: str) -> None:
    """Proved, not guessed. Evaluating GitHub expressions is not something this should attempt.

    The earlier version kept literal truthy and falsey sets and treated everything else as gating,
    so `continue-on-error: ${{ 1 == 1 }}` counted as a gate while it cannot fail the build.
    """
    path = tmp_path / "w.yml"
    path.write_text(f"jobs:\n  check:\n{control}\n    steps:\n      - run: just lint\n", encoding="utf-8")

    assert parity.recipes_invoked_by(path) == set()


@pytest.mark.parametrize("control", ["        continue-on-error: false", "        if: true", ""])
def test_a_step_that_is_provably_blocking_counts(tmp_path: Path, control: str) -> None:
    """The rule must not reject everything, or parity fails for any input and proves nothing."""
    path = tmp_path / "w.yml"
    body = (
        f"jobs:\n  check:\n    steps:\n      - run: just lint\n{control}\n"
        if control
        else "jobs:\n  check:\n    steps:\n      - run: just lint\n"
    )
    path.write_text(body, encoding="utf-8")

    assert parity.recipes_invoked_by(path) == {"lint"}


def test_the_conditional_gate_in_this_repository_is_exempt_by_name_with_a_reason() -> None:
    """diff-cover is conditional, so it is a written-down exception rather than a quiet decision.

    This is the exemption mechanism doing its job: the reason is recorded, an empty one is refused,
    and it becomes stale automatically if diff-cover ever leaves `check` or gains a plain step.
    """
    assert "diff-cover" in parity.EXEMPT
    assert "pull_request" in parity.EXEMPT["diff-cover"].condition
    assert parity.EXEMPT["diff-cover"].reason.strip()


def test_an_exemption_covering_a_step_that_does_not_exist_is_refused(tmp_path: Path, monkeypatch) -> None:
    """Without this an exemption is a permanent blind spot.

    Deleting the conditional `just diff-cover` step from ci.yml left every other check satisfied
    and parity green: it was never in `invoked`, so no staleness rule fired, and it was in EXEMPT,
    so it was skipped. An exemption has to be backed by a step that is actually there.
    """
    workflows = tmp_path / ".github" / "workflows"
    workflows.mkdir(parents=True)
    (workflows / "ci.yml").write_text("jobs:\n  check:\n    steps:\n      - run: just lint\n", encoding="utf-8")
    (tmp_path / "justfile").write_text("check: lint gone\n\nlint:\n    @true\n\ngone:\n    @true\n", encoding="utf-8")
    monkeypatch.setattr(parity, "EXEMPT", {"gone": parity.Exemption("x", "used to run under a condition")})

    with pytest.raises(ValueError, match="covering their absence"):
        parity.missing_from_ci(tmp_path)


def test_an_exemption_backed_by_a_conditional_step_is_honoured(tmp_path: Path, monkeypatch) -> None:
    """The rule must not reject every exemption, or the mechanism is unusable and diff-cover fails."""
    workflows = tmp_path / ".github" / "workflows"
    workflows.mkdir(parents=True)
    (workflows / "ci.yml").write_text(
        "jobs:\n  check:\n    steps:\n      - run: just lint\n"
        "      - run: just guarded\n        if: github.event_name == 'pull_request'\n",
        encoding="utf-8",
    )
    (tmp_path / "justfile").write_text(
        "check: lint guarded\n\nlint:\n    @true\n\nguarded:\n    @true\n", encoding="utf-8"
    )
    monkeypatch.setattr(
        parity,
        "EXEMPT",
        {"guarded": parity.Exemption("github.event_name == 'pull_request'", "runs on pull requests only")},
    )

    assert parity.missing_from_ci(tmp_path) == []


def test_a_conditional_gate_is_reported_as_conditional_not_blocking(tmp_path: Path) -> None:
    """The two sets are what let an exemption be checked against a step that exists."""
    path = tmp_path / "w.yml"
    path.write_text(
        "jobs:\n  check:\n    steps:\n      - run: just lint\n"
        "      - run: just guarded\n        if: github.event_name == 'pull_request'\n",
        encoding="utf-8",
    )

    assert parity.recipes_invoked_by(path) == {"lint"}
    assert parity.conditional_gates(path) == {"guarded": "github.event_name == 'pull_request'"}


def test_a_step_that_literally_never_runs_cannot_back_an_exemption(tmp_path: Path, monkeypatch) -> None:
    """Conditional means "might run", not "does not run".

    A literal `if: false` was landing in the conditional set, and since an exemption may be backed
    by a conditional step, that let an exempt gate be switched off by changing its condition to
    false while parity stayed green.
    """
    workflows = tmp_path / ".github" / "workflows"
    workflows.mkdir(parents=True)
    (workflows / "ci.yml").write_text(
        "jobs:\n  check:\n    steps:\n      - run: just lint\n      - run: just guarded\n        if: false\n",
        encoding="utf-8",
    )
    (tmp_path / "justfile").write_text(
        "check: lint guarded\n\nlint:\n    @true\n\nguarded:\n    @true\n", encoding="utf-8"
    )
    monkeypatch.setattr(
        parity,
        "EXEMPT",
        {"guarded": parity.Exemption("github.event_name == 'pull_request'", "runs on pull requests only")},
    )

    with pytest.raises(ValueError, match="covering their absence"):
        parity.missing_from_ci(tmp_path)


def test_a_gate_in_a_job_with_needs_is_not_counted_as_blocking(tmp_path: Path) -> None:
    """A job whose dependency is skipped is skipped by default, so its gate can be absent from a
    green workflow. Whether the dependency runs is not decidable here."""
    path = tmp_path / "w.yml"
    path.write_text(
        "jobs:\n  first:\n    steps:\n      - run: just lint\n"
        "  second:\n    needs: first\n    steps:\n      - run: just typecheck\n",
        encoding="utf-8",
    )

    assert parity.recipes_invoked_by(path) == {"lint"}
    assert parity.conditional_gates(path) == {"typecheck": "needs: first"}


def test_the_parity_check_is_itself_one_of_the_gates_it_watches() -> None:
    """Keeping it out of `check` left the one gate nothing was watching as this one.

    Deleting `run: just ci-gate-parity` from ci.yml is now reported by ci-gate-parity itself.
    """
    root = Path(__file__).resolve().parents[1]

    assert "ci-gate-parity" in parity.check_dependencies(root)
    assert "ci-gate-parity" in parity.recipes_invoked_by(root / parity.GATE_WORKFLOW)


def test_an_exemption_whose_condition_no_longer_matches_is_refused(tmp_path: Path, monkeypatch) -> None:
    """The exemption names the exact condition it expects, so a changed one is not covered.

    Having *some* unevaluatable condition was enough before, so switching the step's `if` to one
    that never runs kept parity green. Matching the exact string means this never has to decide
    whether an expression is true, only whether it is still the one that was reviewed.
    """
    workflows = tmp_path / ".github" / "workflows"
    workflows.mkdir(parents=True)
    (workflows / "ci.yml").write_text(
        "jobs:\n  check:\n    steps:\n      - run: just lint\n      - run: just guarded\n        if: ${{ 1 == 2 }}\n",
        encoding="utf-8",
    )
    (tmp_path / "justfile").write_text(
        "check: lint guarded\n\nlint:\n    @true\n\nguarded:\n    @true\n", encoding="utf-8"
    )
    monkeypatch.setattr(
        parity,
        "EXEMPT",
        {"guarded": parity.Exemption("github.event_name == 'pull_request'", "runs on pull requests only")},
    )

    with pytest.raises(ValueError, match="a condition nobody reviewed"):
        parity.missing_from_ci(tmp_path)


def test_a_step_whose_shell_does_not_run_the_script_is_not_a_gate(tmp_path: Path) -> None:
    """`shell: /bin/true {0}` runs nothing. Absent means the job default, which is bash."""
    path = tmp_path / "w.yml"
    path.write_text(
        "jobs:\n  check:\n    steps:\n      - run: just lint\n        shell: /bin/true {0}\n",
        encoding="utf-8",
    )

    assert parity.recipes_invoked_by(path) == set()
    assert parity.recipes_invoked_by(_workflow(tmp_path, "          just lint")) == {"lint"}


@pytest.mark.parametrize("scope", ["workflow", "job"])
def test_an_inherited_shell_that_runs_nothing_is_not_a_gate(tmp_path: Path, scope: str) -> None:
    """`defaults.run.shell` applies to steps that name no shell of their own.

    Reading only the step treated an inherited `shell: /bin/true {0}` as bash and counted every
    gate under it, which is a false green across a whole workflow rather than one step.
    """
    defaults = "defaults:\n  run:\n    shell: /bin/true {0}\n"
    if scope == "workflow":
        body = defaults + "jobs:\n  check:\n    steps:\n      - run: just lint\n"
    else:
        body = (
            "jobs:\n  check:\n    "
            + defaults.replace("\n  run", "\n      run").replace("\n    shell", "\n        shell")
            + "    steps:\n      - run: just lint\n"
        )
    path = tmp_path / "w.yml"
    path.write_text(body, encoding="utf-8")

    assert parity.recipes_invoked_by(path) == set()


def test_a_normal_workflow_is_unaffected_by_the_shell_resolution(tmp_path: Path) -> None:
    """The rule must not reject steps in workflows that set no defaults at all."""
    assert parity.recipes_invoked_by(_workflow(tmp_path, "          just lint")) == {"lint"}


def test_an_exemption_naming_a_condition_that_never_runs_is_refused(tmp_path: Path, monkeypatch) -> None:
    """A gate switched off is a missing gate, not an exempt one.

    YAML parses `if: false` as the boolean, which was recorded as the string "False", so an
    exemption naming that value would have matched it exactly and passed.
    """
    workflows = tmp_path / ".github" / "workflows"
    workflows.mkdir(parents=True)
    (workflows / "ci.yml").write_text(
        "jobs:\n  check:\n    steps:\n      - run: just lint\n      - run: just guarded\n        if: false\n",
        encoding="utf-8",
    )
    (tmp_path / "justfile").write_text(
        "check: lint guarded\n\nlint:\n    @true\n\nguarded:\n    @true\n", encoding="utf-8"
    )
    monkeypatch.setattr(parity, "EXEMPT", {"guarded": parity.Exemption("False", "switched off")})

    with pytest.raises(ValueError, match="cannot name a condition that never runs"):
        parity.missing_from_ci(tmp_path)


def test_a_job_guard_around_an_exempt_step_is_not_ignored(tmp_path: Path, monkeypatch) -> None:
    """A job with `if: false` around the exempted step recorded only the step condition.

    So an exemption naming that condition matched and parity passed while the gate could never
    run. Every enclosing guard is recorded now, so an exemption has to name the whole thing.
    """
    workflows = tmp_path / ".github" / "workflows"
    workflows.mkdir(parents=True)
    (workflows / "ci.yml").write_text(
        "jobs:\n  other:\n    steps:\n      - run: just lint\n"
        "  gated:\n    if: false\n    steps:\n"
        "      - run: just guarded\n        if: github.event_name == 'pull_request'\n",
        encoding="utf-8",
    )
    (tmp_path / "justfile").write_text(
        "check: lint guarded\n\nlint:\n    @true\n\nguarded:\n    @true\n", encoding="utf-8"
    )
    monkeypatch.setattr(
        parity,
        "EXEMPT",
        {"guarded": parity.Exemption("github.event_name == 'pull_request'", "runs on pull requests only")},
    )

    with pytest.raises(ValueError, match="a condition nobody reviewed"):
        parity.missing_from_ci(tmp_path)


def test_the_recorded_condition_names_every_guard(tmp_path: Path) -> None:
    """What an exemption has to name, shown directly rather than implied by a refusal."""
    path = tmp_path / "w.yml"
    path.write_text(
        "jobs:\n  gated:\n    needs: other\n    steps:\n"
        "      - run: just guarded\n        if: github.event_name == 'pull_request'\n",
        encoding="utf-8",
    )

    assert parity.conditional_gates(path) == {"guarded": "needs: other; github.event_name == 'pull_request'"}


def test_a_never_running_job_guard_means_off_rather_than_conditional(tmp_path: Path) -> None:
    """A gate behind a guard that never runs is in neither set.

    Recording it as conditional let the disabled guard itself be written into EXEMPT as the
    condition expected, and matched exactly. Off is not a kind of conditional.
    """
    path = tmp_path / "w.yml"
    path.write_text(
        "jobs:\n  gated:\n    if: false\n    steps:\n"
        "      - run: just guarded\n        if: github.event_name == 'pull_request'\n",
        encoding="utf-8",
    )

    assert parity.recipes_invoked_by(path) == set()
    assert parity.conditional_gates(path) == {}
