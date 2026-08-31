#!/usr/bin/env python
"""Require CI to run every gate `just check` runs.

`just check` depends on a list of recipes and `ci.yml` invokes them as separate steps, which is
deliberate: separate steps give per-step timing and a named failure in the UI that one `just check`
step would lose. The cost is two hand-maintained lists, and they have drifted twice. `ci.yml`
records one of those in a comment of its own: the prose file list diverged, so CI never linted
`CONTRIBUTING.md` while a contributor's local run did.

A gate that exists locally and not in CI is worse than no gate, because a green build asserts it
passed. This compares the two lists so adding a recipe to `check` without adding it to CI fails.

Neither list is parsed by hand. `just --dump --dump-format json` is the recipe graph as `just`
itself sees it, and `ci.yml` is read with a YAML parser. Its `run:` scripts are not parsed as
shell: a gate step is one line matching `just <recipe>` with no operator characters in it. See
`_GATE_STEP` for why that is narrower than it could be.
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path
from typing import NamedTuple

import yaml


#: Recipes CI may skip, each with the reason. An exemption is a claim that needs one, so this maps
#: rather than lists: an unexplained name here is how a gate goes missing on purpose and then stays
#: missing by accident.
class Exemption(NamedTuple):
    """A gate CI runs behind a condition this cannot evaluate, and the condition it expects.

    `condition` is matched against the step's `if` exactly. That is what makes an exemption
    specific rather than a blanket pass: it was enough to have *some* unevaluatable condition, so
    changing the condition to one that never runs kept parity green. Binding the exact string
    means the checker never has to decide whether an expression is true, only whether it is still
    the one that was reviewed.
    """

    condition: str
    reason: str


#: Gates CI runs under a condition, by name. An exemption is a written-down exception, and it is
#: checked: the reason must be non-empty, the recipe must still be in `just check`, CI must not
#: have started running it plainly, and a step must exist carrying exactly this condition.
EXEMPT: dict[str, Exemption] = {
    "diff-cover": Exemption(
        condition="github.event_name == 'pull_request'",
        reason=(
            "a real gate on every pull request, which is where changed-line coverage means "
            "anything; on a push to main it would compare main against main and check nothing"
        ),
    ),
}


def check_dependencies(root: Path) -> list[str]:
    """The recipes `just check` depends on, from `just`'s own dump rather than by reading text."""
    dump = subprocess.run(
        ["just", "--justfile", str(root / "justfile"), "--dump", "--dump-format", "json"],
        capture_output=True,
        text=True,
        check=True,
        cwd=root,
    )
    recipes = json.loads(dump.stdout)["recipes"]
    return [
        dependency["recipe"] if isinstance(dependency, dict) else dependency
        for dependency in recipes["check"]["dependencies"]
    ]


#: A job or step counts only when it can be *proved* to run and to fail the build. Absent or
#: literally false `continue-on-error`, absent or literally true `if`. Anything else, including
#: every `${{ ... }}` expression, is not evaluated and does not count.
#:
#: The earlier version kept two small sets of literal truthy and falsey values and treated
#: everything else as gating, which left `continue-on-error: ${{ 1 == 1 }}` and `if: ${{ 1 == 2 }}`
#: counting as gates while neither can fail the build. Evaluating GitHub expressions is not
#: something this should attempt, so it proves rather than guesses, and an unprovable step is
#: reported missing, which someone sees.
#:
#: `diff-cover` runs under `if: github.event_name == 'pull_request'` and is therefore exempt by
#: name, with that reason recorded in EXEMPT. That is the mechanism working: a conditional gate is
#: a deliberate, written-down exception rather than something the checker quietly decides for you.
_DEFINITELY_BLOCKING = frozenset({None, False, "false", "False"})
_DEFINITELY_RUNS = frozenset({None, True, "true", "True"})


def _can_fail_the_build(node: dict) -> bool:
    """Whether this job or step is provably able to fail the build."""
    return node.get("continue-on-error") in _DEFINITELY_BLOCKING and node.get("if") in _DEFINITELY_RUNS


#: Shells that actually execute the script. GitHub lets a step name any shell, including
#: `shell: /bin/true {0}`, which runs nothing. Absent means the job default, which is bash here.
#:
#: THREAT MODEL, because this keeps coming up: the failure this prevents is drift, someone adding
#: a gate to `just check` and forgetting the CI step. It is not proof against a workflow written
#: to defeat it. Anyone who can edit `ci.yml` to route a gate through a shell that discards it can
#: equally delete the step and the recipe. Hardening past the forms that occur in practice trades
#: real clarity for imagined adversaries.
_REAL_SHELLS = frozenset({None, "bash", "sh", "bash -e {0}", "bash --noprofile --norc -eo pipefail {0}"})

#: A step that runs exactly one gate and nothing else.
#:
#: This is the whole rule, and it replaced a scanner that tried to read arbitrary shell. That
#: scanner was wrong four times in four review rounds, each time in a way that let a gate look
#: enforced when it was not: `just g || true` discards the verdict, `bash -e` exempts the left of
#: the final `&&` so a multi-line block continues past a failed gate, `echo just g` is a mention
#: rather than a call, and `shlex.split` splits on whitespace so `just g ||true` hides the operator
#: in a token. Every fix was correct and the next round found the next case, because reading shell
#: correctly needs a shell parser and there is not one here.
#:
#: So the shell is not read at all. A gate step must be one line that is exactly `just <recipe>`.
#: Every one of the 15 gates in this repository already is. Anything cleverer does not count and
#: has to be listed in EXEMPT with its reason, which is a visible, deliberate act rather than a
#: silent pass. The failure mode is now "a real gate is reported missing", which someone sees and
#: fixes, instead of "a missing gate is reported present", which nobody sees.
_GATE_STEP = re.compile(r"\Ajust (?P<recipe>[A-Za-z0-9_][A-Za-z0-9_-]*)(?P<rest> .*)?\Z")

#: Characters that can put another command's exit status in place of the gate's: `|` covers both
#: pipelines and `||`, `&` covers `&&` and backgrounding, and `;` starts a new command. Tested as
#: characters rather than tokens on purpose. `shlex.split` splits on whitespace, so `just g ||true`
#: became `['just', 'g', '||true']` and the operator hid inside a token; a character test cannot
#: miss it. Arguments are allowed: `just diff-cover "origin/${GITHUB_BASE_REF}"` is a gate.
_STATUS_MAY_BE_DISCARDED = frozenset("|&;")


def _gate_steps(workflow: Path) -> tuple[set[str], dict[str, str]]:
    """Recipes this workflow runs as a gate step, split by whether that step provably blocks.

    The second is the conditional ones, mapped to the exact `if` they carry: gate-shaped steps
    this cannot prove will run, either because the step has a condition or because its job has
    `needs` and a job whose dependency is skipped is skipped too. They are not counted as gates,
    but an exemption has to be backed by one carrying the condition the exemption names, so both
    cases matter to what an exemption is allowed to cover.
    """
    document = yaml.safe_load(workflow.read_text(encoding="utf-8"))
    blocking: set[str] = set()
    conditional: dict[str, str] = {}
    for job in (document.get("jobs") or {}).values():
        # Checked at the job as well as the step. A job whose result is ignored contains no gates
        # however its steps are written.
        if job.get("continue-on-error") not in _DEFINITELY_BLOCKING:
            continue
        # `needs` too. A job whose dependency is skipped is skipped by default, so a gate in it
        # can be absent from a green workflow. Whether the dependency runs is not decidable here,
        # so a job with `needs` is treated as one that might not run rather than one that does.
        job_runs = job.get("if") in _DEFINITELY_RUNS and not job.get("needs")
        for step in job.get("steps") or []:
            if step.get("continue-on-error") not in _DEFINITELY_BLOCKING:
                continue
            if step.get("shell") not in _REAL_SHELLS:
                continue
            script = step.get("run")
            if not isinstance(script, str):
                continue
            line = script.strip()
            if "\n" in line or _STATUS_MAY_BE_DISCARDED & set(line):
                continue
            matched = _GATE_STEP.match(line)
            if not matched:
                continue
            step_if = step.get("if")
            if job_runs and step_if in _DEFINITELY_RUNS:
                blocking.add(matched.group("recipe"))
            elif step_if is not None or job.get("needs"):
                # Recorded with its condition, so an exemption can name the exact one it expects.
                # This does not decide whether the condition is true; `if: false` is recorded the
                # same way, and an exemption naming `false` would simply not match the one on
                # record. Deciding truth is what this deliberately does not do.
                # The job's `needs` when there is no step condition, so an exemption for a gate
                # in a dependent job still names something specific rather than "it is guarded
                # somehow".
                conditional[matched.group("recipe")] = str(step_if) if step_if is not None else f"needs: {job['needs']}"
    return blocking, conditional


def recipes_invoked_by(workflow: Path) -> set[str]:
    """Recipes this workflow runs as a dedicated blocking step.

    A step whose `run` is exactly `just <recipe>`, in a job and step that can be proved to run and
    to fail the build. Deliberately narrower than "every recipe mentioned": see `_GATE_STEP`.
    """
    return _gate_steps(workflow)[0]


def conditional_gates(workflow: Path) -> dict[str, str]:
    """Gate steps this cannot prove will run, mapped to the exact `if` each one carries."""
    return _gate_steps(workflow)[1]


#: The workflow that has to run the gates. Named, not globbed. Globbing every file under
#: `.github/workflows/` let a recipe mentioned in any unrelated workflow satisfy the comparison
#: while `ci.yml` never ran it, which is the exact failure this check exists to catch: a gate that
#: looks covered and is not. Issue 290 asks for every `check` dependency to appear in `ci.yml`.
GATE_WORKFLOW = Path(".github") / "workflows" / "ci.yml"


def missing_from_ci(root: Path) -> list[str]:
    """Recipes `just check` runs that the gate workflow does not, excluding explained exemptions."""
    workflow = root / GATE_WORKFLOW
    if not workflow.is_file():
        raise FileNotFoundError(f"{GATE_WORKFLOW} is missing, so gate parity cannot be established.")
    invoked = recipes_invoked_by(workflow)
    unexplained = sorted(name for name, entry in EXEMPT.items() if not entry.reason.strip())
    if unexplained:
        raise ValueError(
            "An exemption must carry the reason it is not run, and these do not: " + ", ".join(unexplained) + "."
        )
    dependencies = check_dependencies(root)
    # Stale in both directions. A name `check` dropped explains nothing, and a name CI now runs
    # is worse: it would keep the gate exempt, so deleting that CI step later would be masked by
    # an exemption nobody remembers writing.
    departed = sorted(set(EXEMPT) - set(dependencies))
    if departed:
        raise ValueError(
            "These exemptions name recipes `just check` no longer runs, so they explain nothing: "
            + ", ".join(departed)
            + "."
        )
    enforced = sorted(set(EXEMPT) & invoked)
    if enforced:
        raise ValueError(
            "CI runs these, so their exemptions are stale and would hide the gate being removed: "
            + ", ".join(enforced)
            + "."
        )
    # An exemption has to be backed by a step that exists. Without this the exemption is a
    # permanent blind spot: deleting the conditional `just diff-cover` step from ci.yml left every
    # other check satisfied and parity green, which is the drift this whole module is about.
    conditional = conditional_gates(workflow)
    unbacked = sorted(
        name for name, entry in EXEMPT.items() if name not in invoked and conditional.get(name) != entry.condition
    )
    if unbacked:
        raise ValueError(
            "These are exempt but no step in "
            + str(GATE_WORKFLOW)
            + " runs them under exactly the condition the exemption names, so the exemption is "
            "covering their absence or a condition nobody reviewed: " + ", ".join(unbacked) + "."
        )
    return [name for name in dependencies if name not in invoked and name not in EXEMPT]


def main() -> int:
    """Report every gate CI is missing, and exit non-zero if there are any."""
    root = Path(__file__).resolve().parents[2]
    try:
        missing = missing_from_ci(root)
    except (ValueError, FileNotFoundError) as error:
        # A refusal, reported as one. A traceback out of a CI gate reads as the gate crashing
        # rather than as the repository being wrong, and the message is the finding.
        print(f"gate parity: {error}")
        return 1
    if missing:
        print(f"gate parity: {len(missing)} recipe(s) in `just check` that CI does not run")
        for name in missing:
            print(f"  {name}")
        print(
            "\nAdd a step to "
            + str(GATE_WORKFLOW)
            + " running each as `just <recipe>`. EXEMPT is not an alternative to that: it only "
            "covers a gate that already has a step this cannot prove will run, such as one behind "
            "an `if` expression, and it records why."
        )
        return 1
    # Naming the exemptions, because "CI runs all of them" is false when one is exempt, and a
    # success line that overstates what was checked is the same defect this whole check is about.
    total = len(check_dependencies(root))
    if EXEMPT:
        print(f"gate parity: CI runs {total - len(EXEMPT)} of {total} recipe(s) in `just check`")
        for name, entry in sorted(EXEMPT.items()):
            print(f"  exempt: {name} (if: {entry.condition}) -- {entry.reason}")
    else:
        print(f"gate parity: CI runs all {total} recipe(s) in `just check`")
    return 0


if __name__ == "__main__":
    sys.exit(main())
