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

import yaml

#: Recipes CI may skip, each with the reason. An exemption is a claim that needs one, so this maps
#: rather than lists: an unexplained name here is how a gate goes missing on purpose and then stays
#: missing by accident.
EXEMPT: dict[str, str] = {
    "diff-cover": (
        "runs under `if: github.event_name == 'pull_request'`, which this cannot prove will run. "
        "It is a real gate on every pull request, which is where changed-line coverage means "
        "anything; on a push to main it would compare main against main and check nothing."
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

#: Literally never runs. Distinguished from "cannot be proved to run", because an exemption may be
#: backed by a step that might run and must not be backed by one that cannot.
_NEVER_RUNS = frozenset({False, "false", "False", "${{ false }}", "${{false}}"})


def _can_fail_the_build(node: dict) -> bool:
    """Whether this job or step is provably able to fail the build."""
    return node.get("continue-on-error") in _DEFINITELY_BLOCKING and node.get("if") in _DEFINITELY_RUNS


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


def _gate_steps(workflow: Path) -> tuple[set[str], set[str]]:
    """Recipes this workflow runs as a gate step, split by whether that step provably blocks.

    The second set is the conditional ones: gate-shaped steps disqualified only by an `if` this
    cannot evaluate. They are not counted as gates, but an exemption has to be backed by one, or
    the exemption would cover a step that is simply gone.
    """
    document = yaml.safe_load(workflow.read_text(encoding="utf-8"))
    blocking: set[str] = set()
    conditional: set[str] = set()
    for job in (document.get("jobs") or {}).values():
        # Checked at the job as well as the step. A job whose result is ignored contains no gates
        # however its steps are written.
        if job.get("continue-on-error") not in _DEFINITELY_BLOCKING:
            continue
        job_runs = job.get("if") in _DEFINITELY_RUNS
        for step in job.get("steps") or []:
            if step.get("continue-on-error") not in _DEFINITELY_BLOCKING:
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
            elif step_if not in _NEVER_RUNS and job.get("if") not in _NEVER_RUNS:
                # Conditional means "might run", not "does not run". A literal `if: false` was
                # landing here, and since an exemption may be backed by a conditional step, that
                # let an exempt gate be switched off by changing its condition to false while
                # parity stayed green.
                conditional.add(matched.group("recipe"))
    return blocking, conditional


def recipes_invoked_by(workflow: Path) -> set[str]:
    """Recipes this workflow runs as a dedicated blocking step.

    A step whose `run` is exactly `just <recipe>`, in a job and step that can be proved to run and
    to fail the build. Deliberately narrower than "every recipe mentioned": see `_GATE_STEP`.
    """
    return _gate_steps(workflow)[0]


def recipes_conditionally_invoked_by(workflow: Path) -> set[str]:
    """Gate steps this cannot prove will run, because of an `if` it will not evaluate."""
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
    unexplained = sorted(name for name, reason in EXEMPT.items() if not str(reason).strip())
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
    unbacked = sorted(set(EXEMPT) - invoked - recipes_conditionally_invoked_by(workflow))
    if unbacked:
        raise ValueError(
            "These are exempt but no step in "
            + str(GATE_WORKFLOW)
            + " runs them at all, so the exemption is covering their absence: "
            + ", ".join(unbacked)
            + "."
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
    # Naming the exemptions, because "CI runs all 15" is false when one of them is exempt, and a
    # success line that overstates what was checked is the same defect this whole check is about.
    total = len(check_dependencies(root))
    if EXEMPT:
        print(f"gate parity: CI runs {total - len(EXEMPT)} of {total} recipe(s) in `just check`")
        for name, reason in sorted(EXEMPT.items()):
            print(f"  exempt: {name} -- {reason}")
    else:
        print(f"gate parity: CI runs all {total} recipe(s) in `just check`")
    return 0


if __name__ == "__main__":
    sys.exit(main())
