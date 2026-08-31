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
itself sees it, and `ci.yml` is read with a YAML parser and its `run:` scripts split with
`shlex`, Python's shell-like lexer.
"""

from __future__ import annotations

import json
import re
import shlex
import subprocess
import sys
from pathlib import Path

import yaml

#: Recipes CI may skip, each with the reason. An exemption is a claim that needs one, so this maps
#: rather than lists: an unexplained name here is how a gate goes missing on purpose and then stays
#: missing by accident.
EXEMPT: dict[str, str] = {}


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


#: Shell tokens that mean the recipe's exit status stops deciding the step's. `just g || true`
#: runs the gate and throws away its verdict, which is the silent gate this check exists to catch,
#: so a line containing any of these is not counted as running that gate.
#:
#: `&&` is deliberately absent, and was wrongly included at first. An AND-list exits with the
#: failing command's status: `just g && echo ok` short-circuits when `g` fails and the step fails
#: with it. Rejecting `&&` reported a genuinely enforced gate as missing. `;` and `|` do discard
#: it, because the list or pipeline takes the last command's status, and `&` backgrounds it.
_NON_BLOCKING = frozenset({"||", ";", "|", "&"})

#: Values of `continue-on-error` that mean GitHub ignores the result. The expression form is
#: included because `${{ true }}` is how it is often written and reads as different from `true`.
_IGNORED_RESULT = frozenset({True, "true", "True", "${{ true }}", "${{true}}"})

#: Values of `if` that mean the job or step never runs. Only the statically decidable ones. An
#: arbitrary expression is NOT evaluated and its step is counted: `diff-cover` in this repository
#: runs under `if: github.event_name == 'pull_request'`, and a gate that runs on every pull
#: request is a gate. Guessing at expressions would trade a real hole for a bigger one.
_NEVER_RUNS = frozenset({False, "false", "False", "${{ false }}", "${{false}}"})

#: Start of a heredoc, and the word that ends it. Lines inside one are data being written, not
#: commands being run, so `just something` in a heredoc body is text. Matched on the raw line
#: because `shlex` strips the quotes that distinguish `<<'EOF'` from `<<EOF`.
_HEREDOC = re.compile(r"<<-?\s*[\'\"]?(?P<word>[A-Za-z_][A-Za-z0-9_]*)")


def recipes_invoked_by(workflow: Path) -> set[str]:
    """Every recipe name a `run:` step in this workflow invokes as `just <name>`, blockingly.

    Blockingly is the whole point. A step that cannot fail the build asserts nothing, and counting
    it would let parity pass while a green build says nothing about that gate, which is the
    condition this check exists to prevent rather than a technicality.
    """
    document = yaml.safe_load(workflow.read_text(encoding="utf-8"))
    invoked: set[str] = set()
    for job in (document.get("jobs") or {}).values():
        # Checked at the job as well as the step. A job whose result is ignored, or which never
        # runs, contains no gates however its steps are written, and looking only at steps left
        # that hole open.
        if job.get("continue-on-error") in _IGNORED_RESULT or job.get("if") in _NEVER_RUNS:
            continue
        for step in job.get("steps") or []:
            if step.get("continue-on-error") in _IGNORED_RESULT or step.get("if") in _NEVER_RUNS:
                continue
            script = step.get("run")
            if not script:
                continue
            # Collected first, because whether a line is the script's last command decides what
            # `&&` means on it, and that cannot be known while streaming the lines.
            commands: list[str] = []
            terminator: str | None = None
            for line in script.splitlines():
                # Inside a heredoc body until its terminator. What is written there is data.
                if terminator is not None:
                    if line.strip() == terminator:
                        terminator = None
                    continue
                opened = _HEREDOC.search(line)
                if opened:
                    terminator = opened.group("word")
                if not line.strip() or line.strip().startswith("#"):
                    continue
                commands.append(line)

            for position, line in enumerate(commands):
                is_final = position == len(commands) - 1
                try:
                    words = shlex.split(line)
                except ValueError:
                    # An unbalanced quote is a shell problem, not this checker's; actionlint owns it.
                    continue
                # `just g || true` and friends run the gate and discard its verdict. Rejected for
                # the whole line, because the operator can sit either side of the call.
                if _NON_BLOCKING & set(words):
                    continue
                # Everything after a comment token is not run. A commented-out gate counting as a
                # gate is a false pass, which is the direction that matters here.
                for index, word in enumerate(words):
                    if word.startswith("#"):
                        words = words[:index]
                        break
                # Only where `just` is the command being run: the start of the line, or straight
                # after `&&` when this is the script's last command. `echo just new-gate` mentions
                # a recipe and runs nothing, and counting it is a false pass.
                #
                # The `&&` restriction is the subtle one. GitHub runs `run:` blocks under `bash -e`,
                # and errexit exempts a command that fails on the left of an AND-list. So in
                #
                #     just lint && echo ok
                #     echo later
                #
                # a failing `just lint` does not stop the script, and the script exits with
                # `echo later`, which is 0. The gate ran and its verdict was thrown away. Only when
                # the AND-list is the final command does its status become the script's.
                #
                # Deliberately not a shell parser. Accepting these positions is conservative: an
                # unusual but real invocation is reported missing, which is visible and fixable,
                # while the alternative is a gate that looks covered and is not.
                positions = [0] + [index + 1 for index, word in enumerate(words) if word == "&&"]
                # On a line that is not the script's last command, only the final position is
                # enforced: errexit exempts everything to the left of the last `&&`, so those run
                # and their verdicts are discarded. On the last line the AND-list's status becomes
                # the script's, so every position in it is enforced.
                enforced = positions if is_final else positions[-1:]
                for index in enforced:
                    if index + 1 >= len(words) or words[index] != "just":
                        continue
                    if not words[index + 1].startswith("-"):
                        invoked.add(words[index + 1])
    return invoked


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
    stale = sorted(set(EXEMPT) - set(dependencies))
    if stale:
        raise ValueError(
            "These exemptions name recipes `just check` no longer runs, so they explain nothing: "
            + ", ".join(stale)
            + "."
        )
    return [name for name in dependencies if name not in invoked and name not in EXEMPT]


def main() -> int:
    """Report every gate CI is missing, and exit non-zero if there are any."""
    root = Path(__file__).resolve().parents[2]
    missing = missing_from_ci(root)
    if missing:
        print(f"gate parity: {len(missing)} recipe(s) in `just check` that CI does not run")
        for name in missing:
            print(f"  {name}")
        print("\nAdd a step running each, or add it to EXEMPT with the reason it is not run.")
        return 1
    print(f"gate parity: CI runs all {len(check_dependencies(root))} recipe(s) in `just check`")
    return 0


if __name__ == "__main__":
    sys.exit(main())
