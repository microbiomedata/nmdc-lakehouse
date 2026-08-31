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
itself sees it, and `ci.yml` is read with a YAML parser and its `run:` scripts tokenised with
`shlex`, which is the shell's own tokeniser.
"""

from __future__ import annotations

import json
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


def recipes_invoked_by(workflow: Path) -> set[str]:
    """Every recipe name a `run:` step in this workflow invokes as `just <name>`."""
    document = yaml.safe_load(workflow.read_text(encoding="utf-8"))
    invoked: set[str] = set()
    for job in (document.get("jobs") or {}).values():
        for step in job.get("steps") or []:
            script = step.get("run")
            if not script:
                continue
            for line in script.splitlines():
                if not line.strip():
                    continue
                try:
                    words = shlex.split(line)
                except ValueError:
                    # An unbalanced quote is a shell problem, not this checker's; actionlint owns it.
                    continue
                # A recipe name, not a flag. `just --summary` invokes nothing.
                if words[:1] == ["just"] and len(words) > 1 and not words[1].startswith("-"):
                    invoked.add(words[1])
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
    return [name for name in check_dependencies(root) if name not in invoked and name not in EXEMPT]


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
