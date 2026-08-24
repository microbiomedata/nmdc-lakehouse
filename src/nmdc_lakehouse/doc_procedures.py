"""Require runnable procedures in maintained docs to record whether anyone ran them.

Five review rounds on
https://github.com/microbiomedata/nmdc-lakehouse/pull/285 traced to one cause:
the document described a procedure nobody had executed, and the claims around it
were inferred from unrelated observations rather than from running it. Reviewers
found that five times; nothing in the repository found it once. This check makes
the state visible before merge.

A fenced block in a runnable language must carry a marker in the lines just above
it, either a record that it was run::

    <!-- verified: 2026-08-24 staged 53 tables against the nmdc tenant -->

or an explicit statement that it was not::

    <!-- unverified: needs a pod terminal, tracked in
         https://github.com/microbiomedata/nmdc-lakehouse/issues/136 -->

Both pass. The point is not to force execution, which is often impossible from a
workstation, but to stop an unrun procedure from reading like a tested one.

Blocks that already existed when this check landed are grandfathered by content
hash in a baseline file, so existing documents do not all fail at once. Editing a
grandfathered block changes its hash and brings it under the rule, which is the
behaviour we want: the blocks being changed are the ones being claimed about.

The hash covers block content only, not the file it sits in, so moving a block
between documents keeps it grandfathered. The cost is that a second copy of an
existing unmarked block also passes; that is accepted in exchange for a baseline
that does not churn on every file rename.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path

BASELINE_FORMAT_VERSION = 1

#: Languages whose blocks are instructions to run something, rather than output,
#: data, or a schema excerpt. A block with no language is treated as inert,
#: because that is how pasted output is written throughout these documents.
RUNNABLE_LANGUAGES = frozenset({"bash", "console", "python", "py", "sh", "shell", "zsh"})

#: How many non-blank lines above a fence are searched for a marker.
MARKER_LOOKBACK = 4

_FENCE = re.compile(r"^(?P<indent>\s*)(?P<ticks>`{3,}|~{3,})(?P<info>.*)$")
_MARKER = re.compile(r"<!--\s*(?P<kind>verified|unverified)\s*:", re.IGNORECASE)


@dataclass(frozen=True)
class ProcedureBlock:
    """One fenced block in a runnable language, and what the document says about it."""

    path: Path
    line: int
    language: str
    fingerprint: str
    marker: str | None

    @property
    def location(self) -> str:
        """Return a clickable ``path:line`` reference."""
        return f"{self.path}:{self.line}"


def fingerprint(body: str) -> str:
    """Return a stable hash of a block's content, ignoring trailing whitespace."""
    # strip("\n") only: a leading space is an edit to the command, not noise.
    normalized = "\n".join(line.rstrip() for line in body.splitlines()).strip("\n")
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _marker_above(lines: Sequence[str], fence_index: int) -> str | None:
    """Return the marker kind declared above ``fence_index``, or None."""
    seen = 0
    for index in range(fence_index - 1, -1, -1):
        line = lines[index].strip()
        if not line:
            continue
        match = _MARKER.search(line)
        if match:
            return match.group("kind").lower()
        seen += 1
        if seen >= MARKER_LOOKBACK:
            return None
    return None


def iter_blocks(text: str, path: Path) -> list[ProcedureBlock]:
    """Return every runnable fenced block in ``text``."""
    lines = text.splitlines()
    blocks: list[ProcedureBlock] = []
    index = 0
    while index < len(lines):
        opening = _FENCE.match(lines[index])
        if opening is None:
            index += 1
            continue
        ticks = opening.group("ticks")
        language = opening.group("info").strip().split(maxsplit=1)
        body: list[str] = []
        cursor = index + 1
        while cursor < len(lines):
            closing = _FENCE.match(lines[cursor])
            if (
                closing is not None
                and closing.group("ticks")[0] == ticks[0]
                and len(closing.group("ticks")) >= len(ticks)
                and not closing.group("info").strip()
            ):
                break
            body.append(lines[cursor])
            cursor += 1
        name = language[0].lower() if language else ""
        if name in RUNNABLE_LANGUAGES:
            blocks.append(
                ProcedureBlock(
                    path=path,
                    line=index + 1,
                    language=name,
                    fingerprint=fingerprint("\n".join(body)),
                    marker=_marker_above(lines, index),
                )
            )
        index = cursor + 1
    return blocks


def scan(paths: Iterable[Path]) -> list[ProcedureBlock]:
    """Return every runnable block across ``paths``, which may name files or directories."""
    blocks: list[ProcedureBlock] = []
    for target in paths:
        files = sorted(target.rglob("*.md")) if target.is_dir() else [target]
        for markdown in files:
            blocks.extend(iter_blocks(markdown.read_text(encoding="utf-8"), markdown))
    return blocks


def load_baseline(path: Path) -> set[str]:
    """Return the grandfathered fingerprints, or an empty set when absent."""
    if not path.exists():
        return set()
    document = json.loads(path.read_text(encoding="utf-8"))
    return set(document.get("fingerprints", []))


def write_baseline(path: Path, blocks: Iterable[ProcedureBlock]) -> None:
    """Record the fingerprints of ``blocks`` as grandfathered."""
    fingerprints = sorted({block.fingerprint for block in blocks})
    document = {
        "baseline_format_version": BASELINE_FORMAT_VERSION,
        "comment": (
            "Runnable doc blocks that predate the verification-marker rule. "
            "Do not add entries by hand: mark the block instead."
        ),
        "fingerprints": fingerprints,
    }
    path.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")


def offending(blocks: Iterable[ProcedureBlock], baseline: set[str]) -> list[ProcedureBlock]:
    """Return blocks that declare nothing and are not grandfathered."""
    return [block for block in blocks if block.marker is None and block.fingerprint not in baseline]


def report(blocks: Sequence[ProcedureBlock], baseline: set[str]) -> str:
    """Return a message naming what was measured, not a presumed cause."""
    bad = offending(blocks, baseline)
    counts = {
        "runnable blocks": len(blocks),
        "grandfathered": sum(1 for b in blocks if b.fingerprint in baseline),
        "verified": sum(1 for b in blocks if b.marker == "verified"),
        "unverified": sum(1 for b in blocks if b.marker == "unverified"),
        "undeclared": len(bad),
    }
    summary = ", ".join(f"{value} {name}" for name, value in counts.items())
    if not bad:
        return f"doc procedures: {summary}"
    lines = [f"doc procedures: {summary}", ""]
    for block in bad:
        lines.append(f"  {block.location} ({block.language}) declares neither")
    lines += [
        "",
        "Add one of these on the line above the fence:",
        "  <!-- verified: <date> <what was run and what came back> -->",
        "  <!-- unverified: <why not, and where it is tracked> -->",
    ]
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    """Check ``paths`` and return a process exit code."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("paths", nargs="*", type=Path, default=[Path("docs")])
    parser.add_argument("--baseline", type=Path, default=Path("docs/procedure-baseline.json"))
    parser.add_argument(
        "--write-baseline",
        action="store_true",
        help="Grandfather every undeclared block found now. Use once, when adopting the check.",
    )
    args = parser.parse_args(argv)
    blocks = scan(args.paths or [Path("docs")])
    if args.write_baseline:
        write_baseline(args.baseline, offending(blocks, set()))
        print(f"wrote {args.baseline} with {len(offending(blocks, set()))} grandfathered blocks")
        return 0
    baseline = load_baseline(args.baseline)
    message = report(blocks, baseline)
    print(message)
    return 1 if offending(blocks, baseline) else 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
