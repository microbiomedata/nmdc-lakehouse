"""Require runnable procedures in maintained docs to record whether anyone ran them.

Five review rounds on
https://github.com/microbiomedata/nmdc-lakehouse/pull/285 traced to one cause:
the document described a procedure nobody had executed, and the claims around it
were inferred from unrelated observations rather than from running it. Reviewers
found that five times; nothing in the repository found it once.

A fenced block in a runnable language must carry a marker in the lines just above
it, either a record that it was run::

    <!-- verified: 2026-08-24 staged 53 tables against the nmdc tenant -->

or an explicit statement that it was not::

    <!-- unverified: needs a pod terminal, tracked in
         https://github.com/microbiomedata/nmdc-lakehouse/issues/136 -->

Both pass. The point is not to force execution, which is often impossible from a
workstation, but to stop an unrun procedure from reading like a tested one.

Blocks that already existed when this check landed are grandfathered in a
baseline file. A baseline entry identifies an *occurrence*, by file, content hash
and which repeat within that file it is, rather than identifying content alone.
Content alone was the first design and it was wrong: `docs/berdl-upload.md`
already repeats its `validate-snapshot` block at lines 77 and 260, so a hash-only
baseline would have let any new block pass by copying the body of a grandfathered
one. Identifying occurrences costs a re-baseline when a document is renamed,
which is visible and rare, and buys a rule that a copy cannot walk around.

Editing a grandfathered block changes its hash and brings it under the rule,
which is the behaviour we want: the blocks being changed are the ones being
claimed about. Changing only its language does too, because the language is part
of the hash.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from collections import Counter
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path

BASELINE_FORMAT_VERSION = 2

#: Languages whose blocks are instructions to run something, rather than output,
#: data, or a schema excerpt. A block with no language is treated as inert,
#: because that is how pasted output is written throughout these documents.
RUNNABLE_LANGUAGES = frozenset({"bash", "console", "python", "py", "sh", "shell", "sql", "zsh"})

#: How many non-blank lines above a fence are searched for a marker. The search
#: also stops at the first fence it meets, so a marker cannot be inherited from
#: the block above.
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
    ordinal: int = 0

    @property
    def location(self) -> str:
        """Return a clickable ``path:line`` reference."""
        return f"{self.path}:{self.line}"

    @property
    def identity(self) -> str:
        """Return the baseline key for this occurrence."""
        return f"{self.path.as_posix()}::{self.fingerprint}::{self.ordinal}"


def fingerprint(body: str, language: str = "") -> str:
    """Return a stable hash of a block's language and content.

    Trailing whitespace on a line is noise. Leading whitespace is an edit to the
    command, so it counts. The language counts too: retagging a grandfathered
    block from ``bash`` to ``python`` is a change to what it claims to be.
    """
    # strip("\n") only: a leading space is an edit to the command, not noise.
    normalized = "\n".join(line.rstrip() for line in body.splitlines()).strip("\n")
    return hashlib.sha256(f"{language}\n{normalized}".encode()).hexdigest()


def _marker_above(lines: Sequence[str], fence_index: int) -> str | None:
    """Return the marker kind declared above ``fence_index``, or None.

    The scan stops at the first fence it meets, so the marker on one block is
    never read as covering the block after it.
    """
    seen = 0
    for index in range(fence_index - 1, -1, -1):
        raw = lines[index]
        if _FENCE.match(raw):
            return None
        line = raw.strip()
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
    """Return every runnable fenced block in ``text``, in document order."""
    lines = text.splitlines()
    blocks: list[ProcedureBlock] = []
    seen: Counter[str] = Counter()
    index = 0
    while index < len(lines):
        opening = _FENCE.match(lines[index])
        if opening is None:
            index += 1
            continue
        ticks = opening.group("ticks")
        info = opening.group("info").strip().split(maxsplit=1)
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
        name = info[0].lower() if info else ""
        if name in RUNNABLE_LANGUAGES:
            digest = fingerprint("\n".join(body), name)
            blocks.append(
                ProcedureBlock(
                    path=path,
                    line=index + 1,
                    language=name,
                    fingerprint=digest,
                    marker=_marker_above(lines, index),
                    ordinal=seen[digest],
                )
            )
            seen[digest] += 1
        index = cursor + 1
    return blocks


def _relative(path: Path) -> Path:
    """Return ``path`` relative to the working directory when it sits under it."""
    try:
        return path.resolve().relative_to(Path.cwd().resolve())
    except ValueError:
        return path


def scan(paths: Iterable[Path]) -> list[ProcedureBlock]:
    """Return every runnable block across ``paths``, which may name files or directories."""
    blocks: list[ProcedureBlock] = []
    for target in paths:
        files = sorted(target.rglob("*.md")) if target.is_dir() else [target]
        for markdown in files:
            found = iter_blocks(markdown.read_text(encoding="utf-8"), _relative(markdown))
            blocks.extend(found)
    return blocks


def load_baseline(path: Path) -> set[str]:
    """Return the grandfathered occurrence identities, or an empty set when absent."""
    if not path.exists():
        return set()
    document = json.loads(path.read_text(encoding="utf-8"))
    return set(document.get("occurrences", []))


def write_baseline(path: Path, blocks: Iterable[ProcedureBlock]) -> None:
    """Record the occurrences of ``blocks`` as grandfathered."""
    occurrences = sorted({block.identity for block in blocks})
    document = {
        "baseline_format_version": BASELINE_FORMAT_VERSION,
        "comment": (
            "Runnable doc blocks that predate the verification-marker rule, keyed by "
            "path, content hash and repeat index. Do not add entries by hand: mark "
            "the block instead."
        ),
        "occurrences": occurrences,
    }
    path.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")


def offending(blocks: Iterable[ProcedureBlock], baseline: set[str]) -> list[ProcedureBlock]:
    """Return blocks that declare nothing and are not grandfathered."""
    return [block for block in blocks if block.marker is None and block.identity not in baseline]


def report(blocks: Sequence[ProcedureBlock], baseline: set[str]) -> str:
    """Return a message naming what was measured, not a presumed cause."""
    bad = offending(blocks, baseline)
    counts = {
        "runnable blocks": len(blocks),
        "grandfathered": sum(1 for b in blocks if b.identity in baseline),
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
        grandfathered = offending(blocks, set())
        write_baseline(args.baseline, grandfathered)
        print(f"wrote {args.baseline} with {len(grandfathered)} grandfathered occurrences")
        return 0
    baseline = load_baseline(args.baseline)
    print(report(blocks, baseline))
    return 1 if offending(blocks, baseline) else 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
