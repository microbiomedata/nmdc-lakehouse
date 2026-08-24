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
baseline file, which records **how many undeclared copies of a given body a given
file is allowed to keep**. Anything past that allowance has to be marked.

Two earlier designs were wrong and are worth recording so they are not tried
again. Keying on content alone let any new block pass by copying the body of a
grandfathered one, which was reachable rather than theoretical:
`docs/berdl-upload.md` already repeats its `validate-snapshot` block at lines 77
and 260. Keying on content plus a repeat index then let a copy *prepended* before
the grandfathered occurrence take over its identity and push the original to a
new one, so the exemption moved to the new block. Counting is immune to both,
because it does not care where in the file anything sits.

The allowance is per file, so a copy pasted into a second document is new and
must be marked. Renaming a document costs a re-baseline, which is visible and
rare.

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

BASELINE_FORMAT_VERSION = 3

#: Languages whose blocks are data, output or configuration rather than
#: instructions to run something. Everything else carrying a language tag is
#: treated as runnable, including tags nobody here has used yet.
#:
#: A denylist on purpose. An allowlist of runnable languages made every unlisted
#: language invisible: a new undeclared ``javascript`` fence produced no block at
#: all and passed in silence. Getting an inert language wrong costs a spurious
#: marker on a data block, which a reviewer sees. Getting a runnable one wrong
#: costs nothing being checked, which nobody sees.
#:
#: A fence with no language stays inert, because that is how pasted output is
#: written throughout these documents: 102 of the 187 fences under docs/ carry no
#: tag at all.
INERT_LANGUAGES = frozenset(
    {
        "",
        "cfg",
        "csv",
        "diff",
        "dotenv",
        "env",
        "html",
        "ini",
        "json",
        "jsonl",
        "log",
        "markdown",
        "md",
        "output",
        "properties",
        "text",
        "toml",
        "tsv",
        "txt",
        "xml",
        "yaml",
        "yml",
    }
)

#: How many lines of the paragraph above a fence are read. A marker may wrap over
#: several lines, so this is not one line; it is a cap so a long document is not
#: scanned backwards without limit.
MARKER_LOOKBACK = 12

_FENCE = re.compile(r"^(?P<indent>\s*)(?P<ticks>`{3,}|~{3,})(?P<info>.*)$")
#: Markdown container prefix: block quote markers and the spaces around them. A
#: fence inside a block quote is a real fence, and was invisible while the fence
#: pattern allowed only whitespace before the delimiter, so an undeclared command
#: written as "> ```bash" passed the gate.
_CONTAINER = re.compile(r"^[ \t]*(?:>[ \t]?)+")
#: Matches the whole paragraph, so it must *be* a marker rather than start like
#: one. Anchoring only the start accepted prose that merely mentions a marker, an
#: unterminated `<!-- verified:`, an empty `<!-- unverified: -->`, and a closed
#: comment followed by unrelated prose. A declaration has to be a complete comment
#: with something said in it.
_MARKER = re.compile(
    r"^<!--\s*(?P<kind>verified|unverified)\s*:\s*(?P<detail>\S.*?)\s*-->$",
    re.IGNORECASE,
)


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

    @property
    def allowance_key(self) -> str:
        """Return the baseline key: which file, and which body.

        Deliberately carries no position. An earlier version keyed on the repeat
        index too, which let a copy prepended above a grandfathered block take
        over its exemption and push the original out of it.
        """
        return f"{self.path.as_posix()}::{self.fingerprint}"


def fingerprint(body: str, language: str = "") -> str:
    """Return a stable hash of a block's language and its content, byte for byte.

    Nothing about the body is normalised. An earlier version stripped trailing
    whitespace as noise, which it is not: in a shell, a backslash followed by a
    space stops escaping the newline, so appending a space to a continuation line
    changes what the command does while leaving the text looking identical.
    ``docs/berdl-upload.md`` alone has 52 continuation lines. Any edit that could
    change behaviour has to change the hash.

    The language counts too, since retagging a block from ``bash`` to ``python``
    is a change to what it claims to be.
    """
    return hashlib.sha256(f"{language}\n{body}".encode()).hexdigest()


def _strip_prefix(line: str, prefix: str) -> str:
    """Return ``line`` without ``prefix``, when it carries exactly that prefix."""
    if prefix and line.startswith(prefix):
        return line[len(prefix) :]
    if prefix and not line.strip():
        return line
    return line


def _marker_above(lines: Sequence[str], fence_index: int, prefix: str = "") -> str | None:
    """Return the marker kind declared above ``fence_index``, or None.

    Reads the paragraph immediately above the fence, skipping blank lines, and
    requires that paragraph to *be* a marker comment rather than to mention one.
    The scan stops at a fence, so the marker on one block is never read as
    covering the block after it.
    """
    index = fence_index - 1
    while index >= 0 and not _strip_prefix(lines[index], prefix).strip():
        index -= 1
    paragraph: list[str] = []
    while index >= 0 and len(paragraph) < MARKER_LOOKBACK:
        candidate = _strip_prefix(lines[index], prefix)
        if not candidate.strip():
            break
        if _FENCE.match(candidate):
            return None
        paragraph.append(candidate.strip())
        index -= 1
    match = _MARKER.match(" ".join(reversed(paragraph)))
    return match.group("kind").lower() if match else None


def iter_blocks(text: str, path: Path) -> list[ProcedureBlock]:
    """Return every runnable fenced block in ``text``, in document order.

    A fence may sit inside a block quote. The prefix is taken from the opening
    fence line and removed from that block's own body and marker only, so a body
    line beginning with ``>`` inside an ordinary fence stays exactly as written.
    Rewriting every line unconditionally was wrong: it made ``output.txt`` and
    ``> output.txt`` hash the same, and those are different commands.
    """
    lines = text.splitlines()
    blocks: list[ProcedureBlock] = []
    index = 0
    while index < len(lines):
        container = _CONTAINER.match(lines[index])
        prefix = container.group(0) if container else ""
        opening = _FENCE.match(lines[index][len(prefix) :])
        if opening is None:
            index += 1
            continue
        ticks = opening.group("ticks")
        info = opening.group("info").strip().split(maxsplit=1)
        body: list[str] = []
        cursor = index + 1
        while cursor < len(lines):
            candidate = _strip_prefix(lines[cursor], prefix)
            closing = _FENCE.match(candidate)
            if (
                closing is not None
                and closing.group("ticks")[0] == ticks[0]
                and len(closing.group("ticks")) >= len(ticks)
                and not closing.group("info").strip()
            ):
                break
            body.append(candidate)
            cursor += 1
        name = info[0].lower() if info else ""
        if name not in INERT_LANGUAGES:
            blocks.append(
                ProcedureBlock(
                    path=path,
                    line=index + 1,
                    language=name,
                    fingerprint=fingerprint("\n".join(body), name),
                    marker=_marker_above(lines, index, prefix),
                )
            )
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


class BaselineFormatError(RuntimeError):
    """Raised when a baseline file is not a format this version understands."""


def load_baseline(path: Path) -> dict[str, int]:
    """Return the per-file undeclared allowances, or an empty mapping when absent.

    A version this build does not understand is an error rather than something to
    read optimistically. Silently trusting an unknown format is how a stale
    baseline would exempt blocks nobody meant to exempt, which is the same shape
    as the failure this whole check exists to catch.
    """
    if not path.exists():
        return {}
    document = json.loads(path.read_text(encoding="utf-8"))
    found = document.get("baseline_format_version")
    if found != BASELINE_FORMAT_VERSION:
        raise BaselineFormatError(
            f"{path} declares baseline_format_version {found!r} and this build reads "
            f"{BASELINE_FORMAT_VERSION}. Regenerate it with --write-baseline, after "
            f"checking that the blocks it exempts are still ones you mean to exempt."
        )
    return {str(key): int(value) for key, value in document.get("allowances", {}).items()}


def write_baseline(path: Path, blocks: Iterable[ProcedureBlock]) -> None:
    """Record how many undeclared copies of each body each file may keep."""
    counts: Counter[str] = Counter(block.allowance_key for block in blocks)
    document = {
        "baseline_format_version": BASELINE_FORMAT_VERSION,
        "comment": (
            "Undeclared runnable doc blocks that predate the verification-marker "
            "rule. Each key is '<path>::<hash of language and body>'; each value is "
            "how many undeclared copies that file may keep. Do not add entries by "
            "hand: mark the block instead."
        ),
        "allowances": dict(sorted(counts.items())),
    }
    path.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")


def offending(blocks: Iterable[ProcedureBlock], baseline: dict[str, int]) -> list[ProcedureBlock]:
    """Return undeclared blocks beyond what their file is allowed to keep.

    Position plays no part. Whether a copy is pasted above or below the block it
    was copied from, the file now holds more undeclared copies of that body than
    the baseline recorded, and the surplus is what gets reported.

    Marking an occurrence spends its file's allowance for that body rather than
    stepping aside from it. Without that, marking the original and prepending an
    unmarked copy left the undeclared count unchanged and moved the exemption onto
    the new text. Byte-identical blocks cannot be told apart, so the rule is that a
    body's copies in one file are declared together or not at all.
    """
    # Materialised first: this function makes two passes, and a generator argument
    # was exhausted by the first, so offending(iter(scan(...))) reported nothing to
    # fix however many undeclared blocks there were.
    blocks = list(blocks)
    declared: Counter[str] = Counter(block.allowance_key for block in blocks if block.marker is not None)
    remaining = {key: max(0, count - declared[key]) for key, count in baseline.items()}
    surplus: list[ProcedureBlock] = []
    for block in blocks:
        if block.marker is not None:
            continue
        key = block.allowance_key
        if remaining.get(key, 0) > 0:
            remaining[key] -= 1
        else:
            surplus.append(block)
    return surplus


def report(blocks: Sequence[ProcedureBlock], baseline: dict[str, int]) -> str:
    """Return a message naming what was measured, not a presumed cause."""
    bad = offending(blocks, baseline)
    undeclared = len(bad)
    total = len(blocks)
    noun = "runnable block" if total == 1 else "runnable blocks"
    summary = ", ".join(
        [
            f"{total} {noun}",
            f"{sum(1 for b in blocks if b.marker is None) - undeclared} grandfathered",
            f"{sum(1 for b in blocks if b.marker == 'verified')} verified",
            f"{sum(1 for b in blocks if b.marker == 'unverified')} unverified",
            f"{undeclared} undeclared",
        ]
    )
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
        grandfathered = offending(blocks, {})
        write_baseline(args.baseline, grandfathered)
        print(f"wrote {args.baseline} with {len(grandfathered)} grandfathered blocks")
        return 0
    baseline = load_baseline(args.baseline)
    print(report(blocks, baseline))
    return 1 if offending(blocks, baseline) else 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
