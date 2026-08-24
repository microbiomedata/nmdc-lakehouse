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

from markdown_it import MarkdownIt
from markdown_it.token import Token

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


#: Anchored at both ends, so a declaration must *be* a marker comment rather than
#: start like one or merely contain one. An unterminated ``<!-- verified:``, an
#: empty ``<!-- unverified: -->`` and a closed comment trailed by prose all failed
#: to be rejected while this was anchored only at the start.
_MARKER = re.compile(
    r"^<!--\s*(?P<kind>verified|unverified)\s*:\s*(?P<detail>\S.*?)\s*-->$",
    re.IGNORECASE | re.DOTALL,
)

#: CommonMark tokeniser. Hand-written fence matching was tried and abandoned: it
#: diverged from Markdown in seven distinct ways that review had to find, and every
#: one of them was a way for a runnable block to go unseen rather than unflagged.
#: An over-indented line was read as a closing fence, so commands after it fell
#: outside the hash. A block quote's optional space after ``>`` was required to be
#: identical on every line, so a valid close was missed and the block swallowed the
#: next one. Neither is exotic Markdown; both are things a person would type.
_PARSER = MarkdownIt("commonmark")


def _marker_before(tokens: Sequence[Token], index: int) -> str | None:
    """Return the marker kind declared immediately above ``tokens[index]``.

    The token before a fence is the block that precedes it, whatever container
    they sit in, so this needs no notion of quoting or indentation. A paragraph
    of prose is a ``paragraph_close`` and therefore declares nothing, which is
    what stops a document explaining the convention from satisfying it.
    """
    if index == 0:
        return None
    previous = tokens[index - 1]
    if previous.type != "html_block":
        return None
    content = previous.content.strip()
    if _encloses_a_fence(content):
        return None
    match = _MARKER.match(content)
    return match.group("kind").lower() if match else None


def iter_blocks(text: str, path: Path) -> list[ProcedureBlock]:
    """Return every runnable fenced block in ``text``, in document order.

    Fences come from a CommonMark parser, so indentation rules, block quotes,
    nesting and fence lengths are its problem rather than this module's. The body
    is whatever the parser says the block contains, hashed exactly.
    """
    tokens = _PARSER.parse(text)
    blocks: list[ProcedureBlock] = []
    for index, token in enumerate(tokens):
        if token.type != "fence":
            continue
        info = token.info.strip().split(maxsplit=1)
        language = info[0].lower() if info else ""
        if language in INERT_LANGUAGES:
            continue
        blocks.append(
            ProcedureBlock(
                path=path,
                line=token.map[0] + 1 if token.map else 0,
                language=language,
                fingerprint=fingerprint(token.content, language),
                marker=_marker_before(tokens, index),
            )
        )
    return blocks


#: Starts like a marker. Used to catch one that never closes, which CommonMark
#: treats as a comment running to the next "-->" and which therefore swallows any
#: fence beneath it. The document renders wrong and the block disappears from this
#: check at the same time, so a typo would hide a procedure rather than flag it.
_MARKER_START = re.compile(r"^<!--\s*(verified|unverified)\s*:", re.IGNORECASE)


def _encloses_a_fence(comment: str) -> bool:
    """Return whether an HTML comment has swallowed a fenced block.

    The comment runs past the fence to a later close, so Markdown emits no fence
    token and the block is invisible. Matching the marker across newlines is what
    let that pass as a well-formed declaration with a long description.

    The comment's inner text is handed to the same parser rather than matched for
    delimiters. A regular expression here found only top-level fences, so a fence
    inside a block quote or a list, whose lines begin with ">" or with spaces, was
    missed again. That is the mistake this whole change is about, repeated one
    level down.
    """
    inner = comment
    for opening, closing in (("<!--", ""), ("", "-->")):
        if opening and inner.startswith(opening):
            inner = inner[len(opening) :]
        if closing and inner.endswith(closing):
            inner = inner[: -len(closing)]
    return any(token.type == "fence" for token in _PARSER.parse(inner))


def malformed_markers(text: str, path: Path) -> list[tuple[int, str]]:
    """Return ``(line, text)`` for marker comments that are not well formed."""
    found: list[tuple[int, str]] = []
    for token in _PARSER.parse(text):
        if token.type != "html_block":
            continue
        content = token.content.strip()
        if not _MARKER_START.match(content):
            continue
        if _encloses_a_fence(content):
            line = token.map[0] + 1 if token.map else 0
            found.append(
                (
                    line,
                    f"{content.splitlines()[0]}   (encloses a fenced block, which "
                    f"Markdown reads as comment, so that block is invisible)",
                )
            )
            continue
        if _MARKER.match(content):
            continue
        if "-->" not in content:
            reason = "never closed, so Markdown reads everything below it as comment"
        elif not content.endswith("-->"):
            reason = "closed, then followed by more text in the same block"
        else:
            reason = "closed but says nothing after the colon"
        line = token.map[0] + 1 if token.map else 0
        found.append((line, f"{content.splitlines()[0]}   ({reason})"))
    return found


def _relative(path: Path) -> Path:
    """Return ``path`` relative to the working directory when it sits under it."""
    try:
        return path.resolve().relative_to(Path.cwd().resolve())
    except ValueError:
        return path


def _markdown_files(paths: Iterable[Path]) -> list[Path]:
    """Return every Markdown file named by ``paths``, directories expanded."""
    files: list[Path] = []
    for target in paths:
        files.extend(sorted(target.rglob("*.md")) if target.is_dir() else [target])
    return files


def scan(paths: Iterable[Path]) -> list[ProcedureBlock]:
    """Return every runnable block across ``paths``, which may name files or directories."""
    blocks: list[ProcedureBlock] = []
    for markdown in _markdown_files(paths):
        blocks.extend(iter_blocks(markdown.read_text(encoding="utf-8"), _relative(markdown)))
    return blocks


def scan_malformed(paths: Iterable[Path]) -> list[str]:
    """Return a location and text for every malformed marker across ``paths``."""
    found: list[str] = []
    for markdown in _markdown_files(paths):
        text = markdown.read_text(encoding="utf-8")
        for line, snippet in malformed_markers(text, _relative(markdown)):
            found.append(f"{_relative(markdown)}:{line}  {snippet}")
    return found


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


def write_counts(path: Path, counts: dict[str, int]) -> None:
    """Write an allowance mapping to ``path``."""
    document = {
        "baseline_format_version": BASELINE_FORMAT_VERSION,
        "comment": (
            "Undeclared runnable doc blocks that predate the verification-marker "
            "rule. Each key is '<path>::<hash of language and body>'; each value is "
            "how many undeclared copies that file may keep. Do not add entries by "
            "hand: mark the block instead, then run --prune-baseline."
        ),
        "allowances": dict(sorted(counts.items())),
    }
    path.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")


def write_baseline(path: Path, blocks: Iterable[ProcedureBlock]) -> None:
    """Record how many undeclared copies of each body each file may keep."""
    write_counts(path, dict(Counter(block.allowance_key for block in blocks)))


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


def stale_allowances(blocks: Iterable[ProcedureBlock], baseline: dict[str, int]) -> list[str]:
    """Return baseline entries whose body no longer appears in their file.

    Compares counts rather than presence. Membership alone only noticed an entry
    once its last copy was gone, so the one entry in the committed baseline with
    an allowance of two survived intact when one of its two copies was deleted,
    leaving a spare exemption for a body with fewer copies than it is budgeted.

    An allowance outlived its block. Edit a grandfathered block into something
    new and marked, and its old entry stayed in the baseline with nothing to
    spend it on; adding a fresh unmarked copy of the old body then spent it and
    passed. An exemption has to be attached to something that exists, so a
    baseline that no longer describes the tree is an error telling you to
    regenerate it rather than a set of spare permissions.
    """
    present: Counter[str] = Counter(block.allowance_key for block in blocks)
    return sorted(key for key, count in baseline.items() if present[key] < count)


def pruned_baseline(blocks: Iterable[ProcedureBlock], baseline: dict[str, int]) -> dict[str, int]:
    """Return ``baseline`` reduced to what the tree still needs, never increased.

    This is the safe way out of a stale entry, and the only one a contributor
    should need. Marking a grandfathered block changes its hash, which leaves its
    old entry with nothing to spend, and the obvious recovery, regenerating the
    baseline, exempts every undeclared block in the tree including whatever was
    just written. Pruning takes the minimum of the recorded allowance and what is
    actually undeclared now, so it can only shrink: it cannot add a key and cannot
    raise a count.
    """
    undeclared: Counter[str] = Counter(block.allowance_key for block in blocks if block.marker is None)
    kept = {key: min(count, undeclared[key]) for key, count in baseline.items()}
    return {key: count for key, count in kept.items() if count > 0}


def added_by(baseline: dict[str, int], proposed: dict[str, int]) -> list[str]:
    """Return keys ``proposed`` would exempt that ``baseline`` does not already."""
    return sorted(key for key, count in proposed.items() if count > baseline.get(key, 0))


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
    parser.add_argument(
        "--prune-baseline",
        action="store_true",
        help="Drop baseline entries the tree no longer needs. Can only shrink it.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="With --write-baseline, allow exempting blocks that are not exempt today.",
    )
    args = parser.parse_args(argv)
    blocks = scan(args.paths or [Path("docs")])
    if args.prune_baseline:
        existing = load_baseline(args.baseline)
        kept = pruned_baseline(blocks, existing)
        dropped = sum(existing.values()) - sum(kept.values())
        write_counts(args.baseline, kept)
        print(f"pruned {args.baseline}: {dropped} exemption(s) dropped, {sum(kept.values())} left")
        return 0
    if args.write_baseline:
        proposed = dict(Counter(block.allowance_key for block in offending(blocks, {})))
        # Adoption is the case where no baseline file exists yet. After that, any
        # widening needs --force, including from an empty baseline: an emptied file
        # is the normal result of --prune-baseline dropping the last exemption, and
        # testing the mapping for truth rather than the file for existence let that
        # state re-grandfather everything.
        adopting = not args.baseline.exists()
        existing = {} if adopting else load_baseline(args.baseline)
        added = added_by(existing, proposed)
        if added and not adopting and not args.force:
            print(
                f"refusing to write {args.baseline}: it would newly exempt "
                f"{len(added)} block(s) that are not exempt today."
            )
            for key in added:
                print(f"  {key}")
            print(
                "\nMark those blocks instead. Regenerating to clear a failure is how a "
                "gate\nstops applying to the work that tripped it. Use --prune-baseline "
                "to drop\nentries the tree no longer needs, or --force if you really mean "
                "to exempt\nthe blocks listed above."
            )
            return 1
        write_baseline(args.baseline, offending(blocks, {}))
        print(f"wrote {args.baseline} with {sum(proposed.values())} grandfathered blocks")
        return 0
    baseline = load_baseline(args.baseline)
    print(report(blocks, baseline))
    malformed = scan_malformed(args.paths or [Path("docs")])
    if malformed:
        print(f"\n{len(malformed)} marker comments are not usable declarations:")
        for entry in malformed:
            print(f"  {entry}")
    stale = stale_allowances(blocks, baseline)
    if stale:
        print(
            f"\n{len(stale)} baseline entries no longer match any block, so they are exemptions with nothing to exempt:"
        )
        for key in stale:
            print(f"  {key}")
        print(
            "\nThis is normal after marking or editing a grandfathered block. Run with "
            "--prune-baseline\nto drop them: that can only shrink the baseline, so it "
            "cannot exempt anything you have\njust written. Do not use --write-baseline "
            "for this, which would grandfather every\nundeclared block in the tree."
        )
    return 1 if offending(blocks, baseline) or stale or malformed else 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
