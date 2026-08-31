"""Require runnable procedures in maintained docs to record whether anyone ran them.

Five review rounds on
https://github.com/microbiomedata/nmdc-lakehouse/pull/285 traced to one cause: the
document described a procedure nobody had executed, and the claims around it were
inferred from unrelated observations rather than from running it. Reviewers found
that five times; nothing in the repository found it once.

A fenced block in a language that runs something must carry a marker immediately
above it, either a record that it was run::

    <!-- verified: 2026-08-24 staged 53 tables against the nmdc tenant -->

or a statement that it was not::

    <!-- unverified: needs a pod terminal, tracked in
         https://github.com/microbiomedata/nmdc-lakehouse/issues/136 -->

Both pass. Forcing execution is not the goal and is impossible from a workstation
for most of these. An unrun procedure that reads like a tested one is the failure.

A ``verified`` marker must carry a date, because that is the claim worth checking:
"verified: ok" asserts something with nothing behind it, which is the shape of the
failure this exists to prevent. ``unverified`` needs only a reason, since it
concedes rather than claims.

There is no exemption list. An earlier version grandfathered the blocks that
predated the rule by content hash, and that mechanism produced seven distinct
defects across eight rounds of review, every one of them a way for the check to
stop applying: a copy could inherit an exemption, a prepended copy could take one
over, marking a block could transfer one, and the documented recovery re-exempted
the work that had tripped the gate. All 81 blocks are declared instead, so the
rule is simply that every runnable block says which it is.
"""

from __future__ import annotations

import argparse
import re
import sys
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from markdown_it import MarkdownIt
from markdown_it.token import Token

#: Languages whose blocks are data, output or configuration rather than
#: instructions to run something. Everything else carrying a language tag is
#: treated as runnable, including tags nobody here has used yet.
#:
#: A denylist on purpose. An allowlist made every unlisted language invisible: a
#: new undeclared ``javascript`` fence produced no block at all and passed in
#: silence. Getting an inert language wrong costs a spurious marker on a data
#: block, which a reviewer sees. Getting a runnable one wrong costs silence.
#:
#: A fence with no language stays inert, because that is how pasted output is
#: written throughout these documents.
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

#: Anchored at both ends, so a declaration must *be* a marker comment rather than
#: start like one or merely contain one. Anchoring only the start accepted prose
#: that mentions a marker, an unterminated comment, an empty detail, and a closed
#: comment trailed by unrelated text.
_MARKER = re.compile(
    r"^<!--\s*(?P<kind>verified|unverified)\s*:\s*(?P<detail>\S.*?)\s*-->$",
    re.IGNORECASE | re.DOTALL,
)

#: Starts like a marker. Used to tell a malformed declaration from ordinary prose.
_MARKER_START = re.compile(r"^<!--\s*(verified|unverified)\s*:", re.IGNORECASE)

#: A date-shaped run somewhere in a ``verified`` marker's detail. Shape only; the
#: value is then parsed, because 2026-99-99 has the shape and is not a date.
_DATE = re.compile(r"\b\d{4}-\d{2}-\d{2}\b")

#: Somewhere the work is tracked, accepted from an ``unverified`` marker. A URL or a
#: bare issue reference both count.
_TRACKER = re.compile(r"https?://\S+|#\d+", re.IGNORECASE)

#: Or an explicit statement that no tracking issue is named. The rule originally required a tracker
#: unconditionally, on the reasoning that a marker saying only "nobody ran it" leaves a reader no
#: way to find out whether that is still true. That assumed a tracker always exists. It does not:
#: issue 291 tracked *declaring* these blocks, closed when the declaring was done, and left 80
#: markers pointing at finished work. Inventing an issue so the pointer resolves is worse than
#: saying there is none, because it tells the reader to go and read something that will not help.
#: A marker that names no tracking issue is still complete: the reader knows the state and knows
#: there is no pointer to follow.
#: `\s+` rather than a space: a marker wraps, so the phrase is routinely split across lines with
#: the continuation indented. Matching a literal space passed every one-line fixture and failed on
#: all 80 real markers.
#:
#: Either wording is accepted. "nothing tracks" is a claim about the world and was wrong for at
#: least five markers whose procedures are tracked by open issues; "no tracking issue is named
#: here" is a claim about the marker and is true by construction. Prefer the second when you have
#: not checked, and name the issue when you have. The failure message offers only the second
#: form, because offering both sends an author who has not checked to the stronger claim.
#: `here` is required. Without it the gate accepted "no tracking issue is named", which reads as
#: the start of a sentence that names one somewhere else, and is a weaker statement than the
#: message and CONTRIBUTING both ask for.
_UNTRACKED = re.compile(r"nothing\s+tracks\b|no\s+tracking\s+issue\s+is\s+named\s+here\b", re.IGNORECASE)

#: CommonMark tokeniser. Hand-written fence matching was tried and abandoned: it
#: diverged from Markdown in seven ways review had to find, and every one was a
#: way for a runnable block to go unseen rather than reported. An over-indented
#: line read as a closing fence, a block quote prefix required byte-identical
#: spacing, a quoted fence was invisible. Neither exotic nor rare.
_PARSER = MarkdownIt("commonmark")


@dataclass(frozen=True)
class ProcedureBlock:
    """One fenced block in a runnable language, and what the document says about it."""

    path: Path
    line: int
    language: str
    marker: str | None

    @property
    def location(self) -> str:
        """Return a clickable ``path:line`` reference."""
        return f"{self.path}:{self.line}"


def _encloses_a_fence(comment: str) -> bool:
    """Return whether an HTML comment has swallowed a fenced block.

    The comment runs past the fence to a later close, so Markdown emits no fence
    token and the block is invisible. The inner text goes through the same parser
    rather than being matched for delimiters: a pattern found only top-level
    fences, so one inside a block quote or a list was missed again.
    """
    inner = comment.removeprefix("<!--").removesuffix("-->")
    return any(token.type == "fence" for token in _PARSER.parse(inner))


def marker_fault(comment: str) -> str | None:
    """Return why ``comment`` is not a usable declaration, or None if it is."""
    if not _MARKER_START.match(comment):
        return None
    if _encloses_a_fence(comment):
        return "encloses a fenced block, which Markdown reads as comment, hiding it"
    match = _MARKER.match(comment)
    if match is None:
        if "-->" not in comment:
            return "never closed, so Markdown reads everything below it as comment"
        if not comment.endswith("-->"):
            return "closed, then followed by more text in the same block"
        return "closed but says nothing after the colon"
    kind, detail = match.group("kind").lower(), match.group("detail")
    if kind == "verified":
        found = _DATE.search(detail)
        if found is None:
            return "claims verified without a date; say when it was run and what came back"
        try:
            date.fromisoformat(found.group(0))
        except ValueError:
            return f"names {found.group(0)}, which is not a real date"
    elif not _TRACKER.search(detail) and not _UNTRACKED.search(detail):
        return (
            "says nobody ran it without saying where that is tracked; name the issue, or say that no "
            "tracking issue is named here"
        )
    return None


def _marker_before(tokens: Sequence[Token], index: int) -> str | None:
    """Return the marker kind declared immediately above ``tokens[index]``.

    The token before a fence is the block that precedes it, whatever container
    they sit in, so this needs no notion of quoting or indentation. A paragraph of
    prose is a ``paragraph_close`` and declares nothing, which is what stops a
    document explaining the convention from satisfying it.
    """
    if index == 0:
        return None
    previous = tokens[index - 1]
    if previous.type != "html_block":
        return None
    content = previous.content.strip()
    if marker_fault(content) is not None:
        return None
    match = _MARKER.match(content)
    return match.group("kind").lower() if match else None


def iter_blocks(text: str, path: Path) -> list[ProcedureBlock]:
    """Return every runnable fenced block in ``text``, in document order."""
    tokens = _PARSER.parse(text)
    return [
        ProcedureBlock(
            path=path,
            line=token.map[0] + 1 if token.map else 0,
            language=(token.info.strip().split(maxsplit=1) or [""])[0].lower(),
            marker=_marker_before(tokens, index),
        )
        for index, token in enumerate(tokens)
        if token.type == "fence" and (token.info.strip().split(maxsplit=1) or [""])[0].lower() not in INERT_LANGUAGES
    ]


def malformed_markers(text: str, path: Path) -> list[tuple[int, str]]:
    """Return ``(line, description)`` for marker comments that are not well formed."""
    found: list[tuple[int, str]] = []
    for token in _PARSER.parse(text):
        if token.type != "html_block":
            continue
        content = token.content.strip()
        fault = marker_fault(content)
        if fault is None:
            continue
        line = token.map[0] + 1 if token.map else 0
        found.append((line, f"{content.splitlines()[0]}   ({fault})"))
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
    """Return a location and description for every malformed marker across ``paths``."""
    found: list[str] = []
    for markdown in _markdown_files(paths):
        text = markdown.read_text(encoding="utf-8")
        for line, description in malformed_markers(text, _relative(markdown)):
            found.append(f"{_relative(markdown)}:{line}  {description}")
    return found


def undeclared(blocks: Iterable[ProcedureBlock]) -> list[ProcedureBlock]:
    """Return blocks that say nothing about whether anyone ran them."""
    return [block for block in blocks if block.marker is None]


def report(blocks: Sequence[ProcedureBlock]) -> str:
    """Return a message naming what was measured, not a presumed cause."""
    bad = undeclared(blocks)
    noun = "runnable block" if len(blocks) == 1 else "runnable blocks"
    summary = ", ".join(
        [
            f"{len(blocks)} {noun}",
            f"{sum(1 for b in blocks if b.marker == 'verified')} verified",
            f"{sum(1 for b in blocks if b.marker == 'unverified')} unverified",
            f"{len(bad)} undeclared",
        ]
    )
    if not bad:
        return f"doc procedures: {summary}"
    lines = [f"doc procedures: {summary}", ""]
    lines += [f"  {block.location} ({block.language}) declares neither" for block in bad]
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
    args = parser.parse_args(argv)
    targets = args.paths or [Path("docs")]

    blocks = scan(targets)
    print(report(blocks))
    malformed = scan_malformed(targets)
    if malformed:
        print(f"\n{len(malformed)} marker comments are not usable declarations:")
        for entry in malformed:
            print(f"  {entry}")
    return 1 if undeclared(blocks) or malformed else 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
