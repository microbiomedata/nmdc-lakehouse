"""Require references in maintained docs to resolve for the reader, not just the author.

Six findings in one day on
https://github.com/microbiomedata/nmdc-lakehouse/pull/310 shared a shape: a file
that is correct on its own and wrong against something it does not mention.
At the time nothing compared one file to another, or a file to GitHub, so every
one of them was found by a person reading two things at once. This module is the
part of that gap now closed: it compares cited paths to the checkout, and cited
issues to GitHub.

Two rules, both cheap:

**A cited script must exist.** `docs/berdl-upload.md` cites five scripts by a
bare ``scripts/`` path that live in a different repository. The surrounding prose
says so, but a reader arriving from a search sees a local-looking path and finds
nothing. That ambiguity cost a real defect: a bootstrap step in
https://github.com/microbiomedata/nmdc-lakehouse/pull/310 named
``scripts/get_minio_creds.py``, which reads credentials, instead of
``scripts/configure_mc.sh``, which sets the alias, and nothing about either path
signalled that both were elsewhere.

**An ``unverified`` marker must say so if the issue it names has closed.** Only
markers, and only ``unverified`` ones. Ordinary prose citing a closed issue is
**not** checked, so a stale sentence elsewhere in a document will not be caught
here and has to be found by reading.

The issue rule needs the network, so it is a separate entry point and not part of
``just check``. The script rule is offline and belongs in the gate.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

# Components may contain dots, so `scripts/migrate.v2.py` is matched rather than silently passing.
# An optional leading `./` is consumed, because `./scripts/x.py` is how people write a path they
# mean to be relative and the left boundary alone made it invisible.
# `.` and `..` are excluded so a traversal component cannot be read as a filename. The trailing
# guard stops a match ending inside a longer name: without it `scripts/tool.py.bak` yielded
# `scripts/tool.py`, which exists, so a citation of a file that does not exist passed.
SCRIPT_REFERENCE = re.compile(
    r"(?<![\w/.-])(?:\./)?(scripts/(?:(?!\.{1,2}/)[\w.-]+/)*(?!\.{1,2}\.)[\w.-]+\.(?:py|sh))(?![\w.-])"
)

#: A document may declare that its ``scripts/`` paths belong to another checkout, which is the
#: honest shape for a runbook whose prose says "from <repo>:" before each block. Rewriting those
#: commands to carry a prefix was tried and reverted: the reader has already changed directory by
#: then, so a qualified path is the one that fails to copy-paste.
#:
#: The declaration applies from its own line forward, not to the whole file. A file-wide reading
#: was the first implementation and it was too weak: `berdl-upload.md` declares at line 896 of
#: 1124, so a broken path anywhere in the 895 maintained lines above it would have been exempted
#: by a declaration introducing a section it is not part of.
EXTERNAL_DECLARATION = re.compile(r"<!--\s*external-scripts:\s*(?P<repo>\S+)\s*-->", re.IGNORECASE)
# Both forms `doc_procedures` accepts. It takes a URL or a bare `#136` (see its `_TRACKER`), so
# matching only URLs left every bare reference unqueried: a marker could name a closed issue as
# `#136`, pass that gate, and never reach this one.
# Anchored to this repository's URLs, because `_issue_states` queries a single `--repo`. A marker
# naming `a-fork/nmdc-lakehouse/issues/12` was matched and then checked against
# `microbiomedata/nmdc-lakehouse#12`, so a differing state produced a confident wrong answer.
ISSUE_REFERENCE = re.compile(r"github\.com/microbiomedata/nmdc-lakehouse/issues/(\d+)|(?<![\w/])#(\d+)\b")

#: A markdown link whose target is an issue somewhere other than this repository. Removed before
#: scanning, because its label is a bare `#N` that would otherwise be queried against this repo,
#: which is how the owner exclusion was bypassed by the ordinary link form.
FOREIGN_LINK = re.compile(
    r"\[[^\]]*\]\(https?://(?!github\.com/microbiomedata/nmdc-lakehouse/)[^)]*?/issues/\d+[^)]*\)"
)
# `unverified:` only. A `verified:` marker naming a closed issue is the normal case and not a
# defect: it records what was being verified, and the issue closed because the verification
# worked. An `unverified:` marker is a live pointer to where an unrun procedure is tracked, so a
# closed target is the pointer going dead. Checked against the data: including verified markers
# reported 83 hits, of which the ones worth acting on were the unverified ones.
MARKER_START = re.compile(r"<!--\s*unverified:", re.IGNORECASE)

#: The only values this checker can act on. Anything else is an unread state, not a new rule.
_KNOWN_STATES = frozenset({"OPEN", "CLOSED"})

# Deliberately not a tense heuristic. A first attempt matched phrases like "tracked in" and
# "future work", and missed three of five known cases because a marker reading "Declaring the 81
# blocks that predate this rule is <url>" carries no such phrase. Guessing whether prose treats a
# reference as live is the wrong problem.
#
# The rule is inverted instead: cite a closed issue and say it is closed. That is what
# https://github.com/microbiomedata/nmdc-lakehouse/issues/312 asks for, it needs no parsing, and
# a reader who meets the reference learns the same thing the checker does.


def _markdown_files(targets: list[Path]) -> list[Path]:
    found: list[Path] = []
    for target in targets:
        if target.is_dir():
            found.extend(sorted(target.rglob("*.md")))
        elif target.suffix == ".md":
            found.append(target)
    return found


def unresolvable_scripts(paths: list[Path], root: Path) -> list[tuple[Path, int, str]]:
    """Cited ``scripts/`` paths that do not exist here, with where each is cited.

    Not every one. A file may declare that its paths belong to another checkout, and everything
    from that declaration to the end of the file is skipped. The declaration is deliberately not
    file-wide: `berdl-upload.md` declares at line 896 of 1124, so treating it as file-wide
    exempted 895 lines of maintained runbook it does not introduce.
    """
    problems = []
    for document in paths:
        declared_from: int | None = None
        for number, line in enumerate(document.read_text(encoding="utf-8").splitlines(), start=1):
            if EXTERNAL_DECLARATION.search(line):
                declared_from = number
                continue
            if declared_from is not None:
                continue
            for reference in SCRIPT_REFERENCE.findall(line):
                if not (root / reference).is_file():
                    problems.append((document, number, reference))
    return problems


def _issue_states(numbers: set[str], repo: str) -> dict[str, str]:
    """Ask GitHub once per issue, returning only the states it could actually read.

    Anything else is left out: a non-zero exit, a missing `gh`, a timeout, and output that exits
    0 but does not parse. The caller fails on every number absent from this mapping, so an
    unreadable state can never read as a clean run.
    """
    states: dict[str, str] = {}
    for number in sorted(numbers, key=int):
        try:
            result = subprocess.run(  # noqa: S603
                ["gh", "issue", "view", number, "--repo", repo, "--json", "state"],  # noqa: S607
                capture_output=True,
                text=True,
                check=False,
                timeout=30,
            )
        except (OSError, subprocess.SubprocessError):
            # No gh, no network, or a timeout. Leaving the number out of `states` makes it
            # unreadable, which the caller already fails on. Crashing here would also be
            # non-zero, but a traceback reads as a broken checker rather than an unchecked
            # reference, and the two want different responses from whoever sees it.
            continue
        if result.returncode != 0:
            continue
        try:
            state = json.loads(result.stdout)["state"]
        except (json.JSONDecodeError, KeyError, TypeError):
            continue
        if not isinstance(state, str) or state not in _KNOWN_STATES:
            # `{"state": null}`, `{"state": []}` or an enum this does not know. The isinstance
            # check comes first because an unhashable value raises TypeError from the membership
            # test itself, so guarding the value without guarding its type moved the crash rather
            # than removing it.
            # Recording it would take the number out of `unreadable` and pass silently, which is
            # the same hole as the unparseable case wearing valid JSON.
            continue
        try:
            states[number] = state
        except TypeError:
            # Exit 0 with output this cannot read is still an unread state. Letting it raise
            # would abandon the remaining issues, so one malformed response would stop the
            # check rather than report one unreadable reference.
            continue
    return states


def _marker_blocks(text: str) -> list[tuple[int, str]]:
    """Each ``unverified`` marker, with the line it starts on.

    Only ``unverified``. `MARKER_START` deliberately excludes ``verified``, for the reason given
    where it is defined, so this returns one of the two kinds despite the shared syntax.

    A marker may wrap across lines, so the issue it names is often not on the line that opens it.
    Joining the block is what makes the reference findable at all.
    """
    blocks = []
    lines = text.splitlines()
    for index, line in enumerate(lines):
        if not MARKER_START.search(line):
            continue
        collected = [line]
        cursor = index
        while "-->" not in collected[-1] and cursor + 1 < len(lines):
            cursor += 1
            collected.append(lines[cursor])
        blocks.append((index + 1, " ".join(collected)))
    return blocks


def markers_citing_closed_issues(paths: list[Path], repo: str) -> tuple[list[tuple[Path, int, str]], set[str]]:
    """Markers naming a closed issue, and the issues whose state could not be read.

    One condition. There is deliberately no escape for a marker that says the issue closed.

    Three attempts at reading that from prose each produced a narrower wrong answer: a keyword
    search matched `done` inside "not done"; a lookbehind missed "has not been closed"; anchoring
    the word to the reference still accepted "#1 is still open and expected to be closed later".
    The last is the one that settles it. No anchoring rule makes a regular expression understand a
    future tense, and a check that reports success for something that has not happened is the
    defect this module exists to catch.

    So the remedy for a marker naming a closed issue is to stop naming it: point at a live issue,
    or say that nothing tracks the work. Both are mechanical, and neither needs this code to judge
    a sentence.
    """
    found: list[tuple[Path, int, str, str]] = []
    numbers: set[str] = set()
    for document in paths:
        for line_number, block in _marker_blocks(document.read_text(encoding="utf-8")):
            # Deduplicated within the block. `[#1](.../issues/1)` matches twice, once for the
            # label and once for the URL, and reporting "2 markers point at issue 1" for one
            # marker overstates the problem in the output people act on.
            # A markdown link to another repository is stripped first. The URL alternative
            # already ignores a foreign owner, but `[#1](https://github.com/a-fork/...)` left the
            # bare `#1` label behind, so the exclusion was bypassed by the ordinary link form.
            scanned = FOREIGN_LINK.sub("", block)
            seen: set[str] = set()
            for url_issue, bare_issue in ISSUE_REFERENCE.findall(scanned):
                issue = url_issue or bare_issue
                if issue in seen:
                    continue
                seen.add(issue)
                numbers.add(issue)
                found.append((document, line_number, block, issue))
    states = _issue_states(numbers, repo)
    unreadable = numbers - set(states)
    # Settlement is judged against the text around each reference, not the whole marker. A marker
    # reading "#1 is closed; follow-up tracked in #2" said `closed` once and suppressed both, so a
    # genuinely live pointer was hidden by a sentence about a different issue.
    problems = [
        (document, line_number, issue) for document, line_number, block, issue in found if states.get(issue) == "CLOSED"
    ]
    return problems, unreadable


def main() -> int:
    """Report unresolvable references, and fail when any are found."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("targets", nargs="+", type=Path)
    parser.add_argument("--check-issues", action="store_true", help="Also query GitHub for issue state.")
    parser.add_argument("--repo", default="microbiomedata/nmdc-lakehouse")
    parser.add_argument("--root", type=Path, default=Path("."))
    arguments = parser.parse_args()

    documents = _markdown_files(arguments.targets)
    failed = False

    missing = unresolvable_scripts(documents, arguments.root)
    print(f"doc references: {len(documents)} file(s), {len(missing)} unresolvable script path(s)")
    for document, number, reference in missing:
        print(f"  {document}:{number} cites {reference}, which is not in this repository")
        failed = True
    if missing:
        print("\nSay which checkout the path is in, or use a path that resolves here.")

    if arguments.check_issues:
        stale, unreadable = markers_citing_closed_issues(documents, arguments.repo)
        print(f"doc references: {len(stale)} marker(s) pointing at a closed issue")
        # Grouped by issue. One closed tracking issue named by eighty markers is one thing to fix,
        # and printing it eighty times buries the second finding under the first.
        by_issue: dict[str, list[str]] = {}
        for document, number, issue in stale:
            by_issue.setdefault(issue, []).append(f"{document}:{number}")
            failed = True
        for issue, places in sorted(by_issue.items(), key=lambda item: -len(item[1])):
            print(f"  issue {issue} is closed, and {len(places)} marker(s) point at it")
            for place in places[:5]:
                print(f"      {place}")
            if len(places) > 5:
                print(f"      ... and {len(places) - 5} more")
        if unreadable:
            # Fail, do not merely mention. An unreadable issue is an unchecked one, and an
            # unchecked reference is what this rule exists to catch. Printing a note and exiting 0
            # would make a network outage indistinguishable from a clean run, which is the exact
            # shape the offline/online split was made to avoid.
            print(f"  could not read the state of: {', '.join(sorted(unreadable, key=int))}")
            print("  treating unreadable issue state as a failure, not as a pass")
            failed = True

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
