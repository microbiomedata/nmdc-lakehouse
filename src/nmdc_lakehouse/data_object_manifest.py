"""Build the download manifest that `scripts/download_to_cache.py` consumes.

This is the fetch stage of the notebook triples, which is the one stage they genuinely shared:
`fetch_pfam_gff` and `fetch_ko_ec_annotations` are 83% identical, differing only in an environment
variable name, a logger name, and the target types. `fetch_taxonomy_summaries` does the same thing
and then parses, which is a separate stage and not this module's business.

What the notebooks did, and what is kept here: resolve the requested `data_object_type` values
against nmdc-schema so a typo fails immediately rather than yielding an empty manifest, select the
columns the downloader and the later stages need, drop objects with no URL, deduplicate, drop
zero-byte placeholders, and report what was dropped.

Two behaviours are deliberately different.

The notebooks hard-coded `url LIKE 'https://data.microbiomedata.org/%'`. Every type they targeted
happens to be hosted there, so it never bit, but 26,423 objects in the 2026-08-21 snapshot are on
`nmdcdemo.emsl.pnnl.gov` and a hard-coded host would silently exclude them. Restricting is now an
option and never a default.

An empty result is refused rather than written. A manifest with no rows is the exact shape of a
successful-looking run that fetched nothing, and the downloader would report a clean pass over it.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlparse

from nmdc_lakehouse.file_types import resolve_file_types

#: What may go inside a SQL string literal here. Every permissible value of the schema's
#: FileTypeEnum matches today, including "Clusters of Orthologous Groups (COG) Annotation GFF";
#: none contains a quote or a backslash. Checked rather than assumed, because the values come
#: from a dependency that can change without this repository noticing.
_QUOTABLE = re.compile(r"[A-Za-z0-9 ()_,./+-]+")

#: Columns the manifest carries. `url` is what `scripts/download_to_cache.py` requires; the rest
#: are what the parse and ingest stages join on, and dropping them here would mean re-querying.
MANIFEST_COLUMNS: tuple[str, ...] = (
    "id",
    "url",
    "data_object_type",
    "was_generated_by",
    "file_size_bytes",
    "md5_checksum",
)


def _as_size(value: object) -> int:
    """`file_size_bytes` arrives as a string from some Parquet schemas.

    That is why the notebooks cast it defensively rather than comparing it directly, and why an
    unparseable value counts as zero here: an object whose size cannot be read is not one whose
    size is known to be non-zero.
    """
    try:
        return int(value)  # type: ignore[call-overload]
    except (TypeError, ValueError):
        return 0


class DataObjectManifestError(ValueError):
    """Raised when a manifest cannot be built, or would describe nothing."""


@dataclass(frozen=True)
class ManifestOutcome:
    """What the manifest holds, and what was dropped on the way.

    The dropped counts are reported rather than merely logged because they are how a surprising
    manifest gets explained, and because the number varies by type far more than it looks. The
    pfam notebook said zero-byte placeholders were "only ~7 of these", which is exactly right for
    Pfam Annotation GFF in the 2026-08-21 snapshot. Across all types that snapshot has 26,232, so
    a reader who carried the ~7 over to another type would be wrong by three orders of magnitude.
    """

    rows: list[dict[str, object]]
    per_type: dict[str, int]
    total_bytes: int
    dropped_no_url: int = 0
    dropped_duplicate: int = 0
    dropped_zero_byte: int = 0
    dropped_other_host: int = 0
    requested_types: tuple[str, ...] = field(default_factory=tuple)

    @property
    def total(self) -> int:
        """How many objects the manifest describes."""
        return len(self.rows)


def build_manifest(
    records: Sequence[dict[str, object]],
    types: Sequence[str],
    host: str | None = None,
) -> ManifestOutcome:
    """Select and clean the objects of `types`, refusing a manifest that would describe nothing.

    `records` is whatever the caller read `data_object_set` from, so this stays testable without a
    catalog and works the same against the snapshot on disk and against a live Spark session.
    """
    wanted = tuple(resolve_file_types(list(types)))
    if not wanted:
        raise DataObjectManifestError("No data object types were named, so there is nothing to fetch.")

    # Built once. Inside the comprehension this was rebuilt for every record, and the snapshot
    # has 290,640 of them.
    targets = set(wanted)
    selected = [r for r in records if r.get("data_object_type") in targets]
    if not selected:
        raise DataObjectManifestError(
            "No objects have any of these types: " + ", ".join(wanted) + ". Nothing would be fetched."
        )

    kept: list[dict[str, object]] = []
    seen: set[tuple[object, object]] = set()
    no_url = duplicate = zero_byte = other_host = 0
    # Ordered before deduplicating, because deduplication keeps the first row it sees and neither
    # source establishes an order: Spark results are unordered, and which `id` survives would then
    # vary between runs over identical data. The notebooks got this from their `ORDER BY`.
    for record in sorted(selected, key=lambda r: (str(r.get("data_object_type") or ""), str(r.get("id") or ""))):
        # Normalised once. A whitespace-padded URL passed the emptiness check, then failed the
        # host filter for a reason nobody could see, and deduplicated as a different object from
        # the same URL without the padding. Normalising in one place is what keeps the three
        # consistent, and the manifest then carries the cleaned value.
        raw = record.get("url")
        url = str(raw).strip() if raw is not None else ""
        if not url:
            no_url += 1
            continue
        if host is not None and not url.startswith(host):
            other_host += 1
            continue
        # Size first, then the key. Marking the key seen before validating meant a zero-byte first
        # row was dropped as zero-byte and its good duplicate then dropped as a duplicate, so the
        # URL left the manifest with no single count explaining where it went.
        #
        # `file_size_bytes` arrives as a string from some Parquet schemas, which is why the
        # notebooks cast it defensively rather than comparing it directly.
        size = _as_size(record.get("file_size_bytes"))
        if size <= 0:
            zero_byte += 1
            continue
        key = (url, record.get("data_object_type"))
        if key in seen:
            duplicate += 1
            continue
        seen.add(key)
        row = {column: record.get(column) for column in MANIFEST_COLUMNS}
        row["url"] = url
        kept.append(row)

    if not kept:
        raise DataObjectManifestError(
            f"Every one of the {len(selected)} object(s) of these types was dropped: "
            f"{no_url} with no URL, {other_host} on another host, {duplicate} duplicate, "
            f"{zero_byte} zero-byte. An empty manifest would download nothing and report success."
        )

    # `scripts/download_to_cache.py::cache_path_for` keys the cache on `urlparse(url).path`
    # alone, so any two URLs sharing a path collapse to one cached file: one payload overwrites
    # the other and the parse stage reads whichever won.
    #
    # Distinct URLs per path, not distinct hosts. Counting hosts was the first version of this and
    # it missed the case that actually exists: 2,733 MassIVE objects in the 2026-08-21 snapshot
    # are all `https://massive.ucsd.edu/ProteoSAFe/DownloadResultFile?file=...`, one host, one
    # path, 2,733 payloads. Cross-host collisions are the rarer shape, and this covers both.
    by_path: dict[str, set[str]] = {}
    for row in kept:
        by_path.setdefault(urlparse(str(row["url"])).path, set()).add(str(row["url"]))
    collisions = sorted((path, len(urls)) for path, urls in by_path.items() if len(urls) > 1)
    if collisions:
        worst = ", ".join(f"{path} ({count} URLs)" for path, count in collisions[:3])
        raise DataObjectManifestError(
            "These URL paths are reached by more than one URL, and the downloader caches by path "
            "alone, so the payloads would overwrite each other: "
            + worst
            + (f" and {len(collisions) - 3} more" if len(collisions) > 3 else "")
            + ". The cache key has to include the query string before these can be fetched."
        )

    per_type: dict[str, int] = {}
    for row in kept:
        name = str(row["data_object_type"])
        per_type[name] = per_type.get(name, 0) + 1
    return ManifestOutcome(
        rows=kept,
        per_type=per_type,
        total_bytes=sum(_as_size(row["file_size_bytes"]) for row in kept),
        dropped_no_url=no_url,
        dropped_duplicate=duplicate,
        dropped_zero_byte=zero_byte,
        dropped_other_host=other_host,
        requested_types=wanted,
    )


def read_data_object_set(path: Path) -> list[dict[str, object]]:
    """Read `data_object_set` from a snapshot Parquet file.

    The snapshot is what exists on disk and needs no pod. A live catalog is the other source and
    is fresher; see `read_data_object_set_from_spark`. Neither is a default: the command requires
    one to be named, because which one was read changes what the manifest describes.
    """
    import pyarrow.parquet as pq

    document = path.expanduser()
    if not document.is_file():
        raise DataObjectManifestError(f"{document} is not a file, so data_object_set cannot be read.")
    # The columns are named on read rather than selected afterwards, so the rest of the snapshot
    # never enters memory. The schema is checked first so a missing column is reported as such
    # instead of surfacing as an arrow error about a field that is not there.
    available = set(pq.read_schema(document).names)
    missing = [column for column in MANIFEST_COLUMNS if column not in available]
    if missing:
        raise DataObjectManifestError(f"{document} is missing the column(s) a manifest needs: {', '.join(missing)}.")
    return pq.read_table(document, columns=list(MANIFEST_COLUMNS)).to_pylist()


def read_data_object_set_from_spark(
    spark: object, namespace: str, types: Sequence[str] | None = None
) -> list[dict[str, object]]:
    """Read `data_object_set` from a live catalog, which is what the notebooks did.

    Fresher than a snapshot and needs a session. The column list is the same one, so the two
    sources cannot drift into producing different manifests.

    `types` filters in the query rather than in the driver, which is what the notebooks did too.
    Without it this collected all 290,640 rows of the 2026-08-21 snapshot to keep about 4,900.
    It is an optimisation and not a second rule: `build_manifest` applies the same selection
    either way, so a source that ignores `types` still produces the same manifest.

    The values are interpolated because they have been through `resolve_file_types` and are
    therefore permissible values of the schema's enum, not caller text. That is asserted here
    rather than assumed, because the guarantee lives in another function.
    """
    # Refused before it reaches a statement. `namespace` is CLI input, and the same
    # catalog-qualified rule the rest of this repository applies is the one that makes it a pair
    # of identifiers rather than arbitrary text.
    from nmdc_lakehouse.derived_tables import DerivedTableError, check_namespace

    try:
        check_namespace(namespace)
    except DerivedTableError as error:
        raise DataObjectManifestError(str(error)) from error
    columns = ", ".join(MANIFEST_COLUMNS)
    where = " WHERE url IS NOT NULL"
    if types:
        wanted = resolve_file_types(list(types))
        unsafe = sorted(t for t in wanted if not _QUOTABLE.fullmatch(t))
        if unsafe:
            raise DataObjectManifestError(
                "These type names cannot be put in a SQL literal safely: " + ", ".join(unsafe) + "."
            )
        where += " AND data_object_type IN (" + ", ".join(f"'{t}'" for t in wanted) + ")"
    statement = f"SELECT {columns} FROM {namespace}.data_object_set{where}"  # noqa: S608 - checked
    try:
        frame = spark.sql(statement)  # type: ignore[attr-defined]
        return [row.asDict() for row in frame.collect()]
    except Exception as error:
        raise DataObjectManifestError(f"Reading '{namespace}.data_object_set' failed.") from error


def write_manifest(outcome: ManifestOutcome, path: Path) -> Path:
    """Write the manifest as the CSV `scripts/download_to_cache.py` reads."""
    import csv
    import os
    import tempfile

    destination = path.expanduser()
    destination.parent.mkdir(parents=True, exist_ok=True)
    # Written beside the destination and renamed, because a partial CSV is a valid manifest. An
    # interrupted write leaves a header and some rows, and the downloader reads that as the whole
    # set and reports a clean pass having fetched part of it.
    handle_fd, temporary = tempfile.mkstemp(dir=destination.parent, prefix=destination.name, suffix=".part")
    try:
        with os.fdopen(handle_fd, "w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(MANIFEST_COLUMNS))
            writer.writeheader()
            writer.writerows(outcome.rows)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
    except BaseException:
        Path(temporary).unlink(missing_ok=True)
        raise
    return destination
