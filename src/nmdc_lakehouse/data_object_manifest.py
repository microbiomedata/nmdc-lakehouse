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

from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path

from nmdc_lakehouse.file_types import resolve_file_types

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
    for record in selected:
        url = record.get("url")
        if not url:
            no_url += 1
            continue
        if host is not None and not str(url).startswith(host):
            other_host += 1
            continue
        key = (url, record.get("data_object_type"))
        if key in seen:
            duplicate += 1
            continue
        seen.add(key)
        # `file_size_bytes` arrives as a string from some Parquet schemas, which is why the
        # notebooks cast it defensively rather than comparing it directly.
        size = _as_size(record.get("file_size_bytes"))
        if size <= 0:
            zero_byte += 1
            continue
        kept.append({column: record.get(column) for column in MANIFEST_COLUMNS})

    if not kept:
        raise DataObjectManifestError(
            f"Every one of the {len(selected)} object(s) of these types was dropped: "
            f"{no_url} with no URL, {other_host} on another host, {duplicate} duplicate, "
            f"{zero_byte} zero-byte. An empty manifest would download nothing and report success."
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


def read_data_object_set_from_spark(spark: object, namespace: str) -> list[dict[str, object]]:
    """Read `data_object_set` from a live catalog, which is what the notebooks did.

    Fresher than a snapshot and needs a session. The column list is the same one, so the two
    sources cannot drift into producing different manifests.
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
    statement = f"SELECT {columns} FROM {namespace}.data_object_set"  # noqa: S608 - checked identifiers
    try:
        frame = spark.sql(statement)  # type: ignore[attr-defined]
        return [row.asDict() for row in frame.collect()]
    except Exception as error:
        raise DataObjectManifestError(f"Reading '{namespace}.data_object_set' failed.") from error


def write_manifest(outcome: ManifestOutcome, path: Path) -> Path:
    """Write the manifest as the CSV `scripts/download_to_cache.py` reads."""
    import csv

    destination = path.expanduser()
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(MANIFEST_COLUMNS))
        writer.writeheader()
        writer.writerows(outcome.rows)
    return destination
