"""Tests for the fetch stage lifted out of the notebook triples.

What these cover is which objects reach the manifest and which are refused. What they do not
cover is downloading, which is `scripts/download_to_cache.py` and has its own tests.
"""

from __future__ import annotations

import csv
from pathlib import Path

import pytest

from nmdc_lakehouse.data_object_manifest import (
    MANIFEST_COLUMNS,
    DataObjectManifestError,
    build_manifest,
    read_data_object_set,
    write_manifest,
)

PFAM = "Pfam Annotation GFF"
KEGG = "Annotation KEGG Orthology"


def _object(**overrides: object) -> dict[str, object]:
    record: dict[str, object] = {
        "id": "nmdc:dobj-1",
        "url": "https://data.microbiomedata.org/data/one.gff",
        "data_object_type": PFAM,
        "was_generated_by": "nmdc:wfmgan-1",
        "file_size_bytes": 1024,
        "md5_checksum": "abc",
    }
    record.update(overrides)
    return record


def test_a_type_that_is_not_in_the_schema_is_refused() -> None:
    """The reason `file_types` exists: a typo used to produce an empty manifest at SQL time.

    An empty manifest downloads nothing and every later stage reports a clean pass over it.
    """
    with pytest.raises(ValueError, match="not a permissible value"):
        build_manifest([_object()], ["Pfam Annotation GFFF"])


def test_only_the_requested_types_are_selected() -> None:
    records = [_object(), _object(id="nmdc:dobj-2", url="https://x/2.tsv", data_object_type=KEGG)]

    outcome = build_manifest(records, [KEGG])

    assert outcome.per_type == {KEGG: 1}
    assert outcome.rows[0]["id"] == "nmdc:dobj-2"


def test_several_types_are_fetched_together() -> None:
    """`ko_ec` is two types and one payload, which is why the notebook did them in one pass."""
    records = [_object(), _object(id="nmdc:dobj-2", url="https://x/2.tsv", data_object_type=KEGG)]

    outcome = build_manifest(records, [PFAM, KEGG])

    assert outcome.per_type == {PFAM: 1, KEGG: 1}
    assert outcome.total == 2


def test_an_object_with_no_url_is_dropped_and_counted() -> None:
    outcome = build_manifest([_object(), _object(id="nmdc:dobj-2", url=None)], [PFAM])

    assert outcome.total == 1
    assert outcome.dropped_no_url == 1


def test_a_zero_byte_placeholder_is_dropped() -> None:
    """There are 26,232 of these across the 2026-08-21 snapshot, and 7 in Pfam alone."""
    outcome = build_manifest([_object(), _object(id="nmdc:dobj-2", url="https://x/2.gff", file_size_bytes=0)], [PFAM])

    assert outcome.total == 1
    assert outcome.dropped_zero_byte == 1


def test_a_numeric_string_size_is_read_rather_than_dropped() -> None:
    """`file_size_bytes` is string-typed in some Parquet schemas, which is why the notebooks cast
    it defensively. Treating a string as unreadable would drop every object in such a snapshot."""
    outcome = build_manifest([_object(file_size_bytes="2048")], [PFAM])

    assert outcome.total == 1
    assert outcome.total_bytes == 2048


def test_a_size_that_cannot_be_read_counts_as_zero() -> None:
    """An object whose size cannot be read is not one whose size is known to be non-zero."""
    with pytest.raises(DataObjectManifestError, match="1 zero-byte"):
        build_manifest([_object(file_size_bytes="not a number")], [PFAM])


def test_the_same_url_is_not_downloaded_twice() -> None:
    """data_object_set carries several ids for one URL, which the notebooks deduplicated."""
    outcome = build_manifest([_object(), _object(id="nmdc:dobj-2")], [PFAM])

    assert outcome.total == 1
    assert outcome.dropped_duplicate == 1


def test_the_same_url_under_two_types_is_kept_once_each() -> None:
    """Deduplication is per (url, type), because the two rows land in different target tables."""
    outcome = build_manifest([_object(), _object(id="nmdc:dobj-2", data_object_type=KEGG)], [PFAM, KEGG])

    assert outcome.total == 2


def test_a_host_restriction_is_opt_in() -> None:
    """The notebooks hard-coded `data.microbiomedata.org`. 26,423 objects in the snapshot are on
    another host, so a hard-coded default would silently exclude them."""
    records = [_object(), _object(id="nmdc:dobj-2", url="https://nmdcdemo.emsl.pnnl.gov/a.gff")]

    assert build_manifest(records, [PFAM]).total == 2

    restricted = build_manifest(records, [PFAM], host="https://data.microbiomedata.org/")
    assert restricted.total == 1
    assert restricted.dropped_other_host == 1


def test_a_manifest_that_would_describe_nothing_is_refused() -> None:
    """An empty manifest downloads nothing and reports success at every later stage."""
    with pytest.raises(DataObjectManifestError, match="Nothing would be fetched"):
        build_manifest([_object()], [KEGG])


def test_a_selection_whose_every_object_is_dropped_is_refused_with_the_reasons() -> None:
    """Distinct from the above: the type exists, and everything of it was unusable."""
    records = [_object(file_size_bytes=0), _object(id="nmdc:dobj-2", url=None)]

    with pytest.raises(DataObjectManifestError, match="1 with no URL.*1 zero-byte"):
        build_manifest(records, [PFAM])


def test_the_manifest_carries_what_the_downloader_and_later_stages_need(tmp_path: Path) -> None:
    """`url` is the downloader's only requirement; the rest are what parse and ingest join on."""
    outcome = build_manifest([_object()], [PFAM])

    written = write_manifest(outcome, tmp_path / "manifest.csv")
    rows = list(csv.DictReader(written.open(encoding="utf-8")))

    assert list(rows[0]) == list(MANIFEST_COLUMNS)
    assert rows[0]["url"] == "https://data.microbiomedata.org/data/one.gff"


def test_reading_a_parquet_without_the_needed_columns_says_which(tmp_path: Path) -> None:
    """Refused on read rather than surfacing as a KeyError once the manifest is half built."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    path = tmp_path / "partial.parquet"
    pq.write_table(pa.table({"id": ["nmdc:dobj-1"], "url": ["https://x/1.gff"]}), path)

    with pytest.raises(DataObjectManifestError, match="data_object_type"):
        read_data_object_set(path)


def test_reading_a_missing_file_is_refused(tmp_path: Path) -> None:
    with pytest.raises(DataObjectManifestError, match="is not a file"):
        read_data_object_set(tmp_path / "absent.parquet")


def _snapshot(tmp_path: Path, records: list[dict[str, object]]) -> Path:
    import pyarrow as pa
    import pyarrow.parquet as pq

    path = tmp_path / "data_object_set.parquet"
    pq.write_table(pa.table({c: [r.get(c) for r in records] for c in MANIFEST_COLUMNS}), path)
    return path


def test_the_command_writes_a_manifest_and_names_the_download_step(tmp_path: Path) -> None:
    """The notebook ended by printing a command to paste, which is the part worth keeping."""
    from click.testing import CliRunner

    from nmdc_lakehouse.cli import cli

    source = _snapshot(tmp_path, [_object(), _object(id="nmdc:dobj-2", url="https://x/2.gff")])
    output = tmp_path / "manifest.csv"

    result = CliRunner().invoke(
        cli,
        ["data-object-manifest", "--type", PFAM, "--data-object-set", str(source), "--output", str(output)],
    )

    assert result.exit_code == 0, result.output
    assert "2  Pfam Annotation GFF" in result.output
    assert "download_to_cache.py" in result.output
    assert len(list(csv.DictReader(output.open(encoding="utf-8")))) == 2


def test_the_command_refuses_rather_than_writing_an_empty_manifest(tmp_path: Path) -> None:
    """The failure this whole command is shaped around: a file that downloads nothing."""
    from click.testing import CliRunner

    from nmdc_lakehouse.cli import cli

    source = _snapshot(tmp_path, [_object()])
    output = tmp_path / "manifest.csv"

    result = CliRunner().invoke(
        cli,
        ["data-object-manifest", "--type", KEGG, "--data-object-set", str(source), "--output", str(output)],
    )

    assert result.exit_code != 0
    assert "Nothing would be fetched" in result.output
    assert not output.exists(), "a refused manifest must not leave a file behind"


def test_the_command_requires_exactly_one_source(tmp_path: Path) -> None:
    """Neither source is a silent default, and both together would be ambiguous about freshness."""
    from click.testing import CliRunner

    from nmdc_lakehouse.cli import cli

    runner = CliRunner()
    base = ["data-object-manifest", "--type", PFAM, "--output", str(tmp_path / "m.csv")]

    neither = runner.invoke(cli, base)
    both = runner.invoke(
        cli, [*base, "--data-object-set", str(_snapshot(tmp_path, [_object()])), "--ingest-checkout", str(tmp_path)]
    )

    for result in (neither, both):
        assert result.exit_code != 0
        assert "exactly one source" in result.output


def test_the_host_option_reaches_the_filter_through_the_command(tmp_path: Path) -> None:
    """Tested through Click, so the option cannot stop being passed while the library test passes."""
    from click.testing import CliRunner

    from nmdc_lakehouse.cli import cli

    source = _snapshot(tmp_path, [_object(), _object(id="nmdc:dobj-2", url="https://nmdcdemo.emsl.pnnl.gov/a.gff")])
    output = tmp_path / "manifest.csv"

    result = CliRunner().invoke(
        cli,
        [
            "data-object-manifest",
            "--type",
            PFAM,
            "--data-object-set",
            str(source),
            "--host",
            "https://data.microbiomedata.org/",
            "--output",
            str(output),
        ],
    )

    assert result.exit_code == 0, result.output
    assert "1 other host" in result.output
    assert len(list(csv.DictReader(output.open(encoding="utf-8")))) == 1


def test_the_live_catalog_path_reads_the_same_columns(tmp_path: Path, monkeypatch) -> None:
    """The two sources must not drift into producing different manifests, so the column list is
    shared. This is the path the notebooks used."""
    from click.testing import CliRunner

    import nmdc_lakehouse.derived_tables as derived_tables
    from nmdc_lakehouse.cli import cli

    class Row:
        def __init__(self, data: dict[str, object]) -> None:
            self._data = data

        def asDict(self) -> dict[str, object]:  # noqa: N802 - Spark's spelling
            return self._data

    class Frame:
        def __init__(self, rows: list[Row]) -> None:
            self._rows = rows

        def collect(self) -> list[Row]:
            return self._rows

    class FakeSpark:
        def __init__(self) -> None:
            self.statements: list[str] = []

        def sql(self, statement: str) -> Frame:
            self.statements.append(statement)
            return Frame([Row(_object())])

    spark = FakeSpark()
    monkeypatch.setattr(derived_tables, "spark_session", lambda _checkout: spark)
    output = tmp_path / "manifest.csv"

    result = CliRunner().invoke(
        cli,
        [
            "data-object-manifest",
            "--type",
            PFAM,
            "--ingest-checkout",
            str(tmp_path),
            "--namespace",
            "nmdc.metadata",
            "--output",
            str(output),
        ],
    )

    assert result.exit_code == 0, result.output
    assert "nmdc.metadata.data_object_set" in spark.statements[0]
    for column in MANIFEST_COLUMNS:
        assert column in spark.statements[0]
    assert len(list(csv.DictReader(output.open(encoding="utf-8")))) == 1
