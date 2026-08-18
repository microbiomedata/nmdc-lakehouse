"""Unit tests for DirectMongoToParquetJob."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pyarrow.parquet as pq
import pytest

from nmdc_lakehouse.jobs.direct_mongo_to_parquet import DirectMongoToParquetJob

_DOCS = [
    {
        "was_generated_by": "wf1",
        "gene_function_id": "KEGG.ORTHOLOGY:K01",
        "count": 5,
        "type": "nmdc:FunctionalAnnotationAggMember",
    },
    {
        "was_generated_by": "wf2",
        "gene_function_id": "KEGG.ORTHOLOGY:K02",
        "count": 3,
        "type": "nmdc:FunctionalAnnotationAggMember",
    },
]


def _make_mock_client(docs: list[dict]) -> MagicMock:
    # Cursor must support .batch_size() chaining and iteration.
    mock_cursor = MagicMock()
    mock_cursor.__iter__ = MagicMock(return_value=iter(docs))
    mock_cursor.batch_size.return_value = mock_cursor

    mock_coll = MagicMock()
    mock_coll.estimated_document_count.return_value = len(docs)
    mock_coll.find.return_value = mock_cursor

    mock_db = MagicMock()
    mock_db.__getitem__.return_value = mock_coll
    mock_client = MagicMock()
    mock_client.__getitem__.return_value = mock_db
    return mock_client


def test_run_writes_correct_row_count(tmp_path):
    """run() writes all documents to Parquet and reports correct counts."""
    with patch(
        "nmdc_lakehouse.jobs.direct_mongo_to_parquet.pymongo.MongoClient", return_value=_make_mock_client(_DOCS)
    ):
        job = DirectMongoToParquetJob(
            collection="functional_annotation_agg",
            root_class="FunctionalAnnotationAggMember",
            mongo_uri="mongodb://localhost/nmdc",
            out_root=tmp_path,
        )
        result = job.run()

    assert result.rows_read == 2
    assert result.rows_written == 2
    assert result.tables_written == ("functional_annotation_agg",)


def test_run_excludes_id_from_parquet(tmp_path):
    """_id is not written to the Parquet output."""
    docs_with_id = [{**d, "_id": "some-oid"} for d in _DOCS]
    with patch(
        "nmdc_lakehouse.jobs.direct_mongo_to_parquet.pymongo.MongoClient", return_value=_make_mock_client(docs_with_id)
    ):
        job = DirectMongoToParquetJob(
            collection="functional_annotation_agg",
            root_class="FunctionalAnnotationAggMember",
            mongo_uri="mongodb://localhost/nmdc",
            out_root=tmp_path,
        )
        result = job.run()

    tbl = pq.read_table(tmp_path / "functional_annotation_agg.parquet")
    assert "_id" not in tbl.schema.names
    assert result.rows_written == 2


def test_dry_run_counts_rows_but_writes_nothing(tmp_path):
    """dry_run=True counts rows but writes no Parquet file."""
    with patch(
        "nmdc_lakehouse.jobs.direct_mongo_to_parquet.pymongo.MongoClient", return_value=_make_mock_client(_DOCS)
    ):
        job = DirectMongoToParquetJob(
            collection="functional_annotation_agg",
            root_class="FunctionalAnnotationAggMember",
            mongo_uri="mongodb://localhost/nmdc",
            out_root=tmp_path,
        )
        result = job.run(dry_run=True)

    assert result.rows_read == 2
    assert result.rows_written == 0
    assert not (tmp_path / "functional_annotation_agg.parquet").exists()


def test_conversion_failure_preserves_previous_parquet(tmp_path):
    """A failed direct conversion never replaces the completed output."""
    output = tmp_path / "functional_annotation_agg.parquet"
    output.write_bytes(b"previous")

    def fail_after_partial_write(sink, _rows, *, table, **_kwargs):
        (sink.root / f"{table}.parquet").write_bytes(b"partial")
        raise RuntimeError("injected conversion failure")

    with (
        patch(
            "nmdc_lakehouse.jobs.direct_mongo_to_parquet.pymongo.MongoClient",
            return_value=_make_mock_client(_DOCS),
        ),
        patch("nmdc_lakehouse.jobs.direct_mongo_to_parquet.ParquetSink.write", fail_after_partial_write),
        pytest.raises(RuntimeError, match="injected conversion failure"),
    ):
        DirectMongoToParquetJob(
            collection="functional_annotation_agg",
            root_class="FunctionalAnnotationAggMember",
            mongo_uri="mongodb://localhost/nmdc",
            out_root=tmp_path,
        ).run()

    assert output.read_bytes() == b"previous"
    assert not (tmp_path / ".staging").exists()


def test_empty_collection_promotes_schema_only_parquet(tmp_path):
    """An empty collection still replaces prior output with a typed empty file."""
    output = tmp_path / "functional_annotation_agg.parquet"
    output.write_bytes(b"previous")
    with patch(
        "nmdc_lakehouse.jobs.direct_mongo_to_parquet.pymongo.MongoClient",
        return_value=_make_mock_client([]),
    ):
        result = DirectMongoToParquetJob(
            collection="functional_annotation_agg",
            root_class="FunctionalAnnotationAggMember",
            mongo_uri="mongodb://localhost/nmdc",
            out_root=tmp_path,
        ).run()

    assert result.table_rows == (("functional_annotation_agg", 0),)
    assert pq.read_metadata(output).num_rows == 0
    assert not (tmp_path / ".staging").exists()
