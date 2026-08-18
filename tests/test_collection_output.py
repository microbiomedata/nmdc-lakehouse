"""Tests for atomic per-collection output staging and promotion."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from nmdc_lakehouse.collection_output import CollectionOutputTransaction, CollectionPromotionError
from nmdc_lakehouse.jobs.collection_to_parquet import _close_side_writers


class _TestWriter:
    def __init__(self, rows: int = 0, error: BaseException | None = None) -> None:
        self.rows = rows
        self.error = error
        self.closed = False

    def close(self) -> int:
        self.closed = True
        if self.error is not None:
            raise self.error
        return self.rows


def test_successful_promotion_replaces_primary_removes_stale_side_and_preserves_unrelated(tmp_path: Path) -> None:
    primary = tmp_path / "sample_set.parquet"
    stale = tmp_path / "sample_set_tags.parquet"
    unrelated = tmp_path / "study_set.parquet"
    primary.write_bytes(b"old-primary")
    stale.write_bytes(b"old-side")
    unrelated.write_bytes(b"unrelated")

    with CollectionOutputTransaction(tmp_path, "sample_set", {"sample_set", "sample_set_tags"}) as transaction:
        (transaction.stage / "sample_set.parquet").write_bytes(b"new-primary")
        transaction.commit(
            (("sample_set", 2),),
            source_schema_id="https://example.org/schema",
            source_schema_version="1.0.0",
        )
        completion = json.loads((transaction.stage / "collection-manifest.json").read_text())
        assert completion["status"] == "complete"
        assert completion["tables"] == [{"rows": 2, "table": "sample_set"}]

    assert primary.read_bytes() == b"new-primary"
    assert not stale.exists()
    assert unrelated.read_bytes() == b"unrelated"
    assert not (tmp_path / ".staging").exists()


def test_conversion_failure_leaves_previous_snapshot_and_no_staging(tmp_path: Path) -> None:
    previous = tmp_path / "sample_set.parquet"
    previous.write_bytes(b"previous")

    with pytest.raises(RuntimeError, match="injected conversion failure"):
        with CollectionOutputTransaction(tmp_path, "sample_set", {"sample_set"}) as transaction:
            (transaction.stage / "sample_set.parquet").write_bytes(b"partial")
            raise RuntimeError("injected conversion failure")

    assert previous.read_bytes() == b"previous"
    assert not (tmp_path / ".staging").exists()


def test_transaction_rejects_symlinked_staging_root(tmp_path: Path) -> None:
    external = tmp_path / "external"
    external.mkdir()
    output = tmp_path / "output"
    output.mkdir()
    (output / ".staging").symlink_to(external, target_is_directory=True)

    with pytest.raises(CollectionPromotionError, match="staging root must be an ordinary directory"):
        with CollectionOutputTransaction(output, "sample_set", {"sample_set"}):
            pytest.fail("A symlinked staging root must not be entered.")

    assert list(external.iterdir()) == []


@pytest.mark.parametrize("blocked_path", ["output", "output/.staging"])
def test_transaction_translates_non_directory_roots(tmp_path: Path, blocked_path: str) -> None:
    output = tmp_path / "output"
    if blocked_path.endswith(".staging"):
        output.mkdir()
    (tmp_path / blocked_path).write_text("not a directory", encoding="utf-8")

    expected = "staging root" if blocked_path.endswith(".staging") else "output root"
    with pytest.raises(CollectionPromotionError, match=expected):
        with CollectionOutputTransaction(output, "sample_set", {"sample_set"}):
            pytest.fail("A non-directory root must not be entered.")


def test_promotion_failure_rolls_back_previous_output(tmp_path: Path, monkeypatch) -> None:
    primary = tmp_path / "sample_set.parquet"
    side = tmp_path / "sample_set_tags.parquet"
    primary.write_bytes(b"old-primary")
    side.write_bytes(b"old-side")
    real_replace = os.replace
    calls = 0

    def fail_first_new_file(source, destination):
        nonlocal calls
        calls += 1
        if calls == 3:
            raise OSError("injected promotion failure")
        real_replace(source, destination)

    monkeypatch.setattr("nmdc_lakehouse.collection_output.os.replace", fail_first_new_file)

    with pytest.raises(OSError, match="injected promotion failure"):
        with CollectionOutputTransaction(tmp_path, "sample_set", {"sample_set", "sample_set_tags"}) as transaction:
            (transaction.stage / "sample_set.parquet").write_bytes(b"new-primary")
            transaction.commit(
                (("sample_set", 2),),
                source_schema_id="https://example.org/schema",
                source_schema_version="1.0.0",
            )

    assert primary.read_bytes() == b"old-primary"
    assert side.read_bytes() == b"old-side"
    assert not (tmp_path / ".staging").exists()


def test_side_writer_failure_closes_all_writers_and_preserves_previous_output(tmp_path: Path) -> None:
    previous = tmp_path / "sample_set.parquet"
    previous.write_bytes(b"previous")
    failing = _TestWriter(error=OSError("injected side flush failure"))
    remaining = _TestWriter(rows=3)

    with pytest.raises(OSError, match="injected side flush failure"):
        with CollectionOutputTransaction(tmp_path, "sample_set", {"sample_set", "sample_set_tags"}) as transaction:
            (transaction.stage / "sample_set.parquet").write_bytes(b"new-primary")
            _close_side_writers({"sample_set_tags": failing, "sample_set_other": remaining})

    assert failing.closed
    assert remaining.closed
    assert previous.read_bytes() == b"previous"
    assert not (tmp_path / ".staging").exists()


def test_failed_rollback_retains_staging_for_manual_recovery(tmp_path: Path, monkeypatch) -> None:
    primary = tmp_path / "sample_set.parquet"
    side = tmp_path / "sample_set_tags.parquet"
    primary.write_bytes(b"old-primary")
    side.write_bytes(b"old-side")
    real_replace = os.replace
    calls = 0

    def fail_promotion_and_primary_restore(source, destination):
        nonlocal calls
        calls += 1
        if calls in {3, 5}:
            raise OSError("injected replace failure")
        real_replace(source, destination)

    monkeypatch.setattr("nmdc_lakehouse.collection_output.os.replace", fail_promotion_and_primary_restore)

    with pytest.raises(OSError, match="injected replace failure") as raised:
        with CollectionOutputTransaction(tmp_path, "sample_set", {"sample_set", "sample_set_tags"}) as transaction:
            stage = transaction.stage
            (stage / "sample_set.parquet").write_bytes(b"new-primary")
            transaction.commit(
                (("sample_set", 2),),
                source_schema_id="https://example.org/schema",
                source_schema_version="1.0.0",
            )

    assert "staging was retained" in " ".join(raised.value.__notes__)
    assert not primary.exists()
    assert side.read_bytes() == b"old-side"
    assert (stage / ".previous" / "sample_set.parquet").read_bytes() == b"old-primary"


def test_promotion_requires_the_primary_table(tmp_path: Path) -> None:
    previous = tmp_path / "sample_set.parquet"
    previous.write_bytes(b"previous")

    with pytest.raises(CollectionPromotionError, match="include the primary table"):
        with CollectionOutputTransaction(tmp_path, "sample_set", {"sample_set", "sample_set_tags"}) as transaction:
            (transaction.stage / "sample_set_tags.parquet").write_bytes(b"side")
            transaction.commit(
                (("sample_set_tags", 1),),
                source_schema_id="https://example.org/schema",
                source_schema_version="1.0.0",
            )

    assert previous.read_bytes() == b"previous"
    assert not (tmp_path / ".staging").exists()
