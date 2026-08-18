import json
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from nmdc_lakehouse.publication_plan import DestinationInventory
from nmdc_lakehouse.snapshot_manifest import _physical_schema_sha256
from scripts.python import audit_database_metadata as audit


class FakeFrame:
    def __init__(self, rows: int, schema: str) -> None:
        self._rows = rows
        self.schema = schema

    def collect(self) -> list[dict[str, int]]:
        return [{"row_count": self._rows}]


class FakeSpark:
    def __init__(self, frames: dict[str, FakeFrame]) -> None:
        self.frames = frames
        self.queries: list[str] = []

    def sql(self, query: str) -> FakeFrame:
        self.queries.append(query)
        table = query.split(".")[-1].split("`")[1]
        return self.frames[table]


def _patch_discovery(monkeypatch: pytest.MonkeyPatch, providers: dict[str, str]) -> None:
    monkeypatch.setattr(audit, "list_tables", lambda _spark, _database: sorted(providers))
    monkeypatch.setattr(
        audit,
        "describe_table",
        lambda _spark, _database, table: {"provider": providers[table]},
    )
    monkeypatch.setattr(audit, "_physical_schema_sha256", lambda schema: {"a": "a" * 64, "b": "b" * 64}[schema])


def test_build_publication_inventory_matches_planner_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_discovery(monkeypatch, {"biosample_set": "delta", "study_set": "delta"})
    spark = FakeSpark(
        {
            "biosample_set": FakeFrame(10, "a"),
            "study_set": FakeFrame(2, "b"),
        }
    )

    value = audit.build_publication_inventory(
        spark,
        "spark_catalog.nmdc_metadata",
        destination_id="nmdc-production",
        provider="spark_catalog",
        table_format="delta",
        metadata_capabilities=["namespace", "table", "column"],
        observed_at="2026-08-18T18:00:00+00:00",
    )

    inventory = DestinationInventory.model_validate_json(json.dumps(value))
    assert [table.name for table in inventory.tables] == ["biosample_set", "study_set"]
    assert [table.rows for table in inventory.tables] == [10, 2]
    assert spark.queries == [
        "SELECT * FROM `spark_catalog`.`nmdc_metadata`.`biosample_set` LIMIT 0",
        "SELECT COUNT(*) AS row_count FROM `spark_catalog`.`nmdc_metadata`.`biosample_set`",
        "SELECT * FROM `spark_catalog`.`nmdc_metadata`.`study_set` LIMIT 0",
        "SELECT COUNT(*) AS row_count FROM `spark_catalog`.`nmdc_metadata`.`study_set`",
    ]
    assert "location" not in json.dumps(value).casefold()


@pytest.mark.parametrize(
    ("capabilities", "message"),
    [([], "nonempty"), (["table", "table"], "without duplicates"), (["unknown"], "Unknown metadata")],
)
def test_inventory_rejects_invalid_capabilities(
    monkeypatch: pytest.MonkeyPatch, capabilities: list[str], message: str
) -> None:
    _patch_discovery(monkeypatch, {"biosample_set": "delta"})

    with pytest.raises(audit.PublicationInventoryError, match=message):
        audit.build_publication_inventory(
            FakeSpark({"biosample_set": FakeFrame(1, "a")}),
            "nmdc_metadata",
            destination_id="nmdc-production",
            provider="spark_catalog",
            table_format="delta",
            metadata_capabilities=capabilities,
        )


def test_inventory_fails_closed_on_table_format_mismatch(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_discovery(monkeypatch, {"biosample_set": "iceberg"})

    with pytest.raises(audit.PublicationInventoryError, match="not reviewed table format 'delta'"):
        audit.build_publication_inventory(
            FakeSpark({"biosample_set": FakeFrame(1, "a")}),
            "nmdc_metadata",
            destination_id="nmdc-production",
            provider="spark_catalog",
            table_format="delta",
            metadata_capabilities=["table"],
        )


def test_inventory_wraps_table_failure_without_exposing_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_discovery(monkeypatch, {"biosample_set": "delta"})

    with pytest.raises(
        audit.PublicationInventoryError, match="Cannot inventory destination table 'biosample_set'"
    ) as error:
        audit.build_publication_inventory(
            FakeSpark({}),
            "nmdc_metadata",
            destination_id="nmdc-production",
            provider="spark_catalog",
            table_format="delta",
            metadata_capabilities=["table"],
        )
    assert "biosample-secret" not in str(error.value)


def test_arrow_schema_hash_matches_snapshot_contract() -> None:
    schema = pa.schema(
        [pa.field("id", pa.string(), nullable=False, metadata={b"comment": b"destination-only"})],
        metadata={b"provider": b"delta"},
    )

    assert audit._arrow_physical_schema_sha256(schema) == _physical_schema_sha256(schema)


def test_write_publication_inventory_is_canonical_and_rejects_symlink(tmp_path: Path) -> None:
    output = tmp_path / "inventory.json"
    value: dict[str, Any] = {"tables": [], "inventory_format_version": 1}

    assert audit.write_publication_inventory(output, value) == output
    assert output.read_text() == '{\n  "inventory_format_version": 1,\n  "tables": []\n}\n'

    link = tmp_path / "link.json"
    link.symlink_to(output)
    with pytest.raises(audit.PublicationInventoryError, match="ordinary file"):
        audit.write_publication_inventory(link, value)


def test_write_publication_inventory_sanitizes_setup_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def reject_temporary_file(**_kwargs: Any) -> tuple[int, str]:
        raise OSError("filesystem detail")

    monkeypatch.setattr(audit.tempfile, "mkstemp", reject_temporary_file)

    with pytest.raises(audit.PublicationInventoryError, match="Cannot write the publication inventory") as error:
        audit.write_publication_inventory(tmp_path / "inventory.json", {})
    assert "filesystem detail" not in str(error.value)


@pytest.mark.parametrize("value", ["nmdc_metadata;DROP", "nmdc metadata", "nmdc..metadata"])
def test_inventory_rejects_unsafe_qualified_database(value: str) -> None:
    with pytest.raises(ValueError, match="unsafe database"):
        audit.build_publication_inventory(
            FakeSpark({}),
            value,
            destination_id="nmdc-production",
            provider="spark_catalog",
            table_format="delta",
            metadata_capabilities=["table"],
        )
