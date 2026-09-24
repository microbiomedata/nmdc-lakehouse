"""Exercise local lineage, its independent SQL comparison, and immutable snapshots."""

from __future__ import annotations

import json
import platform
from importlib.metadata import version
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from nmdc_lakehouse import local_provenance as local
from nmdc_lakehouse import provenance_queries as queries
from nmdc_lakehouse.cli import cli
from nmdc_lakehouse.derived_tables import PROCESSING_TYPES, DerivedTableError
from nmdc_lakehouse.jobs.collection_to_parquet import REVIEWED_SCHEMA_COLLECTIONS
from nmdc_lakehouse.snapshot_manifest import SnapshotManifestError, build_manifest, validate_snapshot, write_manifest


def snapshot(root: Path, overrides: dict | None = None) -> Path:
    """Make two workflows, a pool, and an extraction, including alternate paths."""
    tables = {
        "biosample_set": (["id"], [("nmdc:bsm-a",), ("nmdc:bsm-b",)]),
        "workflow_execution_set": (["id", "type"], [("w0", "nmdc:Example"), ("w1", "nmdc:Example")]),
        "data_generation_set": (["id"], [("g0",), ("g1",)]),
        "processed_sample_set": (["id"], [("p0",), ("p1",)]),
        "material_processing_set": (["id", "type"], [("m0", "nmdc:Pooling"), ("m1", "nmdc:Extraction")]),
        "workflow_execution_set_was_informed_by": (["parent_id", "was_informed_by"], [("w0", "g0"), ("w1", "g1")]),
        "data_generation_set_has_input": (
            ["parent_id", "has_input"],
            [("g0", "nmdc:bsm-a"), ("g1", "p0"), ("g1", "nmdc:bsm-a"), ("g1", "nmdc:bsm-a")],
        ),
        "material_processing_set_has_output": (["parent_id", "has_output"], [("m0", "p0"), ("m1", "p1")]),
        "material_processing_set_has_input": (
            ["parent_id", "has_input"],
            [("m0", "nmdc:bsm-a"), ("m0", "p1"), ("m1", "nmdc:bsm-b")],
        ),
    }
    tables.update(overrides or {})
    root.mkdir()
    metadata = {
        b"nmdc_lakehouse.footer_metadata_format_version": b"2",
        b"nmdc_lakehouse.source_schema_id": b"https://w3id.org/nmdc/nmdc",
        b"nmdc_lakehouse.source_schema_version": b"11.23.0",
        b"nmdc_lakehouse.source_class": b"Database",
        b"nmdc_lakehouse.target_schema_id": b"https://w3id.org/nmdc/nmdc-schema-flattened",
        b"nmdc_lakehouse.target_schema_version": b"11.23.0+flat.1.3.0",
        b"nmdc_lakehouse.target_class": b"Example",
        b"nmdc_lakehouse.mapping": b"test",
    }
    outputs = []
    for name, (columns, rows) in tables.items():
        path = root / f"{name}.parquet"
        schema = pa.schema([pa.field(column, pa.string()) for column in columns], metadata=metadata)
        pq.write_table(
            pa.Table.from_pylist([dict(zip(columns, row, strict=True)) for row in rows], schema=schema), path
        )
        outputs.append({"table": name, "path": path.name, "rows": len(rows), "bytes": path.stat().st_size})
    metrics = root / "etl-metrics.json"
    metrics.write_text(
        json.dumps(
            {
                "format_version": 1,
                "job_name": "all-collections",
                "status": "success",
                "dry_run": False,
                "finished_at": "2026-09-23T00:00:00+00:00",
                "output_root": str(root.resolve()),
                "environment": {
                    "nmdc_lakehouse_version": version("nmdc-lakehouse"),
                    "nmdc_schema_version": version("nmdc-schema"),
                    "python_version": platform.python_version(),
                },
                "skipped_collections": sorted(REVIEWED_SCHEMA_COLLECTIONS - set(local.PRIMARY_TABLES)),
                "children": [{"job_name": name} for name in local.PRIMARY_TABLES],
                "outputs": outputs,
            }
        )
    )
    write_manifest(root, build_manifest(root, metrics, "synthetic"))
    return root


def test_derive_and_compare_all_pairs(tmp_path: Path) -> None:
    source = snapshot(tmp_path / "source")
    before = {path.name: path.read_bytes() for path in source.iterdir()}
    output = tmp_path / "derived"
    messages = []
    manifest = local.derive_provenance(source, output, progress=messages.append)
    assert manifest == validate_snapshot(output)
    assert manifest.parent_snapshot_id == validate_snapshot(source).snapshot_id
    assert {artifact.table: artifact.rows for artifact in manifest.artifacts} == {
        "graph_edges": 11,
        "biosample_to_workflow_run": 3,
    }
    assert before == {path.name: path.read_bytes() for path in source.iterdir()}
    rows = pq.read_table(output / "biosample_to_workflow_run.parquet").to_pylist()
    assert [(row["workflow_run_id"], row["biosample_id"], row["n_hops"]) for row in rows] == [
        ("w0", "nmdc:bsm-a", 2),
        ("w1", "nmdc:bsm-a", 2),
        ("w1", "nmdc:bsm-b", 6),
    ]
    assert not any(rows[0][column] for column in PROCESSING_TYPES.values())
    for row in rows[1:]:
        # Even the direct branch has workflow-wide flags, matching the established contract.
        assert {column for column in PROCESSING_TYPES.values() if row[column]} == {"has_pooling", "has_extraction"}
    for artifact in manifest.artifacts:
        schema = pq.read_schema(output / artifact.path)
        assert schema.metadata[b"nmdc_lakehouse.input_snapshot_id"].decode() == manifest.parent_snapshot_id
        assert schema.metadata[b"nmdc_lakehouse.table_description"]
        assert all(field.metadata[b"nmdc_lakehouse.description"] and not field.nullable for field in schema)
        spark_schema = json.loads(schema.metadata[b"org.apache.spark.sql.parquet.row.metadata"])
        assert all(field["metadata"]["comment"] and not field["nullable"] for field in spark_schema["fields"])
    metrics = json.loads((output / "derivation-metrics.json").read_text())
    assert metrics["max_biosample_hops"] == 6
    assert len(metrics["inputs"]) == 9
    assert metrics["target_schema_sha256"] == local.provenance_schema()[1]
    assert "walked 2/2 workflows" in messages
    report = queries.compare_provenance_queries(source, output)
    assert report["equivalent_pairs_and_hops"] == 3
    assert report["direct_two_hop_pairs"] == 2
    assert report["pairs_missed_by_direct_join"] == 1
    assert all(len(values) == 3 for values in report["seconds"].values())
    (output / "graph_edges.parquet").write_bytes(b"tampered")
    with pytest.raises(SnapshotManifestError):
        validate_snapshot(output)


def test_traversal_order_cycle_depth_and_missing_biosample() -> None:
    edges = [("w", "p", "x"), ("p", "b", "x"), ("w", "b", "x")]
    first = list(local.provenance_pairs(edges, {"w": "type"}, {"p": "nmdc:Extraction"}, {"b"}, max_depth=1))
    assert first == list(local.provenance_pairs(edges[::-1], {"w": "type"}, {"p": "nmdc:Extraction"}, {"b"}))
    assert first[0]["n_hops"] == 1
    assert first[0]["has_extraction"]
    for changed, depth, message in [
        (edges + [("p", "w", "x")], 15, "cycle"),
        (edges[:2], 1, "exceeds max_depth"),
        ([], 15, "no reachable biosample"),
    ]:
        with pytest.raises(DerivedTableError, match=message):
            list(local.provenance_pairs(changed, {"w": "type"}, {}, {"b"}, max_depth=depth))
    with pytest.raises(DerivedTableError, match="PROCESSING_TYPES"):
        list(local.provenance_pairs(edges, {"w": "type"}, {"p": "unknown"}, {"b"}))


@pytest.mark.parametrize(("processing_type", "column"), PROCESSING_TYPES.items())
def test_every_processing_flag_has_a_described_boolean_column(processing_type: str, column: str) -> None:
    target, _ = local.provenance_schema()
    flag_slots = {name for name in target.classes["BiosampleToWorkflowRun"].attributes if name.startswith("has_")}
    assert flag_slots == set(PROCESSING_TYPES.values())
    slot = target.classes["BiosampleToWorkflowRun"].attributes[column]
    assert slot.range == "boolean" and slot.description and slot.required
    row = next(local.provenance_pairs([("w", "p", "x"), ("p", "b", "x")], {"w": "type"}, {"p": processing_type}, {"b"}))
    assert {key for key in flag_slots if row[key]} == {column}


@pytest.mark.parametrize("depth", [0, -1, True, "15"])
def test_bad_depth_before_writes(tmp_path: Path, depth) -> None:
    with pytest.raises(DerivedTableError, match="max_depth"):
        local.derive_provenance(tmp_path / "missing", tmp_path / "output", max_depth=depth)
    with pytest.raises(DerivedTableError, match="max_depth"):
        list(local.provenance_pairs([], {}, {}, set(), max_depth=depth))
    assert not (tmp_path / "output").exists()


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"biosample_set": (["id"], [("b",), ("b",)])}, "unique"),
        ({"biosample_set": (["id"], [("w0",)])}, "unique"),
        ({"biosample_set": (["id"], [])}, "nonempty workflow"),
        ({"biosample_set": (["id"], [(None,)])}, "nonempty string"),
        ({"biosample_set": (["wrong"], [("b",)])}, "required columns"),
        ({"material_processing_set": (["id", "type"], [("m0", "nmdc:Unknown")])}, "PROCESSING_TYPES"),
        ({"material_processing_set_has_input": (["parent_id", "has_input"], [("m0", "missing")])}, "references absent"),
    ],
)
def test_refuse_invalid_inputs(tmp_path: Path, overrides: dict, message: str) -> None:
    source = snapshot(tmp_path / "source", overrides)
    with pytest.raises(DerivedTableError, match=message):
        local.derive_provenance(source, tmp_path / "output")
    assert not (tmp_path / "output").exists()


def test_output_boundaries_and_partial_failure(tmp_path: Path) -> None:
    source = snapshot(tmp_path / "source")
    with pytest.raises(DerivedTableError, match="new directory"):
        local.derive_provenance(source, source)
    with pytest.raises(DerivedTableError, match="outside"):
        local.derive_provenance(source, source / "derived")
    link = tmp_path / "link"
    link.symlink_to(tmp_path / "missing")
    with pytest.raises(DerivedTableError, match="new directory"):
        local.derive_provenance(source, link)
    output = tmp_path / "partial"
    with pytest.raises(DerivedTableError, match="exceeds max_depth"):
        local.derive_provenance(source, output, max_depth=2)
    assert not (output / "snapshot-manifest.json").exists()


def test_recheck_source_before_manifest(tmp_path: Path) -> None:
    source = snapshot(tmp_path / "source")
    output = tmp_path / "derived"

    def change_source(message: str) -> None:
        if message.startswith("rechecking"):
            (source / "etl-metrics.json").write_text("changed")

    with pytest.raises(SnapshotManifestError, match="checksum"):
        local.derive_provenance(source, output, progress=change_source)
    assert not (output / "snapshot-manifest.json").exists()


def test_cli_and_report_boundaries(tmp_path: Path) -> None:
    source = snapshot(tmp_path / "source")
    output = tmp_path / "derived"
    runner = CliRunner()
    result = runner.invoke(cli, ["derive-provenance", str(source), str(output)])
    assert result.exit_code == 0, result.output
    assert "biosample_to_workflow_run=3" in result.output
    repeated = runner.invoke(cli, ["derive-provenance", str(source), str(output)])
    assert repeated.exit_code == 1 and "new directory" in repeated.output
    report = tmp_path / "report.json"
    args = ["compare-provenance-queries", str(source), str(output), str(report)]
    result = runner.invoke(cli, args)
    assert result.exit_code == 0, result.output
    assert json.loads(report.read_text())["status"] == "success"
    assert runner.invoke(cli, args).exit_code == 1
    assert runner.invoke(cli, args[:-1] + [str(source / "extra.json")]).exit_code == 1
    assert runner.invoke(cli, args[:-1] + [str(output / "extra.json")]).exit_code == 1
    invalid = runner.invoke(cli, ["compare-provenance-queries", str(source), str(source), str(tmp_path / "bad.json")])
    assert invalid.exit_code == 1 and "parent" in invalid.output


@pytest.mark.parametrize("repeats", [0, True, "3"])
def test_comparison_repeats(tmp_path: Path, repeats) -> None:
    with pytest.raises(DerivedTableError, match="positive integer"):
        queries.compare_provenance_queries(tmp_path, tmp_path, repeats=repeats)
