"""Tests for `nmdc_lakehouse.feature_tables`: planning, sampling and the overlap checks."""

from __future__ import annotations

from pathlib import Path

import pytest

from nmdc_lakehouse import feature_tables as ft
from tests.feature_files import (  # noqa: F401
    FUNCTIONAL_ROWS,
    G1,
    G2,
    R1,
    RUN,
    _cli_fixture,
    _run,
    _structural,
    _write,
)


def test_split_ko_ec_separates_packed_accessions() -> None:
    assert ft.split_ko_ec("KO:K03273__EC:3.1.3.82_EC:3.1.3.83") == (
        {"KO:K03273"},
        {"EC:3.1.3.82", "EC:3.1.3.83"},
    )
    assert ft.split_ko_ec("KO:K09960") == ({"KO:K09960"}, set())
    # v1.0.2 and v1.0.4 files pack several KOs with a single underscore.
    assert ft.split_ko_ec("KO:K02025_KO:K10118") == ({"KO:K02025", "KO:K10118"}, set())


def test_parse_attributes_keeps_order_repeats_and_bare_keys() -> None:
    assert ft.parse_attributes("ID=a;k=1;k=2;flag;partial=5',3'") == [
        ("ID", "a"),
        ("k", "1"),
        ("k", "2"),
        ("flag", ""),
        ("partial", "5',3'"),
    ]


def test_check_run_passes_on_consistent_files(run_files: dict[str, Path]) -> None:
    checks = ft.check_run(run_files)
    ran = {name: result for name, result in checks.items() if not result.get("skipped")}
    assert all(result["passed"] for result in ran.values()), ran
    assert checks["selected_rows_in_callers"]["unselected_caller_rows"] == 1
    assert checks["product_names_in_functional"]["source_label_only_in_product_names"] == 1
    assert checks["hits_match_functional:tigrfam"]["skipped"] == "missing"


def test_check_run_fails_when_a_hit_file_disagrees(run_files: dict[str, Path], tmp_path: Path) -> None:
    run_files["Pfam Annotation GFF"] = _write(
        tmp_path, "pfam_bad.gff", [f"{G1}\tHMMER 3.1b2\tPF09999\t10\t80\t50.3\t.\t.\tID={G1}_10_80"]
    )
    checks = ft.check_run(run_files)
    assert checks["hits_match_functional:pfam"]["passed"] is False


def test_check_run_fails_when_structural_has_an_extra_row(run_files: dict[str, Path], tmp_path: Path) -> None:
    rows = [_structural(r) for r in FUNCTIONAL_ROWS] + [f"{RUN}_0009\tx\tCDS\t1\t9\t.\t+\t0\tID=extra"]
    run_files[ft.STRUCTURAL] = _write(tmp_path, "structural_bad.gff", rows)
    assert ft.check_run(run_files)["structural_is_functional_subset"]["passed"] is False


def test_plan_runs_keeps_the_latest_run_per_input_and_reports_orphans() -> None:
    runs = [
        _run("nmdc:wfmgan-1.1", ["dobj-in"], ["dobj-a"]),
        _run("nmdc:wfmgan-1.2", ["dobj-in"], ["dobj-b"]),
        _run("nmdc:wfmgan-2.1", ["dobj-in2"], ["dobj-c"]),
    ]
    data_objects = [
        {"id": "dobj-a", "data_object_type": ft.FUNCTIONAL, "url": "u/a", "file_size_bytes": 5},
        {"id": "dobj-b", "data_object_type": ft.FUNCTIONAL, "url": "u/b", "file_size_bytes": 7},
        {"id": "dobj-c", "data_object_type": ft.FUNCTIONAL, "url": "u/c", "file_size_bytes": 9},
        {"id": "dobj-z", "data_object_type": ft.FUNCTIONAL, "url": "u/z", "file_size_bytes": 1},
    ]
    plan = ft.plan_runs(runs, data_objects)
    assert set(plan.selected) == {"nmdc:wfmgan-1.2", "nmdc:wfmgan-2.1"}
    assert plan.superseded == {"nmdc:wfmgan-1.1": "nmdc:wfmgan-1.2"}
    assert [o["id"] for o in plan.orphans] == ["dobj-z"]
    assert plan.summary()["selected_bytes_by_type"] == {ft.FUNCTIONAL: 16}
    assert ft.plan_from_json(ft.plan_to_json(plan)).selected == plan.selected


def test_sample_runs_takes_from_every_version() -> None:
    runs = [_run(f"r{i}.1", [f"in{i}"], [f"o{i}"], version="big" if i < 20 else "small") for i in range(22)]
    data_objects = [
        {"id": f"o{i}", "data_object_type": ft.FUNCTIONAL, "url": f"u{i}", "file_size_bytes": 1} for i in range(22)
    ]
    plan = ft.plan_runs(runs, data_objects)
    chosen = ft.sample_runs(plan, 4, required_types=[ft.FUNCTIONAL])
    versions = sorted(plan.selected[r]["run"]["version"] for r in chosen)
    assert versions == ["big", "big", "small", "small"]
    assert ft.sample_runs(plan, 4, required_types=[ft.FUNCTIONAL], max_run_bytes=0) == []


def test_cli_check_fails_on_a_checksum_mismatch(run_files: dict[str, Path], tmp_path: Path) -> None:
    from click.testing import CliRunner

    from nmdc_lakehouse.cli import cli

    plan_path, runs_path, cache = _cli_fixture(run_files, tmp_path)
    (cache / "data" / "pfam.gff").write_text("changed\n")
    result = CliRunner().invoke(
        cli,
        [
            "feature-check",
            str(plan_path),
            "--runs",
            str(runs_path),
            "--cache-dir",
            str(cache),
            "--output",
            str(tmp_path / "r.json"),
        ],
    )
    assert result.exit_code == 1
    assert "failed  md5_matches_nmdc" in result.output


def test_cli_plan_and_sample(run_files: dict[str, Path], tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import json

    from click.testing import CliRunner

    from nmdc_lakehouse.cli import cli

    plan_path, _, _ = _cli_fixture(run_files, tmp_path)
    saved = json.loads(plan_path.read_text())
    entry = saved["selected"][RUN]
    inventory = {"runs": [entry["run"]], "data_objects": list(entry["files"].values())}
    monkeypatch.setattr(ft, "fetch_inventory", lambda: inventory)
    new_plan = tmp_path / "fetched" / "plan.json"
    result = CliRunner().invoke(cli, ["feature-plan", "--output", str(new_plan)])
    assert result.exit_code == 0, result.output
    assert json.loads(new_plan.read_text())["summary"]["selected_runs"] == 1

    sample = [str(new_plan), "--runs", str(tmp_path / "s.txt"), "--manifest", str(tmp_path / "m.csv")]
    # The fixture has no tRNA, TIGRFam or other required types, so no run qualifies.
    result = CliRunner().invoke(cli, ["feature-sample", *sample])
    assert result.exit_code != 0
    assert "No run qualifies" in result.output


def test_sample_and_manifest_for_a_qualifying_run(tmp_path: Path) -> None:
    import csv

    outputs = [f"o-{t}" for t in ft.SAMPLE_REQUIRED_TYPES]
    data_objects = [
        {"id": f"o-{t}", "data_object_type": t, "url": f"https://example.org/{i}", "file_size_bytes": 3}
        for i, t in enumerate(ft.SAMPLE_REQUIRED_TYPES)
    ]
    plan = ft.plan_runs([_run(RUN, ["in"], outputs)], data_objects)
    chosen = ft.sample_runs(plan, 5)
    assert chosen == [RUN]
    manifest = tmp_path / "m.csv"
    assert ft.write_download_manifest(plan, chosen, ft.CHECK_TYPES, manifest) == len(ft.SAMPLE_REQUIRED_TYPES)
    assert {row["was_generated_by"] for row in csv.DictReader(manifest.open())} == {RUN}


def test_fetch_collection_follows_page_tokens() -> None:
    class Response:
        def __init__(self, body: dict[str, object]) -> None:
            self.body = body

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return self.body

    class Session:
        def __init__(self) -> None:
            self.calls: list[dict[str, str]] = []

        def get(self, url: str, params: dict[str, str], headers: dict[str, str], timeout: int) -> Response:
            self.calls.append(params)
            assert headers["User-Agent"] == ft.USER_AGENT
            if "page_token" not in params:
                return Response({"resources": [{"id": 1}], "next_page_token": "t"})
            return Response({"resources": [{"id": 2}]})

    session = Session()
    rows = ft.fetch_collection("data_object_set", {}, ["id"], session=session)  # type: ignore[arg-type]
    assert rows == [{"id": 1}, {"id": 2}]
    assert session.calls[1]["page_token"] == "t"


def test_cli_check_fails_when_a_planned_file_is_missing(run_files: dict[str, Path], tmp_path: Path) -> None:
    from click.testing import CliRunner

    from nmdc_lakehouse.cli import cli

    plan_path, runs_path, cache = _cli_fixture(run_files, tmp_path)
    (cache / "data" / "functional.gff").unlink()
    report = tmp_path / "r.json"
    args = [str(plan_path), "--runs", str(runs_path), "--cache-dir", str(cache), "--output", str(report)]
    result = CliRunner().invoke(cli, ["feature-check", *args])
    assert result.exit_code == 1
    assert "failed  planned_files_present" in result.output


def test_cached_files_refuses_paths_outside_the_cache(tmp_path: Path) -> None:
    cache = tmp_path / "cache"
    cache.mkdir()
    (tmp_path / "secret.txt").write_text("x")
    entry = {"files": {ft.FUNCTIONAL: {"url": "https://example.org/../../secret.txt"}}}
    with pytest.raises(ValueError, match="outside the cache"):
        ft.cached_files(entry, cache)


def test_plan_runs_uses_neither_file_when_a_type_is_listed_twice() -> None:
    runs = [_run(RUN, ["in"], ["a", "b", "c"])]
    data_objects = [
        {"id": "a", "data_object_type": ft.FUNCTIONAL, "url": "u/a"},
        {"id": "b", "data_object_type": ft.FUNCTIONAL, "url": "u/b"},
        {"id": "c", "data_object_type": ft.STRUCTURAL, "url": "u/c"},
    ]
    plan = ft.plan_runs(runs, data_objects)
    assert plan.ambiguous == [(RUN, ft.FUNCTIONAL)]
    assert set(plan.selected[RUN]["files"]) == {ft.STRUCTURAL}


def test_product_names_source_only_there_fails_for_a_cds(run_files: dict[str, Path], tmp_path: Path) -> None:
    rows = [r.replace(";product_source=COG0001", "") for r in FUNCTIONAL_ROWS]
    run_files[ft.FUNCTIONAL] = _write(tmp_path, "functional_nosource.gff", rows)
    result = ft.check_run(run_files)["product_names_in_functional"]
    assert result["passed"] is False
    # Only the rRNA row may carry a label found in Product Names alone; the CDS may not.
    assert result["source_label_only_in_product_names"] == 1
    assert result["sources_matched"] == 1


def test_cli_check_passes_on_consistent_files(run_files: dict[str, Path], tmp_path: Path) -> None:
    import json

    from click.testing import CliRunner

    from nmdc_lakehouse.cli import cli

    plan_path, runs_path, cache = _cli_fixture(run_files, tmp_path)
    report = tmp_path / "report.json"
    args = [str(plan_path), "--runs", str(runs_path), "--cache-dir", str(cache), "--output", str(report)]
    result = CliRunner().invoke(cli, ["feature-check", *args])
    assert result.exit_code == 0, result.output
    checks = json.loads(report.read_text())["runs"][RUN]["checks"]
    assert checks["md5_matches_nmdc"]["passed"] is True
    assert checks["planned_files_present"]["passed"] is True


def test_plan_records_the_assembly_run() -> None:
    runs = [_run(RUN, ["dobj-contigs"], ["dobj-f"])]
    data_objects = [{"id": "dobj-f", "data_object_type": ft.FUNCTIONAL, "url": "u", "file_size_bytes": 1}]
    assemblies = [{"id": "nmdc:wfmgas-99-a.1", "has_output": ["dobj-contigs"]}]
    assert ft.plan_runs(runs, data_objects, assemblies).selected[RUN]["run"]["assembly_run"] == "nmdc:wfmgas-99-a.1"
    assert ft.plan_runs(runs, data_objects).selected[RUN]["run"]["assembly_run"] is None


def test_manifest_refuses_urls_that_share_a_cache_path(tmp_path: Path) -> None:
    from nmdc_lakehouse.data_object_manifest import DataObjectManifestError

    outputs = [f"o-{t}" for t in ft.SAMPLE_REQUIRED_TYPES]
    data_objects = [
        {"id": f"o-{t}", "data_object_type": t, "url": f"https://example.org/f?{i}", "file_size_bytes": 3}
        for i, t in enumerate(ft.SAMPLE_REQUIRED_TYPES)
    ]
    plan = ft.plan_runs([_run(RUN, ["in"], outputs)], data_objects)
    with pytest.raises(DataObjectManifestError, match="more than one URL"):
        ft.write_download_manifest(plan, [RUN], ft.CHECK_TYPES, tmp_path / "m.csv")


def test_cli_check_fails_when_nmdc_records_no_checksum(run_files: dict[str, Path], tmp_path: Path) -> None:
    import json

    from click.testing import CliRunner

    from nmdc_lakehouse.cli import cli

    plan_path, runs_path, cache = _cli_fixture(run_files, tmp_path)
    plan = json.loads(plan_path.read_text())
    plan["selected"][RUN]["files"][ft.FUNCTIONAL]["md5_checksum"] = None
    plan_path.write_text(json.dumps(plan))
    report = tmp_path / "r.json"
    args = [str(plan_path), "--runs", str(runs_path), "--cache-dir", str(cache), "--output", str(report)]
    result = CliRunner().invoke(cli, ["feature-check", *args])
    assert result.exit_code == 1
    assert json.loads(report.read_text())["runs"][RUN]["checks"]["md5_matches_nmdc"]["mismatched"] == [ft.FUNCTIONAL]


def test_cli_check_does_not_expect_zero_byte_files(run_files: dict[str, Path], tmp_path: Path) -> None:
    import json

    from click.testing import CliRunner

    from nmdc_lakehouse.cli import cli

    plan_path, runs_path, cache = _cli_fixture(run_files, tmp_path)
    plan = json.loads(plan_path.read_text())
    plan["selected"][RUN]["files"]["CRT Annotation GFF"] = {
        "id": "dobj-crt",
        "data_object_type": "CRT Annotation GFF",
        "url": "https://example.org/data/crt.gff",
        "file_size_bytes": 0,
    }
    plan_path.write_text(json.dumps(plan))
    report = tmp_path / "r.json"
    args = [str(plan_path), "--runs", str(runs_path), "--cache-dir", str(cache), "--output", str(report)]
    result = CliRunner().invoke(cli, ["feature-check", *args])
    assert result.exit_code == 0, result.output


def test_product_names_rna_label_must_restate_the_row(run_files: dict[str, Path], tmp_path: Path) -> None:
    rows = [
        f"{G1}\thypothetical protein\tHypo-rule applied",
        f"{G2}\tkinase\tCOG0001",
        f"{R1}\t5S ribosomal RNA\tInfernal",
    ]
    run_files[ft.PRODUCT_NAMES] = _write(tmp_path, "product_names_new_label.tsv", rows)
    result = ft.check_run(run_files)["product_names_in_functional"]
    assert result["passed"] is False
    assert result["source_label_only_in_product_names"] == 0


def test_restates_rna_accepts_only_type_or_subunit_labels() -> None:
    assert ft._restates_rna("tRNA", "tRNA", "tRNA_Leu_TAA")
    assert ft._restates_rna("rRNA_23S", "rRNA", "23S ribosomal RNA")
    assert ft._restates_rna("rRNA_5_8S", "rRNA", "5.8S ribosomal RNA")
    assert not ft._restates_rna("rRNA_16S", "rRNA", "23S ribosomal RNA")
    assert not ft._restates_rna("tRNA", "CDS", "kinase")


def test_sample_cap_counts_optional_files() -> None:
    outputs = [f"o-{t}" for t in ft.CHECK_TYPES]
    data_objects = [
        {
            "id": f"o-{t}",
            "data_object_type": t,
            "url": f"https://example.org/{i}",
            "file_size_bytes": 100 if t == ft.CONTIG_MAPPING else 1,
        }
        for i, t in enumerate(ft.CHECK_TYPES)
    ]
    plan = ft.plan_runs([_run(RUN, ["in"], outputs)], data_objects)
    assert ft.sample_runs(plan, 1, max_run_bytes=50) == []
    assert ft.sample_runs(plan, 1, max_run_bytes=200) == [RUN]


def test_cli_check_refuses_an_empty_run_list(run_files: dict[str, Path], tmp_path: Path) -> None:
    from click.testing import CliRunner

    from nmdc_lakehouse.cli import cli

    plan_path, runs_path, cache = _cli_fixture(run_files, tmp_path)
    runs_path.write_text("\n")
    args = [str(plan_path), "--runs", str(runs_path), "--cache-dir", str(cache), "--output", str(tmp_path / "r.json")]
    result = CliRunner().invoke(cli, ["feature-check", *args])
    assert result.exit_code != 0
    assert "lists no runs" in result.output
