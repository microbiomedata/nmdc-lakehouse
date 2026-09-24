"""Tests for `nmdc_lakehouse.feature_tables`, using small made-up files in NMDC's layout."""

from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq
import pytest

from nmdc_lakehouse import feature_tables as ft

RUN = "nmdc:wfmgan-99-test.1"
G1 = f"{RUN}_0001_2_730"
G2 = f"{RUN}_0001_838_2616"
R1 = f"{RUN}_0002_1_124"

FUNCTIONAL_ROWS = [
    f"{RUN}_0001\tProdigal v2.6.3\tCDS\t2\t730\t42.8\t+\t0\tID={G1};translation_table=11;product=hypothetical protein;"
    "product_source=Hypo-rule applied;pfam=PF00001,PF00002;ko=KO:K00001;ec_number=EC:1.1.1.1,EC:2.2.2.2",
    f"{RUN}_0001\tGeneMark.hmm-2\tCDS\t838\t2616\t48.46\t-\t0\tID={G2};translation_table=11;product=kinase;"
    "product_source=COG0001;cog=COG0001",
    f"{RUN}_0002\tINFERNAL 1.1.3\trRNA\t1\t124\t121.3\t-\t.\tID={R1};model=RF00002;product=5S ribosomal RNA",
]


def _structural(row: str) -> str:
    keep = ("ID", "translation_table", "model")
    cols = row.split("\t")
    cols[8] = ";".join(kv for kv in cols[8].split(";") if kv.split("=", 1)[0] in keep)
    return "\t".join(cols)


def _write(tmp_path: Path, name: str, lines: list[str]) -> Path:
    path = tmp_path / name
    path.write_text("".join(f"{line}\n" for line in lines))
    return path


@pytest.fixture
def run_files(tmp_path: Path) -> dict[str, Path]:
    files = {
        ft.FUNCTIONAL: _write(tmp_path, "functional.gff", FUNCTIONAL_ROWS),
        ft.STRUCTURAL: _write(tmp_path, "structural.gff", [_structural(r) for r in FUNCTIONAL_ROWS]),
        "Prodigal Annotation GFF": _write(
            tmp_path,
            "prodigal.gff",
            [
                "##gff-version  3",
                f"{RUN}_0001\tProdigal v2.6.3\tCDS\t2\t730\t42.8\t+\t0\tID={G1}",
                f"{RUN}_0003\tProdigal v2.6.3\tCDS\t5\t99\t1.0\t+\t0\tID={RUN}_0003_5_99",
            ],
        ),
        "Genemark Annotation GFF": _write(
            tmp_path, "genemark.gff", [f"{RUN}_0001\tGeneMark.hmm-2\tCDS\t838\t2616\t48.46\t-\t0\tID={G2}"]
        ),
        "RFAM Annotation GFF": _write(
            tmp_path, "rfam.gff", [f"{RUN}_0002\tINFERNAL 1.1.3\trRNA\t1\t124\t121.3\t-\t.\tID={R1}"]
        ),
        "Pfam Annotation GFF": _write(
            tmp_path,
            "pfam.gff",
            [
                f"{G1}\tHMMER 3.1b2\tPF00001\t10\t80\t50.3\t.\t.\tID={G1}_10_80;Name=A;e-value=1e-9",
                f"{G1}\tHMMER 3.1b2\tPF00002\t90\t150\t20.0\t.\t.\tID={G1}_90_150;Name=B;e-value=1e-3",
                f"{G1}\tHMMER 3.1b2\tPF00002\t160\t200\t19.0\t.\t.\tID={G1}_160_200;Name=B;e-value=1e-2",
            ],
        ),
        "Clusters of Orthologous Groups (COG) Annotation GFF": _write(
            tmp_path, "cog.gff", [f"{G2}\t\tCOG0001\t1\t175\t69.6\t.\t.\tID={G2}_1_175"]
        ),
        ft.KO_EC: _write(
            tmp_path,
            "ko_ec.gff",
            [f"{G1}\tlastal 1456\tKO:K00001__EC:1.1.1.1_EC:2.2.2.2\t2\t389\t362\t.\t.\tID={G1}_2_389;evalue=1e-9"],
        ),
        ft.KO_TSV: _write(tmp_path, "ko.tsv", [f"{G1}\t277\tKO:K00001\t47.7"]),
        ft.EC_TSV: _write(tmp_path, "ec.tsv", [f"{G1}\t277\tEC:1.1.1.1\t47.7", f"{G1}\t277\tEC:2.2.2.2\t47.7"]),
        ft.PRODUCT_NAMES: _write(
            tmp_path,
            "product_names.tsv",
            [
                f"{G1}\thypothetical protein\tHypo-rule applied",
                f"{G2}\tkinase\tCOG0001",
                f"{R1}\t5S ribosomal RNA\trRNA_5S",
            ],
        ),
        ft.CONTIG_MAPPING: _write(
            tmp_path,
            "contig_mapping.tsv",
            [f"nmdc:wfmgas-99-a_scf_1\t{RUN}_0001", f"nmdc:wfmgas-99-a_scf_2\t{RUN}_0002"],
        ),
        ft.SCAFFOLD_LINEAGE: _write(tmp_path, "lineage.tsv", [f"{RUN}_0001\tBacteria;Pseudomonadota\t0.667"]),
    }
    return files


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


def test_convert_run_loads_each_observation_once(run_files: dict[str, Path], tmp_path: Path) -> None:
    urls = {t: f"https://example.org/{p.name}" for t, p in run_files.items()}
    result = ft.convert_run(RUN, run_files, urls, tmp_path / "out")
    assert result.duplicate_feature_ids == 0
    assert result.dropped_keys == ["cog", "ec_number", "ko", "pfam"]
    features = pq.read_table(result.outputs[0]).to_pylist()
    by_type = {}
    for row in features:
        by_type.setdefault(row["source_data_object_type"], []).append(row)
    assert len(by_type[ft.FUNCTIONAL]) == 3
    assert len(by_type["Pfam Annotation GFF"]) == 3
    assert "Prodigal Annotation GFF" not in by_type

    gene = next(r for r in by_type[ft.FUNCTIONAL] if r["feature_id"] == G1)
    assert gene["product"] == "hypothetical protein"
    assert {a["key"] for a in gene["attributes"]} == {"translation_table"}

    hit = by_type["Pfam Annotation GFF"][0]
    assert hit["coordinate_system"] == "protein"
    assert hit["parent"] == [G1]
    assert hit["seqid"] == f"{RUN}_0001"
    assert hit["strand"] == "."
    assert hit["phase"] is None

    contigs = pq.read_table(result.outputs[1]).to_pylist()
    first = next(c for c in contigs if c["contig_id"] == f"{RUN}_0001")
    assert first["assembly_contig_id"] == "nmdc:wfmgas-99-a_scf_1"
    assert first["taxonomic_lineage"] == ["Bacteria", "Pseudomonadota"]


def test_convert_run_keeps_accessions_when_the_hit_file_is_missing(run_files: dict[str, Path], tmp_path: Path) -> None:
    del run_files["Pfam Annotation GFF"]
    result = ft.convert_run(RUN, run_files, {}, tmp_path / "out")
    assert "pfam" not in result.dropped_keys
    gene = next(r for r in pq.read_table(result.outputs[0]).to_pylist() if r["feature_id"] == G1)
    assert ("pfam", "PF00001,PF00002") in {(a["key"], a["value"]) for a in gene["attributes"]}


def test_convert_run_adds_unselected_calls_only_on_request(run_files: dict[str, Path], tmp_path: Path) -> None:
    result = ft.convert_run(RUN, run_files, {}, tmp_path / "out", include_unselected=True)
    rows = pq.read_table(result.outputs[0]).to_pylist()
    unselected = [r for r in rows if r["is_selected"] is False]
    assert [r["start"] for r in unselected] == [5]


def _run(run_id: str, inputs: list[str], outputs: list[str], version: str = "v1") -> dict[str, object]:
    return {
        "id": run_id,
        "type": "nmdc:MetagenomeAnnotation",
        "version": version,
        "has_input": inputs,
        "has_output": outputs,
    }


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


def test_convert_run_renames_ids_repeated_on_opposite_strands(run_files: dict[str, Path], tmp_path: Path) -> None:
    extra = f"{RUN}_0004\tINFERNAL 1.1.3\tmisc_feature\t328\t430\t%s\t%s\t.\tID=dup;model=RF02000"
    rows = [*FUNCTIONAL_ROWS, extra % ("41.5", "-"), extra % ("42.2", "+")]
    run_files[ft.FUNCTIONAL] = _write(tmp_path, "functional_dup.gff", rows)
    del run_files[ft.STRUCTURAL]
    assert ft.check_run(run_files)["functional_ids_unique_with_strand"]["passed"] is True
    result = ft.convert_run(RUN, run_files, {}, tmp_path / "out")
    assert result.duplicate_feature_ids == 0
    assert result.renamed_duplicate_ids == 2
    renamed = [r for r in pq.read_table(result.outputs[0]).to_pylist() if r["feature_id"].startswith("dup|")]
    assert sorted(r["feature_id"] for r in renamed) == ["dup|+", "dup|-"]
    assert all(("ID", "dup") in {(a["key"], a["value"]) for a in r["attributes"]} for r in renamed)


def test_convert_run_refuses_unselected_when_callers_miss_selected_rows(
    run_files: dict[str, Path], tmp_path: Path
) -> None:
    del run_files["Genemark Annotation GFF"]
    result = ft.convert_run(RUN, run_files, {}, tmp_path / "out", include_unselected=True)
    assert result.unselected_refused
    assert all(r["is_selected"] is not False for r in pq.read_table(result.outputs[0]).to_pylist())


def test_hits_follow_a_renamed_cds(run_files: dict[str, Path], tmp_path: Path) -> None:
    rna = f"{RUN}_0001\tINFERNAL 1.1.3\tmisc_feature\t2\t730\t108.4\t-\t.\tID={G1};model=RF02743"
    run_files[ft.FUNCTIONAL] = _write(tmp_path, "functional_shared.gff", [*FUNCTIONAL_ROWS, rna])
    result = ft.convert_run(RUN, run_files, {}, tmp_path / "out")
    rows = pq.read_table(result.outputs[0]).to_pylist()
    hit = next(r for r in rows if r["source_data_object_type"] == "Pfam Annotation GFF")
    assert hit["parent"] == [f"{G1}|+"]
    assert hit["seqid"] == f"{RUN}_0001"
    contig_ids = {c["contig_id"] for c in pq.read_table(result.outputs[1]).to_pylist()}
    assert contig_ids == {f"{RUN}_0001", f"{RUN}_0002"}
    # The RNA row comes last and has no pfam key; the CDS's accessions must still count.
    assert ft.check_run(run_files)["hits_match_functional:pfam"]["passed"] is True
    assert "pfam" in result.dropped_keys
    assert any(r["feature_id"] == f"{G1}|+" and r["type"] == "CDS" for r in rows)


def _cli_fixture(run_files: dict[str, Path], tmp_path: Path) -> tuple[Path, Path, Path]:
    """A plan whose data objects point at `run_files`, copied into a download cache layout."""
    import hashlib
    import json

    cache = tmp_path / "cache"
    data_objects = []
    for index, (data_object_type, path) in enumerate(run_files.items()):
        url = f"https://example.org/data/{path.name}"
        target = cache / "data" / path.name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(path.read_bytes())
        data_objects.append(
            {
                "id": f"dobj-{index}",
                "data_object_type": data_object_type,
                "url": url,
                "file_size_bytes": path.stat().st_size,
                "md5_checksum": hashlib.md5(path.read_bytes(), usedforsecurity=False).hexdigest(),
            }
        )
    plan = ft.plan_runs([_run(RUN, ["assembly"], [d["id"] for d in data_objects])], data_objects)
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(json.dumps(ft.plan_to_json(plan)))
    runs_path = tmp_path / "runs.txt"
    runs_path.write_text(f"{RUN}\n")
    return plan_path, runs_path, cache


def test_cli_check_and_convert(run_files: dict[str, Path], tmp_path: Path) -> None:
    import json

    from click.testing import CliRunner

    from nmdc_lakehouse.cli import cli

    plan_path, runs_path, cache = _cli_fixture(run_files, tmp_path)
    report = tmp_path / "report.json"
    common = [str(plan_path), "--runs", str(runs_path), "--cache-dir", str(cache)]
    result = CliRunner().invoke(cli, ["feature-check", *common, "--output", str(report)])
    # Genemark and the other callers are all present, so every check that ran should pass.
    assert result.exit_code == 0, result.output
    checks = json.loads(report.read_text())["runs"][RUN]["checks"]
    assert checks["md5_matches_nmdc"]["passed"] is True

    out = tmp_path / "parquet"
    result = CliRunner().invoke(cli, ["feature-convert", *common, "--out-dir", str(out), "--include-unselected"])
    assert result.exit_code == 0, result.output
    summary = json.loads((out / "conversion_summary.json").read_text())
    assert summary[0]["run_id"] == RUN
    assert summary[0]["unselected_refused"] is None


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


def test_cli_convert_skips_a_run_without_its_functional_file(run_files: dict[str, Path], tmp_path: Path) -> None:
    from click.testing import CliRunner

    from nmdc_lakehouse.cli import cli

    plan_path, runs_path, cache = _cli_fixture(run_files, tmp_path)
    (cache / "data" / "functional.gff").unlink()
    result = CliRunner().invoke(
        cli,
        [
            "feature-convert",
            str(plan_path),
            "--runs",
            str(runs_path),
            "--cache-dir",
            str(cache),
            "--out-dir",
            str(tmp_path / "out"),
        ],
    )
    assert result.exit_code == 0
    assert "skip" in result.output


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


def test_convert_run_refuses_to_write_a_repeated_feature_id(run_files: dict[str, Path], tmp_path: Path) -> None:
    # Same ID, strand and everything: renaming by strand cannot separate these, but the counter can.
    row = f"{RUN}_0004\tx\tmisc_feature\t1\t9\t.\t+\t.\tID=same"
    run_files[ft.FUNCTIONAL] = _write(tmp_path, "functional_same.gff", [*FUNCTIONAL_ROWS, row, row])
    result = ft.convert_run(RUN, run_files, {}, tmp_path / "ok")
    assert result.renamed_duplicate_ids == 2
    # A hit whose derived feature_id collides has no renaming rule, so conversion must stop.
    hit = f"{G1}\tHMMER 3.1b2\tPF00001\t10\t80\t50.3\t.\t.\tID={G1}_10_80"
    run_files["Pfam Annotation GFF"] = _write(tmp_path, "pfam_twice.gff", [hit, hit])
    out = tmp_path / "bad"
    with pytest.raises(ft.DuplicateFeatureIdError):
        ft.convert_run(RUN, run_files, {}, out)
    assert not out.exists()


def test_plan_records_the_assembly_run_and_contigs_use_it(run_files: dict[str, Path], tmp_path: Path) -> None:
    runs = [_run(RUN, ["dobj-contigs"], ["dobj-f"])]
    data_objects = [{"id": "dobj-f", "data_object_type": ft.FUNCTIONAL, "url": "u", "file_size_bytes": 1}]
    assemblies = [{"id": "nmdc:wfmgas-99-a.1", "has_output": ["dobj-contigs"]}]
    plan = ft.plan_runs(runs, data_objects, assemblies)
    assert plan.selected[RUN]["run"]["assembly_run"] == "nmdc:wfmgas-99-a.1"
    assert ft.plan_runs(runs, data_objects).selected[RUN]["run"]["assembly_run"] is None

    result = ft.convert_run(RUN, run_files, {}, tmp_path / "out", assembly_run="nmdc:wfmgas-99-a.1")
    contigs = pq.read_table(result.outputs[1]).to_pylist()
    assert {c["generated_by"] for c in contigs} == {"nmdc:wfmgas-99-a.1"}
    features = pq.read_table(result.outputs[0]).to_pylist()
    assert {f["generated_by"] for f in features} == {RUN}
    unknown = ft.convert_run(RUN, run_files, {}, tmp_path / "unknown")
    assert {c["generated_by"] for c in pq.read_table(unknown.outputs[1]).to_pylist()} == {None}


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


def test_convert_run_counts_hits_on_unknown_genes_without_writing_them(
    run_files: dict[str, Path], tmp_path: Path
) -> None:
    stray = "nmdc:missing_gene\tHMMER 3.1b2\tPF00003\t1\t9\t5.0\t.\t.\tID=stray"
    pfam = run_files["Pfam Annotation GFF"]
    pfam.write_text(pfam.read_text() + stray + "\n")
    result = ft.convert_run(RUN, run_files, {}, tmp_path / "out")
    assert result.orphan_hits == {"Pfam Annotation GFF": 1}
    features = pq.read_table(result.outputs[0]).to_pylist()
    assert not any(r["feature_id"].startswith("stray") for r in features)
    contigs = {c["contig_id"] for c in pq.read_table(result.outputs[1]).to_pylist()}
    assert "nmdc:missing_gene" not in contigs


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
