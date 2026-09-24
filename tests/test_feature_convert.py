"""Tests for `nmdc_lakehouse.feature_convert`."""

from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq
import pytest

from nmdc_lakehouse import feature_convert as fc
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


def test_convert_run_loads_each_observation_once(run_files: dict[str, Path], tmp_path: Path) -> None:
    urls = {t: f"https://example.org/{p.name}" for t, p in run_files.items()}
    result = fc.convert_run(RUN, run_files, urls, tmp_path / "out")
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
    result = fc.convert_run(RUN, run_files, {}, tmp_path / "out")
    assert "pfam" not in result.dropped_keys
    gene = next(r for r in pq.read_table(result.outputs[0]).to_pylist() if r["feature_id"] == G1)
    assert ("pfam", "PF00001,PF00002") in {(a["key"], a["value"]) for a in gene["attributes"]}


def test_convert_run_adds_unselected_calls_only_on_request(run_files: dict[str, Path], tmp_path: Path) -> None:
    result = fc.convert_run(RUN, run_files, {}, tmp_path / "out", include_unselected=True)
    rows = pq.read_table(result.outputs[0]).to_pylist()
    unselected = [r for r in rows if r["is_selected"] is False]
    assert [r["start"] for r in unselected] == [5]


def test_convert_run_renames_ids_repeated_on_opposite_strands(run_files: dict[str, Path], tmp_path: Path) -> None:
    extra = f"{RUN}_0004\tINFERNAL 1.1.3\tmisc_feature\t328\t430\t%s\t%s\t.\tID=dup;model=RF02000"
    rows = [*FUNCTIONAL_ROWS, extra % ("41.5", "-"), extra % ("42.2", "+")]
    run_files[ft.FUNCTIONAL] = _write(tmp_path, "functional_dup.gff", rows)
    del run_files[ft.STRUCTURAL]
    assert ft.check_run(run_files)["functional_ids_unique_with_strand"]["passed"] is True
    result = fc.convert_run(RUN, run_files, {}, tmp_path / "out")
    assert result.duplicate_feature_ids == 0
    assert result.renamed_duplicate_ids == 2
    renamed = [r for r in pq.read_table(result.outputs[0]).to_pylist() if r["feature_id"].startswith("dup|")]
    assert sorted(r["feature_id"] for r in renamed) == ["dup|+", "dup|-"]
    assert all(("ID", "dup") in {(a["key"], a["value"]) for a in r["attributes"]} for r in renamed)


def test_convert_run_refuses_unselected_when_callers_miss_selected_rows(
    run_files: dict[str, Path], tmp_path: Path
) -> None:
    del run_files["Genemark Annotation GFF"]
    result = fc.convert_run(RUN, run_files, {}, tmp_path / "out", include_unselected=True)
    assert result.unselected_refused
    assert all(r["is_selected"] is not False for r in pq.read_table(result.outputs[0]).to_pylist())


def test_hits_follow_a_renamed_cds(run_files: dict[str, Path], tmp_path: Path) -> None:
    rna = f"{RUN}_0001\tINFERNAL 1.1.3\tmisc_feature\t2\t730\t108.4\t-\t.\tID={G1};model=RF02743"
    run_files[ft.FUNCTIONAL] = _write(tmp_path, "functional_shared.gff", [*FUNCTIONAL_ROWS, rna])
    result = fc.convert_run(RUN, run_files, {}, tmp_path / "out")
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


def test_cli_convert_writes_a_summary(run_files: dict[str, Path], tmp_path: Path) -> None:
    import json

    from click.testing import CliRunner

    from nmdc_lakehouse.cli import cli

    plan_path, runs_path, cache = _cli_fixture(run_files, tmp_path)
    out = tmp_path / "parquet"
    args = [str(plan_path), "--runs", str(runs_path), "--cache-dir", str(cache), "--out-dir", str(out)]
    result = CliRunner().invoke(cli, ["feature-convert", *args, "--include-unselected"])
    assert result.exit_code == 0, result.output
    summary = json.loads((out / "conversion_summary.json").read_text())
    assert summary["missing_functional_gff"] == []
    assert summary["converted"][0]["run_id"] == RUN
    assert summary["converted"][0]["unselected_refused"] is None
    assert summary["converted"][0]["orphan_hits"] == {}


def test_cli_convert_fails_when_a_run_has_no_functional_file(run_files: dict[str, Path], tmp_path: Path) -> None:
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
    assert result.exit_code != 0
    assert "had no Functional Annotation GFF" in result.output
    import json

    summary = json.loads((tmp_path / "out" / "conversion_summary.json").read_text())
    assert summary == {"converted": [], "missing_functional_gff": [RUN]}


def test_convert_run_refuses_to_write_a_repeated_feature_id(run_files: dict[str, Path], tmp_path: Path) -> None:
    # Same ID and strand: the strand cannot separate these, so the run is refused, not numbered.
    row = f"{RUN}_0004\tx\tmisc_feature\t1\t9\t.\t+\t.\tID=same"
    same = dict(run_files)
    same[ft.FUNCTIONAL] = _write(tmp_path, "functional_same.gff", [*FUNCTIONAL_ROWS, row, row])
    with pytest.raises(fc.DuplicateFeatureIdError, match=r"same\|\+"):
        fc.convert_run(RUN, same, {}, tmp_path / "same")
    # A hit whose derived feature_id collides has no renaming rule either.
    hit = f"{G1}\tHMMER 3.1b2\tPF00001\t10\t80\t50.3\t.\t.\tID={G1}_10_80"
    run_files["Pfam Annotation GFF"] = _write(tmp_path, "pfam_twice.gff", [hit, hit])
    out = tmp_path / "bad"
    with pytest.raises(fc.DuplicateFeatureIdError):
        fc.convert_run(RUN, run_files, {}, out)
    assert [p.name for p in out.iterdir()] == [".partial"]
    assert list((out / ".partial").iterdir()) == []


def test_contigs_carry_the_assembly_run(run_files: dict[str, Path], tmp_path: Path) -> None:
    result = fc.convert_run(RUN, run_files, {}, tmp_path / "out", assembly_run="nmdc:wfmgas-99-a.1")
    contigs = pq.read_table(result.outputs[1]).to_pylist()
    assert {c["generated_by"] for c in contigs} == {"nmdc:wfmgas-99-a.1"}
    features = pq.read_table(result.outputs[0]).to_pylist()
    assert {f["generated_by"] for f in features} == {RUN}
    unknown = fc.convert_run(RUN, run_files, {}, tmp_path / "unknown")
    assert {c["generated_by"] for c in pq.read_table(unknown.outputs[1]).to_pylist()} == {None}


def test_convert_run_counts_hits_on_unknown_genes_without_writing_them(
    run_files: dict[str, Path], tmp_path: Path
) -> None:
    stray = "nmdc:missing_gene\tHMMER 3.1b2\tPF00003\t1\t9\t5.0\t.\t.\tID=stray"
    pfam = run_files["Pfam Annotation GFF"]
    pfam.write_text(pfam.read_text() + stray + "\n")
    result = fc.convert_run(RUN, run_files, {}, tmp_path / "out")
    assert result.orphan_hits == {"Pfam Annotation GFF": 1}
    features = pq.read_table(result.outputs[0]).to_pylist()
    assert not any(r["feature_id"].startswith("stray") for r in features)
    contigs = {c["contig_id"] for c in pq.read_table(result.outputs[1]).to_pylist()}
    assert "nmdc:missing_gene" not in contigs


def test_hits_on_an_id_shared_by_two_cds_rows_are_counted_not_written(
    run_files: dict[str, Path], tmp_path: Path
) -> None:
    twin = FUNCTIONAL_ROWS[0].replace("\t+\t0\t", "\t-\t0\t")
    run_files[ft.FUNCTIONAL] = _write(tmp_path, "functional_twin.gff", [*FUNCTIONAL_ROWS, twin])
    result = fc.convert_run(RUN, run_files, {}, tmp_path / "out")
    assert result.ambiguous_parent_hits["Pfam Annotation GFF"] == 3
    assert "pfam" in result.dropped_keys
    rows = pq.read_table(result.outputs[0]).to_pylist()
    twins = [r for r in rows if r["feature_id"].startswith(f"{G1}|")]
    # Their hits were not written, so both rows keep the Pfam accessions.
    assert all(("pfam", "PF00001,PF00002") in {(a["key"], a["value"]) for a in r["attributes"]} for r in twins)
    ids = {r["feature_id"] for r in rows}
    assert all(p in ids for r in rows for p in r["parent"])


def test_small_batches_give_the_same_output(run_files: dict[str, Path], tmp_path: Path) -> None:
    one = fc.convert_run(RUN, run_files, {}, tmp_path / "a")
    many = fc.convert_run(RUN, run_files, {}, tmp_path / "b", batch_rows=1)
    assert pq.read_table(one.outputs[0]).to_pylist() == pq.read_table(many.outputs[0]).to_pylist()
    assert pq.read_table(one.outputs[1]).to_pylist() == pq.read_table(many.outputs[1]).to_pylist()
    assert many.contig_rows == one.contig_rows == 2
    assert list((tmp_path / "b" / ".partial").iterdir()) == []


def test_cli_convert_refuses_an_empty_run_list(run_files: dict[str, Path], tmp_path: Path) -> None:
    from click.testing import CliRunner

    from nmdc_lakehouse.cli import cli

    plan_path, runs_path, cache = _cli_fixture(run_files, tmp_path)
    runs_path.write_text("")
    args = [str(plan_path), "--runs", str(runs_path), "--cache-dir", str(cache), "--out-dir", str(tmp_path / "o")]
    result = CliRunner().invoke(cli, ["feature-convert", *args])
    assert result.exit_code != 0
    assert "lists no runs" in result.output


@pytest.mark.parametrize("run_id", ["../escape", "/abs/path", "a/b", "..", ""])
def test_convert_run_refuses_run_ids_that_are_not_one_directory_name(
    run_files: dict[str, Path], tmp_path: Path, run_id: str
) -> None:
    victim = tmp_path / "escape"
    victim.mkdir()
    (victim / "keep.txt").write_text("x")
    with pytest.raises(ValueError, match="not a usable run ID"):
        fc.convert_run(run_id, run_files, {}, tmp_path / "out")
    assert (victim / "keep.txt").exists()


def test_unselected_keeps_a_competing_call_at_a_selected_interval(run_files: dict[str, Path], tmp_path: Path) -> None:
    competing = f"{RUN}_0001\tGeneMark.hmm-2\tCDS\t2\t730\t21.5\t+\t0\tID={G1};translation_table=11"
    genemark = run_files["Genemark Annotation GFF"]
    genemark.write_text(genemark.read_text() + competing + "\n")
    result = fc.convert_run(RUN, run_files, {}, tmp_path / "out", include_unselected=True)
    rows = pq.read_table(result.outputs[0]).to_pylist()
    unselected = sorted((r["source"], r["start"]) for r in rows if r["is_selected"] is False)
    assert unselected == [("GeneMark.hmm-2", 2), ("Prodigal v2.6.3", 5)]


def test_convert_run_does_not_replace_another_runs_output(run_files: dict[str, Path], tmp_path: Path) -> None:
    out = tmp_path / "out"
    first = fc.convert_run("nmdc:wfmgan-x.1", run_files, {}, out)
    with pytest.raises(ValueError, match="another run"):
        fc.convert_run("nmdc_wfmgan-x.1", run_files, {}, out)
    assert Path(first.outputs[0]).exists()
    assert (Path(first.outputs[0]).parent / fc.RUN_ID_FILE).read_text().strip() == "nmdc:wfmgan-x.1"
    # The same run converts again over its own output.
    fc.convert_run("nmdc:wfmgan-x.1", run_files, {}, out)
    assert list((out / ".partial").iterdir()) == []


def test_staging_does_not_touch_a_run_named_like_a_staging_directory(
    run_files: dict[str, Path], tmp_path: Path
) -> None:
    out = tmp_path / "out"
    other = fc.convert_run("nmdc:a.partial", run_files, {}, out)
    fc.convert_run("nmdc:a", run_files, {}, out)
    assert Path(other.outputs[0]).exists()


def test_unselected_keeps_a_same_call_row_that_differs(run_files: dict[str, Path], tmp_path: Path) -> None:
    # Prodigal reported G1's call twice: once as selected, once with another score.
    rescored = f"{RUN}_0001\tProdigal v2.6.3\tCDS\t2\t730\t99.9\t+\t0\tID={G1}"
    prodigal = run_files["Prodigal Annotation GFF"]
    prodigal.write_text(prodigal.read_text() + rescored + "\n")
    result = fc.convert_run(RUN, run_files, {}, tmp_path / "out", include_unselected=True)
    unselected = [r for r in pq.read_table(result.outputs[0]).to_pylist() if r["is_selected"] is False]
    # The selected row's own Prodigal row is skipped; the rescored one is kept.
    assert sorted((r["start"], r["score"]) for r in unselected) == [(2, 99.9), (5, 1.0)]


def test_hits_on_a_repeated_id_with_no_cds_are_counted_not_written(run_files: dict[str, Path], tmp_path: Path) -> None:
    # G2's ID is carried by two RNA rows and no CDS, so a hit on G2 has no gene to name.
    rows = [r for r in FUNCTIONAL_ROWS if f"ID={G2};" not in r] + [
        f"{RUN}_0001\tINFERNAL 1.1.3\tmisc_feature\t838\t2616\t9.0\t+\t.\tID={G2};model=RF1",
        f"{RUN}_0001\tINFERNAL 1.1.3\tmisc_feature\t838\t2616\t9.0\t-\t.\tID={G2};model=RF1",
    ]
    run_files[ft.FUNCTIONAL] = _write(tmp_path, "functional_rna_twins.gff", rows)
    result = fc.convert_run(RUN, run_files, {}, tmp_path / "out")
    assert result.ambiguous_parent_hits["Clusters of Orthologous Groups (COG) Annotation GFF"] == 1
    written = pq.read_table(result.outputs[0]).to_pylist()
    ids = {r["feature_id"] for r in written}
    assert all(p in ids for r in written for p in r["parent"])


def test_convert_run_refuses_a_symlinked_staging_directory(run_files: dict[str, Path], tmp_path: Path) -> None:
    outside = tmp_path / "outside"
    (outside / f"{RUN.replace(':', '_')}").mkdir(parents=True)
    (outside / f"{RUN.replace(':', '_')}" / "keep.txt").write_text("x")
    out = tmp_path / "out"
    out.mkdir()
    (out / ".partial").symlink_to(outside, target_is_directory=True)
    with pytest.raises(ValueError, match="symlink"):
        fc.convert_run(RUN, run_files, {}, out)
    assert (outside / f"{RUN.replace(':', '_')}" / "keep.txt").exists()
