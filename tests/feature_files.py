"""Small made-up annotation files in NMDC's layout, shared by the feature table tests."""

from __future__ import annotations

from pathlib import Path

from nmdc_lakehouse import feature_tables as ft

#: KO and EC TSV columns 4-11 for the one KO_EC hit: identity, query start and end, subject start
#: and end, e-value, bit score, alignment length.
BLAST_FIELDS = "47.7\t2\t389\t231\t624\t1e-9\t362\t388"

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


def make_run_files(tmp_path: Path) -> dict[str, Path]:
    """One consistent run: every relation `check_run` tests holds."""
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
            [
                f"{G1}\tlastal 1456\tKO:K00001__EC:1.1.1.1_EC:2.2.2.2\t2\t389\t362\t.\t.\tID={G1}_2_389;"
                "subject_gene_ids=277;subject_start=231;subject_end=624;evalue=1e-9;percent_identity=47.7;"
                "alignment_length=388"
            ],
        ),
        ft.KO_TSV: _write(tmp_path, "ko.tsv", [f"{G1}\t277\tKO:K00001\t{BLAST_FIELDS}"]),
        ft.EC_TSV: _write(
            tmp_path, "ec.tsv", [f"{G1}\t277\tEC:1.1.1.1\t{BLAST_FIELDS}", f"{G1}\t277\tEC:2.2.2.2\t{BLAST_FIELDS}"]
        ),
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


def _run(run_id: str, inputs: list[str], outputs: list[str], version: str = "v1") -> dict[str, object]:
    return {
        "id": run_id,
        "type": "nmdc:MetagenomeAnnotation",
        "version": version,
        "has_input": inputs,
        "has_output": outputs,
    }


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
