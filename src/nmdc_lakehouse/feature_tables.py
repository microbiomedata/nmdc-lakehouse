"""Plan, check and convert NMDC annotation feature files into the BER feature model's shape.

The BER feature model is the draft LinkML schema at
https://github.com/turbomam/feature-table-corpus/blob/main/model/schema/ber_feature_model.yaml .
An NMDC metagenome or metatranscriptome annotation run writes about twenty feature-like files,
and most of them repeat one another. Measured on one run on 2026-09-23
(`nmdc:wfmgan-11-grwgtd27.2`):

- Structural Annotation GFF is the Functional Annotation GFF without the functional keys.
- Annotation KEGG Orthology, Annotation Enzyme Commission and Product Names repeat the KO_EC
  Annotation GFF and the Functional Annotation GFF.
- The per-system hit GFFs (Pfam, COG, TIGRFam, SMART, CATH FunFams, SUPERFam, KO_EC) are the only
  place hit coordinates, scores and repeated hits exist. The Functional Annotation GFF lists only
  their accessions.
- The gene-caller GFFs (Prodigal, GeneMark, tRNA, RFAM, CRT) add only the calls the pipeline did
  not select.

`check_run` restates each of those as a check, so a sample of runs can confirm them before they
are relied on. `convert_run` loads each observation once: genome features from the Functional
Annotation GFF, hits from the per-system GFFs, and, only on request, unselected caller rows.

Separately, some assemblies were annotated more than once (an older `.1` run and a newer `.2` run
over the same input). `plan_runs` keeps one annotation run per input so the same genes are not
loaded twice. Nothing here writes to BERDL.
"""

from __future__ import annotations

import csv
import json
import random
import re
from collections import Counter, defaultdict
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import requests

API_BASE = "https://api.microbiomedata.org"
USER_AGENT = "nmdc-lakehouse/feature_tables"

FUNCTIONAL = "Functional Annotation GFF"
STRUCTURAL = "Structural Annotation GFF"
CONTIG_MAPPING = "Contig Mapping File"
SCAFFOLD_LINEAGE = "Scaffold Lineage tsv"
KO_TSV = "Annotation KEGG Orthology"
EC_TSV = "Annotation Enzyme Commission"
PRODUCT_NAMES = "Product Names"
KO_EC = "KO_EC Annotation GFF"

#: Per-system hit GFFs, mapped to the Functional Annotation GFF key that lists the same accessions.
HIT_TYPES: dict[str, str] = {
    "Pfam Annotation GFF": "pfam",
    "Clusters of Orthologous Groups (COG) Annotation GFF": "cog",
    "TIGRFam Annotation GFF": "tigrfam",
    "SMART Annotation GFF": "smart",
    "CATH FunFams (Functional Families) Annotation GFF": "cath_funfam",
    "SUPERFam Annotation GFF": "superfamily",
    KO_EC: "ko",
}

CALLER_TYPES: tuple[str, ...] = (
    "Prodigal Annotation GFF",
    "Genemark Annotation GFF",
    "TRNA Annotation GFF",
    "RFAM Annotation GFF",
    "CRT Annotation GFF",
)

#: Types `check_run` reads. `convert_run` reads only FUNCTIONAL, HIT_TYPES, CONTIG_MAPPING and
#: SCAFFOLD_LINEAGE, plus CALLER_TYPES when unselected calls are requested.
CHECK_TYPES: tuple[str, ...] = (
    FUNCTIONAL,
    STRUCTURAL,
    *HIT_TYPES,
    *CALLER_TYPES,
    KO_TSV,
    EC_TSV,
    PRODUCT_NAMES,
    CONTIG_MAPPING,
    SCAFFOLD_LINEAGE,
    "Crispr Terms",
    "Gene Phylogeny tsv",
)

#: Types a sampled run must have. Contig Mapping File and Scaffold Lineage tsv are absent from
#: whole pipeline versions (for example every v1.0.5 run lacks Contig Mapping File, counted
#: 2026-09-23), so requiring them would leave those versions out of the sample.
SAMPLE_REQUIRED_TYPES: tuple[str, ...] = tuple(t for t in CHECK_TYPES if t not in (CONTIG_MAPPING, SCAFFOLD_LINEAGE))

ANNOTATION_RUN_TYPES = ("nmdc:MetagenomeAnnotation", "nmdc:MetatranscriptomeAnnotation")

MANIFEST_COLUMNS: tuple[str, ...] = (
    "id",
    "url",
    "data_object_type",
    "was_generated_by",
    "file_size_bytes",
    "md5_checksum",
)


# ---------------------------------------------------------------------------
# Public NMDC API
# ---------------------------------------------------------------------------


def fetch_collection(
    collection: str,
    filter_: Mapping[str, Any],
    projection: Sequence[str],
    *,
    page_size: int = 2000,
    session: requests.Session | None = None,
) -> list[dict[str, Any]]:
    """Every record of `collection` matching `filter_`, following page tokens to the end.

    The API refuses requests carrying Python's default User-Agent with HTTP 403 (observed
    2026-09-23), so one is always set.
    """
    session = session or requests.Session()
    rows: list[dict[str, Any]] = []
    token: str | None = None
    while True:
        params = {
            "filter": json.dumps(filter_),
            "max_page_size": str(page_size),
            "projection": ",".join(projection),
        }
        if token:
            params["page_token"] = token
        response = session.get(
            f"{API_BASE}/nmdcschema/{collection}",
            params=params,
            headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
            timeout=300,
        )
        response.raise_for_status()
        body = response.json()
        rows.extend(body["resources"])
        token = body.get("next_page_token")
        if not token:
            return rows


def fetch_inventory(types: Sequence[str] = CHECK_TYPES) -> dict[str, Any]:
    """Annotation runs and every data object of `types`, from the public API."""
    runs = fetch_collection(
        "workflow_execution_set",
        {"type": {"$in": list(ANNOTATION_RUN_TYPES)}},
        ["id", "type", "has_input", "has_output", "was_informed_by", "version", "ended_at_time"],
    )
    data_objects = fetch_collection(
        "data_object_set",
        {"data_object_type": {"$in": list(types)}},
        ["id", "data_object_type", "url", "file_size_bytes", "md5_checksum", "was_generated_by"],
    )
    return {"runs": runs, "data_objects": data_objects}


# ---------------------------------------------------------------------------
# One annotation run per input
# ---------------------------------------------------------------------------


def _run_rank(run: Mapping[str, Any]) -> tuple[int, str]:
    """Order reruns: the `.N` suffix of the run id, then the end time.

    Observed 2026-09-23: reruns over one input carry increasing suffixes (`.1`, `.2`, `.4`) and
    newer pipeline versions. An id without a numeric suffix ranks lowest.
    """
    suffix = str(run["id"]).rsplit(".", 1)[-1]
    return (int(suffix) if suffix.isdigit() else -1, str(run.get("ended_at_time") or ""))


@dataclass
class RunPlan:
    """Which annotation runs to load, and what was left out and why."""

    #: run id -> {"run": run record, "files": {data_object_type: data object}}
    selected: dict[str, dict[str, Any]] = field(default_factory=dict)
    #: superseded run id -> the selected run id over the same input
    superseded: dict[str, str] = field(default_factory=dict)
    #: data objects that no annotation run lists in has_output
    orphans: list[dict[str, Any]] = field(default_factory=list)
    #: (run id, data_object_type) pairs where one run lists more than one data object of a type
    ambiguous: list[tuple[str, str]] = field(default_factory=list)

    def summary(self) -> dict[str, Any]:
        """Counts of what was kept and left out, and bytes of the kept files by type."""
        sizes: Counter[str] = Counter()
        for entry in self.selected.values():
            for data_object_type, data_object in entry["files"].items():
                sizes[data_object_type] += int(data_object.get("file_size_bytes") or 0)
        return {
            "selected_runs": len(self.selected),
            "superseded_runs": len(self.superseded),
            "orphan_data_objects": dict(Counter(o["data_object_type"] for o in self.orphans)),
            "ambiguous": len(self.ambiguous),
            "selected_bytes_by_type": dict(sorted(sizes.items())),
        }


def plan_runs(runs: Iterable[Mapping[str, Any]], data_objects: Iterable[Mapping[str, Any]]) -> RunPlan:
    """Keep the highest-ranked annotation run per input and attach its files.

    Files are attached through the run's `has_output`, not the data object's `was_generated_by`:
    on 2026-09-23, 1,870 of 5,146 Functional Annotation GFF records had no `was_generated_by`
    although their run listed them.
    """
    plan = RunPlan()
    by_id = {d["id"]: d for d in data_objects}
    by_input: dict[tuple[str, ...], list[Mapping[str, Any]]] = defaultdict(list)
    for run in runs:
        by_input[tuple(sorted(run.get("has_input") or [run["id"]]))].append(run)
    owned: set[str] = set()
    for group in by_input.values():
        group = sorted(group, key=_run_rank, reverse=True)
        keep = group[0]
        for other in group[1:]:
            plan.superseded[str(other["id"])] = str(keep["id"])
        files: dict[str, dict[str, Any]] = {}
        for run in group:
            owned.update(run.get("has_output") or [])
        for output in keep.get("has_output") or []:
            data_object = by_id.get(output)
            if data_object is None:
                continue
            data_object_type = str(data_object["data_object_type"])
            if data_object_type in files:
                plan.ambiguous.append((str(keep["id"]), data_object_type))
                continue
            files[data_object_type] = dict(data_object)
        plan.selected[str(keep["id"])] = {"run": dict(keep), "files": files}
    plan.orphans = [dict(d) for d in by_id.values() if d["id"] not in owned]
    return plan


def sample_runs(
    plan: RunPlan,
    count: int,
    *,
    required_types: Sequence[str] = SAMPLE_REQUIRED_TYPES,
    max_run_bytes: int | None = None,
    seed: int = 0,
) -> list[str]:
    """Pick `count` selected runs spread across run type and pipeline version.

    Only runs with a non-empty file of every required type qualify, except CRT Annotation GFF
    and Crispr Terms, which are empty whenever no CRISPR array was found. Runs are taken from
    each (run type, version) stratum in turn, so a small version is represented as well as a
    large one: the point is to catch a version whose files relate differently. `max_run_bytes`
    keeps a local sample small; it biases the sample toward small runs, and the caller should
    say so wherever the result is reported.
    """
    may_be_empty = {"CRT Annotation GFF", "Crispr Terms"}
    strata: dict[tuple[str, str], list[str]] = defaultdict(list)
    for run_id, entry in sorted(plan.selected.items()):
        files = entry["files"]
        if any(t not in files for t in required_types):
            continue
        if any(int(files[t].get("file_size_bytes") or 0) == 0 for t in required_types if t not in may_be_empty):
            continue
        total = sum(int(files[t].get("file_size_bytes") or 0) for t in required_types)
        if max_run_bytes is not None and total > max_run_bytes:
            continue
        run = entry["run"]
        strata[(str(run.get("type")), str(run.get("version")))].append(run_id)
    rng = random.Random(seed)
    eligible = sum(len(v) for v in strata.values())
    if eligible == 0:
        return []
    chosen: list[str] = []
    remaining = {k: rng.sample(v, len(v)) for k, v in sorted(strata.items())}
    while len(chosen) < min(count, eligible):
        for key in remaining:
            if remaining[key] and len(chosen) < count:
                chosen.append(remaining[key].pop())
    return sorted(chosen)


def write_download_manifest(plan: RunPlan, run_ids: Sequence[str], types: Sequence[str], path: Path) -> int:
    """Write the CSV `scripts/download_to_cache.py` reads. Returns the number of rows."""
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = 0
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=MANIFEST_COLUMNS)
        writer.writeheader()
        for run_id in run_ids:
            for data_object_type in types:
                data_object = plan.selected[run_id]["files"].get(data_object_type)
                if not data_object or not data_object.get("url"):
                    continue
                writer.writerow(
                    {
                        "id": data_object["id"],
                        "url": data_object["url"],
                        "data_object_type": data_object_type,
                        "was_generated_by": run_id,
                        "file_size_bytes": data_object.get("file_size_bytes"),
                        "md5_checksum": data_object.get("md5_checksum"),
                    }
                )
                rows += 1
    return rows


# ---------------------------------------------------------------------------
# Reading the files
# ---------------------------------------------------------------------------


def read_table(path: Path) -> Iterator[list[str]]:
    """Tab-separated rows, skipping blank lines and `#` comment or directive lines."""
    with path.open(encoding="utf-8", errors="strict") as handle:
        for line in handle:
            if not line.strip() or line.startswith("#"):
                continue
            yield line.rstrip("\n").split("\t")


def parse_attributes(column9: str) -> list[tuple[str, str]]:
    """GFF column 9 as ordered (key, value) pairs; repeated keys stay separate.

    NMDC files do not percent-encode, so `;` and `=` are split literally. A segment with no `=`
    is kept with an empty value rather than dropped.
    """
    pairs: list[tuple[str, str]] = []
    for segment in column9.split(";"):
        if not segment:
            continue
        key, _, value = segment.partition("=")
        pairs.append((key, value))
    return pairs


def _first(pairs: Sequence[tuple[str, str]], key: str) -> str | None:
    return next((v for k, v in pairs if k == key), None)


def _accessions(pairs: Sequence[tuple[str, str]], key: str) -> set[str]:
    return {a for k, v in pairs if k == key for a in v.split(",") if a}


_EC_SPLIT = re.compile(r"_(?=(?:KO|EC):)")


def split_ko_ec(column3: str) -> tuple[set[str], set[str]]:
    """KO_EC Annotation GFF column 3, e.g. `KO:K03273__EC:3.1.3.82_EC:3.1.3.83`, as (KOs, ECs)."""
    kos: set[str] = set()
    ecs: set[str] = set()
    for part in column3.split("__"):
        for token in _EC_SPLIT.split(part):
            if token.startswith("KO:"):
                kos.add(token)
            elif token.startswith("EC:"):
                ecs.add(token)
    return kos, ecs


# ---------------------------------------------------------------------------
# Overlap checks
# ---------------------------------------------------------------------------


def _check(passed: bool, **counts: Any) -> dict[str, Any]:
    return {"passed": passed, **counts}


def check_run(files: Mapping[str, Path]) -> dict[str, dict[str, Any]]:
    """Test, for one run's downloaded files, each claim that lets a file type be skipped.

    Every check reports counts as well as a verdict, so a failure says how far off it was.
    A type missing from `files` produces a `skipped` entry rather than a pass.
    """
    results: dict[str, dict[str, Any]] = {}
    if FUNCTIONAL not in files:
        return {"functional": {"passed": False, "skipped": "no Functional Annotation GFF"}}
    functional = list(read_table(files[FUNCTIONAL]))
    f_pairs = [parse_attributes(r[8]) if len(r) > 8 else [] for r in functional]
    f_ids = [_first(p, "ID") for p in f_pairs]
    with_strand = {(i, r[6]) for i, r in zip(f_ids, functional, strict=True)}
    results["functional_ids_unique_with_strand"] = _check(
        len(with_strand) == len(f_ids) and None not in f_ids,
        rows=len(f_ids),
        distinct_ids=len(set(f_ids)),
        distinct_ids_with_strand=len(with_strand),
    )

    if STRUCTURAL in files:
        structural = list(read_table(files[STRUCTURAL]))
        same_core = [r[:8] for r in structural] == [r[:8] for r in functional]
        contained = sum(
            1
            for s, fp in zip(structural, f_pairs, strict=False)
            if set(parse_attributes(s[8]) if len(s) > 8 else []) <= set(fp)
        )
        results["structural_is_functional_subset"] = _check(
            same_core and contained == len(structural),
            structural_rows=len(structural),
            functional_rows=len(functional),
            columns_1_to_8_identical=same_core,
            rows_with_attributes_contained=contained,
        )
    else:
        results["structural_is_functional_subset"] = {"passed": False, "skipped": "missing"}

    def locus(r: Sequence[str]) -> tuple[str, ...]:
        return (r[0], r[2], r[3], r[4], r[6])

    present_callers = [t for t in CALLER_TYPES if t in files]
    caller_keys: set[tuple[str, ...]] = set()
    for t in present_callers:
        caller_keys.update(locus(r) for r in read_table(files[t]))
    selected_keys = {locus(r) for r in functional}
    results["selected_rows_in_callers"] = _check(
        selected_keys <= caller_keys,
        callers_present=len(present_callers),
        selected=len(selected_keys),
        selected_found_in_callers=len(selected_keys & caller_keys),
        caller_rows=len(caller_keys),
        unselected_caller_rows=len(caller_keys - selected_keys),
    )

    gene_ids = set(f_ids)
    for hit_type, key in HIT_TYPES.items():
        name = f"hits_match_functional:{key if hit_type != KO_EC else 'ko_ec'}"
        if hit_type not in files:
            results[name] = {"passed": False, "skipped": "missing"}
            continue
        hits = list(read_table(files[hit_type]))
        by_gene: dict[str, set[str]] = defaultdict(set)
        ec_by_gene: dict[str, set[str]] = defaultdict(set)
        for r in hits:
            if hit_type == KO_EC:
                kos, ecs = split_ko_ec(r[2])
                by_gene[r[0]].update(kos)
                ec_by_gene[r[0]].update(ecs)
            else:
                by_gene[r[0]].add(r[2])
        f_sets = {g: _accessions(p, key) for g, p in zip(f_ids, f_pairs, strict=True) if g}
        genes = {g for g, s in f_sets.items() if s} | set(by_gene)
        equal = sum(1 for g in genes if f_sets.get(g, set()) == by_gene.get(g, set()))
        detail: dict[str, Any] = {
            "hit_rows": len(hits),
            "genes": len(genes),
            "genes_equal": equal,
            "hit_seqids_not_functional_ids": len(set(by_gene) - gene_ids),
        }
        passed = equal == len(genes) and detail["hit_seqids_not_functional_ids"] == 0
        if hit_type == KO_EC:
            f_ec = {g: _accessions(p, "ec_number") for g, p in zip(f_ids, f_pairs, strict=True) if g}
            ec_genes = {g for g, s in f_ec.items() if s} | set(ec_by_gene)
            ec_equal = sum(1 for g in ec_genes if f_ec.get(g, set()) == ec_by_gene.get(g, set()))
            detail.update(ec_genes=len(ec_genes), ec_genes_equal=ec_equal)
            passed = passed and ec_equal == len(ec_genes)
        results[name] = _check(passed, **detail)

    for tsv_type, label, pick in ((KO_TSV, "ko_tsv_in_ko_ec_gff", 0), (EC_TSV, "ec_tsv_in_ko_ec_gff", 1)):
        if tsv_type not in files or KO_EC not in files:
            results[label] = {"passed": False, "skipped": "missing"}
            continue
        tsv_pairs = {(r[0], r[2]) for r in read_table(files[tsv_type])}
        gff_pairs = {(r[0], a) for r in read_table(files[KO_EC]) for a in split_ko_ec(r[2])[pick]}
        results[label] = _check(
            tsv_pairs == gff_pairs,
            tsv_pairs=len(tsv_pairs),
            gff_pairs=len(gff_pairs),
            tsv_only=len(tsv_pairs - gff_pairs),
            gff_only=len(gff_pairs - tsv_pairs),
        )

    if PRODUCT_NAMES in files:
        product = {g: (_first(p, "product"), _first(p, "product_source")) for g, p in zip(f_ids, f_pairs, strict=True)}
        names = list(read_table(files[PRODUCT_NAMES]))
        name_matched = source_matched = source_only_here = 0
        for r in names:
            f_product, f_source = product.get(r[0], (None, None))
            source_label = r[2] if len(r) > 2 else None
            name_matched += f_product == r[1]
            if f_source is None and source_label:
                # Observed 2026-09-23 on RNA rows: Product Names says `rRNA_28S` or `tRNA` where
                # the Functional Annotation GFF has no product_source at all.
                source_only_here += 1
            else:
                source_matched += f_source == source_label
        results["product_names_in_functional"] = _check(
            name_matched == len(names) and source_matched + source_only_here == len(names),
            rows=len(names),
            names_matched=name_matched,
            sources_matched=source_matched,
            source_label_only_in_product_names=source_only_here,
        )
    else:
        results["product_names_in_functional"] = {"passed": False, "skipped": "missing"}

    seqids = {r[0] for r in functional}
    if CONTIG_MAPPING in files:
        mapped = {r[1] for r in read_table(files[CONTIG_MAPPING]) if len(r) > 1}
        results["functional_seqids_in_contig_mapping"] = _check(
            seqids <= mapped, seqids=len(seqids), mapped=len(mapped), unmapped=len(seqids - mapped)
        )
    else:
        results["functional_seqids_in_contig_mapping"] = {"passed": False, "skipped": "missing"}

    if "Crispr Terms" in files and "CRT Annotation GFF" in files:
        terms = sum(1 for _ in read_table(files["Crispr Terms"]))
        crt = [r for r in read_table(files["CRT Annotation GFF"])]
        arrays = sum(1 for r in crt if r[2] == "CRISPR")
        crispr_selected = sum(1 for r in functional if r[2] == "CRISPR")
        results["crispr_observed"] = _check(
            True, crispr_terms_rows=terms, crt_rows=len(crt), crt_arrays=arrays, functional_crispr_rows=crispr_selected
        )
    return results


# ---------------------------------------------------------------------------
# Conversion to Parquet
# ---------------------------------------------------------------------------

#: Functional Annotation GFF keys that repeat a per-system hit file. `convert_run` drops one only
#: when that run's hit file exists and the matching check passed, so nothing is lost when a hit
#: file is missing or disagrees.
DERIVABLE_KEYS: dict[str, str] = {**{v: k for k, v in HIT_TYPES.items()}, "ec_number": KO_EC}


def _feature_schema() -> Any:
    import pyarrow as pa

    attribute = pa.struct([("key", pa.string()), ("value", pa.string())])
    return pa.schema(
        [
            ("feature_id", pa.string()),
            ("seqid", pa.string()),
            ("source", pa.string()),
            ("type", pa.string()),
            ("start", pa.int64()),
            ("end", pa.int64()),
            ("coordinate_system", pa.string()),
            ("score", pa.float64()),
            ("strand", pa.string()),
            ("phase", pa.int8()),
            ("parent", pa.list_(pa.string())),
            ("attributes", pa.list_(attribute)),
            ("generated_by", pa.string()),
            ("source_files", pa.list_(pa.string())),
            ("is_selected", pa.bool_()),
            ("product", pa.string()),
            ("product_source", pa.string()),
            ("source_data_object_type", pa.string()),
        ]
    )


def _contig_schema() -> Any:
    import pyarrow as pa

    return pa.schema(
        [
            ("contig_id", pa.string()),
            ("assembly_contig_id", pa.string()),
            ("taxonomic_lineage", pa.list_(pa.string())),
            ("lineage_confidence", pa.float64()),
            ("generated_by", pa.string()),
            ("source_files", pa.list_(pa.string())),
        ]
    )


def _number(value: str, kind: type) -> Any:
    if value in ("", "."):
        return None
    return kind(value)


@dataclass
class ConversionResult:
    """What `convert_run` wrote for one run, and what it left out."""

    run_id: str
    feature_rows: Counter[str] = field(default_factory=Counter)
    contig_rows: int = 0
    dropped_keys: list[str] = field(default_factory=list)
    duplicate_feature_ids: int = 0
    renamed_duplicate_ids: int = 0
    #: Why unselected calls were not written although requested, or None.
    unselected_refused: str | None = None
    outputs: list[str] = field(default_factory=list)


def convert_run(
    run_id: str,
    files: Mapping[str, Path],
    urls: Mapping[str, str],
    out_dir: Path,
    *,
    include_unselected: bool = False,
    checks: Mapping[str, Mapping[str, Any]] | None = None,
) -> ConversionResult:
    """Write `features.parquet` and `contigs.parquet` for one run under `out_dir/<run id>/`.

    Genome features come from the Functional Annotation GFF with `coordinate_system` `contig`.
    Hits come from each per-system GFF with `coordinate_system` `protein`, `parent` set to the
    gene they sit on, and `seqid` set to that gene's contig, following the corpus profile
    `nmdc-pfam-protein/1.0.0`. A hit's `feature_id` joins its GFF `ID`, its file type key and its
    column 3, because one gene can carry the same coordinates in several systems; the original
    `ID` stays in `attributes`.

    `assembly_contig_id` and `source_data_object_type` are not slots of the model yet.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    checks = checks if checks is not None else check_run(files)
    result = ConversionResult(run_id=run_id)
    rows: list[dict[str, Any]] = []

    drop: set[str] = set()
    for key, hit_type in DERIVABLE_KEYS.items():
        label = f"hits_match_functional:{'ko_ec' if hit_type == KO_EC else key}"
        if hit_type in files and checks.get(label, {}).get("passed"):
            drop.add(key)
    result.dropped_keys = sorted(drop)

    gene_seqid: dict[str, str] = {}
    selected_loci: set[tuple[str, ...]] = set()
    functional_url = urls.get(FUNCTIONAL)
    # Observed 2026-09-23 in v1.0.2 and v1.0.4 runs: an RFAM hit over one interval on both strands
    # gets one `ID` twice, because the ID encodes the interval but not the strand. Those rows get
    # the strand appended, and a counter if that is still not enough.
    id_counts = Counter(
        _first(parse_attributes(r[8]) if len(r) > 8 else [], "ID") for r in read_table(files[FUNCTIONAL])
    )
    seen_ids: Counter[str] = Counter()
    cds_renamed: dict[str, list[str]] = {}
    for r in read_table(files[FUNCTIONAL]):
        pairs = parse_attributes(r[8]) if len(r) > 8 else []
        source_id = _first(pairs, "ID")
        feature_id = source_id or f"{r[0]}_{r[3]}_{r[4]}"
        if source_id and id_counts[source_id] > 1:
            feature_id = f"{source_id}|{r[6]}"
            seen_ids[feature_id] += 1
            if seen_ids[feature_id] > 1:
                feature_id = f"{feature_id}|{seen_ids[feature_id]}"
            result.renamed_duplicate_ids += 1
            if r[2] == "CDS":
                # Hits sit on proteins, so a renamed CDS is the only row they can belong to.
                # Observed 2026-09-23: a GeneMark CDS and an RFAM sRNA sharing one ID.
                cds_renamed.setdefault(source_id, []).append(feature_id)
        gene_seqid[feature_id] = r[0]
        selected_loci.add((r[0], r[2], r[3], r[4], r[6]))
        rows.append(
            {
                "feature_id": feature_id,
                "seqid": r[0],
                "source": r[1] or None,
                "type": r[2],
                "start": int(r[3]),
                "end": int(r[4]),
                "coordinate_system": "contig",
                "score": _number(r[5], float),
                "strand": r[6],
                "phase": _number(r[7], int),
                "parent": [v for k, v in pairs if k == "Parent"],
                "attributes": [
                    {"key": k, "value": v}
                    for k, v in pairs
                    if (k != "ID" or feature_id != source_id)
                    and k not in ("Parent", "product", "product_source")
                    and k not in drop
                ],
                "generated_by": run_id,
                "source_files": [functional_url] if functional_url else [],
                "is_selected": True,
                "product": _first(pairs, "product"),
                "product_source": _first(pairs, "product_source"),
                "source_data_object_type": FUNCTIONAL,
            }
        )
        result.feature_rows[FUNCTIONAL] += 1

    for hit_type, key in HIT_TYPES.items():
        if hit_type not in files:
            continue
        label = "ko_ec" if hit_type == KO_EC else key
        for r in read_table(files[hit_type]):
            pairs = parse_attributes(r[8]) if len(r) > 8 else []
            source_id = _first(pairs, "ID") or f"{r[0]}_{r[3]}_{r[4]}"
            rows.append(
                {
                    "feature_id": f"{source_id}|{label}|{r[2]}",
                    "seqid": gene_seqid.get(r[0], r[0]),
                    "source": r[1] or None,
                    "type": r[2],
                    "start": int(r[3]),
                    "end": int(r[4]),
                    "coordinate_system": "protein",
                    "score": _number(r[5], float),
                    "strand": r[6],
                    "phase": _number(r[7], int),
                    "parent": cds_renamed[r[0]] if len(cds_renamed.get(r[0], [])) == 1 else [r[0]],
                    "attributes": [{"key": k, "value": v} for k, v in pairs],
                    "generated_by": run_id,
                    "source_files": [urls[hit_type]] if hit_type in urls else [],
                    "is_selected": None,
                    "product": None,
                    "product_source": None,
                    "source_data_object_type": hit_type,
                }
            )
            result.feature_rows[hit_type] += 1

    if include_unselected and not checks.get("selected_rows_in_callers", {}).get("passed"):
        # In v1.0.2 and v1.0.4 runs some selected rows have coordinates no caller reported (a
        # Prodigal call at 1-336 selected as 1-177, observed 2026-09-23), so "caller row not
        # selected" would mislabel the source of a selected feature as unselected.
        result.unselected_refused = "selected rows are not all found in the caller files"
    elif include_unselected:
        for caller_type in CALLER_TYPES:
            if caller_type not in files:
                continue
            for r in read_table(files[caller_type]):
                if (r[0], r[2], r[3], r[4], r[6]) in selected_loci:
                    continue
                pairs = parse_attributes(r[8]) if len(r) > 8 else []
                source_id = _first(pairs, "ID") or f"{r[0]}_{r[3]}_{r[4]}"
                rows.append(
                    {
                        "feature_id": f"{source_id}|unselected|{r[1]}",
                        "seqid": r[0],
                        "source": r[1] or None,
                        "type": r[2],
                        "start": int(r[3]),
                        "end": int(r[4]),
                        "coordinate_system": "contig",
                        "score": _number(r[5], float),
                        "strand": r[6],
                        "phase": _number(r[7], int),
                        "parent": [v for k, v in pairs if k == "Parent"],
                        "attributes": [{"key": k, "value": v} for k, v in pairs],
                        "generated_by": run_id,
                        "source_files": [urls[caller_type]] if caller_type in urls else [],
                        "is_selected": False,
                        "product": None,
                        "product_source": None,
                        "source_data_object_type": caller_type,
                    }
                )
                result.feature_rows[caller_type] += 1

    ids = Counter(r["feature_id"] for r in rows)
    result.duplicate_feature_ids = sum(v - 1 for v in ids.values() if v > 1)

    lineage: dict[str, tuple[list[str], float | None]] = {}
    if SCAFFOLD_LINEAGE in files:
        for r in read_table(files[SCAFFOLD_LINEAGE]):
            lineage[r[0]] = (
                r[1].split(";") if len(r) > 1 and r[1] else [],
                _number(r[2], float) if len(r) > 2 else None,
            )
    assembly: dict[str, str] = {}
    if CONTIG_MAPPING in files:
        for r in read_table(files[CONTIG_MAPPING]):
            if len(r) > 1:
                assembly[r[1]] = r[0]
    contig_rows = []
    for contig_id in sorted({r["seqid"] for r in rows}):
        tax, confidence = lineage.get(contig_id, ([], None))
        sources = [urls[t] for t in (CONTIG_MAPPING, SCAFFOLD_LINEAGE) if t in urls and t in files]
        contig_rows.append(
            {
                "contig_id": contig_id,
                "assembly_contig_id": assembly.get(contig_id),
                "taxonomic_lineage": tax,
                "lineage_confidence": confidence,
                "generated_by": run_id,
                "source_files": sources,
            }
        )
    result.contig_rows = len(contig_rows)

    run_dir = out_dir / run_id.replace(":", "_")
    run_dir.mkdir(parents=True, exist_ok=True)
    features_path = run_dir / "features.parquet"
    contigs_path = run_dir / "contigs.parquet"
    pq.write_table(pa.Table.from_pylist(rows, schema=_feature_schema()), features_path, compression="zstd")
    pq.write_table(pa.Table.from_pylist(contig_rows, schema=_contig_schema()), contigs_path, compression="zstd")
    result.outputs = [str(features_path), str(contigs_path)]
    return result


def cached_files(entry: Mapping[str, Any], cache_dir: Path) -> tuple[dict[str, Path], dict[str, str]]:
    """Local paths and URLs of one planned run's files, where `download_to_cache.py` put them."""
    from urllib.parse import urlparse

    paths: dict[str, Path] = {}
    urls: dict[str, str] = {}
    for data_object_type, data_object in entry["files"].items():
        url = data_object.get("url")
        if not url:
            continue
        path = cache_dir / urlparse(url).path.lstrip("/")
        if path.exists():
            paths[data_object_type] = path
            urls[data_object_type] = url
    return paths, urls


def plan_to_json(plan: RunPlan) -> dict[str, Any]:
    """The plan as JSON-ready data; `plan_from_json` reverses it."""
    return {
        "selected": plan.selected,
        "superseded": plan.superseded,
        "orphans": plan.orphans,
        "ambiguous": plan.ambiguous,
    }


def plan_from_json(data: Mapping[str, Any]) -> RunPlan:
    """Rebuild a plan written by `plan_to_json`."""
    return RunPlan(
        selected=dict(data["selected"]),
        superseded=dict(data["superseded"]),
        orphans=list(data["orphans"]),
        ambiguous=[tuple(x) for x in data["ambiguous"]],
    )
