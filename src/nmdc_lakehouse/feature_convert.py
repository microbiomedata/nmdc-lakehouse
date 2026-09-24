"""Convert one NMDC annotation run's feature files to Parquet in the BER feature model's shape.

Which files are read, and which are skipped as repeats, follows the checks in
`nmdc_lakehouse.feature_tables`: genome features come from the Functional Annotation GFF, hits
from the per-system hit GFFs, and unselected caller rows only on request. Nothing here writes to
BERDL.
"""

from __future__ import annotations

import re
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from nmdc_lakehouse.feature_tables import (
    CALLER_TYPES,
    CONTIG_MAPPING,
    FUNCTIONAL,
    HIT_TYPES,
    KO_EC,
    SCAFFOLD_LINEAGE,
    _first,
    check_run,
    parse_attributes,
    read_table,
)

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


#: Written into each run directory, so a later conversion only replaces its own run's output.
RUN_ID_FILE = "run_id.txt"

_RUN_DIR_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")


def _safe_run_dir_name(run_id: str) -> str:
    """The run's output directory name, refused unless it is one plain path component.

    The run ID comes from a plan and runs file, and the converter deletes and replaces this
    directory, so `/`, `..` or an absolute path must never reach it.
    """
    name = run_id.replace(":", "_")
    if not _RUN_DIR_NAME.fullmatch(name) or name in {".", ".."}:
        raise ValueError(f"{run_id!r} is not a usable run ID for an output directory name")
    return name


class DuplicateFeatureIdError(ValueError):
    """Two rows of one run would share a feature_id, which the model requires to be unique."""


@dataclass
class ConversionResult:
    """What `convert_run` wrote for one run, and what it left out."""

    run_id: str
    feature_rows: Counter[str] = field(default_factory=Counter)
    contig_rows: int = 0
    dropped_keys: list[str] = field(default_factory=list)
    duplicate_feature_ids: int = 0
    renamed_duplicate_ids: int = 0
    #: Hits on genes absent from the Functional Annotation GFF, by file type; not written.
    orphan_hits: Counter[str] = field(default_factory=Counter)
    #: Hits on an ID two CDS rows share, so their gene is ambiguous, by file type; not written.
    ambiguous_parent_hits: Counter[str] = field(default_factory=Counter)
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
    assembly_run: str | None = None,
    batch_rows: int = 200_000,
) -> ConversionResult:
    """Write `features.parquet` and `contigs.parquet` for one run under `out_dir/<run id>/`.

    Genome features come from the Functional Annotation GFF with `coordinate_system` `contig`.
    Hits come from each per-system GFF with `coordinate_system` `protein`, `parent` set to the
    gene they sit on, and `seqid` set to that gene's contig. That borrows the protein-relative
    coordinate and parent mapping of the corpus profile `nmdc-pfam-protein/1.0.0`; it does not
    implement that profile, which covers Pfam only and needs CDS translations and bindings.
    A hit's `feature_id` joins its GFF `ID`, its file type key and its
    column 3, because one gene can carry the same coordinates in several systems; the original
    `ID` stays in `attributes`.

    A contig's `generated_by` is `assembly_run`, the workflow that made the contig, and stays
    null when that is unknown rather than naming the annotation run.

    `assembly_contig_id` and `source_data_object_type` are not slots of the model yet.

    Rows are written in batches of `batch_rows` into `<out_dir>/.partial/<run dir>` and renamed into place
    only when the run is complete, so memory stays bounded and no half-written run is published.

    Raises DuplicateFeatureIdError if two rows would share a `feature_id`; nothing is published
    for that run.
    """
    import shutil

    import pyarrow as pa
    import pyarrow.parquet as pq

    checks = checks if checks is not None else check_run(files)
    result = ConversionResult(run_id=run_id)
    run_dir = out_dir / _safe_run_dir_name(run_id)
    # Staged under `.partial/`, a name `_safe_run_dir_name` cannot produce, so staging never
    # touches another run's directory.
    partial = out_dir / ".partial" / run_dir.name
    shutil.rmtree(partial, ignore_errors=True)
    partial.mkdir(parents=True)
    schema = _feature_schema()
    writer = pq.ParquetWriter(partial / "features.parquet", schema, compression="zstd")
    buffer: list[dict[str, Any]] = []
    seen_feature_ids: set[str] = set()
    repeated: list[str] = []
    contig_ids: set[str] = set()

    def emit(row: dict[str, Any]) -> None:
        if row["feature_id"] in seen_feature_ids:
            repeated.append(row["feature_id"])
        seen_feature_ids.add(row["feature_id"])
        contig_ids.add(row["seqid"])
        buffer.append(row)
        if len(buffer) >= batch_rows:
            writer.write_table(pa.Table.from_pylist(buffer, schema=schema))
            buffer.clear()

    drop: set[str] = set()
    for key, hit_type in DERIVABLE_KEYS.items():
        label = f"hits_match_functional:{'ko_ec' if hit_type == KO_EC else key}"
        if hit_type in files and checks.get(label, {}).get("passed"):
            drop.add(key)
    result.dropped_keys = sorted(drop)

    gene_seqid: dict[str, str] = {}
    # A call is its caller (column 2) and its location, so another caller's call at a selected
    # interval is kept as unselected rather than mistaken for the selected one.
    # Each selected row's score, phase and attributes, so only the one caller row it repeats is
    # skipped; kept only when unselected calls are wanted.
    selected_calls: dict[tuple[str, ...], list[tuple[str, str, frozenset[tuple[str, str]]]]] = {}
    functional_url = urls.get(FUNCTIONAL)
    # Observed 2026-09-23 in v1.0.2 and v1.0.4 runs: an RFAM hit over one interval on both strands
    # gets one `ID` twice, because the ID encodes the interval but not the strand. Those rows get
    # the strand appended, and a counter if that is still not enough.
    id_counts: Counter[str | None] = Counter()
    cds_counts: Counter[str | None] = Counter()
    for r in read_table(files[FUNCTIONAL]):
        source = _first(parse_attributes(r[8]) if len(r) > 8 else [], "ID")
        id_counts[source] += 1
        cds_counts[source] += r[2] == "CDS"
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
        if source_id:
            # Hits name their gene by the source ID, which a renamed row no longer carries.
            gene_seqid.setdefault(source_id, r[0])
        if include_unselected:
            selected_calls.setdefault((r[0], r[1], r[2], r[3], r[4], r[6]), []).append((r[5], r[7], frozenset(pairs)))
        emit(
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
                    # Hits on an ID two CDS rows share are not written, so those rows keep their
                    # accession keys rather than lose the evidence both ways.
                    and (k not in drop or cds_counts[source_id] > 1)
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
            if r[0] not in gene_seqid:
                # The model needs a hit's parent to be a Feature and its seqid a Contig; a hit on a
                # gene the Functional Annotation GFF lacks has neither, so it is counted, not written.
                result.orphan_hits[hit_type] += 1
                continue
            if len(cds_renamed.get(r[0], [])) > 1:
                # Two CDS rows share this ID (opposite strands), so the hit's gene is ambiguous.
                result.ambiguous_parent_hits[hit_type] += 1
                continue
            pairs = parse_attributes(r[8]) if len(r) > 8 else []
            source_id = _first(pairs, "ID") or f"{r[0]}_{r[3]}_{r[4]}"
            emit(
                {
                    "feature_id": f"{source_id}|{label}|{r[2]}",
                    "seqid": gene_seqid[r[0]],
                    "source": r[1] or None,
                    "type": r[2],
                    "start": int(r[3]),
                    "end": int(r[4]),
                    "coordinate_system": "protein",
                    "score": _number(r[5], float),
                    "strand": r[6],
                    "phase": _number(r[7], int),
                    "parent": cds_renamed.get(r[0], [r[0]]),
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
                pairs = parse_attributes(r[8]) if len(r) > 8 else []
                # Skip the caller row only if an unclaimed selected row repeats it in full, the
                # same test `check_run` applies; a same-call row that differs is unselected.
                waiting = selected_calls.get((r[0], r[1], r[2], r[3], r[4], r[6]), [])
                match = next(
                    (
                        i
                        for i, (score, phase, kept) in enumerate(waiting)
                        if (score, phase) == (r[5], r[7]) and set(pairs) <= kept
                    ),
                    None,
                )
                if match is not None:
                    waiting.pop(match)
                    continue
                source_id = _first(pairs, "ID") or f"{r[0]}_{r[3]}_{r[4]}"
                emit(
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

    if buffer:
        writer.write_table(pa.Table.from_pylist(buffer, schema=schema))
        buffer.clear()
    writer.close()
    result.duplicate_feature_ids = len(repeated)
    if repeated:
        shutil.rmtree(partial, ignore_errors=True)
        examples = sorted(set(repeated))[:3]
        raise DuplicateFeatureIdError(f"{run_id}: {len(repeated)} repeated feature_id, e.g. {examples}")

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
    contig_schema = _contig_schema()
    contig_writer = pq.ParquetWriter(partial / "contigs.parquet", contig_schema, compression="zstd")
    sources = [urls[t] for t in (CONTIG_MAPPING, SCAFFOLD_LINEAGE) if t in urls and t in files]
    contig_batch: list[dict[str, Any]] = []
    for contig_id in sorted(contig_ids):
        tax, confidence = lineage.get(contig_id, ([], None))
        contig_batch.append(
            {
                "contig_id": contig_id,
                "assembly_contig_id": assembly.get(contig_id),
                "taxonomic_lineage": tax,
                "lineage_confidence": confidence,
                "generated_by": assembly_run,
                "source_files": sources,
            }
        )
        result.contig_rows += 1
        if len(contig_batch) >= batch_rows:
            contig_writer.write_table(pa.Table.from_pylist(contig_batch, schema=contig_schema))
            contig_batch.clear()
    # Written even when empty, so a run with no contigs still has a readable contigs.parquet.
    contig_writer.write_table(pa.Table.from_pylist(contig_batch, schema=contig_schema))
    contig_writer.close()
    (partial / RUN_ID_FILE).write_text(run_id + "\n")
    if run_dir.exists():
        # Two run IDs can map to one directory name (`nmdc:x` and `nmdc_x`). Only replace a
        # directory this same run wrote.
        previous = run_dir / RUN_ID_FILE
        if not previous.is_file() or previous.read_text().strip() != run_id:
            shutil.rmtree(partial, ignore_errors=True)
            raise ValueError(f"{run_dir} holds output for another run; refusing to replace it with {run_id}")
        shutil.rmtree(run_dir)
    partial.rename(run_dir)
    result.outputs = [str(run_dir / "features.parquet"), str(run_dir / "contigs.parquet")]
    return result
