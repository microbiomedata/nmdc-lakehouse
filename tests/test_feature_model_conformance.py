"""feature_convert's Parquet validates against the pinned BER feature model release.

The model and its validator live in https://github.com/turbomam/feature-table-corpus and are pinned
in pyproject.toml. A model change that NMDC output no longer meets fails here when the pin moves.
"""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import pytest
from ber_feature_model import validate

from nmdc_lakehouse import feature_convert as fc
from tests.feature_files import RUN

#: Columns feature_convert writes that the model has no slot for.
NOT_IN_MODEL = {"features": {"source_data_object_type"}, "contigs": {"assembly_contig_id"}}


def _dataset(run_files: dict[str, Path], out: Path, *, include_unselected: bool) -> dict[str, Any]:
    """Convert one run and read its Parquet back as a model Dataset; a null or empty cell is an absent slot."""
    urls = {t: f"https://example.org/{p.name}" for t, p in run_files.items()}
    result = fc.convert_run(
        RUN, run_files, urls, out, include_unselected=include_unselected, assembly_run="nmdc:wfmgas-99-a.1"
    )
    dataset: dict[str, Any] = {}
    for key, path in zip(("features", "contigs"), result.outputs, strict=True):
        dataset[key] = [
            {k: v for k, v in row.items() if k not in NOT_IN_MODEL[key] and v is not None and v != []}
            for row in pq.read_table(path).to_pylist()
        ]
    return dataset


@pytest.mark.parametrize("include_unselected", [False, True])
def test_converted_run_is_a_valid_dataset(run_files: dict[str, Path], tmp_path: Path, include_unselected: bool) -> None:
    dataset = _dataset(run_files, tmp_path / "out", include_unselected=include_unselected)
    assert {f["coordinate_system"] for f in dataset["features"]} == {"contig", "protein"}
    assert validate(dataset) == []


def test_validator_rejects_a_hit_placed_on_its_contig(run_files: dict[str, Path], tmp_path: Path) -> None:
    """Negative control: the check above can fail, on the rule NMDC output depends on most."""
    dataset = _dataset(run_files, tmp_path / "out", include_unselected=False)
    broken = copy.deepcopy(dataset)
    hit = next(f for f in broken["features"] if f["coordinate_system"] == "protein")
    cds = next(f for f in broken["features"] if f["feature_id"] == hit["seqid"])
    hit["seqid"] = cds["seqid"]
    assert any("protein coordinates need their CDS as seqid" in str(e) for e in validate(broken))


def test_listed_columns_outside_the_model_are_still_written(run_files: dict[str, Path], tmp_path: Path) -> None:
    """Keeps NOT_IN_MODEL from going stale. A new column the model lacks fails the validity test instead."""
    urls = {t: f"https://example.org/{p.name}" for t, p in run_files.items()}
    result = fc.convert_run(RUN, run_files, urls, tmp_path / "out")
    for key, path in zip(("features", "contigs"), result.outputs, strict=True):
        assert NOT_IN_MODEL[key] <= set(pq.read_schema(path).names)
