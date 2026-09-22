"""Exercise the pinned schema package through the collection job and Parquet sink."""

from importlib import resources

import pyarrow.parquet as pq
import pytest
from linkml.validator import Validator
from linkml_runtime import SchemaView

from nmdc_lakehouse.jobs import collection_to_parquet as collection_module
from nmdc_lakehouse.jobs.collection_to_parquet import CollectionToParquetJob
from nmdc_lakehouse.producer_identity import flattener_mapping_id
from nmdc_lakehouse.target_validation import assert_source_schema_aligned


def _run_record(tmp_path, monkeypatch, collection, root, record):
    class Source:
        def __init__(self, _uri):
            pass

        def iter_records(self, _collection, page_size):
            del page_size
            yield record

        def estimated_count(self, _collection):
            return 1

    monkeypatch.setattr(collection_module, "MongoSource", Source)
    return CollectionToParquetJob(collection, root, "mongodb://localhost/nmdc", tmp_path).run()


@pytest.mark.parametrize(
    "collection,root,concrete,required",
    [
        ("study_set", "Study", "Study", {"study_category": "research_study"}),
        (
            "data_generation_set",
            "DataGeneration",
            "MassSpectrometry",
            {
                "analyte_category": "metabolome",
                "associated_studies": ["example:study"],
                "has_input": ["example:sample"],
            },
        ),
        (
            "data_generation_set",
            "DataGeneration",
            "NucleotideSequencing",
            {
                "analyte_category": "metagenome",
                "associated_studies": ["example:study"],
                "has_input": ["example:sample"],
            },
        ),
    ],
)
def test_agent_fields_and_writer_identity_survive_parquet(tmp_path, monkeypatch, collection, root, concrete, required):
    assert_source_schema_aligned()
    record = {
        "id": "example:record",
        "type": f"nmdc:{concrete}",
        **required,
        "has_credit_associations": [
            {
                "type": "nmdc:CreditAssociation",
                "applied_roles": ["Investigation"],
                "applies_to_agent": {
                    "type": "nmdc:Person",
                    "name": "Synthetic person",
                    "email": "person@example.org",
                    "orcid": "orcid:0000-0002-1825-0097",
                },
            },
            {
                "type": "nmdc:CreditAssociation",
                "applied_roles": ["Resources"],
                "applies_to_agent": {
                    "type": "nmdc:Organization",
                    "name": "Synthetic organization",
                    "ror": "ror:02jbv0t02",
                },
            },
        ],
    }
    _run_record(tmp_path, monkeypatch, collection, root, record)
    side_name = f"{collection}_has_credit_associations"
    child_rows = pq.read_table(tmp_path / f"{side_name}.parquet").to_pylist()
    assert len(child_rows) == 2
    assert child_rows[0]["applies_to_agent_email"] == "person@example.org"
    assert child_rows[0]["applies_to_agent_orcid"] == "orcid:0000-0002-1825-0097"
    assert child_rows[1]["applies_to_agent_ror"] == "ror:02jbv0t02"
    assert all(row["parent_id"] == "example:record" and row["type"] == "nmdc:CreditAssociation" for row in child_rows)
    target = SchemaView(str(resources.files("nmdc_lakehouse_schema").joinpath("schema/nmdc_schema_flattened.yaml")))
    validator = Validator(target.schema)
    for table, target_class in ((collection, f"{root}Flat"), (side_name, side_name)):
        parquet = pq.read_table(tmp_path / f"{table}.parquet")
        metadata = parquet.schema.metadata
        assert metadata[b"nmdc_lakehouse.mapping"].decode() == flattener_mapping_id()
        assert metadata[b"nmdc_lakehouse.source_schema_version"].decode() == "11.24.0"
        assert metadata[b"nmdc_lakehouse.target_schema_version"].decode() == "11.24.0+flat.1.2.0"
        for row in parquet.to_pylist():
            populated = {key: value for key, value in row.items() if value is not None}
            assert not list(validator.iter_results(populated, target_class=target_class))
    assert pq.read_table(tmp_path / f"{collection}.parquet").to_pylist()[0]["type"] == record["type"]


def test_textvalue_arrays_stay_on_parent_and_reject_extra_content(tmp_path, monkeypatch):
    record = {
        "id": "example:sample",
        "type": "nmdc:Biosample",
        "name": "Synthetic sample",
        "associated_studies": ["example:study"],
        "host_diet": [{"has_raw_value": value} for value in ["one", "one", "", None]],
    }
    _run_record(tmp_path, monkeypatch, "biosample_set", "Biosample", record)
    path = tmp_path / "biosample_set.parquet"
    assert pq.read_table(path).to_pylist()[0]["host_diet"] == ["one", "one", "", None]
    assert not (tmp_path / "biosample_set_host_diet.parquet").exists()
    previous = path.read_bytes()
    record["host_diet"][0]["language"] = "en"
    with pytest.raises(ValueError, match="discard populated content"):
        _run_record(tmp_path, monkeypatch, "biosample_set", "Biosample", record)
    assert path.read_bytes() == previous
