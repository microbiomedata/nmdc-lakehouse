"""Run each selected source contract through Parquet, manifest, and target validation."""

from copy import deepcopy
from importlib.metadata import version
from unittest.mock import MagicMock

import pyarrow.parquet as pq
from linkml_runtime import SchemaView
from nmdc_lakehouse_schema.artifacts import flat_schema_resource

from nmdc_lakehouse import source_preflight
from nmdc_lakehouse.jobs import collection_to_parquet as collection_module
from nmdc_lakehouse.jobs.collection_to_parquet import REVIEWED_SCHEMA_COLLECTIONS, AllCollectionsToParquetJob
from nmdc_lakehouse.metrics import stamp_result, success_record, write_record
from nmdc_lakehouse.snapshot_manifest import build_manifest, write_manifest
from nmdc_lakehouse.target_validation import validate_target_snapshot


def test_selected_pair_roundtrips_current_source_fields_and_nested_substances(tmp_path, monkeypatch):
    """The same test runs in separately locked 11.23.0 and 11.24.0 environments."""
    installed = version("nmdc-schema")
    legacy = installed == "11.23.0"
    person_slot = "applies_to_person" if legacy else "applies_to_agent"
    person = {
        "type": "nmdc:PersonValue" if legacy else "nmdc:Person",
        "name": "Example Researcher",
        "orcid": "orcid:0000-0002-1825-0097",
    }
    if legacy:
        person["has_raw_value"] = "Example Researcher"
    credit = {"type": "prov:Association", person_slot: person, "applied_roles": ["Investigation"]}
    study = {
        "id": "example:study",
        "type": "nmdc:Study",
        "study_category": "research_study",
        "has_credit_associations": [credit],
    }
    generation = {
        "id": "example:generation",
        "type": "nmdc:MassSpectrometry",
        "analyte_category": "metabolome",
        "has_input": ["example:sample"],
        "associated_studies": ["example:study"],
    }
    if legacy:
        study["principal_investigator"] = person
        generation["principal_investigator"] = person
    else:
        generation["has_credit_associations"] = [credit]
    substance = {
        "type": "nmdc:PortionOfSubstance",
        "known_as": "water",
        "substance_role": "solvent",
        "volume": {"has_numeric_value": 5.5, "has_unit": "mL"},
    }
    phase = {"type": "nmdc:MobilePhaseSegment", "substances_used": [substance, deepcopy(substance)]}
    fixtures = {
        "study_set": study,
        "data_generation_set": generation,
        "biosample_set": {
            "id": "example:sample",
            "type": "nmdc:Biosample",
            "name": "Example sample",
            "associated_studies": ["example:study"],
            "host_diet": [{"type": "nmdc:TextValue", "has_raw_value": "fruit"}],
        },
        **{
            collection: {
                "id": f"example:{collection}",
                "type": concrete,
                "ordered_mobile_phases": [phase, {"type": "nmdc:MobilePhaseSegment"}, deepcopy(phase)],
            }
            for collection, concrete in [
                ("configuration_set", "nmdc:ChromatographyConfiguration"),
                ("material_processing_set", "nmdc:ChromatographicSeparationProcess"),
            ]
        },
    }
    original = deepcopy(fixtures)
    client = MagicMock()
    client.__enter__.return_value = client
    view = client.get_default_database.return_value.__getitem__.return_value
    view.find.return_value.limit.return_value = [{"schema_version": installed}]
    monkeypatch.setattr(source_preflight, "MongoClient", lambda *args, **kwargs: client)

    class FixtureSource:
        def __init__(self, _uri):
            pass

        def iter_records(self, collection, page_size):
            yield fixtures[collection]

        def estimated_count(self, _collection):
            return 1

    monkeypatch.setattr(collection_module, "MongoSource", FixtureSource)
    skipped = REVIEWED_SCHEMA_COLLECTIONS - fixtures.keys()
    result = AllCollectionsToParquetJob("mongodb://localhost/fixture", tmp_path, skip=skipped).run()
    stamp_result(
        result,
        output_root=tmp_path,
        started_at="2026-09-23T00:00:00+00:00",
        finished_at="2026-09-23T00:00:01+00:00",
        elapsed_seconds=1,
    )
    metrics = tmp_path / "etl-metrics.json"
    write_record(metrics, success_record(result, skipped_collections=tuple(sorted(skipped)), dry_run=False))
    manifest = build_manifest(tmp_path, metrics, "synthetic-source-pair")
    write_manifest(tmp_path, manifest)
    report = validate_target_snapshot(tmp_path, requested_mode="full")
    assert report.status == "success"
    assert report.invalid_rows == 0
    assert report.selected_rows == sum(count for _table, count in result.table_rows)
    assert report.target_schema_source_package_version == installed
    assert manifest.software.nmdc_schema_version == installed
    assert manifest.target_schema_versions == [f"{installed}+flat.1.3.0"]
    assert fixtures == original
    target = SchemaView(str(flat_schema_resource(installed)))
    for table, _count in result.table_rows:
        parquet = pq.read_table(tmp_path / f"{table}.parquet")
        target_class = parquet.schema.metadata[b"nmdc_lakehouse.target_class"].decode()
        assert set(parquet.column_names) == set(target.get_class(target_class).attributes)
        assert all(row["type"] for row in parquet.to_pylist() if "type" in row)
    biosample = pq.read_table(tmp_path / "biosample_set.parquet").to_pylist()[0]
    assert biosample["host_diet"] == ["fruit"]
    assert not (tmp_path / "biosample_set_host_diet.parquet").exists()
    credits = pq.read_table(tmp_path / "study_set_has_credit_associations.parquet").to_pylist()
    assert credits[0][f"{person_slot}_name"] == person["name"]
    assert credits[0][f"{person_slot}_orcid"] == person["orcid"]
    for collection in ("study_set", "data_generation_set"):
        row = pq.read_table(tmp_path / f"{collection}.parquet").to_pylist()[0]
        if legacy:
            assert row["principal_investigator_name"] == person["name"]
            assert row["principal_investigator_has_raw_value"] == person["has_raw_value"]
        else:
            assert "principal_investigator_name" not in row
    for collection in ("configuration_set", "material_processing_set"):
        substances = pq.read_table(tmp_path / f"{collection}_ordered_mobile_phases_substances_used.parquet").to_pylist()
        assert [(row["mobile_phase_index"], row["substance_index"]) for row in substances] == [
            (0, 0),
            (0, 1),
            (2, 0),
            (2, 1),
        ]
        assert all(row["volume_has_numeric_value"] == 5.5 for row in substances)
