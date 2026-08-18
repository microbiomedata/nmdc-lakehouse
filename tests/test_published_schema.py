"""Tests for the canonical published NMDC metadata target schema."""

from importlib.metadata import version
from importlib.util import find_spec
from pathlib import Path

from linkml.validator import validate
from linkml_runtime import SchemaView

from nmdc_lakehouse.transforms.schema_generator import flatten_database_schema

PUBLISHED_SCHEMA = Path(__file__).parents[1] / "src/nmdc_lakehouse/schemas/nmdc_metadata.yaml"


def _installed_source_schema() -> SchemaView:
    spec = find_spec("nmdc_schema")
    assert spec is not None and spec.submodule_search_locations
    path = Path(spec.submodule_search_locations[0]) / "nmdc_materialized_patterns.yaml"
    return SchemaView(str(path))


def test_published_schema_matches_complete_installed_topology() -> None:
    source = _installed_source_schema()
    expected = flatten_database_schema(source, source_package_version=version("nmdc-schema"))
    published = SchemaView(str(PUBLISHED_SCHEMA))

    assert set(published.schema.classes) == set(expected.classes)
    assert published.schema.annotations["source_schema_id"].value == source.schema.id
    assert published.schema.annotations["source_schema_version"].value == source.schema.version
    assert published.schema.annotations["source_package_version"].value == version("nmdc-schema")


def test_published_schema_has_declared_ranges_and_unambiguous_identifiers() -> None:
    published = SchemaView(str(PUBLISHED_SCHEMA))
    declared_ranges = {
        *published.schema.classes,
        *published.schema.enums,
        *published.schema.types,
    }

    for class_name, class_def in published.schema.classes.items():
        identifiers = []
        for slot in class_def.attributes.values():
            assert slot.range in declared_ranges, f"{class_name}.{slot.name} has undeclared range {slot.range}"
            if slot.identifier:
                identifiers.append(slot.name)
        assert len(identifiers) <= 1, f"{class_name} has multiple identifiers: {identifiers}"


def test_published_schema_accepts_source_type_values() -> None:
    report = validate(
        {
            "id": "nmdc:sty-1",
            "study_category": "research_study",
            "type": "nmdc:Study",
        },
        str(PUBLISHED_SCHEMA),
        target_class="StudyFlat",
    )

    assert report.results == []
