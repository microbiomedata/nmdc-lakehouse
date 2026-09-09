"""Tests for producer-identity labels (package==version) written into Parquet footers."""

from importlib.metadata import version

from nmdc_lakehouse.producer_identity import (
    DIRECT_PACKAGE,
    FLATTENER_PACKAGE,
    LEGACY_DIRECT_IDS,
    LEGACY_FLATTENER_IDS,
    direct_mapping_id,
    flattener_mapping_id,
    is_direct_identity,
    is_flattener_identity,
)


def test_identities_are_package_and_version() -> None:
    assert flattener_mapping_id() == f"{FLATTENER_PACKAGE}=={version(FLATTENER_PACKAGE)}"
    assert direct_mapping_id() == f"{DIRECT_PACKAGE}=={version(DIRECT_PACKAGE)}"


def test_classification_by_package_not_version() -> None:
    """A snapshot written by any version of a package still classifies as that producer."""
    assert is_flattener_identity(flattener_mapping_id())
    assert is_flattener_identity(f"{FLATTENER_PACKAGE}==9.9.9")
    assert not is_flattener_identity(direct_mapping_id())

    assert is_direct_identity(direct_mapping_id())
    assert is_direct_identity(f"{DIRECT_PACKAGE}==9.9.9")
    assert not is_direct_identity(flattener_mapping_id())


def test_direct_package_name_is_not_a_prefix_false_positive() -> None:
    """`nmdc-lakehouse` is a prefix of `nmdc-lakehouse-schema`; the `==` delimiter must disambiguate."""
    assert not is_direct_identity(flattener_mapping_id())
    assert not is_flattener_identity(direct_mapping_id())


def test_legacy_import_path_identities_still_accepted() -> None:
    """Already-published snapshots carry the old import-path labels and must keep validating."""
    for legacy in LEGACY_FLATTENER_IDS:
        assert is_flattener_identity(legacy)
        assert not is_direct_identity(legacy)
    for legacy in LEGACY_DIRECT_IDS:
        assert is_direct_identity(legacy)
        assert not is_flattener_identity(legacy)
