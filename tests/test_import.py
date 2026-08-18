"""Smoke tests: the package and its subpackages import cleanly."""

from importlib.metadata import version


def test_package_import():
    import nmdc_lakehouse

    assert nmdc_lakehouse.__version__ == version("nmdc-lakehouse")
    assert nmdc_lakehouse.__version__ != "0.0.0"


def test_subpackages_import():
    import nmdc_lakehouse.cli  # noqa: F401
    import nmdc_lakehouse.config  # noqa: F401
    import nmdc_lakehouse.io  # noqa: F401
    import nmdc_lakehouse.jobs  # noqa: F401
    import nmdc_lakehouse.sinks  # noqa: F401
    import nmdc_lakehouse.sources  # noqa: F401
    import nmdc_lakehouse.transforms  # noqa: F401
