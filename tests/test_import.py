"""Smoke tests: the package and its subpackages import cleanly."""


def test_package_import():
    import nmdc_lakehouse

    assert nmdc_lakehouse.__version__


def test_subpackages_import():
    import nmdc_lakehouse.cli  # noqa: F401
    import nmdc_lakehouse.config  # noqa: F401
    import nmdc_lakehouse.io  # noqa: F401
    import nmdc_lakehouse.jobs  # noqa: F401
    import nmdc_lakehouse.sinks  # noqa: F401
    import nmdc_lakehouse.sources  # noqa: F401
    import nmdc_lakehouse.transforms  # noqa: F401
