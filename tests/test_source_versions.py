"""The source-* extras in pyproject.toml are the one list of supported nmdc-schema releases."""

from pathlib import Path

import pytest

from nmdc_lakehouse.source_versions import default_source_version, source_extras

ROOT = Path(__file__).resolve().parents[1]


def _pyproject(tmp_path: Path, extras: str) -> Path:
    path = tmp_path / "pyproject.toml"
    path.write_text(f"[project]\nname = 'x'\n\n[project.optional-dependencies]\ndev = ['pytest']\n{extras}")
    return path


def test_checked_in_extras_pin_one_release_each():
    supported = source_extras(ROOT / "pyproject.toml")
    assert supported
    for version, extra in supported.items():
        major, minor = version.split(".")[:2]
        assert extra == f"source-{major}-{minor}"


def test_release_candidate_is_supported_but_never_the_default(tmp_path):
    supported = source_extras(
        _pyproject(
            tmp_path,
            "source-11-24 = ['nmdc-schema==11.24.0']\n"
            "source-11-25 = ['nmdc-schema==11.25.0rc2']\n"
            "source-11-3 = ['nmdc-schema==11.3.0']\n",
        )
    )
    assert supported == {"11.24.0": "source-11-24", "11.25.0rc2": "source-11-25", "11.3.0": "source-11-3"}
    assert default_source_version(supported) == "11.24.0"


@pytest.mark.parametrize(
    "extras",
    [
        "source-11-24 = ['nmdc-schema==11.23.0']\n",
        "source-11-24 = ['nmdc-schema>=11.24.0']\n",
        "source-11-24 = ['nmdc-schema==11.24.0', 'pymongo']\n",
        "",
    ],
)
def test_malformed_source_extras_are_rejected(tmp_path, extras):
    with pytest.raises(ValueError):
        source_extras(_pyproject(tmp_path, extras))


def test_only_release_candidates_leave_no_default():
    with pytest.raises(ValueError, match="no final source release"):
        default_source_version({"11.25.0rc2": "source-11-25"})
