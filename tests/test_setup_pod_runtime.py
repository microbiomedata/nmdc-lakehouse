import importlib.util
from pathlib import Path

import pytest

from nmdc_lakehouse.berdl_staging import _SUPPORTED_INGEST_REVISIONS

SCRIPT = Path(__file__).resolve().parents[1] / "scripts/python/setup_pod_runtime.py"
SPEC = importlib.util.spec_from_file_location("setup_pod_runtime", SCRIPT)
assert SPEC and SPEC.loader
setup_runtime = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(setup_runtime)

COMMIT = "f0cacb67188699545f6a67dfdfd74f1e029d7297"


@pytest.fixture
def pod(tmp_path, monkeypatch):
    """A fake clone plus recorders for every command the setup would run."""
    checkout = tmp_path / "runtime" / "nmdc-lakehouse"
    checkout.mkdir(parents=True)
    commands = []
    state = {"head": COMMIT, "status": "", "ingest_head": setup_runtime.INGEST_REVISION}

    def fake_git_output(path, *arguments):
        if arguments[:2] == ("rev-parse", "HEAD"):
            return (state["ingest_head"] if path.name == "data-lakehouse-ingest" else state["head"]) + "\n"
        if arguments[0] == "status":
            return state["status"]
        raise AssertionError(arguments)

    def fake_run(step, command, *, cwd, env=None):
        commands.append((step, command, env))

    monkeypatch.setattr(setup_runtime, "git_output", fake_git_output)
    monkeypatch.setattr(setup_runtime, "run", fake_run)
    monkeypatch.setattr(setup_runtime.sys, "version_info", (3, 13, 9))
    monkeypatch.setattr(setup_runtime.sys, "prefix", "/opt/conda")
    monkeypatch.setattr(setup_runtime.sys, "base_prefix", "/opt/conda")
    monkeypatch.setattr(setup_runtime.sys, "executable", "/opt/conda/bin/python3.13")
    return checkout, commands, state


def test_ingest_revision_matches_the_planner():
    assert setup_runtime.INGEST_REVISION in _SUPPORTED_INGEST_REVISIONS


def test_setup_runs_every_step_in_order(pod):
    checkout, commands, _ = pod
    ingest = setup_runtime.setup(checkout, COMMIT, "11.23.0")
    assert ingest == checkout.parent / "data-lakehouse-ingest"
    steps = [step for step, _, _ in commands]
    assert steps == [
        "Clone the official ingest code",
        f"Check out ingest revision {setup_runtime.INGEST_REVISION}",
        "Create the tool environment",
        f"Install uv {setup_runtime.UV_VERSION}",
        "Create the project environment with the pod's packages",
        "Install the locked dependencies for source 11.23.0",
        "Check the runtime imports",
    ]
    pip = commands[3][1]
    assert "--no-user" in pip and f"uv=={setup_runtime.UV_VERSION}" in pip
    sync_env = commands[5][2]
    assert sync_env["NMDC_SCHEMA_VERSION"] == "11.23.0"
    assert sync_env["PATH"].startswith(str(checkout / ".tools" / "bin"))


def test_project_environment_uses_the_base_interpreter_not_the_tool_venv(pod):
    checkout, commands, _ = pod
    setup_runtime.setup(checkout, COMMIT, "11.24.0")
    base = [c for s, c, _ in commands if s.startswith("Create the tool")][0]
    venv = [c for s, c, _ in commands if s.startswith("Create the project")][0]
    assert base[0] == "/opt/conda/bin/python3.13"
    assert venv[venv.index("--python") + 1] == "/opt/conda/bin/python3.13"
    assert "--system-site-packages" in venv


def test_refuses_to_run_inside_a_virtual_environment(pod, monkeypatch):
    checkout, commands, _ = pod
    monkeypatch.setattr(setup_runtime.sys, "prefix", "/home/user/.tools")
    with pytest.raises(setup_runtime.SetupError, match="virtual environment"):
        setup_runtime.setup(checkout, COMMIT, "11.23.0")
    assert commands == []


def test_refuses_other_python_versions(pod, monkeypatch):
    checkout, commands, _ = pod
    monkeypatch.setattr(setup_runtime.sys, "version_info", (3, 12, 1))
    with pytest.raises(setup_runtime.SetupError, match="3.13"):
        setup_runtime.setup(checkout, COMMIT, "11.23.0")
    assert commands == []


def test_refuses_a_checkout_at_another_commit(pod):
    checkout, commands, state = pod
    state["head"] = "0" * 40
    with pytest.raises(setup_runtime.SetupError, match="not the reviewed commit"):
        setup_runtime.setup(checkout, COMMIT, "11.23.0")
    assert commands == []


def test_refuses_a_checkout_with_local_changes(pod):
    checkout, commands, state = pod
    state["status"] = " M justfile\0"
    with pytest.raises(setup_runtime.SetupError, match="local changes"):
        setup_runtime.setup(checkout, COMMIT, "11.23.0")
    assert commands == []


@pytest.mark.parametrize("existing", [".tools", ".venv"])
def test_refuses_an_existing_environment(pod, existing):
    checkout, commands, _ = pod
    (checkout / existing).mkdir()
    with pytest.raises(setup_runtime.SetupError, match="new runtime directory"):
        setup_runtime.setup(checkout, COMMIT, "11.23.0")
    assert commands == []


def test_refuses_an_existing_ingest_checkout(pod):
    checkout, commands, _ = pod
    (checkout.parent / "data-lakehouse-ingest").mkdir()
    with pytest.raises(setup_runtime.SetupError, match="new runtime directory"):
        setup_runtime.setup(checkout, COMMIT, "11.23.0")
    assert commands == []


def test_refuses_an_ingest_checkout_at_another_revision(pod):
    checkout, _, state = pod
    state["ingest_head"] = "1" * 40
    with pytest.raises(setup_runtime.SetupError, match="approved revision"):
        setup_runtime.setup(checkout, COMMIT, "11.23.0")


def test_failed_step_is_named(monkeypatch, tmp_path):
    monkeypatch.setattr(setup_runtime.subprocess, "run", lambda *a, **k: type("R", (), {"returncode": 7})())
    with pytest.raises(setup_runtime.SetupError, match="Install uv .* failed with exit status 7"):
        setup_runtime.run(f"Install uv {setup_runtime.UV_VERSION}", ["false"], cwd=tmp_path)


def test_main_rejects_a_short_commit(capsys):
    with pytest.raises(SystemExit):
        setup_runtime.main(["--commit", "f0cacb6", "--source-version", "11.23.0"])
    assert "40-character" in capsys.readouterr().err


def test_main_reports_a_setup_error(monkeypatch, capsys):
    def fail(*_):
        raise setup_runtime.SetupError("example failure")

    monkeypatch.setattr(setup_runtime, "setup", fail)
    assert setup_runtime.main(["--commit", COMMIT, "--source-version", "11.23.0"]) == 1
    assert "Setup stopped: example failure" in capsys.readouterr().err
