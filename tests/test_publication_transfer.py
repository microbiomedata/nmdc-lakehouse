import hashlib
import importlib.util
import io
import json
import shlex
import subprocess
import zipfile
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts/python/publication_transfer.py"
SPEC = importlib.util.spec_from_file_location("publication_transfer", SCRIPT)
assert SPEC and SPEC.loader
transfer = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(transfer)


def sha(value):
    return hashlib.sha256(value).hexdigest()


@pytest.fixture
def prepared(tmp_path):
    root = tmp_path / "prepared"
    (root / "snapshot").mkdir(parents=True)
    (root / "evidence").mkdir()
    (root / "snapshot/table.parquet").write_bytes(b"parquet-fixture")
    (root / "snapshot/etl-metrics.json").write_text("{}")
    manifest = {
        "snapshot_id": "sha256:test",
        "performance_record": {"path": "etl-metrics.json", "sha256": sha(b"{}")},
        "artifacts": [{"path": "table.parquet", "sha256": sha(b"parquet-fixture")}],
    }
    (root / "snapshot/snapshot-manifest.json").write_text(json.dumps(manifest))
    for name in transfer.EVIDENCE:
        (root / "evidence" / name).write_text("{}")
    (root / "preparation.json").write_text(
        json.dumps(
            {
                "status": "prepared",
                "snapshot_id": "sha256:test",
                "evidence": dict.fromkeys(transfer.EVIDENCE, sha(b"{}")),
            }
        )
    )
    return root


def test_pack_receive_exact_bytes_excludes_private_and_runtime_files(prepared, tmp_path):
    for name in [".env", "export.log", "evidence/inventory.log", "evidence/berdl-staging-plan.json"]:
        (prepared / name).write_text("private or runtime-bound")
    inventory = transfer.pack(prepared, tmp_path / "transfer", part_bytes=100)
    data = json.loads(inventory.read_text())
    assert len(data["parts"]) > 1
    assert not any("private" in str(v) for v in data.values())
    received = tmp_path / "received"
    transfer.receive(inventory, received, transfer.digest(inventory))
    assert set(p.relative_to(received).as_posix() for p in received.rglob("*") if p.is_file()) == set(data["files"])
    for name in data["files"]:
        assert (received / name).read_bytes() == (prepared / name).read_bytes()


@pytest.mark.parametrize("kind", ["corrupt", "missing", "duplicate", "archive_hash", "extra_member", "traversal"])
def test_receive_refuses_bad_transfer_before_creating_output(prepared, tmp_path, kind):
    inventory = transfer.pack(prepared, tmp_path / "transfer")
    data = json.loads(inventory.read_text())
    part = inventory.parent / data["parts"][0]["name"]
    if kind == "corrupt":
        part.write_bytes(b"changed")
    elif kind == "missing":
        part.unlink()
    elif kind == "duplicate":
        data["parts"].append(data["parts"][0])
    elif kind == "archive_hash":
        data["archive_sha256"] = "0" * 64
    elif kind == "extra_member":
        stream = io.BytesIO(part.read_bytes())
        with zipfile.ZipFile(stream, "a") as archive:
            archive.writestr("unexpected", "not selected")
        part.write_bytes(stream.getvalue())
        data["parts"][0].update(sha256=transfer.digest(part), bytes=part.stat().st_size)
        data["archive_sha256"] = transfer.digest(part)
    else:
        data["files"]["snapshot/../../outside"] = "0" * 64
    inventory.write_text(json.dumps(data))
    with pytest.raises((ValueError, OSError)):
        transfer.receive(inventory, tmp_path / "received", transfer.digest(inventory))
    assert not (tmp_path / "received").exists()


def test_refuses_changed_input_and_preserves_existing_output(prepared, tmp_path):
    (prepared / "snapshot/table.parquet").write_bytes(b"changed")
    with pytest.raises(ValueError, match="changed"):
        transfer.pack(prepared, tmp_path / "transfer")
    assert not (tmp_path / "transfer").exists()
    assert transfer.main(["receive", "absent.json", str(prepared), "--sha256", "0" * 64]) == 1
    assert (prepared / "preparation.json").exists()


def test_refuses_symlinks_and_overlapping_output(prepared, tmp_path):
    link = tmp_path / "linked"
    link.symlink_to(prepared, target_is_directory=True)
    with pytest.raises(ValueError, match="symlink"):
        transfer.pack(link, tmp_path / "transfer")
    with pytest.raises(ValueError, match="disjoint"):
        transfer.pack(prepared, prepared / "transfer")


def test_send_uses_existing_labctl_and_stops_on_failure(prepared, tmp_path, monkeypatch):
    inventory = transfer.pack(prepared, tmp_path / "transfer")
    calls = []

    def run(command, *, check):
        calls.append(command)
        assert check and command[:3] == ["labctl", "pod", "put"]

    monkeypatch.setattr(transfer.subprocess, "run", run)
    transfer.send(inventory.parent, transfer.digest(inventory))
    assert calls[-1][-1] == inventory.name
    assert all(Path(c[-2]).name == c[-1] for c in calls)

    def fail(command, *, check):
        raise subprocess.CalledProcessError(1, command)

    monkeypatch.setattr(transfer.subprocess, "run", fail)
    assert transfer.main(["send", str(inventory), "--sha256", transfer.digest(inventory)]) == 1


def test_cli_pack_receive_and_no_extra_dependencies(prepared, tmp_path):
    assert transfer.main(["pack", str(prepared), str(tmp_path / "transfer")]) == 0
    inventory = next((tmp_path / "transfer").glob("*.json"))
    assert (
        transfer.main(["receive", str(inventory), str(tmp_path / "received"), "--sha256", transfer.digest(inventory)])
        == 0
    )
    result = subprocess.run(["python3", "-S", str(SCRIPT), "--help"], capture_output=True, text=True, check=True)
    assert "pack" in result.stdout and "send" in result.stdout and "receive" in result.stdout


def test_inventory_checksum_and_incomplete_send_refused(prepared, tmp_path):
    inventory = transfer.pack(prepared, tmp_path / "transfer")
    with pytest.raises(ValueError, match="sender checksum"):
        transfer.receive(inventory, tmp_path / "received", "0" * 64)
    assert not (tmp_path / "received").exists()
    inventory.unlink()
    with pytest.raises(ValueError, match="exactly one"):
        transfer.send(inventory.parent, "0" * 64)


def test_receive_rejects_changed_helper_before_creating_output(prepared, tmp_path):
    inventory = transfer.pack(prepared, tmp_path / "transfer")
    data = json.loads(inventory.read_text())
    (inventory.parent / data["script"]["name"]).write_text("print('changed but valid Python')\n")
    with pytest.raises(ValueError, match="reviewed helper"):
        transfer.receive(inventory, tmp_path / "received", transfer.digest(inventory))
    assert not (tmp_path / "received").exists()


def test_printed_command_verifies_script_before_executing_it(prepared, tmp_path, monkeypatch, capsys):
    inventory = transfer.pack(prepared, tmp_path / "transfer")
    send_command = capsys.readouterr().out.splitlines()[-1].removeprefix("Send: ")
    with monkeypatch.context() as patch:
        patch.setattr(transfer.subprocess, "run", lambda *a, **kw: None)
        assert transfer.main(shlex.split(send_command)[2:]) == 0
    command = (
        capsys.readouterr()
        .out.splitlines()[-1]
        .replace("NEW_PUBLICATION_DIRECTORY", shlex.quote(str(tmp_path / "received from printed command")))
    )
    completed = subprocess.run(command, shell=True, cwd=inventory.parent, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stderr
    assert (tmp_path / "received from printed command/preparation.json").is_file()
    data = json.loads(inventory.read_text())
    (inventory.parent / data["script"]["name"]).write_text(
        "from pathlib import Path\nPath('unreviewed-helper-executed').touch()\n"
    )
    result = subprocess.run(command, shell=True, cwd=inventory.parent, capture_output=True)
    assert result.returncode != 0
    assert not (inventory.parent / "unreviewed-helper-executed").exists()


def test_send_rejects_consistently_changed_helper_and_inventory(prepared, tmp_path, monkeypatch):
    inventory = transfer.pack(prepared, tmp_path / "transfer")
    packed_checksum = transfer.digest(inventory)
    data = json.loads(inventory.read_text())
    helper = inventory.parent / data["script"]["name"]
    helper.write_text("print('unreviewed code')\n")
    data["script"]["sha256"] = transfer.digest(helper)
    inventory.write_text(json.dumps(data))
    calls = []
    monkeypatch.setattr(transfer.subprocess, "run", lambda *a, **kw: calls.append(a))
    with pytest.raises(ValueError, match="sender checksum"):
        transfer.send(inventory, packed_checksum)
    assert calls == []


def test_send_refuses_inventory_changed_during_upload(prepared, tmp_path, monkeypatch, capsys):
    inventory = transfer.pack(prepared, tmp_path / "transfer")
    packed_checksum = transfer.digest(inventory)
    calls = []

    def change_inventory(command, *, check):
        calls.append(command)
        inventory.write_text("{}")

    monkeypatch.setattr(transfer.subprocess, "run", change_inventory)
    with pytest.raises(ValueError, match="changed during sending"):
        transfer.send(inventory, packed_checksum)
    assert inventory.name not in [c[-1] for c in calls]
    assert "NEW_PUBLICATION_DIRECTORY" not in capsys.readouterr().out


@pytest.mark.parametrize("action", ["send", "receive"])
def test_inventory_symlink_parent_refused_before_transfer_or_output(prepared, tmp_path, monkeypatch, action):
    inventory = transfer.pack(prepared, tmp_path / "transfer")
    checksum = transfer.digest(inventory)
    alias = tmp_path / "linked-transfer"
    alias.symlink_to(inventory.parent, target_is_directory=True)
    calls = []
    monkeypatch.setattr(transfer.subprocess, "run", lambda *a, **kw: calls.append(a))
    with pytest.raises(ValueError, match="symlinks"):
        if action == "send":
            transfer.send(alias / inventory.name, checksum)
        else:
            transfer.receive(alias / inventory.name, tmp_path / "received", checksum)
    assert not calls and not (tmp_path / "received").exists()
