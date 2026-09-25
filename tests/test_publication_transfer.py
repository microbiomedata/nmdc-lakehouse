import base64
import hashlib
import importlib.util
import io
import json
import shlex
import subprocess
import threading
import zipfile
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote

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


@pytest.fixture
def jupyter(monkeypatch):
    calls = []
    token = "test-token-never-print"

    class Handler(BaseHTTPRequestHandler):
        def do_PUT(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            calls.append((self.path, self.headers.get("Authorization"), body))
            self.send_response(server.reply_status)
            self.send_header("Content-Type", server.reply_type)
            self.send_header("Location", f"http://127.0.0.1:{server.server_port}/redirect-must-not-receive-token")
            self.end_headers()
            name = unquote(self.path.rsplit("/", 1)[-1])
            self.wfile.write(json.dumps({"type": "file", "name": name, "path": name, "message": token}).encode())

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.reply_status = 201
    server.reply_type = "application/json"
    monkeypatch.setenv("JUPYTERHUB_URL", f"http://127.0.0.1:{server.server_port}/prefix")
    monkeypatch.setenv("JUPYTERHUB_USER", "test user")
    monkeypatch.setenv("JUPYTERHUB_API_TOKEN", token)
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
    thread.start()
    try:
        yield server, calls
    finally:
        server.shutdown()
        thread.join(timeout=2)
        server.server_close()


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


def test_send_contents_api_bytes_authentication_and_inventory_last(prepared, tmp_path, jupyter):
    inventory = transfer.pack(prepared, tmp_path / "transfer")
    _, calls = jupyter
    transfer.send(inventory.parent, transfer.digest(inventory))
    assert calls[-1][0].endswith("/" + inventory.name)
    for path, authorization, body in calls:
        assert path.startswith("/prefix/user/test%20user/api/contents/")
        assert authorization == "token test-token-never-print"
        assert body["type"] == "file" and body["format"] == "base64"
        assert base64.b64decode(body["content"]) == (inventory.parent / unquote(path.rsplit("/", 1)[-1])).read_bytes()


@pytest.mark.parametrize("status", [401, 403])
def test_send_auth_failure_stops_before_parts_and_redacts_token(prepared, tmp_path, jupyter, capsys, status):
    inventory = transfer.pack(prepared, tmp_path / "transfer")
    server, calls = jupyter
    server.reply_status = status
    assert transfer.main(["send", str(inventory), "--sha256", transfer.digest(inventory)]) == 1
    captured = capsys.readouterr()
    assert "this can be temporary" in captured.err
    assert "Check the Hub token page" in captured.err
    assert f"HTTP {status}" in captured.err
    assert "test-token-never-print" not in captured.err + captured.out
    assert len(calls) == 1 and calls[0][0].endswith(".py")
    assert "NEW_PUBLICATION_DIRECTORY" not in captured.out


def test_send_refuses_redirect_without_forwarding_credentials(prepared, tmp_path, jupyter):
    inventory = transfer.pack(prepared, tmp_path / "transfer")
    server, calls = jupyter
    server.reply_status = 307
    with pytest.raises(ValueError, match="redirects are refused"):
        transfer.send(inventory, transfer.digest(inventory))
    assert len(calls) == 1 and not any("redirect-must" in c[0] for c in calls)


def test_send_refuses_html_success_response(prepared, tmp_path, jupyter):
    inventory = transfer.pack(prepared, tmp_path / "transfer")
    server, calls = jupyter
    server.reply_status, server.reply_type = 200, "text/html"
    with pytest.raises(ValueError, match="unexpected content"):
        transfer.send(inventory, transfer.digest(inventory))
    assert len(calls) == 1


@pytest.mark.parametrize(
    "variable,value",
    [
        ("JUPYTERHUB_API_TOKEN", ""),
        ("JUPYTERHUB_API_TOKEN", "secret-token\r\nInjected: header"),
        ("JUPYTERHUB_URL", "https://secret-token@hub.example"),
        ("JUPYTERHUB_URL", "http://hub.example"),
    ],
)
def test_send_bad_credentials_or_insecure_destination_never_uploads(
    prepared, tmp_path, monkeypatch, capsys, variable, value
):
    inventory = transfer.pack(prepared, tmp_path / "transfer")
    monkeypatch.setenv("JUPYTERHUB_URL", "https://hub.example")
    monkeypatch.setenv("JUPYTERHUB_USER", "test-user")
    monkeypatch.setenv("JUPYTERHUB_API_TOKEN", "secret-token")
    monkeypatch.setenv(variable, value)
    monkeypatch.setattr(transfer, "upload", lambda *a: pytest.fail("Must not upload"))
    assert transfer.main(["send", str(inventory), "--sha256", transfer.digest(inventory)]) == 1
    captured = capsys.readouterr()
    assert "secret-token" not in captured.err + captured.out


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


def test_printed_command_verifies_script_before_executing_it(prepared, tmp_path, jupyter, capsys):
    inventory = transfer.pack(prepared, tmp_path / "transfer")
    send_command = capsys.readouterr().out.splitlines()[-1].removeprefix("Send: ")
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
    monkeypatch.setattr(transfer, "upload", lambda *a, **kw: calls.append(a))
    with pytest.raises(ValueError, match="sender checksum"):
        transfer.send(inventory, packed_checksum)
    assert calls == []


def test_send_refuses_inventory_changed_during_upload(prepared, tmp_path, monkeypatch, capsys, jupyter):
    inventory = transfer.pack(prepared, tmp_path / "transfer")
    packed_checksum = transfer.digest(inventory)
    calls = []

    def change_inventory(file, *args):
        calls.append(file)
        inventory.write_text("{}")

    monkeypatch.setattr(transfer, "upload", change_inventory)
    with pytest.raises(ValueError, match="changed during sending"):
        transfer.send(inventory, packed_checksum)
    assert inventory not in calls
    assert "NEW_PUBLICATION_DIRECTORY" not in capsys.readouterr().out


@pytest.mark.parametrize("action", ["send", "receive"])
def test_inventory_symlink_parent_refused_before_transfer_or_output(prepared, tmp_path, monkeypatch, action):
    inventory = transfer.pack(prepared, tmp_path / "transfer")
    checksum = transfer.digest(inventory)
    alias = tmp_path / "linked-transfer"
    alias.symlink_to(inventory.parent, target_is_directory=True)
    calls = []
    monkeypatch.setattr(transfer, "upload", lambda *a, **kw: calls.append(a))
    with pytest.raises(ValueError, match="symlinks"):
        if action == "send":
            transfer.send(alias / inventory.name, checksum)
        else:
            transfer.receive(alias / inventory.name, tmp_path / "received", checksum)
    assert not calls and not (tmp_path / "received").exists()
