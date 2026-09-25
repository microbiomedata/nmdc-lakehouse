"""Transfer a prepared publication with Python's standard library and existing labctl.

This transports immutable files. It neither validates LinkML rows nor changes a catalog.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
import uuid
import zipfile
from pathlib import Path, PurePosixPath
from typing import Any

EVIDENCE = {"metadata-profile.json", "metadata-bundle.json", "target-validation.json"}
CHUNK = 1024 * 1024


def digest(path: Path) -> str:
    """Hash an ordinary file without retaining its contents in memory."""
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"Expected an ordinary file: {path.name}")
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(CHUNK), b""):
            value.update(chunk)
    return value.hexdigest()


def relative(name: str) -> str:
    """Require a canonical relative POSIX file path."""
    path = PurePosixPath(name)
    if not name or path.is_absolute() or ".." in path.parts or str(path) != name or "\\" in name:
        raise ValueError("Unsafe transfer member path.")
    return name


def ordinary(root: Path, name: str) -> Path:
    """Refuse symlinks in both the file and its parent directories."""
    path = root / relative(name)
    if any(p.is_symlink() for p in [path, *path.parents]):
        raise ValueError("Transfer paths cannot contain symlinks.")
    return path


def new_directory(path: Path) -> Path:
    """Resolve a new output location without following a symlinked parent."""
    path = path.expanduser().absolute()
    if any(p.is_symlink() for p in [path, *path.parents]) or path.exists():
        raise ValueError("Use a new directory without symlinked parents.")
    return path.resolve()


def prepared_files(root: Path) -> tuple[str, dict[str, str]]:
    """Select only receipt-bound evidence and manifest-owned snapshot files."""
    receipt_path = ordinary(root, "preparation.json")
    manifest_path = ordinary(root, "snapshot/snapshot-manifest.json")
    receipt = json.loads(receipt_path.read_bytes())
    manifest = json.loads(manifest_path.read_bytes())
    if receipt.get("status") != "prepared" or set(receipt.get("evidence", {})) != EVIDENCE:
        raise ValueError("A complete preparation receipt is required.")
    if receipt["snapshot_id"] != manifest["snapshot_id"]:
        raise ValueError("Preparation and snapshot identities differ.")
    selected = {"preparation.json": digest(receipt_path), "snapshot/snapshot-manifest.json": digest(manifest_path)}
    for name, checksum in receipt["evidence"].items():
        selected[f"evidence/{name}"] = checksum
    for artifact in [manifest["performance_record"], *manifest["artifacts"]]:
        name = "snapshot/" + relative(artifact["path"])
        if name in selected:
            raise ValueError("Duplicate snapshot file.")
        selected[name] = artifact["sha256"]
    for name, checksum in selected.items():
        if digest(ordinary(root, name)) != checksum:
            raise ValueError(f"Prepared file changed: {name}")
    return receipt["snapshot_id"], selected


def pack(root: Path, output: Path, part_bytes: int = 64 * CHUNK) -> Path:
    """Create bounded parts and a completion inventory in a new private directory."""
    if part_bytes < 1:
        raise ValueError("Part size must be positive.")
    root, output = root.expanduser().absolute(), new_directory(output)
    ordinary(root, "preparation.json")
    root = root.resolve()
    if output.is_relative_to(root) or root.is_relative_to(output):
        raise ValueError("Preparation and transfer directories must be disjoint.")
    snapshot_id, selected = prepared_files(root)
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    prefix = "nmdc-transfer-" + uuid.uuid4().hex[:12]
    script = output / f"{prefix}.py"
    shutil.copyfile(__file__, script)
    parts: list[dict[str, Any]] = []
    archive_hash = hashlib.sha256()
    with tempfile.TemporaryFile(dir=output) as stream:
        with zipfile.ZipFile(stream, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=1) as archive:
            for name in sorted(selected):
                archive.write(ordinary(root, name), name)
        stream.seek(0)
        with zipfile.ZipFile(stream) as archive:
            for name, checksum in selected.items():
                value = hashlib.sha256()
                with archive.open(name) as member:
                    for chunk in iter(lambda: member.read(CHUNK), b""):
                        value.update(chunk)
                if value.hexdigest() != checksum:
                    raise ValueError("A source changed while its archive member was being written.")
        stream.seek(0)
        while chunk := stream.read(part_bytes):
            path = output / f"{prefix}.part-{len(parts):05d}"
            path.write_bytes(chunk)
            archive_hash.update(chunk)
            parts.append({"name": path.name, "bytes": len(chunk), "sha256": digest(path)})
    if prepared_files(root) != (snapshot_id, selected):
        raise ValueError("Preparation changed during packing; no transfer inventory was published.")
    inventory = {
        "format_version": 1,
        "snapshot_id": snapshot_id,
        "archive_sha256": archive_hash.hexdigest(),
        "files": selected,
        "parts": parts,
        "script": {"name": script.name, "sha256": digest(script)},
    }
    destination = output / f"{prefix}.json"
    destination.write_text(json.dumps(inventory, indent=2, sort_keys=True) + "\n")
    print(f"transfer={destination}\nsha256={digest(destination)}", flush=True)
    print("Send: " + shlex.join([sys.executable, __file__, "send", str(destination)]), flush=True)
    return destination


def load_inventory(path: Path) -> dict[str, Any]:
    """Validate transport paths before reading any part or creating output."""
    digest(path)
    data = json.loads(path.read_bytes())
    if data["format_version"] != 1 or not data["parts"] or not data["files"]:
        raise ValueError("Unsupported or incomplete transfer inventory.")
    names = [path.name, *[part["name"] for part in data["parts"]], data["script"]["name"]]
    if len(set(names)) != len(names) or any(relative(name) != PurePosixPath(name).name for name in names):
        raise ValueError("Transfer part names must be unique ordinary basenames.")
    for name in data["files"]:
        relative(name)
        if name != "preparation.json" and not name.startswith(("snapshot/", "evidence/")):
            raise ValueError("Unexpected publication member.")
    return data


def checked_parts(path: Path, data: dict[str, Any]):
    """Yield verified parts in their recorded order, never a shell wildcard order."""
    for part in data["parts"]:
        file = ordinary(path.parent, part["name"])
        if digest(file) != part["sha256"] or file.stat().st_size != part["bytes"]:
            raise ValueError(f"Transfer part differs: {file.name}")
        yield file


def send(path: Path) -> None:
    """Send checked files through the operator's configured labctl session."""
    path = path.expanduser().absolute()
    if path.is_dir() and not path.is_symlink():
        matches = list(path.glob("nmdc-transfer-*.json"))
        if len(matches) != 1:
            raise ValueError("The transfer directory must contain exactly one completed inventory.")
        path = matches[0]
    data = load_inventory(path)
    script = ordinary(path.parent, data["script"]["name"])
    if digest(script) != data["script"]["sha256"]:
        raise ValueError("Transfer script differs.")
    parts = list(checked_parts(path, data))
    for file in [script, *parts, path]:
        print(f"Sending {file.name}", flush=True)
        subprocess.run(["labctl", "pod", "put", str(file), file.name], check=True)
    print("In the pod home directory, choose a new output directory and run:", flush=True)
    print(
        shlex.join(
            ["python3", script.name, "receive", path.name, "NEW_PUBLICATION_DIRECTORY", "--sha256", digest(path)]
        ),
        flush=True,
    )


def receive(path: Path, output: Path, expected_digest: str) -> None:
    """Reassemble and verify before extracting regular files into a new directory."""
    path, output = path.expanduser().absolute(), new_directory(output)
    if digest(path) != expected_digest:
        raise ValueError("Transfer inventory differs from the reviewed sender checksum.")
    data = load_inventory(path)
    with tempfile.TemporaryFile(dir=path.parent) as stream:
        archive_hash = hashlib.sha256()
        for part in checked_parts(path, data):
            with part.open("rb") as source:
                for chunk in iter(lambda: source.read(CHUNK), b""):
                    archive_hash.update(chunk)
                    stream.write(chunk)
            print(f"Verified {part.name}", flush=True)
        if archive_hash.hexdigest() != data["archive_sha256"]:
            raise ValueError("Complete archive checksum differs.")
        stream.seek(0)
        with zipfile.ZipFile(stream) as archive:
            names = archive.namelist()
            if len(set(names)) != len(names) or set(names) != set(data["files"]):
                raise ValueError("Archive member set differs from the transfer inventory.")
            output.mkdir(mode=0o700, parents=True, exist_ok=False)
            for name in names:
                destination = ordinary(output, name)
                destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                with archive.open(name) as source, destination.open("xb") as target:
                    shutil.copyfileobj(source, target, CHUNK)
                if digest(destination) != data["files"][name]:
                    raise ValueError(f"Extracted file differs: {name}")
    if prepared_files(output) != (data["snapshot_id"], data["files"]):
        raise ValueError("Received publication has unexpected files or identity.")
    print(f"Received {data['snapshot_id']}; verified {len(data['files'])} files.", flush=True)
    print("Next: set up the matching pod runtime and run plan-publication. No lakehouse writes occurred.")


def main(argv: list[str] | None = None) -> int:
    """Expose the same dependency-free helper on workstation and pod."""
    parser = argparse.ArgumentParser(description=__doc__)
    actions = parser.add_subparsers(dest="action", required=True)
    packing = actions.add_parser("pack", help="Package one prepared publication; exclude logs and runtime evidence.")
    packing.add_argument("root", type=Path)
    packing.add_argument("output", type=Path)
    sending = actions.add_parser("send", help="Upload verified parts with the existing labctl; no catalog writes.")
    sending.add_argument("inventory", type=Path)
    receiving = actions.add_parser("receive", help="Verify and unpack into a new pod-local directory.")
    receiving.add_argument("inventory", type=Path)
    receiving.add_argument("output", type=Path)
    receiving.add_argument("--sha256", required=True, help="Inventory checksum printed by the reviewed client send.")
    args = parser.parse_args(argv)
    os.umask(0o077)
    try:
        if args.action == "pack":
            pack(args.root, args.output)
        elif args.action == "send":
            send(args.inventory)
        else:
            receive(args.inventory, args.output, args.sha256)
    except (OSError, ValueError, KeyError, TypeError, zipfile.BadZipFile, subprocess.CalledProcessError) as error:
        print(f"Transfer stopped ({type(error).__name__}): {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
