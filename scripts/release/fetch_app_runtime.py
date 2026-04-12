#!/usr/bin/env python3

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import ssl
import sys
import tarfile
import tempfile
import urllib.request
from pathlib import Path

try:
    import certifi
except ImportError:  # pragma: no cover - falls back to system cert store
    certifi = None


SUPPORTED_TARGETS = {
    "macos-aarch64",
    "macos-x86_64",
    "linux-x86_64",
    "windows-x86_64",
}


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def pins_path() -> Path:
    return repo_root() / "scripts" / "release" / "runtime_pins.json"


def runtime_dir() -> Path:
    return repo_root() / "crates" / "tine-app" / "resources" / "runtime"


def load_pins() -> dict:
    return json.loads(pins_path().read_text())


def python_pins() -> dict:
    return load_pins()["python"]


def download_ssl_context() -> ssl.SSLContext:
    if certifi is not None:
        return ssl.create_default_context(cafile=certifi.where())
    return ssl.create_default_context()


def download(url: str, destination: Path) -> None:
    with urllib.request.urlopen(url, context=download_ssl_context()) as response, destination.open(
        "wb"
    ) as handle:
        shutil.copyfileobj(response, handle)


def verify_sha256(path: Path, expected_sha256: str) -> None:
    actual = hashlib.sha256(path.read_bytes()).hexdigest()
    if actual != expected_sha256:
        raise RuntimeError(
            f"checksum mismatch for {path.name}: expected {expected_sha256}, got {actual}"
        )


def extract_tar_gz(archive_path: Path, destination: Path) -> None:
    with tarfile.open(archive_path, "r:gz") as archive:
        extract_kwargs = {"filter": "data"} if sys.version_info >= (3, 12) else {}
        archive.extractall(destination, **extract_kwargs)


def is_python_runtime_root(path: Path) -> bool:
    return (path / "bin" / "python3").is_file() or (path / "python.exe").is_file()


def locate_python_root(extracted_root: Path) -> Path:
    direct = extracted_root / "python"
    if is_python_runtime_root(direct):
        return direct

    matches = []
    for candidate in extracted_root.rglob("*"):
        if candidate.is_dir() and is_python_runtime_root(candidate):
            matches.append(candidate)

    if len(matches) != 1:
        raise RuntimeError(
            f"expected exactly one extracted python runtime under {extracted_root}, found {len(matches)}"
        )
    return matches[0]


def python_executable(path: Path) -> Path:
    if (path / "bin" / "python3").is_file():
        return path / "bin" / "python3"
    if (path / "python.exe").is_file():
        return path / "python.exe"
    raise RuntimeError(f"python executable missing from extracted runtime at {path}")


def sentinel_payload(target: str, artifact: dict, python_root: Path) -> dict:
    return {
        "target": target,
        "python_version": python_pins()["version"],
        "python_url": artifact["url"],
        "python_sha256": artifact["sha256"],
        "python_executable": python_executable(python_root).relative_to(runtime_dir()).as_posix(),
    }


def sentinel_path() -> Path:
    return runtime_dir() / ".fetched-runtime.json"


def has_matching_runtime(target: str, artifact: dict) -> bool:
    marker = sentinel_path()
    python_root = runtime_dir() / "python"
    if not marker.is_file() or not python_root.exists():
        return False

    try:
        current = json.loads(marker.read_text())
    except json.JSONDecodeError:
        return False

    expected = sentinel_payload(target, artifact, python_root)
    return current == expected and python_executable(python_root).is_file()


def stage_python_runtime(target: str, artifact: dict) -> None:
    output_root = runtime_dir()
    output_root.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(prefix="tine-app-runtime-") as tmpdir:
        temp_root = Path(tmpdir)
        archive_path = temp_root / "python-runtime.tar.gz"
        extract_root = temp_root / "extract"
        extract_root.mkdir()

        download(artifact["url"], archive_path)
        verify_sha256(archive_path, artifact["sha256"])
        extract_tar_gz(archive_path, extract_root)

        extracted_python_root = locate_python_root(extract_root)
        staged_python_root = temp_root / "python"
        shutil.move(str(extracted_python_root), staged_python_root)

        destination = output_root / "python"
        if destination.exists():
            shutil.rmtree(destination)
        shutil.move(str(staged_python_root), destination)

        sentinel_path().write_text(
            json.dumps(sentinel_payload(target, artifact, destination), indent=2) + "\n"
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Fetch the pinned standalone Python runtime for the Tine desktop app."
    )
    parser.add_argument(
        "--target",
        required=True,
        choices=sorted(SUPPORTED_TARGETS),
        help="Desktop runtime target to fetch.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-download and re-extract even if the pinned runtime is already present.",
    )
    args = parser.parse_args(argv)

    artifact = python_pins()["artifacts"][args.target]

    if not args.force and has_matching_runtime(args.target, artifact):
        print(f"desktop app runtime already present for {args.target}: {runtime_dir() / 'python'}")
        return 0

    stage_python_runtime(args.target, artifact)
    print(f"fetched desktop app runtime for {args.target}: {runtime_dir() / 'python'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
