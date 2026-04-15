from __future__ import annotations

import hashlib
import json
import os
import shutil
import ssl
import subprocess
import sys
import tarfile
import tempfile
import urllib.request
from pathlib import Path

try:
    import certifi
except ImportError:  # pragma: no cover - falls back to system cert store
    certifi = None


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def runtime_pins_path() -> Path:
    return repo_root() / "scripts" / "release" / "runtime_pins.json"


def load_runtime_pins() -> dict:
    return json.loads(runtime_pins_path().read_text())


def baseline_package_specs() -> list[str]:
    return [
        f"{pin['package']}=={pin['version']}"
        for pin in load_runtime_pins()["desktop_runtime"]["baseline_packages"]
    ]


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


def verify_sha256_from_sums(path: Path, sha256_sums_path: Path) -> None:
    expected = None
    for line in sha256_sums_path.read_text().splitlines():
        parts = line.strip().split()
        if len(parts) >= 2 and parts[-1] == path.name:
            expected = parts[0]
            break
    if expected is None:
        raise RuntimeError(f"missing checksum entry for {path.name} in {sha256_sums_path.name}")
    verify_sha256(path, expected)


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


def python_executable(python_root: Path) -> Path:
    if (python_root / "bin" / "python3").is_file():
        return python_root / "bin" / "python3"
    if (python_root / "python.exe").is_file():
        return python_root / "python.exe"
    raise RuntimeError(f"python executable missing from extracted runtime at {python_root}")


def upgrade_pip(python_root: Path) -> None:
    python_path = python_executable(python_root)
    subprocess.run(
        [str(python_path), "-m", "ensurepip", "--upgrade"],
        check=True,
        capture_output=True,
        text=True,
    )
    subprocess.run(
        [str(python_path), "-m", "pip", "install", "--upgrade", "pip"],
        check=True,
        capture_output=True,
        text=True,
        env={**dict(os.environ), "PIP_DISABLE_PIP_VERSION_CHECK": "1"},
    )


def seed_baseline_packages(python_root: Path) -> None:
    subprocess.run(
        [str(python_executable(python_root)), "-m", "pip", "install", *baseline_package_specs()],
        check=True,
        capture_output=True,
        text=True,
        env={**dict(os.environ), "PIP_DISABLE_PIP_VERSION_CHECK": "1"},
    )


def prune_desktop_runtime(python_root: Path) -> None:
    removable_paths = [
        python_root / "include",
        python_root / "lib" / "pkgconfig",
        python_root / "share" / "man",
        python_root / "share" / "terminfo",
    ]

    for path in removable_paths:
        if path.is_dir():
            shutil.rmtree(path)
        elif path.exists():
            path.unlink()


def stage_python_runtime(destination_root: Path, artifact_url: str, artifact_sha256: str) -> Path:
    destination_root.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(prefix="tine-python-runtime-") as tmpdir:
        temp_root = Path(tmpdir)
        archive_path = temp_root / "python-runtime.tar.gz"
        extract_root = temp_root / "extract"
        extract_root.mkdir()

        download(artifact_url, archive_path)
        verify_sha256(archive_path, artifact_sha256)
        extract_tar_gz(archive_path, extract_root)

        extracted_python_root = locate_python_root(extract_root)
        staged_python_root = temp_root / "python"
        shutil.move(str(extracted_python_root), staged_python_root)

        destination = destination_root / "python"
        if destination.exists():
            shutil.rmtree(destination)
        shutil.move(str(staged_python_root), destination)
        prune_desktop_runtime(destination)

    return destination


def stage_python_runtime_from_checksum_file(
    destination_root: Path,
    artifact_url: str,
    sha256_sums_url: str,
) -> Path:
    destination_root.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(prefix="tine-python-runtime-") as tmpdir:
        temp_root = Path(tmpdir)
        archive_path = temp_root / Path(artifact_url).name
        sha256_sums_path = temp_root / "SHA256SUMS"
        extract_root = temp_root / "extract"
        extract_root.mkdir()

        download(artifact_url, archive_path)
        download(sha256_sums_url, sha256_sums_path)
        verify_sha256_from_sums(archive_path, sha256_sums_path)
        extract_tar_gz(archive_path, extract_root)

        extracted_python_root = locate_python_root(extract_root)
        staged_python_root = temp_root / "python"
        shutil.move(str(extracted_python_root), staged_python_root)

        destination = destination_root / "python"
        if destination.exists():
            shutil.rmtree(destination)
        shutil.move(str(staged_python_root), destination)
        prune_desktop_runtime(destination)

    return destination