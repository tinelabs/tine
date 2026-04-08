#!/usr/bin/env python3

from __future__ import annotations

import argparse
import hashlib
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


PYTHON_STANDALONE_RELEASE = "20260408"
PYTHON_STANDALONE_VERSION = "3.11.15"
PYTHON_STANDALONE_BASE_URL = (
    f"https://github.com/astral-sh/python-build-standalone/releases/download/{PYTHON_STANDALONE_RELEASE}/"
)


def python_asset_name(rust_target: str) -> str:
    return (
        f"cpython-{PYTHON_STANDALONE_VERSION}+{PYTHON_STANDALONE_RELEASE}-"
        f"{rust_target}-install_only.tar.gz"
    )


def download(url: str, destination: Path) -> None:
    with urllib.request.urlopen(url, context=download_ssl_context()) as response, destination.open(
        "wb"
    ) as handle:
        shutil.copyfileobj(response, handle)


def download_ssl_context() -> ssl.SSLContext:
    if certifi is not None:
        return ssl.create_default_context(cafile=certifi.where())
    return ssl.create_default_context()


def verify_download(asset_path: Path, sha256_path: Path) -> None:
    expected = None
    for line in sha256_path.read_text().splitlines():
        parts = line.strip().split()
        if len(parts) >= 2 and parts[-1] == asset_path.name:
            expected = parts[0]
            break
    if expected is None:
        raise RuntimeError(f"missing checksum entry for {asset_path.name} in {sha256_path.name}")

    actual = hashlib.sha256(asset_path.read_bytes()).hexdigest()
    if actual != expected:
        raise RuntimeError(
            f"checksum mismatch for {asset_path.name}: expected {expected}, got {actual}"
        )


def extract_tar_gz(archive_path: Path, destination: Path) -> None:
    with tarfile.open(archive_path, "r:gz") as archive:
        extract_kwargs = {"filter": "data"} if sys.version_info >= (3, 12) else {}
        archive.extractall(destination, **extract_kwargs)


def bundled_python_path(runtime_dir: Path, rust_target: str) -> Path:
    return runtime_dir / "python" / "bin" / "python3"


def upgrade_bundled_pip(runtime_dir: Path, rust_target: str) -> None:
    python_path = bundled_python_path(runtime_dir, rust_target)
    if not python_path.is_file():
        raise RuntimeError(f"bundled runtime is missing its Python executable at {python_path}")

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


def build_archive(staging_dir: Path, archive_path: Path) -> None:
    with tarfile.open(archive_path, "w:gz") as archive:
        for file_path in staging_dir.rglob("*"):
            if file_path.is_file():
                archive.add(file_path, arcname=file_path.relative_to(staging_dir))


def write_checksum(archive_path: Path) -> None:
    digest = hashlib.sha256(archive_path.read_bytes()).hexdigest()
    archive_path.with_suffix(archive_path.suffix + ".sha256").write_text(
        f"{digest}  {archive_path.name}\n"
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Package a Tine release artifact with a bundled standalone Python runtime."
    )
    parser.add_argument("--binary", required=True, help="Path to the built tine binary.")
    parser.add_argument("--binary-name", required=True, help="Name of the binary inside the archive.")
    parser.add_argument("--rust-target", required=True, help="Rust target triple for the build.")
    parser.add_argument("--archive-path", required=True, help="Output archive path.")
    args = parser.parse_args(argv)

    binary_path = Path(args.binary).resolve()
    archive_path = Path(args.archive_path).resolve()

    if not binary_path.is_file():
        raise RuntimeError(f"missing built binary at {binary_path}")

    with tempfile.TemporaryDirectory(prefix="tine-runtime-bundle-") as tmpdir:
        temp_root = Path(tmpdir)
        staging_dir = temp_root / "staging"
        runtime_dir = staging_dir / "runtime"
        staging_dir.mkdir()
        runtime_dir.mkdir()

        shutil.copy2(binary_path, staging_dir / args.binary_name)

        asset_name = python_asset_name(args.rust_target)
        asset_path = temp_root / asset_name
        sha256_path = temp_root / "SHA256SUMS"
        download(PYTHON_STANDALONE_BASE_URL + asset_name, asset_path)
        download(PYTHON_STANDALONE_BASE_URL + "SHA256SUMS", sha256_path)
        verify_download(asset_path, sha256_path)
        extract_tar_gz(asset_path, runtime_dir)
        upgrade_bundled_pip(runtime_dir, args.rust_target)

        archive_path.parent.mkdir(parents=True, exist_ok=True)
        build_archive(staging_dir, archive_path)
        write_checksum(archive_path)

    print(f"packaged runtime bundle: {archive_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())