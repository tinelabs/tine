#!/usr/bin/env python3

from __future__ import annotations

import argparse
import hashlib
import shutil
import tarfile
import tempfile
import zipfile
from pathlib import Path

from standalone_python import (
    seed_baseline_packages,
    stage_python_runtime_from_checksum_file,
    upgrade_pip,
)


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


def build_archive(staging_dir: Path, archive_path: Path) -> None:
    if archive_path.suffix == ".zip":
        with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for file_path in staging_dir.rglob("*"):
                if file_path.is_file():
                    archive.write(file_path, arcname=file_path.relative_to(staging_dir))
        return

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
        python_root = stage_python_runtime_from_checksum_file(
            runtime_dir,
            PYTHON_STANDALONE_BASE_URL + asset_name,
            PYTHON_STANDALONE_BASE_URL + "SHA256SUMS",
        )
        upgrade_pip(python_root)
        seed_baseline_packages(python_root)

        archive_path.parent.mkdir(parents=True, exist_ok=True)
        build_archive(staging_dir, archive_path)
        write_checksum(archive_path)

    print(f"packaged runtime bundle: {archive_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())