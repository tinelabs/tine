#!/usr/bin/env python3

from __future__ import annotations

import argparse
import subprocess
from pathlib import Path


def is_macho_binary(path: Path) -> bool:
    result = subprocess.run(
        ["file", "-b", str(path)],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.startswith("Mach-O")


def requires_hardened_runtime(path: Path) -> bool:
    result = subprocess.run(
        ["file", "-b", str(path)],
        check=True,
        capture_output=True,
        text=True,
    )
    description = result.stdout.lower()
    return "executable" in description


def iter_signable_binaries(root: Path) -> list[tuple[Path, bool]]:
    binaries: dict[Path, bool] = {}
    for candidate in sorted(root.rglob("*")):
        if not candidate.is_file():
            continue
        resolved = candidate.resolve()
        if resolved in binaries or not is_macho_binary(resolved):
            continue
        binaries[resolved] = requires_hardened_runtime(resolved)
    return sorted(binaries.items())


def sign_binary(path: Path, identity: str, hardened_runtime: bool) -> None:
    command = [
        "codesign",
        "--force",
        "--sign",
        identity,
        "--timestamp",
    ]
    if hardened_runtime:
        command.extend(["--options", "runtime"])
    command.append(str(path))
    subprocess.run(command, check=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Codesign bundled macOS runtime binaries before Tauri notarization."
    )
    parser.add_argument("--root", required=True, help="Root directory containing bundled runtime files.")
    parser.add_argument("--identity", required=True, help="Developer ID signing identity.")
    args = parser.parse_args(argv)

    root = Path(args.root).resolve()
    if not root.is_dir():
        raise RuntimeError(f"runtime root does not exist: {root}")

    binaries = iter_signable_binaries(root)
    if not binaries:
        print(f"no Mach-O runtime binaries found under {root}")
        return 0

    for binary_path, hardened_runtime in binaries:
        sign_binary(binary_path, args.identity, hardened_runtime)
        print(f"signed {binary_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())