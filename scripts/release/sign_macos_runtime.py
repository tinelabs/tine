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


def binary_sign_order(path: Path, hardened_runtime: bool) -> tuple[int, str]:
    name = path.name
    if name.endswith(".dylib"):
        return (1, str(path))
    if ".so" in name:
        return (0, str(path))
    if hardened_runtime:
        return (2, str(path))
    return (1, str(path))


def iter_signable_binaries(root: Path) -> list[tuple[Path, bool]]:
    binaries: dict[Path, bool] = {}
    for candidate in sorted(root.rglob("*")):
        if not candidate.is_file():
            continue
        resolved = candidate.resolve()
        if resolved in binaries or not is_macho_binary(resolved):
            continue
        binaries[resolved] = requires_hardened_runtime(resolved)
    return sorted(binaries.items(), key=lambda item: binary_sign_order(item[0], item[1]))


def sign_binary(
    path: Path,
    identity: str,
    hardened_runtime: bool,
    entitlements_path: Path | None,
) -> None:
    command = [
        "codesign",
        "--force",
        "--sign",
        identity,
        "--timestamp",
    ]
    if KEYCHAIN_PATH is not None:
        command.extend(["--keychain", str(KEYCHAIN_PATH)])
    if hardened_runtime:
        command.extend(["--options", "runtime"])
        if entitlements_path is not None:
            command.extend(["--entitlements", str(entitlements_path)])
    command.append(str(path))
    subprocess.run(command, check=True)


KEYCHAIN_PATH: Path | None = None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Codesign bundled macOS runtime binaries before Tauri notarization."
    )
    parser.add_argument("--root", required=True, help="Root directory containing bundled runtime files.")
    parser.add_argument("--identity", required=True, help="Developer ID signing identity.")
    parser.add_argument("--keychain", help="Optional keychain to use for codesign lookup.")
    parser.add_argument(
        "--entitlements",
        help="Optional entitlements plist to apply to Mach-O executables only.",
    )
    args = parser.parse_args(argv)

    global KEYCHAIN_PATH
    KEYCHAIN_PATH = Path(args.keychain).resolve() if args.keychain else None

    root = Path(args.root).resolve()
    if not root.is_dir():
        raise RuntimeError(f"runtime root does not exist: {root}")
    entitlements_path = Path(args.entitlements).resolve() if args.entitlements else None
    if entitlements_path is not None and not entitlements_path.is_file():
        raise RuntimeError(f"entitlements plist does not exist: {entitlements_path}")

    # Strip extended attributes (com.apple.provenance, com.apple.quarantine)
    # before signing. python-build-standalone binaries carry these from GitHub
    # Releases, causing codesign failures on macOS Sequoia+.
    subprocess.run(["xattr", "-cr", str(root)], check=True)
    print(f"stripped extended attributes from {root}")

    binaries = iter_signable_binaries(root)
    if not binaries:
        print(f"no Mach-O runtime binaries found under {root}")
        return 0

    for binary_path, hardened_runtime in binaries:
        sign_binary(binary_path, args.identity, hardened_runtime, entitlements_path)
        print(f"signed {binary_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())