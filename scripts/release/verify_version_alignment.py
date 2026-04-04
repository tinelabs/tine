#!/usr/bin/env python3

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path


def read_workspace_version(repo_root: Path) -> str:
    text = (repo_root / "Cargo.toml").read_text()
    match = re.search(r"(?m)^\[workspace\.package\]\s*(?:.*\n)*?version = \"([^\"]+)\"", text)
    if not match:
        raise RuntimeError("failed to locate workspace.package.version in Cargo.toml")
    return match.group(1)


def read_python_version(repo_root: Path) -> str:
    text = (repo_root / "packaging/python/pyproject.toml").read_text()
    match = re.search(r"(?m)^\[project\]\s*(?:.*\n)*?version = \"([^\"]+)\"", text)
    if not match:
        raise RuntimeError("failed to locate project.version in packaging/python/pyproject.toml")
    return match.group(1)


def read_binary_version(binary_path: Path) -> str:
    result = subprocess.run(
        [str(binary_path), "version"],
        check=True,
        capture_output=True,
        text=True,
    )
    output = result.stdout.strip()
    if not output:
        raise RuntimeError(f"no version output from {binary_path}")
    return output.split()[-1]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Verify Python wrapper, workspace, and optional binary versions align."
    )
    parser.add_argument("--repo-root", default=".", help="Path to the repository root.")
    parser.add_argument(
        "--binary",
        help="Optional path to a built `tine` binary whose version should also match.",
    )
    args = parser.parse_args(argv)

    repo_root = Path(args.repo_root).resolve()
    workspace_version = read_workspace_version(repo_root)
    python_version = read_python_version(repo_root)
    if workspace_version != python_version:
        print(
            f"version mismatch: workspace Cargo.toml={workspace_version} packaging/python={python_version}",
            file=sys.stderr,
        )
        return 1

    if args.binary:
        binary_version = read_binary_version(Path(args.binary).resolve())
        if binary_version != python_version:
            print(
                f"version mismatch: packaging/python={python_version} binary={binary_version}",
                file=sys.stderr,
            )
            return 1

    print(f"version alignment ok: {python_version}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
