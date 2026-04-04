#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path


def package_version(repo_root: Path) -> str:
    text = (repo_root / "packaging/python/pyproject.toml").read_text()
    match = re.search(r"(?m)^\[project\]\s*(?:.*\n)*?version = \"([^\"]+)\"", text)
    if not match:
        raise RuntimeError("failed to locate project.version in packaging/python/pyproject.toml")
    return match.group(1)


def run(command: list[str], env: dict[str, str]) -> None:
    subprocess.run(command, check=True, env=env)


def run_capture(command: list[str], env: dict[str, str]) -> str:
    return subprocess.check_output(command, env=env, text=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run wrapper smoke checks against a built Tine engine binary."
    )
    parser.add_argument("--repo-root", default=".", help="Path to the repository root.")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--binary", help="Path to the built `tine` engine binary.")
    source.add_argument(
        "--release-artifact-dir",
        help="Directory containing the release archive and checksum for the current platform.",
    )
    parser.add_argument(
        "--python",
        default=sys.executable,
        help="Python interpreter to use for smoke tests.",
    )
    args = parser.parse_args(argv)

    repo_root = Path(args.repo_root).resolve()
    env = os.environ.copy()
    env["PYTHONPATH"] = str(repo_root / "packaging/python/python")
    env["TINE_PACKAGE_VERSION"] = package_version(repo_root)
    env["TINE_CACHE_DIR"] = str(repo_root / ".tmp" / "wrapper-smoke-cache")

    if args.binary:
        env["TINE_BIN"] = str(Path(args.binary).resolve())
    else:
        env["TINE_RELEASE_BASE_URL"] = Path(args.release_artifact_dir).resolve().as_uri() + "/"

    run([args.python, "-m", "tine.cli", "version"], env)
    run([args.python, "-m", "tine.cli", "mcp", "serve", "--help"], env)
    normalized = run_capture(
        [
            args.python,
            "-c",
            (
                "import json; "
                "from tine.mcp import _cell_payload; "
                "payload = _cell_payload({'name': 'draft'}, object_key='first_cell', experiment_id='tree_1'); "
                "print(json.dumps(payload, sort_keys=True))"
            ),
        ],
        env,
    )
    normalized_payload = json.loads(normalized)
    assert normalized_payload["tree_id"] == "tree_1"
    assert normalized_payload["branch_id"] == "ignored"
    assert normalized_payload["code"]["language"] == "python"
    print("wrapper smoke checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
