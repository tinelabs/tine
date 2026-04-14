#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path


def read_repo_version(repo_root: Path) -> str:
    version = (repo_root / "VERSION").read_text().strip()
    if not version:
        raise RuntimeError("failed to locate repo version in VERSION")
    return version


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


def read_tauri_config_version(config_path: Path) -> str:
    payload = json.loads(config_path.read_text())
    version = payload.get("version")
    if not isinstance(version, str) or not version:
        raise RuntimeError(f"failed to locate version in {config_path}")
    return version


def render_tauri_config(template_path: Path, version: str) -> str:
    template = template_path.read_text()
    return template.replace("{{VERSION}}", version)


def read_tauri_config_version_from_text(payload_text: str, source: str) -> str:
    payload = json.loads(payload_text)
    version = payload.get("version")
    if not isinstance(version, str) or not version:
        raise RuntimeError(f"failed to locate version in {source}")
    return version


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Verify Python wrapper, workspace, and optional binary versions align."
    )
    parser.add_argument("--repo-root", default=".", help="Path to the repository root.")
    parser.add_argument(
        "--binary",
        help="Optional path to a built `tine` binary whose version should also match.",
    )
    parser.add_argument(
        "--tauri-config",
        help="Optional path to a rendered tine-app tauri.conf.json whose version should also match.",
    )
    parser.add_argument(
        "--tauri-template",
        help="Optional path to tine-app tauri.conf.template.json to render for validation.",
    )
    parser.add_argument(
        "--write-tauri-config",
        action="store_true",
        help="When used with --tauri-template and --tauri-config, write the rendered config to disk.",
    )
    args = parser.parse_args(argv)

    repo_root = Path(args.repo_root).resolve()
    repo_version = read_repo_version(repo_root)
    workspace_version = read_workspace_version(repo_root)
    if repo_version != workspace_version:
        print(
            f"version mismatch: VERSION={repo_version} workspace Cargo.toml={workspace_version}",
            file=sys.stderr,
        )
        return 1

    python_version = read_python_version(repo_root)
    if repo_version != python_version:
        print(
            f"version mismatch: VERSION={repo_version} packaging/python={python_version}",
            file=sys.stderr,
        )
        return 1

    if args.binary:
        binary_version = read_binary_version(Path(args.binary).resolve())
        if binary_version != repo_version:
            print(
                f"version mismatch: VERSION={repo_version} binary={binary_version}",
                file=sys.stderr,
            )
            return 1

    if args.tauri_template:
        template_path = Path(args.tauri_template).resolve()
        rendered = render_tauri_config(template_path, repo_version)
        rendered_version = read_tauri_config_version_from_text(rendered, str(template_path))
        if rendered_version != repo_version:
            print(
                f"version mismatch: VERSION={repo_version} tauri-template-rendered={rendered_version}",
                file=sys.stderr,
            )
            return 1

        if args.write_tauri_config:
            if not args.tauri_config:
                raise RuntimeError("--write-tauri-config requires --tauri-config")
            Path(args.tauri_config).resolve().write_text(rendered)

    if args.tauri_config:
        tauri_version = read_tauri_config_version(Path(args.tauri_config).resolve())
        if tauri_version != repo_version:
            print(
                f"version mismatch: VERSION={repo_version} tauri-config={tauri_version}",
                file=sys.stderr,
            )
            return 1

    print(f"version alignment ok: {repo_version}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
