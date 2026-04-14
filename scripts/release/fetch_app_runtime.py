#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
from pathlib import Path

from standalone_python import (
    load_runtime_pins,
    python_executable,
    repo_root,
    seed_baseline_packages,
    stage_python_runtime as stage_bundled_python_runtime,
)


SUPPORTED_TARGETS = {
    "macos-aarch64",
    "macos-x86_64",
    "linux-x86_64",
    "windows-x86_64",
}


def pins_path() -> Path:
    return repo_root() / "scripts" / "release" / "runtime_pins.json"


def runtime_dir() -> Path:
    return repo_root() / "crates" / "tine-app" / "resources" / "runtime"


def load_pins() -> dict:
    return load_runtime_pins()


def python_pins() -> dict:
    return load_pins()["python"]


def baseline_package_pins() -> list[dict[str, str]]:
    return load_pins()["desktop_runtime"]["baseline_packages"]


def baseline_package_specs() -> list[str]:
    return [f"{pin['package']}=={pin['version']}" for pin in baseline_package_pins()]


def sentinel_payload(target: str, artifact: dict, python_root: Path) -> dict:
    return {
        "target": target,
        "python_version": python_pins()["version"],
        "python_url": artifact["url"],
        "python_sha256": artifact["sha256"],
        "python_executable": python_executable(python_root).relative_to(runtime_dir()).as_posix(),
        "baseline_packages": baseline_package_specs(),
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


def stage_desktop_runtime(target: str, artifact: dict) -> None:
    output_root = runtime_dir()
    output_root.mkdir(parents=True, exist_ok=True)
    destination = stage_bundled_python_runtime(output_root, artifact["url"], artifact["sha256"])
    seed_baseline_packages(destination)
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

    stage_desktop_runtime(args.target, artifact)
    print(f"fetched desktop app runtime for {args.target}: {runtime_dir() / 'python'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
