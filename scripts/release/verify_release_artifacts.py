#!/usr/bin/env python3

from __future__ import annotations

import argparse
import sys
from pathlib import Path


def load_runtime(repo_root: Path):
    sys.path.insert(0, str(repo_root / "packaging/python/python"))
    from tine.runtime import expected_release_artifacts_for_target, supported_release_targets

    return expected_release_artifacts_for_target, supported_release_targets


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Verify that release artifacts and checksums exist for the supported matrix."
    )
    parser.add_argument("--repo-root", default=".", help="Path to the repository root.")
    parser.add_argument("--artifact-dir", required=True, help="Directory containing release artifacts.")
    parser.add_argument("--version", required=True, help="Release version to verify.")
    args = parser.parse_args(argv)

    repo_root = Path(args.repo_root).resolve()
    artifact_dir = Path(args.artifact_dir).resolve()
    expected_release_artifacts_for_target, supported_release_targets = load_runtime(repo_root)

    missing: list[str] = []
    for target in supported_release_targets():
        for artifact_name in expected_release_artifacts_for_target(target.rust_target, args.version):
            artifact_path = artifact_dir / artifact_name
            if not artifact_path.is_file():
                missing.append(artifact_name)

    if missing:
        for artifact_name in missing:
            print(f"missing artifact: {artifact_name}", file=sys.stderr)
        return 1

    print(f"release artifacts verified for version {args.version}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
