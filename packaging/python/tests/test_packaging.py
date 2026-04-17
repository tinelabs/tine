from __future__ import annotations

import shutil
import subprocess
import sys
import tarfile
import tempfile
import unittest
import zipfile
from pathlib import Path


class PackagingSmokeTests(unittest.TestCase):
    REQUIRED_UI_FILES = [
        "tine/ui/index.html",
        "tine/ui/app.js",
        "tine/ui/style.css",
        "tine/ui/app-helpers.js",
    ]

    def test_sdist_and_wheel_from_sdist_include_ui_assets(self) -> None:
        repo_root = Path(__file__).resolve().parents[3]
        packaging_root = repo_root / "packaging" / "python"

        with tempfile.TemporaryDirectory() as tmpdir:
            temp_root = Path(tmpdir) / "repo"
            self._copy_packaging_fixture(repo_root, temp_root)

            build_root = temp_root / "packaging" / "python"
            dist_dir = temp_root / "dist"
            self._run([sys.executable, "setup.py", "sdist", "--dist-dir", str(dist_dir)], cwd=build_root)

            sdist_path = next(dist_dir.glob("*.tar.gz"))
            self._assert_tar_contains(
                sdist_path,
                [f"python/{path}" for path in self.REQUIRED_UI_FILES],
            )

            unpack_root = temp_root / "sdist-unpack"
            with tarfile.open(sdist_path, "r:gz") as archive:
                archive.extractall(unpack_root)

            extracted_root = next(unpack_root.iterdir())
            shutil.rmtree(extracted_root / "python" / "tine" / "ui", ignore_errors=True)
            self._run(
                [sys.executable, "setup.py", "bdist_wheel", "--dist-dir", str(dist_dir)],
                cwd=extracted_root,
            )

            wheel_path = next(dist_dir.glob("*.whl"))
            self._assert_wheel_contains(wheel_path, self.REQUIRED_UI_FILES)

    def _copy_packaging_fixture(self, repo_root: Path, destination_root: Path) -> None:
        shutil.copytree(repo_root / "packaging", destination_root / "packaging")
        shutil.copytree(repo_root / "ui", destination_root / "ui")
        runtime_pins_src = repo_root / "scripts" / "release" / "runtime_pins.json"
        runtime_pins_dst = destination_root / "scripts" / "release" / "runtime_pins.json"
        runtime_pins_dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(runtime_pins_src, runtime_pins_dst)
        shutil.rmtree(destination_root / "packaging" / "python" / "python" / "tine" / "ui", ignore_errors=True)

    def _run(self, command: list[str], *, cwd: Path) -> None:
        subprocess.run(command, cwd=cwd, check=True, capture_output=True, text=True)

    def _assert_tar_contains(self, archive_path: Path, expected_paths: list[str]) -> None:
        with tarfile.open(archive_path, "r:gz") as archive:
            members = set(archive.getnames())
        for expected_path in expected_paths:
            self.assertTrue(any(member.endswith(f"/{expected_path}") for member in members))

    def _assert_wheel_contains(self, wheel_path: Path, expected_paths: list[str]) -> None:
        with zipfile.ZipFile(wheel_path) as archive:
            members = set(archive.namelist())
        for expected_path in expected_paths:
            self.assertIn(expected_path, members)


if __name__ == "__main__":
    unittest.main()