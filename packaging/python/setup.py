from __future__ import annotations

from pathlib import Path
from shutil import copy2, copytree, rmtree

from setuptools import setup
from setuptools.command.build_py import build_py as _build_py
from setuptools.command.sdist import sdist as _sdist


PACKAGE_DIR = Path(__file__).resolve().parent / "python" / "tine"
PROJECT_ROOT = Path(__file__).resolve().parents[2]


def sync_ui_tree() -> None:
    """Mirror the top-level `ui/` into the Python package.

    This is a no-op when the checkout does not have a top-level ui directory,
    which keeps wheel builds from unpacked sdists idempotent.
    """
    source = PROJECT_ROOT / "ui"
    if not source.is_dir():
        return
    destination = PACKAGE_DIR / "ui"
    if destination.exists():
        rmtree(destination)
    copytree(source, destination)


class build_py(_build_py):
    def run(self) -> None:
        sync_ui_tree()
        super().run()

        source_manifest = PROJECT_ROOT / "scripts" / "release" / "runtime_pins.json"
        if not source_manifest.exists():
            source_manifest = PACKAGE_DIR / "runtime_pins.json"
        destination = Path(self.build_lib) / "tine" / "runtime_pins.json"
        destination.parent.mkdir(parents=True, exist_ok=True)
        copy2(source_manifest, destination)


class sdist(_sdist):
    def run(self) -> None:
        sync_ui_tree()
        super().run()


setup(cmdclass={"build_py": build_py, "sdist": sdist})