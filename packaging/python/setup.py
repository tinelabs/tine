from __future__ import annotations

from pathlib import Path
from shutil import copy2

from setuptools import setup
from setuptools.command.build_py import build_py as _build_py


class build_py(_build_py):
    def run(self) -> None:
        super().run()

        project_root = Path(__file__).resolve().parents[2]
        source_manifest = project_root / "scripts" / "release" / "runtime_pins.json"
        if not source_manifest.exists():
            source_manifest = Path(__file__).resolve().parent / "python" / "tine" / "runtime_pins.json"
        destination = Path(self.build_lib) / "tine" / "runtime_pins.json"
        destination.parent.mkdir(parents=True, exist_ok=True)
        copy2(source_manifest, destination)


setup(cmdclass={"build_py": build_py})