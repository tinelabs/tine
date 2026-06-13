from __future__ import annotations

import json
import os
import platform
import sys
import tempfile
import unittest
from pathlib import Path

from tine import runtime

# The install-stage producer lives under scripts/release (not on the wrapper's
# import path), so make it importable for the producer tests.
_REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_REPO_ROOT / "scripts" / "release"))
import standalone_python as sp  # noqa: E402
import fetch_app_runtime as far  # noqa: E402


def _fake_python_root(root: Path) -> Path:
    """Create a python-root layout whose interpreter is this test's python,
    so the producer can actually probe a real interpreter."""
    python_root = root / "python"
    if platform.system() == "Windows":
        python_root.mkdir(parents=True, exist_ok=True)
        # Copy is overkill; a symlink to the running interpreter suffices.
        (python_root / "python.exe").symlink_to(sys.executable)
    else:
        (python_root / "bin").mkdir(parents=True, exist_ok=True)
        (python_root / "bin" / "python3").symlink_to(sys.executable)
    return python_root


class PlatformDescriptorProducerTests(unittest.TestCase):
    def test_payload_reports_running_interpreter_identity(self) -> None:
        payload = sp.platform_descriptor_payload(Path(sys.executable))
        self.assertEqual(payload["machine"], platform.machine())
        self.assertTrue(payload["platform_tag"])
        self.assertTrue(payload["python_version"])

    def test_write_descriptor_persists_into_python_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            python_root = _fake_python_root(Path(tmp))
            written = sp.write_platform_descriptor(python_root)
            descriptor = sp.platform_descriptor_path(python_root)
            self.assertTrue(descriptor.is_file())
            on_disk = json.loads(descriptor.read_text())
            self.assertEqual(on_disk, written)
            self.assertEqual(on_disk["machine"], platform.machine())


class PlatformDescriptorConsumerTests(unittest.TestCase):
    def _runtime_root_with_descriptor(self, root: Path, machine: str | None) -> Path:
        """Lay out a runtime root with a bundled python and (optionally) a
        descriptor, matching `bundled_python_relative_path()`."""
        python_dir = root / "runtime" / "python"
        if platform.system() == "Windows":
            python_dir.mkdir(parents=True, exist_ok=True)
            (python_dir / "python.exe").write_text("")
        else:
            (python_dir / "bin").mkdir(parents=True, exist_ok=True)
            (python_dir / "bin" / "python3").write_text("")
        if machine is not None:
            (python_dir / sp.PLATFORM_DESCRIPTOR_FILENAME).write_text(
                json.dumps({"machine": machine, "platform_tag": "t", "python_version": "3.12.0"})
            )
        return root

    def test_reads_machine_from_bundled_descriptor(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = self._runtime_root_with_descriptor(Path(tmp), "arm64")
            self.assertEqual(runtime.bundled_platform_machine(root), "arm64")

    def test_returns_none_without_descriptor(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = self._runtime_root_with_descriptor(Path(tmp), None)
            self.assertIsNone(runtime.bundled_platform_machine(root))

    def test_returns_none_without_bundled_python(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            # Empty root: no bundled python at all.
            self.assertIsNone(runtime.bundled_platform_machine(Path(tmp)))

    def test_blank_machine_is_treated_as_absent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = self._runtime_root_with_descriptor(Path(tmp), "   ")
            self.assertIsNone(runtime.bundled_platform_machine(root))

    def test_process_is_translated_is_false_off_darwin(self) -> None:
        if platform.system() == "Darwin":
            # On macOS it must at least not raise and return a bool.
            self.assertIsInstance(runtime.process_is_translated(), bool)
        else:
            self.assertFalse(runtime.process_is_translated())


class TargetConsistencyGuardTests(unittest.TestCase):
    def test_matching_arch_passes(self) -> None:
        # No raise.
        far.assert_descriptor_matches_target("macos-aarch64", {"machine": "arm64"})
        far.assert_descriptor_matches_target("linux-x86_64", {"machine": "x86_64"})
        far.assert_descriptor_matches_target("windows-x86_64", {"machine": "AMD64"})

    def test_mismatched_arch_raises(self) -> None:
        with self.assertRaises(RuntimeError):
            far.assert_descriptor_matches_target("macos-aarch64", {"machine": "x86_64"})
        with self.assertRaises(RuntimeError):
            far.assert_descriptor_matches_target("macos-x86_64", {"machine": "arm64"})

    def test_every_supported_target_has_expected_machines(self) -> None:
        self.assertEqual(
            set(far.TARGET_EXPECTED_MACHINE), set(far.SUPPORTED_TARGETS)
        )


if __name__ == "__main__":
    unittest.main()
