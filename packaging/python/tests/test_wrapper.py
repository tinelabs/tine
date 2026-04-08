from __future__ import annotations

import hashlib
import os
import tarfile
import stat
import subprocess
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest import mock

import tine
from tine import cli, runtime


class WrapperTests(unittest.TestCase):
    def test_sdk_surface_is_removed(self) -> None:
        with self.assertRaisesRegex(AttributeError, "Python SDK support has been removed"):
            _ = tine.Workspace

    def test_expected_release_artifacts_match_linux_x86_64_contract(self) -> None:
        with mock.patch("platform.system", return_value="Linux"), mock.patch(
            "platform.machine", return_value="x86_64"
        ), mock.patch.object(runtime, "package_version", return_value="0.1.0"):
            self.assertEqual(
                runtime.expected_release_artifacts(),
                [
                    "tine-0.1.0-x86_64-unknown-linux-gnu.tar.gz",
                    "tine-0.1.0-x86_64-unknown-linux-gnu.tar.gz.sha256",
                ],
            )

    def test_expected_release_artifacts_match_windows_contract(self) -> None:
        with mock.patch("platform.system", return_value="Windows"), mock.patch(
            "platform.machine", return_value="AMD64"
        ), mock.patch.object(runtime, "package_version", return_value="0.1.0"):
            self.assertEqual(
                runtime.expected_release_artifacts(),
                [
                    "tine-0.1.0-x86_64-pc-windows-msvc.zip",
                    "tine-0.1.0-x86_64-pc-windows-msvc.zip.sha256",
                ],
            )

    def test_wrapper_execs_compatible_binary(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            binary = Path(tmpdir) / "tine"
            binary.write_text("#!/bin/sh\necho 'tine 0.1.0'\n")
            binary.chmod(binary.stat().st_mode | stat.S_IEXEC)

            with mock.patch.dict(os.environ, {"TINE_BIN": str(binary), "TINE_PACKAGE_VERSION": "0.1.0"}):
                with mock.patch("os.execv") as execv:
                    exit_code = cli.main(["version"])

                self.assertEqual(os.environ["TINE_WRAPPER_PYTHON"], sys.executable)
                self.assertEqual(os.environ["TINE_RUNTIME_ROOT"], str(binary.parent))

            self.assertIsNone(exit_code)
            execv.assert_called_once_with(str(binary), [str(binary), "version"])

    def test_wrapper_sets_tine_ui_dir_when_packaged_ui_exists(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            binary = Path(tmpdir) / "tine"
            binary.write_text("#!/bin/sh\necho 'tine 0.1.0'\n")
            binary.chmod(binary.stat().st_mode | stat.S_IEXEC)
            ui_dir = Path(tmpdir) / "ui"
            ui_dir.mkdir()
            (ui_dir / "index.html").write_text("<html></html>\n")

            with mock.patch.dict(os.environ, {"TINE_BIN": str(binary), "TINE_PACKAGE_VERSION": "0.1.0"}, clear=True):
                with mock.patch("tine.cli.package_ui_dir", return_value=ui_dir):
                    with mock.patch("os.execv") as execv:
                        exit_code = cli.main(["version"])

                    self.assertEqual(os.environ["TINE_UI_DIR"], str(ui_dir))
                    self.assertEqual(os.environ["TINE_WRAPPER_PYTHON"], sys.executable)
                    self.assertEqual(os.environ["TINE_RUNTIME_ROOT"], str(binary.parent))

            self.assertIsNone(exit_code)
            execv.assert_called_once_with(str(binary), [str(binary), "version"])

    def test_wrapper_sets_bundled_python_env_when_runtime_contains_python(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            runtime_root = Path(tmpdir)
            binary = runtime_root / "tine"
            binary.write_text("#!/bin/sh\necho 'tine 0.1.0'\n")
            binary.chmod(binary.stat().st_mode | stat.S_IEXEC)
            bundled_python = runtime_root / runtime.bundled_python_relative_path()
            bundled_python.parent.mkdir(parents=True)
            bundled_python.write_text("#!/bin/sh\n")
            bundled_python.chmod(bundled_python.stat().st_mode | stat.S_IEXEC)

            with mock.patch.dict(os.environ, {"TINE_BIN": str(binary), "TINE_PACKAGE_VERSION": "0.1.0"}, clear=True):
                with mock.patch("os.execv") as execv:
                    exit_code = cli.main(["doctor"])

                    self.assertEqual(os.environ["TINE_BUNDLED_PYTHON"], str(bundled_python))

            self.assertIsNone(exit_code)
            execv.assert_called_once_with(str(binary), [str(binary), "doctor"])

    def test_fetches_binary_from_release_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as release_dir, tempfile.TemporaryDirectory() as cache_dir:
            release_root = Path(release_dir)
            self._write_release_artifact_set(release_root, version="0.1.0")

            with mock.patch("platform.system", return_value="Linux"), mock.patch(
                "platform.machine", return_value="x86_64"
            ), mock.patch.dict(
                os.environ,
                {
                    "TINE_PACKAGE_VERSION": "0.1.0",
                    "TINE_RELEASE_BASE_URL": release_root.as_uri() + "/",
                    "TINE_CACHE_DIR": cache_dir,
                },
                clear=False,
            ), mock.patch.object(runtime, "source_checkout_binary_candidates", return_value=[]):
                binary = runtime.ensure_compatible_binary()

            self.assertTrue(binary.is_file())
            self.assertIn("x86_64-unknown-linux-gnu", str(binary))

    def test_fetches_runtime_root_from_release_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as release_dir, tempfile.TemporaryDirectory() as cache_dir:
            release_root = Path(release_dir)
            self._write_release_artifact_set(release_root, version="0.1.0", include_runtime_python=True)

            with mock.patch("platform.system", return_value="Linux"), mock.patch(
                "platform.machine", return_value="x86_64"
            ), mock.patch.dict(
                os.environ,
                {
                    "TINE_PACKAGE_VERSION": "0.1.0",
                    "TINE_RELEASE_BASE_URL": release_root.as_uri() + "/",
                    "TINE_CACHE_DIR": cache_dir,
                },
                clear=False,
            ), mock.patch.object(runtime, "source_checkout_binary_candidates", return_value=[]):
                resolved = runtime.ensure_compatible_runtime()

            self.assertTrue(resolved.binary_path.is_file())
            self.assertEqual(resolved.runtime_root, Path(cache_dir) / "engine" / "0.1.0" / "x86_64-unknown-linux-gnu")
            self.assertEqual(
                resolved.bundled_python_path,
                resolved.runtime_root / "runtime" / "python" / "bin" / "python3",
            )

    def test_reuses_cached_binary_without_redownloading(self) -> None:
        with tempfile.TemporaryDirectory() as release_dir, tempfile.TemporaryDirectory() as cache_dir:
            release_root = Path(release_dir)
            self._write_release_artifact_set(release_root, version="0.1.0")

            env = {
                "TINE_PACKAGE_VERSION": "0.1.0",
                "TINE_RELEASE_BASE_URL": release_root.as_uri() + "/",
                "TINE_CACHE_DIR": cache_dir,
            }
            with mock.patch("platform.system", return_value="Linux"), mock.patch(
                "platform.machine", return_value="x86_64"
            ), mock.patch.dict(os.environ, env, clear=False), mock.patch.object(
                runtime, "source_checkout_binary_candidates", return_value=[]
            ):
                first = runtime.ensure_compatible_binary()
                with mock.patch.object(runtime, "download_file", side_effect=AssertionError("should not redownload")):
                    second = runtime.ensure_compatible_binary()

            self.assertEqual(first, second)

    def test_reuses_cached_runtime_without_redownloading(self) -> None:
        with tempfile.TemporaryDirectory() as release_dir, tempfile.TemporaryDirectory() as cache_dir:
            release_root = Path(release_dir)
            self._write_release_artifact_set(release_root, version="0.1.0", include_runtime_python=True)

            env = {
                "TINE_PACKAGE_VERSION": "0.1.0",
                "TINE_RELEASE_BASE_URL": release_root.as_uri() + "/",
                "TINE_CACHE_DIR": cache_dir,
            }
            with mock.patch("platform.system", return_value="Linux"), mock.patch(
                "platform.machine", return_value="x86_64"
            ), mock.patch.dict(os.environ, env, clear=False), mock.patch.object(
                runtime, "source_checkout_binary_candidates", return_value=[]
            ):
                first = runtime.ensure_compatible_runtime()
                with mock.patch.object(runtime, "download_file", side_effect=AssertionError("should not redownload")):
                    second = runtime.ensure_compatible_runtime()

            self.assertEqual(first, second)

    def test_download_file_uses_certifi_ssl_context(self) -> None:
        destination = Path(tempfile.mkdtemp()) / "artifact.bin"

        response = mock.MagicMock()
        response.__enter__.return_value = response
        response.__exit__.return_value = None

        certifi_mock = mock.Mock()
        certifi_mock.where.return_value = "/tmp/certifi.pem"

        with mock.patch.object(runtime, "certifi", certifi_mock):
            with mock.patch("tine.runtime.ssl.create_default_context", return_value="ssl-context") as context_mock:
                with mock.patch("tine.runtime.urlopen", return_value=response) as urlopen_mock:
                    with mock.patch("shutil.copyfileobj") as copy_mock:
                        runtime.download_file("https://example.com/tine.tar.gz", destination)

        certifi_mock.where.assert_called_once_with()
        context_mock.assert_called_once_with(cafile="/tmp/certifi.pem")
        urlopen_mock.assert_called_once_with(
            "https://example.com/tine.tar.gz",
            context="ssl-context",
        )
        copy_mock.assert_called_once()

    def test_download_file_falls_back_to_default_ssl_context_without_certifi(self) -> None:
        destination = Path(tempfile.mkdtemp()) / "artifact.bin"

        response = mock.MagicMock()
        response.__enter__.return_value = response
        response.__exit__.return_value = None

        with mock.patch.object(runtime, "certifi", None):
            with mock.patch("tine.runtime.ssl.create_default_context", return_value="ssl-context") as context_mock:
                with mock.patch("tine.runtime.urlopen", return_value=response) as urlopen_mock:
                    with mock.patch("shutil.copyfileobj") as copy_mock:
                        runtime.download_file("https://example.com/tine.tar.gz", destination)

        context_mock.assert_called_once_with()
        urlopen_mock.assert_called_once_with(
            "https://example.com/tine.tar.gz",
            context="ssl-context",
        )
        copy_mock.assert_called_once()

    def test_runtime_imports_without_certifi_installed(self) -> None:
        package_root = Path(__file__).resolve().parents[1] / "python"
        env = os.environ.copy()
        env["PYTHONPATH"] = str(package_root)

        result = subprocess.run(
            [sys.executable, "-S", "-c", "import tine.runtime; print('ok')"],
            check=True,
            capture_output=True,
            text=True,
            env=env,
        )

        self.assertEqual(result.stdout.strip(), "ok")

    def test_fetches_windows_binary_from_zip_release_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as release_dir, tempfile.TemporaryDirectory() as cache_dir:
            release_root = Path(release_dir)
            self._write_release_artifact_set(
                release_root,
                version="0.1.0",
                target="x86_64-pc-windows-msvc",
                binary_filename="tine.exe",
            )

            with mock.patch("platform.system", return_value="Windows"), mock.patch(
                "platform.machine", return_value="AMD64"
            ), mock.patch.dict(
                os.environ,
                {
                    "TINE_PACKAGE_VERSION": "0.1.0",
                    "TINE_RELEASE_BASE_URL": release_root.as_uri() + "/",
                    "TINE_CACHE_DIR": cache_dir,
                },
                clear=False,
            ), mock.patch.object(runtime, "read_binary_version", return_value="0.1.0"), mock.patch.object(
                runtime, "source_checkout_binary_candidates", return_value=[]
            ):
                binary = runtime.ensure_compatible_binary()

            self.assertTrue(binary.is_file())
            self.assertTrue(str(binary).endswith("tine.exe"))

    def test_finds_repo_checkout_binary_before_release_download(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_root = Path(tmpdir)
            (repo_root / "Cargo.toml").write_text("[workspace]\nmembers = []\n")
            packaging_dir = repo_root / "packaging" / "python"
            packaging_dir.mkdir(parents=True)
            (packaging_dir / "pyproject.toml").write_text("[project]\nname='tine'\n")
            module_file = repo_root / "packaging" / "python" / "python" / "tine" / "runtime.py"
            module_file.parent.mkdir(parents=True)
            module_file.write_text("# test runtime module path\n")

            binary = repo_root / "target" / "debug" / "tine"
            binary.parent.mkdir(parents=True)
            binary.write_text("#!/bin/sh\necho 'tine 0.1.0'\n")
            binary.chmod(binary.stat().st_mode | stat.S_IEXEC)

            with mock.patch.dict(os.environ, {"TINE_PACKAGE_VERSION": "0.1.0"}, clear=False):
                with mock.patch.object(runtime, "__file__", str(module_file)):
                    with mock.patch.object(runtime, "package_root_path", return_value=repo_root / "empty-package"):
                        with mock.patch.object(
                            runtime,
                            "fetch_binary_release",
                            side_effect=AssertionError("should not fetch release when local repo binary exists"),
                        ):
                            resolved = runtime.ensure_compatible_binary()

            self.assertEqual(resolved.resolve(), binary.resolve())

    def test_source_checkout_ignores_packaged_binary_candidates(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_root = Path(tmpdir)
            (repo_root / "Cargo.toml").write_text("[workspace]\nmembers = []\n")
            packaging_dir = repo_root / "packaging" / "python"
            packaging_dir.mkdir(parents=True)
            (packaging_dir / "pyproject.toml").write_text("[project]\nname='tine'\n")

            module_file = repo_root / "packaging" / "python" / "python" / "tine" / "runtime.py"
            module_file.parent.mkdir(parents=True)
            module_file.write_text("# test runtime module path\n")

            packaged_binary = module_file.parent / "bin" / "aarch64-apple-darwin" / "tine"
            packaged_binary.parent.mkdir(parents=True)
            packaged_binary.write_text("#!/bin/sh\necho 'tine 0.0.9'\n")
            packaged_binary.chmod(packaged_binary.stat().st_mode | stat.S_IEXEC)

            repo_binary = repo_root / "target" / "debug" / "tine"
            repo_binary.parent.mkdir(parents=True)
            repo_binary.write_text("#!/bin/sh\necho 'tine 0.1.0'\n")
            repo_binary.chmod(repo_binary.stat().st_mode | stat.S_IEXEC)

            with mock.patch("platform.system", return_value="Darwin"), mock.patch(
                "platform.machine", return_value="arm64"
            ), mock.patch.dict(os.environ, {"TINE_PACKAGE_VERSION": "0.1.0"}, clear=False):
                with mock.patch.object(runtime, "__file__", str(module_file)):
                    with mock.patch.object(
                        runtime,
                        "fetch_binary_release",
                        side_effect=AssertionError("should not fetch release when source checkout binary exists"),
                    ):
                        resolved = runtime.ensure_compatible_binary()

            self.assertEqual(resolved.resolve(), repo_binary.resolve())

    def test_release_base_url_skips_source_checkout_binary_candidates(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_root = Path(tmpdir)
            (repo_root / "Cargo.toml").write_text("[workspace]\nmembers = []\n")
            packaging_dir = repo_root / "packaging" / "python"
            packaging_dir.mkdir(parents=True)
            (packaging_dir / "pyproject.toml").write_text("[project]\nname='tine'\n")

            module_file = repo_root / "packaging" / "python" / "python" / "tine" / "runtime.py"
            module_file.parent.mkdir(parents=True)
            module_file.write_text("# test runtime module path\n")

            repo_binary = repo_root / "target" / "debug" / "tine"
            repo_binary.parent.mkdir(parents=True)
            repo_binary.write_text("#!/bin/sh\necho 'tine 0.1.0'\n")
            repo_binary.chmod(repo_binary.stat().st_mode | stat.S_IEXEC)

            with mock.patch("platform.system", return_value="Darwin"), mock.patch(
                "platform.machine", return_value="arm64"
            ), mock.patch.object(runtime, "__file__", str(module_file)), mock.patch.dict(
                os.environ,
                {
                    "TINE_PACKAGE_VERSION": "0.1.0",
                    "TINE_RELEASE_BASE_URL": "file:///tmp/releases/",
                },
                clear=False,
            ):
                candidates = runtime.binary_candidates()

            self.assertNotIn(repo_binary, candidates)

    def test_wrapper_reports_version_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            binary = Path(tmpdir) / "tine"
            binary.write_text("#!/bin/sh\necho 'tine 0.0.9'\n")
            binary.chmod(binary.stat().st_mode | stat.S_IEXEC)

            with mock.patch.dict(os.environ, {"TINE_BIN": str(binary), "TINE_PACKAGE_VERSION": "0.1.0"}):
                exit_code = cli.main(["doctor"])

        self.assertEqual(exit_code, 1)

    def test_tine_mcp_serve_routes_to_python_mcp(self) -> None:
        with mock.patch("tine.cli.mcp.main", return_value=0) as mcp_main:
            exit_code = cli.main(["mcp", "serve", "--api-url", "http://127.0.0.1:9473"])

        self.assertEqual(exit_code, 0)
        mcp_main.assert_called_once_with(
            ["--api-url", "http://127.0.0.1:9473"],
            prog="tine mcp serve",
        )

    def test_tine_mcp_print_config_is_handled_in_python(self) -> None:
        with mock.patch("tine.cli.mcp.build_config_document", return_value={"mcpServers": {}}) as build:
            with mock.patch("builtins.print") as print_mock:
                exit_code = cli.main(["mcp", "print-config", "--host", "claude"])

        self.assertEqual(exit_code, 0)
        build.assert_called_once_with(
            host="claude",
            name="tine",
            api_url=None,
            command_path=None,
        )
        print_mock.assert_called_once()

    def test_tine_mcp_register_is_handled_in_python(self) -> None:
        with mock.patch(
            "tine.cli.mcp.build_config_document",
            return_value={"mcpServers": {"tine": {"command": "tine", "args": ["mcp", "serve"]}}},
        ) as build:
            with mock.patch("tine.cli.mcp.register_config", return_value=Path("/tmp/claude.json")) as register:
                with mock.patch("builtins.print") as print_mock:
                    exit_code = cli.main(
                        [
                            "mcp",
                            "register",
                            "--host",
                            "claude",
                            "--api-url",
                            "http://127.0.0.1:9473",
                            "--config-path",
                            "/tmp/claude.json",
                        ]
                    )

        self.assertEqual(exit_code, 0)
        build.assert_called_once_with(
            host="claude",
            name="tine",
            api_url="http://127.0.0.1:9473",
            command_path=None,
        )
        register.assert_called_once_with(
            host="claude",
            document={"mcpServers": {"tine": {"command": "tine", "args": ["mcp", "serve"]}}},
            name="tine",
            config_path="/tmp/claude.json",
        )
        print_mock.assert_called_once_with("Registered MCP server 'tine' in /tmp/claude.json")

    @staticmethod
    def _write_release_artifact_set(
        release_root: Path,
        *,
        version: str,
        target: str = "x86_64-unknown-linux-gnu",
        binary_filename: str = "tine",
        include_runtime_python: bool = False,
    ) -> None:
        archive_name, checksum_name = runtime.expected_release_artifacts_for_target(target, version)
        staging = release_root / "staging"
        staging.mkdir()
        binary = staging / binary_filename
        binary.write_text("#!/bin/sh\necho 'tine 0.1.0'\n")
        binary.chmod(binary.stat().st_mode | stat.S_IEXEC)

        if include_runtime_python:
            if archive_name.endswith(".zip"):
                python_rel = Path("runtime") / "python" / "python.exe"
            else:
                python_rel = Path("runtime") / "python" / "bin" / "python3"
            bundled_python = staging / python_rel
            bundled_python.parent.mkdir(parents=True, exist_ok=True)
            bundled_python.write_text("#!/bin/sh\n")
            bundled_python.chmod(bundled_python.stat().st_mode | stat.S_IEXEC)

        archive_path = release_root / archive_name
        if archive_name.endswith(".zip"):
            with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
                for file_path in staging.rglob("*"):
                    if file_path.is_file():
                        archive.write(file_path, arcname=file_path.relative_to(staging))
        else:
            with tarfile.open(archive_path, "w:gz") as archive:
                for file_path in staging.rglob("*"):
                    if file_path.is_file():
                        archive.add(file_path, arcname=file_path.relative_to(staging))

        checksum = hashlib.sha256(archive_path.read_bytes()).hexdigest()
        (release_root / checksum_name).write_text(f"{checksum}  {archive_name}\n")
