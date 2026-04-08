from __future__ import annotations

import hashlib
import os
import platform
import shutil
import ssl
import subprocess
import sys
import tarfile
import tempfile
import zipfile
from dataclasses import dataclass
from importlib import metadata, resources
from pathlib import Path
from urllib.parse import urljoin
from urllib.request import urlopen

try:
    import certifi
except ImportError:  # pragma: no cover - exercised in source smoke path
    certifi = None


@dataclass(frozen=True)
class SupportedTarget:
    os_name: str
    machine: str
    rust_target: str
    archive_ext: str = ".tar.gz"


@dataclass(frozen=True)
class ResolvedRuntime:
    binary_path: Path
    runtime_root: Path

    @property
    def bundled_python_path(self) -> Path | None:
        candidate = bundled_python_path_for_root(self.runtime_root)
        if candidate.is_file():
            return candidate
        return None


_SUPPORTED_TARGETS: dict[tuple[str, str], SupportedTarget] = {
    ("Darwin", "x86_64"): SupportedTarget("macOS", "x86_64", "x86_64-apple-darwin"),
    ("Darwin", "arm64"): SupportedTarget("macOS", "arm64", "aarch64-apple-darwin"),
    ("Darwin", "aarch64"): SupportedTarget("macOS", "arm64", "aarch64-apple-darwin"),
    ("Linux", "x86_64"): SupportedTarget("Linux", "x86_64", "x86_64-unknown-linux-gnu"),
    ("Linux", "arm64"): SupportedTarget("Linux", "arm64", "aarch64-unknown-linux-gnu"),
    ("Linux", "aarch64"): SupportedTarget("Linux", "arm64", "aarch64-unknown-linux-gnu"),
    ("Windows", "AMD64"): SupportedTarget("Windows", "x86_64", "x86_64-pc-windows-msvc", ".zip"),
    ("Windows", "x86_64"): SupportedTarget("Windows", "x86_64", "x86_64-pc-windows-msvc", ".zip"),
}


def supported_release_targets() -> list[SupportedTarget]:
    deduped: dict[str, SupportedTarget] = {}
    for target in _SUPPORTED_TARGETS.values():
        deduped[target.rust_target] = target
    return [deduped[key] for key in sorted(deduped)]


def package_version() -> str:
    overridden = os.environ.get("TINE_PACKAGE_VERSION")
    if overridden:
        return overridden
    try:
        return metadata.version("tine")
    except metadata.PackageNotFoundError:  # pragma: no cover - local source checkout
        return "0.2.0-dev"


def supported_target() -> SupportedTarget:
    key = (platform.system(), platform.machine())
    try:
        return _SUPPORTED_TARGETS[key]
    except KeyError as exc:
        raise RuntimeError(
            f"unsupported platform for public `tine` wrapper: system={key[0]!r} machine={key[1]!r}"
        ) from exc


def binary_name() -> str:
    return "tine.exe" if platform.system() == "Windows" else "tine"


def bundled_python_relative_path() -> Path:
    if platform.system() == "Windows":
        return Path("runtime") / "python" / "python.exe"
    return Path("runtime") / "python" / "bin" / "python3"


def bundled_python_path_for_root(runtime_root: Path) -> Path:
    return runtime_root / bundled_python_relative_path()


def release_base_url(version: str | None = None) -> str:
    resolved_version = version or package_version()
    configured = os.environ.get("TINE_RELEASE_BASE_URL")
    if configured:
        return configured.rstrip("/") + "/"
    return f"https://github.com/tinelabs/tine/releases/download/v{resolved_version}/"


def cache_root() -> Path:
    configured = os.environ.get("TINE_CACHE_DIR")
    if configured:
        return Path(configured)
    return Path.home() / ".cache" / "tine"


def expected_release_artifacts(version: str | None = None) -> list[str]:
    target = supported_target()
    return expected_release_artifacts_for_target(target.rust_target, version)


def expected_release_artifacts_for_target(
    rust_target: str, version: str | None = None
) -> list[str]:
    resolved_version = version or package_version()
    target = next(
        item for item in supported_release_targets() if item.rust_target == rust_target
    )
    base = f"tine-{resolved_version}-{target.rust_target}{target.archive_ext}"
    return [base, f"{base}.sha256"]


def binary_candidates() -> list[Path]:
    candidates: list[Path] = []
    prefer_release_artifacts = os.environ.get("TINE_RELEASE_BASE_URL") is not None

    env_bin = os.environ.get("TINE_BIN")
    if env_bin:
        candidates.append(Path(env_bin))

    env_bin_dir = os.environ.get("TINE_BIN_DIR")
    if env_bin_dir:
        candidates.append(Path(env_bin_dir) / binary_name())

    if not prefer_release_artifacts:
        candidates.extend(source_checkout_binary_candidates())

    if not prefer_release_artifacts and source_checkout_root() is None:
        package_root = package_root_path()
        target = supported_target().rust_target
        candidates.append(package_root / "bin" / target / binary_name())
        candidates.append(package_root / "bin" / binary_name())

    candidates.append(cached_binary_path())

    return candidates


def runtime_root_for_binary(binary_path: Path) -> Path:
    return binary_path.parent


def package_root_path() -> Path:
    return Path(str(resources.files("tine")))


def package_ui_dir() -> Path | None:
    candidate = package_root_path() / "ui"
    if (candidate / "index.html").is_file():
        return candidate
    return None


def source_checkout_root(module_file: Path | None = None) -> Path | None:
    start = (module_file or Path(__file__)).resolve()

    for ancestor in [start.parent, *start.parents]:
        cargo_toml = ancestor / "Cargo.toml"
        packaging_pyproject = ancestor / "packaging" / "python" / "pyproject.toml"
        if cargo_toml.is_file() and packaging_pyproject.is_file():
            return ancestor

    return None


def source_checkout_binary_candidates(module_file: Path | None = None) -> list[Path]:
    root = source_checkout_root(module_file)
    if root is None:
        return []

    names = [binary_name()]
    candidates: list[Path] = []
    seen: set[Path] = set()

    for relative in (
        Path("target") / "debug",
        Path("target") / "release",
    ):
        for name in names:
            candidate = root / relative / name
            if candidate in seen:
                continue
            seen.add(candidate)
            candidates.append(candidate)

    return candidates


def resolve_binary() -> Path:
    return resolve_runtime().binary_path


def resolve_runtime() -> ResolvedRuntime:
    for candidate in binary_candidates():
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return ResolvedRuntime(candidate, runtime_root_for_binary(candidate))
    fetched = fetch_runtime_release()
    if fetched.binary_path.is_file() and os.access(fetched.binary_path, os.X_OK):
        return fetched
    expected = ", ".join(expected_release_artifacts())
    raise FileNotFoundError(
        "unable to find or fetch a Tine engine binary for this install. "
        f"Expected one of: {expected}. You can set TINE_BIN, TINE_BIN_DIR, or TINE_RELEASE_BASE_URL for local development."
    )


def read_binary_version(binary_path: Path) -> str:
    result = subprocess.run(
        [str(binary_path), "version"],
        check=True,
        capture_output=True,
        text=True,
    )
    output = result.stdout.strip()
    if not output:
        raise RuntimeError(f"failed to read version from {binary_path}")
    tokens = output.split()
    return tokens[-1]


def ensure_compatible_binary(binary_path: Path | None = None) -> Path:
    resolved = binary_path or resolve_runtime().binary_path
    expected = package_version()
    actual = read_binary_version(resolved)
    if expected != actual:
        raise RuntimeError(
            f"Tine Python wrapper {expected} requires Tine engine {expected}, but found {actual}. "
            "Reinstall `tine` so the Python package and Rust engine match."
        )
    return resolved


def ensure_compatible_runtime(binary_path: Path | None = None) -> ResolvedRuntime:
    runtime = (
        ResolvedRuntime(binary_path, runtime_root_for_binary(binary_path))
        if binary_path is not None
        else resolve_runtime()
    )
    expected = package_version()
    actual = read_binary_version(runtime.binary_path)
    if expected != actual:
        raise RuntimeError(
            f"Tine Python wrapper {expected} requires Tine engine {expected}, but found {actual}. "
            "Reinstall `tine` so the Python package and Rust engine match."
        )
    return runtime


def cached_binary_path(version: str | None = None) -> Path:
    resolved_version = version or package_version()
    target = supported_target()
    return cache_root() / "engine" / resolved_version / target.rust_target / binary_name()


def cached_runtime_root(version: str | None = None) -> Path:
    resolved_version = version or package_version()
    target = supported_target()
    return cache_root() / "engine" / resolved_version / target.rust_target


def fetch_binary_release(version: str | None = None) -> Path:
    return fetch_runtime_release(version).binary_path


def fetch_runtime_release(version: str | None = None) -> ResolvedRuntime:
    resolved_version = version or package_version()
    runtime_root = cached_runtime_root(resolved_version)
    destination = runtime_root / binary_name()
    if destination.is_file() and os.access(destination, os.X_OK):
        return ResolvedRuntime(destination, runtime_root)

    if runtime_root.exists():
        shutil.rmtree(runtime_root)
    runtime_root.mkdir(parents=True, exist_ok=True)
    archive_name, checksum_name = expected_release_artifacts(resolved_version)
    base_url = release_base_url(resolved_version)

    with tempfile.TemporaryDirectory(prefix="tine-download-") as tmpdir:
        temp_root = Path(tmpdir)
        archive_path = temp_root / archive_name
        checksum_path = temp_root / checksum_name
        extracted_root = temp_root / "extracted"
        extracted_root.mkdir()

        download_file(urljoin(base_url, archive_name), archive_path)
        download_file(urljoin(base_url, checksum_name), checksum_path)
        verify_checksum(archive_path, checksum_path)
        extract_archive(archive_path, extracted_root)
        extracted_binary = locate_binary_in_tree(extracted_root)
        extracted_binary.chmod(extracted_binary.stat().st_mode | 0o111)
        move_tree_contents(extracted_root, runtime_root)

    destination.chmod(destination.stat().st_mode | 0o111)
    return ResolvedRuntime(destination, runtime_root)


def download_file(url: str, destination: Path) -> None:
    with urlopen(url, context=download_ssl_context()) as response, destination.open("wb") as handle:
        shutil.copyfileobj(response, handle)


def download_ssl_context() -> ssl.SSLContext:
    certifi_module = certifi
    if certifi_module is not None:
        return ssl.create_default_context(cafile=certifi_module.where())
    return ssl.create_default_context()


def verify_checksum(archive_path: Path, checksum_path: Path) -> None:
    expected_line = checksum_path.read_text().strip()
    expected_hash = expected_line.split()[0]
    actual_hash = hashlib.sha256(archive_path.read_bytes()).hexdigest()
    if actual_hash != expected_hash:
        raise RuntimeError(
            f"checksum verification failed for {archive_path.name}: expected {expected_hash}, got {actual_hash}"
        )


def extract_archive(archive_path: Path, destination_dir: Path) -> None:
    if archive_path.name.endswith(".zip"):
        with zipfile.ZipFile(archive_path) as archive:
            archive.extractall(destination_dir)
            return

    with tarfile.open(archive_path, "r:gz") as archive:
        extract_kwargs = {"filter": "data"} if sys.version_info >= (3, 12) else {}
        archive.extractall(destination_dir, **extract_kwargs)


def locate_binary_in_tree(root: Path) -> Path:
    matches = [candidate for candidate in root.rglob(binary_name()) if candidate.is_file()]
    if not matches:
        raise RuntimeError(f"downloaded runtime archive does not contain {binary_name()}")
    if len(matches) == 1:
        return matches[0]

    exact_root = root / binary_name()
    if exact_root in matches:
        return exact_root

    exact_nested = root / "tine"
    if exact_nested in matches:
        return exact_nested

    raise RuntimeError(f"downloaded runtime archive contains multiple {binary_name()} candidates")


def move_tree_contents(source_root: Path, destination_root: Path) -> None:
    for child in source_root.iterdir():
        destination = destination_root / child.name
        if destination.exists():
            if destination.is_dir():
                shutil.rmtree(destination)
            else:
                destination.unlink()
        shutil.move(str(child), str(destination))
