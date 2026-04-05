from __future__ import annotations

import hashlib
import os
import platform
import shutil
import subprocess
import tarfile
import tempfile
import zipfile
from dataclasses import dataclass
from importlib import metadata, resources
from pathlib import Path
from urllib.parse import urljoin
from urllib.request import urlopen


@dataclass(frozen=True)
class SupportedTarget:
    os_name: str
    machine: str
    rust_target: str
    archive_ext: str = ".tar.gz"


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
    try:
        return metadata.version("tine")
    except metadata.PackageNotFoundError:  # pragma: no cover - local source checkout
        return os.environ.get("TINE_PACKAGE_VERSION", "0.1.1-dev")


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

    env_bin = os.environ.get("TINE_BIN")
    if env_bin:
        candidates.append(Path(env_bin))

    env_bin_dir = os.environ.get("TINE_BIN_DIR")
    if env_bin_dir:
        candidates.append(Path(env_bin_dir) / binary_name())

    package_root = resources.files("tine")
    target = supported_target().rust_target
    candidates.append(Path(str(package_root / "bin" / target / binary_name())))
    candidates.append(Path(str(package_root / "bin" / binary_name())))
    candidates.extend(source_checkout_binary_candidates())
    candidates.append(cached_binary_path())

    return candidates


def source_checkout_binary_candidates(module_file: Path | None = None) -> list[Path]:
    start = (module_file or Path(__file__)).resolve()
    names = [binary_name()]
    candidates: list[Path] = []
    seen: set[Path] = set()

    for ancestor in [start.parent, *start.parents]:
        cargo_toml = ancestor / "Cargo.toml"
        packaging_pyproject = ancestor / "packaging" / "python" / "pyproject.toml"
        if not cargo_toml.is_file() or not packaging_pyproject.is_file():
            continue

        for relative in (
            Path("target") / "debug",
            Path("target") / "release",
        ):
            for name in names:
                candidate = ancestor / relative / name
                if candidate in seen:
                    continue
                seen.add(candidate)
                candidates.append(candidate)

    return candidates


def resolve_binary() -> Path:
    for candidate in binary_candidates():
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return candidate
    fetched = fetch_binary_release()
    if fetched.is_file() and os.access(fetched, os.X_OK):
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
    resolved = binary_path or resolve_binary()
    expected = package_version()
    actual = read_binary_version(resolved)
    if expected != actual:
        raise RuntimeError(
            f"Tine Python wrapper {expected} requires Tine engine {expected}, but found {actual}. "
            "Reinstall `tine` so the Python package and Rust engine match."
        )
    return resolved


def cached_binary_path(version: str | None = None) -> Path:
    resolved_version = version or package_version()
    target = supported_target()
    return cache_root() / "engine" / resolved_version / target.rust_target / binary_name()


def fetch_binary_release(version: str | None = None) -> Path:
    resolved_version = version or package_version()
    destination = cached_binary_path(resolved_version)
    if destination.is_file() and os.access(destination, os.X_OK):
        return destination

    destination.parent.mkdir(parents=True, exist_ok=True)
    archive_name, checksum_name = expected_release_artifacts(resolved_version)
    base_url = release_base_url(resolved_version)

    with tempfile.TemporaryDirectory(prefix="tine-download-") as tmpdir:
        temp_root = Path(tmpdir)
        archive_path = temp_root / archive_name
        checksum_path = temp_root / checksum_name

        download_file(urljoin(base_url, archive_name), archive_path)
        download_file(urljoin(base_url, checksum_name), checksum_path)
        verify_checksum(archive_path, checksum_path)
        extracted_binary = extract_binary_from_archive(archive_path, temp_root)
        extracted_binary.chmod(extracted_binary.stat().st_mode | 0o111)
        shutil.move(str(extracted_binary), str(destination))

    destination.chmod(destination.stat().st_mode | 0o111)
    return destination


def download_file(url: str, destination: Path) -> None:
    with urlopen(url) as response, destination.open("wb") as handle:
        shutil.copyfileobj(response, handle)


def verify_checksum(archive_path: Path, checksum_path: Path) -> None:
    expected_line = checksum_path.read_text().strip()
    expected_hash = expected_line.split()[0]
    actual_hash = hashlib.sha256(archive_path.read_bytes()).hexdigest()
    if actual_hash != expected_hash:
        raise RuntimeError(
            f"checksum verification failed for {archive_path.name}: expected {expected_hash}, got {actual_hash}"
        )


def extract_binary_from_archive(archive_path: Path, destination_dir: Path) -> Path:
    if archive_path.name.endswith(".zip"):
        with zipfile.ZipFile(archive_path) as archive:
            member = next((item for item in archive.namelist() if Path(item).name == binary_name()), None)
            if member is None:
                raise RuntimeError(f"archive {archive_path.name} does not contain {binary_name()}")
            archive.extract(member, destination_dir)
            extracted = destination_dir / member
            final_path = destination_dir / binary_name()
            if extracted != final_path:
                extracted.replace(final_path)
            return final_path

    with tarfile.open(archive_path, "r:gz") as archive:
        member = next((item for item in archive.getmembers() if Path(item.name).name == binary_name()), None)
        if member is None:
            raise RuntimeError(f"archive {archive_path.name} does not contain {binary_name()}")
        archive.extract(member, destination_dir)
        extracted = destination_dir / member.name
        final_path = destination_dir / binary_name()
        if extracted != final_path:
            extracted.replace(final_path)
        return final_path
