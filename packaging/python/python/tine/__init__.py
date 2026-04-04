"""Python distribution wrapper for the Tine runtime.

This package is the public `pip install tine` surface. It installs wrapper
entrypoints and the MCP adapter around the canonical Rust runtime.
"""

from __future__ import annotations

from importlib import metadata

from .runtime import expected_release_artifacts, supported_target

try:
    __version__ = metadata.version("tine")
except metadata.PackageNotFoundError:  # pragma: no cover - local source checkout
    __version__ = "0.1.0-dev"

__all__ = ["__version__", "expected_release_artifacts", "supported_target"]

_REMOVED_SDK_NAMES = {"Workspace", "Experiment"}


def __getattr__(name: str) -> object:
    if name in _REMOVED_SDK_NAMES:
        raise AttributeError(
            "Python SDK support has been removed from the public `tine` package. "
            "Use `tine serve`, `tine doctor`, and `tine mcp serve` "
            "instead of importing runtime APIs from Python."
        )
    raise AttributeError(f"module 'tine' has no attribute {name!r}")
