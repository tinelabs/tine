from __future__ import annotations

import argparse
import json
import os
import sys

from . import mcp
from .runtime import (
    bundled_platform_machine,
    ensure_compatible_runtime,
    package_ui_dir,
    process_is_translated,
)


def _bundled_uv_path() -> str | None:
    """Path to the uv binary installed alongside this wrapper (the `uv`
    PyPI package is a pinned dependency). The engine uses it for fast
    package installs and falls back to pip when unavailable."""
    try:
        from uv import find_uv_bin

        return str(find_uv_bin())
    except Exception:
        return None


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    try:
        if args and args[0] == "mcp":
            return _run_mcp(args[1:])

        runtime = ensure_compatible_runtime()
        ui_dir = package_ui_dir()
        if ui_dir is not None:
            os.environ.setdefault("TINE_UI_DIR", str(ui_dir))
        os.environ.setdefault("TINE_WRAPPER_PYTHON", sys.executable)
        os.environ.setdefault("TINE_RUNTIME_ROOT", str(runtime.runtime_root))
        bundled_python = runtime.bundled_python_path
        if bundled_python is not None:
            os.environ.setdefault("TINE_BUNDLED_PYTHON", str(bundled_python))
            # Export the install-stage architecture pin so the engine refuses a
            # venv built with a mismatched interpreter. Best-effort: absent
            # descriptor leaves enforcement off.
            machine = bundled_platform_machine(runtime.runtime_root)
            if machine:
                os.environ.setdefault("TINE_PYTHON_PLATFORM", machine)
                if process_is_translated() and machine.lower() in ("x86_64", "amd64"):
                    print(
                        "warning: tine is running under Rosetta with an x86_64 "
                        "bundled Python on Apple Silicon. Reinstall the native "
                        "arm64 build for full performance.",
                        file=sys.stderr,
                    )
        uv_bin = _bundled_uv_path()
        if uv_bin is not None:
            os.environ.setdefault("TINE_UV_PATH", uv_bin)
        os.execv(str(runtime.binary_path), [str(runtime.binary_path), *args])
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 1


def _run_mcp(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        prog="tine mcp",
        description="MCP adapter commands exposed by the Python wrapper.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    serve = subparsers.add_parser(
        "serve",
        prog="tine mcp serve",
        help="Run the Tine MCP server over stdio using the canonical Tine API.",
    )
    serve.add_argument(
        "--api-url",
        default=None,
        help="Base URL for the running Tine API server. Defaults to the local operator API.",
    )
    serve.add_argument(
        "--api-key",
        default=None,
        help="Bearer API key for Tine Cloud. Defaults to TINE_API_KEY.",
    )
    print_config = subparsers.add_parser(
        "print-config",
        prog="tine mcp print-config",
        help="Print an MCP config document for the given host.",
    )
    print_config.add_argument("--host", default="claude", choices=mcp.SUPPORTED_MCP_HOSTS)
    print_config.add_argument("--name", default="tine")
    print_config.add_argument("--api-url", default=None)
    print_config.add_argument("--api-key", default=None)
    print_config.add_argument("--command", dest="command_path", default=None)
    register = subparsers.add_parser(
        "register",
        prog="tine mcp register",
        help="Register the Tine MCP config with a host config file.",
    )
    register.add_argument("--host", default="claude", choices=mcp.SUPPORTED_MCP_HOSTS)
    register.add_argument("--name", default="tine")
    register.add_argument("--api-url", default=None)
    register.add_argument("--api-key", default=None)
    register.add_argument("--command", dest="command_path", default=None)
    register.add_argument("--config-path", default=None)
    parsed = parser.parse_args(argv)
    if parsed.command == "serve":
        mcp_args: list[str] = []
        if parsed.api_url:
            mcp_args.extend(["--api-url", parsed.api_url])
        if parsed.api_key:
            mcp_args.extend(["--api-key", parsed.api_key])
        return mcp.main(mcp_args, prog="tine mcp serve")
    if parsed.command == "print-config":
        document = mcp.build_config_document(
            host=parsed.host,
            name=parsed.name,
            api_url=parsed.api_url,
            api_key=parsed.api_key,
            command_path=parsed.command_path,
        )
        print(json.dumps(document, indent=2))
        return 0
    if parsed.command == "register":
        document = mcp.build_config_document(
            host=parsed.host,
            name=parsed.name,
            api_url=parsed.api_url,
            api_key=parsed.api_key,
            command_path=parsed.command_path,
        )
        path = mcp.register_config(
            host=parsed.host,
            document=document,
            name=parsed.name,
            config_path=parsed.config_path,
        )
        print(f"Registered MCP server '{parsed.name}' in {path}")
        return 0
    return 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
