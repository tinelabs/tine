from __future__ import annotations

import argparse
import json
import os
import sys

from . import mcp
from .runtime import ensure_compatible_binary, package_ui_dir


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    try:
        if args and args[0] == "mcp":
            return _run_mcp(args[1:])

        binary_path = ensure_compatible_binary()
        ui_dir = package_ui_dir()
        if ui_dir is not None:
            os.environ.setdefault("TINE_UI_DIR", str(ui_dir))
        os.execv(str(binary_path), [str(binary_path), *args])
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
    print_config = subparsers.add_parser(
        "print-config",
        prog="tine mcp print-config",
        help="Print an MCP config document for the given host.",
    )
    print_config.add_argument("--host", default="claude", choices=mcp.SUPPORTED_MCP_HOSTS)
    print_config.add_argument("--name", default="tine")
    print_config.add_argument("--api-url", default=None)
    print_config.add_argument("--command", dest="command_path", default=None)
    register = subparsers.add_parser(
        "register",
        prog="tine mcp register",
        help="Register the Tine MCP config with a host config file.",
    )
    register.add_argument("--host", default="claude", choices=mcp.SUPPORTED_MCP_HOSTS)
    register.add_argument("--name", default="tine")
    register.add_argument("--api-url", default=None)
    register.add_argument("--command", dest="command_path", default=None)
    register.add_argument("--config-path", default=None)
    parsed = parser.parse_args(argv)
    if parsed.command == "serve":
        mcp_args: list[str] = []
        if parsed.api_url:
            mcp_args.extend(["--api-url", parsed.api_url])
        return mcp.main(mcp_args, prog="tine mcp serve")
    if parsed.command == "print-config":
        document = mcp.build_config_document(
            host=parsed.host,
            name=parsed.name,
            api_url=parsed.api_url,
            command_path=parsed.command_path,
        )
        print(json.dumps(document, indent=2))
        return 0
    if parsed.command == "register":
        document = mcp.build_config_document(
            host=parsed.host,
            name=parsed.name,
            api_url=parsed.api_url,
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
