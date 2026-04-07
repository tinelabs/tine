from __future__ import annotations

import argparse
import json
import os
import platform
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from importlib import metadata
from pathlib import Path
from typing import Any

from .api_client import TineApiClient

MCP_PROTOCOL_VERSION = "2024-11-05"
SERVER_NAME = "tine"
SUPPORTED_MCP_HOSTS = ("claude", "cursor", "vscode", "generic")

# Keep these aligned with crates/tine-env/src/environment.rs.
TINE_REQUIRED_PACKAGES = ("ipykernel", "cloudpickle")
TINE_DEFAULT_PACKAGES = (
    "pyarrow>=14",
    "numpy>=1.26",
    "pandas>=2.1",
    "polars>=0.20",
    "scipy>=1.12",
    "scikit-learn>=1.4",
    "matplotlib>=3.8",
    "seaborn>=0.13",
    "tqdm>=4.66",
    "requests>=2.31",
    "pillow>=10",
)


def _server_version() -> str:
    try:
        return metadata.version("tine")
    except metadata.PackageNotFoundError:
        return "0.0.0-dev"


@dataclass
class ToolDef:
    name: str
    description: str
    input_schema: dict[str, Any]


@dataclass
class ToolResult:
    content: list[dict[str, str]]
    is_error: bool


class McpServer:
    def __init__(self, api_url: str) -> None:
        self.api = TineApiClient(api_url)

    def list_tools(self) -> list[ToolDef]:
        return [
            _tool(
                "list_experiment_trees",
                "List all experiment trees in the workspace.",
                {"type": "object", "properties": {}},
            ),
            _tool(
                "create_experiment",
                "Create a new experiment tree with a default main branch. You can optionally populate the root Cell 1 using either a full `first_cell` object or lightweight authoring fields such as `source`, `language`, `cell_name`, `outputs`, `cache`, and `timeout_secs`. The response includes agent context with the experiment's current package list and guidance to use `!` in a setup cell for any extra packages it needs.",
                {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "project_id": {"type": "string"},
                        "first_cell": {
                            "type": "object",
                            "description": "Full or partial cell object used to populate the root Cell 1. Provide this OR the lightweight authoring fields.",
                        },
                        "id": {
                            "type": "string",
                            "description": "Optional stable id in lightweight mode. Ignored for create_experiment because the root cell id is fixed to the existing Cell 1.",
                        },
                        "source": {
                            "type": "string",
                            "description": "Root Cell 1 source code for lightweight authoring.",
                        },
                        "language": {"type": "string"},
                        "cell_name": {"type": "string"},
                        "upstream": {"type": "array", "items": {"type": "string"}},
                        "outputs": {"type": "array", "items": {"type": "string"}},
                        "cache": {"type": "boolean"},
                        "timeout_secs": {"type": "integer"},
                    },
                    "required": ["name"],
                },
            ),
            _tool(
                "save_experiment",
                "Save a full experiment tree definition back through the API.",
                {
                    "type": "object",
                    "properties": {"definition": {"type": "object"}},
                    "required": ["definition"],
                },
            ),
            _tool(
                "get_experiment",
                "Get an experiment tree definition by ID. The response includes agent context with the experiment's current package list and guidance to use `!` in a setup cell for any extra packages it needs.",
                {
                    "type": "object",
                    "properties": {"experiment_id": {"type": "string"}},
                    "required": ["experiment_id"],
                },
            ),
            _tool(
                "get_experiment_summary",
                "Get a concise experiment summary with branch and cell metadata for quick state inspection.",
                {
                    "type": "object",
                    "properties": {"experiment_id": {"type": "string"}},
                    "required": ["experiment_id"],
                },
            ),
            _tool(
                "rename_experiment",
                "Rename an experiment tree.",
                {
                    "type": "object",
                    "properties": {
                        "experiment_id": {"type": "string"},
                        "name": {"type": "string"},
                    },
                    "required": ["experiment_id", "name"],
                },
            ),
            _tool(
                "delete_experiment",
                "Delete an experiment tree.",
                {
                    "type": "object",
                    "properties": {"experiment_id": {"type": "string"}},
                    "required": ["experiment_id"],
                },
            ),
            _tool(
                "create_branch",
                "Create a branch in an experiment tree from a parent branch and branch point cell. You can supply the first cell using either a full `first_cell` object or lightweight authoring fields such as `source`, `language`, `cell_name`, `id`, `upstream`, `outputs`, `cache`, `timeout_secs`. The MCP adapter expands lightweight inputs into the full backend cell shape and fills server-owned defaults like `tree_id` and placeholder `branch_id`. If omitted entirely, Tine creates an empty Python starter cell so the agent can begin writing immediately.",
                {
                    "type": "object",
                    "properties": {
                        "experiment_id": {"type": "string"},
                        "parent_branch_id": {"type": "string"},
                        "name": {"type": "string"},
                        "branch_point_cell_id": {"type": "string"},
                        "first_cell": {
                            "type": "object",
                            "description": "Full or partial cell object. Provide this OR the lightweight authoring fields (source, etc.). The adapter fills missing backend-required fields such as `tree_id`, `branch_id`, `upstream_cell_ids`, and runtime defaults.",
                        },
                        "id": {
                            "type": "string",
                            "description": "Optional stable id for lightweight authoring. If omitted, Tine generates one.",
                        },
                        "source": {
                            "type": "string",
                            "description": "Cell source code for lightweight authoring. Provide this OR `first_cell`. If omitted, Tine creates an empty starter cell.",
                        },
                        "language": {"type": "string"},
                        "cell_name": {"type": "string"},
                        "upstream": {"type": "array", "items": {"type": "string"}},
                        "outputs": {"type": "array", "items": {"type": "string"}},
                        "cache": {"type": "boolean"},
                        "timeout_secs": {"type": "integer"},
                    },
                    "required": ["experiment_id", "parent_branch_id", "name", "branch_point_cell_id"],
                },
            ),
            _tool(
                "add_cell",
                "Add a cell to a branch. You can supply the cell using either a full `cell` object or lightweight authoring fields such as `source`, `language`, `cell_name`, `id`, `upstream`, `outputs`, `cache`, and `timeout_secs`.",
                {
                    "type": "object",
                    "properties": {
                        "experiment_id": {"type": "string"},
                        "branch_id": {"type": "string", "default": "main"},
                        "after_cell_id": {"type": "string"},
                        "cell": {
                            "type": "object",
                            "description": "Full or partial cell object. Provide this OR the lightweight authoring fields.",
                        },
                        "id": {"type": "string"},
                        "source": {"type": "string"},
                        "language": {"type": "string"},
                        "cell_name": {"type": "string"},
                        "upstream": {"type": "array", "items": {"type": "string"}},
                        "outputs": {"type": "array", "items": {"type": "string"}},
                        "cache": {"type": "boolean"},
                        "timeout_secs": {"type": "integer"},
                    },
                    "required": ["experiment_id"],
                },
            ),
            _tool(
                "update_cell",
                "Update the source code of one cell in a branch. `branch_id` defaults to `main` when omitted.",
                {
                    "type": "object",
                    "properties": {
                        "experiment_id": {"type": "string"},
                        "branch_id": {"type": "string", "default": "main"},
                        "cell_id": {"type": "string"},
                        "source": {"type": "string"},
                    },
                    "required": ["experiment_id", "cell_id", "source"],
                },
            ),
            _tool(
                "move_cell",
                "Move one cell up or down within a branch. `branch_id` defaults to `main` when omitted.",
                {
                    "type": "object",
                    "properties": {
                        "experiment_id": {"type": "string"},
                        "branch_id": {"type": "string", "default": "main"},
                        "cell_id": {"type": "string"},
                        "direction": {"type": "string", "enum": ["up", "down"]},
                    },
                    "required": ["experiment_id", "cell_id", "direction"],
                },
            ),
            _tool(
                "delete_cell",
                "Delete one cell from a branch. `branch_id` defaults to `main` when omitted.",
                {
                    "type": "object",
                    "properties": {
                        "experiment_id": {"type": "string"},
                        "branch_id": {"type": "string", "default": "main"},
                        "cell_id": {"type": "string"},
                    },
                    "required": ["experiment_id", "cell_id"],
                },
            ),
            _tool(
                "delete_branch",
                "Delete one non-main branch from an experiment tree.",
                {
                    "type": "object",
                    "properties": {
                        "experiment_id": {"type": "string"},
                        "branch_id": {"type": "string"},
                    },
                    "required": ["experiment_id", "branch_id"],
                },
            ),
            _tool(
                "inspect_cell",
                "Inspect one branch target without mutating runtime state. Returns lineage, effective path, replay plan, and current runtime state. `branch_id` defaults to `main` when omitted.",
                {
                    "type": "object",
                    "properties": {
                        "experiment_id": {"type": "string"},
                        "branch_id": {"type": "string", "default": "main"},
                        "cell_id": {"type": "string"},
                    },
                    "required": ["experiment_id", "cell_id"],
                },
            ),
            _tool(
                "execute_branch",
                "Execute one branch in an experiment tree and return the accepted execution envelope, including execution id, submission status, phase, and queue position when available. `branch_id` defaults to `main` when omitted.",
                {
                    "type": "object",
                    "properties": {
                        "experiment_id": {"type": "string"},
                        "branch_id": {"type": "string", "default": "main"},
                    },
                    "required": ["experiment_id"],
                },
            ),
            _tool(
                "execute_cell",
                "Execute one cell in a branch and return the accepted execution envelope, including execution id, submission status, phase, and queue position when available. `branch_id` defaults to `main` when omitted.",
                {
                    "type": "object",
                    "properties": {
                        "experiment_id": {"type": "string"},
                        "branch_id": {"type": "string", "default": "main"},
                        "cell_id": {"type": "string"},
                    },
                    "required": ["experiment_id", "cell_id"],
                },
            ),
            _tool(
                "execute_all_branches",
                "Execute all branches in one experiment tree and return accepted execution envelopes for each submitted branch.",
                {
                    "type": "object",
                    "properties": {"experiment_id": {"type": "string"}},
                    "required": ["experiment_id"],
                },
            ),
            _tool(
                "cancel",
                "Cancel a running execution.",
                {
                    "type": "object",
                    "properties": {"execution_id": {"type": "string"}},
                    "required": ["execution_id"],
                },
            ),
            _tool(
                "status",
                "Get durable execution status, lifecycle phase, and node progress for an execution id.",
                {
                    "type": "object",
                    "properties": {"execution_id": {"type": "string"}},
                    "required": ["execution_id"],
                },
            ),
            _tool(
                "wait_for_execution",
                "Wait for an execution to reach a terminal lifecycle state by polling the status API. Returns the latest status object plus whether the wait itself timed out.",
                {
                    "type": "object",
                    "properties": {
                        "execution_id": {"type": "string"},
                        "timeout_secs": {"type": "integer", "default": 30},
                        "poll_interval_ms": {"type": "integer", "default": 500},
                    },
                    "required": ["execution_id"],
                },
            ),
            _tool(
                "logs",
                "Get logs for one cell in one experiment tree branch. `branch_id` defaults to `main` when omitted.",
                {
                    "type": "object",
                    "properties": {
                        "experiment_id": {"type": "string"},
                        "branch_id": {"type": "string", "default": "main"},
                        "cell_id": {"type": "string"},
                    },
                    "required": ["experiment_id", "cell_id"],
                },
            ),
            _tool(
                "create_project",
                "Create a project container for experiments.",
                {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "workspace_dir": {"type": "string"},
                        "description": {"type": "string"},
                    },
                    "required": ["name", "workspace_dir"],
                },
            ),
            _tool(
                "list_projects",
                "List all projects.",
                {"type": "object", "properties": {}},
            ),
            _tool(
                "get_project",
                "Get one project by id.",
                {
                    "type": "object",
                    "properties": {"project_id": {"type": "string"}},
                    "required": ["project_id"],
                },
            ),
            _tool(
                "list_experiments",
                "List experiment trees that belong to a project.",
                {
                    "type": "object",
                    "properties": {"project_id": {"type": "string"}},
                    "required": ["project_id"],
                },
            ),
        ]

    def call_tool(self, name: str, args: dict[str, Any]) -> ToolResult:
        try:
            if name == "list_experiment_trees":
                return self._ok(self.api.list_experiment_trees())
            if name == "create_experiment":
                has_root_authoring = _has_root_cell_authoring_args(args)
                tree = self.api.create_experiment_tree(
                    _required_string(args, "name"),
                    args.get("project_id"),
                )
                if has_root_authoring:
                    tree = _populate_root_cell(tree, args)
                    tree = self.api.save_experiment_tree(tree)
                return self._ok(
                    _experiment_payload(
                        tree,
                        root_cell_was_seeded=has_root_authoring,
                    )
                )
            if name == "save_experiment":
                definition = _required_object(args, "definition")
                return self._ok(_experiment_payload(self.api.save_experiment_tree(definition)))
            if name == "get_experiment":
                return self._ok(
                    _experiment_payload(
                        self.api.get_experiment_tree(
                            _required_string(args, "experiment_id")
                        )
                    )
                )
            if name == "get_experiment_summary":
                return self._ok(
                    _experiment_summary(
                        self.api.get_experiment_tree(
                            _required_string(args, "experiment_id")
                        )
                    )
                )
            if name == "rename_experiment":
                experiment_id = _required_string(args, "experiment_id")
                self.api.rename_experiment_tree(
                    experiment_id,
                    _required_string(args, "name"),
                )
                return self._text_ok(f"Experiment {experiment_id} renamed")
            if name == "delete_experiment":
                experiment_id = _required_string(args, "experiment_id")
                self.api.delete_experiment_tree(experiment_id)
                return self._text_ok(f"Experiment {experiment_id} deleted")
            if name == "create_branch":
                experiment_id = _required_string(args, "experiment_id")
                branch_id = self.api.create_branch_in_experiment_tree(
                    experiment_id,
                    _required_string(args, "parent_branch_id"),
                    _required_string(args, "name"),
                    _required_string(args, "branch_point_cell_id"),
                    _cell_payload(
                        args,
                        object_key="first_cell",
                        experiment_id=experiment_id,
                    ),
                )
                return self._text_ok(f"Branch created: {branch_id}")
            if name == "add_cell":
                experiment_id = _required_string(args, "experiment_id")
                branch_id = _optional_string(args, "branch_id") or "main"
                self.api.add_cell_to_experiment_tree_branch(
                    experiment_id,
                    branch_id,
                    _cell_payload(
                        args,
                        object_key="cell",
                        experiment_id=experiment_id,
                        branch_id=branch_id,
                    ),
                    _optional_string(args, "after_cell_id"),
                )
                return self._text_ok(f"Cell added to branch {branch_id}")
            if name == "update_cell":
                experiment_id = _required_string(args, "experiment_id")
                branch_id = _optional_string(args, "branch_id") or "main"
                cell_id = _required_string(args, "cell_id")
                self.api.update_cell_code_in_experiment_tree_branch(
                    experiment_id,
                    branch_id,
                    cell_id,
                    _required_string(args, "source"),
                )
                return self._text_ok(f"Cell {cell_id} updated in branch {branch_id}")
            if name == "move_cell":
                experiment_id = _required_string(args, "experiment_id")
                branch_id = _optional_string(args, "branch_id") or "main"
                cell_id = _required_string(args, "cell_id")
                direction = _required_string(args, "direction")
                if direction not in {"up", "down"}:
                    raise RuntimeError("invalid field 'direction': expected 'up' or 'down'")
                self.api.move_cell_in_experiment_tree_branch(
                    experiment_id,
                    branch_id,
                    cell_id,
                    direction,
                )
                return self._text_ok(f"Cell {cell_id} moved {direction} in branch {branch_id}")
            if name == "delete_cell":
                experiment_id = _required_string(args, "experiment_id")
                branch_id = _optional_string(args, "branch_id") or "main"
                cell_id = _required_string(args, "cell_id")
                self.api.delete_cell_from_experiment_tree_branch(
                    experiment_id,
                    branch_id,
                    cell_id,
                )
                return self._text_ok(f"Cell {cell_id} deleted from branch {branch_id}")
            if name == "delete_branch":
                experiment_id = _required_string(args, "experiment_id")
                branch_id = _required_string(args, "branch_id")
                self.api.delete_experiment_tree_branch(experiment_id, branch_id)
                return self._text_ok(f"Branch {branch_id} deleted")
            if name == "inspect_cell":
                experiment_id = _required_string(args, "experiment_id")
                branch_id = _optional_string(args, "branch_id") or "main"
                return self._ok(
                    self.api.inspect_cell_in_experiment_tree_branch(
                        experiment_id,
                        branch_id,
                        _required_string(args, "cell_id"),
                    )
                )
            if name == "execute_branch":
                experiment_id = _required_string(args, "experiment_id")
                branch_id = _optional_string(args, "branch_id") or "main"
                execution = self.api.execute_branch_in_experiment_tree(
                    experiment_id, branch_id
                )
                return self._ok(execution)
            if name == "execute_cell":
                experiment_id = _required_string(args, "experiment_id")
                branch_id = _optional_string(args, "branch_id") or "main"
                cell_id = _required_string(args, "cell_id")
                execution = self.api.execute_cell_in_experiment_tree_branch(
                    experiment_id, branch_id, cell_id
                )
                return self._ok(execution)
            if name == "execute_all_branches":
                experiment_id = _required_string(args, "experiment_id")
                executions = self.api.execute_all_branches_in_experiment_tree(experiment_id)
                return self._ok(executions)
            if name == "cancel":
                execution_id = _required_string(args, "execution_id")
                self.api.cancel(execution_id)
                return self._text_ok(
                    f"Cancellation requested for execution {execution_id}"
                )
            if name == "status":
                return self._ok(self.api.status(_required_string(args, "execution_id")))
            if name == "wait_for_execution":
                execution_id = _required_string(args, "execution_id")
                timeout_secs = _optional_int(args, "timeout_secs")
                poll_interval_ms = _optional_int(args, "poll_interval_ms")
                timeout_secs = 30 if timeout_secs is None else timeout_secs
                poll_interval_ms = 500 if poll_interval_ms is None else poll_interval_ms
                deadline = time.monotonic() + timeout_secs
                def _is_terminal_execution_status(status: dict[str, Any]) -> bool:
                    lifecycle = str(status.get("status") or "").strip().lower()
                    if lifecycle in {"completed", "failed", "cancelled", "timed_out", "rejected"}:
                        return True
                    return status.get("finished_at") is not None
                while True:
                    status = self.api.status(execution_id)
                    if _is_terminal_execution_status(status):
                        return self._ok({"timed_out": False, "status": status})
                    if time.monotonic() >= deadline:
                        return self._ok({"timed_out": True, "status": status})
                    time.sleep(max(poll_interval_ms, 50) / 1000)
            if name == "logs":
                return self._ok(
                    self.api.logs_for_tree_cell(
                        _required_string(args, "experiment_id"),
                        _optional_string(args, "branch_id") or "main",
                        _required_string(args, "cell_id"),
                    )
                )
            if name == "create_project":
                project_id = self.api.create_project(
                    _required_string(args, "name"),
                    _required_string(args, "workspace_dir"),
                    _optional_string(args, "description"),
                )
                return self._text_ok(f"Project created: {project_id}")
            if name == "list_projects":
                return self._ok(self.api.list_projects())
            if name == "get_project":
                return self._ok(self.api.get_project(_required_string(args, "project_id")))
            if name == "list_experiments":
                return self._ok(
                    self.api.list_experiments(_required_string(args, "project_id"))
                )
            raise RuntimeError(f"unknown tool: {name}")
        except Exception as exc:
            return ToolResult(
                content=[{"type": "text", "text": str(exc)}],
                is_error=True,
            )

    @staticmethod
    def _ok(payload: Any) -> ToolResult:
        return ToolResult(
            content=[{"type": "text", "text": json.dumps(payload, indent=2)}],
            is_error=False,
        )

    @staticmethod
    def _text_ok(text: str) -> ToolResult:
        return ToolResult(content=[{"type": "text", "text": text}], is_error=False)


def run_stdio(server: McpServer) -> int:
    for raw_line in sys.stdin:
        line = raw_line.strip()
        if not line:
            continue
        try:
            request_obj = json.loads(line)
        except json.JSONDecodeError as exc:
            _write_response(
                {
                    "jsonrpc": "2.0",
                    "id": None,
                    "error": {"code": -32700, "message": f"Parse error: {exc}"},
                }
            )
            continue

        response = _handle_request(server, request_obj)
        if request_obj.get("id") is None:
            continue
        _write_response(response)
    return 0


def main(argv: list[str] | None = None, *, prog: str = "tine-mcp") -> int:
    parser = argparse.ArgumentParser(
        prog=prog,
        description="Run the Tine MCP server over stdio using the canonical Tine API.",
    )
    parser.add_argument(
        "--api-url",
        default=os.environ.get("TINE_API_URL", "http://127.0.0.1:9473"),
        help="Base URL for the running Tine API server.",
    )
    args = parser.parse_args(argv)
    return run_stdio(McpServer(args.api_url))


def build_config_document(
    *,
    host: str,
    name: str = "tine",
    api_url: str | None = None,
    command_path: str | None = None,
) -> dict[str, Any]:
    resolved_host = _normalize_host(host)
    command = command_path or "tine"
    server_entry = _build_server_entry(
        resolved_host, command, _build_command_args(api_url)
    )
    return {_mcp_servers_key(resolved_host): {name: server_entry}}


def resolve_config_path(host: str, explicit_path: str | None = None) -> Path:
    if explicit_path:
        return Path(explicit_path)

    resolved_host = _normalize_host(host)
    system = platform.system()
    home = Path.home()
    if resolved_host == "generic":
        return home / ".mcp" / "config.json"
    if system == "Darwin":
        base = home / "Library" / "Application Support"
        return {
            "claude": base / "Claude" / "claude_desktop_config.json",
            "cursor": base / "Cursor" / "User" / "mcp.json",
            "vscode": base / "Code" / "User" / "mcp.json",
        }[resolved_host]
    if system == "Windows":
        appdata = os.environ.get("APPDATA")
        if not appdata:
            raise RuntimeError("APPDATA is not set")
        base = Path(appdata)
        return {
            "claude": base / "Claude" / "claude_desktop_config.json",
            "cursor": base / "Cursor" / "User" / "mcp.json",
            "vscode": base / "Code" / "User" / "mcp.json",
        }[resolved_host]
    base = home / ".config"
    return {
        "claude": base / "Claude" / "claude_desktop_config.json",
        "cursor": base / "Cursor" / "User" / "mcp.json",
        "vscode": base / "Code" / "User" / "mcp.json",
    }[resolved_host]


def register_config(
    *,
    host: str,
    document: dict[str, Any],
    name: str = "tine",
    config_path: str | None = None,
) -> Path:
    resolved_host = _normalize_host(host)
    path = resolve_config_path(resolved_host, config_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.is_file():
        text = path.read_text()
        root = {} if not text.strip() else json.loads(text)
    else:
        root = {}
    if not isinstance(root, dict):
        raise RuntimeError("existing MCP config must be a JSON object")
    root_key = _mcp_servers_key(resolved_host)
    servers = root.setdefault(root_key, {})
    if not isinstance(servers, dict):
        raise RuntimeError(f"{root_key} must be a JSON object")
    new_server = document.get(root_key, {}).get(name)
    if not isinstance(new_server, dict):
        raise RuntimeError("generated MCP config is missing the requested server entry")
    servers[name] = new_server
    path.write_text(json.dumps(root, indent=2) + "\n")
    return path


def _handle_request(server: McpServer, request_obj: dict[str, Any]) -> dict[str, Any]:
    req_id = request_obj.get("id")
    method = request_obj.get("method", "")
    params = request_obj.get("params") or {}

    if method == "initialize":
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {
                "protocolVersion": MCP_PROTOCOL_VERSION,
                "capabilities": {"tools": {"listChanged": False}},
                "serverInfo": {"name": SERVER_NAME, "version": _server_version()},
            },
        }

    if method == "notifications/initialized":
        return {"jsonrpc": "2.0", "id": req_id, "result": None}

    if method == "ping":
        return {"jsonrpc": "2.0", "id": req_id, "result": {}}

    if method == "tools/list":
        tools = [
            {
                "name": tool.name,
                "description": tool.description,
                "inputSchema": tool.input_schema,
            }
            for tool in server.list_tools()
        ]
        return {"jsonrpc": "2.0", "id": req_id, "result": {"tools": tools}}

    if method == "tools/call":
        tool_name = params.get("name", "")
        arguments = params.get("arguments") or {}
        tool_result = server.call_tool(tool_name, arguments)
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {
                "content": tool_result.content,
                "isError": tool_result.is_error,
            },
        }

    return {
        "jsonrpc": "2.0",
        "id": req_id,
        "error": {"code": -32601, "message": f"Method not found: {method}"},
    }


def _tool(name: str, description: str, input_schema: dict[str, Any]) -> ToolDef:
    return ToolDef(name=name, description=description, input_schema=input_schema)


def _normalize_host(host: str) -> str:
    normalized = host.lower()
    if normalized not in SUPPORTED_MCP_HOSTS:
        raise RuntimeError(
            f"unsupported MCP host {host!r}; expected one of {', '.join(SUPPORTED_MCP_HOSTS)}"
        )
    return normalized


def _mcp_servers_key(host: str) -> str:
    return "servers" if host == "vscode" else "mcpServers"


def _build_command_args(api_url: str | None) -> list[str]:
    args = ["mcp", "serve"]
    if api_url:
        args.extend(["--api-url", api_url])
    return args


def _build_server_entry(host: str, command: str, args: list[str]) -> dict[str, Any]:
    if host == "vscode":
        return {"type": "stdio", "command": command, "args": args}
    return {"command": command, "args": args}


def _required_string(payload: dict[str, Any], key: str) -> str:
    if key not in payload:
        raise RuntimeError(f"missing required field '{key}': expected a non-empty string")
    value = payload.get(key)
    if not isinstance(value, str):
        raise RuntimeError(
            f"invalid field '{key}': expected a string, got {_value_kind(value)}"
        )
    if not value:
        raise RuntimeError(f"invalid field '{key}': expected a non-empty string")
    return value


def _optional_string(payload: dict[str, Any], key: str) -> str | None:
    value = payload.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise RuntimeError(
            f"invalid field '{key}': expected a string when provided, got {_value_kind(value)}"
        )
    if not value:
        raise RuntimeError(
            f"invalid field '{key}': expected a non-empty string when provided"
        )
    return value


def _required_object(payload: dict[str, Any], key: str) -> dict[str, Any]:
    if key not in payload:
        raise RuntimeError(f"missing required field '{key}': expected an object")
    value = payload.get(key)
    if not isinstance(value, dict):
        raise RuntimeError(
            f"invalid field '{key}': expected an object, got {_value_kind(value)}"
        )
    return value


def _optional_string_list(payload: dict[str, Any], key: str) -> list[str]:
    value = payload.get(key)
    if value is None:
        return []
    if not isinstance(value, list):
        raise RuntimeError(
            f"invalid field '{key}': expected an array of strings, got {_value_kind(value)}"
        )
    result: list[str] = []
    for index, item in enumerate(value):
        if not isinstance(item, str):
            raise RuntimeError(
                f"invalid field '{key}[{index}]': expected a string, got {_value_kind(item)}"
            )
        result.append(item)
    return result


def _cell_payload(
    payload: dict[str, Any], *, object_key: str, experiment_id: str, branch_id: str = "ignored"
) -> dict[str, Any]:
    explicit = payload.get(object_key)
    if explicit is not None:
        if not isinstance(explicit, dict):
            raise RuntimeError(
                f"invalid field '{object_key}': expected an object, got {_value_kind(explicit)}"
            )
        return _normalized_cell_payload(
            explicit,
            experiment_id=experiment_id,
            fallback_name=_optional_string(payload, "cell_name")
            or _optional_string(payload, "name"),
        )

    cell_id = _optional_string(payload, "id") or f"cell_{uuid.uuid4().hex}"
    source = payload.get("source", "")
    if source is None:
        source = ""
    if not isinstance(source, str):
        raise RuntimeError(
            f"invalid field 'source': expected a string when provided, got {_value_kind(source)}"
        )
    return _normalized_cell_payload(
        {
        "id": cell_id,
        "name": _optional_string(payload, "cell_name")
        or _optional_string(payload, "name")
        or cell_id,
        "code": {
            "source": source,
            "language": _optional_string(payload, "language") or "python",
        },
        "upstream_cell_ids": _optional_string_list(payload, "upstream"),
        "declared_outputs": _optional_string_list(payload, "outputs"),
        "cache": payload.get("cache", True),
        "timeout_secs": payload.get("timeout_secs"),
        },
        experiment_id=experiment_id,
        branch_id=branch_id,
    )


def _has_root_cell_authoring_args(payload: dict[str, Any]) -> bool:
    return any(
        key in payload
        for key in (
            "first_cell",
            "id",
            "source",
            "language",
            "cell_name",
            "upstream",
            "outputs",
            "cache",
            "timeout_secs",
        )
    )


def _populate_root_cell(tree: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    tree_id = _required_string(tree, "id")
    cells = tree.get("cells")
    if not isinstance(cells, list) or not cells:
        raise RuntimeError("create_experiment response missing root cell")

    first_cell = cells[0]
    if not isinstance(first_cell, dict):
        raise RuntimeError("create_experiment response contained an invalid root cell")

    root_branch_id = _required_string(tree, "root_branch_id")
    normalized = _cell_payload(
        payload,
        object_key="first_cell",
        experiment_id=tree_id,
        branch_id=root_branch_id,
    )
    normalized["id"] = _required_string(first_cell, "id")
    normalized["tree_id"] = tree_id
    normalized["branch_id"] = root_branch_id
    normalized["name"] = (
        _optional_string(payload, "cell_name")
        or _optional_string(normalized, "name")
        or _optional_string(first_cell, "name")
        or "Cell 1"
    )
    cells[0] = normalized
    return tree


def _normalized_cell_payload(
    cell: dict[str, Any], *, experiment_id: str, branch_id: str = "ignored", fallback_name: str | None = None
) -> dict[str, Any]:
    cell_id = _optional_string(cell, "id") or f"cell_{uuid.uuid4().hex}"
    code = cell.get("code")
    if code is None:
        code = {}
    if not isinstance(code, dict):
        raise RuntimeError(
            f"invalid field 'code': expected an object when provided, got {_value_kind(code)}"
        )

    source = code.get("source", cell.get("source", ""))
    if source is None:
        source = ""
    if not isinstance(source, str):
        raise RuntimeError(
            f"invalid field 'source': expected a string when provided, got {_value_kind(source)}"
        )

    language = code.get("language", cell.get("language", "python"))
    if language is None:
        language = "python"
    if not isinstance(language, str) or not language:
        raise RuntimeError(
            f"invalid field 'language': expected a non-empty string when provided, got {_value_kind(language)}"
        )

    upstream = _optional_string_list(cell, "upstream_cell_ids")
    if not upstream:
        upstream = _optional_string_list(cell, "upstream")

    outputs = _optional_string_list(cell, "declared_outputs")
    if not outputs:
        outputs = _optional_string_list(cell, "outputs")

    cache = cell.get("cache", True)
    if not isinstance(cache, bool):
        raise RuntimeError(
            f"invalid field 'cache': expected a boolean when provided, got {_value_kind(cache)}"
        )

    map_concurrency = cell.get("map_concurrency")
    if map_concurrency is not None:
        if (
            not isinstance(map_concurrency, int)
            or isinstance(map_concurrency, bool)
            or map_concurrency < 0
        ):
            raise RuntimeError(
                "invalid field 'map_concurrency': expected a non-negative integer when provided"
            )

    map_over = cell.get("map_over")
    if map_over is not None and not isinstance(map_over, str):
        raise RuntimeError(
            f"invalid field 'map_over': expected a string when provided, got {_value_kind(map_over)}"
        )

    tags = cell.get("tags")
    if tags is None:
        tags = {}
    if not isinstance(tags, dict) or not all(
        isinstance(key, str) and isinstance(value, str) for key, value in tags.items()
    ):
        raise RuntimeError(
            "invalid field 'tags': expected an object with string keys and string values"
        )

    return {
        "id": cell_id,
        "tree_id": _optional_string(cell, "tree_id") or experiment_id,
        "branch_id": _optional_string(cell, "branch_id") or branch_id,
        "name": _optional_string(cell, "name") or fallback_name or cell_id,
        "code": {"source": source, "language": language},
        "upstream_cell_ids": upstream,
        "declared_outputs": outputs,
        "cache": cache,
        "map_over": map_over,
        "map_concurrency": map_concurrency,
        "timeout_secs": _optional_int(cell, "timeout_secs"),
        "tags": tags,
        "revision_id": cell.get("revision_id"),
        "state": _optional_string(cell, "state") or "clean",
    }


def _optional_int(payload: dict[str, Any], key: str) -> int | None:
    value = payload.get(key)
    if value is None:
        return None
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise RuntimeError(
            f"invalid field '{key}': expected a non-negative integer, got {_value_kind(value)}"
        )
    return value


def _value_kind(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return type(value).__name__


def _write_response(response_obj: dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(response_obj) + "\n")
    sys.stdout.flush()


def _experiment_payload(
    tree: dict[str, Any], *, root_cell_was_seeded: bool | None = None
) -> dict[str, Any]:
    environment = tree.get("environment")
    dependencies = []
    if isinstance(environment, dict):
        raw_dependencies = environment.get("dependencies")
        if isinstance(raw_dependencies, list):
            dependencies = [dep for dep in raw_dependencies if isinstance(dep, str)]

    always_available = [*TINE_REQUIRED_PACKAGES, *TINE_DEFAULT_PACKAGES]
    effective_packages = _merge_package_specs(always_available, dependencies)
    root_cell_id = _root_cell_id(tree)
    authoring_context: dict[str, Any] = {
        "root_cell_id": root_cell_id,
        "preferred_first_step": (
            "Prefer using the root Cell 1 for initial setup, imports, and dataset loading "
            "before adding more cells or branches."
        ),
    }
    if root_cell_was_seeded is False and root_cell_id:
        authoring_context["next_step_hint"] = (
            f"This experiment was created with an empty root cell (`{root_cell_id}`). "
            f"Prefer updating `{root_cell_id}` for the initial setup or EDA cell instead of "
            "immediately adding a second cell."
        )

    return {
        "experiment": tree,
        "agent_context": {
            "environment": {
                "declared_dependencies": dependencies,
                "required_runtime_packages": list(TINE_REQUIRED_PACKAGES),
                "always_available_packages": list(TINE_DEFAULT_PACKAGES),
                "effective_packages": effective_packages,
                "guidance": (
                    "Every Tine experiment already includes the runtime packages in "
                    "`required_runtime_packages` and the common data-science stack in "
                    "`always_available_packages`. `declared_dependencies` are additional "
                    "packages explicitly requested for this experiment. Do not add inline "
                    "package-install helpers for those built-ins. If you need something not "
                    "listed in `effective_packages`, add a setup cell that uses `!` to "
                    "install it before importing."
                ),
            },
            "authoring": authoring_context,
        },
    }


def _root_cell_id(tree: dict[str, Any]) -> str | None:
    cells = tree.get("cells")
    if isinstance(cells, list) and cells:
        first_cell = cells[0]
        if isinstance(first_cell, dict):
            cell_id = _optional_string(first_cell, "id")
            if cell_id:
                return cell_id
    return "cell_1"


def _merge_package_specs(base: list[str], extra: list[str]) -> list[str]:
    merged: dict[str, str] = {}
    for spec in base:
        merged[_package_name(spec)] = spec
    for spec in extra:
        merged[_package_name(spec)] = spec
    return [merged[name] for name in sorted(merged)]


def _package_name(spec: str) -> str:
    stop = len(spec)
    for marker in ("<", ">", "=", "!", "~"):
        marker_index = spec.find(marker)
        if marker_index != -1:
            stop = min(stop, marker_index)
    return spec[:stop].strip()


def _experiment_summary(tree: dict[str, Any]) -> dict[str, Any]:
    branches = tree.get("branches")
    cells = tree.get("cells")
    safe_branches = branches if isinstance(branches, list) else []
    safe_cells = cells if isinstance(cells, list) else []

    return {
        "experiment_id": tree.get("id"),
        "name": tree.get("name"),
        "project_id": tree.get("project_id"),
        "root_branch_id": tree.get("root_branch_id"),
        "branch_count": len(safe_branches),
        "cell_count": len(safe_cells),
        "dependencies": (
            tree.get("environment", {}).get("dependencies", [])
            if isinstance(tree.get("environment"), dict)
            else []
        ),
        "branches": [
            {
                "id": branch.get("id"),
                "name": branch.get("name"),
                "parent_branch_id": branch.get("parent_branch_id"),
                "branch_point_cell_id": branch.get("branch_point_cell_id"),
                "cell_order": branch.get("cell_order", []),
                "cell_count": len(branch.get("cell_order", []))
                if isinstance(branch.get("cell_order"), list)
                else 0,
            }
            for branch in safe_branches
            if isinstance(branch, dict)
        ],
        "cells": [
            {
                "id": cell.get("id"),
                "branch_id": cell.get("branch_id"),
                "name": cell.get("name"),
                "upstream_cell_ids": cell.get("upstream_cell_ids", []),
                "declared_outputs": cell.get("declared_outputs", []),
                "cache": cell.get("cache"),
                "timeout_secs": cell.get("timeout_secs"),
                "state": cell.get("state"),
            }
            for cell in safe_cells
            if isinstance(cell, dict)
        ],
    }


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
