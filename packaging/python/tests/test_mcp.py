from __future__ import annotations

import json
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest import mock

import tine.mcp as mcp_module
from tine.mcp import (
    McpServer,
    _handle_request,
    build_config_document,
    register_config,
    resolve_config_path,
)


class _Handler(BaseHTTPRequestHandler):
    last_authorization: str | None = None
    last_create_project_payload: dict[str, object] | None = None
    last_create_branch_payload: dict[str, object] | None = None
    last_add_cell_payload: dict[str, object] | None = None
    last_update_cell_payload: dict[str, object] | None = None
    last_move_cell_payload: dict[str, object] | None = None
    deleted_path: str | None = None
    last_execute_payload: dict[str, object] | None = None
    cancel_requested = False
    restart_requested = False
    wait_status_requests = 0

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/api/experiment-trees":
            self._json(200, [{"id": "tree_1", "name": "demo"}])
            return
        if self.path == "/api/experiment-trees/tree_1":
            self._json(
                200,
                {
                    "id": "tree_1",
                    "name": "demo",
                    "environment": {"dependencies": ["pandas", "matplotlib"]},
                },
            )
            return
        if self.path == "/api/experiment-trees/tree_1/branches/main/cells/cell_1/inspect":
            self._json(
                200,
                {
                    "tree_id": "tree_1",
                    "branch_id": "main",
                    "target_cell_id": "cell_1",
                    "lineage": ["main"],
                    "path_cell_order": ["cell_1"],
                    "topo_order": ["cell_1"],
                    "has_live_kernel": False,
                    "current_runtime_state": None,
                    "shared_prefix_cell_ids": [],
                    "divergence_cell_id": "cell_1",
                    "replay_from_idx": 0,
                    "replay_cell_ids": [],
                    "replay_prefix_before_target": [],
                },
            )
            return
        if self.path == "/api/experiment-trees/tree_1/inspect-kernel":
            self._json(
                200,
                {
                    "tree_id": "tree_1",
                    "has_live_kernel": type(self).restart_requested,
                    "tree_kernel_state": "needs_replay" if type(self).restart_requested else "ready",
                    "replay_required": type(self).restart_requested,
                    "active_branch_id": "main",
                    "runtime_epoch": 4,
                },
            )
            return
        if self.path == "/api/executions/exec_branch_1":
            self._json(
                200,
                {
                    "execution_id": "exec_branch_1",
                    "status": "completed",
                    "phase": "completed",
                    "cancellation_requested_at": None,
                    "node_statuses": {"cell_1": "completed"},
                    "finished_at": "2026-04-07T10:15:05Z",
                },
            )
            return
        if self.path == "/api/executions/exec_branch_1/results":
            self._json(
                200,
                {
                    "status": {
                        "execution_id": "exec_branch_1",
                        "status": "completed",
                        "phase": "completed",
                        "node_statuses": {"cell_1": "completed"},
                        "finished_at": "2026-04-07T10:15:05Z",
                    },
                    "node_logs": {
                        "cell_1": {
                            "stdout": "line1\nline2\n",
                            "stderr": "",
                            "outputs": [
                                {"data": {"image/png": "A" * 9000}, "metadata": {}}
                            ],
                            "error": None,
                            "duration_ms": 5,
                            "metrics": {},
                        }
                    },
                },
            )
            return
        if self.path == "/api/executions/exec_logs_error":
            self._json(
                200,
                {
                    "execution_id": "exec_logs_error",
                    "status": "completed",
                    "phase": "completed",
                    "cancellation_requested_at": None,
                    "node_statuses": {"cell_1": "completed"},
                    "finished_at": "2026-04-07T10:15:05Z",
                },
            )
            return
        if self.path == "/api/executions/exec_logs_error/results":
            self._json(500, {"error": "results store unavailable", "code": "database"})
            return
        if self.path == "/api/experiment-trees/tree_1/branches/main/plan":
            self._json(
                200,
                {
                    "cells": [
                        {"cell_id": "cell_1", "action": "cache_hit", "reason": "cached"}
                    ],
                    "summary": {"run": 0, "cache_hits": 1},
                },
            )
            return
        if self.path == "/api/executions/exec_1":
            if type(self).cancel_requested:
                self._json(
                    200,
                    {
                        "execution_id": "exec_1",
                        "status": "running",
                        "phase": "cancellation_requested",
                        "cancellation_requested_at": "2026-04-07T10:15:02Z",
                        "node_statuses": {},
                        "finished_at": None,
                    },
                )
                return
            self._json(
                200,
                {
                    "execution_id": "exec_1",
                    "status": "running",
                    "phase": "running",
                    "cancellation_requested_at": None,
                    "node_statuses": {},
                    "finished_at": None,
                },
            )
            return
        if self.path == "/api/executions/exec_timeout":
            self._json(
                200,
                {
                    "execution_id": "exec_timeout",
                    "status": "timed_out",
                    "phase": "timed_out",
                    "cancellation_requested_at": None,
                    "node_statuses": {"step1": "failed"},
                    "finished_at": "2026-04-07T10:16:00Z",
                },
            )
            return
        if self.path == "/api/executions/exec_wait":
            type(self).wait_status_requests += 1
            self._json(
                200,
                {
                    "execution_id": "exec_wait",
                    "status": "running",
                    "phase": "running",
                    "cancellation_requested_at": None,
                    "node_statuses": {"step1": "running"},
                    "finished_at": None,
                },
            )
            return
        if self.path == "/api/executions/exec_queued":
            self._json(
                200,
                {
                    "execution_id": "exec_queued",
                    "status": "queued",
                    "phase": "queued",
                    "queue_position": 2,
                    "queue": {
                        "pending_ahead": 1,
                        "pending_total": 2,
                        "active_executions": 1,
                        "max_concurrent_executions": 1,
                        "max_queue_depth": 32,
                        "queue_head": False,
                        "queued_reason": "waiting_for_earlier_executions",
                    },
                    "cancellation_requested_at": None,
                    "node_statuses": {},
                    "finished_at": None,
                },
            )
            return
        if self.path == "/api/experiment-trees/tree_1/branches/main/cells/cell_1/logs":
            self._json(
                200,
                {
                    "stdout": "line1\nline2\nline3\n",
                    "stderr": "warn1\nwarn2\n",
                    "outputs": [],
                    "error": None,
                    "duration_ms": 12,
                    "metrics": {},
                },
            )
            return
        self._json(404, {"error": "not found", "code": "not_found"})

    def do_POST(self) -> None:  # noqa: N802
        type(self).last_authorization = self.headers.get("Authorization")
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length).decode("utf-8") if length else ""
        payload = json.loads(body) if body else {}
        if self.path == "/api/projects":
            type(self).last_create_project_payload = payload
            self._json(201, {"id": "project_1"})
            return
        if self.path == "/api/experiment-trees":
            self._json(
                201,
                {
                    "id": "tree_1",
                    "name": payload["name"],
                    "environment": {"dependencies": []},
                },
            )
            return
        if self.path == "/api/experiment-trees/tree_1/branches":
            type(self).last_create_branch_payload = payload
            self._json(201, "branch_1")
            return
        if self.path == "/api/experiment-trees/tree_1/branches/main/cells":
            type(self).last_add_cell_payload = payload
            self.send_response(201)
            self.end_headers()
            return
        if self.path == "/api/experiment-trees/tree_1/branches/main/cells/cell_1/code":
            type(self).last_update_cell_payload = payload
            self.send_response(200)
            self.end_headers()
            return
        if self.path == "/api/experiment-trees/tree_1/branches/main/cells/cell_1/move":
            type(self).last_move_cell_payload = payload
            self.send_response(200)
            self.end_headers()
            return
        if self.path == "/api/experiment-trees/tree_1/branches/main/execute":
            type(self).last_execute_payload = payload
            self._json(
                202,
                {
                    "execution_id": "exec_branch_1",
                    "status": "accepted",
                    "phase": "queued",
                    "target": {
                        "kind": "branch",
                        "tree_id": "tree_1",
                        "branch_id": "main",
                        "cell_id": None,
                    },
                    "queue_position": None,
                    "created_at": "2026-04-07T10:15:00Z",
                },
            )
            return
        if self.path == "/api/experiment-trees/tree_1/branches/main/cells/cell_1/execute":
            self._json(
                202,
                {
                    "execution_id": "exec_cell_1",
                    "status": "accepted",
                    "phase": "queued",
                    "target": {
                        "kind": "cell",
                        "tree_id": "tree_1",
                        "branch_id": "main",
                        "cell_id": "cell_1",
                    },
                    "queue_position": None,
                    "created_at": "2026-04-07T10:15:01Z",
                },
            )
            return
        if self.path == "/api/experiment-trees/tree_1/execute-all-branches":
            self._json(
                202,
                {
                    "executions": [
                        {
                            "execution_id": "exec_branch_1",
                            "status": "accepted",
                            "phase": "queued",
                            "target": {
                                "kind": "branch",
                                "tree_id": "tree_1",
                                "branch_id": "main",
                                "cell_id": None,
                            },
                            "queue_position": None,
                            "created_at": "2026-04-07T10:15:00Z",
                        }
                    ]
                },
            )
            return
        if self.path == "/api/executions/exec_1/cancel":
            type(self).cancel_requested = True
            self.send_response(200)
            self.end_headers()
            return
        if self.path == "/api/experiment-trees/tree_1/restart-kernel":
            type(self).restart_requested = True
            self.send_response(200)
            self.end_headers()
            return
        self._json(404, {"error": "not found", "code": "not_found"})

    def do_PUT(self) -> None:  # noqa: N802
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length).decode("utf-8") if length else ""
        payload = json.loads(body) if body else {}
        if self.path == "/api/experiment-trees/tree_1":
            self._json(200, payload)
            return
        self._json(404, {"error": "not found", "code": "not_found"})

    def do_DELETE(self) -> None:  # noqa: N802
        if self.path in {
            "/api/experiment-trees/tree_1/branches/main/cells/cell_1",
            "/api/experiment-trees/tree_1/branches/branch_1",
        }:
            type(self).deleted_path = self.path
            self.send_response(204)
            self.end_headers()
            return
        self._json(404, {"error": "not found", "code": "not_found"})

    def log_message(self, format: str, *args) -> None:  # noqa: A003
        return

    def _json(self, status: int, payload: object) -> None:
        encoded = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)


class McpPythonTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.httpd = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        cls.thread = threading.Thread(target=cls.httpd.serve_forever, daemon=True)
        cls.thread.start()
        host, port = cls.httpd.server_address
        cls.server = McpServer(f"http://{host}:{port}")

    def setUp(self) -> None:
        _Handler.last_authorization = None
        _Handler.last_create_project_payload = None
        _Handler.last_create_branch_payload = None
        _Handler.last_add_cell_payload = None
        _Handler.last_update_cell_payload = None
        _Handler.last_move_cell_payload = None
        _Handler.deleted_path = None
        _Handler.last_execute_payload = None
        _Handler.cancel_requested = False
        _Handler.restart_requested = False
        _Handler.wait_status_requests = 0

    @classmethod
    def tearDownClass(cls) -> None:
        cls.httpd.shutdown()
        cls.thread.join(timeout=5)

    def test_list_tools_contains_expected_surface(self) -> None:
        names = [tool.name for tool in self.server.list_tools()]
        self.assertIn("list_experiment_trees", names)
        self.assertIn("add_cell", names)
        self.assertIn("update_cell", names)
        self.assertIn("move_cell", names)
        self.assertIn("delete_cell", names)
        self.assertIn("delete_branch", names)
        self.assertIn("inspect_cell", names)
        self.assertIn("inspect_kernel", names)
        self.assertIn("restart_kernel", names)
        self.assertIn("execute_branch", names)
        self.assertIn("logs", names)
        self.assertIn("get_experiment_summary", names)
        self.assertIn("wait_for_execution", names)

        create_experiment = next(
            tool for tool in self.server.list_tools() if tool.name == "create_experiment"
        )
        self.assertIn("source", create_experiment.input_schema["properties"])
        self.assertIn("first_cell", create_experiment.input_schema["properties"])

        create_branch = next(
            tool for tool in self.server.list_tools() if tool.name == "create_branch"
        )
        self.assertEqual(create_branch.input_schema["type"], "object")
        self.assertNotIn("oneOf", create_branch.input_schema)
        self.assertNotIn("anyOf", create_branch.input_schema)
        self.assertIn("empty Python starter cell", create_branch.description)

    def test_call_tool_round_trips_over_api(self) -> None:
        result = self.server.call_tool("list_experiment_trees", {})
        self.assertFalse(result.is_error)
        payload = json.loads(result.content[0]["text"])
        self.assertEqual(payload[0]["id"], "tree_1")

        created = self.server.call_tool("create_experiment", {"name": "demo"})
        self.assertFalse(created.is_error)
        created_payload = json.loads(created.content[0]["text"])
        self.assertEqual(created_payload["experiment"]["name"], "demo")
        self.assertEqual(created_payload["agent_context"]["authoring"]["root_cell_id"], "cell_1")
        self.assertIn(
            "Prefer updating `cell_1`",
            created_payload["agent_context"]["authoring"]["next_step_hint"],
        )
        self.assertEqual(
            created_payload["agent_context"]["environment"]["declared_dependencies"], []
        )
        self.assertEqual(
            created_payload["agent_context"]["environment"]["required_runtime_packages"],
            ["ipykernel==7.2.0", "cloudpickle==3.1.2"],
        )
        self.assertIn(
            "numpy==2.4.4",
            created_payload["agent_context"]["environment"]["always_available_packages"],
        )
        self.assertIn(
            "scikit-learn==1.8.0",
            created_payload["agent_context"]["environment"]["effective_packages"],
        )
        self.assertIn(
            "Do not add inline package-install helpers",
            created_payload["agent_context"]["environment"]["guidance"],
        )
        self.assertIn(
            "`!pip install ...`",
            created_payload["agent_context"]["environment"]["guidance"],
        )
        self.assertIn(
            "Only install packages that are missing from `effective_packages`.",
            created_payload["agent_context"]["environment"]["install_policy"],
        )
        self.assertIn(
            "Do not use plain `pip install`",
            created_payload["agent_context"]["environment"]["install_policy"],
        )
        self.assertIn(
            "Use the root cell for setup, imports, dataset loading, and package installs.",
            created_payload["agent_context"]["environment"]["workflow_guidelines"],
        )
        self.assertIn(
            "For non-default packages, install them in a setup cell with `!pip install <package>` before import.",
            created_payload["agent_context"]["environment"]["workflow_guidelines"],
        )
        self.assertIn(
            "first runnable cell a setup cell",
            created_payload["agent_context"]["authoring"]["setup_workflow"],
        )

        fetched = self.server.call_tool("get_experiment", {"experiment_id": "tree_1"})
        self.assertFalse(fetched.is_error)
        fetched_payload = json.loads(fetched.content[0]["text"])
        self.assertEqual(
            fetched_payload["agent_context"]["environment"]["declared_dependencies"],
            ["pandas", "matplotlib"],
        )
        self.assertIn(
            "pandas",
            fetched_payload["agent_context"]["environment"]["effective_packages"],
        )
        self.assertIn(
            "matplotlib",
            fetched_payload["agent_context"]["environment"]["effective_packages"],
        )

        summary = self.server.call_tool("get_experiment_summary", {"experiment_id": "tree_1"})
        self.assertFalse(summary.is_error)
        summary_payload = json.loads(summary.content[0]["text"])
        self.assertEqual(summary_payload["experiment_id"], "tree_1")

        kernel_inspection = self.server.call_tool(
            "inspect_kernel", {"experiment_id": "tree_1"}
        )
        self.assertFalse(kernel_inspection.is_error)
        kernel_inspection_payload = json.loads(kernel_inspection.content[0]["text"])
        self.assertEqual(kernel_inspection_payload["tree_id"], "tree_1")
        self.assertFalse(kernel_inspection_payload["has_live_kernel"])
        self.assertEqual(kernel_inspection_payload["tree_kernel_state"], "ready")

        restart_kernel = self.server.call_tool(
            "restart_kernel", {"experiment_id": "tree_1"}
        )
        self.assertFalse(restart_kernel.is_error)
        restart_payload = json.loads(restart_kernel.content[0]["text"])
        self.assertEqual(restart_payload["experiment_id"], "tree_1")
        self.assertEqual(
            restart_payload["message"],
            "Kernel restart requested for experiment tree_1",
        )

        post_restart_inspection = self.server.call_tool(
            "inspect_kernel", {"experiment_id": "tree_1"}
        )
        self.assertFalse(post_restart_inspection.is_error)
        post_restart_payload = json.loads(post_restart_inspection.content[0]["text"])
        self.assertTrue(post_restart_payload["has_live_kernel"])
        self.assertTrue(post_restart_payload["replay_required"])
        self.assertEqual(post_restart_payload["tree_kernel_state"], "needs_replay")

        saved = self.server.call_tool(
            "save_experiment",
            {
                "definition": {
                    "id": "tree_1",
                    "name": "demo",
                    "root_branch_id": "main",
                    "cells": [
                        {
                            "id": "cell_1",
                            "tree_id": "tree_1",
                            "branch_id": "main",
                            "name": "Cell 1",
                            "code": {"source": "value = 3\n", "language": "python"},
                            "upstream_cell_ids": [],
                            "declared_outputs": [],
                            "cache": True,
                            "tags": {},
                            "state": "clean",
                        }
                    ],
                    "environment": {"dependencies": ["xgboost"]},
                }
            },
        )
        self.assertFalse(saved.is_error)
        saved_payload = json.loads(saved.content[0]["text"])
        self.assertEqual(saved_payload["experiment"]["id"], "tree_1")
        self.assertEqual(
            saved_payload["experiment"]["cells"][0]["code"]["source"],
            "value = 3\n",
        )
        self.assertEqual(
            saved_payload["agent_context"]["environment"]["declared_dependencies"],
            ["xgboost"],
        )

        status = self.server.call_tool("status", {"execution_id": "exec_1"})
        self.assertFalse(status.is_error)
        status_payload = json.loads(status.content[0]["text"])
        self.assertEqual(status_payload["execution_id"], "exec_1")
        self.assertEqual(status_payload["status"], "running")
        self.assertEqual(status_payload["phase"], "running")
        self.assertIsNone(status_payload["cancellation_requested_at"])

        cancel = self.server.call_tool("cancel", {"execution_id": "exec_1"})
        self.assertFalse(cancel.is_error)
        cancel_payload = json.loads(cancel.content[0]["text"])
        self.assertEqual(cancel_payload["execution_id"], "exec_1")

        requested_status = self.server.call_tool("status", {"execution_id": "exec_1"})
        self.assertFalse(requested_status.is_error)
        requested_status_payload = json.loads(requested_status.content[0]["text"])
        self.assertEqual(requested_status_payload["status"], "running")
        self.assertEqual(requested_status_payload["phase"], "cancellation_requested")
        self.assertEqual(
            requested_status_payload["cancellation_requested_at"],
            "2026-04-07T10:15:02Z",
        )

        execute_branch = self.server.call_tool(
            "execute_branch", {"experiment_id": "tree_1", "branch_id": "main"}
        )
        self.assertFalse(execute_branch.is_error)
        execute_branch_payload = json.loads(execute_branch.content[0]["text"])
        self.assertEqual(execute_branch_payload["execution_id"], "exec_branch_1")
        self.assertEqual(execute_branch_payload["status"], "accepted")
        self.assertEqual(execute_branch_payload["phase"], "queued")
        self.assertEqual(execute_branch_payload["target"]["kind"], "branch")

        execute_cell = self.server.call_tool(
            "execute_cell",
            {"experiment_id": "tree_1", "branch_id": "main", "cell_id": "cell_1"},
        )
        self.assertFalse(execute_cell.is_error)
        execute_cell_payload = json.loads(execute_cell.content[0]["text"])
        self.assertEqual(execute_cell_payload["execution_id"], "exec_cell_1")
        self.assertEqual(execute_cell_payload["target"]["kind"], "cell")
        self.assertEqual(execute_cell_payload["target"]["cell_id"], "cell_1")

        execute_all = self.server.call_tool("execute_all_branches", {"experiment_id": "tree_1"})
        self.assertFalse(execute_all.is_error)
        execute_all_payload = json.loads(execute_all.content[0]["text"])
        self.assertEqual(len(execute_all_payload), 1)
        self.assertEqual(execute_all_payload[0]["execution_id"], "exec_branch_1")

        timed_out_status = self.server.call_tool("status", {"execution_id": "exec_timeout"})
        self.assertFalse(timed_out_status.is_error)
        timed_out_status_payload = json.loads(timed_out_status.content[0]["text"])
        self.assertEqual(timed_out_status_payload["status"], "timed_out")
        self.assertEqual(timed_out_status_payload["phase"], "timed_out")

        waited_timed_out = self.server.call_tool(
            "wait_for_execution",
            {"execution_id": "exec_timeout", "poll_interval_ms": 50},
        )
        self.assertFalse(waited_timed_out.is_error)
        waited_timed_out_payload = json.loads(waited_timed_out.content[0]["text"])
        self.assertEqual(waited_timed_out_payload["status"], "timed_out")
        self.assertEqual(waited_timed_out_payload["phase"], "timed_out")
        self.assertTrue(waited_timed_out_payload["terminal"])
        self.assertFalse(waited_timed_out_payload["wait_exhausted"])
        self.assertEqual(waited_timed_out_payload["suggested_next_action"], "done")

        waited_running = self.server.call_tool(
            "wait_for_execution",
            {"execution_id": "exec_wait", "wait_timeout_secs": 0, "poll_interval_ms": 50},
        )
        self.assertFalse(waited_running.is_error)
        waited_running_payload = json.loads(waited_running.content[0]["text"])
        self.assertEqual(waited_running_payload["execution_id"], "exec_wait")
        self.assertEqual(waited_running_payload["status"], "running")
        self.assertEqual(waited_running_payload["phase"], "running")
        self.assertFalse(waited_running_payload["terminal"])
        self.assertTrue(waited_running_payload["wait_exhausted"])
        self.assertEqual(waited_running_payload["summary"], "Execution is running.")
        self.assertEqual(waited_running_payload["suggested_next_action"], "wait")
        self.assertEqual(_Handler.wait_status_requests, 1)

        waited_legacy_alias = self.server.call_tool(
            "wait_for_execution",
            {"execution_id": "exec_wait", "timeout_secs": 0, "poll_interval_ms": 50},
        )
        self.assertFalse(waited_legacy_alias.is_error)
        waited_legacy_alias_payload = json.loads(waited_legacy_alias.content[0]["text"])
        self.assertEqual(waited_legacy_alias_payload["execution_id"], "exec_wait")
        self.assertFalse(waited_legacy_alias_payload["terminal"])
        self.assertTrue(waited_legacy_alias_payload["wait_exhausted"])

        waited_queued = self.server.call_tool(
            "wait_for_execution",
            {"execution_id": "exec_queued", "wait_timeout_secs": 0, "poll_interval_ms": 50},
        )
        self.assertFalse(waited_queued.is_error)
        waited_queued_payload = json.loads(waited_queued.content[0]["text"])
        self.assertEqual(waited_queued_payload["execution_id"], "exec_queued")
        self.assertEqual(
            waited_queued_payload["summary"],
            "Execution is queued and waiting to start.",
        )
        self.assertEqual(waited_queued_payload["suggested_next_action"], "wait")
        self.assertFalse(waited_queued_payload["terminal"])
        self.assertTrue(waited_queued_payload["wait_exhausted"])

        tailed_logs = self.server.call_tool(
            "logs",
            {
                "experiment_id": "tree_1",
                "branch_id": "main",
                "cell_id": "cell_1",
                "tail_lines": 1,
            },
        )
        self.assertFalse(tailed_logs.is_error)
        tailed_logs_payload = json.loads(tailed_logs.content[0]["text"])
        self.assertEqual(tailed_logs_payload["stdout"], "line3\n")
        self.assertEqual(tailed_logs_payload["stderr"], "warn2\n")
        self.assertEqual(tailed_logs_payload["view"]["tail_lines"], 1)
        self.assertEqual(tailed_logs_payload["view"]["stdout_total_lines"], 3)
        self.assertEqual(tailed_logs_payload["view"]["stderr_total_lines"], 2)
        self.assertTrue(tailed_logs_payload["view"]["stdout_truncated"])
        self.assertTrue(tailed_logs_payload["view"]["stderr_truncated"])

    def test_create_experiment_populates_root_cell_and_saves_tree(self) -> None:
        created_tree = {
            "id": "tree_1",
            "name": "demo",
            "root_branch_id": "main",
            "cells": [
                {
                    "id": "cell_1",
                    "tree_id": "tree_1",
                    "branch_id": "main",
                    "name": "Cell 1",
                    "code": {"source": "", "language": "python"},
                    "upstream_cell_ids": [],
                    "declared_outputs": [],
                    "cache": True,
                    "tags": {},
                    "state": "clean",
                }
            ],
            "environment": {"dependencies": []},
        }
        saved_tree = json.loads(json.dumps(created_tree))
        saved_tree["cells"][0]["code"] = {
            "source": "print('root')\n",
            "language": "python",
        }
        saved_tree["cells"][0]["declared_outputs"] = ["result"]
        saved_tree["cells"][0]["cache"] = False

        with (
            mock.patch.object(
                self.server.api,
                "create_experiment_tree",
                return_value=json.loads(json.dumps(created_tree)),
            ) as create_experiment,
            mock.patch.object(
                self.server.api,
                "save_experiment_tree",
                return_value=saved_tree,
            ) as save_experiment,
        ):
            result = self.server.call_tool(
                "create_experiment",
                {
                    "name": "demo",
                    "source": "print('root')\n",
                    "outputs": ["result"],
                    "cache": False,
                },
            )

        self.assertFalse(result.is_error)
        create_experiment.assert_called_once_with("demo", None)
        save_experiment.assert_called_once()
        saved_tree = save_experiment.call_args.args[0]
        first_cell = saved_tree["cells"][0]
        self.assertEqual(first_cell["id"], "cell_1")
        self.assertEqual(first_cell["tree_id"], "tree_1")
        self.assertEqual(first_cell["branch_id"], "main")
        self.assertEqual(first_cell["code"], {"source": "print('root')\n", "language": "python"})
        self.assertEqual(first_cell["declared_outputs"], ["result"])
        self.assertEqual(first_cell["cache"], False)

        result_payload = json.loads(result.content[0]["text"])
        self.assertEqual(
            result_payload["experiment"]["cells"][0]["code"]["source"],
            "print('root')\n",
        )
        self.assertEqual(result_payload["agent_context"]["authoring"]["root_cell_id"], "cell_1")
        self.assertNotIn(
            "next_step_hint",
            result_payload["agent_context"]["authoring"],
        )

    def test_save_experiment_returns_saved_tree_payload(self) -> None:
        definition = {
            "id": "tree_1",
            "name": "demo",
            "root_branch_id": "main",
            "cells": [
                {
                    "id": "cell_1",
                    "tree_id": "tree_1",
                    "branch_id": "main",
                    "name": "Cell 1",
                    "code": {"source": "value = 1\n", "language": "python"},
                    "upstream_cell_ids": [],
                    "declared_outputs": [],
                    "cache": True,
                    "tags": {},
                    "state": "clean",
                }
            ],
            "environment": {"dependencies": ["xgboost"]},
        }

        with mock.patch.object(
            self.server.api,
            "save_experiment_tree",
            return_value=json.loads(json.dumps(definition)),
        ) as save_experiment:
            result = self.server.call_tool("save_experiment", {"definition": definition})

        self.assertFalse(result.is_error)
        save_experiment.assert_called_once_with(definition)
        payload = json.loads(result.content[0]["text"])
        self.assertEqual(payload["experiment"]["id"], "tree_1")
        self.assertEqual(
            payload["agent_context"]["environment"]["declared_dependencies"],
            ["xgboost"],
        )

    def test_create_experiment_without_root_cell_authoring_does_not_save_tree(self) -> None:
        created_tree = {
            "id": "tree_1",
            "name": "demo",
            "root_branch_id": "main",
            "cells": [
                {
                    "id": "cell_1",
                    "tree_id": "tree_1",
                    "branch_id": "main",
                    "name": "Cell 1",
                    "code": {"source": "", "language": "python"},
                    "upstream_cell_ids": [],
                    "declared_outputs": [],
                    "cache": True,
                    "tags": {},
                    "state": "clean",
                }
            ],
            "environment": {"dependencies": []},
        }

        with (
            mock.patch.object(
                self.server.api,
                "create_experiment_tree",
                return_value=json.loads(json.dumps(created_tree)),
            ) as create_experiment,
            mock.patch.object(self.server.api, "save_experiment_tree") as save_experiment,
        ):
            result = self.server.call_tool("create_experiment", {"name": "demo"})

        self.assertFalse(result.is_error)
        create_experiment.assert_called_once_with("demo", None)
        save_experiment.assert_not_called()
        payload = json.loads(result.content[0]["text"])
        self.assertIn(
            "empty root cell (`cell_1`)",
            payload["agent_context"]["authoring"]["next_step_hint"],
        )
        self.assertIn(
            "`!pip install ...`",
            payload["agent_context"]["authoring"]["next_step_hint"],
        )

    def test_handle_request_supports_initialize_and_tools(self) -> None:
        initialize = _handle_request(
            self.server,
            {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}},
        )
        self.assertEqual(initialize["result"]["serverInfo"]["name"], "tine")

        tools = _handle_request(
            self.server,
            {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}},
        )
        tool_names = [tool["name"] for tool in tools["result"]["tools"]]
        self.assertIn("create_project", tool_names)

    def test_create_project_omits_workspace_dir_unless_explicit(self) -> None:
        cloud_result = self.server.call_tool("create_project", {"name": "cloud project"})

        self.assertFalse(cloud_result.is_error)
        self.assertEqual(_Handler.last_create_project_payload, {"name": "cloud project"})

        local_result = self.server.call_tool(
            "create_project",
            {"name": "local project", "workspace_dir": "/tmp/tine-local"},
        )

        self.assertFalse(local_result.is_error)
        self.assertEqual(
            _Handler.last_create_project_payload,
            {"name": "local project", "workspace_dir": "/tmp/tine-local"},
        )

    def test_cloud_api_key_is_sent_as_bearer_auth(self) -> None:
        server = McpServer(self.server.api.base_url, api_key="tine_sk_test")
        result = server.call_tool("create_project", {"name": "cloud project"})

        self.assertFalse(result.is_error)
        self.assertEqual(_Handler.last_authorization, "Bearer tine_sk_test")

    def test_create_branch_defaults_to_empty_starter_cell(self) -> None:
        with mock.patch.object(
            self.server.api,
            "create_branch_in_experiment_tree",
            return_value="branch_1",
        ) as create_branch:
            result = self.server.call_tool(
                "create_branch",
                {
                    "experiment_id": "tree_1",
                    "parent_branch_id": "main",
                    "name": "draft",
                    "branch_point_cell_id": "cell_1",
                },
            )

        self.assertFalse(result.is_error)
        create_branch.assert_called_once()
        first_cell = create_branch.call_args.args[4]
        self.assertEqual(first_cell["tree_id"], "tree_1")
        self.assertEqual(first_cell["branch_id"], "ignored")
        self.assertEqual(first_cell["name"], "draft")
        self.assertEqual(first_cell["code"]["source"], "")
        self.assertEqual(first_cell["code"]["language"], "python")
        self.assertEqual(first_cell["upstream_cell_ids"], [])
        self.assertEqual(first_cell["declared_outputs"], [])
        self.assertEqual(first_cell["tags"], {})
        self.assertEqual(first_cell["state"], "clean")

    def test_create_branch_accepts_explicit_empty_source(self) -> None:
        with mock.patch.object(
            self.server.api,
            "create_branch_in_experiment_tree",
            return_value="branch_1",
        ) as create_branch:
            result = self.server.call_tool(
                "create_branch",
                {
                    "experiment_id": "tree_1",
                    "parent_branch_id": "main",
                    "name": "draft",
                    "branch_point_cell_id": "cell_1",
                    "source": "",
                },
            )

        self.assertFalse(result.is_error)
        first_cell = create_branch.call_args.args[4]
        self.assertEqual(first_cell["tree_id"], "tree_1")
        self.assertEqual(first_cell["branch_id"], "ignored")
        self.assertEqual(first_cell["code"]["source"], "")

    def test_create_branch_normalizes_partial_first_cell(self) -> None:
        with mock.patch.object(
            self.server.api,
            "create_branch_in_experiment_tree",
            return_value="branch_1",
        ) as create_branch:
            result = self.server.call_tool(
                "create_branch",
                {
                    "experiment_id": "tree_1",
                    "parent_branch_id": "main",
                    "name": "branch draft",
                    "branch_point_cell_id": "cell_1",
                    "first_cell": {
                        "id": "branch_cell_1",
                        "code": {"source": "print('hi')\n", "language": "python"},
                        "upstream": ["cell_1"],
                        "outputs": ["result"],
                    },
                },
            )

        self.assertFalse(result.is_error)
        first_cell = create_branch.call_args.args[4]
        self.assertEqual(first_cell["id"], "branch_cell_1")
        self.assertEqual(first_cell["tree_id"], "tree_1")
        self.assertEqual(first_cell["branch_id"], "ignored")
        self.assertEqual(first_cell["name"], "branch draft")
        self.assertEqual(first_cell["upstream_cell_ids"], ["cell_1"])
        self.assertEqual(first_cell["declared_outputs"], ["result"])
        self.assertEqual(first_cell["state"], "clean")

    def test_create_branch_round_trips_lightweight_payload_over_api(self) -> None:
        result = self.server.call_tool(
            "create_branch",
            {
                "experiment_id": "tree_1",
                "parent_branch_id": "main",
                "name": "api draft",
                "branch_point_cell_id": "cell_1",
                "source": "print('api')\n",
                "outputs": ["result"],
                "cache": False,
            },
        )

        self.assertFalse(result.is_error)
        branch_payload = json.loads(result.content[0]["text"])
        self.assertEqual(branch_payload["branch_id"], "branch_1")
        self.assertTrue(str(branch_payload["first_cell_id"]).startswith("cell_"))
        payload = _Handler.last_create_branch_payload
        self.assertIsNotNone(payload)
        assert payload is not None
        self.assertEqual(payload["parent_branch_id"], "main")
        self.assertEqual(payload["name"], "api draft")
        self.assertEqual(payload["branch_point_cell_id"], "cell_1")
        first_cell = payload["first_cell"]
        self.assertIsInstance(first_cell, dict)
        assert isinstance(first_cell, dict)
        self.assertEqual(first_cell["tree_id"], "tree_1")
        self.assertEqual(first_cell["branch_id"], "ignored")
        self.assertEqual(first_cell["name"], "api draft")
        self.assertEqual(first_cell["code"], {"source": "print('api')\n", "language": "python"})
        self.assertEqual(first_cell["declared_outputs"], ["result"])
        self.assertEqual(first_cell["cache"], False)
        self.assertEqual(first_cell["state"], "clean")

    def test_add_cell_round_trips_lightweight_payload_over_api(self) -> None:
        result = self.server.call_tool(
            "add_cell",
            {
                "experiment_id": "tree_1",
                "branch_id": "main",
                "after_cell_id": "cell_1",
                "source": "print('child')\n",
                "outputs": ["result"],
                "cache": False,
            },
        )

        self.assertFalse(result.is_error)
        add_payload = json.loads(result.content[0]["text"])
        self.assertEqual(add_payload["branch_id"], "main")
        self.assertTrue(str(add_payload["cell_id"]).startswith("cell_"))
        payload = _Handler.last_add_cell_payload
        self.assertIsNotNone(payload)
        assert payload is not None
        self.assertEqual(payload["after_cell_id"], "cell_1")
        cell = payload["cell"]
        self.assertIsInstance(cell, dict)
        assert isinstance(cell, dict)
        self.assertEqual(cell["tree_id"], "tree_1")
        self.assertEqual(cell["branch_id"], "main")
        self.assertEqual(cell["code"], {"source": "print('child')\n", "language": "python"})
        self.assertEqual(cell["declared_outputs"], ["result"])
        self.assertEqual(cell["cache"], False)

    def test_update_move_and_delete_cell_round_trip_over_api(self) -> None:
        updated = self.server.call_tool(
            "update_cell",
            {
                "experiment_id": "tree_1",
                "branch_id": "main",
                "cell_id": "cell_1",
                "source": "value = 2\n",
            },
        )
        self.assertFalse(updated.is_error)
        updated_payload = json.loads(updated.content[0]["text"])
        self.assertEqual(updated_payload["cell_id"], "cell_1")
        self.assertEqual(updated_payload["branch_id"], "main")
        self.assertEqual(_Handler.last_update_cell_payload, {"source": "value = 2\n"})

        moved = self.server.call_tool(
            "move_cell",
            {
                "experiment_id": "tree_1",
                "branch_id": "main",
                "cell_id": "cell_1",
                "direction": "down",
            },
        )
        self.assertFalse(moved.is_error)
        moved_payload = json.loads(moved.content[0]["text"])
        self.assertEqual(moved_payload["cell_id"], "cell_1")
        self.assertEqual(_Handler.last_move_cell_payload, {"direction": "down"})

        deleted = self.server.call_tool(
            "delete_cell",
            {
                "experiment_id": "tree_1",
                "branch_id": "main",
                "cell_id": "cell_1",
            },
        )
        self.assertFalse(deleted.is_error)
        deleted_payload = json.loads(deleted.content[0]["text"])
        self.assertEqual(deleted_payload["cell_id"], "cell_1")
        self.assertEqual(
            _Handler.deleted_path,
            "/api/experiment-trees/tree_1/branches/main/cells/cell_1",
        )

    def test_delete_branch_round_trips_over_api(self) -> None:
        result = self.server.call_tool(
            "delete_branch",
            {"experiment_id": "tree_1", "branch_id": "branch_1"},
        )

        self.assertFalse(result.is_error)
        delete_payload = json.loads(result.content[0]["text"])
        self.assertEqual(delete_payload["branch_id"], "branch_1")
        self.assertEqual(_Handler.deleted_path, "/api/experiment-trees/tree_1/branches/branch_1")

    def test_inspect_cell_round_trips_over_api(self) -> None:
        result = self.server.call_tool(
            "inspect_cell",
            {"experiment_id": "tree_1", "branch_id": "main", "cell_id": "cell_1"},
        )

        self.assertFalse(result.is_error)
        payload = json.loads(result.content[0]["text"])
        self.assertEqual(payload["tree_id"], "tree_1")
        self.assertEqual(payload["branch_id"], "main")
        self.assertEqual(payload["target_cell_id"], "cell_1")
        self.assertEqual(payload["path_cell_order"], ["cell_1"])
        self.assertEqual(payload["divergence_cell_id"], "cell_1")

    def test_build_config_document_matches_host_shapes(self) -> None:
        claude = build_config_document(host="claude", name="tine")
        self.assertEqual(claude["mcpServers"]["tine"]["command"], "tine")
        self.assertEqual(claude["mcpServers"]["tine"]["args"], ["mcp", "serve"])

        vscode = build_config_document(
            host="vscode",
            name="tinemcp",
            command_path="/tmp/tine",
            api_url="http://127.0.0.1:9473",
        )
        self.assertEqual(vscode["servers"]["tinemcp"]["type"], "stdio")
        self.assertEqual(vscode["servers"]["tinemcp"]["command"], "/tmp/tine")
        self.assertEqual(
            vscode["servers"]["tinemcp"]["args"],
            ["mcp", "serve", "--api-url", "http://127.0.0.1:9473"],
        )
        self.assertEqual(
            vscode["servers"]["tinemcp"]["env"],
            {"TINE_API_URL": "http://127.0.0.1:9473"},
        )

        cloud = build_config_document(
            host="claude",
            name="tine-cloud",
            api_url="https://cloud.tine.test",
            api_key="tine_sk_live",
        )
        self.assertEqual(
            cloud["mcpServers"]["tine-cloud"]["env"],
            {
                "TINE_API_URL": "https://cloud.tine.test",
                "TINE_API_KEY": "tine_sk_live",
            },
        )

    def test_register_config_merges_existing_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "claude.json"
            path.write_text(json.dumps({"mcpServers": {"other": {"command": "echo"}}}))
            document = build_config_document(host="claude", name="tine")
            registered = register_config(
                host="claude",
                document=document,
                name="tine",
                config_path=str(path),
            )

            self.assertEqual(registered, path)
            written = json.loads(path.read_text())
            self.assertEqual(written["mcpServers"]["other"]["command"], "echo")
            self.assertEqual(written["mcpServers"]["tine"]["command"], "tine")

    def test_resolve_default_config_path_is_os_specific(self) -> None:
        with mock.patch("platform.system", return_value="Darwin"):
            path = resolve_config_path("claude")
        self.assertIn("Claude/claude_desktop_config.json", str(path))

    def test_runtime_package_fallback_uses_embedded_pins_when_no_manifest_exists(self) -> None:
        with mock.patch("tine.mcp._repo_runtime_pins_path", return_value=None), mock.patch(
            "tine.mcp._packaged_runtime_pins_path", return_value=None
        ):
            baseline = mcp_module._desktop_runtime_baseline()
            required = mcp_module._packages_for_category(mcp_module.CORE_RUNTIME_CATEGORY)
            defaults = tuple(
                mcp_module._package_spec(package, version)
                for category, package, version in baseline
                if category != mcp_module.CORE_RUNTIME_CATEGORY
            )

        self.assertEqual(baseline, mcp_module._EMBEDDED_DESKTOP_RUNTIME_BASELINE)
        self.assertIn("ipykernel==7.2.0", required)
        self.assertIn("cloudpickle==3.1.2", required)
        self.assertIn("pandas==3.0.2", defaults)
        self.assertIn("scikit-learn==1.8.0", defaults)

    def test_execute_branch_waits_for_terminal_and_passes_idempotency_key(self) -> None:
        result = self.server.call_tool(
            "execute_branch",
            {
                "experiment_id": "tree_1",
                "wait_timeout_secs": 5,
                "include_logs": True,
                "idempotency_key": "agent-key-1",
            },
        )

        self.assertFalse(result.is_error)
        payload = json.loads(result.content[0]["text"])
        self.assertTrue(payload["terminal"])
        self.assertEqual(payload["status"], "completed")
        self.assertIn("cell_1", payload["node_logs"])
        self.assertEqual(
            _Handler.last_execute_payload, {"idempotency_key": "agent-key-1"}
        )

    def test_execute_branch_without_wait_returns_submission_envelope(self) -> None:
        result = self.server.call_tool("execute_branch", {"experiment_id": "tree_1"})

        self.assertFalse(result.is_error)
        payload = json.loads(result.content[0]["text"])
        self.assertEqual(payload["execution_id"], "exec_branch_1")
        self.assertNotIn("terminal", payload)

    def test_execute_branch_auto_generates_and_echoes_idempotency_key(self) -> None:
        """Execute submissions must be retry-safe by default: when the agent
        omits an idempotency key, the adapter generates one, sends it with
        the request, and echoes it in the response so a timed-out submission
        can be retried with the same key."""
        result = self.server.call_tool("execute_branch", {"experiment_id": "tree_1"})

        self.assertFalse(result.is_error)
        sent = _Handler.last_execute_payload or {}
        generated_key = str(sent.get("idempotency_key") or "")
        self.assertTrue(
            generated_key,
            "execute_branch must submit an auto-generated idempotency key",
        )
        payload = json.loads(result.content[0]["text"])
        self.assertEqual(payload["idempotency_key"], generated_key)

    def test_execute_branch_wait_result_includes_idempotency_key(self) -> None:
        result = self.server.call_tool(
            "execute_branch",
            {
                "experiment_id": "tree_1",
                "wait_timeout_secs": 5,
                "poll_interval_ms": 50,
            },
        )

        self.assertFalse(result.is_error)
        sent = _Handler.last_execute_payload or {}
        payload = json.loads(result.content[0]["text"])
        self.assertEqual(payload["idempotency_key"], sent.get("idempotency_key"))

    def test_wait_with_include_logs_surfaces_log_retrieval_failure(self) -> None:
        """A failed results fetch must be distinguishable from an execution
        that produced no logs: the wait result stays valid but carries a
        structured logs_error instead of silently returning empty logs."""
        result = self.server.call_tool(
            "wait_for_execution",
            {
                "execution_id": "exec_logs_error",
                "wait_timeout_secs": 5,
                "poll_interval_ms": 50,
                "include_logs": True,
            },
        )

        self.assertFalse(result.is_error)
        payload = json.loads(result.content[0]["text"])
        self.assertEqual(payload["node_logs"], {})
        self.assertIn("logs_error", payload)
        self.assertTrue(payload["logs_error"]["message"])
        self.assertTrue(payload["logs_error"]["code"])

    def test_results_caps_large_inline_outputs_by_default(self) -> None:
        capped = self.server.call_tool("results", {"execution_id": "exec_branch_1"})
        self.assertFalse(capped.is_error)
        capped_logs = json.loads(capped.content[0]["text"])["node_logs"]["cell_1"]
        self.assertTrue(capped_logs["outputs_truncated"])
        self.assertIn("bytes omitted", capped_logs["outputs"][0]["data"]["image/png"])

        full = self.server.call_tool(
            "results", {"execution_id": "exec_branch_1", "include_outputs": True}
        )
        full_logs = json.loads(full.content[0]["text"])["node_logs"]["cell_1"]
        self.assertEqual(len(full_logs["outputs"][0]["data"]["image/png"]), 9000)

    def test_plan_branch_round_trips_over_api(self) -> None:
        result = self.server.call_tool("plan_branch", {"experiment_id": "tree_1"})

        self.assertFalse(result.is_error)
        payload = json.loads(result.content[0]["text"])
        self.assertEqual(payload["summary"], {"run": 0, "cache_hits": 1})
        self.assertEqual(payload["cells"][0]["reason"], "cached")

    def test_errors_carry_machine_readable_code_and_next_action(self) -> None:
        missing = self.server.call_tool(
            "get_experiment", {"experiment_id": "missing_tree"}
        )
        self.assertTrue(missing.is_error)
        missing_error = json.loads(missing.content[0]["text"])["error"]
        self.assertEqual(missing_error["code"], "not_found")
        self.assertIn("suggested_next_action", missing_error)

        invalid = self.server.call_tool(
            "move_cell",
            {"experiment_id": "tree_1", "cell_id": "cell_1", "direction": "sideways"},
        )
        self.assertTrue(invalid.is_error)
        invalid_error = json.loads(invalid.content[0]["text"])["error"]
        self.assertEqual(invalid_error["code"], "validation")


class _StallingHandler(BaseHTTPRequestHandler):
    """HTTP handler that accepts requests but never responds, simulating a wedged
    or deadlocked Tine REST server. The matching test event is set in test
    setUp so each test can release the in-flight request before tearDown."""

    release_event: threading.Event | None = None

    def _wait_then_drop(self) -> None:
        evt = type(self).release_event
        if evt is not None:
            evt.wait(timeout=15)
        # Intentionally do not write a response — we want the client to
        # observe a stall, not a 503.

    def do_GET(self) -> None:  # noqa: N802
        self._wait_then_drop()

    def do_POST(self) -> None:  # noqa: N802
        self._wait_then_drop()

    def log_message(self, *args, **kwargs) -> None:  # noqa: D401
        return


class StalledServerFailureTests(unittest.TestCase):
    """Replicates the urlopen-no-timeout wedge: the api_client's HTTP calls
    have no timeout, so a stalled server blocks them forever, which in turn
    blocks the synchronous MCP dispatcher and causes 'failed to tool call'
    cascades on the host side."""

    def setUp(self) -> None:
        from tine.api_client import TineApiClient

        self.release_event = threading.Event()
        _StallingHandler.release_event = self.release_event
        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), _StallingHandler)
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()
        host, port = self.httpd.server_address
        # Tight timeout for the test so we can prove the wedge is bounded
        # without making the suite take 30s. Production uses 30s default.
        self.api = TineApiClient(
            f"http://{host}:{port}", default_timeout_secs=1.0, long_timeout_secs=1.0
        )

    def tearDown(self) -> None:
        # Release any in-flight blocked handlers so threads exit cleanly.
        self.release_event.set()
        self.httpd.shutdown()
        self.thread.join(timeout=5)

    def test_api_status_does_not_block_indefinitely_when_server_stalls(self) -> None:
        """A stalled API server must not wedge api.status() forever."""
        result: dict[str, Any] = {}

        def call() -> None:
            try:
                result["ok"] = self.api.status("exec-1")
            except Exception as exc:  # noqa: BLE001
                result["err"] = exc

        worker = threading.Thread(target=call, daemon=True)
        worker.start()
        worker.join(timeout=2.0)
        self.assertFalse(
            worker.is_alive(),
            "api.status() must time out against a stalled server, not block forever; "
            "this is the urlopen-no-timeout wedge that causes MCP 'failed to tool call' cascades",
        )
        self.assertIn("err", result)

    def test_execute_branch_timeout_warns_about_duplicate_submission_risk(self) -> None:
        """execute_* submissions are non-idempotent: an HTTP timeout on a
        POST does not prove the server abandoned the request. A pure
        `timeout=None` would prevent the duplicate, but it also makes an
        orphaned submission un-cancellable from the wrapper. The
        compromise: bound the timeout long-but-finite, and require the
        timeout error to *explicitly* warn about duplicate-risk so the
        agent / user does not blindly retry.

        The test client uses `long_timeout_secs=1.0` so we observe the
        timeout fire quickly; production uses 600s.
        """
        with self.assertRaises(RuntimeError) as ctx:
            self.api.execute_branch_in_experiment_tree("tree_1", "main")
        message = str(ctx.exception)
        self.assertIn(
            "WARNING",
            message,
            f"timeout error must escalate visibility for non-idempotent submissions: {message}",
        )
        self.assertIn(
            "idempotency_key",
            message,
            f"timeout error must instruct against blind retry to prevent "
            f"duplicate executions: {message}",
        )
        self.assertIn(
            "duplicate",
            message.lower(),
            f"timeout error must mention duplicate-risk: {message}",
        )

    def test_execute_branch_timeout_error_echoes_generated_idempotency_key(self) -> None:
        """When an execute submission times out, the MCP error envelope must
        carry the idempotency key that was sent (auto-generated when the
        agent omitted one) — without it, the recovery advice to 'retry with
        the same idempotency_key' is impossible to follow."""
        server = McpServer("http://placeholder")
        server.api = self.api

        result = server.call_tool("execute_branch", {"experiment_id": "tree_1"})

        self.assertTrue(result.is_error)
        error = json.loads(result.content[0]["text"])["error"]
        self.assertEqual(error["code"], "timeout")
        self.assertTrue(
            str(error.get("idempotency_key", "")).startswith("mcp-"),
            f"timeout error must echo the generated idempotency key: {error}",
        )

    def test_create_experiment_tree_timeout_warns_about_duplicate_mutation_risk(self) -> None:
        """Tree creation is also a non-idempotent write: if the server
        accepts the POST and the response stalls, a blind retry can create
        duplicate trees. The wrapper must surface the same explicit
        duplicate-risk warning path used for execute submissions."""
        with self.assertRaises(RuntimeError) as ctx:
            self.api.create_experiment_tree("demo")
        message = str(ctx.exception)
        self.assertIn(
            "WARNING",
            message,
            f"timeout error must escalate visibility for mutating writes: {message}",
        )
        self.assertIn(
            "idempotency_key",
            message,
            f"timeout error must instruct against blind retry for mutating writes: {message}",
        )
        self.assertTrue(
            "duplicate" in message.lower() or "conflicting state" in message.lower(),
            f"timeout error must mention duplicate/conflicting-state risk: {message}",
        )


class _PauseHandler(BaseHTTPRequestHandler):
    """HTTP handler that sleeps inside slow endpoints and returns immediately
    for fast ones. Used to simulate the 'long tool call followed by short
    tool call' pattern that exposes the single-threaded dispatcher."""

    pause_event: threading.Event | None = None
    fast_calls = 0
    slow_calls = 0

    def do_GET(self) -> None:  # noqa: N802
        if self.path.endswith("/inspect-kernel"):
            type(self).slow_calls += 1
            evt = type(self).pause_event
            if evt is not None:
                evt.wait(timeout=10)
            self._json(200, {
                "tree_id": "tree_1",
                "has_live_kernel": False,
                "tree_kernel_state": None,
                "replay_required": False,
                "active_branch_id": None,
                "runtime_epoch": 0,
            })
            return
        if self.path.startswith("/api/executions/"):
            # Fast recovery path used by `status` — must always respond
            # promptly, even while slow paths are wedged.
            type(self).fast_calls += 1
            self._json(200, {
                "execution_id": "exec_1",
                "status": "running",
                "phase": "running",
                "cancellation_requested_at": None,
                "node_statuses": {},
                "finished_at": None,
            })
            return
        if self.path == "/api/experiment-trees":
            type(self).fast_calls += 1
            self._json(200, [{"id": "tree_1", "name": "demo"}])
            return
        self.send_response(404)
        self.end_headers()

    def do_POST(self) -> None:  # noqa: N802
        if self.path.endswith("/execute"):
            # Slow path simulating a wedged execute submission. Holds the
            # dispatcher worker until the test releases pause_event.
            type(self).slow_calls += 1
            evt = type(self).pause_event
            if evt is not None:
                evt.wait(timeout=15)
            self._json(202, {
                "execution_id": "exec_pending",
                "status": "queued",
                "phase": "queued",
                "target": {"kind": "branch", "tree_id": "tree_1", "branch_id": "main"},
                "queue_position": None,
                "created_at": "2026-05-10T00:00:00Z",
            })
            return
        self.send_response(404)
        self.end_headers()

    def _json(self, status: int, payload: object) -> None:
        encoded = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def log_message(self, *args, **kwargs) -> None:  # noqa: D401
        return


class DispatcherSerialBlockingTests(unittest.TestCase):
    """Replicates the observed 'first call works, every subsequent call fails'
    pattern: the JSON-RPC dispatcher is single-threaded, so a single slow tool
    call (e.g. wait_for_execution with extended wait_timeout_secs, or any
    api_client call hitting a stalled server) blocks every other tool call
    arriving on stdin until it completes."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.httpd = ThreadingHTTPServer(("127.0.0.1", 0), _PauseHandler)
        cls.thread = threading.Thread(target=cls.httpd.serve_forever, daemon=True)
        cls.thread.start()
        host, port = cls.httpd.server_address
        cls.server = McpServer(f"http://{host}:{port}")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.httpd.shutdown()
        cls.thread.join(timeout=5)

    def setUp(self) -> None:
        _PauseHandler.pause_event = threading.Event()
        _PauseHandler.fast_calls = 0
        _PauseHandler.slow_calls = 0

    def tearDown(self) -> None:
        # Release any blocked slow endpoint so the worker thread exits.
        evt = _PauseHandler.pause_event
        if evt is not None:
            evt.set()

    def test_run_stdio_must_dispatch_concurrent_requests(self) -> None:
        """A slow tool call must not block other tool calls from being
        serviced. With the current synchronous `run_stdio`, the second
        request waits behind the first; we need concurrent dispatch so
        unrelated tools (status, logs, cancel) stay responsive while a
        long wait_for_execution is in flight."""
        from tine.mcp import run_stdio

        slow_request = json.dumps({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "inspect_kernel",
                "arguments": {"experiment_id": "tree_1"},
            },
        })
        fast_request = json.dumps({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": "list_experiment_trees",
                "arguments": {},
            },
        })

        # Pre-load both requests into stdin; the loop will consume them sequentially.
        import io

        stdin = io.StringIO(slow_request + "\n" + fast_request + "\n")
        stdout = _ThreadSafeStringIO()

        # Run the loop in a thread so we can observe response timing.
        loop_thread = threading.Thread(
            target=run_stdio, args=(self.server, stdin, stdout), daemon=True
        )
        loop_thread.start()

        # Wait until at least one response has been written, capturing its
        # arrival time. We expect this to be the FAST one if dispatch is
        # concurrent. With today's serial dispatcher it will be neither
        # until we release the slow endpoint.
        first_response_at = stdout.wait_for_lines(1, timeout=2.0)
        self.assertIsNotNone(
            first_response_at,
            "no response within 2s — dispatcher is blocked on the slow tool call; "
            "a concurrent dispatcher should service the fast tool while the slow one is in flight",
        )
        first_response = json.loads(stdout.lines()[0])
        self.assertEqual(
            first_response.get("id"),
            2,
            "expected the fast (id=2) response to arrive first under concurrent dispatch, "
            f"but got id={first_response.get('id')} — dispatcher is serial",
        )

        # Now release the slow endpoint so the second response (id=1) lands.
        _PauseHandler.pause_event.set()
        stdout.wait_for_lines(2, timeout=5.0)
        loop_thread.join(timeout=5.0)


class StdioLoopResilienceTests(unittest.TestCase):
    """The JSON-RPC loop must survive an unhandled exception in one tool call
    and continue serving subsequent requests. Today's bare `_handle_request`
    catches tool errors at the dispatcher boundary, but anything escaping that
    catch (e.g. a malformed protocol message that hits an unexpected code
    path) used to take the whole loop down. Concurrent dispatch closes that
    gap by wrapping every per-request worker in a top-level try/except."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.httpd = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        cls.thread = threading.Thread(target=cls.httpd.serve_forever, daemon=True)
        cls.thread.start()
        host, port = cls.httpd.server_address
        cls.server = McpServer(f"http://{host}:{port}")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.httpd.shutdown()
        cls.thread.join(timeout=5)

    def test_run_stdio_survives_handler_raising_outside_call_tool_catch(self) -> None:
        """Patch `_handle_request` to raise on the first call; assert that
        a subsequent valid `tools/list` call still receives a structured
        response."""
        from tine import mcp as mcp_module
        from tine.mcp import run_stdio

        import io

        original_handle_request = mcp_module._handle_request
        call_count = {"n": 0}

        def flaky_handle_request(server, request_obj):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise RuntimeError("synthetic dispatcher crash")
            return original_handle_request(server, request_obj)

        bad_request = json.dumps({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/list",
            "params": {},
        })
        good_request = json.dumps({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/list",
            "params": {},
        })

        stdin = io.StringIO(bad_request + "\n" + good_request + "\n")
        stdout = _ThreadSafeStringIO()

        with mock.patch.object(mcp_module, "_handle_request", side_effect=flaky_handle_request):
            run_stdio(self.server, stdin, stdout)

        responses = [json.loads(line) for line in stdout.lines()]
        self.assertEqual(
            len(responses),
            2,
            f"loop must produce a response for both requests, got {responses}",
        )
        ids = sorted(r.get("id") for r in responses)
        self.assertEqual(ids, [1, 2], f"both ids must round-trip, got {responses}")
        # The crashed request must come back as a JSON-RPC error response,
        # not a silent drop.
        bad_response = next(r for r in responses if r.get("id") == 1)
        self.assertIn("error", bad_response, f"crashed request must surface an error: {bad_response}")
        self.assertIn(
            "synthetic dispatcher crash",
            bad_response["error"]["message"],
            f"error must include the underlying exception: {bad_response}",
        )
        # The healthy request must succeed.
        good_response = next(r for r in responses if r.get("id") == 2)
        self.assertIn("result", good_response, f"healthy request must succeed: {good_response}")


class DispatcherSaturationTests(unittest.TestCase):
    """The dispatcher must keep reading stdin even when its concurrency cap
    is exhausted; otherwise a burst of slow tool calls deadlocks the bridge
    against any subsequent fast call (host-visible 'failed to tool call'
    cascade once the host's per-call timeout fires)."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.httpd = ThreadingHTTPServer(("127.0.0.1", 0), _PauseHandler)
        cls.thread = threading.Thread(target=cls.httpd.serve_forever, daemon=True)
        cls.thread.start()
        host, port = cls.httpd.server_address
        cls.server = McpServer(f"http://{host}:{port}")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.httpd.shutdown()
        cls.thread.join(timeout=5)

    def setUp(self) -> None:
        _PauseHandler.pause_event = threading.Event()
        _PauseHandler.fast_calls = 0
        _PauseHandler.slow_calls = 0

    def tearDown(self) -> None:
        evt = _PauseHandler.pause_event
        if evt is not None:
            evt.set()

    def test_run_stdio_reader_does_not_block_when_concurrency_cap_is_saturated(self) -> None:
        """Submit (concurrency + 2) slow requests followed by 1 fast request.
        The fast request must receive a response promptly even while the
        slow ones are still in flight. With the previous reader-blocks-on-
        semaphore design, the reader stops consuming stdin once N slow
        requests are in flight, and the fast request sits unread until one
        slow request completes."""
        import io
        import time as _time

        from tine.mcp import run_stdio

        # Saturate at the dispatcher's concurrency cap + 2. The reader
        # must keep accepting input regardless.
        concurrency = 4
        slow_count = concurrency + 2
        slow_requests = "\n".join(
            json.dumps(
                {
                    "jsonrpc": "2.0",
                    "id": 100 + i,
                    "method": "tools/call",
                    "params": {
                        "name": "inspect_kernel",
                        "arguments": {"experiment_id": "tree_1"},
                    },
                }
            )
            for i in range(slow_count)
        )
        fast_request = json.dumps(
            {
                "jsonrpc": "2.0",
                "id": 999,
                "method": "tools/call",
                "params": {
                    "name": "list_experiment_trees",
                    "arguments": {},
                },
            }
        )

        stdin = io.StringIO(slow_requests + "\n" + fast_request + "\n")
        stdout = _ThreadSafeStringIO()

        loop_thread = threading.Thread(
            target=run_stdio,
            args=(self.server, stdin, stdout),
            kwargs={"concurrency": concurrency},
            daemon=True,
        )
        loop_thread.start()

        # Wait up to 3s for the fast (id=999) response to arrive while slow
        # endpoints are still parked. Either a `result` (would mean a slot
        # opened and the dispatcher serviced it) OR an `error` with the
        # overload code (means the dispatcher chose to fast-fail rather
        # than block) is acceptable — both prove the reader is responsive.
        # What we want to rule out is "no response at all", which is what
        # happens when the reader blocks on the semaphore.
        deadline = _time.monotonic() + 3.0
        fast_response = None
        while _time.monotonic() < deadline:
            for line in stdout.lines():
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if payload.get("id") == 999:
                    fast_response = payload
                    break
            if fast_response is not None:
                break
            _time.sleep(0.05)

        self.assertIsNotNone(
            fast_response,
            f"fast (id=999) request did not receive any response within 3s while "
            f"{slow_count} slow requests were in flight (concurrency cap={concurrency}); "
            "the reader is blocking on the semaphore and not accepting new stdin",
        )
        self.assertTrue(
            "result" in fast_response or "error" in fast_response,
            f"fast (id=999) response missing both result and error: {fast_response}",
        )

        # Release the slow endpoint and drain the loop.
        _PauseHandler.pause_event.set()
        stdout.wait_for_lines(slow_count + 1, timeout=10.0)
        loop_thread.join(timeout=10.0)

    def test_slow_tools_must_not_starve_fast_recovery_tools(self) -> None:
        """Saturate the dispatcher with non-idempotent slow tool calls
        (e.g. `execute_branch`) and assert that a recovery-class tool
        (`status`) still receives a real success response — not an
        overload error. Without a reserved fast lane, enough hung slow
        submissions will consume every dispatcher slot and the user is
        locked out of `status`/`cancel` until the process is restarted —
        exactly the deadlock-of-recovery scenario the adversarial review
        flagged."""
        import io
        import time as _time

        from tine.mcp import run_stdio

        # Saturate the dispatcher with slow execute_branch calls.
        # `concurrency=4` is the dispatcher cap; we send 4 slow ones.
        concurrency = 4
        slow_count = concurrency
        slow_requests = "\n".join(
            json.dumps(
                {
                    "jsonrpc": "2.0",
                    "id": 200 + i,
                    "method": "tools/call",
                    "params": {
                        "name": "execute_branch",
                        "arguments": {
                            "experiment_id": "tree_1",
                            "branch_id": "main",
                        },
                    },
                }
            )
            for i in range(slow_count)
        )
        # Recovery tool — `status` is fast and stateless, must reach the
        # API even when execute submissions are wedged.
        fast_request = json.dumps(
            {
                "jsonrpc": "2.0",
                "id": 777,
                "method": "tools/call",
                "params": {
                    "name": "status",
                    "arguments": {"execution_id": "exec_1"},
                },
            }
        )

        stdin = io.StringIO(slow_requests + "\n" + fast_request + "\n")
        stdout = _ThreadSafeStringIO()

        loop_thread = threading.Thread(
            target=run_stdio,
            args=(self.server, stdin, stdout),
            kwargs={"concurrency": concurrency},
            daemon=True,
        )
        loop_thread.start()

        # Wait up to 3s for the fast recovery tool to land. It MUST come
        # back with a real `result` — not an overload `error` — even
        # though all `concurrency` slow slots are wedged.
        deadline = _time.monotonic() + 3.0
        fast_response = None
        while _time.monotonic() < deadline:
            for line in stdout.lines():
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if payload.get("id") == 777:
                    fast_response = payload
                    break
            if fast_response is not None:
                break
            _time.sleep(0.05)

        self.assertIsNotNone(
            fast_response,
            f"recovery tool (id=777, status) did not receive any response within 3s "
            f"while {slow_count} execute_branch submissions were wedged "
            f"(concurrency cap={concurrency}); the dispatcher is locking out recovery",
        )
        self.assertIn(
            "result",
            fast_response,
            f"recovery tool (id=777, status) was rejected with overload error "
            f"instead of executing — slow execute submissions are starving "
            f"fast recovery tools out of dispatcher slots: {fast_response}",
        )

        # Release the slow endpoint and drain the loop.
        _PauseHandler.pause_event.set()
        loop_thread.join(timeout=10.0)


class _ThreadSafeStringIO:
    """Minimal thread-safe sink for run_stdio. Writes are line-buffered and
    we expose an explicit `wait_for_lines(n, timeout)` so tests can observe
    response arrival timing without polling."""

    def __init__(self) -> None:
        self._cond = threading.Condition()
        self._buf: list[str] = []

    def write(self, s: str) -> int:
        with self._cond:
            self._buf.append(s)
            self._cond.notify_all()
        return len(s)

    def flush(self) -> None:
        return None

    def _lines_locked(self) -> list[str]:
        joined = "".join(self._buf)
        return [line for line in joined.splitlines() if line.strip()]

    def lines(self) -> list[str]:
        with self._cond:
            return self._lines_locked()

    def wait_for_lines(self, n: int, timeout: float) -> bool | None:
        import time as _time

        deadline = _time.monotonic() + timeout if timeout is not None else None
        with self._cond:
            while len(self._lines_locked()) < n:
                remaining = None
                if deadline is not None:
                    remaining = deadline - _time.monotonic()
                    if remaining <= 0:
                        return None
                self._cond.wait(timeout=remaining)
        return True


if __name__ == "__main__":
    unittest.main()
