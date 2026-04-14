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
    last_create_branch_payload: dict[str, object] | None = None
    last_add_cell_payload: dict[str, object] | None = None
    last_update_cell_payload: dict[str, object] | None = None
    last_move_cell_payload: dict[str, object] | None = None
    deleted_path: str | None = None
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
        self._json(404, {"error": "not found"})

    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length).decode("utf-8") if length else ""
        payload = json.loads(body) if body else {}
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
        self._json(404, {"error": "not found"})

    def do_PUT(self) -> None:  # noqa: N802
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length).decode("utf-8") if length else ""
        payload = json.loads(body) if body else {}
        if self.path == "/api/experiment-trees/tree_1":
            self._json(200, payload)
            return
        self._json(404, {"error": "not found"})

    def do_DELETE(self) -> None:  # noqa: N802
        if self.path in {
            "/api/experiment-trees/tree_1/branches/main/cells/cell_1",
            "/api/experiment-trees/tree_1/branches/branch_1",
        }:
            type(self).deleted_path = self.path
            self.send_response(204)
            self.end_headers()
            return
        self._json(404, {"error": "not found"})

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
        _Handler.last_create_branch_payload = None
        _Handler.last_add_cell_payload = None
        _Handler.last_update_cell_payload = None
        _Handler.last_move_cell_payload = None
        _Handler.deleted_path = None
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
        self.assertEqual(
            restart_kernel.content[0]["text"],
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
        self.assertEqual(
            cancel.content[0]["text"],
            "Cancellation requested for execution exec_1",
        )

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
        self.assertEqual(result.content[0]["text"], "Branch created: branch_1")
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
        self.assertEqual(result.content[0]["text"], "Cell added to branch main")
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
        self.assertEqual(updated.content[0]["text"], "Cell cell_1 updated in branch main")
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
        self.assertEqual(moved.content[0]["text"], "Cell cell_1 moved down in branch main")
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
        self.assertEqual(deleted.content[0]["text"], "Cell cell_1 deleted from branch main")
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
        self.assertEqual(result.content[0]["text"], "Branch branch_1 deleted")
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


if __name__ == "__main__":
    unittest.main()
