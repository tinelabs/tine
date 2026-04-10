#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import re
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from urllib import request


def package_version(repo_root: Path) -> str:
    text = (repo_root / "packaging/python/pyproject.toml").read_text()
    match = re.search(r"(?m)^\[project\]\s*(?:.*\n)*?version = \"([^\"]+)\"", text)
    if not match:
        raise RuntimeError("failed to locate project.version in packaging/python/pyproject.toml")
    return match.group(1)


def run(command: list[str], env: dict[str, str]) -> None:
    subprocess.run(command, check=True, env=env)


def run_capture(command: list[str], env: dict[str, str]) -> str:
    return subprocess.check_output(command, env=env, text=True)


def run_server_e2e(python: str, env: dict[str, str]) -> None:
    with tempfile.TemporaryDirectory(prefix="tine-wrapper-smoke-") as temp_root:
        workspace = Path(temp_root) / "workspace"
        workspace.mkdir()
        bind = f"127.0.0.1:{pick_free_port()}"
        server = subprocess.Popen(
            [python, "-m", "tine.cli", "serve", "--workspace", str(workspace), "--bind", bind],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        failure: Exception | None = None
        try:
            base = f"http://{bind}"
            for _ in range(80):
                try:
                    with request.urlopen(base + "/healthz") as response:
                        if response.read().decode().strip() == "ok":
                            break
                except Exception:
                    pass
                time.sleep(0.5)
            else:
                raise RuntimeError("wrapper smoke server did not become ready")

            project_id = json.loads(
                http_post(base + "/api/projects", {"name": "smoke-project", "workspace_dir": "project-a"})
            )["id"]
            tree = json.loads(
                http_post(base + "/api/experiment-trees", {"name": "smoke-experiment", "project_id": project_id})
            )
            tree_id = tree["id"]
            http_post(
                base + f"/api/experiment-trees/{tree_id}/branches/main/cells/cell_1/code",
                {"source": 'print("smoke")\ncell_1 = 1\n'},
            )
            execution_id = json.loads(
                http_post(base + f"/api/experiment-trees/{tree_id}/branches/main/execute", None)
            )["execution_id"]
            status = wait_for_execution(base, execution_id)
            if status["status"] != "completed":
                raise RuntimeError(f"main branch execution failed during smoke: {status}")
            branch_id = json.loads(
                http_post(
                    base + f"/api/experiment-trees/{tree_id}/branches",
                    {
                        "parent_branch_id": "main",
                        "name": "branch-a",
                        "branch_point_cell_id": "cell_1",
                        "first_cell": {
                            "id": "branch_cell_1",
                            "tree_id": tree_id,
                            "branch_id": "ignored",
                            "name": "branch_cell_1",
                            "code": {"source": "branch_value = 2\n", "language": "python"},
                            "upstream_cell_ids": [],
                            "declared_outputs": [],
                            "cache": False,
                            "map_over": None,
                            "map_concurrency": None,
                            "tags": {},
                            "revision_id": None,
                            "state": "clean",
                        },
                    },
                )
            )
            http_post(
                base + f"/api/experiment-trees/{tree_id}/branches/{branch_id}/cells",
                {
                    "cell": {
                        "id": "branch_cell_2",
                        "tree_id": tree_id,
                        "branch_id": branch_id,
                        "name": "branch_cell_2",
                        "code": {"source": "branch_value_2 = branch_value + 1\n", "language": "python"},
                        "upstream_cell_ids": [],
                        "declared_outputs": [],
                        "cache": False,
                        "map_over": None,
                        "map_concurrency": None,
                        "tags": {},
                        "revision_id": None,
                        "state": "clean",
                    },
                    "after_cell_id": "branch_cell_1",
                },
            )
            branch_execution_id = json.loads(
                http_post(base + f"/api/experiment-trees/{tree_id}/branches/{branch_id}/execute", None)
            )["execution_id"]
            branch_status = wait_for_execution(base, branch_execution_id)
            if branch_status["status"] != "completed":
                raise RuntimeError(f"branch execution failed during smoke: {branch_status}")
        except Exception as exc:
            failure = exc
        finally:
            server.terminate()
            try:
                server.wait(timeout=10)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait(timeout=10)

            output = ""
            if server.stdout is not None:
                output = server.stdout.read()

            if failure is not None:
                detail = f"{failure}\n\n--- server output ---\n{output}" if output else str(failure)
                raise RuntimeError(detail) from failure


def pick_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def http_post(url: str, payload: dict | None) -> str:
    data = None if payload is None else json.dumps(payload).encode()
    headers = {} if payload is None else {"Content-Type": "application/json"}
    request_obj = request.Request(url, data=data, headers=headers, method="POST")
    with request.urlopen(request_obj) as response:
        return response.read().decode()


def wait_for_execution(base: str, execution_id: str) -> dict:
    for _ in range(160):
        with request.urlopen(base + f"/api/executions/{execution_id}") as response:
            payload = json.loads(response.read().decode())
        if payload.get("finished_at"):
            return payload
        time.sleep(0.5)
    raise RuntimeError(f"execution {execution_id} did not finish during smoke")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run wrapper smoke checks against a built Tine engine binary."
    )
    parser.add_argument("--repo-root", default=".", help="Path to the repository root.")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--binary", help="Path to the built `tine` engine binary.")
    source.add_argument(
        "--release-artifact-dir",
        help="Directory containing the release archive and checksum for the current platform.",
    )
    parser.add_argument(
        "--python",
        default=sys.executable,
        help="Python interpreter to use for smoke tests.",
    )
    args = parser.parse_args(argv)

    repo_root = Path(args.repo_root).resolve()
    env = os.environ.copy()
    env["PYTHONPATH"] = str(repo_root / "packaging/python/python")
    env["TINE_PACKAGE_VERSION"] = package_version(repo_root)
    env["TINE_CACHE_DIR"] = str(repo_root / ".tmp" / "wrapper-smoke-cache")

    if args.binary:
        env["TINE_BIN"] = str(Path(args.binary).resolve())
    else:
        env["TINE_RELEASE_BASE_URL"] = Path(args.release_artifact_dir).resolve().as_uri() + "/"

    run([args.python, "-m", "tine.cli", "version"], env)
    run([args.python, "-m", "tine.cli", "mcp", "serve", "--help"], env)
    normalized = run_capture(
        [
            args.python,
            "-c",
            (
                "import json; "
                "from tine.mcp import _cell_payload; "
                "payload = _cell_payload({'name': 'draft'}, object_key='first_cell', experiment_id='tree_1'); "
                "print(json.dumps(payload, sort_keys=True))"
            ),
        ],
        env,
    )
    normalized_payload = json.loads(normalized)
    assert normalized_payload["tree_id"] == "tree_1"
    assert normalized_payload["branch_id"] == "ignored"
    assert normalized_payload["code"]["language"] == "python"
    run_server_e2e(args.python, env)
    print("wrapper smoke checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
