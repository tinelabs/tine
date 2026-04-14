#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from urllib import request


REQUEST_TIMEOUT_SECONDS = 5.0
EXECUTION_TIMEOUT_SECONDS = 180.0
DOCTOR_REQUEST_TIMEOUT_SECONDS = 300.0
EMBEDDED_SERVER_BIND_ENV = "TINE_EMBEDDED_SERVER_BIND"


def run_capture(command: list[str]) -> str:
    return subprocess.check_output(command, text=True)


def verify_bundle_exists(bundle_path: Path) -> None:
    if not bundle_path.exists():
        raise RuntimeError(f"bundle not found at {bundle_path}")


def candidate_pids(pid: int) -> set[int]:
    pids = {pid}
    if sys.platform == "win32":
        return pids

    pending = [pid]
    while pending:
        parent_pid = pending.pop()
        try:
            output = run_capture(["pgrep", "-P", str(parent_pid)])
        except (FileNotFoundError, subprocess.CalledProcessError):
            continue

        for line in output.splitlines():
            child_pid = line.strip()
            if not child_pid.isdigit():
                continue
            child = int(child_pid)
            if child in pids:
                continue
            pids.add(child)
            pending.append(child)

    return pids


def listening_ports(pid: int) -> set[int]:
    if sys.platform == "win32":
        command = [
            "powershell",
            "-NoProfile",
            "-Command",
            (
                f"Get-NetTCPConnection -State Listen -OwningProcess {pid} | "
                "Select-Object -ExpandProperty LocalPort"
            ),
        ]
        try:
            output = run_capture(command)
        except subprocess.CalledProcessError:
            return set()
        return {int(line.strip()) for line in output.splitlines() if line.strip().isdigit()}

    command = ["lsof", "-nP", "-iTCP", "-sTCP:LISTEN", "-a"]
    for candidate_pid in sorted(candidate_pids(pid)):
        command.extend(["-p", str(candidate_pid)])
    try:
        output = run_capture(command)
    except subprocess.CalledProcessError:
        return set()

    ports = set()
    for line in output.splitlines()[1:]:
        fields = line.split()
        if not fields:
            continue
        address = fields[-1]
        if ":" not in address:
            continue
        try:
            ports.add(int(address.rsplit(":", 1)[1]))
        except ValueError:
            continue
    return ports


def wait_for_health(process: subprocess.Popen[str], timeout_seconds: int = 60) -> str:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"app exited before becoming healthy with code {process.returncode}")

        for port in sorted(listening_ports(process.pid)):
            base = f"http://127.0.0.1:{port}"
            try:
                with request.urlopen(base + "/healthz", timeout=1) as response:
                    if response.read().decode().strip() == "ok":
                        return base
            except Exception:
                continue
        time.sleep(0.5)

    raise RuntimeError("app did not expose a healthy embedded server in time")


def wait_for_health_at(base_url: str, process: subprocess.Popen[str], timeout_seconds: int = 60) -> str:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"app exited before becoming healthy with code {process.returncode}")

        try:
            with request.urlopen(base_url + "/healthz", timeout=1) as response:
                if response.read().decode().strip() == "ok":
                    return base_url
        except Exception:
            pass
        time.sleep(0.5)

    raise RuntimeError(f"app did not expose a healthy embedded server at {base_url} in time")


def http_post(url: str, payload: dict | None) -> str:
    data = None if payload is None else json.dumps(payload).encode()
    headers = {} if payload is None else {"Content-Type": "application/json"}
    req = request.Request(url, data=data, headers=headers, method="POST")
    with request.urlopen(req) as response:
        return response.read().decode()


def http_get_json(url: str, timeout_seconds: float = REQUEST_TIMEOUT_SECONDS) -> dict:
    with request.urlopen(url, timeout=timeout_seconds) as response:
        return json.loads(response.read().decode())


def wait_for_execution(base_url: str, execution_id: str, timeout_seconds: float = EXECUTION_TIMEOUT_SECONDS) -> dict:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        with request.urlopen(
            base_url + f"/api/executions/{execution_id}", timeout=REQUEST_TIMEOUT_SECONDS
        ) as response:
            payload = json.loads(response.read().decode())
        if payload.get("finished_at"):
            return payload
        time.sleep(0.5)
    raise RuntimeError(f"execution {execution_id} did not finish during smoke")


def run_execution_smoke(base_url: str) -> None:
    project_id = json.loads(
        http_post(base_url + "/api/projects", {"name": "desktop-smoke-project", "workspace_dir": "project-a"})
    )["id"]
    tree = json.loads(
        http_post(
            base_url + "/api/experiment-trees",
            {"name": "desktop-smoke-experiment", "project_id": project_id},
        )
    )
    tree_id = tree["id"]
    http_post(
        base_url + f"/api/experiment-trees/{tree_id}/branches/main/cells/cell_1/code",
        {"source": 'print("hi")\nvalue = 1\n'},
    )
    execution_id = json.loads(
        http_post(base_url + f"/api/experiment-trees/{tree_id}/branches/main/execute", None)
    )["execution_id"]
    status = wait_for_execution(base_url, execution_id)
    if status.get("status") != "completed":
        raise RuntimeError(f"main branch execution failed during smoke: {status}")

    branch_id = json.loads(
        http_post(
            base_url + f"/api/experiment-trees/{tree_id}/branches",
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
        base_url + f"/api/experiment-trees/{tree_id}/branches/{branch_id}/cells",
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
        http_post(base_url + f"/api/experiment-trees/{tree_id}/branches/{branch_id}/execute", None)
    )["execution_id"]
    branch_status = wait_for_execution(base_url, branch_execution_id)
    if branch_status.get("status") != "completed":
        raise RuntimeError(f"branch execution failed during smoke: {branch_status}")


def run_doctor_smoke(base_url: str) -> None:
    payload = http_get_json(
        base_url + "/api/system/doctor",
        timeout_seconds=DOCTOR_REQUEST_TIMEOUT_SECONDS,
    )
    if payload.get("ok"):
        return

    failing_checks = [check for check in payload.get("checks", []) if not check.get("ok")]
    detail = json.dumps(failing_checks or payload, indent=2)
    raise RuntimeError(f"doctor reported blocking issues:\n{detail}")


def terminate_process(process: subprocess.Popen[str]) -> str:
    output = ""
    process.terminate()
    try:
        process.wait(timeout=15)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=15)
    if process.stdout is not None:
        output = process.stdout.read()
    return output


def assert_bundled_python(output: str) -> None:
    if "using bundled python" not in output:
        raise RuntimeError("smoke run did not confirm bundled Python activation")
    if "no bundled runtime found; falling back to host Python discovery" in output:
        raise RuntimeError("smoke run fell back to host Python discovery")


def launch_app(launch_path: Path, workspace_dir: Path, server_port: int | None) -> subprocess.Popen[str]:
    env = os.environ.copy()
    if server_port is not None:
        env[EMBEDDED_SERVER_BIND_ENV] = f"127.0.0.1:{server_port}"
    return subprocess.Popen(
        [str(launch_path), str(workspace_dir)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=env,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Smoke-test a built Tine desktop app bundle and its embedded server."
    )
    parser.add_argument("--bundle-path", required=True, help="Path to the built platform bundle.")
    parser.add_argument(
        "--launch-path",
        help="Path to a launchable app binary for L2/L3 checks. If omitted, only L1 bundle validation runs.",
    )
    parser.add_argument(
        "--require-bundled-python",
        action="store_true",
        help="Fail if the app logs do not confirm bundled Python was activated.",
    )
    parser.add_argument(
        "--health-only",
        action="store_true",
        help="Only verify the app launches and serves healthz; skip execution smoke.",
    )
    parser.add_argument(
        "--doctor-runtime",
        action="store_true",
        help="Run the app's fast doctor/runtime-preflight endpoint after healthz.",
    )
    parser.add_argument(
        "--server-port",
        type=int,
        help="Fixed embedded server port to request from the app and probe directly.",
    )
    args = parser.parse_args(argv)

    bundle_path = Path(args.bundle_path).resolve()
    verify_bundle_exists(bundle_path)

    if not args.launch_path:
        print(f"bundle smoke passed: {bundle_path}")
        return 0

    launch_path = Path(args.launch_path).resolve()
    if not launch_path.exists():
        raise RuntimeError(f"launch path not found at {launch_path}")

    with tempfile.TemporaryDirectory(prefix="tine-app-smoke-") as temp_root:
        workspace_dir = Path(temp_root) / "workspace"
        workspace_dir.mkdir()
        process = launch_app(launch_path, workspace_dir, args.server_port)
        failure: Exception | None = None
        try:
            if args.server_port is not None:
                base_url = wait_for_health_at(f"http://127.0.0.1:{args.server_port}", process)
            else:
                base_url = wait_for_health(process)
            if args.doctor_runtime:
                run_doctor_smoke(base_url)
            if not args.health_only:
                run_execution_smoke(base_url)
        except Exception as exc:
            failure = exc
        finally:
            output = terminate_process(process)

        if args.require_bundled_python:
            assert_bundled_python(output)

        if failure is not None:
            detail = f"{failure}\n\n--- app output ---\n{output}" if output else str(failure)
            raise RuntimeError(detail) from failure

    print(f"desktop app smoke passed: {bundle_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
