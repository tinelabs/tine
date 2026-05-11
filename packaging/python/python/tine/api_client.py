from __future__ import annotations

import json
import socket
from dataclasses import dataclass
from typing import Any
from urllib import error, request

# `socket.timeout` is its own class on Python 3.9 and an alias for
# `TimeoutError` on 3.10+. Catch the platform variant explicitly.
_SOCKET_TIMEOUT = socket.timeout


@dataclass
class _Response:
    status: int
    body: bytes


# Default request timeout for idempotent calls (GETs, status reads, log
# reads, etc). Bounds the worst-case wedge where a stalled server would
# block urlopen indefinitely.
_DEFAULT_REQUEST_TIMEOUT_SECS: float = 30.0
# Long-but-bounded timeout for non-idempotent execute submissions. Sized
# generously above realistic cold-venv-setup duration (typically 30–120 s
# on a fresh machine) so legitimate slow submissions complete normally,
# while still bounding the worst case so an orphaned submission becomes
# visible in finite time. On timeout we surface a *duplicate-risk*
# warning so callers cannot blindly retry — a true idempotency-token
# fix needs server cooperation and is deferred to a future release.
_LONG_REQUEST_TIMEOUT_SECS: float = 600.0
# Sentinel distinguishing "no timeout argument provided → use the
# client's default" from "explicitly None → no timeout at all".
_USE_DEFAULT_TIMEOUT = object()


class TineApiClient:
    def __init__(
        self,
        base_url: str,
        *,
        default_timeout_secs: float = _DEFAULT_REQUEST_TIMEOUT_SECS,
        long_timeout_secs: float = _LONG_REQUEST_TIMEOUT_SECS,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self._default_timeout = default_timeout_secs
        self._long_timeout = long_timeout_secs

    def list_experiment_trees(self) -> list[dict[str, Any]]:
        return self._get_json("/api/experiment-trees")

    def create_experiment_tree(
        self,
        name: str,
        project_id: str | None = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"name": name}
        if project_id is not None:
            body["project_id"] = project_id
        return self._post_json("/api/experiment-trees", body, non_idempotent=True)

    def save_experiment_tree(self, definition: dict[str, Any]) -> dict[str, Any]:
        tree_id = self._require(definition, "id")
        response = self._put_json(
            f"/api/experiment-trees/{tree_id}", definition, non_idempotent=True
        )
        if isinstance(response, dict):
            return response
        raise RuntimeError(f"unexpected save response: {response!r}")

    def get_experiment_tree(self, experiment_id: str) -> dict[str, Any]:
        return self._get_json(f"/api/experiment-trees/{experiment_id}")

    def rename_experiment_tree(self, experiment_id: str, name: str) -> None:
        self._post_no_content(
            f"/api/experiment-trees/{experiment_id}/rename",
            {"name": name},
            non_idempotent=True,
        )

    def delete_experiment_tree(self, experiment_id: str) -> None:
        self._delete_no_content(
            f"/api/experiment-trees/{experiment_id}", non_idempotent=True
        )

    def create_branch_in_experiment_tree(
        self,
        experiment_id: str,
        parent_branch_id: str,
        name: str,
        branch_point_cell_id: str,
        first_cell: dict[str, Any],
    ) -> str:
        body = {
            "parent_branch_id": parent_branch_id,
            "name": name,
            "branch_point_cell_id": branch_point_cell_id,
            "first_cell": first_cell,
        }
        response = self._post_json(
            f"/api/experiment-trees/{experiment_id}/branches",
            body,
            non_idempotent=True,
        )
        if isinstance(response, str):
            return response
        raise RuntimeError(f"unexpected branch response: {response!r}")

    def add_cell_to_experiment_tree_branch(
        self,
        experiment_id: str,
        branch_id: str,
        cell: dict[str, Any],
        after_cell_id: str | None = None,
    ) -> None:
        body: dict[str, Any] = {"cell": cell}
        if after_cell_id is not None:
            body["after_cell_id"] = after_cell_id
        self._post_no_content(
            f"/api/experiment-trees/{experiment_id}/branches/{branch_id}/cells",
            body,
            non_idempotent=True,
        )

    def update_cell_code_in_experiment_tree_branch(
        self,
        experiment_id: str,
        branch_id: str,
        cell_id: str,
        source: str,
    ) -> None:
        self._post_no_content(
            f"/api/experiment-trees/{experiment_id}/branches/{branch_id}/cells/{cell_id}/code",
            {"source": source},
            non_idempotent=True,
        )

    def move_cell_in_experiment_tree_branch(
        self,
        experiment_id: str,
        branch_id: str,
        cell_id: str,
        direction: str,
    ) -> None:
        self._post_no_content(
            f"/api/experiment-trees/{experiment_id}/branches/{branch_id}/cells/{cell_id}/move",
            {"direction": direction},
            non_idempotent=True,
        )

    def delete_cell_from_experiment_tree_branch(
        self,
        experiment_id: str,
        branch_id: str,
        cell_id: str,
    ) -> None:
        self._delete_no_content(
            f"/api/experiment-trees/{experiment_id}/branches/{branch_id}/cells/{cell_id}",
            non_idempotent=True,
        )

    def delete_experiment_tree_branch(
        self,
        experiment_id: str,
        branch_id: str,
    ) -> None:
        self._delete_no_content(
            f"/api/experiment-trees/{experiment_id}/branches/{branch_id}",
            non_idempotent=True,
        )

    def inspect_cell_in_experiment_tree_branch(
        self,
        experiment_id: str,
        branch_id: str,
        cell_id: str,
    ) -> dict[str, Any]:
        return self._get_json(
            f"/api/experiment-trees/{experiment_id}/branches/{branch_id}/cells/{cell_id}/inspect"
        )

    def inspect_experiment_tree_kernel(self, experiment_id: str) -> dict[str, Any]:
        return self._get_json(f"/api/experiment-trees/{experiment_id}/inspect-kernel")

    def restart_experiment_tree_kernel(self, experiment_id: str) -> None:
        self._post_no_content(
            f"/api/experiment-trees/{experiment_id}/restart-kernel",
            None,
            non_idempotent=True,
        )

    def execute_branch_in_experiment_tree(
        self,
        experiment_id: str,
        branch_id: str,
    ) -> dict[str, Any]:
        # Long-but-bounded timeout. A pure `timeout=None` would prevent
        # client-side retries that could double-submit, but it also makes
        # an orphaned submission un-cancellable: the server may have
        # accepted the run while the client never sees the execution_id.
        # The compromise: 600s (10 min) is well above realistic cold-
        # venv-setup time so normal flows are unaffected, but the
        # timeout-error message explicitly warns about duplicate-risk so
        # an agent / user does not blindly retry. A proper fix needs a
        # server-side idempotency token and is tracked for a future
        # release.
        return self._post_json(
            f"/api/experiment-trees/{experiment_id}/branches/{branch_id}/execute",
            None,
            timeout=self._long_timeout,
            non_idempotent=True,
        )

    def execute_cell_in_experiment_tree_branch(
        self,
        experiment_id: str,
        branch_id: str,
        cell_id: str,
    ) -> dict[str, Any]:
        # See `execute_branch_in_experiment_tree`.
        return self._post_json(
            f"/api/experiment-trees/{experiment_id}/branches/{branch_id}/cells/{cell_id}/execute",
            None,
            timeout=self._long_timeout,
            non_idempotent=True,
        )

    def execute_all_branches_in_experiment_tree(
        self,
        experiment_id: str,
    ) -> list[dict[str, Any]]:
        # See `execute_branch_in_experiment_tree`.
        response = self._post_json(
            f"/api/experiment-trees/{experiment_id}/execute-all-branches",
            None,
            timeout=self._long_timeout,
            non_idempotent=True,
        )
        return self._require(response, "executions")

    def cancel(self, execution_id: str) -> None:
        self._post_no_content(
            f"/api/executions/{execution_id}/cancel", None, non_idempotent=True
        )

    def status(self, execution_id: str) -> dict[str, Any]:
        return self._get_json(f"/api/executions/{execution_id}")

    def logs_for_tree_cell(
        self,
        experiment_id: str,
        branch_id: str,
        cell_id: str,
    ) -> dict[str, Any]:
        return self._get_json(
            f"/api/experiment-trees/{experiment_id}/branches/{branch_id}/cells/{cell_id}/logs"
        )

    def create_project(
        self,
        name: str,
        workspace_dir: str,
        description: str | None = None,
    ) -> str:
        body: dict[str, Any] = {"name": name, "workspace_dir": workspace_dir}
        if description is not None:
            body["description"] = description
        response = self._post_json("/api/projects", body, non_idempotent=True)
        return self._require(response, "id")

    def list_projects(self) -> list[dict[str, Any]]:
        return self._get_json("/api/projects")

    def get_project(self, project_id: str) -> dict[str, Any]:
        return self._get_json(f"/api/projects/{project_id}")

    def list_experiments(self, project_id: str) -> list[dict[str, Any]]:
        return self._get_json(f"/api/projects/{project_id}/experiments")

    def _get_json(self, path: str, *, timeout: Any = _USE_DEFAULT_TIMEOUT) -> Any:
        response = self._request("GET", path, timeout=timeout)
        return self._decode_json(response)

    def _post_json(
        self,
        path: str,
        body: Any,
        *,
        timeout: Any = _USE_DEFAULT_TIMEOUT,
        non_idempotent: bool = False,
    ) -> Any:
        response = self._request(
            "POST", path, body, timeout=timeout, non_idempotent=non_idempotent
        )
        return self._decode_json(response)

    def _put_json(
        self,
        path: str,
        body: Any,
        *,
        timeout: Any = _USE_DEFAULT_TIMEOUT,
        non_idempotent: bool = False,
    ) -> Any:
        response = self._request(
            "PUT", path, body, timeout=timeout, non_idempotent=non_idempotent
        )
        return self._decode_json(response)

    def _post_no_content(
        self,
        path: str,
        body: Any,
        *,
        timeout: Any = _USE_DEFAULT_TIMEOUT,
        non_idempotent: bool = False,
    ) -> None:
        self._request("POST", path, body, timeout=timeout, non_idempotent=non_idempotent)

    def _delete_no_content(
        self,
        path: str,
        *,
        timeout: Any = _USE_DEFAULT_TIMEOUT,
        non_idempotent: bool = False,
    ) -> None:
        self._request("DELETE", path, timeout=timeout, non_idempotent=non_idempotent)

    def _request(
        self,
        method: str,
        path: str,
        body: Any = None,
        *,
        timeout: Any = _USE_DEFAULT_TIMEOUT,
        non_idempotent: bool = False,
    ) -> _Response:
        data = None
        headers = {"Accept": "application/json"}
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"

        req = request.Request(
            f"{self.base_url}{path}",
            data=data,
            headers=headers,
            method=method,
        )
        # Three modes:
        #   - sentinel `_USE_DEFAULT_TIMEOUT` → use the client's default
        #   - explicit `None` → no timeout at all (urlopen blocks
        #     until the server replies or the connection drops)
        #   - explicit number → use that many seconds
        if timeout is _USE_DEFAULT_TIMEOUT:
            request_timeout: float | None = self._default_timeout
        else:
            request_timeout = timeout
        try:
            if request_timeout is None:
                cm = request.urlopen(req)
            else:
                cm = request.urlopen(req, timeout=request_timeout)
            with cm as resp:
                return _Response(status=resp.status, body=resp.read())
        except error.HTTPError as exc:
            body_text = exc.read().decode("utf-8", errors="replace")
            try:
                parsed = json.loads(body_text)
            except json.JSONDecodeError:
                parsed = None
            if isinstance(parsed, dict) and "error" in parsed:
                raise RuntimeError(str(parsed["error"])) from None
            raise RuntimeError(
                f"request failed with status {exc.code}: {body_text or exc.reason}"
            ) from None
        except error.URLError as exc:
            # urllib raises URLError for both socket.timeout (transport stall)
            # and connection refused / DNS errors. Surface the common-cause
            # symptom clearly so MCP tool errors point at the right thing.
            import socket

            if isinstance(exc.reason, socket.timeout) or isinstance(exc, TimeoutError):
                raise RuntimeError(
                    self._format_timeout_message(path, request_timeout, non_idempotent)
                ) from None
            raise RuntimeError(
                f"failed to reach Tine API at {self.base_url}: {exc.reason}"
            ) from None
        except (TimeoutError, _SOCKET_TIMEOUT):
            # `socket.timeout` is a distinct exception type on Python 3.9
            # and below (alias for TimeoutError on 3.10+). Some urllib code
            # paths re-raise it directly without wrapping in URLError.
            raise RuntimeError(
                self._format_timeout_message(path, request_timeout, non_idempotent)
            ) from None

    @staticmethod
    def _format_timeout_message(
        path: str,
        request_timeout: float | None,
        non_idempotent: bool,
    ) -> str:
        seconds = f"{request_timeout:.0f}s" if request_timeout is not None else "unbounded"
        base = (
            f"Tine API call to {path} timed out after {seconds}; "
            "the local server may be stalled or restarting"
        )
        if not non_idempotent:
            return base
        # The submission may have been accepted by the server even though
        # the client never received the execution_id. A blind retry can
        # produce duplicate runs. Tell the agent / user to recover via
        # inspection rather than retrying immediately.
        return (
            f"{base}. WARNING: this was a non-idempotent submission "
            "(write request); the server may have already accepted or applied "
            "the change. DO NOT retry this call without first checking the "
            "Tine UI or experiment-tree state for the intended mutation. "
            "Retrying blindly risks duplicate or conflicting state."
        )

    @staticmethod
    def _decode_json(response: _Response) -> Any:
        if not response.body:
            return None
        return json.loads(response.body.decode("utf-8"))

    @staticmethod
    def _require(payload: dict[str, Any], key: str) -> Any:
        if key not in payload:
            raise RuntimeError(f"response missing {key}")
        return payload[key]
