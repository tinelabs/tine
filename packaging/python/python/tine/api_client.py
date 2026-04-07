from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from urllib import error, request


@dataclass
class _Response:
    status: int
    body: bytes


class TineApiClient:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")

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
        return self._post_json("/api/experiment-trees", body)

    def save_experiment_tree(self, definition: dict[str, Any]) -> dict[str, Any]:
        tree_id = self._require(definition, "id")
        response = self._put_json(f"/api/experiment-trees/{tree_id}", definition)
        if isinstance(response, dict):
            return response
        raise RuntimeError(f"unexpected save response: {response!r}")

    def get_experiment_tree(self, experiment_id: str) -> dict[str, Any]:
        return self._get_json(f"/api/experiment-trees/{experiment_id}")

    def rename_experiment_tree(self, experiment_id: str, name: str) -> None:
        self._post_no_content(
            f"/api/experiment-trees/{experiment_id}/rename", {"name": name}
        )

    def delete_experiment_tree(self, experiment_id: str) -> None:
        self._delete_no_content(f"/api/experiment-trees/{experiment_id}")

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
        response = self._post_json(f"/api/experiment-trees/{experiment_id}/branches", body)
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
        )

    def delete_cell_from_experiment_tree_branch(
        self,
        experiment_id: str,
        branch_id: str,
        cell_id: str,
    ) -> None:
        self._delete_no_content(
            f"/api/experiment-trees/{experiment_id}/branches/{branch_id}/cells/{cell_id}"
        )

    def delete_experiment_tree_branch(
        self,
        experiment_id: str,
        branch_id: str,
    ) -> None:
        self._delete_no_content(
            f"/api/experiment-trees/{experiment_id}/branches/{branch_id}"
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

    def execute_branch_in_experiment_tree(
        self,
        experiment_id: str,
        branch_id: str,
    ) -> dict[str, Any]:
        return self._post_json(
            f"/api/experiment-trees/{experiment_id}/branches/{branch_id}/execute", None
        )

    def execute_cell_in_experiment_tree_branch(
        self,
        experiment_id: str,
        branch_id: str,
        cell_id: str,
    ) -> dict[str, Any]:
        return self._post_json(
            f"/api/experiment-trees/{experiment_id}/branches/{branch_id}/cells/{cell_id}/execute",
            None,
        )

    def execute_all_branches_in_experiment_tree(
        self,
        experiment_id: str,
    ) -> list[dict[str, Any]]:
        response = self._post_json(
            f"/api/experiment-trees/{experiment_id}/execute-all-branches", None
        )
        return self._require(response, "executions")

    def cancel(self, execution_id: str) -> None:
        self._post_no_content(f"/api/executions/{execution_id}/cancel", None)

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
        response = self._post_json("/api/projects", body)
        return self._require(response, "id")

    def list_projects(self) -> list[dict[str, Any]]:
        return self._get_json("/api/projects")

    def get_project(self, project_id: str) -> dict[str, Any]:
        return self._get_json(f"/api/projects/{project_id}")

    def list_experiments(self, project_id: str) -> list[dict[str, Any]]:
        return self._get_json(f"/api/projects/{project_id}/experiments")

    def _get_json(self, path: str) -> Any:
        response = self._request("GET", path)
        return self._decode_json(response)

    def _post_json(self, path: str, body: Any) -> Any:
        response = self._request("POST", path, body)
        return self._decode_json(response)

    def _put_json(self, path: str, body: Any) -> Any:
        response = self._request("PUT", path, body)
        return self._decode_json(response)

    def _post_no_content(self, path: str, body: Any) -> None:
        self._request("POST", path, body)

    def _delete_no_content(self, path: str) -> None:
        self._request("DELETE", path)

    def _request(self, method: str, path: str, body: Any = None) -> _Response:
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
        try:
            with request.urlopen(req) as resp:
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
            raise RuntimeError(f"failed to reach Tine API at {self.base_url}: {exc.reason}") from None

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
