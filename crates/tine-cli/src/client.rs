//! Thin HTTP client for the Tine server API.
//!
//! The CLI is a front door over the canonical server contract: it shapes
//! requests and formats responses, but owns no execution semantics. Routing
//! everything through the running server keeps a single queue, kernel
//! manager, and SQLite writer per workspace — a CLI-side `Workspace::open`
//! would race the server (including startup reconciliation marking the
//! server's live executions as failed) and its spawned executions would die
//! with the CLI process.

use std::time::Duration;

use serde_json::Value;

/// Default timeout for idempotent calls (reads, status polls).
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
/// Long-but-bounded timeout for non-idempotent execute submissions; sized
/// above realistic cold-venv setup so legitimate slow submissions complete.
const LONG_TIMEOUT: Duration = Duration::from_secs(600);
/// Poll cadence while waiting for an execution to reach a terminal state.
const WAIT_POLL_INTERVAL: Duration = Duration::from_millis(500);

pub type ClientError = Box<dyn std::error::Error>;
pub type ClientResult<T> = Result<T, ClientError>;

pub struct TineClient {
    base_url: String,
    http: reqwest::Client,
}

impl TineClient {
    /// Build a client from the configured bind address (e.g. "127.0.0.1:9473").
    pub fn from_bind(bind: &str) -> ClientResult<Self> {
        let base_url = if bind.starts_with("http://") || bind.starts_with("https://") {
            bind.trim_end_matches('/').to_string()
        } else {
            format!("http://{}", bind.trim_end_matches('/'))
        };
        let http = reqwest::Client::builder()
            .timeout(DEFAULT_TIMEOUT)
            .build()?;
        Ok(Self { base_url, http })
    }

    #[allow(dead_code)]
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    // -- experiment trees ---------------------------------------------------

    pub async fn list_experiment_trees(&self) -> ClientResult<Value> {
        self.get_json("/api/experiment-trees").await
    }

    pub async fn get_experiment_tree(&self, tree_id: &str) -> ClientResult<Value> {
        self.get_json(&format!("/api/experiment-trees/{tree_id}"))
            .await
    }

    pub async fn create_experiment_tree(
        &self,
        name: &str,
        project_id: Option<&str>,
    ) -> ClientResult<Value> {
        let mut body = serde_json::json!({ "name": name });
        if let Some(project_id) = project_id {
            body["project_id"] = Value::String(project_id.to_string());
        }
        self.post_json("/api/experiment-trees", Some(body), DEFAULT_TIMEOUT)
            .await
    }

    pub async fn delete_experiment_tree(&self, tree_id: &str) -> ClientResult<()> {
        self.delete(&format!("/api/experiment-trees/{tree_id}"))
            .await
    }

    pub async fn rename_experiment_tree(&self, tree_id: &str, name: &str) -> ClientResult<()> {
        self.post_no_content(
            &format!("/api/experiment-trees/{tree_id}/rename"),
            Some(serde_json::json!({ "name": name })),
        )
        .await
    }

    pub async fn tree_runtime_state(&self, tree_id: &str) -> ClientResult<Value> {
        self.get_json(&format!("/api/experiment-trees/{tree_id}/runtime-state"))
            .await
    }

    // -- branches -----------------------------------------------------------

    pub async fn create_branch(
        &self,
        tree_id: &str,
        parent_branch_id: &str,
        name: &str,
        branch_point_cell_id: &str,
        first_cell: Value,
    ) -> ClientResult<Value> {
        self.post_json(
            &format!("/api/experiment-trees/{tree_id}/branches"),
            Some(serde_json::json!({
                "parent_branch_id": parent_branch_id,
                "name": name,
                "branch_point_cell_id": branch_point_cell_id,
                "first_cell": first_cell,
            })),
            DEFAULT_TIMEOUT,
        )
        .await
    }

    pub async fn delete_branch(&self, tree_id: &str, branch_id: &str) -> ClientResult<()> {
        self.delete(&format!(
            "/api/experiment-trees/{tree_id}/branches/{branch_id}"
        ))
        .await
    }

    pub async fn export_branch(
        &self,
        tree_id: &str,
        branch_id: &str,
        format: &str,
    ) -> ClientResult<String> {
        let response = self
            .request(
                reqwest::Method::GET,
                &format!("/api/experiment-trees/{tree_id}/branches/{branch_id}/export.{format}"),
            )
            .send()
            .await
            .map_err(|err| self.map_transport_error(err))?;
        let response = Self::check_status(response).await?;
        Ok(response.text().await?)
    }

    // -- cells --------------------------------------------------------------

    pub async fn add_cell(
        &self,
        tree_id: &str,
        branch_id: &str,
        cell: Value,
        after_cell_id: Option<&str>,
    ) -> ClientResult<()> {
        let mut body = serde_json::json!({ "cell": cell });
        if let Some(after) = after_cell_id {
            body["after_cell_id"] = Value::String(after.to_string());
        }
        self.post_no_content(
            &format!("/api/experiment-trees/{tree_id}/branches/{branch_id}/cells"),
            Some(body),
        )
        .await
    }

    pub async fn update_cell_code(
        &self,
        tree_id: &str,
        branch_id: &str,
        cell_id: &str,
        source: &str,
    ) -> ClientResult<()> {
        self.post_no_content(
            &format!("/api/experiment-trees/{tree_id}/branches/{branch_id}/cells/{cell_id}/code"),
            Some(serde_json::json!({ "source": source })),
        )
        .await
    }

    pub async fn move_cell(
        &self,
        tree_id: &str,
        branch_id: &str,
        cell_id: &str,
        direction: &str,
    ) -> ClientResult<()> {
        self.post_no_content(
            &format!("/api/experiment-trees/{tree_id}/branches/{branch_id}/cells/{cell_id}/move"),
            Some(serde_json::json!({ "direction": direction })),
        )
        .await
    }

    pub async fn delete_cell(
        &self,
        tree_id: &str,
        branch_id: &str,
        cell_id: &str,
    ) -> ClientResult<()> {
        self.delete(&format!(
            "/api/experiment-trees/{tree_id}/branches/{branch_id}/cells/{cell_id}"
        ))
        .await
    }

    pub async fn cell_logs(
        &self,
        tree_id: &str,
        branch_id: &str,
        cell_id: &str,
    ) -> ClientResult<Value> {
        self.get_json(&format!(
            "/api/experiment-trees/{tree_id}/branches/{branch_id}/cells/{cell_id}/logs"
        ))
        .await
    }

    // -- executions ----------------------------------------------------------

    pub async fn execute_cell(
        &self,
        tree_id: &str,
        branch_id: &str,
        cell_id: &str,
        idempotency_key: &str,
    ) -> ClientResult<Value> {
        self.post_execute(
            &format!(
                "/api/experiment-trees/{tree_id}/branches/{branch_id}/cells/{cell_id}/execute"
            ),
            idempotency_key,
        )
        .await
    }

    pub async fn execute_branch(
        &self,
        tree_id: &str,
        branch_id: &str,
        idempotency_key: &str,
    ) -> ClientResult<Value> {
        self.post_execute(
            &format!("/api/experiment-trees/{tree_id}/branches/{branch_id}/execute"),
            idempotency_key,
        )
        .await
    }

    /// Execute submissions always carry an idempotency key: if the request
    /// times out after the server accepted it, retrying with the same key
    /// reattaches to the original run instead of starting a duplicate, so
    /// the timeout error names the key to retry with.
    async fn post_execute(&self, path: &str, idempotency_key: &str) -> ClientResult<Value> {
        let response = self
            .request(reqwest::Method::POST, path)
            .timeout(LONG_TIMEOUT)
            .json(&serde_json::json!({ "idempotency_key": idempotency_key }))
            .send()
            .await
            .map_err(|err| {
                if err.is_timeout() {
                    self.execute_timeout_error(idempotency_key)
                } else {
                    self.map_transport_error(err)
                }
            })?;
        let response = Self::check_status(response).await?;
        Ok(response.json().await?)
    }

    fn execute_timeout_error(&self, idempotency_key: &str) -> ClientError {
        format!(
            "Tine API call timed out; the server at {} may be stalled or restarting. \
             The submission may already have been accepted — retry with \
             `--idempotency-key {}` to reattach to the original run instead of \
             starting a duplicate.",
            self.base_url, idempotency_key
        )
        .into()
    }

    pub async fn execute_all_branches(&self, tree_id: &str) -> ClientResult<Value> {
        self.post_json(
            &format!("/api/experiment-trees/{tree_id}/execute-all-branches"),
            None,
            LONG_TIMEOUT,
        )
        .await
    }

    pub async fn execution_status(&self, execution_id: &str) -> ClientResult<Value> {
        self.get_json(&format!("/api/executions/{execution_id}"))
            .await
    }

    pub async fn cancel_execution(&self, execution_id: &str) -> ClientResult<()> {
        self.post_no_content(&format!("/api/executions/{execution_id}/cancel"), None)
            .await
    }

    /// Poll an execution's status until it reaches a terminal state.
    pub async fn wait_for_terminal(&self, execution_id: &str) -> ClientResult<Value> {
        loop {
            let status = self.execution_status(execution_id).await?;
            if execution_status_is_terminal(&status) {
                return Ok(status);
            }
            tokio::time::sleep(WAIT_POLL_INTERVAL).await;
        }
    }

    // -- projects -------------------------------------------------------------

    pub async fn list_projects(&self) -> ClientResult<Value> {
        self.get_json("/api/projects").await
    }

    pub async fn create_project(
        &self,
        name: &str,
        workspace_dir: &str,
        description: Option<&str>,
    ) -> ClientResult<Value> {
        let mut body = serde_json::json!({ "name": name, "workspace_dir": workspace_dir });
        if let Some(description) = description {
            body["description"] = Value::String(description.to_string());
        }
        self.post_json("/api/projects", Some(body), DEFAULT_TIMEOUT)
            .await
    }

    pub async fn get_project(&self, project_id: &str) -> ClientResult<Value> {
        self.get_json(&format!("/api/projects/{project_id}")).await
    }

    pub async fn list_project_experiments(&self, project_id: &str) -> ClientResult<Value> {
        self.get_json(&format!("/api/projects/{project_id}/experiments"))
            .await
    }

    // -- files ----------------------------------------------------------------

    pub async fn list_files(&self, path: &str, project_id: Option<&str>) -> ClientResult<Value> {
        let mut request = self
            .request(reqwest::Method::GET, "/api/files")
            .query(&[("path", path)]);
        if let Some(project_id) = project_id {
            request = request.query(&[("project_id", project_id)]);
        }
        let response = request
            .send()
            .await
            .map_err(|err| self.map_transport_error(err))?;
        let response = Self::check_status(response).await?;
        Ok(response.json().await?)
    }

    pub async fn read_file(&self, path: &str, project_id: Option<&str>) -> ClientResult<Vec<u8>> {
        let mut request = self
            .request(reqwest::Method::GET, "/api/files/read")
            .query(&[("path", path)]);
        if let Some(project_id) = project_id {
            request = request.query(&[("project_id", project_id)]);
        }
        let response = request
            .send()
            .await
            .map_err(|err| self.map_transport_error(err))?;
        let response = Self::check_status(response).await?;
        Ok(response.bytes().await?.to_vec())
    }

    pub async fn write_file(
        &self,
        path: &str,
        content: &str,
        project_id: Option<&str>,
    ) -> ClientResult<()> {
        self.post_no_content(
            "/api/files/write",
            Some(serde_json::json!({
                "path": path,
                "content": content,
                "project_id": project_id,
            })),
        )
        .await
    }

    // -- transport helpers ------------------------------------------------------

    fn request(&self, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        self.http
            .request(method, format!("{}{}", self.base_url, path))
    }

    async fn get_json(&self, path: &str) -> ClientResult<Value> {
        let response = self
            .request(reqwest::Method::GET, path)
            .send()
            .await
            .map_err(|err| self.map_transport_error(err))?;
        let response = Self::check_status(response).await?;
        Ok(response.json().await?)
    }

    async fn post_json(
        &self,
        path: &str,
        body: Option<Value>,
        timeout: Duration,
    ) -> ClientResult<Value> {
        let mut request = self.request(reqwest::Method::POST, path).timeout(timeout);
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request
            .send()
            .await
            .map_err(|err| self.map_transport_error(err))?;
        let response = Self::check_status(response).await?;
        Ok(response.json().await?)
    }

    async fn post_no_content(&self, path: &str, body: Option<Value>) -> ClientResult<()> {
        let mut request = self.request(reqwest::Method::POST, path);
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request
            .send()
            .await
            .map_err(|err| self.map_transport_error(err))?;
        Self::check_status(response).await?;
        Ok(())
    }

    async fn delete(&self, path: &str) -> ClientResult<()> {
        let response = self
            .request(reqwest::Method::DELETE, path)
            .send()
            .await
            .map_err(|err| self.map_transport_error(err))?;
        Self::check_status(response).await?;
        Ok(())
    }

    fn map_transport_error(&self, err: reqwest::Error) -> ClientError {
        if err.is_connect() {
            return format!(
                "could not reach the Tine server at {} — is it running? Start it with `tine serve`.",
                self.base_url
            )
            .into();
        }
        if err.is_timeout() {
            return format!(
                "Tine API call timed out; the server at {} may be stalled or restarting. \
                 If this was an execute submission it may already have been accepted — \
                 check execution status before retrying.",
                self.base_url
            )
            .into();
        }
        err.into()
    }

    async fn check_status(response: reqwest::Response) -> ClientResult<reqwest::Response> {
        let status = response.status();
        if status.is_success() {
            return Ok(response);
        }
        let body = response.text().await.unwrap_or_default();
        let message = serde_json::from_str::<Value>(&body)
            .ok()
            .and_then(|value| {
                value
                    .get("error")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            })
            .unwrap_or(body);
        Err(format!("request failed with status {status}: {message}").into())
    }
}

/// True when an execution status JSON payload describes a terminal state.
pub fn execution_status_is_terminal(status: &Value) -> bool {
    if status
        .get("finished_at")
        .is_some_and(|finished| !finished.is_null())
    {
        return true;
    }
    matches!(
        status.get("status").and_then(Value::as_str),
        Some("completed" | "failed" | "cancelled" | "timed_out" | "rejected")
    )
}

#[cfg(test)]
mod tests {
    use super::{execution_status_is_terminal, TineClient};

    /// A timed-out execute submission may already be running server-side;
    /// the error must name the idempotency key so the user can retry with
    /// it and reattach instead of double-running.
    #[test]
    fn execute_timeout_error_names_the_idempotency_key() {
        let client = TineClient::from_bind("127.0.0.1:9473").expect("client must build");
        let message = client.execute_timeout_error("cli-key-1").to_string();
        assert!(message.contains("--idempotency-key cli-key-1"), "{message}");
        assert!(message.to_lowercase().contains("duplicate"), "{message}");
    }

    #[test]
    fn terminal_detection_covers_all_terminal_states() {
        for state in ["completed", "failed", "cancelled", "timed_out", "rejected"] {
            assert!(execution_status_is_terminal(&serde_json::json!({
                "status": state,
                "finished_at": null,
            })));
        }
        for state in ["queued", "running"] {
            assert!(!execution_status_is_terminal(&serde_json::json!({
                "status": state,
                "finished_at": null,
            })));
        }
        assert!(execution_status_is_terminal(&serde_json::json!({
            "status": "running",
            "finished_at": "2026-06-11T00:00:00Z",
        })));
    }
}
