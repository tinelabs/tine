//! Integration tests for the CLI's HTTP front door.
//!
//! These start a real Axum server on an ephemeral port against a temp
//! workspace and drive the actual `tine` binary against it, validating the
//! thin-client contract end to end: data-plane round trips, friendly errors
//! when the server is down, and execute commands that block until terminal.

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use tempfile::TempDir;

use tine_api::Workspace;
use tine_core::{ArtifactKey, ArtifactMetadata, ArtifactStore, TineError, TineResult};
use tine_server::{build_router, AppState};

struct MemoryArtifactStore {
    data: DashMap<String, Vec<u8>>,
}

#[async_trait]
impl ArtifactStore for MemoryArtifactStore {
    async fn put(&self, key: &ArtifactKey, data: &[u8]) -> TineResult<[u8; 32]> {
        self.data.insert(key.as_str().to_string(), data.to_vec());
        Ok(*blake3::hash(data).as_bytes())
    }

    async fn get(&self, key: &ArtifactKey) -> TineResult<Vec<u8>> {
        self.data
            .get(key.as_str())
            .map(|entry| entry.clone())
            .ok_or_else(|| TineError::ArtifactNotFound(key.clone()))
    }

    async fn delete(&self, key: &ArtifactKey) -> TineResult<()> {
        self.data.remove(key.as_str());
        Ok(())
    }

    async fn exists(&self, key: &ArtifactKey) -> TineResult<bool> {
        Ok(self.data.contains_key(key.as_str()))
    }

    async fn metadata(&self, key: &ArtifactKey) -> TineResult<ArtifactMetadata> {
        Err(TineError::ArtifactNotFound(key.clone()))
    }

    async fn list(&self) -> TineResult<Vec<ArtifactKey>> {
        Ok(Vec::new())
    }
}

/// Start a server on an ephemeral port against a fresh temp workspace.
/// Returns the workspace temp dir (keep alive), a CWD temp dir for the CLI
/// (so it picks up no stray `.tine/config.toml`), and the bind address.
async fn start_test_server() -> (TempDir, TempDir, String) {
    let workspace_tmp = TempDir::new().expect("failed to create workspace dir");
    let cli_cwd = TempDir::new().expect("failed to create cli cwd");
    let store: Arc<dyn ArtifactStore> = Arc::new(MemoryArtifactStore {
        data: DashMap::new(),
    });
    let workspace = Workspace::open(workspace_tmp.path().to_path_buf(), store, 4)
        .await
        .expect("failed to open workspace");

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("failed to bind ephemeral port");
    let addr = listener.local_addr().expect("missing local addr");
    let state = Arc::new(AppState {
        workspace: Arc::new(workspace),
        metrics_handle: None,
        ui_dir: PathBuf::from("ui"),
        api_base_url: format!("http://{addr}"),
    });
    let app = build_router(state);
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("server crashed");
    });

    (workspace_tmp, cli_cwd, addr.to_string())
}

async fn run_cli(cwd: &TempDir, bind: &str, args: &[&str]) -> (bool, String, String) {
    let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_tine"))
        .args(args)
        .env("TINE_BIND", bind)
        .current_dir(cwd.path())
        .output()
        .await
        .expect("failed to run tine binary");
    (
        output.status.success(),
        String::from_utf8_lossy(&output.stdout).to_string(),
        String::from_utf8_lossy(&output.stderr).to_string(),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_data_plane_round_trips_over_http() {
    let (_workspace_tmp, cli_cwd, bind) = start_test_server().await;

    let (ok, stdout, stderr) = run_cli(
        &cli_cwd,
        &bind,
        &["internal", "experiments", "create", "CLI HTTP Tree"],
    )
    .await;
    assert!(ok, "create failed: stdout={stdout} stderr={stderr}");
    let tree: serde_json::Value = serde_json::from_str(&stdout).expect("create output not JSON");
    assert_eq!(tree["name"], "CLI HTTP Tree");

    let (ok, stdout, stderr) = run_cli(&cli_cwd, &bind, &["internal", "experiments", "list"]).await;
    assert!(ok, "list failed: stdout={stdout} stderr={stderr}");
    assert!(
        stdout.contains("CLI HTTP Tree"),
        "list output missing created tree: {stdout}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_reports_friendly_error_when_server_unreachable() {
    let cli_cwd = TempDir::new().expect("failed to create cli cwd");
    // Bind-then-drop to get a port that is almost certainly closed.
    let closed_port = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    };

    let (ok, stdout, stderr) = run_cli(
        &cli_cwd,
        &format!("127.0.0.1:{closed_port}"),
        &["internal", "experiments", "list"],
    )
    .await;
    assert!(!ok, "expected failure, got stdout={stdout}");
    assert!(
        stderr.contains("tine serve"),
        "error should tell the user to start the server: {stderr}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn cli_branch_execute_blocks_until_terminal() {
    let (_workspace_tmp, cli_cwd, bind) = start_test_server().await;

    // Create a tree, then give its first cell real code — all through the CLI.
    let (ok, stdout, stderr) = run_cli(
        &cli_cwd,
        &bind,
        &["internal", "experiments", "create", "CLI Exec Tree"],
    )
    .await;
    assert!(ok, "create failed: stdout={stdout} stderr={stderr}");
    let tree: serde_json::Value = serde_json::from_str(&stdout).expect("create output not JSON");
    let tree_id = tree["id"].as_str().expect("tree id").to_string();
    let branch_id = tree["root_branch_id"]
        .as_str()
        .expect("root branch id")
        .to_string();
    let cell_id = tree["cells"][0]["id"]
        .as_str()
        .expect("cell id")
        .to_string();

    let (ok, stdout, stderr) = run_cli(
        &cli_cwd,
        &bind,
        &[
            "internal",
            "cells",
            "update-code",
            &tree_id,
            &branch_id,
            &cell_id,
            "--source",
            "result = 21 * 2\nprint(result, flush=True)",
        ],
    )
    .await;
    assert!(ok, "update-code failed: stdout={stdout} stderr={stderr}");

    // Default behavior blocks until the execution is terminal: the status in
    // the output must already be a final state, not queued/running.
    let (ok, stdout, stderr) = run_cli(
        &cli_cwd,
        &bind,
        &["internal", "branches", "execute", &tree_id, &branch_id],
    )
    .await;
    assert!(ok, "execute failed: stdout={stdout} stderr={stderr}");
    let result: serde_json::Value = serde_json::from_str(&stdout).expect("execute output not JSON");
    assert_eq!(
        result["status"]["status"], "completed",
        "execute must wait for a terminal state: {result}"
    );

    // --no-wait returns immediately with just the submission envelope.
    let (ok, stdout, stderr) = run_cli(
        &cli_cwd,
        &bind,
        &[
            "internal",
            "branches",
            "execute",
            &tree_id,
            &branch_id,
            "--no-wait",
        ],
    )
    .await;
    assert!(
        ok,
        "no-wait execute failed: stdout={stdout} stderr={stderr}"
    );
    let submitted: serde_json::Value =
        serde_json::from_str(&stdout).expect("no-wait output not JSON");
    assert!(submitted.get("status").is_none());
    assert!(submitted["execution_id"].as_str().is_some());
    // Submissions are idempotent by default: the CLI generates a key when
    // none is given and echoes it so a timed-out submission can be retried.
    let generated_key = submitted["idempotency_key"].as_str().expect("echoed key");
    assert!(
        generated_key.starts_with("cli-"),
        "expected auto-generated key, got {generated_key}"
    );

    // An explicit key reaches the server: resubmitting with the same key
    // reattaches to the original execution instead of starting a duplicate.
    let mut execution_ids = Vec::new();
    for _ in 0..2 {
        let (ok, stdout, stderr) = run_cli(
            &cli_cwd,
            &bind,
            &[
                "internal",
                "branches",
                "execute",
                &tree_id,
                &branch_id,
                "--no-wait",
                "--idempotency-key",
                "cli-retry-key-1",
            ],
        )
        .await;
        assert!(ok, "keyed execute failed: stdout={stdout} stderr={stderr}");
        let submitted: serde_json::Value =
            serde_json::from_str(&stdout).expect("keyed output not JSON");
        assert_eq!(submitted["idempotency_key"], "cli-retry-key-1");
        execution_ids.push(
            submitted["execution_id"]
                .as_str()
                .expect("execution id")
                .to_string(),
        );
    }
    assert_eq!(
        execution_ids[0], execution_ids[1],
        "a resubmission with the same idempotency key must reattach to the original run"
    );
}
