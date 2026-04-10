use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::{header, HeaderValue, StatusCode},
    response::Response,
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use tower_http::{
    cors::{Any, CorsLayer},
    services::ServeDir,
};
use tracing::info;

use tine_api::FileEntry;
use tine_api::Workspace;
use tine_api::{export_branch_as_ipynb, export_branch_as_python};
use tine_core::{
    BranchId, BranchTargetInspection, CellDef, CellId, CellRuntimeState, ExecutionId,
    ExecutionAccepted, ExecutionStatus, ExperimentTreeDef, ExperimentTreeId, NodeCode,
    NodeLogs, ProjectDef, ProjectId, RevisionId, SlotName, TreeRuntimeState, WorkspaceApi,
};

use crate::file_watcher::start_file_watcher;

/// Application state shared across all handlers.
pub struct AppState {
    pub workspace: Arc<Workspace>,
    pub metrics_handle: Option<metrics_exporter_prometheus::PrometheusHandle>,
    pub ui_dir: PathBuf,
    pub api_base_url: String,
}

/// Start the full tine server (REST + WebSocket) on the given bind address.
pub async fn serve(workspace: Workspace, bind: &str) -> Result<(), Box<dyn std::error::Error>> {
    let listener = tokio::net::TcpListener::bind(bind).await?;
    serve_listener(workspace, listener).await
}

/// Start the full tine server (REST + WebSocket) on a pre-bound listener.
pub async fn serve_listener(
    workspace: Workspace,
    listener: tokio::net::TcpListener,
) -> Result<(), Box<dyn std::error::Error>> {
    // Start file watcher before wrapping workspace in Arc
    let _watcher = start_file_watcher(workspace.workspace_root(), workspace.event_sender())
        .map_err(|e| format!("file watcher: {e}"))?;
    let ui_dir = resolve_ui_dir(workspace.workspace_root())?;
    let local_addr = listener.local_addr()?;

    let state = Arc::new(AppState {
        workspace: Arc::new(workspace),
        metrics_handle: None,
        ui_dir,
        api_base_url: local_server_url(local_addr),
    });
    let app = build_router(state);
    info!(bind = %local_addr, "tine server listening");
    axum::serve(listener, app).await?;
    Ok(())
}

/// Start the full tine server with a pre-installed Prometheus metrics handle.
/// Handles SIGTERM / Ctrl-C gracefully by draining active kernels before exit.
pub async fn serve_with_metrics(
    workspace: Workspace,
    bind: &str,
    metrics_handle: metrics_exporter_prometheus::PrometheusHandle,
) -> Result<(), Box<dyn std::error::Error>> {
    let listener = tokio::net::TcpListener::bind(bind).await?;
    serve_listener_with_metrics(workspace, listener, metrics_handle).await
}

/// Start the full tine server with a pre-installed Prometheus metrics handle.
/// Handles SIGTERM / Ctrl-C gracefully by draining active kernels before exit.
pub async fn serve_listener_with_metrics(
    workspace: Workspace,
    listener: tokio::net::TcpListener,
    metrics_handle: metrics_exporter_prometheus::PrometheusHandle,
) -> Result<(), Box<dyn std::error::Error>> {
    let _watcher = start_file_watcher(workspace.workspace_root(), workspace.event_sender())
        .map_err(|e| format!("file watcher: {e}"))?;
    let ui_dir = resolve_ui_dir(workspace.workspace_root())?;
    let local_addr = listener.local_addr()?;

    let state = Arc::new(AppState {
        workspace: Arc::new(workspace),
        metrics_handle: Some(metrics_handle),
        ui_dir,
        api_base_url: local_server_url(local_addr),
    });
    let ws_ref = Arc::clone(&state.workspace);
    let app = build_router(state);
    info!(bind = %local_addr, "tine server listening (metrics enabled)");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    info!("shutting down — draining kernels");
    ws_ref.shutdown().await?;
    info!("shutdown complete");
    Ok(())
}

/// Wait for SIGTERM (Unix) or Ctrl-C to initiate graceful shutdown.
async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();

    #[cfg(unix)]
    {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler");
        tokio::select! {
            _ = ctrl_c => info!("received Ctrl-C, starting graceful shutdown"),
            _ = sigterm.recv() => info!("received SIGTERM, starting graceful shutdown"),
        }
    }
    #[cfg(not(unix))]
    {
        ctrl_c.await.expect("failed to listen for Ctrl-C");
        info!("received Ctrl-C, starting graceful shutdown");
    }
}

/// Build the axum router with all REST endpoints.
pub fn build_router(state: Arc<AppState>) -> Router {
    let ui_dir = state.ui_dir.clone();

    let api = Router::new()
        // Experiment tree management
        .route(
            "/api/experiment-trees",
            get(list_experiment_trees).post(create_experiment_tree_handler),
        )
        .route(
            "/api/experiment-trees/{id}",
            get(get_experiment_tree)
                .put(save_experiment_tree_handler)
                .delete(delete_experiment_tree_handler),
        )
        .route(
            "/api/experiment-trees/{id}/rename",
            post(rename_experiment_tree_handler),
        )
        .route(
            "/api/experiment-trees/{id}/runtime-state",
            get(get_experiment_tree_runtime_state),
        )
        .route(
            "/api/experiment-trees/{id}/branches",
            post(create_experiment_tree_branch),
        )
        .route(
            "/api/experiment-trees/{id}/branches/{branch_id}",
            axum::routing::delete(delete_experiment_tree_branch),
        )
        .route(
            "/api/experiment-trees/{id}/branches/{branch_id}/cells",
            post(add_experiment_tree_branch_cell),
        )
        .route(
            "/api/experiment-trees/{id}/branches/{branch_id}/cells/{cell_id}/code",
            post(update_experiment_tree_branch_cell_code),
        )
        .route(
            "/api/experiment-trees/{id}/branches/{branch_id}/cells/{cell_id}/move",
            post(move_experiment_tree_branch_cell),
        )
        .route(
            "/api/experiment-trees/{id}/branches/{branch_id}/cells/{cell_id}",
            axum::routing::delete(delete_experiment_tree_branch_cell),
        )
        .route(
            "/api/experiment-trees/{id}/branches/{branch_id}/cells/{cell_id}/execute",
            post(execute_experiment_tree_branch_cell),
        )
        .route(
            "/api/experiment-trees/{id}/branches/{branch_id}/cells/{cell_id}/logs",
            get(get_experiment_tree_branch_cell_logs),
        )
        .route(
            "/api/experiment-trees/{id}/branches/{branch_id}/cells/{cell_id}/inspect",
            get(inspect_experiment_tree_branch_cell),
        )
        .route(
            "/api/experiment-trees/{id}/branches/{branch_id}/execute",
            post(execute_experiment_tree_branch),
        )
        .route(
            "/api/experiment-trees/{id}/execute-all-branches",
            post(execute_all_experiment_tree_branches),
        )
        .route(
            "/api/experiment-trees/{id}/branches/{branch_id}/export.py",
            get(export_experiment_tree_branch_python),
        )
        .route(
            "/api/experiment-trees/{id}/branches/{branch_id}/export.ipynb",
            get(export_experiment_tree_branch_ipynb),
        )
        .route("/api/executions/{id}", get(get_execution_status))
        .route("/api/executions/{id}/cancel", post(cancel_execution))
        .route("/api/files", get(list_files_handler))
        .route("/api/files/read", get(read_file_handler))
        .route("/api/files/write", post(write_file_handler))
        .route(
            "/api/projects",
            get(list_projects).post(create_project_handler),
        )
        .route("/api/projects/{id}", get(get_project))
        .route("/api/projects/{id}/experiments", get(list_experiments))
        .route(
            "/api/system/default-projects-dir",
            get(default_projects_dir_handler),
        )
        .route("/api/system/pick-directory", post(pick_directory_handler))
        // WebSocket
        .route("/ws", get(crate::websocket::ws_handler))
        // Observability
        .route("/healthz", get(healthz))
        .route("/metrics", get(metrics))
        .with_state(state);

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    // Serve static UI files, falling back to index.html for SPA routing
    api.fallback_service(ServeDir::new(&ui_dir).fallback(ServeDir::new(&ui_dir)))
        .layer(cors)
}

// ---------------------------------------------------------------------------
// Handlers — each ~5 lines, delegating to WorkspaceApi
// ---------------------------------------------------------------------------

async fn list_experiment_trees(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<ExperimentTreeDef>>, AppError> {
    let trees = state.workspace.list_experiment_trees().await?;
    Ok(Json(trees))
}

async fn get_experiment_tree(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<ExperimentTreeDef>, AppError> {
    let def = state
        .workspace
        .get_experiment_tree(&ExperimentTreeId::new(id))
        .await?;
    Ok(Json(def))
}

async fn save_experiment_tree_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(def): Json<ExperimentTreeDef>,
) -> Result<Json<ExperimentTreeDef>, AppError> {
    if def.id.as_str() != id {
        return Err(AppError(tine_core::TineError::Config(format!(
            "path/tree id mismatch: expected {}, got {}",
            id, def.id
        ))));
    }
    let saved_tree = state.workspace.save_experiment_tree(&def).await?;
    Ok(Json(saved_tree))
}

#[derive(Deserialize)]
struct CreateExperimentTreeRequest {
    name: String,
    project_id: Option<String>,
}

async fn create_experiment_tree_handler(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateExperimentTreeRequest>,
) -> Result<(StatusCode, Json<ExperimentTreeDef>), AppError> {
    let project_id = req.project_id.map(ProjectId::new);
    let tree = state
        .workspace
        .create_experiment_tree(&req.name, project_id.as_ref())
        .await?;
    Ok((StatusCode::CREATED, Json(tree)))
}

async fn delete_experiment_tree_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<StatusCode, AppError> {
    state
        .workspace
        .delete_experiment_tree(&ExperimentTreeId::new(id))
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn rename_experiment_tree_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(req): Json<RenameRequest>,
) -> Result<StatusCode, AppError> {
    state
        .workspace
        .rename_experiment_tree(&ExperimentTreeId::new(id), &req.name)
        .await?;
    Ok(StatusCode::OK)
}

async fn get_experiment_tree_runtime_state(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<Option<TreeRuntimeState>>, AppError> {
    let runtime_state = state
        .workspace
        .get_tree_runtime_state(&ExperimentTreeId::new(id))
        .await;
    Ok(Json(runtime_state))
}

#[derive(Deserialize)]
struct CreateExperimentTreeBranchRequest {
    parent_branch_id: String,
    name: String,
    branch_point_cell_id: String,
    first_cell: CreateExperimentTreeBranchFirstCell,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum CreateExperimentTreeBranchFirstCell {
    Full(CellDef),
    Lightweight(LightweightBranchCellRequest),
}

#[derive(Deserialize)]
struct LightweightBranchCellRequest {
    id: Option<String>,
    branch_id: Option<String>,
    name: Option<String>,
    cell_name: Option<String>,
    code: Option<LightweightNodeCodeRequest>,
    source: Option<String>,
    language: Option<String>,
    upstream_cell_ids: Option<Vec<String>>,
    upstream: Option<Vec<String>>,
    declared_outputs: Option<Vec<String>>,
    outputs: Option<Vec<String>>,
    cache: Option<bool>,
    map_over: Option<String>,
    map_concurrency: Option<usize>,
    tags: Option<HashMap<String, String>>,
    revision_id: Option<String>,
    state: Option<CellRuntimeState>,
}

#[derive(Deserialize)]
struct LightweightNodeCodeRequest {
    source: Option<String>,
    language: Option<String>,
}

impl CreateExperimentTreeBranchFirstCell {
    fn into_cell_def(
        self,
        tree_id: &ExperimentTreeId,
        branch_name: &str,
    ) -> Result<CellDef, AppError> {
        match self {
            Self::Full(cell) => Ok(cell),
            Self::Lightweight(cell) => cell.into_cell_def(tree_id, branch_name),
        }
    }
}

impl LightweightBranchCellRequest {
    fn into_cell_def(
        self,
        tree_id: &ExperimentTreeId,
        branch_name: &str,
    ) -> Result<CellDef, AppError> {
        let cell_id = self
            .id
            .filter(|value| !value.is_empty())
            .map(CellId::new)
            .unwrap_or_else(CellId::generate);

        let source = self
            .code
            .as_ref()
            .and_then(|code| code.source.clone())
            .or(self.source)
            .unwrap_or_default();
        let language = self
            .code
            .as_ref()
            .and_then(|code| code.language.clone())
            .or(self.language)
            .unwrap_or_else(|| "python".to_string());

        if language.trim().is_empty() {
            return Err(AppError(tine_core::TineError::Config(
                "invalid lightweight first_cell payload: language must not be empty"
                    .to_string(),
            )));
        }

        let upstream_cell_ids = self
            .upstream_cell_ids
            .or(self.upstream)
            .unwrap_or_default()
            .into_iter()
            .map(CellId::new)
            .collect();
        let declared_outputs = self
            .declared_outputs
            .or(self.outputs)
            .unwrap_or_default()
            .into_iter()
            .map(SlotName::new)
            .collect();

        Ok(CellDef {
            id: cell_id.clone(),
            tree_id: tree_id.clone(),
            branch_id: BranchId::new(
                self.branch_id
                    .filter(|value| !value.is_empty())
                    .unwrap_or_else(|| "ignored".to_string()),
            ),
            name: self
                .name
                .or(self.cell_name)
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| branch_name.to_string()),
            code: NodeCode { source, language },
            upstream_cell_ids,
            declared_outputs,
            cache: self.cache.unwrap_or(true),
            map_over: self.map_over.map(SlotName::new),
            map_concurrency: self.map_concurrency,
            tags: self.tags.unwrap_or_default(),
            revision_id: self.revision_id.map(RevisionId::new),
            state: self.state.unwrap_or(CellRuntimeState::Clean),
        })
    }
}

async fn create_experiment_tree_branch(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(req): Json<CreateExperimentTreeBranchRequest>,
) -> Result<(StatusCode, Json<BranchId>), AppError> {
    let tree_id = ExperimentTreeId::new(id);
    let first_cell = req.first_cell.into_cell_def(&tree_id, &req.name)?;
    let branch_id = state
        .workspace
        .create_branch_in_experiment_tree(
            &tree_id,
            &BranchId::new(req.parent_branch_id),
            req.name,
            &CellId::new(req.branch_point_cell_id),
            first_cell,
        )
        .await?;
    Ok((StatusCode::CREATED, Json(branch_id)))
}

#[derive(Deserialize)]
struct AddExperimentTreeBranchCellRequest {
    cell: CellDef,
    after_cell_id: Option<String>,
}

async fn add_experiment_tree_branch_cell(
    State(state): State<Arc<AppState>>,
    Path((id, branch_id)): Path<(String, String)>,
    Json(req): Json<AddExperimentTreeBranchCellRequest>,
) -> Result<StatusCode, AppError> {
    let after = req.after_cell_id.map(CellId::new);
    state
        .workspace
        .add_cell_to_experiment_tree_branch(
            &ExperimentTreeId::new(id),
            &BranchId::new(branch_id),
            req.cell,
            after.as_ref(),
        )
        .await?;
    Ok(StatusCode::CREATED)
}

#[derive(Deserialize)]
struct UpdateCodeRequest {
    source: String,
}

async fn update_experiment_tree_branch_cell_code(
    State(state): State<Arc<AppState>>,
    Path((id, branch_id, cell_id)): Path<(String, String, String)>,
    Json(req): Json<UpdateCodeRequest>,
) -> Result<StatusCode, AppError> {
    state
        .workspace
        .update_cell_code_in_experiment_tree_branch(
            &ExperimentTreeId::new(id),
            &BranchId::new(branch_id),
            &CellId::new(cell_id),
            &req.source,
        )
        .await?;
    Ok(StatusCode::OK)
}

#[derive(Deserialize)]
struct MoveNodeRequest {
    direction: String,
}

async fn move_experiment_tree_branch_cell(
    State(state): State<Arc<AppState>>,
    Path((id, branch_id, cell_id)): Path<(String, String, String)>,
    Json(req): Json<MoveNodeRequest>,
) -> Result<StatusCode, AppError> {
    state
        .workspace
        .move_cell_in_experiment_tree_branch(
            &ExperimentTreeId::new(id),
            &BranchId::new(branch_id),
            &CellId::new(cell_id),
            &req.direction,
        )
        .await?;
    Ok(StatusCode::OK)
}

async fn delete_experiment_tree_branch_cell(
    State(state): State<Arc<AppState>>,
    Path((id, branch_id, cell_id)): Path<(String, String, String)>,
) -> Result<StatusCode, AppError> {
    state
        .workspace
        .delete_cell_from_experiment_tree_branch(
            &ExperimentTreeId::new(id),
            &BranchId::new(branch_id),
            &CellId::new(cell_id),
        )
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn delete_experiment_tree_branch(
    State(state): State<Arc<AppState>>,
    Path((id, branch_id)): Path<(String, String)>,
) -> Result<StatusCode, AppError> {
    state
        .workspace
        .delete_experiment_tree_branch(&ExperimentTreeId::new(id), &BranchId::new(branch_id))
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn execute_experiment_tree_branch_cell(
    State(state): State<Arc<AppState>>,
    Path((id, branch_id, cell_id)): Path<(String, String, String)>,
) -> Result<(StatusCode, Json<ExecutionAccepted>), AppError> {
    let accepted = Workspace::submit_cell_execution_in_experiment_tree_branch(
        state.workspace.clone(),
        &ExperimentTreeId::new(id),
        &BranchId::new(branch_id),
        &CellId::new(cell_id),
    )
    .await?;
    Ok((StatusCode::ACCEPTED, Json(accepted)))
}

async fn execute_experiment_tree_branch(
    State(state): State<Arc<AppState>>,
    Path((id, branch_id)): Path<(String, String)>,
) -> Result<(StatusCode, Json<ExecutionAccepted>), AppError> {
    let tree_id = ExperimentTreeId::new(id);
    let branch_id = BranchId::new(branch_id);
    let exec_id = state
        .workspace
        .execute_branch_in_experiment_tree(&tree_id, &branch_id)
        .await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(ExecutionAccepted::for_branch(
            exec_id,
            tree_id,
            branch_id,
            chrono::Utc::now(),
        )),
    ))
}

async fn execute_all_experiment_tree_branches(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<(StatusCode, Json<serde_json::Value>), AppError> {
    let tree_id = ExperimentTreeId::new(id);
    let executions = state
        .workspace
        .execute_all_branches_in_experiment_tree(&tree_id)
        .await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(serde_json::json!({
            "executions": executions
                .into_iter()
                .map(|(branch_id, execution_id)| ExecutionAccepted::for_branch(
                    execution_id,
                    tree_id.clone(),
                    branch_id,
                    chrono::Utc::now(),
                ))
                .collect::<Vec<_>>()
        })),
    ))
}

async fn export_experiment_tree_branch_python(
    State(state): State<Arc<AppState>>,
    Path((id, branch_id)): Path<(String, String)>,
) -> Result<Response, AppError> {
    let tree = state
        .workspace
        .get_experiment_tree(&ExperimentTreeId::new(id.clone()))
        .await?;
    let exported = export_branch_as_python(&tree, &BranchId::new(branch_id.clone()))?;
    let mut response = Response::new(exported.into());
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/x-python; charset=utf-8"),
    );
    response.headers_mut().insert(
        header::CONTENT_DISPOSITION,
        HeaderValue::from_str(&format!("attachment; filename=\"{}-{}.py\"", id, branch_id))
            .unwrap(),
    );
    Ok(response)
}

async fn export_experiment_tree_branch_ipynb(
    State(state): State<Arc<AppState>>,
    Path((id, branch_id)): Path<(String, String)>,
) -> Result<Response, AppError> {
    let tree = state
        .workspace
        .get_experiment_tree(&ExperimentTreeId::new(id.clone()))
        .await?;
    let exported = export_branch_as_ipynb(&tree, &BranchId::new(branch_id.clone()))?;
    let mut response = Response::new(serde_json::to_vec_pretty(&exported).unwrap().into());
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/x-ipynb+json"),
    );
    response.headers_mut().insert(
        header::CONTENT_DISPOSITION,
        HeaderValue::from_str(&format!(
            "attachment; filename=\"{}-{}.ipynb\"",
            id, branch_id
        ))
        .unwrap(),
    );
    Ok(response)
}

async fn get_execution_status(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<ExecutionStatus>, AppError> {
    let status = state.workspace.status(&ExecutionId::new(id)).await?;
    Ok(Json(status))
}

async fn cancel_execution(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<StatusCode, AppError> {
    state.workspace.cancel(&ExecutionId::new(id)).await?;
    Ok(StatusCode::OK)
}

async fn get_experiment_tree_branch_cell_logs(
    State(state): State<Arc<AppState>>,
    Path((id, branch_id, cell_id)): Path<(String, String, String)>,
) -> Result<Json<NodeLogs>, AppError> {
    let logs = state
        .workspace
        .logs_for_tree_cell(
            &ExperimentTreeId::new(id),
            &BranchId::new(branch_id),
            &CellId::new(cell_id),
        )
        .await?;
    Ok(Json(logs))
}

async fn inspect_experiment_tree_branch_cell(
    State(state): State<Arc<AppState>>,
    Path((id, branch_id, cell_id)): Path<(String, String, String)>,
) -> Result<Json<BranchTargetInspection>, AppError> {
    let inspection = state
        .workspace
        .inspect_branch_target(
            &ExperimentTreeId::new(id),
            &BranchId::new(branch_id),
            &CellId::new(cell_id),
        )
        .await?;
    Ok(Json(inspection))
}

#[derive(Deserialize)]
struct RenameRequest {
    name: String,
}

#[derive(Deserialize)]
struct FileQueryParams {
    path: Option<String>,
    project_id: Option<String>,
}

async fn list_files_handler(
    State(state): State<Arc<AppState>>,
    axum::extract::Query(params): axum::extract::Query<FileQueryParams>,
) -> Result<Json<Vec<FileEntry>>, AppError> {
    let path = params.path.unwrap_or_default();
    let project_id = params.project_id.as_deref().map(ProjectId::new);
    let entries = state
        .workspace
        .list_project_files(project_id.as_ref(), &path)
        .await?;
    Ok(Json(entries))
}

async fn read_file_handler(
    State(state): State<Arc<AppState>>,
    axum::extract::Query(params): axum::extract::Query<FileQueryParams>,
) -> Result<Response, AppError> {
    let path = params.path.unwrap_or_default();
    let project_id = params.project_id.as_deref().map(ProjectId::new);
    let content = state
        .workspace
        .read_project_file_bytes(project_id.as_ref(), &path)
        .await?;
    let content_type = infer_content_type(&path);
    let mut response = Response::new(content.into());
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    Ok(response)
}

#[derive(Deserialize)]
struct WriteFileRequest {
    path: String,
    content: String,
    project_id: Option<String>,
}

async fn write_file_handler(
    State(state): State<Arc<AppState>>,
    Json(req): Json<WriteFileRequest>,
) -> Result<StatusCode, AppError> {
    let project_id = req.project_id.as_deref().map(ProjectId::new);
    state
        .workspace
        .write_project_file(project_id.as_ref(), &req.path, &req.content)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

// ---------------------------------------------------------------------------
// Project handlers
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct CreateProjectRequest {
    name: String,
    #[serde(default)]
    description: Option<String>,
    #[serde(default = "default_workspace_dir")]
    workspace_dir: String,
}

fn default_workspace_dir() -> String {
    ".".to_string()
}

async fn create_project_handler(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateProjectRequest>,
) -> Result<Json<serde_json::Value>, AppError> {
    let project = ProjectDef {
        id: ProjectId::generate(),
        name: req.name,
        description: req.description,
        workspace_dir: req.workspace_dir,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };
    let id = state.workspace.create_project(project).await?;
    Ok(Json(serde_json::json!({ "id": id.as_str() })))
}

async fn list_projects(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<ProjectDef>>, AppError> {
    let projects = state.workspace.list_projects().await?;
    Ok(Json(projects))
}

async fn get_project(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<ProjectDef>, AppError> {
    let project = state.workspace.get_project(&ProjectId::new(id)).await?;
    Ok(Json(project))
}

async fn list_experiments(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<Vec<ExperimentTreeDef>>, AppError> {
    let experiments = state
        .workspace
        .list_experiments(&ProjectId::new(id))
        .await?;
    Ok(Json(experiments))
}

#[derive(serde::Serialize)]
struct DefaultProjectsDirResponse {
    path: String,
    native_picker_available: bool,
}

async fn default_projects_dir_handler(
    State(state): State<Arc<AppState>>,
) -> Json<DefaultProjectsDirResponse> {
    Json(DefaultProjectsDirResponse {
        path: default_projects_root(state.workspace.workspace_root())
            .display()
            .to_string(),
        native_picker_available: native_directory_picker_available(),
    })
}

#[derive(Deserialize)]
struct PickDirectoryRequest {
    #[serde(default)]
    initial_dir: Option<String>,
}

#[derive(serde::Serialize)]
struct PickDirectoryResponse {
    path: Option<String>,
}

async fn pick_directory_handler(
    Json(req): Json<PickDirectoryRequest>,
) -> Result<Json<PickDirectoryResponse>, AppError> {
    let picked = tokio::task::spawn_blocking(move || pick_directory(req.initial_dir))
        .await
        .map_err(|error| {
            AppError(tine_core::TineError::Config(format!(
                "directory picker join error: {error}"
            )))
        })??;
    Ok(Json(PickDirectoryResponse { path: picked }))
}

async fn healthz() -> &'static str {
    "ok"
}

fn local_server_url(addr: SocketAddr) -> String {
    let host = match addr.ip() {
        IpAddr::V4(ip) if ip.is_unspecified() => "127.0.0.1".to_string(),
        IpAddr::V6(ip) if ip.is_unspecified() => "::1".to_string(),
        ip => ip.to_string(),
    };
    if host.contains(':') {
        format!("http://[{host}]:{}", addr.port())
    } else {
        format!("http://{host}:{}", addr.port())
    }
}

pub fn default_projects_root(workspace_dir: &std::path::Path) -> PathBuf {
    if let Some(home) = std::env::var_os("HOME").map(PathBuf::from) {
        let documents = home.join("Documents");
        if documents.exists() || cfg!(target_os = "macos") {
            return documents.join("Tine");
        }
        return home.join("Tine");
    }
    workspace_dir.join("projects")
}

fn native_directory_picker_available() -> bool {
    #[cfg(target_os = "macos")]
    {
        true
    }

    #[cfg(target_os = "windows")]
    {
        true
    }

    #[cfg(all(unix, not(target_os = "macos")))]
    {
        command_exists("zenity") || command_exists("kdialog")
    }
}

fn pick_directory(initial_dir: Option<String>) -> Result<Option<String>, AppError> {
    #[cfg(target_os = "macos")]
    {
        let script = match initial_dir
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            Some(path) => format!(
                "POSIX path of (choose folder with prompt \"Choose Project Folder\" default location POSIX file {path:?})"
            ),
            None => "POSIX path of (choose folder with prompt \"Choose Project Folder\")".to_string(),
        };
        return parse_picker_output(Command::new("osascript").arg("-e").arg(script).output());
    }

    #[cfg(target_os = "windows")]
    {
        let script = r#"
Add-Type -AssemblyName System.Windows.Forms
$dialog = New-Object System.Windows.Forms.FolderBrowserDialog
$dialog.Description = 'Choose Project Folder'
if ($dialog.ShowDialog() -eq [System.Windows.Forms.DialogResult]::OK) {
  $dialog.SelectedPath
}
"#;
        return parse_picker_output(
            Command::new("powershell")
                .args(["-NoProfile", "-Command", script])
                .output(),
        );
    }

    #[cfg(all(unix, not(target_os = "macos")))]
    {
        if command_exists("zenity") {
            let mut command = Command::new("zenity");
            command.args([
                "--file-selection",
                "--directory",
                "--title=Choose Project Folder",
            ]);
            if let Some(path) = initial_dir
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                command.arg(format!("--filename={path}/"));
            }
            return parse_picker_output(command.output());
        }
        if command_exists("kdialog") {
            let mut command = Command::new("kdialog");
            command.arg("--getexistingdirectory");
            if let Some(path) = initial_dir
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                command.arg(path);
            }
            return parse_picker_output(command.output());
        }
        return Err(AppError(tine_core::TineError::Config(
            "native directory picker unavailable on this system".to_string(),
        )));
    }
}

#[cfg(all(unix, not(target_os = "macos")))]
fn command_exists(binary: &str) -> bool {
    std::env::var_os("PATH")
        .map(|paths| std::env::split_paths(&paths).any(|dir| dir.join(binary).is_file()))
        .unwrap_or(false)
}

fn parse_picker_output(
    output: Result<std::process::Output, std::io::Error>,
) -> Result<Option<String>, AppError> {
    let output = output.map_err(|error| {
        AppError(tine_core::TineError::Config(format!(
            "failed to launch directory picker: {error}"
        )))
    })?;
    if output.status.success() {
        let picked = String::from_utf8_lossy(&output.stdout).trim().to_string();
        return Ok((!picked.is_empty()).then_some(picked));
    }

    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let combined = format!("{}{}", stdout, stderr);
    if combined.contains("-128") || combined.to_ascii_lowercase().contains("cancel") {
        return Ok(None);
    }

    Err(AppError(tine_core::TineError::Config(format!(
        "directory picker failed: {}",
        combined.trim()
    ))))
}

pub fn resolve_ui_dir(
    workspace_dir: &std::path::Path,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let mut candidates = Vec::new();

    if let Ok(configured) = std::env::var("TINE_UI_DIR") {
        if !configured.trim().is_empty() {
            candidates.push(PathBuf::from(configured));
        }
    }

    candidates.push(workspace_dir.join("ui"));

    if let Ok(current_dir) = std::env::current_dir() {
        candidates.push(current_dir.join("ui"));
    }

    if let Ok(current_exe) = std::env::current_exe() {
        let mut current = current_exe.parent().map(|dir| dir.to_path_buf());
        while let Some(dir) = current {
            candidates.push(dir.join("ui"));
            current = dir.parent().map(|parent| parent.to_path_buf());
        }
    }

    candidates
        .into_iter()
        .find(|dir| dir.join("index.html").is_file())
        .ok_or_else(|| "unable to locate ui assets".into())
}

fn infer_content_type(path: &str) -> &'static str {
    match path.rsplit('.').next().map(|ext| ext.to_ascii_lowercase()) {
        Some(ext) => match ext.as_str() {
            "png" => "image/png",
            "jpg" | "jpeg" => "image/jpeg",
            "gif" => "image/gif",
            "svg" => "image/svg+xml",
            "webp" => "image/webp",
            "csv" => "text/csv; charset=utf-8",
            "tsv" => "text/tab-separated-values; charset=utf-8",
            "json" => "application/json; charset=utf-8",
            "html" => "text/html; charset=utf-8",
            "css" => "text/css; charset=utf-8",
            "js" => "text/javascript; charset=utf-8",
            "md" | "txt" | "py" | "rs" | "toml" | "yaml" | "yml" | "sql" | "sh" => {
                "text/plain; charset=utf-8"
            }
            _ => "application/octet-stream",
        },
        None => "application/octet-stream",
    }
}

async fn metrics(State(state): State<Arc<AppState>>) -> String {
    match &state.metrics_handle {
        Some(handle) => handle.render(),
        None => "# metrics not enabled\n".to_string(),
    }
}

// ---------------------------------------------------------------------------
// Error handling
// ---------------------------------------------------------------------------

struct AppError(tine_core::TineError);

impl From<tine_core::TineError> for AppError {
    fn from(e: tine_core::TineError) -> Self {
        AppError(e)
    }
}

impl axum::response::IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        let status = match &self.0 {
            tine_core::TineError::RuntimeNotFound(_)
            | tine_core::TineError::ExecutionNotFound(_)
            | tine_core::TineError::ArtifactNotFound(_)
            | tine_core::TineError::ProjectNotFound(_)
            | tine_core::TineError::KernelNotFound { .. }
            | tine_core::TineError::NodeNotFound { .. } => StatusCode::NOT_FOUND,
            tine_core::TineError::DuplicateNode { .. }
            | tine_core::TineError::CycleDetected { .. }
            | tine_core::TineError::InvalidEdge { .. }
            | tine_core::TineError::Config(_) => StatusCode::BAD_REQUEST,
            tine_core::TineError::BudgetExceeded(_) => StatusCode::TOO_MANY_REQUESTS,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };

        let body = serde_json::json!({
            "error": self.0.to_string(),
        });

        (status, Json(body)).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use async_trait::async_trait;
    use axum::body::{to_bytes, Body};
    use axum::http::{Method, Request};
    use dashmap::DashMap;
    use serial_test::serial;
    use tempfile::TempDir;
    use tine_core::{
        ArtifactKey, ArtifactMetadata, ArtifactStore, BranchId, BranchIsolationMode, CellId,
        CellRuntimeState, ExperimentTreeDef, ExperimentTreeId, ProjectId, TineResult,
        TreeKernelState, TreeRuntimeState,
    };
    use tower::util::ServiceExt;

    struct MemoryArtifactStore {
        data: DashMap<String, Vec<u8>>,
    }

    impl MemoryArtifactStore {
        fn new() -> Self {
            Self {
                data: DashMap::new(),
            }
        }
    }

    #[async_trait]
    impl ArtifactStore for MemoryArtifactStore {
        async fn put(&self, key: &ArtifactKey, data: &[u8]) -> TineResult<[u8; 32]> {
            let hash = *blake3::hash(data).as_bytes();
            self.data.insert(key.as_str().to_string(), data.to_vec());
            Ok(hash)
        }

        async fn get(&self, key: &ArtifactKey) -> TineResult<Vec<u8>> {
            self.data
                .get(key.as_str())
                .map(|v| v.value().clone())
                .ok_or_else(|| tine_core::TineError::ArtifactNotFound(key.clone()))
        }

        async fn delete(&self, key: &ArtifactKey) -> TineResult<()> {
            self.data.remove(key.as_str());
            Ok(())
        }

        async fn exists(&self, key: &ArtifactKey) -> TineResult<bool> {
            Ok(self.data.contains_key(key.as_str()))
        }

        async fn metadata(&self, key: &ArtifactKey) -> TineResult<ArtifactMetadata> {
            let data = self.get(key).await?;
            Ok(ArtifactMetadata {
                key: key.clone(),
                size_bytes: data.len() as u64,
                schema: None,
                created_at: chrono::Utc::now(),
                content_hash: *blake3::hash(&data).as_bytes(),
            })
        }

        async fn list(&self) -> TineResult<Vec<ArtifactKey>> {
            Ok(self
                .data
                .iter()
                .map(|entry| ArtifactKey::new(entry.key().clone()))
                .collect())
        }
    }

    async fn test_app() -> (TempDir, Router) {
        let tmp = TempDir::new().expect("failed to create temp dir");
        eprintln!(
            "[rest-tests] opening temp workspace at {}",
            tmp.path().display()
        );
        let store: Arc<dyn ArtifactStore> = Arc::new(MemoryArtifactStore::new());
        let workspace = Workspace::open(tmp.path().to_path_buf(), store, 4)
            .await
            .expect("failed to open workspace");
        let state = Arc::new(AppState {
            workspace: Arc::new(workspace),
            metrics_handle: None,
            ui_dir: PathBuf::from("ui"),
            api_base_url: "http://127.0.0.1:9473".to_string(),
        });
        (tmp, build_router(state))
    }

    async fn send_json(
        app: &Router,
        method: Method,
        uri: &str,
        body: Option<serde_json::Value>,
    ) -> axum::response::Response {
        let request = Request::builder()
            .method(method)
            .uri(uri)
            .header(header::CONTENT_TYPE, "application/json")
            .body(match body {
                Some(payload) => Body::from(serde_json::to_vec(&payload).unwrap()),
                None => Body::empty(),
            })
            .unwrap();
        app.clone().oneshot(request).await.unwrap()
    }

    async fn read_json<T: serde::de::DeserializeOwned>(response: axum::response::Response) -> T {
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        serde_json::from_slice(&body).unwrap()
    }

    async fn read_text(response: axum::response::Response) -> String {
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        String::from_utf8(body.to_vec()).unwrap()
    }

    async fn wait_for_finished_status(
        app: &Router,
        execution_id: &str,
    ) -> tine_core::ExecutionStatus {
        for attempt in 0..480 {
            let response = send_json(
                app,
                Method::GET,
                &format!("/api/executions/{execution_id}"),
                None,
            )
            .await;
            assert_eq!(response.status(), StatusCode::OK);
            let status: tine_core::ExecutionStatus = read_json(response).await;
            if status.finished_at.is_some() {
                eprintln!(
                    "[rest-tests] execution {} finished on poll {} tree={:?} branch={:?} states={:?}",
                    execution_id,
                    attempt,
                    status.tree_id,
                    status.branch_id,
                    status.node_statuses
                );
                return status;
            }
            if attempt == 0 || attempt % 10 == 0 {
                eprintln!(
                    "[rest-tests] waiting for execution {} poll={} tree={:?} branch={:?} states={:?}",
                    execution_id,
                    attempt,
                    status.tree_id,
                    status.branch_id,
                    status.node_statuses
                );
            }
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }
        panic!("execution {execution_id} did not finish in time");
    }

    fn create_tree_payload(name: &str, project_id: Option<&str>) -> serde_json::Value {
        serde_json::json!({
            "name": name,
            "project_id": project_id
        })
    }

    fn branch_cell_payload(
        tree_id: &str,
        branch_id: &str,
        cell_id: &str,
        source: &str,
    ) -> serde_json::Value {
        serde_json::json!({
            "id": cell_id,
            "tree_id": tree_id,
            "branch_id": branch_id,
            "name": cell_id,
            "code": {
                "source": source,
                "language": "python"
            },
            "upstream_cell_ids": [],
            "declared_outputs": [],
            "cache": false,
            "map_over": null,
            "map_concurrency": null,
            "tags": {},
            "revision_id": null,
            "state": CellRuntimeState::Clean
        })
    }

    fn lightweight_branch_cell_payload(source: &str) -> serde_json::Value {
        serde_json::json!({
            "source": source,
            "language": "python",
            "outputs": ["branch_value"],
            "cache": false
        })
    }

    #[tokio::test]
    #[serial]
    async fn test_project_experiment_flow_over_http() {
        let (_tmp, app) = test_app().await;

        let create_project = send_json(
            &app,
            Method::POST,
            "/api/projects",
            Some(serde_json::json!({
                "name": "demo-project",
                "workspace_dir": "."
            })),
        )
        .await;
        assert_eq!(create_project.status(), StatusCode::OK);
        let project_body: serde_json::Value = read_json(create_project).await;
        let project_id = project_body["id"].as_str().unwrap().to_string();

        let create_tree = send_json(
            &app,
            Method::POST,
            "/api/experiment-trees",
            Some(create_tree_payload("experiment_http", Some(&project_id))),
        )
        .await;
        assert_eq!(create_tree.status(), StatusCode::CREATED);
        let tree: ExperimentTreeDef = read_json(create_tree).await;
        let tree_id = tree.id.clone();

        let list_experiments = send_json(
            &app,
            Method::GET,
            &format!("/api/projects/{project_id}/experiments"),
            None,
        )
        .await;
        assert_eq!(list_experiments.status(), StatusCode::OK);
        let experiments: Vec<ExperimentTreeDef> = read_json(list_experiments).await;
        assert_eq!(experiments.len(), 1);
        assert_eq!(experiments[0].id.as_str(), tree_id.as_str());
        assert_eq!(
            experiments[0].project_id.as_ref().map(ProjectId::as_str),
            Some(project_id.as_str())
        );

        let get_tree = send_json(
            &app,
            Method::GET,
            &format!("/api/experiment-trees/{}", tree_id.as_str()),
            None,
        )
        .await;
        assert_eq!(get_tree.status(), StatusCode::OK);
        let loaded_tree: ExperimentTreeDef = read_json(get_tree).await;
        assert_eq!(loaded_tree.id, tree_id);
        assert_eq!(
            loaded_tree.project_id.as_ref().map(ProjectId::as_str),
            Some(project_id.as_str())
        );
        assert_eq!(loaded_tree.root_branch_id, BranchId::new("main"));
        assert_eq!(loaded_tree.cells.len(), 1);
    }

    #[tokio::test]
    #[serial]
    async fn test_inspect_branch_target_over_http() {
        let (_tmp, app) = test_app().await;

        let create_tree = send_json(
            &app,
            Method::POST,
            "/api/experiment-trees",
            Some(create_tree_payload("inspect-tree", None)),
        )
        .await;
        assert_eq!(create_tree.status(), StatusCode::CREATED);
        let tree: ExperimentTreeDef = read_json(create_tree).await;

        let inspect = send_json(
            &app,
            Method::GET,
            &format!(
                "/api/experiment-trees/{}/branches/main/cells/cell_1/inspect",
                tree.id
            ),
            None,
        )
        .await;
        assert_eq!(inspect.status(), StatusCode::OK);
        let inspection: BranchTargetInspection = read_json(inspect).await;
        assert_eq!(inspection.tree_id, tree.id);
        assert_eq!(inspection.branch_id, BranchId::new("main"));
        assert_eq!(inspection.target_cell_id, CellId::new("cell_1"));
        assert_eq!(inspection.lineage, vec![BranchId::new("main")]);
        assert_eq!(inspection.path_cell_order, vec![CellId::new("cell_1")]);
        assert_eq!(inspection.topo_order, vec![CellId::new("cell_1")]);
        assert!(!inspection.has_live_kernel);
        assert!(inspection.current_runtime_state.is_none());
        assert_eq!(inspection.shared_prefix_cell_ids, Vec::<CellId>::new());
        assert_eq!(inspection.divergence_cell_id, Some(CellId::new("cell_1")));
        assert_eq!(inspection.replay_from_idx, 0);
        assert_eq!(inspection.replay_cell_ids, Vec::<CellId>::new());
        assert_eq!(inspection.replay_prefix_before_target, Vec::<CellId>::new());
    }

    #[tokio::test]
    #[serial]
    async fn test_save_experiment_tree_returns_saved_definition_over_http() {
        let (_tmp, app) = test_app().await;

        let create_tree = send_json(
            &app,
            Method::POST,
            "/api/experiment-trees",
            Some(create_tree_payload("save-http", None)),
        )
        .await;
        assert_eq!(create_tree.status(), StatusCode::CREATED);
        let mut tree: ExperimentTreeDef = read_json(create_tree).await;
        tree.name = "save-http-renamed".to_string();
        tree.cells[0].code.source = "value = 7\n".to_string();

        let saved = send_json(
            &app,
            Method::PUT,
            &format!("/api/experiment-trees/{}", tree.id.as_str()),
            Some(serde_json::to_value(&tree).unwrap()),
        )
        .await;
        assert_eq!(saved.status(), StatusCode::OK);
        let saved_tree: ExperimentTreeDef = read_json(saved).await;
        assert_eq!(saved_tree.id, tree.id);
        assert_eq!(saved_tree.name, "save-http-renamed");
        assert_eq!(saved_tree.cells[0].code.source, "value = 7\n");

        let get_tree = send_json(
            &app,
            Method::GET,
            &format!("/api/experiment-trees/{}", tree.id.as_str()),
            None,
        )
        .await;
        assert_eq!(get_tree.status(), StatusCode::OK);
        let persisted: ExperimentTreeDef = read_json(get_tree).await;
        assert_eq!(persisted.name, "save-http-renamed");
        assert_eq!(persisted.cells[0].code.source, "value = 7\n");
    }

    #[tokio::test]
    #[serial]
    async fn test_branch_create_add_edit_flow_over_http() {
        let (_tmp, app) = test_app().await;

        let create_tree = send_json(
            &app,
            Method::POST,
            "/api/experiment-trees",
            Some(create_tree_payload("branch-http", None)),
        )
        .await;
        assert_eq!(create_tree.status(), StatusCode::CREATED);
        let tree: ExperimentTreeDef = read_json(create_tree).await;
        let tree_id = tree.id.clone();

        let create_branch = send_json(
            &app,
            Method::POST,
            &format!("/api/experiment-trees/{}/branches", tree_id.as_str()),
            Some(serde_json::json!({
                "parent_branch_id": "main",
                "name": "branch-a",
                "branch_point_cell_id": "cell_1",
                "first_cell": branch_cell_payload(
                    tree_id.as_str(),
                    "ignored",
                    "branch_cell_1",
                    "branch_value = 2\n"
                )
            })),
        )
        .await;
        assert_eq!(create_branch.status(), StatusCode::CREATED);
        let branch_id: BranchId = read_json(create_branch).await;

        let add_cell = send_json(
            &app,
            Method::POST,
            &format!(
                "/api/experiment-trees/{}/branches/{}/cells",
                tree_id.as_str(),
                branch_id.as_str()
            ),
            Some(serde_json::json!({
                "cell": branch_cell_payload(
                    tree_id.as_str(),
                    branch_id.as_str(),
                    "branch_cell_2",
                    "branch_value_2 = branch_value + 1\n"
                ),
                "after_cell_id": "branch_cell_1"
            })),
        )
        .await;
        assert_eq!(add_cell.status(), StatusCode::CREATED);

        let update_code = send_json(
            &app,
            Method::POST,
            &format!(
                "/api/experiment-trees/{}/branches/{}/cells/branch_cell_2/code",
                tree_id.as_str(),
                branch_id.as_str()
            ),
            Some(serde_json::json!({
                "source": "branch_value_2 = branch_value + 5\n"
            })),
        )
        .await;
        assert_eq!(update_code.status(), StatusCode::OK);

        let get_tree = send_json(
            &app,
            Method::GET,
            &format!("/api/experiment-trees/{}", tree_id.as_str()),
            None,
        )
        .await;
        assert_eq!(get_tree.status(), StatusCode::OK);
        let tree: ExperimentTreeDef = read_json(get_tree).await;
        let branch = tree
            .branches
            .iter()
            .find(|branch| branch.id == branch_id)
            .expect("expected created branch");
        assert_eq!(
            branch.cell_order,
            vec![
                tine_core::CellId::new("branch_cell_1"),
                tine_core::CellId::new("branch_cell_2")
            ]
        );
        let edited_cell = tree
            .cells
            .iter()
            .find(|cell| cell.id.as_str() == "branch_cell_2")
            .expect("expected edited branch cell");
        assert_eq!(edited_cell.branch_id, branch_id);
        assert_eq!(
            edited_cell.code.source,
            "branch_value_2 = branch_value + 5\n"
        );
    }

    #[tokio::test]
    #[serial]
    async fn test_branch_create_accepts_lightweight_first_cell_over_http() {
        let (_tmp, app) = test_app().await;

        let create_tree = send_json(
            &app,
            Method::POST,
            "/api/experiment-trees",
            Some(create_tree_payload("branch-http-lightweight", None)),
        )
        .await;
        assert_eq!(create_tree.status(), StatusCode::CREATED);
        let tree: ExperimentTreeDef = read_json(create_tree).await;
        let tree_id = tree.id.clone();

        let create_branch = send_json(
            &app,
            Method::POST,
            &format!("/api/experiment-trees/{}/branches", tree_id.as_str()),
            Some(serde_json::json!({
                "parent_branch_id": "main",
                "name": "branch-lightweight",
                "branch_point_cell_id": "cell_1",
                "first_cell": lightweight_branch_cell_payload("branch_value = 2\nprint(branch_value)\n")
            })),
        )
        .await;
        assert_eq!(create_branch.status(), StatusCode::CREATED);
        let branch_id: BranchId = read_json(create_branch).await;

        let get_tree = send_json(
            &app,
            Method::GET,
            &format!("/api/experiment-trees/{}", tree_id.as_str()),
            None,
        )
        .await;
        assert_eq!(get_tree.status(), StatusCode::OK);
        let loaded_tree: ExperimentTreeDef = read_json(get_tree).await;
        let branch_cell = loaded_tree
            .cells
            .iter()
            .find(|cell| cell.branch_id == branch_id)
            .expect("expected lightweight branch cell to be stored");

        assert_eq!(branch_cell.tree_id, tree_id);
        assert_eq!(branch_cell.name, "branch-lightweight");
        assert_eq!(branch_cell.code.source, "branch_value = 2\nprint(branch_value)\n");
        assert_eq!(branch_cell.code.language, "python");
        assert_eq!(branch_cell.declared_outputs, vec![SlotName::new("branch_value")]);
        assert!(!branch_cell.cache);
    }

    #[tokio::test]
    #[serial]
    async fn test_execute_poll_status_and_fetch_logs_over_http() {
        let (_tmp, app) = test_app().await;

        let create_tree = send_json(
            &app,
            Method::POST,
            "/api/experiment-trees",
            Some(create_tree_payload("execute-http", None)),
        )
        .await;
        assert_eq!(create_tree.status(), StatusCode::CREATED);
        let tree: ExperimentTreeDef = read_json(create_tree).await;
        let tree_id = tree.id.clone();

        let warmup_code = send_json(
            &app,
            Method::POST,
            &format!(
                "/api/experiment-trees/{}/branches/main/cells/cell_1/code",
                tree_id.as_str()
            ),
            Some(serde_json::json!({
                "source": "print('warmup')\ncell_1 = 1\n"
            })),
        )
        .await;
        assert_eq!(warmup_code.status(), StatusCode::OK);

        let warmup_execute = send_json(
            &app,
            Method::POST,
            &format!(
                "/api/experiment-trees/{}/branches/main/execute",
                tree_id.as_str()
            ),
            None,
        )
        .await;
        assert_eq!(warmup_execute.status(), StatusCode::ACCEPTED);
        let warmup_body: ExecutionAccepted = read_json(warmup_execute).await;
        assert_eq!(warmup_body.phase, tine_core::ExecutionPhase::Queued);
        let warmup_execution_id = warmup_body.execution_id.as_str().to_string();
        let warmup_status = wait_for_finished_status(&app, &warmup_execution_id).await;
        assert_eq!(
            warmup_status
                .node_statuses
                .get(&tine_core::NodeId::new("cell_1")),
            Some(&tine_core::NodeStatus::Completed)
        );

        let update_code = send_json(
            &app,
            Method::POST,
            &format!(
                "/api/experiment-trees/{}/branches/main/cells/cell_1/code",
                tree_id.as_str()
            ),
            Some(serde_json::json!({
                "source": "print('hello from rest test')\ncell_1 = 41\n"
            })),
        )
        .await;
        assert_eq!(update_code.status(), StatusCode::OK);

        let execute = send_json(
            &app,
            Method::POST,
            &format!(
                "/api/experiment-trees/{}/branches/main/execute",
                tree_id.as_str()
            ),
            None,
        )
        .await;
        assert_eq!(execute.status(), StatusCode::ACCEPTED);
        let execute_body: ExecutionAccepted = read_json(execute).await;
        assert_eq!(execute_body.phase, tine_core::ExecutionPhase::Queued);
        let execution_id = execute_body.execution_id.as_str().to_string();

        let status = wait_for_finished_status(&app, &execution_id).await;
        assert_eq!(status.tree_id.as_ref(), Some(&tree_id));
        assert_eq!(status.status, tine_core::ExecutionLifecycleStatus::Completed);
        assert_eq!(status.phase, tine_core::ExecutionPhase::Completed);

        let logs_response = send_json(
            &app,
            Method::GET,
            &format!(
                "/api/experiment-trees/{}/branches/main/cells/cell_1/logs",
                tree_id.as_str()
            ),
            None,
        )
        .await;
        assert_eq!(logs_response.status(), StatusCode::OK);
        let logs: tine_core::NodeLogs = read_json(logs_response).await;
        assert!(
            logs.stdout.contains("hello from rest test"),
            "expected stdout from executed cell, got {:?}",
            logs.stdout
        );
    }

    #[tokio::test]
    #[serial]
    async fn test_missing_execution_is_404_and_missing_logs_are_empty() {
        let (_tmp, app) = test_app().await;

        let missing_status =
            send_json(&app, Method::GET, "/api/executions/missing-exec", None).await;
        assert_eq!(missing_status.status(), StatusCode::NOT_FOUND);
        let missing_status_body: serde_json::Value = read_json(missing_status).await;
        assert!(missing_status_body["error"]
            .as_str()
            .unwrap()
            .contains("missing-exec"));
    }

    #[tokio::test]
    #[serial]
    async fn test_project_scoped_file_write_read_and_list_over_http() {
        let (_tmp, app) = test_app().await;

        let create_project = send_json(
            &app,
            Method::POST,
            "/api/projects",
            Some(serde_json::json!({
                "name": "files-project",
                "workspace_dir": "project-a"
            })),
        )
        .await;
        assert_eq!(create_project.status(), StatusCode::OK);
        let project_body: serde_json::Value = read_json(create_project).await;
        let project_id = project_body["id"].as_str().unwrap().to_string();

        let write = send_json(
            &app,
            Method::POST,
            "/api/files/write",
            Some(serde_json::json!({
                "path": "src/notebook.py",
                "content": "print('project scoped file')\n",
                "project_id": project_id
            })),
        )
        .await;
        assert_eq!(write.status(), StatusCode::NO_CONTENT);

        let list_root = send_json(
            &app,
            Method::GET,
            &format!("/api/files?path=&project_id={project_id}"),
            None,
        )
        .await;
        assert_eq!(list_root.status(), StatusCode::OK);
        let root_entries: Vec<serde_json::Value> = read_json(list_root).await;
        assert_eq!(root_entries.len(), 1);
        assert_eq!(root_entries[0]["name"], "src");
        assert_eq!(root_entries[0]["is_dir"], true);

        let list_src = send_json(
            &app,
            Method::GET,
            &format!("/api/files?path=src&project_id={project_id}"),
            None,
        )
        .await;
        assert_eq!(list_src.status(), StatusCode::OK);
        let src_entries: Vec<serde_json::Value> = read_json(list_src).await;
        assert_eq!(src_entries.len(), 1);
        assert_eq!(src_entries[0]["name"], "notebook.py");
        assert_eq!(src_entries[0]["is_dir"], false);
        assert!(src_entries[0]["size"].as_u64().unwrap() > 0);

        let read = send_json(
            &app,
            Method::GET,
            &format!("/api/files/read?path=src/notebook.py&project_id={project_id}"),
            None,
        )
        .await;
        assert_eq!(read.status(), StatusCode::OK);
        assert_eq!(
            read.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/plain; charset=utf-8"
        );
        let content = read_text(read).await;
        assert_eq!(content, "print('project scoped file')\n");

        let workspace_root = send_json(&app, Method::GET, "/api/files?path=", None).await;
        assert_eq!(workspace_root.status(), StatusCode::OK);
        let workspace_entries: Vec<serde_json::Value> = read_json(workspace_root).await;
        assert!(
            workspace_entries.iter().all(|entry| entry["name"] != "src"),
            "project-scoped file should not appear in workspace root listing"
        );
    }

    #[tokio::test]
    #[serial]
    async fn test_project_file_routes_keep_projects_isolated_over_http() {
        let (_tmp, app) = test_app().await;

        let create_a = send_json(
            &app,
            Method::POST,
            "/api/projects",
            Some(serde_json::json!({
                "name": "project-a",
                "workspace_dir": "project-a"
            })),
        )
        .await;
        let project_a: serde_json::Value = read_json(create_a).await;
        let project_a_id = project_a["id"].as_str().unwrap().to_string();

        let create_b = send_json(
            &app,
            Method::POST,
            "/api/projects",
            Some(serde_json::json!({
                "name": "project-b",
                "workspace_dir": "project-b"
            })),
        )
        .await;
        let project_b: serde_json::Value = read_json(create_b).await;
        let project_b_id = project_b["id"].as_str().unwrap().to_string();

        for (project_id, content) in [
            (project_a_id.as_str(), "alpha\n"),
            (project_b_id.as_str(), "beta\n"),
        ] {
            let write = send_json(
                &app,
                Method::POST,
                "/api/files/write",
                Some(serde_json::json!({
                    "path": "notes.txt",
                    "content": content,
                    "project_id": project_id
                })),
            )
            .await;
            assert_eq!(write.status(), StatusCode::NO_CONTENT);
        }

        let read_a = send_json(
            &app,
            Method::GET,
            &format!("/api/files/read?path=notes.txt&project_id={project_a_id}"),
            None,
        )
        .await;
        let read_b = send_json(
            &app,
            Method::GET,
            &format!("/api/files/read?path=notes.txt&project_id={project_b_id}"),
            None,
        )
        .await;

        assert_eq!(read_text(read_a).await, "alpha\n");
        assert_eq!(read_text(read_b).await, "beta\n");
    }

    #[tokio::test]
    #[serial]
    async fn test_execute_branch_cell_and_fetch_tree_logs_over_http() {
        let (_tmp, app) = test_app().await;

        let create_tree = send_json(
            &app,
            Method::POST,
            "/api/experiment-trees",
            Some(create_tree_payload("execute-cell-http", None)),
        )
        .await;
        assert_eq!(create_tree.status(), StatusCode::CREATED);
        let base_tree: ExperimentTreeDef = read_json(create_tree).await;
        let tree_id = base_tree.id.clone();

        let _update_step1 = send_json(
            &app,
            Method::POST,
            &format!(
                "/api/experiment-trees/{}/branches/main/cells/cell_1/code",
                tree_id.as_str()
            ),
            Some(serde_json::json!({ "source": "cell_1 = 42\n" })),
        )
        .await;

        let _add_step2 = send_json(
            &app,
            Method::POST,
            &format!(
                "/api/experiment-trees/{}/branches/main/cells",
                tree_id.as_str()
            ),
            Some(serde_json::json!({
                "cell": {
                    "id": "step2",
                    "tree_id": tree_id.as_str(),
                    "branch_id": "main",
                    "name": "step2",
                    "code": {
                        "source": "step2 = cell_1 * 2\n",
                        "language": "python"
                    },
                    "upstream_cell_ids": ["cell_1"],
                    "declared_outputs": ["step2"],
                    "cache": false,
                    "map_over": null,
                    "map_concurrency": null,
                    "tags": {},
                    "revision_id": null,
                    "state": "clean"
                },
                "after_cell_id": "cell_1"
            })),
        )
        .await;

        let create_branch = send_json(
            &app,
            Method::POST,
            &format!("/api/experiment-trees/{}/branches", tree_id.as_str()),
            Some(serde_json::json!({
                "parent_branch_id": "main",
                "name": "branch-runtime",
                "branch_point_cell_id": "step2",
                "first_cell": {
                    "id": "branch_cell_1",
                    "tree_id": tree_id.as_str(),
                    "branch_id": "ignored",
                    "name": "branch_cell_1",
                    "code": {
                        "source": "print(step2 + 1)\nbranch_value = step2 + 1\n",
                        "language": "python"
                    },
                    "upstream_cell_ids": ["step2"],
                    "declared_outputs": ["branch_value"],
                    "cache": false,
                    "map_over": null,
                    "map_concurrency": null,
                    "tags": {},
                    "revision_id": null,
                    "state": "clean"
                }
            })),
        )
        .await;
        assert_eq!(create_branch.status(), StatusCode::CREATED);
        let branch_id: BranchId = read_json(create_branch).await;

        let execute_cell = send_json(
            &app,
            Method::POST,
            &format!(
                "/api/experiment-trees/{}/branches/{}/cells/branch_cell_1/execute",
                tree_id.as_str(),
                branch_id.as_str()
            ),
            None,
        )
        .await;
        assert_eq!(execute_cell.status(), StatusCode::ACCEPTED);
        let execute_cell_body: ExecutionAccepted = read_json(execute_cell).await;
        assert_eq!(execute_cell_body.phase, tine_core::ExecutionPhase::Queued);
        let execution_id = execute_cell_body.execution_id.as_str().to_string();

        let status = wait_for_finished_status(&app, &execution_id).await;
        assert_eq!(status.tree_id.as_ref(), Some(&tree_id));
        assert_eq!(status.branch_id.as_ref(), Some(&branch_id));
        assert_eq!(status.status, tine_core::ExecutionLifecycleStatus::Completed);
        assert_eq!(status.phase, tine_core::ExecutionPhase::Completed);

        let persisted_logs = send_json(
            &app,
            Method::GET,
            &format!(
                "/api/experiment-trees/{}/branches/{}/cells/branch_cell_1/logs",
                tree_id.as_str(),
                branch_id.as_str()
            ),
            None,
        )
        .await;
        assert_eq!(persisted_logs.status(), StatusCode::OK);
        let persisted_logs: tine_core::NodeLogs = read_json(persisted_logs).await;
        assert!(
            persisted_logs.stdout.contains("85"),
            "expected persisted tree-cell logs, got {:?}",
            persisted_logs.stdout
        );
    }

    #[tokio::test]
    #[serial]
    async fn test_execute_branch_over_http_persists_branch_status_and_logs() {
        let (_tmp, app) = test_app().await;

        let create_tree = send_json(
            &app,
            Method::POST,
            "/api/experiment-trees",
            Some(create_tree_payload("branch-exec-http", None)),
        )
        .await;
        assert_eq!(create_tree.status(), StatusCode::CREATED);
        let base_tree: ExperimentTreeDef = read_json(create_tree).await;
        let tree_id = base_tree.id.clone();

        let _update_step1 = send_json(
            &app,
            Method::POST,
            &format!(
                "/api/experiment-trees/{}/branches/main/cells/cell_1/code",
                tree_id.as_str()
            ),
            Some(serde_json::json!({ "source": "cell_1 = 42\n" })),
        )
        .await;

        let _add_step2 = send_json(
            &app,
            Method::POST,
            &format!(
                "/api/experiment-trees/{}/branches/main/cells",
                tree_id.as_str()
            ),
            Some(serde_json::json!({
                "cell": {
                    "id": "step2",
                    "tree_id": tree_id.as_str(),
                    "branch_id": "main",
                    "name": "step2",
                    "code": {
                        "source": "step2 = cell_1 * 2\n",
                        "language": "python"
                    },
                    "upstream_cell_ids": ["cell_1"],
                    "declared_outputs": ["step2"],
                    "cache": false,
                    "map_over": null,
                    "map_concurrency": null,
                    "tags": {},
                    "revision_id": null,
                    "state": "clean"
                },
                "after_cell_id": "cell_1"
            })),
        )
        .await;

        let create_branch = send_json(
            &app,
            Method::POST,
            &format!("/api/experiment-trees/{}/branches", tree_id.as_str()),
            Some(serde_json::json!({
                "parent_branch_id": "main",
                "name": "branch-run",
                "branch_point_cell_id": "step2",
                "first_cell": {
                    "id": "branch_cell_1",
                    "tree_id": tree_id.as_str(),
                    "branch_id": "ignored",
                    "name": "branch_cell_1",
                    "code": {
                        "source": "print(step2 + 1)\nbranch_value = step2 + 1\n",
                        "language": "python"
                    },
                    "upstream_cell_ids": ["step2"],
                    "declared_outputs": ["branch_value"],
                    "cache": false,
                    "map_over": null,
                    "map_concurrency": null,
                    "tags": {},
                    "revision_id": null,
                    "state": "clean"
                }
            })),
        )
        .await;
        assert_eq!(create_branch.status(), StatusCode::CREATED);
        let branch_id: BranchId = read_json(create_branch).await;

        let execute_branch = send_json(
            &app,
            Method::POST,
            &format!(
                "/api/experiment-trees/{}/branches/{}/execute",
                tree_id.as_str(),
                branch_id.as_str()
            ),
            None,
        )
        .await;
        assert_eq!(execute_branch.status(), StatusCode::ACCEPTED);
        let execute_branch_body: ExecutionAccepted = read_json(execute_branch).await;
        assert_eq!(execute_branch_body.phase, tine_core::ExecutionPhase::Queued);
        let execution_id = execute_branch_body.execution_id.as_str();

        let status = wait_for_finished_status(&app, execution_id).await;
        assert_eq!(status.tree_id.as_ref(), Some(&tree_id));
        assert_eq!(status.branch_id.as_ref(), Some(&branch_id));
        assert_eq!(status.status, tine_core::ExecutionLifecycleStatus::Completed);
        assert_eq!(status.phase, tine_core::ExecutionPhase::Completed);
        assert_eq!(
            status
                .node_statuses
                .get(&tine_core::NodeId::new("branch_cell_1")),
            Some(&tine_core::NodeStatus::Completed)
        );

        let branch_logs = send_json(
            &app,
            Method::GET,
            &format!(
                "/api/experiment-trees/{}/branches/{}/cells/branch_cell_1/logs",
                tree_id.as_str(),
                branch_id.as_str()
            ),
            None,
        )
        .await;
        assert_eq!(branch_logs.status(), StatusCode::OK);
        let branch_logs: tine_core::NodeLogs = read_json(branch_logs).await;
        assert!(
            branch_logs.stdout.contains("85"),
            "expected branch execution logs to be fetchable by tree/branch/cell, got {:?}",
            branch_logs.stdout
        );
    }

    #[tokio::test]
    #[serial]
    async fn test_cancel_over_http_preserves_partial_logs_and_keeps_server_responsive() {
        let tmp = TempDir::new().expect("failed to create temp dir");
        let store: Arc<dyn ArtifactStore> = Arc::new(MemoryArtifactStore::new());
        let workspace = Arc::new(
            Workspace::open(tmp.path().to_path_buf(), store, 4)
                .await
                .expect("failed to open workspace"),
        );
        let state = Arc::new(AppState {
            workspace: workspace.clone(),
            metrics_handle: None,
            ui_dir: PathBuf::from("ui"),
            api_base_url: "http://127.0.0.1:9473".to_string(),
        });
        let app = build_router(state);

        let create_tree = send_json(
            &app,
            Method::POST,
            "/api/experiment-trees",
            Some(create_tree_payload("cancel-http", None)),
        )
        .await;
        assert_eq!(create_tree.status(), StatusCode::CREATED);
        let tree: ExperimentTreeDef = read_json(create_tree).await;
        let tree_id = tree.id.clone();

        let warmup_code = send_json(
            &app,
            Method::POST,
            &format!(
                "/api/experiment-trees/{}/branches/main/cells/cell_1/code",
                tree_id.as_str()
            ),
            Some(serde_json::json!({
                "source": "print('warmup')\ncell_1 = 1\n"
            })),
        )
        .await;
        assert_eq!(warmup_code.status(), StatusCode::OK);

        let warmup_execute = send_json(
            &app,
            Method::POST,
            &format!(
                "/api/experiment-trees/{}/branches/main/execute",
                tree_id.as_str()
            ),
            None,
        )
        .await;
        assert_eq!(warmup_execute.status(), StatusCode::ACCEPTED);
        let warmup_body: ExecutionAccepted = read_json(warmup_execute).await;
        let warmup_execution_id = warmup_body.execution_id.as_str().to_string();
        let warmup_status = wait_for_finished_status(&app, &warmup_execution_id).await;
        assert_eq!(
            warmup_status
                .node_statuses
                .get(&tine_core::NodeId::new("cell_1")),
            Some(&tine_core::NodeStatus::Completed)
        );
        let mut runtime_state =
            workspace
                .get_tree_runtime_state(&tree_id)
                .await
                .unwrap_or(TreeRuntimeState {
                    tree_id: tree_id.clone(),
                    active_branch_id: BranchId::new("main"),
                    materialized_path_cell_ids: vec![CellId::new("cell_1")],
                    runtime_epoch: 0,
                    kernel_state: TreeKernelState::Ready,
                    last_prepared_cell_id: Some(CellId::new("cell_1")),
                    isolation_mode: BranchIsolationMode::Disabled,
                    last_isolation_result: None,
                });
        runtime_state.isolation_mode = BranchIsolationMode::NamespaceGuarded;
        workspace
            .set_tree_runtime_state(runtime_state)
            .await
            .expect("failed to update isolation mode");

        let update_code = send_json(
            &app,
            Method::POST,
            &format!(
                "/api/experiment-trees/{}/branches/main/cells/cell_1/code",
                tree_id.as_str()
            ),
            Some(serde_json::json!({
                "source": "import time\nprint('starting cancel test')\ntime.sleep(20)\nprint('should not reach here')\ncell_1 = 1\n"
            })),
        )
        .await;
        assert_eq!(update_code.status(), StatusCode::OK);

        let execute = send_json(
            &app,
            Method::POST,
            &format!(
                "/api/experiment-trees/{}/branches/main/execute",
                tree_id.as_str()
            ),
            None,
        )
        .await;
        assert_eq!(execute.status(), StatusCode::ACCEPTED);
        let execute_body: ExecutionAccepted = read_json(execute).await;
        let execution_id = execute_body.execution_id.as_str().to_string();

        let mut saw_running = false;
        for _ in 0..240 {
            let status = send_json(
                &app,
                Method::GET,
                &format!("/api/executions/{execution_id}"),
                None,
            )
            .await;
            assert_eq!(status.status(), StatusCode::OK);
            let status: tine_core::ExecutionStatus = read_json(status).await;
            if matches!(
                status.node_statuses.get(&tine_core::NodeId::new("cell_1")),
                Some(tine_core::NodeStatus::Running)
            ) {
                saw_running = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }
        assert!(
            saw_running,
            "timed out waiting for execution to enter running state before cancel"
        );
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        let cancel = send_json(
            &app,
            Method::POST,
            &format!("/api/executions/{execution_id}/cancel"),
            None,
        )
        .await;
        assert_eq!(cancel.status(), StatusCode::OK);

        let requested_status = send_json(
            &app,
            Method::GET,
            &format!("/api/executions/{execution_id}"),
            None,
        )
        .await;
        assert_eq!(requested_status.status(), StatusCode::OK);
        let requested_status: tine_core::ExecutionStatus = read_json(requested_status).await;
        assert_eq!(requested_status.status, tine_core::ExecutionLifecycleStatus::Running);
        assert_eq!(requested_status.phase, tine_core::ExecutionPhase::CancellationRequested);
        assert!(requested_status.cancellation_requested_at.is_some());

        let cancel_again = send_json(
            &app,
            Method::POST,
            &format!("/api/executions/{execution_id}/cancel"),
            None,
        )
        .await;
        assert_eq!(cancel_again.status(), StatusCode::OK);

        let status = loop {
            let status = send_json(
                &app,
                Method::GET,
                &format!("/api/executions/{execution_id}"),
                None,
            )
            .await;
            assert_eq!(status.status(), StatusCode::OK);
            let status: tine_core::ExecutionStatus = read_json(status).await;
            if status.finished_at.is_some()
                && status.status == tine_core::ExecutionLifecycleStatus::Cancelled
            {
                break status;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        };
        assert_eq!(status.tree_id.as_ref(), Some(&tree_id));
        assert_eq!(status.status, tine_core::ExecutionLifecycleStatus::Cancelled);
        assert_eq!(status.phase, tine_core::ExecutionPhase::Cancelled);
        assert!(status.cancellation_requested_at.is_some());
        assert_eq!(
            status.node_statuses.get(&tine_core::NodeId::new("cell_1")),
            Some(&tine_core::NodeStatus::Interrupted)
        );

        let logs_response = send_json(
            &app,
            Method::GET,
            &format!(
                "/api/experiment-trees/{}/branches/main/cells/cell_1/logs",
                tree_id.as_str()
            ),
            None,
        )
        .await;
        assert_eq!(logs_response.status(), StatusCode::OK);
        let logs: tine_core::NodeLogs = read_json(logs_response).await;
        assert!(
            logs.stdout.contains("starting cancel test"),
            "expected partial stdout to persist on cancel, got {:?}",
            logs.stdout
        );
        assert!(
            !logs.stdout.contains("should not reach here"),
            "unexpected post-cancel output in {:?}",
            logs.stdout
        );

        let health = send_json(&app, Method::GET, "/healthz", None).await;
        assert_eq!(health.status(), StatusCode::OK);
        let health_text = read_text(health).await;
        assert_eq!(health_text.trim(), "ok");

        let default_projects_dir =
            send_json(&app, Method::GET, "/api/system/default-projects-dir", None).await;
        assert_eq!(default_projects_dir.status(), StatusCode::OK);
        let default_projects_dir_body: serde_json::Value = read_json(default_projects_dir).await;
        assert!(default_projects_dir_body["path"]
            .as_str()
            .is_some_and(|value| !value.is_empty()));
        assert!(default_projects_dir_body["native_picker_available"].is_boolean());
    }

    #[tokio::test]
    #[serial]
    async fn test_export_branch_formats_over_http() {
        let (_tmp, app) = test_app().await;

        let create_tree = send_json(
            &app,
            Method::POST,
            "/api/experiment-trees",
            Some(create_tree_payload("export-http", None)),
        )
        .await;
        assert_eq!(create_tree.status(), StatusCode::CREATED);
        let base_tree: ExperimentTreeDef = read_json(create_tree).await;
        let tree_id = base_tree.id.clone();

        let create_branch = send_json(
            &app,
            Method::POST,
            &format!("/api/experiment-trees/{}/branches", tree_id.as_str()),
            Some(serde_json::json!({
                "parent_branch_id": "main",
                "name": "branch-export",
                "branch_point_cell_id": "cell_1",
                "first_cell": branch_cell_payload(
                    tree_id.as_str(),
                    "ignored",
                    "branch_cell_1",
                    "branch_value = value + 1\n"
                )
            })),
        )
        .await;
        assert_eq!(create_branch.status(), StatusCode::CREATED);
        let branch_id: BranchId = read_json(create_branch).await;

        let export_main_py = send_json(
            &app,
            Method::GET,
            &format!(
                "/api/experiment-trees/{}/branches/main/export.py",
                tree_id.as_str()
            ),
            None,
        )
        .await;
        assert_eq!(export_main_py.status(), StatusCode::OK);
        assert_eq!(
            export_main_py.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/x-python; charset=utf-8"
        );
        assert!(export_main_py
            .headers()
            .get(header::CONTENT_DISPOSITION)
            .unwrap()
            .to_str()
            .unwrap()
            .contains(".py"));
        let main_py = read_text(export_main_py).await;
        assert!(main_py.contains("# %% [cell_1] Cell 1"));
        assert!(!main_py.contains("branch_cell_1"));

        let export_branch_ipynb = send_json(
            &app,
            Method::GET,
            &format!(
                "/api/experiment-trees/{}/branches/{}/export.ipynb",
                tree_id.as_str(),
                branch_id.as_str()
            ),
            None,
        )
        .await;
        assert_eq!(export_branch_ipynb.status(), StatusCode::OK);
        assert_eq!(
            export_branch_ipynb
                .headers()
                .get(header::CONTENT_TYPE)
                .unwrap(),
            "application/x-ipynb+json"
        );
        let notebook: serde_json::Value = read_json(export_branch_ipynb).await;
        let cells = notebook["cells"].as_array().unwrap();
        assert_eq!(cells.len(), 2);
        assert_eq!(cells[0]["metadata"]["tine"]["cell_id"], "cell_1");
        assert_eq!(cells[1]["metadata"]["tine"]["cell_id"], "branch_cell_1");
        assert_eq!(
            notebook["metadata"]["tine"]["branch_id"],
            branch_id.as_str()
        );
    }

    #[tokio::test]
    #[serial]
    async fn test_delete_tree_branch_removes_subtree_and_rejects_main() {
        let (_tmp, app) = test_app().await;

        let create_tree = send_json(
            &app,
            Method::POST,
            "/api/experiment-trees",
            Some(create_tree_payload("delete-branch-http", None)),
        )
        .await;
        assert_eq!(create_tree.status(), StatusCode::CREATED);
        let base_tree: ExperimentTreeDef = read_json(create_tree).await;
        let tree_id = base_tree.id.clone();

        let create_branch = send_json(
            &app,
            Method::POST,
            &format!("/api/experiment-trees/{}/branches", tree_id.as_str()),
            Some(serde_json::json!({
                "parent_branch_id": "main",
                "name": "branch-a",
                "branch_point_cell_id": "cell_1",
                "first_cell": branch_cell_payload(
                    tree_id.as_str(),
                    "ignored",
                    "branch_cell_1",
                    "branch_value = value + 1\n"
                )
            })),
        )
        .await;
        assert_eq!(create_branch.status(), StatusCode::CREATED);
        let branch_a_id: BranchId = read_json(create_branch).await;

        let create_child_branch = send_json(
            &app,
            Method::POST,
            &format!("/api/experiment-trees/{}/branches", tree_id.as_str()),
            Some(serde_json::json!({
                "parent_branch_id": branch_a_id.as_str(),
                "name": "branch-b",
                "branch_point_cell_id": "branch_cell_1",
                "first_cell": branch_cell_payload(
                    tree_id.as_str(),
                    "ignored",
                    "branch_cell_2",
                    "branch_value_2 = branch_value + 1\n"
                )
            })),
        )
        .await;
        assert_eq!(create_child_branch.status(), StatusCode::CREATED);
        let branch_b_id: BranchId = read_json(create_child_branch).await;

        let delete_branch = send_json(
            &app,
            Method::DELETE,
            &format!(
                "/api/experiment-trees/{}/branches/{}",
                tree_id.as_str(),
                branch_a_id.as_str()
            ),
            None,
        )
        .await;
        assert_eq!(delete_branch.status(), StatusCode::NO_CONTENT);

        let get_tree = send_json(
            &app,
            Method::GET,
            &format!("/api/experiment-trees/{}", tree_id.as_str()),
            None,
        )
        .await;
        assert_eq!(get_tree.status(), StatusCode::OK);
        let tree: ExperimentTreeDef = read_json(get_tree).await;
        assert_eq!(tree.branches.len(), 1);
        assert_eq!(tree.branches[0].id, BranchId::new("main"));
        assert!(!tree
            .cells
            .iter()
            .any(|cell| cell.id.as_str() == "branch_cell_1"));
        assert!(!tree
            .cells
            .iter()
            .any(|cell| cell.id.as_str() == "branch_cell_2"));
        assert!(!tree.branches.iter().any(|branch| branch.id == branch_b_id));

        let delete_main = send_json(
            &app,
            Method::DELETE,
            &format!("/api/experiment-trees/{}/branches/main", tree_id.as_str()),
            None,
        )
        .await;
        assert_eq!(delete_main.status(), StatusCode::BAD_REQUEST);
        let error_body: serde_json::Value = read_json(delete_main).await;
        assert!(error_body["error"]
            .as_str()
            .unwrap()
            .contains("cannot delete the main branch"));
    }

    // ── move_cell tests ────────────────────────────────────────────────

    /// Helper: create a tree and add two extra cells to main so cell_order = [cell_1, cell_2, cell_3].
    async fn tree_with_three_main_cells(app: &Router) -> (ExperimentTreeId, BranchId) {
        let resp = send_json(
            app,
            Method::POST,
            "/api/experiment-trees",
            Some(create_tree_payload("move-del-test", None)),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::CREATED);
        let tree: ExperimentTreeDef = read_json(resp).await;
        let tid = tree.id.clone();
        let bid = BranchId::new("main");

        for (cell_id, after, src) in [
            ("cell_2", "cell_1", "x = 2\n"),
            ("cell_3", "cell_2", "x = 3\n"),
        ] {
            let resp = send_json(
                app,
                Method::POST,
                &format!("/api/experiment-trees/{}/branches/main/cells", tid.as_str()),
                Some(serde_json::json!({
                    "cell": branch_cell_payload(tid.as_str(), "main", cell_id, src),
                    "after_cell_id": after
                })),
            )
            .await;
            assert_eq!(resp.status(), StatusCode::CREATED);
        }
        (tid, bid)
    }

    fn cell_order_ids(tree: &ExperimentTreeDef, branch_id: &BranchId) -> Vec<String> {
        tree.branches
            .iter()
            .find(|b| &b.id == branch_id)
            .expect("branch not found")
            .cell_order
            .iter()
            .map(|c| c.as_str().to_string())
            .collect()
    }

    #[tokio::test]
    #[serial]
    async fn test_move_cell_down_reorders() {
        let (_tmp, app) = test_app().await;
        let (tid, bid) = tree_with_three_main_cells(&app).await;

        // Move cell_1 down → [cell_2, cell_1, cell_3]
        let resp = send_json(
            &app,
            Method::POST,
            &format!(
                "/api/experiment-trees/{}/branches/main/cells/cell_1/move",
                tid.as_str()
            ),
            Some(serde_json::json!({"direction": "down"})),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);

        let tree: ExperimentTreeDef = read_json(
            send_json(
                &app,
                Method::GET,
                &format!("/api/experiment-trees/{}", tid.as_str()),
                None,
            )
            .await,
        )
        .await;
        assert_eq!(
            cell_order_ids(&tree, &bid),
            vec!["cell_2", "cell_1", "cell_3"]
        );
    }

    #[tokio::test]
    #[serial]
    async fn test_move_cell_up_reorders() {
        let (_tmp, app) = test_app().await;
        let (tid, bid) = tree_with_three_main_cells(&app).await;

        // Move cell_3 up → [cell_1, cell_3, cell_2]
        let resp = send_json(
            &app,
            Method::POST,
            &format!(
                "/api/experiment-trees/{}/branches/main/cells/cell_3/move",
                tid.as_str()
            ),
            Some(serde_json::json!({"direction": "up"})),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);

        let tree: ExperimentTreeDef = read_json(
            send_json(
                &app,
                Method::GET,
                &format!("/api/experiment-trees/{}", tid.as_str()),
                None,
            )
            .await,
        )
        .await;
        assert_eq!(
            cell_order_ids(&tree, &bid),
            vec!["cell_1", "cell_3", "cell_2"]
        );
    }

    #[tokio::test]
    #[serial]
    async fn test_move_cell_boundary_is_noop() {
        let (_tmp, app) = test_app().await;
        let (tid, bid) = tree_with_three_main_cells(&app).await;

        // Move first cell up → no-op
        let resp = send_json(
            &app,
            Method::POST,
            &format!(
                "/api/experiment-trees/{}/branches/main/cells/cell_1/move",
                tid.as_str()
            ),
            Some(serde_json::json!({"direction": "up"})),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);

        // Move last cell down → no-op
        let resp = send_json(
            &app,
            Method::POST,
            &format!(
                "/api/experiment-trees/{}/branches/main/cells/cell_3/move",
                tid.as_str()
            ),
            Some(serde_json::json!({"direction": "down"})),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);

        let tree: ExperimentTreeDef = read_json(
            send_json(
                &app,
                Method::GET,
                &format!("/api/experiment-trees/{}", tid.as_str()),
                None,
            )
            .await,
        )
        .await;
        assert_eq!(
            cell_order_ids(&tree, &bid),
            vec!["cell_1", "cell_2", "cell_3"]
        );
    }

    // ── delete_cell tests ──────────────────────────────────────────────

    #[tokio::test]
    #[serial]
    async fn test_delete_cell_removes_from_order_and_cells() {
        let (_tmp, app) = test_app().await;
        let (tid, bid) = tree_with_three_main_cells(&app).await;

        // Delete cell_2
        let resp = send_json(
            &app,
            Method::DELETE,
            &format!(
                "/api/experiment-trees/{}/branches/main/cells/cell_2",
                tid.as_str()
            ),
            None,
        )
        .await;
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);

        let tree: ExperimentTreeDef = read_json(
            send_json(
                &app,
                Method::GET,
                &format!("/api/experiment-trees/{}", tid.as_str()),
                None,
            )
            .await,
        )
        .await;
        assert_eq!(cell_order_ids(&tree, &bid), vec!["cell_1", "cell_3"]);
        assert!(!tree.cells.iter().any(|c| c.id.as_str() == "cell_2"));
    }

    #[tokio::test]
    #[serial]
    async fn test_delete_nonexistent_cell_returns_error() {
        let (_tmp, app) = test_app().await;
        let (tid, _bid) = tree_with_three_main_cells(&app).await;

        let resp = send_json(
            &app,
            Method::DELETE,
            &format!(
                "/api/experiment-trees/{}/branches/main/cells/no_such_cell",
                tid.as_str()
            ),
            None,
        )
        .await;
        assert_ne!(resp.status(), StatusCode::NO_CONTENT);
    }

    #[test]
    fn resolve_ui_dir_uses_tine_ui_dir_env_var() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let workspace_dir = tmp.path().join("workspace");
        let packaged_ui = tmp.path().join("packaged-ui");
        std::fs::create_dir_all(&workspace_dir).expect("workspace dir");
        std::fs::create_dir_all(&packaged_ui).expect("packaged ui dir");
        std::fs::write(packaged_ui.join("index.html"), "<html></html>").expect("index.html");

        std::env::set_var("TINE_UI_DIR", &packaged_ui);
        let resolved = resolve_ui_dir(&workspace_dir).expect("resolve ui dir");
        std::env::remove_var("TINE_UI_DIR");

        assert_eq!(resolved, packaged_ui);
    }
}
