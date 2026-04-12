// Prevents additional console window on Windows in release
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU16, Ordering};

use async_trait::async_trait;
use chrono::Utc;
use tauri::Manager;
use tauri_plugin_dialog::{DialogExt, FilePath};
use tracing::info;

use tine_api::Workspace;
use tine_core::{ArtifactKey, ArtifactMetadata, ArtifactStore, TineResult};
use tine_server::{default_projects_root, resolve_ui_dir};

/// Resolve the bundled Python interpreter inside the app's Tauri
/// `resource_dir` and export it to the current process environment so that
/// the embedded backend (`tine-env`) picks it up via its existing
/// `TINE_BUNDLED_PYTHON` resolution logic.
///
/// The backend already honours `TINE_BUNDLED_PYTHON` ahead of `uv python find`
/// and PATH-based discovery (see `crates/tine-env/src/environment.rs`), so this
/// function does not need to modify any backend code — it just has to set the
/// variables before the embedded server is constructed.
///
/// Behaviour:
///   - If `resource_dir/runtime/python/...` exists, `TINE_BUNDLED_PYTHON` is set.
///   - If the runtime dir is absent (for example in a `cargo run -p tine-app`
///     dev checkout where `scripts/release/fetch_app_runtime.py` has not been
///     run), a loud warning is logged with remediation steps and the app falls
///     back to the backend's standard discovery logic. This keeps the dev loop
///     fast for frontend iteration while giving developers without a
///     compatible system Python a clear next step.
fn configure_bundled_runtime(app: &tauri::App) {
    // `Manager` trait (already imported at crate root) brings `.path()` into scope.
    let resource_dir = match app.path().resource_dir() {
        Ok(dir) => dir,
        Err(error) => {
            tracing::warn!(
                %error,
                "unable to resolve resource_dir; skipping bundled runtime wiring"
            );
            return;
        }
    };

    let runtime_dir = [
        resource_dir.join("runtime"),
        resource_dir.join("resources").join("runtime"),
    ]
    .into_iter()
    .find(|dir| dir.join("python").exists() || dir.exists())
    .unwrap_or_else(|| resource_dir.join("runtime"));

    if let Some(bundled_ui_dir) = [
        resource_dir.join("ui"),
        resource_dir.join("_up_").join("_up_").join("ui"),
    ]
    .into_iter()
    .find(|dir| dir.join("index.html").is_file())
    {
        std::env::set_var("TINE_UI_DIR", &bundled_ui_dir);
        tracing::info!(path = %bundled_ui_dir.display(), "using bundled ui assets");
    }

    if !runtime_dir.exists() {
        tracing::warn!(
            path = %runtime_dir.display(),
            "no bundled runtime found; falling back to host Python discovery. \
             For the full production experience run: \
             python3 scripts/release/fetch_app_runtime.py --target <your-target>"
        );
        return;
    }

    let python_bin = if cfg!(windows) {
        runtime_dir.join("python").join("python.exe")
    } else {
        runtime_dir.join("python").join("bin").join("python3")
    };
    if python_bin.exists() {
        std::env::set_var("TINE_BUNDLED_PYTHON", &python_bin);
        tracing::info!(path = %python_bin.display(), "using bundled python");
    } else {
        tracing::warn!(
            path = %python_bin.display(),
            "runtime dir exists but bundled python binary is missing"
        );
    }

}

/// IPC: list experiment trees
#[tauri::command]
async fn list_experiment_trees(
    state: tauri::State<'_, AppState>,
) -> Result<serde_json::Value, String> {
    let ws = state
        .workspace
        .lock()
        .map_err(|_| "embedded workspace lock poisoned".to_string())?
        .clone()
        .ok_or_else(|| "embedded workspace not ready".to_string())?;
    let trees = ws
        .list_experiment_trees()
        .await
        .map_err(|e| e.to_string())?;
    serde_json::to_value(&trees).map_err(|e| e.to_string())
}

/// IPC: get workspace path
#[tauri::command]
fn workspace_path(state: tauri::State<'_, AppState>) -> String {
    state.workspace_dir.display().to_string()
}

/// IPC: get the default base directory for user projects.
#[tauri::command]
fn default_projects_dir(state: tauri::State<'_, AppState>) -> String {
    default_projects_root(&state.workspace_dir)
        .display()
        .to_string()
}

/// IPC: open a native folder picker for choosing a project directory.
#[tauri::command]
async fn pick_project_folder(
    app: tauri::AppHandle,
    initial_dir: Option<String>,
) -> Result<Option<String>, String> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let mut dialog = app.dialog().file().set_title("Choose Project Folder");
    if let Some(initial_dir) = initial_dir
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        dialog = dialog.set_directory(initial_dir);
    }
    dialog.pick_folder(move |folder| {
        let _ = tx.send(folder.map(file_path_to_string));
    });
    rx.await
        .map_err(|_| "folder picker was cancelled before returning".to_string())
}

/// IPC: open a native save dialog and write exported text content to disk.
#[tauri::command]
async fn save_export_file(
    app: tauri::AppHandle,
    suggested_name: String,
    contents: String,
) -> Result<Option<String>, String> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    app.dialog()
        .file()
        .set_title("Save Export")
        .set_file_name(&suggested_name)
        .save_file(move |path| {
            let _ = tx.send(path);
        });

    let selected = rx
        .await
        .map_err(|_| "save dialog was cancelled before returning".to_string())?;
    let Some(selected) = selected else {
        return Ok(None);
    };
    let path = file_path_to_path_buf(selected)?;
    tokio::fs::write(&path, contents.as_bytes())
        .await
        .map_err(|e| e.to_string())?;
    Ok(Some(path.display().to_string()))
}

/// IPC: get the embedded server port
#[tauri::command]
fn server_port(state: tauri::State<'_, AppState>) -> Result<u16, String> {
    let port = state.port.load(Ordering::Relaxed);
    if port == 0 {
        Err("embedded server port unavailable".to_string())
    } else {
        Ok(port)
    }
}

#[derive(Clone)]
struct AppState {
    workspace: Arc<Mutex<Option<Arc<Workspace>>>>,
    workspace_dir: PathBuf,
    port: Arc<AtomicU16>,
}

fn main() {
    // Initialise logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,tine=debug".into()),
        )
        .init();

    // Resolve workspace directory: arg > cwd
    let workspace_dir = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(resolve_workspace_dir);

    info!(workspace = %workspace_dir.display(), "tine-app starting");

    // Build Tauri app
    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_shell::init())
        .on_page_load(|_, payload| {
            info!(url = %payload.url(), event = ?payload.event(), "webview page load");
        })
        .setup(move |app| {
            // Wire bundled Python into the process environment before the
            // embedded server (and the backend `EnvironmentManager` it builds)
            // is constructed. This is the *only* place in the codebase that
            // references the bundled runtime; the web flow (`tine-cli serve`
            // and `pip install tine`) is untouched and still uses host Python.
            configure_bundled_runtime(app);

            let handle = app.handle().clone();
            let ws_dir = workspace_dir.clone();
            let state = AppState {
                workspace: Arc::new(Mutex::new(None)),
                workspace_dir: ws_dir.clone(),
                port: Arc::new(AtomicU16::new(0)),
            };

            app.manage(state.clone());

            // Spawn the embedded server on a background tokio task
            tauri::async_runtime::spawn(async move {
                match start_embedded_server(&ws_dir).await {
                    Ok((workspace, port)) => {
                        info!(port = port, "embedded tine server started");

                        if let Ok(mut state_workspace) = state.workspace.lock() {
                            *state_workspace = Some(workspace);
                        }
                        state.port.store(port, Ordering::Relaxed);

                        // Navigate the main window to the embedded server
                        if let Some(window) = handle.get_webview_window("main") {
                            let url = format!("http://127.0.0.1:{}", port);
                            match tauri::Url::parse(&url) {
                                Ok(url) => {
                                    if let Err(error) = window.navigate(url) {
                                        tracing::error!(%error, "failed to navigate main window");
                                    }
                                }
                                Err(error) => {
                                    tracing::error!(%error, "failed to parse embedded server url");
                                }
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "failed to start embedded server");
                    }
                }
            });

            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            list_experiment_trees,
            workspace_path,
            default_projects_dir,
            pick_project_folder,
            save_export_file,
            server_port,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tine");
}

fn resolve_workspace_dir() -> PathBuf {
    if let Some(root) = find_workspace_root() {
        return root;
    }

    if let Ok(current_dir) = std::env::current_dir() {
        if current_dir != Path::new("/") {
            return current_dir;
        }
    }

    default_projects_root(Path::new("."))
}

/// Start the tine axum server on a random free port.
async fn start_embedded_server(
    workspace_dir: &std::path::Path,
) -> Result<(Arc<Workspace>, u16), Box<dyn std::error::Error>> {
    // Ensure .tine/ directory exists
    let tine_dir = workspace_dir.join(".tine");
    tokio::fs::create_dir_all(&tine_dir).await?;

    let store: Arc<dyn ArtifactStore> =
        Arc::new(LocalArtifactStore::new(tine_dir.join("artifacts")));
    let workspace = Arc::new(Workspace::open(workspace_dir.to_path_buf(), store, 8).await?);

    // Bind to port 0 to get a random free port
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();

    let ui_dir = resolve_ui_dir(workspace_dir)?;

    let state = Arc::new(tine_server::AppState {
        workspace: workspace.clone(),
        metrics_handle: None,
        ui_dir,
        api_base_url: format!("http://127.0.0.1:{port}"),
    });
    let router = tine_server::build_router(state);

    info!(port = port, "serving tine API on 127.0.0.1:{}", port);

    tokio::spawn(async move {
        axum::serve(listener, router).await.ok();
    });

    Ok((workspace, port))
}

fn find_workspace_root_from(start: &Path) -> Option<PathBuf> {
    let mut dir = start.to_path_buf();
    loop {
        if dir.join(".tine").is_dir()
            || (dir.join("ui").join("index.html").is_file() && dir.join("Cargo.toml").is_file())
        {
            return Some(dir.clone());
        }
        if !dir.pop() {
            return None;
        }
    }
}

/// Walk up from cwd or the executable path looking for the workspace root.
fn find_workspace_root() -> Option<PathBuf> {
    let exe_root = std::env::current_exe()
        .ok()
        .and_then(|path| path.parent().map(|dir| dir.to_path_buf()))
        .and_then(|dir| find_workspace_root_from(&dir));

    if let Some(root) = exe_root
        .as_ref()
        .filter(|root| root.join("ui").join("index.html").is_file())
    {
        return Some(root.clone());
    }

    if let Ok(current_dir) = std::env::current_dir() {
        if let Some(root) = find_workspace_root_from(&current_dir) {
            return Some(root);
        }
    }

    exe_root
}

fn file_path_to_string(path: FilePath) -> String {
    match path {
        FilePath::Path(path) => path.display().to_string(),
        FilePath::Url(url) => url.to_string(),
    }
}

fn file_path_to_path_buf(path: FilePath) -> Result<PathBuf, String> {
    match path {
        FilePath::Path(path) => Ok(path),
        FilePath::Url(url) => url
            .to_file_path()
            .map_err(|_| format!("unsupported save path: {}", url)),
    }
}

// ---------------------------------------------------------------------------
// Local filesystem artifact store (shared with tine-cli)
// ---------------------------------------------------------------------------

struct LocalArtifactStore {
    root: PathBuf,
}

impl LocalArtifactStore {
    fn new(root: PathBuf) -> Self {
        Self { root }
    }

    fn path_for(&self, key: &ArtifactKey) -> PathBuf {
        self.root.join(key.as_str())
    }
}

#[async_trait]
impl ArtifactStore for LocalArtifactStore {
    async fn put(&self, key: &ArtifactKey, data: &[u8]) -> TineResult<[u8; 32]> {
        let path = self.path_for(key);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(&path, data).await?;
        Ok(*blake3::hash(data).as_bytes())
    }

    async fn get(&self, key: &ArtifactKey) -> TineResult<Vec<u8>> {
        let path = self.path_for(key);
        Ok(tokio::fs::read(&path).await?)
    }

    async fn delete(&self, key: &ArtifactKey) -> TineResult<()> {
        let path = self.path_for(key);
        if path.exists() {
            tokio::fs::remove_file(&path).await?;
        }
        Ok(())
    }

    async fn exists(&self, key: &ArtifactKey) -> TineResult<bool> {
        Ok(self.path_for(key).exists())
    }

    async fn metadata(&self, key: &ArtifactKey) -> TineResult<ArtifactMetadata> {
        let path = self.path_for(key);
        let data = tokio::fs::read(&path).await?;
        let hash = *blake3::hash(&data).as_bytes();
        let meta = tokio::fs::metadata(&path).await?;
        let created = meta
            .created()
            .ok()
            .and_then(|t| chrono::DateTime::<Utc>::from(t).into())
            .unwrap_or_else(Utc::now);
        Ok(ArtifactMetadata {
            key: key.clone(),
            size_bytes: meta.len(),
            schema: None,
            created_at: created,
            content_hash: hash,
        })
    }

    async fn list(&self) -> TineResult<Vec<ArtifactKey>> {
        let mut keys = Vec::new();
        let mut entries = tokio::fs::read_dir(&self.root).await?;
        while let Some(entry) = entries.next_entry().await? {
            if entry.file_type().await?.is_file() {
                if let Some(name) = entry.file_name().to_str() {
                    keys.push(ArtifactKey::new(name));
                }
            }
        }
        Ok(keys)
    }
}
