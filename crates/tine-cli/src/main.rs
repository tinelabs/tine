use std::collections::HashMap;
use std::io::Write;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;

use async_trait::async_trait;
use clap::{Args, Parser, Subcommand};
use figment::{
    providers::{Env, Format, Serialized, Toml},
    Figment,
};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};
use tine_env::{EnvironmentManager, DEFAULT_PYTHON_VERSION};

use tine_api::{export_branch_as_ipynb, export_branch_as_python, Workspace};
use tine_core::{
    ArtifactKey, ArtifactMetadata, ArtifactStore, BranchId, CellDef, CellId, CellRuntimeState,
    ExecutionId, ExperimentTreeId, NodeCode, ProjectDef, ProjectId, SlotName, TineResult,
    WorkspaceApi,
};
use tine_observe::{init_logging, init_metrics};

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TineConfig {
    pub workspace_dir: PathBuf,
    pub bind: String,
    pub log_json: bool,
}

impl Default for TineConfig {
    fn default() -> Self {
        Self {
            workspace_dir: PathBuf::from("."),
            bind: "127.0.0.1:9473".to_string(),
            log_json: false,
        }
    }
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

#[derive(Parser)]
#[command(
    name = "tine",
    version,
    about = "Local operator shell for the Tine runtime"
)]
pub struct Cli {
    /// Path to workspace (overrides config)
    #[arg(long, global = true)]
    pub workspace: Option<PathBuf>,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Start the local Tine server and web UI
    #[command(alias = "daemon")]
    Serve {
        #[arg(long, default_value = "127.0.0.1:9473")]
        bind: String,
        #[arg(long, help = "Open the local web UI in your browser after startup")]
        open: bool,
    },

    /// Run local diagnostics for the Tine install and workspace
    Doctor,

    /// Print the Tine CLI version
    Version,

    #[command(hide = true)]
    Internal {
        #[command(subcommand)]
        command: InternalCommands,
    },
}

#[derive(Subcommand)]
pub enum InternalCommands {
    /// Initialize a new tine workspace in the current directory
    Init,

    /// Experiment tree operations
    Experiments {
        #[command(subcommand)]
        command: ExperimentCommands,
    },

    /// Branch operations
    Branches {
        #[command(subcommand)]
        command: BranchCommands,
    },

    /// Cell operations
    Cells {
        #[command(subcommand)]
        command: CellCommands,
    },

    /// Execution operations
    Executions {
        #[command(subcommand)]
        command: ExecutionCommands,
    },

    /// File operations
    Files {
        #[command(subcommand)]
        command: FileCommands,
    },

    /// Project operations
    Projects {
        #[command(subcommand)]
        command: Option<ProjectCommands>,
    },

    /// Show resolved configuration
    Config,
}

#[derive(Subcommand)]
pub enum ExperimentCommands {
    /// List experiment trees
    List,
    /// Get an experiment tree
    Get { tree_id: String },
    /// Create an experiment tree
    Create {
        name: String,
        #[arg(long)]
        project: Option<String>,
    },
    /// Delete an experiment tree
    Delete { tree_id: String },
    /// Rename an experiment tree
    Rename {
        tree_id: String,
        #[arg(long)]
        name: String,
    },
    /// Get persisted tree runtime state
    RuntimeState { tree_id: String },
}

#[derive(Subcommand)]
pub enum BranchCommands {
    /// Create a branch with its first cell
    Create {
        tree_id: String,
        #[arg(long)]
        parent_branch_id: String,
        #[arg(long = "branch-name")]
        branch_name: String,
        #[arg(long)]
        branch_point_cell_id: String,
        #[command(flatten)]
        cell: CellSpecArgs,
    },
    /// Delete a branch
    Delete { tree_id: String, branch_id: String },
    /// Execute one branch
    Execute { tree_id: String, branch_id: String },
    /// Execute all branches in a tree
    ExecuteAll { tree_id: String },
    /// Export a branch as Python
    ExportPy {
        tree_id: String,
        branch_id: String,
        #[arg(long)]
        output: Option<PathBuf>,
    },
    /// Export a branch as Jupyter notebook JSON
    ExportIpynb {
        tree_id: String,
        branch_id: String,
        #[arg(long)]
        output: Option<PathBuf>,
    },
}

#[derive(Subcommand)]
pub enum CellCommands {
    /// Add a cell to a branch
    Add {
        tree_id: String,
        branch_id: String,
        #[arg(long)]
        after_cell_id: Option<String>,
        #[command(flatten)]
        cell: CellSpecArgs,
    },
    /// Update cell source code
    UpdateCode {
        tree_id: String,
        branch_id: String,
        cell_id: String,
        #[arg(long)]
        source: Option<String>,
        #[arg(long)]
        source_file: Option<PathBuf>,
    },
    /// Move a cell up or down within a branch
    Move {
        tree_id: String,
        branch_id: String,
        cell_id: String,
        #[arg(long)]
        direction: String,
    },
    /// Delete a cell from a branch
    Delete {
        tree_id: String,
        branch_id: String,
        cell_id: String,
    },
    /// Execute a cell in branch context
    Execute {
        tree_id: String,
        branch_id: String,
        cell_id: String,
    },
    /// Fetch logs for a cell
    Logs {
        tree_id: String,
        branch_id: String,
        cell_id: String,
    },
}

#[derive(Subcommand)]
pub enum ExecutionCommands {
    /// Get execution status
    Status { execution_id: String },
    /// Cancel an execution
    Cancel { execution_id: String },
}

#[derive(Subcommand)]
pub enum ProjectCommands {
    /// List projects
    List,
    /// Create a project
    Create {
        name: String,
        #[arg(long, default_value = ".")]
        workspace_dir: String,
        #[arg(long)]
        description: Option<String>,
    },
    /// Get a project
    Get { project_id: String },
    /// List experiments in a project
    Experiments { project_id: String },
}

#[derive(Subcommand)]
pub enum FileCommands {
    /// List files in the workspace or project
    List {
        #[arg(long)]
        path: Option<String>,
        #[arg(long)]
        project: Option<String>,
    },
    /// Read a file
    Read {
        path: String,
        #[arg(long)]
        project: Option<String>,
        #[arg(long)]
        output: Option<PathBuf>,
    },
    /// Write a file
    Write {
        path: String,
        #[arg(long)]
        content: Option<String>,
        #[arg(long)]
        content_file: Option<PathBuf>,
        #[arg(long)]
        project: Option<String>,
    },
}

#[derive(Args, Debug, Clone)]
pub struct CellSpecArgs {
    #[arg(long)]
    cell_file: Option<PathBuf>,
    #[arg(long)]
    id: Option<String>,
    #[arg(long)]
    name: Option<String>,
    #[arg(long)]
    source: Option<String>,
    #[arg(long)]
    source_file: Option<PathBuf>,
    #[arg(long, default_value = "python")]
    language: String,
    #[arg(long = "upstream")]
    upstream: Vec<String>,
    #[arg(long = "output")]
    outputs: Vec<String>,
    #[arg(long, default_value_t = false)]
    no_cache: bool,
    #[arg(long)]
    timeout_secs: Option<u64>,
}

// ---------------------------------------------------------------------------
// Entrypoint
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    // Build config: defaults → .tine/config.toml → TINE_* env vars → CLI args
    let mut figment = Figment::from(Serialized::defaults(TineConfig::default()))
        .merge(Toml::file(".tine/config.toml"))
        .merge(Env::prefixed("TINE_"));

    if let Some(ref ws) = cli.workspace {
        figment = figment.merge(Serialized::default("workspace_dir", ws));
    }

    let config: TineConfig = figment.extract()?;

    init_logging();

    match cli.command {
        Commands::Serve { bind, open } => {
            let metrics_handle = init_metrics();
            ensure_workspace_bootstrap(&config.workspace_dir)?;
            let ws = open_workspace(&config).await?;
            let listener = tokio::net::TcpListener::bind(&bind).await?;
            let local_addr = listener.local_addr()?;
            let local_url = local_server_url(local_addr);

            info!(
                bind = %local_addr,
                workspace = %config.workspace_dir.display(),
                url = %local_url,
                "Starting tine local server"
            );
            if !config.log_json {
                eprintln!("Tine local server ready");
                eprintln!("  URL: {local_url}");
                eprintln!("  Workspace: {}", config.workspace_dir.display());
                eprintln!("  MCP stdio: tine mcp serve");
                eprintln!("  Register MCP: tine mcp register --host vscode");
            }
            if open {
                if let Err(error) = open_browser(&local_url) {
                    warn!(url = %local_url, %error, "Failed to open browser");
                    if !config.log_json {
                        eprintln!("  Browser open failed: {error}");
                    }
                }
            }

            tine_server::serve_listener_with_metrics(ws, listener, metrics_handle).await?;
        }

        Commands::Doctor => {
            run_doctor(&config).await?;
        }

        Commands::Version => {
            println!("tine {}", env!("CARGO_PKG_VERSION"));
        }

        Commands::Internal { command } => match command {
            InternalCommands::Init => {
                ensure_workspace_bootstrap(std::path::Path::new("."))?;
                println!("Initialized tine workspace in .tine/");
            }

            InternalCommands::Experiments { command } => {
                let ws = open_workspace(&config).await?;
                handle_experiment_command(&ws, command).await?;
            }

            InternalCommands::Branches { command } => {
                let ws = open_workspace(&config).await?;
                handle_branch_command(&ws, command).await?;
            }

            InternalCommands::Cells { command } => {
                let ws = open_workspace(&config).await?;
                handle_cell_command(&ws, command).await?;
            }

            InternalCommands::Executions { command } => {
                let ws = open_workspace(&config).await?;
                handle_execution_command(&ws, command).await?;
            }

            InternalCommands::Files { command } => {
                let ws = open_workspace(&config).await?;
                handle_file_command(&ws, command).await?;
            }

            InternalCommands::Projects { command } => {
                let ws = open_workspace(&config).await?;
                handle_project_command(&ws, command.unwrap_or(ProjectCommands::List)).await?;
            }

            InternalCommands::Config => {
                println!("{}", serde_json::to_string_pretty(&config)?);
            }
        },
    }

    Ok(())
}

async fn handle_experiment_command(
    ws: &Workspace,
    command: ExperimentCommands,
) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        ExperimentCommands::List => {
            let trees = ws.list_experiment_trees().await?;
            print_json_pretty(&trees)?;
        }
        ExperimentCommands::Get { tree_id } => {
            let tree = ws
                .get_experiment_tree(&ExperimentTreeId::new(tree_id))
                .await?;
            print_json_pretty(&tree)?;
        }
        ExperimentCommands::Create { name, project } => {
            let project_id = project.as_deref().map(ProjectId::new);
            let tree = ws
                .create_experiment_tree(&name, project_id.as_ref())
                .await?;
            print_json_pretty(&tree)?;
        }
        ExperimentCommands::Delete { tree_id } => {
            ws.delete_experiment_tree(&ExperimentTreeId::new(&tree_id))
                .await?;
            println!("Deleted {}", tree_id);
        }
        ExperimentCommands::Rename { tree_id, name } => {
            ws.rename_experiment_tree(&ExperimentTreeId::new(&tree_id), &name)
                .await?;
            println!("Renamed {} to {}", tree_id, name);
        }
        ExperimentCommands::RuntimeState { tree_id } => {
            let runtime_state = ws
                .get_tree_runtime_state(&ExperimentTreeId::new(tree_id))
                .await;
            print_json_pretty(&runtime_state)?;
        }
    }
    Ok(())
}

async fn handle_branch_command(
    ws: &Workspace,
    command: BranchCommands,
) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        BranchCommands::Create {
            tree_id,
            parent_branch_id,
            branch_name,
            branch_point_cell_id,
            cell,
        } => {
            let tree_id = ExperimentTreeId::new(tree_id);
            let first_cell =
                load_or_build_cell_def(&cell, tree_id.clone(), BranchId::new("pending-branch"))?;
            let branch_id = ws
                .create_branch_in_experiment_tree(
                    &tree_id,
                    &BranchId::new(parent_branch_id),
                    branch_name,
                    &CellId::new(branch_point_cell_id),
                    first_cell,
                )
                .await?;
            print_json_pretty(&serde_json::json!({ "branch_id": branch_id.as_str() }))?;
        }
        BranchCommands::Delete { tree_id, branch_id } => {
            ws.delete_experiment_tree_branch(
                &ExperimentTreeId::new(tree_id),
                &BranchId::new(branch_id.clone()),
            )
            .await?;
            println!("Deleted branch {}", branch_id);
        }
        BranchCommands::Execute { tree_id, branch_id } => {
            let execution_id = ws
                .execute_branch_in_experiment_tree(
                    &ExperimentTreeId::new(tree_id),
                    &BranchId::new(branch_id.clone()),
                )
                .await?;
            print_json_pretty(&serde_json::json!({
                "branch_id": branch_id,
                "execution_id": execution_id.as_str(),
            }))?;
        }
        BranchCommands::ExecuteAll { tree_id } => {
            let executions = ws
                .execute_all_branches_in_experiment_tree(&ExperimentTreeId::new(tree_id))
                .await?;
            let payload = executions
                .into_iter()
                .map(|(branch_id, execution_id)| {
                    serde_json::json!({
                        "branch_id": branch_id.as_str(),
                        "execution_id": execution_id.as_str(),
                    })
                })
                .collect::<Vec<_>>();
            print_json_pretty(&serde_json::json!({ "executions": payload }))?;
        }
        BranchCommands::ExportPy {
            tree_id,
            branch_id,
            output,
        } => {
            let tree = ws
                .get_experiment_tree(&ExperimentTreeId::new(tree_id))
                .await?;
            let exported = export_branch_as_python(&tree, &BranchId::new(branch_id))?;
            write_or_print_text(output.as_ref(), &exported)?;
        }
        BranchCommands::ExportIpynb {
            tree_id,
            branch_id,
            output,
        } => {
            let tree = ws
                .get_experiment_tree(&ExperimentTreeId::new(tree_id))
                .await?;
            let exported = export_branch_as_ipynb(&tree, &BranchId::new(branch_id))?;
            let text = serde_json::to_string_pretty(&exported)?;
            write_or_print_text(output.as_ref(), &text)?;
        }
    }
    Ok(())
}

async fn handle_cell_command(
    ws: &Workspace,
    command: CellCommands,
) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        CellCommands::Add {
            tree_id,
            branch_id,
            after_cell_id,
            cell,
        } => {
            let tree_id = ExperimentTreeId::new(tree_id);
            let branch_id = BranchId::new(branch_id);
            let cell = load_or_build_cell_def(&cell, tree_id.clone(), branch_id.clone())?;
            let after = after_cell_id.as_deref().map(CellId::new);
            ws.add_cell_to_experiment_tree_branch(&tree_id, &branch_id, cell, after.as_ref())
                .await?;
            println!("Added cell to branch {}", branch_id.as_str());
        }
        CellCommands::UpdateCode {
            tree_id,
            branch_id,
            cell_id,
            source,
            source_file,
        } => {
            let source = load_text_input(source, source_file)?;
            ws.update_cell_code_in_experiment_tree_branch(
                &ExperimentTreeId::new(tree_id),
                &BranchId::new(branch_id),
                &CellId::new(cell_id.clone()),
                &source,
            )
            .await?;
            println!("Updated code for cell {}", cell_id);
        }
        CellCommands::Move {
            tree_id,
            branch_id,
            cell_id,
            direction,
        } => {
            ws.move_cell_in_experiment_tree_branch(
                &ExperimentTreeId::new(tree_id),
                &BranchId::new(branch_id),
                &CellId::new(cell_id.clone()),
                &direction,
            )
            .await?;
            println!("Moved cell {} {}", cell_id, direction);
        }
        CellCommands::Delete {
            tree_id,
            branch_id,
            cell_id,
        } => {
            ws.delete_cell_from_experiment_tree_branch(
                &ExperimentTreeId::new(tree_id),
                &BranchId::new(branch_id),
                &CellId::new(cell_id.clone()),
            )
            .await?;
            println!("Deleted cell {}", cell_id);
        }
        CellCommands::Execute {
            tree_id,
            branch_id,
            cell_id,
        } => {
            let (execution_id, logs) = ws
                .execute_cell_in_experiment_tree_branch(
                    &ExperimentTreeId::new(tree_id),
                    &BranchId::new(branch_id),
                    &CellId::new(cell_id.clone()),
                )
                .await?;
            print_json_pretty(&serde_json::json!({
                "execution_id": execution_id.as_str(),
                "cell_id": cell_id,
                "logs": logs,
            }))?;
        }
        CellCommands::Logs {
            tree_id,
            branch_id,
            cell_id,
        } => {
            let logs = ws
                .logs_for_tree_cell(
                    &ExperimentTreeId::new(tree_id),
                    &BranchId::new(branch_id),
                    &CellId::new(cell_id),
                )
                .await?;
            print_json_pretty(&logs)?;
        }
    }
    Ok(())
}

async fn handle_execution_command(
    ws: &Workspace,
    command: ExecutionCommands,
) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        ExecutionCommands::Status { execution_id } => {
            let status = ws.status(&ExecutionId::new(execution_id)).await?;
            print_json_pretty(&status)?;
        }
        ExecutionCommands::Cancel { execution_id } => {
            ws.cancel(&ExecutionId::new(&execution_id)).await?;
            println!("Canceled {}", execution_id);
        }
    }
    Ok(())
}

async fn handle_project_command(
    ws: &Workspace,
    command: ProjectCommands,
) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        ProjectCommands::List => {
            let projects = ws.list_projects().await?;
            print_json_pretty(&projects)?;
        }
        ProjectCommands::Create {
            name,
            workspace_dir,
            description,
        } => {
            let project = ProjectDef {
                id: ProjectId::generate(),
                name,
                description,
                workspace_dir,
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            };
            let id = ws.create_project(project).await?;
            print_json_pretty(&serde_json::json!({ "id": id.as_str() }))?;
        }
        ProjectCommands::Get { project_id } => {
            let project = ws.get_project(&ProjectId::new(project_id)).await?;
            print_json_pretty(&project)?;
        }
        ProjectCommands::Experiments { project_id } => {
            let experiments = ws.list_experiments(&ProjectId::new(project_id)).await?;
            print_json_pretty(&experiments)?;
        }
    }
    Ok(())
}

async fn handle_file_command(
    ws: &Workspace,
    command: FileCommands,
) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        FileCommands::List { path, project } => {
            let project_id = project.as_deref().map(ProjectId::new);
            let entries = ws
                .list_project_files(project_id.as_ref(), path.as_deref().unwrap_or_default())
                .await?;
            print_json_pretty(&entries)?;
        }
        FileCommands::Read {
            path,
            project,
            output,
        } => {
            let project_id = project.as_deref().map(ProjectId::new);
            let bytes = ws
                .read_project_file_bytes(project_id.as_ref(), &path)
                .await?;
            if let Some(output) = output {
                std::fs::write(output, &bytes)?;
            } else if let Ok(text) = String::from_utf8(bytes.clone()) {
                print!("{text}");
            } else {
                std::io::stdout().write_all(&bytes)?;
            }
        }
        FileCommands::Write {
            path,
            content,
            content_file,
            project,
        } => {
            let project_id = project.as_deref().map(ProjectId::new);
            let content = load_text_input(content, content_file)?;
            ws.write_project_file(project_id.as_ref(), &path, &content)
                .await?;
            println!("Wrote {}", path);
        }
    }
    Ok(())
}

fn load_or_build_cell_def(
    args: &CellSpecArgs,
    tree_id: ExperimentTreeId,
    branch_id: BranchId,
) -> Result<CellDef, Box<dyn std::error::Error>> {
    if let Some(path) = &args.cell_file {
        let text = std::fs::read_to_string(path)?;
        let cell = serde_json::from_str::<CellDef>(&text)?;
        return Ok(cell);
    }

    let id = args
        .id
        .clone()
        .ok_or("missing --id when --cell-file is not provided")?;
    let source = load_text_input(args.source.clone(), args.source_file.clone())?;
    let name = args.name.clone().unwrap_or_else(|| id.clone());

    Ok(CellDef {
        id: CellId::new(id),
        tree_id,
        branch_id,
        name,
        code: NodeCode {
            source,
            language: args.language.clone(),
        },
        upstream_cell_ids: args.upstream.iter().cloned().map(CellId::new).collect(),
        declared_outputs: args.outputs.iter().cloned().map(SlotName::new).collect(),
        cache: !args.no_cache,
        map_over: None,
        map_concurrency: None,
        timeout_secs: args.timeout_secs,
        tags: HashMap::new(),
        revision_id: None,
        state: CellRuntimeState::Clean,
    })
}

fn load_text_input(
    inline: Option<String>,
    file: Option<PathBuf>,
) -> Result<String, Box<dyn std::error::Error>> {
    match (inline, file) {
        (Some(text), None) => Ok(text),
        (None, Some(path)) => Ok(std::fs::read_to_string(path)?),
        (Some(_), Some(_)) => Err("provide either inline content or a file, not both".into()),
        (None, None) => Err("missing content input".into()),
    }
}

fn write_or_print_text(
    output: Option<&PathBuf>,
    text: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(path) = output {
        std::fs::write(path, text)?;
    } else {
        print!("{text}");
    }
    Ok(())
}

fn print_json_pretty<T: Serialize>(value: &T) -> Result<(), Box<dyn std::error::Error>> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
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


fn open_browser(url: &str) -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(target_os = "macos")]
    let mut command = {
        let mut command = Command::new("open");
        command.arg(url);
        command
    };

    #[cfg(target_os = "windows")]
    let mut command = {
        let mut command = Command::new("cmd");
        command.args(["/C", "start", "", url]);
        command
    };

    #[cfg(all(unix, not(target_os = "macos")))]
    let mut command = {
        let mut command = Command::new("xdg-open");
        command.arg(url);
        command
    };

    let status = command.status()?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("browser command exited with status {status}").into())
    }
}

async fn run_doctor(config: &TineConfig) -> Result<(), Box<dyn std::error::Error>> {
    ensure_workspace_bootstrap(&config.workspace_dir)?;
    let mut failed = false;

    print_doctor_check(
        "workspace directory",
        config.workspace_dir.is_dir(),
        config.workspace_dir.display().to_string(),
    );
    failed |= !config.workspace_dir.is_dir();

    let tine_dir = config.workspace_dir.join(".tine");
    print_doctor_check(
        "workspace state dir",
        tine_dir.is_dir(),
        tine_dir.display().to_string(),
    );

    let config_path = config.workspace_dir.join(".tine/config.toml");
    print_doctor_check(
        "workspace config",
        config_path.is_file(),
        config_path.display().to_string(),
    );

    let bind_addr = config.bind.parse::<SocketAddr>().is_ok();
    print_doctor_check("bind address", bind_addr, config.bind.clone());
    failed |= !bind_addr;

    let ui_dir = tine_server::resolve_ui_dir(&config.workspace_dir);
    match ui_dir {
        Ok(path) => print_doctor_check("web UI assets", true, path.display().to_string()),
        Err(error) => {
            print_doctor_check("web UI assets", false, error.to_string());
            failed = true;
        }
    }

    let env_manager = EnvironmentManager::new(config.workspace_dir.clone());
    let uv_ok = match env_manager.ensure_uv().await {
        Ok(()) => {
            print_doctor_check("uv", true, "available".to_string());
            true
        }
        Err(error) => {
            print_doctor_check("uv", false, error.to_string());
            failed = true;
            false
        }
    };

    if uv_ok {
        match env_manager
            .ensure_python_version_available(DEFAULT_PYTHON_VERSION)
            .await
        {
            Ok(path) => print_doctor_check(
                &format!("python {} via uv", DEFAULT_PYTHON_VERSION),
                true,
                path,
            ),
            Err(error) => {
                print_doctor_check(
                    &format!("python {} via uv", DEFAULT_PYTHON_VERSION),
                    false,
                    error.to_string(),
                );
                failed = true;
            }
        }

        if !failed {
            match env_manager.doctor_runtime_check().await {
                Ok(_) => print_doctor_check(
                    "runtime preflight",
                    true,
                    "temporary kernel environment created successfully".to_string(),
                ),
                Err(error) => {
                    print_doctor_check("runtime preflight", false, error.to_string());
                    failed = true;
                }
            }
        }
    } else {
        print_doctor_check(
            &format!("python {} via uv", DEFAULT_PYTHON_VERSION),
            false,
            "uv unavailable".to_string(),
        );
        print_doctor_check(
            "runtime preflight",
            false,
            "uv unavailable".to_string(),
        );
    }

    if failed {
        return Err("doctor found blocking issues".into());
    }

    println!("doctor: ok");
    Ok(())
}

fn print_doctor_check(name: &str, ok: bool, detail: String) {
    let status = if ok { "PASS" } else { "FAIL" };
    println!("{status} {name}: {detail}");
}

async fn open_workspace(config: &TineConfig) -> Result<Workspace, Box<dyn std::error::Error>> {
    ensure_workspace_bootstrap(&config.workspace_dir)?;
    let store = Arc::new(LocalArtifactStore::new(
        config.workspace_dir.join(".tine/artifacts"),
    ));
    let ws = Workspace::open(config.workspace_dir.clone(), store, 8).await?;
    Ok(ws)
}

fn default_workspace_config() -> &'static str {
    r#"# tine workspace configuration
workspace_dir = "."
bind = "127.0.0.1:9473"
log_json = false
"#
}

fn ensure_workspace_bootstrap(workspace_dir: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    std::fs::create_dir_all(workspace_dir)?;

    let tine_dir = workspace_dir.join(".tine");
    std::fs::create_dir_all(&tine_dir)?;

    let config_path = tine_dir.join("config.toml");
    if !config_path.is_file() {
        std::fs::write(config_path, default_workspace_config())?;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Local filesystem artifact store
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
        let meta = tokio::fs::metadata(&path).await?;
        Ok(ArtifactMetadata {
            key: key.clone(),
            size_bytes: meta.len(),
            schema: None,
            content_hash: [0u8; 32],
            created_at: chrono::Utc::now(),
        })
    }

    async fn list(&self) -> TineResult<Vec<ArtifactKey>> {
        let mut keys = Vec::new();
        if self.root.exists() {
            let mut entries = tokio::fs::read_dir(&self.root).await?;
            while let Some(entry) = entries.next_entry().await? {
                if let Some(name) = entry.file_name().to_str() {
                    keys.push(ArtifactKey::new(name));
                }
            }
        }
        Ok(keys)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn top_level_help_shows_narrow_public_surface() {
        let help = Cli::command().render_help().to_string();

        assert!(help.contains("\n  serve    "));
        assert!(help.contains("\n  doctor   "));
        assert!(help.contains("\n  version  "));

        assert!(!help.contains("\n  experiments  "));
        assert!(!help.contains("\n  branches     "));
        assert!(!help.contains("\n  cells        "));
        assert!(!help.contains("\n  executions   "));
        assert!(!help.contains("\n  files        "));
        assert!(!help.contains("\n  projects     "));
        assert!(!help.contains("\n  mcp         "));
        assert!(!help.contains("\n  internal     "));
    }

    #[test]
    fn legacy_daemon_alias_still_parses() {
        let cli = Cli::try_parse_from(["tine", "daemon"]).expect("daemon alias should parse");

        match cli.command {
            Commands::Serve { .. } => {}
            _ => panic!("expected serve command"),
        }
    }

    #[test]
    fn ensure_workspace_bootstrap_creates_state_dir_and_config() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        let workspace_dir = std::env::temp_dir().join(format!("tine-bootstrap-test-{unique}"));

        if workspace_dir.exists() {
            std::fs::remove_dir_all(&workspace_dir).expect("clear existing temp workspace");
        }

        ensure_workspace_bootstrap(&workspace_dir).expect("bootstrap workspace");

        assert!(workspace_dir.join(".tine").is_dir());
        assert!(workspace_dir.join(".tine/config.toml").is_file());

        std::fs::remove_dir_all(&workspace_dir).expect("cleanup temp workspace");
    }

}
