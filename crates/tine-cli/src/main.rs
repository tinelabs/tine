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
use tine_env::{EnvironmentManager, DEFAULT_PYTHON_VERSION};
use tracing::{info, warn};

mod client;

use client::TineClient;
use tine_api::Workspace;
use tine_core::{
    ArtifactKey, ArtifactMetadata, ArtifactStore, BranchId, CellDef, CellId, CellRuntimeState,
    ExperimentTreeId, NodeCode, SlotName, TineResult,
};
use tine_observe::{init_logging, init_metrics};

const TINE_VERSION: &str = include_str!("../../../VERSION");

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
    version = TINE_VERSION,
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
    Execute {
        tree_id: String,
        branch_id: String,
        /// Return immediately after submission instead of waiting for the
        /// execution to finish (the server keeps running it).
        #[arg(long)]
        no_wait: bool,
        /// Idempotency key for the submission (auto-generated when omitted
        /// and echoed in the output). Resubmitting with the same key
        /// reattaches to the original run instead of starting a duplicate.
        #[arg(long)]
        idempotency_key: Option<String>,
    },
    /// Execute all branches in a tree
    ExecuteAll {
        tree_id: String,
        /// Return immediately after submission instead of waiting for the
        /// executions to finish (the server keeps running them).
        #[arg(long)]
        no_wait: bool,
    },
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
        /// Return immediately after submission instead of waiting for the
        /// execution to finish (the server keeps running it).
        #[arg(long)]
        no_wait: bool,
        /// Idempotency key for the submission (auto-generated when omitted
        /// and echoed in the output). Resubmitting with the same key
        /// reattaches to the original run instead of starting a duplicate.
        #[arg(long)]
        idempotency_key: Option<String>,
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
            println!("tine {}", TINE_VERSION);
        }

        Commands::Internal { command } => match command {
            InternalCommands::Init => {
                ensure_workspace_bootstrap(std::path::Path::new("."))?;
                println!("Initialized tine workspace in .tine/");
            }

            // Data-plane commands are thin HTTP front doors over the running
            // server (see SURFACE_CONSOLIDATION_PLAN.md). Opening the
            // workspace in-process here would race a running server: two
            // queues, two kernel managers, and a startup reconciliation that
            // marks the server's live executions as failed.
            InternalCommands::Experiments { command } => {
                let client = TineClient::from_bind(&config.bind)?;
                handle_experiment_command(&client, command).await?;
            }

            InternalCommands::Branches { command } => {
                let client = TineClient::from_bind(&config.bind)?;
                handle_branch_command(&client, command).await?;
            }

            InternalCommands::Cells { command } => {
                let client = TineClient::from_bind(&config.bind)?;
                handle_cell_command(&client, command).await?;
            }

            InternalCommands::Executions { command } => {
                let client = TineClient::from_bind(&config.bind)?;
                handle_execution_command(&client, command).await?;
            }

            InternalCommands::Files { command } => {
                let client = TineClient::from_bind(&config.bind)?;
                handle_file_command(&client, command).await?;
            }

            InternalCommands::Projects { command } => {
                let client = TineClient::from_bind(&config.bind)?;
                handle_project_command(&client, command.unwrap_or(ProjectCommands::List)).await?;
            }

            InternalCommands::Config => {
                println!("{}", serde_json::to_string_pretty(&config)?);
            }
        },
    }

    Ok(())
}

async fn handle_experiment_command(
    client: &TineClient,
    command: ExperimentCommands,
) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        ExperimentCommands::List => {
            print_json_pretty(&client.list_experiment_trees().await?)?;
        }
        ExperimentCommands::Get { tree_id } => {
            print_json_pretty(&client.get_experiment_tree(&tree_id).await?)?;
        }
        ExperimentCommands::Create { name, project } => {
            let tree = client
                .create_experiment_tree(&name, project.as_deref())
                .await?;
            print_json_pretty(&tree)?;
        }
        ExperimentCommands::Delete { tree_id } => {
            client.delete_experiment_tree(&tree_id).await?;
            println!("Deleted {}", tree_id);
        }
        ExperimentCommands::Rename { tree_id, name } => {
            client.rename_experiment_tree(&tree_id, &name).await?;
            println!("Renamed {} to {}", tree_id, name);
        }
        ExperimentCommands::RuntimeState { tree_id } => {
            print_json_pretty(&client.tree_runtime_state(&tree_id).await?)?;
        }
    }
    Ok(())
}

async fn handle_branch_command(
    client: &TineClient,
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
            let first_cell = load_or_build_cell_def(
                &cell,
                ExperimentTreeId::new(&tree_id),
                BranchId::new("pending-branch"),
            )?;
            let branch_id = client
                .create_branch(
                    &tree_id,
                    &parent_branch_id,
                    &branch_name,
                    &branch_point_cell_id,
                    serde_json::to_value(&first_cell)?,
                )
                .await?;
            print_json_pretty(&serde_json::json!({ "branch_id": branch_id }))?;
        }
        BranchCommands::Delete { tree_id, branch_id } => {
            client.delete_branch(&tree_id, &branch_id).await?;
            println!("Deleted branch {}", branch_id);
        }
        BranchCommands::Execute {
            tree_id,
            branch_id,
            no_wait,
            idempotency_key,
        } => {
            let idempotency_key = idempotency_key.unwrap_or_else(generate_idempotency_key);
            let accepted = client
                .execute_branch(&tree_id, &branch_id, &idempotency_key)
                .await?;
            let execution_id = required_str(&accepted, "execution_id")?;
            if no_wait {
                print_json_pretty(&serde_json::json!({
                    "branch_id": branch_id,
                    "execution_id": execution_id,
                    "idempotency_key": idempotency_key,
                }))?;
            } else {
                let status = client.wait_for_terminal(&execution_id).await?;
                print_json_pretty(&serde_json::json!({
                    "branch_id": branch_id,
                    "execution_id": execution_id,
                    "idempotency_key": idempotency_key,
                    "status": status,
                }))?;
            }
        }
        BranchCommands::ExecuteAll { tree_id, no_wait } => {
            let response = client.execute_all_branches(&tree_id).await?;
            let accepted = response
                .get("executions")
                .and_then(serde_json::Value::as_array)
                .cloned()
                .unwrap_or_default();
            let mut payload = Vec::with_capacity(accepted.len());
            for execution in accepted {
                let execution_id = required_str(&execution, "execution_id")?;
                let branch_id = execution
                    .pointer("/target/branch_id")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                if no_wait {
                    payload.push(serde_json::json!({
                        "branch_id": branch_id,
                        "execution_id": execution_id,
                    }));
                } else {
                    let status = client.wait_for_terminal(&execution_id).await?;
                    payload.push(serde_json::json!({
                        "branch_id": branch_id,
                        "execution_id": execution_id,
                        "status": status,
                    }));
                }
            }
            print_json_pretty(&serde_json::json!({ "executions": payload }))?;
        }
        BranchCommands::ExportPy {
            tree_id,
            branch_id,
            output,
        } => {
            let exported = client.export_branch(&tree_id, &branch_id, "py").await?;
            write_or_print_text(output.as_ref(), &exported)?;
        }
        BranchCommands::ExportIpynb {
            tree_id,
            branch_id,
            output,
        } => {
            let exported = client.export_branch(&tree_id, &branch_id, "ipynb").await?;
            write_or_print_text(output.as_ref(), &exported)?;
        }
    }
    Ok(())
}

/// Execute submissions are idempotent by default: when the user omits
/// `--idempotency-key`, the CLI generates one and echoes it in the output
/// (and in timeout errors) so a timed-out submission can be retried safely.
fn generate_idempotency_key() -> String {
    format!("cli-{}", tine_core::ExecutionId::generate().as_str())
}

fn required_str(
    value: &serde_json::Value,
    key: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    value
        .get(key)
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| format!("server response missing '{key}': {value}").into())
}

async fn handle_cell_command(
    client: &TineClient,
    command: CellCommands,
) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        CellCommands::Add {
            tree_id,
            branch_id,
            after_cell_id,
            cell,
        } => {
            let cell = load_or_build_cell_def(
                &cell,
                ExperimentTreeId::new(&tree_id),
                BranchId::new(&branch_id),
            )?;
            client
                .add_cell(
                    &tree_id,
                    &branch_id,
                    serde_json::to_value(&cell)?,
                    after_cell_id.as_deref(),
                )
                .await?;
            println!("Added cell to branch {}", branch_id);
        }
        CellCommands::UpdateCode {
            tree_id,
            branch_id,
            cell_id,
            source,
            source_file,
        } => {
            let source = load_text_input(source, source_file)?;
            client
                .update_cell_code(&tree_id, &branch_id, &cell_id, &source)
                .await?;
            println!("Updated code for cell {}", cell_id);
        }
        CellCommands::Move {
            tree_id,
            branch_id,
            cell_id,
            direction,
        } => {
            client
                .move_cell(&tree_id, &branch_id, &cell_id, &direction)
                .await?;
            println!("Moved cell {} {}", cell_id, direction);
        }
        CellCommands::Delete {
            tree_id,
            branch_id,
            cell_id,
        } => {
            client.delete_cell(&tree_id, &branch_id, &cell_id).await?;
            println!("Deleted cell {}", cell_id);
        }
        CellCommands::Execute {
            tree_id,
            branch_id,
            cell_id,
            no_wait,
            idempotency_key,
        } => {
            let idempotency_key = idempotency_key.unwrap_or_else(generate_idempotency_key);
            let accepted = client
                .execute_cell(&tree_id, &branch_id, &cell_id, &idempotency_key)
                .await?;
            let execution_id = required_str(&accepted, "execution_id")?;
            if no_wait {
                print_json_pretty(&serde_json::json!({
                    "execution_id": execution_id,
                    "cell_id": cell_id,
                    "idempotency_key": idempotency_key,
                }))?;
            } else {
                let status = client.wait_for_terminal(&execution_id).await?;
                let logs = client.cell_logs(&tree_id, &branch_id, &cell_id).await?;
                print_json_pretty(&serde_json::json!({
                    "execution_id": execution_id,
                    "cell_id": cell_id,
                    "idempotency_key": idempotency_key,
                    "status": status.get("status"),
                    "logs": logs,
                }))?;
            }
        }
        CellCommands::Logs {
            tree_id,
            branch_id,
            cell_id,
        } => {
            print_json_pretty(&client.cell_logs(&tree_id, &branch_id, &cell_id).await?)?;
        }
    }
    Ok(())
}

async fn handle_execution_command(
    client: &TineClient,
    command: ExecutionCommands,
) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        ExecutionCommands::Status { execution_id } => {
            print_json_pretty(&client.execution_status(&execution_id).await?)?;
        }
        ExecutionCommands::Cancel { execution_id } => {
            client.cancel_execution(&execution_id).await?;
            println!("Canceled {}", execution_id);
        }
    }
    Ok(())
}

async fn handle_project_command(
    client: &TineClient,
    command: ProjectCommands,
) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        ProjectCommands::List => {
            print_json_pretty(&client.list_projects().await?)?;
        }
        ProjectCommands::Create {
            name,
            workspace_dir,
            description,
        } => {
            let project = client
                .create_project(&name, &workspace_dir, description.as_deref())
                .await?;
            print_json_pretty(&project)?;
        }
        ProjectCommands::Get { project_id } => {
            print_json_pretty(&client.get_project(&project_id).await?)?;
        }
        ProjectCommands::Experiments { project_id } => {
            print_json_pretty(&client.list_project_experiments(&project_id).await?)?;
        }
    }
    Ok(())
}

async fn handle_file_command(
    client: &TineClient,
    command: FileCommands,
) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        FileCommands::List { path, project } => {
            let entries = client
                .list_files(path.as_deref().unwrap_or_default(), project.as_deref())
                .await?;
            print_json_pretty(&entries)?;
        }
        FileCommands::Read {
            path,
            project,
            output,
        } => {
            let bytes = client.read_file(&path, project.as_deref()).await?;
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
            let content = load_text_input(content, content_file)?;
            client
                .write_file(&path, &content, project.as_deref())
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
    print_doctor_check(
        "package installer",
        true,
        env_manager.installer_description().await,
    );

    match env_manager
        .ensure_python_version_available(DEFAULT_PYTHON_VERSION)
        .await
    {
        Ok(path) => print_doctor_check(&format!("python {}", DEFAULT_PYTHON_VERSION), true, path),
        Err(error) => {
            print_doctor_check(
                &format!("python {}", DEFAULT_PYTHON_VERSION),
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

fn ensure_workspace_bootstrap(
    workspace_dir: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>> {
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
