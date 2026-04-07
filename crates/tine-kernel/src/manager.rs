use std::collections::HashMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use serde::de::DeserializeOwned;
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio::sync::{broadcast, Mutex, Semaphore};
use tracing::{debug, error, info, warn};

use jupyter_protocol::{
    ConnectionInfo, ExecuteRequest, ExecutionState, InterruptRequest, JupyterMessage,
    JupyterMessageContent, ShutdownRequest, Transport,
};
use runtimelib::{
    create_client_control_connection, create_client_heartbeat_connection,
    create_client_iopub_connection, create_client_shell_connection_with_identity, peek_ports,
    peer_identity_for_session, ClientControlConnection, ClientHeartbeatConnection,
    ClientIoPubConnection, ClientShellConnection,
};

use tine_core::{
    ExperimentTreeId, KernelConnectionInfo, NamespaceDelta, NodeOutput, TineError, TineResult,
};

/// Maximum number of consecutive 30s IOPub read timeouts before giving up
/// (in addition to the 7200s overall timeout).  Prevents infinite retry loops
/// when a kernel is stuck but technically alive.
const MAX_IOPUB_TIMEOUTS: u32 = 10;

pub const DEFAULT_EXECUTION_TIMEOUT_SECS: u64 = 7200;

/// Default idle timeout for kernels (seconds).  Kernels that have not executed
/// code within this window are eligible for LRU eviction.
const DEFAULT_IDLE_TIMEOUT_SECS: u64 = 600; // 10 minutes

/// Maximum time to wait for kernel heartbeat during startup.
const HEARTBEAT_STARTUP_TIMEOUT_SECS: u64 = 30;
/// Maximum time to wait for each individual client-channel connect attempt.
const CHANNEL_CONNECT_ATTEMPT_TIMEOUT_SECS: u64 = 5;
/// Retries for transient kernel startup bind failures when port selection races.
const KERNEL_STARTUP_PORT_BIND_RETRIES: u32 = 5;
const MAX_CONCURRENT_KERNEL_STARTUPS: usize = 1;

/// Interval between heartbeat liveness checks in the background monitor.
const HEARTBEAT_CHECK_INTERVAL_SECS: u64 = 30;

/// RSS high-water mark per kernel (bytes).  Kernels exceeding this are logged
/// as warnings.  Configurable at startup is a future enhancement.
const RSS_WARNING_BYTES: u64 = 2 * 1024 * 1024 * 1024; // 2 GiB

// ---------------------------------------------------------------------------
// ManagedKernel — a running Jupyter kernel with ZMQ connections
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum KernelOwnerId {
    ExperimentTree(ExperimentTreeId),
    MapWorker(String),
}

impl KernelOwnerId {
    fn runtime_label(&self) -> String {
        match self {
            Self::ExperimentTree(tree_id) => tree_id.as_str().to_string(),
            Self::MapWorker(worker_id) => worker_id.clone(),
        }
    }

    fn tree_id(&self) -> Option<ExperimentTreeId> {
        match self {
            Self::ExperimentTree(tree_id) => Some(tree_id.clone()),
            Self::MapWorker(_) => None,
        }
    }

    fn connection_file_stem(&self) -> String {
        self.runtime_label()
            .chars()
            .map(|ch| match ch {
                'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' => ch,
                _ => '_',
            })
            .collect()
    }
}

impl fmt::Display for KernelOwnerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ExperimentTree(tree_id) => write!(f, "tree:{tree_id}"),
            Self::MapWorker(worker_id) => write!(f, "worker:{worker_id}"),
        }
    }
}

pub struct ManagedKernel {
    pub id: String,
    pub shell: ClientShellConnection,
    pub iopub: ClientIoPubConnection,
    pub control: ClientControlConnection,
    pub heartbeat: ClientHeartbeatConnection,
    pub connection_info: ConnectionInfo,
    pub connection_file_path: PathBuf,
    pub process: tokio::process::Child,
    pub venv_dir: PathBuf,
    pub working_dir: PathBuf,
    pub session_id: String,
    /// Timestamp of the last `execute_code` call (epoch seconds).
    pub last_used: AtomicU64,
    /// Whether the kernel is currently executing user code.
    pub is_executing: AtomicBool,
}

impl ManagedKernel {
    fn touch(&self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.last_used.store(now, Ordering::Relaxed);
    }

    fn idle_secs(&self) -> u64 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        now.saturating_sub(self.last_used.load(Ordering::Relaxed))
    }

    fn set_executing(&self, value: bool) {
        self.is_executing.store(value, Ordering::Relaxed);
        if value {
            self.touch();
        }
    }

    fn is_executing(&self) -> bool {
        self.is_executing.load(Ordering::Relaxed)
    }
}

#[derive(Debug, Clone)]
pub enum KernelLifecycleEvent {
    Restarted { tree_id: ExperimentTreeId },
    Evicted { tree_id: ExperimentTreeId },
    HeartbeatFailed { tree_id: ExperimentTreeId },
}

#[derive(Debug, Clone, serde::Deserialize, PartialEq, Eq)]
pub struct KernelIsolationOutcome {
    pub contaminated: bool,
    pub signals: Vec<String>,
    pub delta: NamespaceDelta,
}

// ---------------------------------------------------------------------------
// KernelManager — pool and lifecycle management
// ---------------------------------------------------------------------------
//
// Kernels are now keyed by an explicit owner identity. Tree-owned kernels use
// `KernelOwnerId::ExperimentTree`, while remaining legacy/worker flows can keep
// using `KernelOwnerId::Pipeline` until scheduler inputs are fully tree-native.

pub struct KernelManager {
    kernels: DashMap<KernelOwnerId, Arc<Mutex<ManagedKernel>>>,
    kernel_pids: DashMap<KernelOwnerId, u32>,
    max_kernels: usize,
    work_dir: PathBuf,
    /// Per-owner mutex to prevent concurrent duplicate kernel starts.
    start_locks: DashMap<KernelOwnerId, Arc<Mutex<()>>>,
    lifecycle_tx: broadcast::Sender<KernelLifecycleEvent>,
}

impl KernelManager {
    #[cfg(unix)]
    fn send_sigint(&self, owner_id: &KernelOwnerId) -> Option<u32> {
        let pid = self.kernel_pids.get(owner_id).map(|entry| *entry.value());
        if let Some(pid) = pid {
            unsafe {
                libc::kill(pid as i32, libc::SIGINT);
            }
        }
        pid
    }

    #[cfg(not(unix))]
    fn send_sigint(&self, _owner_id: &KernelOwnerId) -> Option<u32> {
        None
    }

    fn normalize_workspace_root(workspace_root: &Path) -> PathBuf {
        let candidate = if workspace_root.is_absolute() {
            workspace_root.to_path_buf()
        } else if let Ok(current_dir) = std::env::current_dir() {
            current_dir.join(workspace_root)
        } else {
            workspace_root.to_path_buf()
        };

        std::fs::canonicalize(&candidate).unwrap_or(candidate)
    }

    fn global_startup_gate() -> &'static Semaphore {
        static GLOBAL_STARTUP_GATE: OnceLock<Semaphore> = OnceLock::new();
        GLOBAL_STARTUP_GATE.get_or_init(|| Semaphore::new(MAX_CONCURRENT_KERNEL_STARTUPS))
    }

    async fn read_child_pipe(pipe: &mut Option<impl tokio::io::AsyncRead + Unpin>) -> String {
        let mut buf = Vec::new();
        if let Some(mut reader) = pipe.take() {
            let _ = reader.read_to_end(&mut buf).await;
        }
        String::from_utf8_lossy(&buf).trim().to_string()
    }

    fn truncate_for_error(text: &str) -> String {
        const LIMIT: usize = 1200;
        if text.len() <= LIMIT {
            text.to_string()
        } else {
            format!("{}…", &text[..LIMIT])
        }
    }

    async fn terminate_startup_process(
        owner_id: &KernelOwnerId,
        pid: u32,
        process: &mut tokio::process::Child,
        conn_path: &Path,
        stage: &str,
    ) -> TineError {
        let _ = process.start_kill();
        let process_status =
            match tokio::time::timeout(Duration::from_secs(2), process.wait()).await {
                Ok(Ok(status)) => status.to_string(),
                Ok(Err(err)) => format!("wait failed: {err}"),
                Err(_) => "timed out waiting for kernel process to exit after kill".to_string(),
            };
        let stdout = Self::truncate_for_error(&Self::read_child_pipe(&mut process.stdout).await);
        let stderr = Self::truncate_for_error(&Self::read_child_pipe(&mut process.stderr).await);
        error!(
            owner = %owner_id,
            pid = pid,
            process_status = %process_status,
            stdout = %stdout,
            stderr = %stderr,
            stage = stage,
            "kernel startup failed after timeout"
        );
        let _ = tokio::fs::remove_file(conn_path).await;
        TineError::KernelStartupFailed {
            runtime_id: owner_id.to_string(),
            message: format!(
                "{stage} after {HEARTBEAT_STARTUP_TIMEOUT_SECS}s (process status: {process_status}). stderr: {stderr}. stdout: {stdout}"
            ),
        }
    }

    fn owner_id_for_tree(tree_id: &ExperimentTreeId) -> KernelOwnerId {
        KernelOwnerId::ExperimentTree(tree_id.clone())
    }

    fn owner_id_for_worker(worker_id: &str) -> KernelOwnerId {
        KernelOwnerId::MapWorker(worker_id.to_string())
    }

    pub fn new(workspace_root: &Path, max_kernels: usize) -> Self {
        let workspace_root = Self::normalize_workspace_root(workspace_root);
        let work_dir = workspace_root.join(".tine").join("kernels");
        let (lifecycle_tx, _) = broadcast::channel(256);
        Self {
            kernels: DashMap::new(),
            kernel_pids: DashMap::new(),
            max_kernels,
            work_dir,
            start_locks: DashMap::new(),
            lifecycle_tx,
        }
    }

    pub fn subscribe_lifecycle(&self) -> broadcast::Receiver<KernelLifecycleEvent> {
        self.lifecycle_tx.subscribe()
    }

    fn emit_lifecycle(&self, event: KernelLifecycleEvent) {
        let _ = self.lifecycle_tx.send(event);
    }

    pub async fn cleanup_orphans(&self) -> TineResult<usize> {
        if !self.work_dir.exists() {
            return Ok(0);
        }
        let mut cleaned = 0;
        let mut entries = tokio::fs::read_dir(&self.work_dir).await?;
        while let Ok(Some(entry)) = entries.next_entry().await {
            let path = entry.path();
            if path.extension().map(|e| e == "json").unwrap_or(false) {
                if let Some(pid) = Self::extract_pid_from_path(&path) {
                    if !process_alive(pid) {
                        info!(path = %path.display(), pid = pid, "removing orphaned connection file");
                        let _ = tokio::fs::remove_file(&path).await;
                        cleaned += 1;
                    }
                }
            }
        }
        if cleaned > 0 {
            info!(cleaned = cleaned, "cleaned orphaned kernel files");
        }
        Ok(cleaned)
    }

    fn extract_pid_from_path(path: &Path) -> Option<u32> {
        path.file_stem()
            .and_then(|s| s.to_str())
            .and_then(|s| s.strip_prefix("kernel-"))
            .and_then(|s| s.parse::<u32>().ok())
    }

    /// Start a new Jupyter kernel for an ephemeral map worker runtime.
    /// Polls the heartbeat channel instead of sleeping to confirm the kernel
    /// is ready before returning.
    pub async fn start_worker_kernel(
        &self,
        worker_id: &str,
        venv_dir: &Path,
        working_dir: &Path,
    ) -> TineResult<()> {
        self.start_owned_kernel(&Self::owner_id_for_worker(worker_id), venv_dir, working_dir)
            .await
    }

    async fn start_owned_kernel(
        &self,
        owner_id: &KernelOwnerId,
        venv_dir: &Path,
        working_dir: &Path,
    ) -> TineResult<()> {
        // Acquire per-owner start lock to prevent concurrent duplicate starts
        let lock = self
            .start_locks
            .entry(owner_id.clone())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        let _guard = lock.lock().await;
        let _startup_permit = Self::global_startup_gate()
            .acquire()
            .await
            .map_err(|_| TineError::Internal("kernel startup gate closed".to_string()))?;

        // Double-check: another task may have started the kernel while we waited
        if self.kernels.contains_key(owner_id) {
            return Ok(());
        }

        // Check budget — try to evict an idle kernel if we're at capacity
        if self.kernels.len() >= self.max_kernels {
            if !self.try_evict_idle().await {
                return Err(TineError::BudgetExceeded(format!(
                    "maximum kernels ({}) reached and no idle kernels to evict",
                    self.max_kernels
                )));
            }
        }

        tokio::fs::create_dir_all(&self.work_dir).await?;

        let mut last_retryable_error = None;
        for attempt in 1..=KERNEL_STARTUP_PORT_BIND_RETRIES {
            match self
                .start_kernel_once(owner_id, venv_dir, working_dir)
                .await
            {
                Ok(()) => return Ok(()),
                Err(TineError::KernelStartupFailed {
                    runtime_id,
                    message,
                }) if Self::is_port_bind_startup_error(&message)
                    && attempt < KERNEL_STARTUP_PORT_BIND_RETRIES =>
                {
                    warn!(
                        runtime = runtime_id,
                        attempt,
                        max_attempts = KERNEL_STARTUP_PORT_BIND_RETRIES,
                        message = %message,
                        "kernel startup hit transient port bind failure, retrying"
                    );
                    last_retryable_error = Some(TineError::KernelStartupFailed {
                        runtime_id,
                        message,
                    });
                }
                Err(err) => return Err(err),
            }
        }

        Err(
            last_retryable_error.unwrap_or_else(|| TineError::KernelStartupFailed {
                runtime_id: owner_id.to_string(),
                message: "kernel startup failed after exhausting retries".to_string(),
            }),
        )
    }

    async fn start_kernel_once(
        &self,
        owner_id: &KernelOwnerId,
        venv_dir: &Path,
        working_dir: &Path,
    ) -> TineResult<()> {
        // Allocate 5 ports via runtimelib
        let ip: std::net::IpAddr = "127.0.0.1".parse().unwrap();
        let ports = peek_ports(ip, 5)
            .await
            .map_err(|e| TineError::Internal(format!("failed to allocate ports: {}", e)))?;

        let session_id = uuid::Uuid::new_v4().to_string();
        let key = uuid::Uuid::new_v4().to_string().replace('-', "");

        let conn_info = ConnectionInfo {
            transport: Transport::TCP,
            ip: "127.0.0.1".to_string(),
            shell_port: ports[0],
            iopub_port: ports[1],
            stdin_port: ports[2],
            control_port: ports[3],
            hb_port: ports[4],
            key: key.clone(),
            signature_scheme: "hmac-sha256".to_string(),
            kernel_name: Some("python3".to_string()),
        };

        // Write connection file
        let conn_json = serde_json::to_string_pretty(&conn_info)
            .map_err(|e| TineError::Internal(e.to_string()))?;
        let conn_path = self
            .work_dir
            .join(format!("kernel-{}.json", owner_id.connection_file_stem()));
        tokio::fs::write(&conn_path, &conn_json).await?;

        // Spawn ipykernel
        let python = if cfg!(windows) {
            venv_dir.join("Scripts").join("python.exe")
        } else {
            venv_dir.join("bin").join("python")
        };

        info!(
            owner = %owner_id,
            python = %python.display(),
            cwd = %working_dir.display(),
            shell_port = ports[0],
            iopub_port = ports[1],
            "starting kernel"
        );

        let mut process = Command::new(&python)
            .args([
                "-m",
                "ipykernel_launcher",
                "-f",
                &conn_path.display().to_string(),
            ])
            .current_dir(working_dir)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| TineError::KernelStartupFailed {
                runtime_id: owner_id.to_string(),
                message: format!("failed to spawn ipykernel: {}", e),
            })?;

        let pid = process.id().unwrap_or(0);
        let kernel_id = format!("kernel-{}", pid);

        // Connect heartbeat channel first, then poll it to confirm readiness
        // (replaces the old hardcoded 2s sleep)
        let connect_deadline = Instant::now() + Duration::from_secs(HEARTBEAT_STARTUP_TIMEOUT_SECS);
        let mut connect_delay = Duration::from_millis(100);
        let mut heartbeat = loop {
            if let Some(status) = process.try_wait()? {
                let stdout = Self::read_child_pipe(&mut process.stdout).await;
                let stderr = Self::read_child_pipe(&mut process.stderr).await;
                let stdout = Self::truncate_for_error(&stdout);
                let stderr = Self::truncate_for_error(&stderr);
                error!(
                    owner = %owner_id,
                    pid = pid,
                    status = %status,
                    stdout = %stdout,
                    stderr = %stderr,
                    "kernel process exited before heartbeat connection"
                );
                let _ = tokio::fs::remove_file(&conn_path).await;
                return Err(TineError::KernelStartupFailed {
                    runtime_id: owner_id.to_string(),
                    message: format!(
                        "ipykernel exited before heartbeat connect (status: {status}). stderr: {stderr}"
                    ),
                });
            }
            match tokio::time::timeout(
                Duration::from_secs(CHANNEL_CONNECT_ATTEMPT_TIMEOUT_SECS),
                create_client_heartbeat_connection(&conn_info),
            )
            .await
            {
                Ok(Ok(heartbeat)) => break heartbeat,
                Ok(Err(err)) => {
                    debug!(
                        owner = %owner_id,
                        pid = pid,
                        error = %err,
                        "heartbeat channel connect failed, retrying"
                    );
                }
                Err(_) => {
                    debug!(
                        owner = %owner_id,
                        pid = pid,
                        timeout_secs = CHANNEL_CONNECT_ATTEMPT_TIMEOUT_SECS,
                        "heartbeat channel connect timed out, retrying"
                    );
                }
            }
            if Instant::now() >= connect_deadline {
                return Err(Self::terminate_startup_process(
                    owner_id,
                    pid,
                    &mut process,
                    &conn_path,
                    "timed out connecting heartbeat channel",
                )
                .await);
            }
            tokio::time::sleep(connect_delay).await;
            connect_delay = (connect_delay * 2).min(Duration::from_secs(2));
        };

        // Poll heartbeat with exponential backoff until the kernel responds
        let hb_deadline = Instant::now() + Duration::from_secs(HEARTBEAT_STARTUP_TIMEOUT_SECS);
        let mut hb_delay = Duration::from_millis(100);
        let mut hb_attempt = 0u32;
        let mut hb_ok = false;
        while Instant::now() < hb_deadline {
            hb_attempt += 1;
            if let Some(status) = process.try_wait()? {
                let stdout = Self::read_child_pipe(&mut process.stdout).await;
                let stderr = Self::read_child_pipe(&mut process.stderr).await;
                let stdout = Self::truncate_for_error(&stdout);
                let stderr = Self::truncate_for_error(&stderr);
                error!(
                    owner = %owner_id,
                    pid = pid,
                    status = %status,
                    stdout = %stdout,
                    stderr = %stderr,
                    "kernel process exited before heartbeat"
                );
                let _ = tokio::fs::remove_file(&conn_path).await;
                return Err(TineError::KernelStartupFailed {
                    runtime_id: owner_id.to_string(),
                    message: format!(
                        "ipykernel exited before heartbeat (status: {status}). stderr: {stderr}"
                    ),
                });
            }
            match tokio::time::timeout(Duration::from_secs(2), heartbeat.single_heartbeat()).await {
                Ok(Ok(())) => {
                    hb_ok = true;
                    break;
                }
                Ok(Err(err)) => {
                    debug!(
                        owner = %owner_id,
                        pid = pid,
                        attempt = hb_attempt,
                        error = %err,
                        "kernel heartbeat probe failed"
                    );
                    tokio::time::sleep(hb_delay).await;
                    hb_delay = (hb_delay * 2).min(Duration::from_secs(2));
                }
                Err(_) => {
                    debug!(
                        owner = %owner_id,
                        pid = pid,
                        attempt = hb_attempt,
                        "kernel heartbeat probe timed out"
                    );
                    // Kernel not ready yet — back off and retry
                    tokio::time::sleep(hb_delay).await;
                    hb_delay = (hb_delay * 2).min(Duration::from_secs(2));
                }
            }
        }
        if !hb_ok {
            if let Some(status) = process.try_wait()? {
                let stdout = Self::read_child_pipe(&mut process.stdout).await;
                let stderr = Self::read_child_pipe(&mut process.stderr).await;
                let stdout = Self::truncate_for_error(&stdout);
                let stderr = Self::truncate_for_error(&stderr);
                error!(
                    owner = %owner_id,
                    pid = pid,
                    status = %status,
                    stdout = %stdout,
                    stderr = %stderr,
                    "kernel exited during heartbeat startup timeout"
                );
                let _ = tokio::fs::remove_file(&conn_path).await;
                return Err(TineError::KernelStartupFailed {
                    runtime_id: owner_id.to_string(),
                    message: format!(
                        "ipykernel exited during startup timeout (status: {status}). stderr: {stderr}"
                    ),
                });
            }
            // Clean up the connection file and let the process die via kill_on_drop
            return Err(Self::terminate_startup_process(
                owner_id,
                pid,
                &mut process,
                &conn_path,
                "kernel heartbeat not confirmed before startup timeout",
            )
            .await);
        }

        info!(owner = %owner_id, pid = pid, "heartbeat confirmed, connecting channels");

        // Connect remaining ZMQ channels
        let peer_identity = peer_identity_for_session(&session_id)
            .map_err(|e| TineError::Internal(format!("peer identity error: {}", e)))?;

        let shell = tokio::time::timeout(
            Duration::from_secs(CHANNEL_CONNECT_ATTEMPT_TIMEOUT_SECS),
            create_client_shell_connection_with_identity(&conn_info, &session_id, peer_identity),
        )
        .await
        .map_err(|_| TineError::KernelStartupFailed {
            runtime_id: owner_id.to_string(),
            message: format!(
                "timed out connecting shell channel after {}s",
                CHANNEL_CONNECT_ATTEMPT_TIMEOUT_SECS
            ),
        })?
        .map_err(|e| TineError::KernelStartupFailed {
            runtime_id: owner_id.to_string(),
            message: format!("failed to connect shell: {}", e),
        })?;

        let iopub = tokio::time::timeout(
            Duration::from_secs(CHANNEL_CONNECT_ATTEMPT_TIMEOUT_SECS),
            create_client_iopub_connection(&conn_info, "", &session_id),
        )
        .await
        .map_err(|_| TineError::KernelStartupFailed {
            runtime_id: owner_id.to_string(),
            message: format!(
                "timed out connecting iopub channel after {}s",
                CHANNEL_CONNECT_ATTEMPT_TIMEOUT_SECS
            ),
        })?
        .map_err(|e| TineError::KernelStartupFailed {
            runtime_id: owner_id.to_string(),
            message: format!("failed to connect iopub: {}", e),
        })?;

        let control = tokio::time::timeout(
            Duration::from_secs(CHANNEL_CONNECT_ATTEMPT_TIMEOUT_SECS),
            create_client_control_connection(&conn_info, &session_id),
        )
        .await
        .map_err(|_| TineError::KernelStartupFailed {
            runtime_id: owner_id.to_string(),
            message: format!(
                "timed out connecting control channel after {}s",
                CHANNEL_CONNECT_ATTEMPT_TIMEOUT_SECS
            ),
        })?
        .map_err(|e| TineError::KernelStartupFailed {
            runtime_id: owner_id.to_string(),
            message: format!("failed to connect control: {}", e),
        })?;

        let now_epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let kernel = ManagedKernel {
            id: kernel_id.clone(),
            shell,
            iopub,
            control,
            heartbeat,
            connection_info: conn_info,
            connection_file_path: conn_path,
            process,
            venv_dir: venv_dir.to_path_buf(),
            working_dir: working_dir.to_path_buf(),
            session_id,
            last_used: AtomicU64::new(now_epoch),
            is_executing: AtomicBool::new(false),
        };
        self.kernels
            .insert(owner_id.clone(), Arc::new(Mutex::new(kernel)));
        self.kernel_pids.insert(owner_id.clone(), pid);

        metrics::counter!("tine_kernel_startups_total").increment(1);
        metrics::gauge!("tine_kernels_active").increment(1.0);

        info!(
            owner = %owner_id,
            kernel_id = %kernel_id,
            pid = pid,
            "kernel started successfully"
        );

        // Send tine helper functions into the kernel namespace
        self.send_setup_code(owner_id).await?;

        Ok(())
    }

    fn is_port_bind_startup_error(message: &str) -> bool {
        let message = message.to_ascii_lowercase();
        message.contains("address already in use")
            || (message.contains("bind") && message.contains("port"))
    }

    /// Start the tree-owned kernel for an experiment runtime.
    pub async fn start_tree_kernel(
        &self,
        tree_id: &ExperimentTreeId,
        venv_dir: &Path,
        working_dir: &Path,
    ) -> TineResult<()> {
        self.start_owned_kernel(&Self::owner_id_for_tree(tree_id), venv_dir, working_dir)
            .await
    }

    /// Execute code in an ephemeral map-worker kernel.
    pub async fn execute_worker_code(
        &self,
        worker_id: &str,
        code: &str,
    ) -> TineResult<KernelExecutionResult> {
        self.execute_owned_code_with_timeout(
            &Self::owner_id_for_worker(worker_id),
            code,
            DEFAULT_EXECUTION_TIMEOUT_SECS,
        )
            .await
    }

    /// Execute code in the tree-owned kernel and return stdout/stderr + outputs.
    pub async fn execute_tree_code(
        &self,
        tree_id: &ExperimentTreeId,
        code: &str,
    ) -> TineResult<KernelExecutionResult> {
        self.execute_owned_code_with_timeout(
            &Self::owner_id_for_tree(tree_id),
            code,
            DEFAULT_EXECUTION_TIMEOUT_SECS,
        )
            .await
    }

    /// Execute code in an ephemeral map-worker kernel with a custom timeout.
    pub async fn execute_worker_code_with_timeout(
        &self,
        worker_id: &str,
        code: &str,
        timeout_secs: u64,
    ) -> TineResult<KernelExecutionResult> {
        self.execute_owned_code_with_timeout(
            &Self::owner_id_for_worker(worker_id),
            code,
            timeout_secs,
        )
        .await
    }

    async fn execute_owned_code_with_timeout(
        &self,
        owner_id: &KernelOwnerId,
        code: &str,
        timeout_secs: u64,
    ) -> TineResult<KernelExecutionResult> {
        self.execute_owned_code_with_timeout_and_stream(owner_id, code, timeout_secs, |_, _| {})
            .await
    }

    pub async fn execute_worker_code_with_timeout_and_stream<F>(
        &self,
        worker_id: &str,
        code: &str,
        timeout_secs: u64,
        on_stream: F,
    ) -> TineResult<KernelExecutionResult>
    where
        F: FnMut(&str, &str),
    {
        self.execute_owned_code_with_timeout_and_stream(
            &Self::owner_id_for_worker(worker_id),
            code,
            timeout_secs,
            on_stream,
        )
        .await
    }

    async fn execute_owned_code_with_timeout_and_stream<F>(
        &self,
        owner_id: &KernelOwnerId,
        code: &str,
        timeout_secs: u64,
        mut on_stream: F,
    ) -> TineResult<KernelExecutionResult>
    where
        F: FnMut(&str, &str),
    {
        let kernel = self
            .kernels
            .get(owner_id)
            .map(|entry| Arc::clone(entry.value()))
            .ok_or_else(|| TineError::KernelNotFound {
                kernel_id: format!("owner:{}", owner_id),
            })?;
        let mut kernel = kernel.lock().await;

        kernel.touch();
        kernel.set_executing(true);
        debug!(owner = %owner_id, code_len = code.len(), "executing code");

        // Build and send ExecuteRequest on shell channel
        let execute_request = ExecuteRequest::new(code.to_string());
        let message: JupyterMessage = execute_request.into();
        kernel
            .shell
            .send(message)
            .await
            .map_err(|e| TineError::KernelComm(format!("send execute request failed: {}", e)))?;

        // Collect IOPub messages until Status::Idle
        let mut stdout = String::new();
        let mut stderr = String::new();
        let mut outputs: Vec<NodeOutput> = Vec::new();
        let mut exec_error: Option<KernelExecutionError> = None;
        let start = Instant::now();
        let timeout = Duration::from_secs(timeout_secs);
        let mut consecutive_timeouts: u32 = 0;

        loop {
            let elapsed = start.elapsed();
            if elapsed >= timeout {
                let pid = self.send_sigint(owner_id);
                warn!(owner = %owner_id, pid = ?pid, timeout_secs, "execution exceeded timeout; interrupting kernel");
                kernel.set_executing(false);
                return Err(TineError::ExecutionTimedOut { timeout_secs });
            }

            let remaining = timeout.saturating_sub(elapsed);
            let read_timeout = remaining.min(Duration::from_secs(30));

            let msg = match tokio::time::timeout(read_timeout, kernel.iopub.read()).await
            {
                Ok(Ok(msg)) => {
                    consecutive_timeouts = 0; // reset on success
                    kernel.touch();
                    msg
                }
                Ok(Err(e)) => {
                    kernel.set_executing(false);
                    error!(owner = %owner_id, error = %e, "iopub read error");
                    return Err(TineError::KernelComm(format!("iopub read error: {}", e)));
                }
                Err(_) => {
                    if start.elapsed() >= timeout {
                        let pid = self.send_sigint(owner_id);
                        warn!(owner = %owner_id, pid = ?pid, timeout_secs, "execution timed out while waiting for iopub; interrupting kernel");
                        kernel.set_executing(false);
                        return Err(TineError::ExecutionTimedOut { timeout_secs });
                    }
                    consecutive_timeouts += 1;
                    if consecutive_timeouts >= MAX_IOPUB_TIMEOUTS {
                        error!(
                            owner = %owner_id,
                            timeouts = consecutive_timeouts,
                            "iopub read timed out {} consecutive times, giving up",
                            MAX_IOPUB_TIMEOUTS
                        );
                        kernel.set_executing(false);
                        return Err(TineError::KernelComm(format!(
                            "kernel unresponsive: {} consecutive 30s IOPub timeouts",
                            consecutive_timeouts
                        )));
                    }
                    warn!(
                        owner = %owner_id,
                        timeouts = consecutive_timeouts,
                        max = MAX_IOPUB_TIMEOUTS,
                        "iopub read timeout, retrying"
                    );
                    continue;
                }
            };

            match msg.content {
                JupyterMessageContent::Status(status) => {
                    if status.execution_state == ExecutionState::Idle {
                        kernel.set_executing(false);
                        debug!(owner = %owner_id, "kernel idle, execution complete");
                        break;
                    }
                }
                JupyterMessageContent::StreamContent(stream) => match stream.name {
                    jupyter_protocol::Stdio::Stdout => {
                        stdout.push_str(&stream.text);
                        on_stream("stdout", &stream.text);
                    }
                    jupyter_protocol::Stdio::Stderr => {
                        stderr.push_str(&stream.text);
                        on_stream("stderr", &stream.text);
                    }
                },
                JupyterMessageContent::ExecuteResult(result) => {
                    let data = media_to_map(&result.data);
                    outputs.push(NodeOutput {
                        data,
                        metadata: HashMap::new(),
                    });
                }
                JupyterMessageContent::DisplayData(display) => {
                    let data = media_to_map(&display.data);
                    outputs.push(NodeOutput {
                        data,
                        metadata: HashMap::new(),
                    });
                }
                JupyterMessageContent::ErrorOutput(err) => {
                    exec_error = Some(KernelExecutionError {
                        ename: err.ename.clone(),
                        evalue: err.evalue.clone(),
                        traceback: err.traceback.clone(),
                    });
                    stderr.push_str(&err.traceback.join("\n"));
                }
                JupyterMessageContent::UpdateDisplayData(update) => {
                    let data = media_to_map(&update.data);
                    outputs.push(NodeOutput {
                        data,
                        metadata: HashMap::new(),
                    });
                }
                _ => {} // ExecuteInput, ClearOutput, etc.
            }
        }

        // Read shell reply to stay in sync
        match tokio::time::timeout(Duration::from_secs(5), kernel.shell.read()).await {
            Ok(Ok(reply)) => {
                debug!(owner = %owner_id, msg_type = reply.message_type(), "shell reply");
            }
            Ok(Err(e)) => {
                warn!(owner = %owner_id, error = %e, "shell reply error");
            }
            Err(_) => {
                warn!(owner = %owner_id, "shell reply timed out");
            }
        }

        kernel.set_executing(false);
        Ok(KernelExecutionResult {
            stdout,
            stderr,
            outputs,
            error: exec_error,
            duration_ms: 0, // Set by caller (scheduler measures wall-clock)
        })
    }

    /// Tree-scoped compatibility adapter.
    pub async fn execute_tree_code_with_timeout(
        &self,
        tree_id: &ExperimentTreeId,
        code: &str,
        timeout_secs: u64,
    ) -> TineResult<KernelExecutionResult> {
        self.execute_owned_code_with_timeout(&Self::owner_id_for_tree(tree_id), code, timeout_secs)
            .await
    }

    pub async fn execute_tree_code_with_timeout_and_stream<F>(
        &self,
        tree_id: &ExperimentTreeId,
        code: &str,
        timeout_secs: u64,
        on_stream: F,
    ) -> TineResult<KernelExecutionResult>
    where
        F: FnMut(&str, &str),
    {
        self.execute_owned_code_with_timeout_and_stream(
            &Self::owner_id_for_tree(tree_id),
            code,
            timeout_secs,
            on_stream,
        )
        .await
    }

    pub async fn begin_tree_branch_session(
        &self,
        tree_id: &ExperimentTreeId,
        session_id: &str,
    ) -> TineResult<()> {
        let result = self
            .execute_tree_code(
                tree_id,
                &format!(
                    r#"
if not _pf_begin_branch_session({session_id:?}):
    raise RuntimeError("branch session did not start")
"#
                ),
            )
            .await?;
        if let Some(err) = result.error {
            return Err(TineError::KernelComm(format!(
                "failed to begin branch session: {}",
                err.evalue
            )));
        }
        Ok(())
    }

    pub async fn end_tree_branch_session(
        &self,
        tree_id: &ExperimentTreeId,
        session_id: &str,
    ) -> TineResult<KernelIsolationOutcome> {
        let result = self
            .execute_tree_code(
                tree_id,
                &format!(
                    r#"
_pf_result = _pf_end_branch_session({session_id:?})
print("__tine_isolation__" + _pf_json.dumps(_pf_result, sort_keys=True))
"#
                ),
            )
            .await?;
        if let Some(err) = result.error {
            return Err(TineError::KernelComm(format!(
                "failed to end branch session: {}",
                err.evalue
            )));
        }
        Self::parse_stdout_json_marker(&result.stdout, "__tine_isolation__")
    }

    /// Interrupt a running kernel via the control channel.
    async fn interrupt_owned(&self, owner_id: &KernelOwnerId) -> TineResult<()> {
        let pid = self.send_sigint(owner_id);
        info!(owner = %owner_id, pid = ?pid, "interrupting kernel");

        if let Some(kernel) = self
            .kernels
            .get(owner_id)
            .map(|entry| Arc::clone(entry.value()))
        {
            if let Ok(mut kernel) =
                tokio::time::timeout(Duration::from_millis(50), kernel.lock()).await
            {
                let interrupt_msg: JupyterMessage = InterruptRequest {}.into();
                if let Err(e) = kernel.control.send(interrupt_msg).await {
                    warn!(owner = %owner_id, error = %e, "control interrupt failed after SIGINT");
                    return Ok(());
                }

                match tokio::time::timeout(Duration::from_secs(5), kernel.control.read()).await {
                    Ok(Ok(_)) => debug!(owner = %owner_id, "interrupt acknowledged"),
                    Ok(Err(e)) => warn!(owner = %owner_id, error = %e, "interrupt reply error"),
                    Err(_) => warn!(owner = %owner_id, "interrupt acknowledgement timed out"),
                }
            }
        }
        Ok(())
    }

    /// Tree-scoped compatibility adapter.
    pub async fn interrupt_tree(&self, tree_id: &ExperimentTreeId) -> TineResult<()> {
        self.interrupt_owned(&Self::owner_id_for_tree(tree_id))
            .await
    }

    /// Shutdown a kernel gracefully via the control channel.
    async fn shutdown_owned(&self, owner_id: &KernelOwnerId) -> TineResult<()> {
        if let Some((_, kernel)) = self.kernels.remove(owner_id) {
            self.kernel_pids.remove(owner_id);
            let mut kernel = kernel.lock().await;
            info!(owner = %owner_id, kernel = %kernel.id, "shutting down kernel");

            metrics::gauge!("tine_kernels_active").decrement(1.0);

            let shutdown_msg: JupyterMessage = ShutdownRequest { restart: false }.into();
            let _ = kernel.control.send(shutdown_msg).await;

            match tokio::time::timeout(Duration::from_secs(5), kernel.control.read()).await {
                Ok(Ok(_)) => debug!(owner = %owner_id, "shutdown acknowledged"),
                _ => warn!(owner = %owner_id, "shutdown reply not received, killing"),
            }

            let _ = kernel.process.kill().await;
            let _ = tokio::fs::remove_file(&kernel.connection_file_path).await;
        }
        Ok(())
    }

    pub async fn shutdown_worker_kernel(&self, worker_id: &str) -> TineResult<()> {
        self.shutdown_owned(&Self::owner_id_for_worker(worker_id))
            .await
    }

    /// Tree-scoped compatibility adapter.
    pub async fn shutdown_tree(&self, tree_id: &ExperimentTreeId) -> TineResult<()> {
        self.shutdown_owned(&Self::owner_id_for_tree(tree_id)).await
    }

    /// Restart a kernel: shut it down and start a fresh one.
    /// Used for recovery when ZMQ sockets become disconnected.
    async fn restart_owned_kernel(&self, owner_id: &KernelOwnerId) -> TineResult<()> {
        let kernel = self
            .kernels
            .get(owner_id)
            .map(|entry| Arc::clone(entry.value()))
            .ok_or_else(|| TineError::KernelNotFound {
                kernel_id: format!("owner:{}", owner_id),
            })?;
        let kernel = kernel.lock().await;
        let (venv_dir, working_dir) = (kernel.venv_dir.clone(), kernel.working_dir.clone());
        drop(kernel);
        info!(owner = %owner_id, "restarting kernel (ZMQ recovery)");
        self.shutdown_owned(owner_id).await?;
        self.start_owned_kernel(owner_id, &venv_dir, &working_dir)
            .await?;
        if let Some(tree_id) = owner_id.tree_id() {
            self.emit_lifecycle(KernelLifecycleEvent::Restarted { tree_id });
        }
        Ok(())
    }

    /// Tree-scoped compatibility adapter.
    pub async fn restart_tree_kernel(&self, tree_id: &ExperimentTreeId) -> TineResult<()> {
        self.restart_owned_kernel(&Self::owner_id_for_tree(tree_id))
            .await
    }

    /// Shutdown all kernels.
    pub async fn shutdown_all(&self) -> TineResult<()> {
        let keys: Vec<KernelOwnerId> = self.kernels.iter().map(|e| e.key().clone()).collect();
        for key in keys {
            self.shutdown_owned(&key).await?;
        }
        Ok(())
    }

    /// Check whether a tree-owned kernel is active.
    pub fn has_tree_kernel(&self, tree_id: &ExperimentTreeId) -> bool {
        self.kernels.contains_key(&Self::owner_id_for_tree(tree_id))
    }

    /// Get connection info for a tree-owned kernel.
    pub fn connection_info_for_tree(
        &self,
        tree_id: &ExperimentTreeId,
    ) -> Option<KernelConnectionInfo> {
        let entry = self.kernels.get(&Self::owner_id_for_tree(tree_id))?;
        let kernel = Arc::clone(entry.value());
        drop(entry);
        let k = kernel.try_lock().ok()?;
        Some(KernelConnectionInfo {
            transport: k.connection_info.transport.to_string(),
            ip: k.connection_info.ip.clone(),
            shell_port: k.connection_info.shell_port,
            iopub_port: k.connection_info.iopub_port,
            stdin_port: k.connection_info.stdin_port,
            control_port: k.connection_info.control_port,
            hb_port: k.connection_info.hb_port,
            key: k.connection_info.key.clone(),
        })
    }

    /// Get the Jupyter connection file path for a tree-owned kernel.
    pub fn connection_file_path_for_tree(&self, tree_id: &ExperimentTreeId) -> Option<PathBuf> {
        let entry = self.kernels.get(&Self::owner_id_for_tree(tree_id))?;
        let kernel = Arc::clone(entry.value());
        drop(entry);
        let k = kernel.try_lock().ok()?;
        Some(k.connection_file_path.clone())
    }

    pub fn active_count(&self) -> usize {
        self.kernels.len()
    }

    /// Check heartbeat for a specific kernel.  Returns `true` if the kernel
    /// responded within 2 seconds.
    async fn check_owned_heartbeat(&self, owner_id: &KernelOwnerId) -> bool {
        let kernel = match self.kernels.get(owner_id) {
            Some(entry) => Arc::clone(entry.value()),
            None => return false,
        };
        let mut kernel = kernel.lock().await;
        matches!(
            tokio::time::timeout(Duration::from_secs(2), kernel.heartbeat.single_heartbeat()).await,
            Ok(Ok(()))
        )
    }

    /// Tree-scoped compatibility adapter.
    pub async fn check_tree_heartbeat(&self, tree_id: &ExperimentTreeId) -> bool {
        self.check_owned_heartbeat(&Self::owner_id_for_tree(tree_id))
            .await
    }

    /// Spawn a background task that periodically checks heartbeats for all
    /// kernels and evicts idle ones that exceed `DEFAULT_IDLE_TIMEOUT_SECS`.
    ///
    /// Call this once after creating the `KernelManager` (wrapped in `Arc`).
    /// The task runs until the `CancellationToken` is cancelled or the
    /// `Arc<KernelManager>` is the last reference.
    pub fn spawn_monitor(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let mgr = Arc::clone(self);
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(Duration::from_secs(HEARTBEAT_CHECK_INTERVAL_SECS));
            loop {
                interval.tick().await;

                let keys: Vec<KernelOwnerId> =
                    mgr.kernels.iter().map(|e| e.key().clone()).collect();

                for owner_id in &keys {
                    let executing = if let Some(entry) = mgr.kernels.get(owner_id) {
                        let kernel = Arc::clone(entry.value());
                        drop(entry);
                        let executing = kernel.lock().await.is_executing();
                        executing
                    } else {
                        false
                    };
                    if executing {
                        continue;
                    }
                    // 1. Evict kernels that have been idle too long
                    let idle = if let Some(entry) = mgr.kernels.get(owner_id) {
                        let kernel = Arc::clone(entry.value());
                        drop(entry);
                        let idle = kernel.lock().await.idle_secs();
                        idle
                    } else {
                        0
                    };
                    if idle >= DEFAULT_IDLE_TIMEOUT_SECS {
                        info!(
                            owner = %owner_id,
                            idle_secs = idle,
                            "evicting idle kernel"
                        );
                        let _ = mgr.shutdown_owned(owner_id).await;
                        if let Some(tree_id) = owner_id.tree_id() {
                            mgr.emit_lifecycle(KernelLifecycleEvent::Evicted { tree_id });
                        }
                        continue;
                    }

                    // 2. Check heartbeat — log dead kernels (callers will get
                    //    errors on next execute_code and can handle accordingly)
                    if !mgr.check_owned_heartbeat(owner_id).await {
                        warn!(
                            owner = %owner_id,
                            "kernel heartbeat failed — kernel may be dead"
                        );
                        metrics::counter!("tine_kernel_heartbeat_failures_total").increment(1);
                        if let Some(tree_id) = owner_id.tree_id() {
                            mgr.emit_lifecycle(KernelLifecycleEvent::HeartbeatFailed { tree_id });
                        }
                    }

                    // 3. Check RSS — warn if kernel is using excessive memory
                    if let Some(entry) = mgr.kernels.get(owner_id) {
                        let kernel = Arc::clone(entry.value());
                        drop(entry);
                        let k = kernel.lock().await;
                        if let Some(process_pid) = k.process.id() {
                            if let Some(rss) = process_rss_bytes(process_pid) {
                                let rss_mb = rss / (1024 * 1024);
                                metrics::gauge!("tine_kernel_rss_bytes", "owner" => owner_id.to_string())
                                    .set(rss as f64);
                                if rss > RSS_WARNING_BYTES {
                                    warn!(
                                        owner = %owner_id,
                                        rss_mb = rss_mb,
                                        limit_mb = RSS_WARNING_BYTES / (1024 * 1024),
                                        "kernel RSS exceeds warning threshold"
                                    );
                                }
                            }
                        }
                    }
                }
            }
        })
    }

    /// Try to evict the least-recently-used idle kernel to free capacity.
    /// Returns `true` if a kernel was evicted.
    async fn try_evict_idle(&self) -> bool {
        // Find the kernel with the largest idle_secs
        let mut best: Option<(KernelOwnerId, u64)> = None;
        for entry in self.kernels.iter() {
            let idle = entry.value().try_lock().map(|k| k.idle_secs()).unwrap_or(0);
            if idle > best.as_ref().map(|(_, s)| *s).unwrap_or(0) {
                best = Some((entry.key().clone(), idle));
            }
        }
        if let Some((owner_id, idle)) = best {
            // Only evict if the kernel has been idle for at least 60 seconds
            if idle >= 60 {
                info!(
                    owner = %owner_id,
                    idle_secs = idle,
                    "evicting LRU kernel to free capacity"
                );
                let _ = self.shutdown_owned(&owner_id).await;
                if let Some(tree_id) = owner_id.tree_id() {
                    self.emit_lifecycle(KernelLifecycleEvent::Evicted { tree_id });
                }
                return true;
            }
        }
        false
    }

    /// Send tine helper functions into the kernel namespace.
    async fn send_setup_code(&self, owner_id: &KernelOwnerId) -> TineResult<()> {
        let setup_code = r#"
import base64 as _pf_base64, cloudpickle as _pf_pickle, copy as _pf_copy, json as _pf_json, os as _pf_os, sys as _pf_sys, warnings as _pf_warnings

try:
    from IPython import get_ipython as _pf_get_ipython
    _pf_ip = _pf_get_ipython()
    if _pf_ip is not None:
        _pf_ip.run_line_magic('matplotlib', 'inline')
except Exception:
    try:
        import matplotlib as _pf_matplotlib
        _pf_matplotlib.use('module://matplotlib_inline.backend_inline')
    except Exception:
        pass

def _pf_save_artifact(obj, path):
    """Serialize any Python object to disk via cloudpickle. Returns metadata."""
    with open(path, 'wb') as f:
        _pf_pickle.dump(obj, f)
    info = {"size": _pf_os.path.getsize(path), "type": type(obj).__name__}
    # Auto-extract metrics from scalar / dict-of-scalar values
    if isinstance(obj, (int, float)) and not isinstance(obj, bool):
        info["metric_value"] = float(obj)
    elif isinstance(obj, dict):
        scalar_vals = {k: float(v) for k, v in obj.items()
                       if isinstance(v, (int, float)) and not isinstance(v, bool)}
        if scalar_vals:
            info["dict_metrics"] = scalar_vals
    return info

def _pf_load_artifact(path):
    """Deserialize a cloudpickle artifact from disk."""
    with open(path, 'rb') as f:
        return _pf_pickle.load(f)

_pf_branch_snapshots = {}

def _pf_snapshot_namespace():
    snapshot = {}
    for name, obj in list(globals().items()):
        if name.startswith('_pf_') or name.startswith('_') or callable(obj):
            continue
        entry = {"type": type(obj).__name__, "id": id(obj), "payload": None}
        try:
            payload = _pf_pickle.dumps(obj)
            entry["payload"] = _pf_base64.b64encode(payload).decode("ascii")
        except Exception:
            pass
        snapshot[name] = entry
    return snapshot

def _pf_snapshot_modules():
    module_state = {
        "sys_path": list(_pf_sys.path),
        "sys_modules": sorted(_pf_sys.modules.keys()),
        "warnings_filters": _pf_copy.deepcopy(_pf_warnings.filters),
    }
    try:
        import matplotlib as _pf_matplotlib_runtime
        module_state["matplotlib_backend"] = _pf_matplotlib_runtime.get_backend()
    except Exception:
        module_state["matplotlib_backend"] = None
    return module_state

def _pf_namespace_delta(before_namespace, after_namespace):
    before_names = set(before_namespace.keys())
    after_names = set(after_namespace.keys())
    added = sorted(after_names - before_names)
    removed = sorted(before_names - after_names)
    changed = []
    for name in sorted(before_names & after_names):
        before = before_namespace[name]
        after = after_namespace[name]
        if before.get("type") != after.get("type"):
            changed.append(name)
            continue
        before_payload = before.get("payload")
        after_payload = after.get("payload")
        if before_payload is None or after_payload is None:
            if before.get("id") != after.get("id"):
                changed.append(name)
        elif before_payload != after_payload:
            changed.append(name)
    return {
        "added": added,
        "removed": removed,
        "changed": changed,
        "module_drift": [],
    }

def _pf_restore_namespace(before_namespace, after_namespace):
    signals = []
    g = globals()
    before_names = set(before_namespace.keys())
    after_names = set(after_namespace.keys())

    for name in sorted(after_names - before_names):
        try:
            del g[name]
        except Exception:
            signals.append(f"delete:{name}")

    for name in sorted(before_names):
        before = before_namespace[name]
        current = after_namespace.get(name)
        payload = before.get("payload")
        needs_restore = current is None
        if not needs_restore and before.get("type") != current.get("type"):
            needs_restore = True
        elif not needs_restore:
            before_payload = before.get("payload")
            after_payload = current.get("payload")
            if before_payload is None or after_payload is None:
                needs_restore = before.get("id") != current.get("id")
            else:
                needs_restore = before_payload != after_payload

        if not needs_restore:
            continue
        if payload is None:
            signals.append(f"unrestorable:{name}")
            continue
        try:
            g[name] = _pf_pickle.loads(_pf_base64.b64decode(payload.encode("ascii")))
        except Exception:
            signals.append(f"restore:{name}")
    return signals

def _pf_restore_modules(before_modules):
    signals = []
    try:
        _pf_sys.path[:] = list(before_modules.get("sys_path", []))
    except Exception:
        signals.append("restore:sys.path")
    try:
        _pf_warnings.filters[:] = _pf_copy.deepcopy(before_modules.get("warnings_filters", []))
    except Exception:
        signals.append("restore:warnings.filters")
    try:
        import matplotlib as _pf_matplotlib_runtime
        backend = before_modules.get("matplotlib_backend")
        if backend:
            _pf_matplotlib_runtime.use(backend)
    except Exception:
        if before_modules.get("matplotlib_backend"):
            signals.append("restore:matplotlib_backend")
    try:
        before_sys_modules = set(before_modules.get("sys_modules", []))
        current_sys_modules = set(_pf_sys.modules.keys())
        for name in sorted(current_sys_modules - before_sys_modules):
            _pf_sys.modules.pop(name, None)
    except Exception:
        signals.append("restore:sys.modules")
    return signals

def _pf_begin_branch_session(session_id):
    global _pf_branch_snapshots
    _pf_branch_snapshots[session_id] = {
        "namespace": _pf_snapshot_namespace(),
        "modules": _pf_snapshot_modules(),
        "session_overlap": False,
    }
    if len(_pf_branch_snapshots) > 1:
        for snapshot in _pf_branch_snapshots.values():
            snapshot["session_overlap"] = True
    return True

def _pf_end_branch_session(session_id):
    global _pf_branch_snapshots
    snapshot = _pf_branch_snapshots.pop(session_id, None)
    if snapshot is None:
        return {
            "contaminated": True,
            "signals": ["missing_snapshot"],
            "delta": {"added": [], "removed": [], "changed": [], "module_drift": []},
        }

    before_namespace = snapshot.get("namespace", {})
    after_namespace = _pf_snapshot_namespace()
    delta = _pf_namespace_delta(before_namespace, after_namespace)
    before_modules = snapshot.get("modules", {})
    signals = []
    if snapshot.get("session_overlap"):
        signals.append("session_overlap")
    signals.extend(_pf_restore_namespace(before_namespace, after_namespace))
    signals.extend(_pf_restore_modules(before_modules))

    restored_namespace = _pf_snapshot_namespace()
    restored_delta = _pf_namespace_delta(before_namespace, restored_namespace)
    delta["module_drift"] = []
    if restored_delta["added"]:
        signals.append("namespace_added")
    if restored_delta["removed"]:
        signals.append("namespace_removed")
    if restored_delta["changed"]:
        signals.append("namespace_changed")
    restored_modules = _pf_snapshot_modules()
    if before_modules.get("sys_path") != restored_modules.get("sys_path"):
        delta["module_drift"].append("sys.path")
        signals.append("module_drift")
    if before_modules.get("matplotlib_backend") != restored_modules.get("matplotlib_backend"):
        delta["module_drift"].append("matplotlib_backend")
        signals.append("module_drift")
    if set(before_modules.get("sys_modules", [])) != set(restored_modules.get("sys_modules", [])):
        delta["module_drift"].append("sys.modules")
        signals.append("module_drift")
    if before_modules.get("warnings_filters") != restored_modules.get("warnings_filters"):
        delta["module_drift"].append("warnings.filters")
        signals.append("module_drift")

    return {
        "contaminated": bool(set(signals)),
        "signals": sorted(set(signals)),
        "delta": delta,
    }

def _pf_context():
    """Inspect the live kernel namespace — returns info about all user variables."""
    ctx = {}
    for name, obj in list(globals().items()):
        if name.startswith('_') or callable(obj):
            continue
        try:
            t = type(obj).__name__
            if t == 'DataFrame':
                ctx[name] = {"type": "DataFrame", "shape": list(obj.shape),
                             "columns": list(obj.columns),
                             "dtypes": {c: str(d) for c, d in obj.dtypes.items()}}
            elif isinstance(obj, (int, float, str, bool)):
                ctx[name] = {"type": t, "value": repr(obj)[:200]}
            elif isinstance(obj, dict):
                ctx[name] = {"type": "dict", "keys": list(obj.keys())[:20]}
            elif isinstance(obj, (list, tuple)):
                ctx[name] = {"type": t, "len": len(obj)}
            else:
                ctx[name] = {"type": t}
        except Exception:
            ctx[name] = {"type": "unknown"}
    return ctx
"#;
        debug!(owner = %owner_id, "sending setup code to kernel");

        let result = self
            .execute_owned_code_with_timeout(
                owner_id,
                setup_code,
                DEFAULT_EXECUTION_TIMEOUT_SECS,
            )
            .await?;
        if let Some(ref err) = result.error {
            warn!(
                owner = %owner_id,
                error = %err.evalue,
                "kernel setup code had errors (non-fatal)"
            );
        }

        debug!(owner = %owner_id, "kernel setup code injected");
        Ok(())
    }

    fn parse_stdout_json_marker<T: DeserializeOwned>(stdout: &str, marker: &str) -> TineResult<T> {
        let payload = stdout
            .lines()
            .find_map(|line| line.strip_prefix(marker))
            .ok_or_else(|| TineError::KernelComm(format!("missing marker '{}'", marker)))?;
        serde_json::from_str(payload).map_err(|e| {
            TineError::KernelComm(format!("failed to parse kernel JSON payload: {}", e))
        })
    }
}

// ---------------------------------------------------------------------------
// Media conversion helper
// ---------------------------------------------------------------------------

/// Convert jupyter_protocol Media bundles to a simple HashMap.
fn media_to_map(media: &jupyter_protocol::Media) -> HashMap<String, String> {
    let mut map = HashMap::new();
    if let Ok(val) = serde_json::to_value(media) {
        if let Some(obj) = val.as_object() {
            for (k, v) in obj {
                let text = match v {
                    serde_json::Value::String(s) => s.clone(),
                    other => other.to_string(),
                };
                map.insert(k.clone(), text);
            }
        }
    }
    map
}

// ---------------------------------------------------------------------------
// Result types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct KernelExecutionResult {
    pub stdout: String,
    pub stderr: String,
    pub outputs: Vec<NodeOutput>,
    pub error: Option<KernelExecutionError>,
    /// Duration in milliseconds (set by the scheduler, not the kernel itself).
    pub duration_ms: u64,
}

#[derive(Debug, Clone)]
pub struct KernelExecutionError {
    pub ename: String,
    pub evalue: String,
    pub traceback: Vec<String>,
}

/// Check if a process with the given PID is alive.
fn process_alive(pid: u32) -> bool {
    #[cfg(unix)]
    {
        unsafe { libc::kill(pid as i32, 0) == 0 }
    }
    #[cfg(not(unix))]
    {
        false
    }
}

/// Get the RSS (Resident Set Size) of a process in bytes.
/// Returns `None` if the process doesn't exist or the info is unavailable.
fn process_rss_bytes(pid: u32) -> Option<u64> {
    #[cfg(target_os = "macos")]
    {
        let output = std::process::Command::new("ps")
            .args(["-o", "rss=", "-p", &pid.to_string()])
            .output()
            .ok()?;
        let text = String::from_utf8_lossy(&output.stdout);
        let kb: u64 = text.trim().parse().ok()?;
        Some(kb * 1024)
    }
    #[cfg(target_os = "linux")]
    {
        let status_path = format!("/proc/{}/status", pid);
        let content = std::fs::read_to_string(&status_path).ok()?;
        for line in content.lines() {
            if let Some(rest) = line.strip_prefix("VmRSS:") {
                let kb_str = rest.trim().trim_end_matches(" kB").trim();
                let kb: u64 = kb_str.parse().ok()?;
                return Some(kb * 1024);
            }
        }
        None
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        let _ = pid;
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::{env, path::Path};

    use serial_test::serial;
    use tempfile::TempDir;
    use tine_core::EnvironmentSpec;
    use tine_env::{EnvironmentManager, TreeEnvironmentDescriptor};

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn test_concurrent_tree_kernel_starts_share_single_owned_kernel() {
        let tmp = TempDir::new().expect("failed to create temp dir");
        let tree_id = ExperimentTreeId::new("kernel-start-race");
        let descriptor =
            TreeEnvironmentDescriptor::new(tree_id.clone(), None, EnvironmentSpec::default());
        let env_mgr = EnvironmentManager::new(tmp.path().to_path_buf());
        let venv_dir = env_mgr
            .ensure_tree_environment(&descriptor)
            .await
            .expect("failed to prepare tree environment");
        let kernel_mgr = Arc::new(KernelManager::new(tmp.path(), 4));
        let working_dir = tmp.path().to_path_buf();

        let mut tasks = Vec::new();
        for _ in 0..4 {
            let kernel_mgr = Arc::clone(&kernel_mgr);
            let tree_id = tree_id.clone();
            let venv_dir = venv_dir.clone();
            let working_dir = working_dir.clone();
            tasks.push(tokio::spawn(async move {
                kernel_mgr
                    .start_tree_kernel(&tree_id, &venv_dir, &working_dir)
                    .await
            }));
        }

        for task in tasks {
            task.await
                .expect("kernel start task panicked")
                .expect("kernel start failed");
        }

        assert!(kernel_mgr.has_tree_kernel(&tree_id));
        assert_eq!(kernel_mgr.kernels.len(), 1);
        assert_eq!(kernel_mgr.kernel_pids.len(), 1);

        let result = kernel_mgr
            .execute_tree_code(&tree_id, "print('kernel-start-race-ok', flush=True)")
            .await
            .expect("failed to execute test code in shared tree kernel");
        assert!(result.stdout.contains("kernel-start-race-ok"));

        kernel_mgr
            .shutdown_tree(&tree_id)
            .await
            .expect("failed to shut down shared tree kernel");
        assert!(!kernel_mgr.has_tree_kernel(&tree_id));
        assert!(kernel_mgr.connection_info_for_tree(&tree_id).is_none());
        assert_eq!(kernel_mgr.kernels.len(), 0);
        assert_eq!(kernel_mgr.kernel_pids.len(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_overlapping_branch_sessions_use_distinct_snapshots() {
        let tmp = TempDir::new().expect("failed to create temp dir");
        let tree_id = ExperimentTreeId::new("branch-session-overlap");
        let descriptor =
            TreeEnvironmentDescriptor::new(tree_id.clone(), None, EnvironmentSpec::default());
        let env_mgr = EnvironmentManager::new(tmp.path().to_path_buf());
        let venv_dir = env_mgr
            .ensure_tree_environment(&descriptor)
            .await
            .expect("failed to prepare tree environment");
        let kernel_mgr = KernelManager::new(tmp.path(), 2);
        kernel_mgr
            .start_tree_kernel(&tree_id, &venv_dir, tmp.path())
            .await
            .expect("failed to start tree kernel");

        kernel_mgr
            .begin_tree_branch_session(&tree_id, "session-a")
            .await
            .expect("failed to begin session a");
        kernel_mgr
            .execute_tree_code(&tree_id, "alpha = 1")
            .await
            .expect("failed to mutate kernel for session a");

        kernel_mgr
            .begin_tree_branch_session(&tree_id, "session-b")
            .await
            .expect("failed to begin session b");
        kernel_mgr
            .execute_tree_code(&tree_id, "beta = alpha + 1")
            .await
            .expect("failed to mutate kernel for session b");

        let outcome_a = kernel_mgr
            .end_tree_branch_session(&tree_id, "session-a")
            .await
            .expect("failed to end session a");
        let outcome_b = kernel_mgr
            .end_tree_branch_session(&tree_id, "session-b")
            .await
            .expect("failed to end session b");

        assert!(
            !outcome_a
                .signals
                .iter()
                .any(|signal| signal == "missing_snapshot"),
            "session a unexpectedly lost its snapshot: {:?}",
            outcome_a.signals
        );
        assert!(
            !outcome_b
                .signals
                .iter()
                .any(|signal| signal == "missing_snapshot"),
            "session b unexpectedly lost its snapshot: {:?}",
            outcome_b.signals
        );
        assert!(
            outcome_a
                .signals
                .iter()
                .chain(outcome_b.signals.iter())
                .any(|signal| signal == "session_overlap"),
            "expected overlapping sessions to record a session_overlap signal"
        );

        kernel_mgr
            .shutdown_tree(&tree_id)
            .await
            .expect("failed to shut down tree kernel");
    }

    #[test]
    #[serial]
    fn kernel_manager_normalizes_relative_workspace_root() {
        let tmp = TempDir::new().expect("failed to create temp dir");
        let original_dir = env::current_dir().expect("read current dir");
        env::set_current_dir(tmp.path()).expect("set current dir");

        let kernel_mgr = KernelManager::new(Path::new("."), 1);
        let expected_root = std::fs::canonicalize(tmp.path()).expect("canonicalize temp dir");

        env::set_current_dir(original_dir).expect("restore current dir");

        assert!(
            kernel_mgr.work_dir.is_absolute(),
            "expected absolute work dir, got {}",
            kernel_mgr.work_dir.display()
        );
        assert_eq!(
            kernel_mgr.work_dir,
            expected_root.join(".tine").join("kernels")
        );
    }
}
