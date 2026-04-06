use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use sqlx::sqlite::SqlitePool;
use tokio::sync::broadcast;
use tracing::{debug, error, info, warn};

use tine_catalog::DataCatalog;
use tine_core::{
    ArtifactKey, ExecutableTreeBranch, ExecutableTreeCell, ExecutionEvent, ExecutionId,
    ExecutionMode, ExecutionOutcome, ExecutionTargetKind, ExecutionTargetRef, ExperimentTreeId,
    NodeCacheKey, NodeError, NodeId, NodeLogs, NodeStatus, SlotName, TineError, TineResult,
};
use tine_env::{EnvironmentManager, TreeEnvironmentDescriptor};
use tine_graph::ExecutableTreeGraph;
use tine_kernel::{KernelExecutionResult, KernelManager, DEFAULT_EXECUTION_TIMEOUT_SECS};

// Metric name constants (matching tine-observe)
const M_NODES_EXECUTED: &str = "tine_nodes_executed_total";
const M_NODES_CACHE_HIT: &str = "tine_nodes_cache_hit_total";
const M_NODES_FAILED: &str = "tine_nodes_failed_total";
const M_PIPELINES_EXECUTED: &str = "tine_pipelines_executed_total";
const M_EXECUTION_DURATION: &str = "tine_execution_duration_seconds";

// ---------------------------------------------------------------------------
// Scheduler — the DAG executor
// ---------------------------------------------------------------------------

/// Orchestrates executable-branch execution: environment setup, kernel management,
/// DAG traversal, caching, and event streaming.
///
/// The scheduler is tree-native end to end: executable branches/cells are the
/// canonical runtime input shape, while kernels remain owned 1:1 by tree id.
pub struct Scheduler {
    kernel_mgr: Arc<KernelManager>,
    env_mgr: Arc<EnvironmentManager>,
    catalog: Arc<DataCatalog>,
    #[allow(dead_code)]
    workspace_root: PathBuf,
    /// Event broadcaster for streaming execution events.
    event_tx: broadcast::Sender<ExecutionEvent>,
    /// Exclusive lock for sequential-mode tree executions.
    /// Parallel executions read-lock; sequential executions write-lock.
    exec_lock: Arc<tokio::sync::RwLock<()>>,
}

#[derive(Debug, Clone)]
struct SchedulerExecutionTarget {
    tree_id: ExperimentTreeId,
    branch_id: Option<tine_core::BranchId>,
    target_kind: ExecutionTargetKind,
    target: ExecutionTargetRef,
}

impl Scheduler {
    pub fn new(
        kernel_mgr: Arc<KernelManager>,
        env_mgr: Arc<EnvironmentManager>,
        catalog: Arc<DataCatalog>,
        workspace_root: PathBuf,
    ) -> Self {
        let (event_tx, _) = broadcast::channel(1024);
        Self {
            kernel_mgr,
            env_mgr,
            catalog,
            workspace_root,
            event_tx,
            exec_lock: Arc::new(tokio::sync::RwLock::new(())),
        }
    }

    /// Subscribe to execution events.
    pub fn subscribe(&self) -> broadcast::Receiver<ExecutionEvent> {
        self.event_tx.subscribe()
    }

    /// Get a clone of the event sender for broadcasting external events.
    pub fn event_sender(&self) -> broadcast::Sender<ExecutionEvent> {
        self.event_tx.clone()
    }

    fn scheduler_target(target: &ExecutionTargetRef) -> SchedulerExecutionTarget {
        let ExecutionTargetRef::ExperimentTreeBranch { tree_id, branch_id } = target;
        SchedulerExecutionTarget {
            tree_id: tree_id.clone(),
            branch_id: Some(branch_id.clone()),
            target_kind: ExecutionTargetKind::ExperimentTreeBranch,
            target: target.clone(),
        }
    }

    fn tree_environment(branch: &ExecutableTreeBranch) -> TreeEnvironmentDescriptor {
        TreeEnvironmentDescriptor::new(
            branch.tree_id.clone(),
            branch.project_id.clone(),
            branch.environment.clone(),
        )
    }

    fn node_id_for_cell(cell: &ExecutableTreeCell) -> NodeId {
        NodeId::new(cell.cell_id.as_str())
    }

    fn branch_runtime_id(branch: &ExecutableTreeBranch) -> String {
        format!("{}::{}", branch.tree_id.as_str(), branch.branch_id.as_str())
    }

    fn cell_by_node_id<'a>(
        branch: &'a ExecutableTreeBranch,
        node_id: &NodeId,
    ) -> &'a ExecutableTreeCell {
        branch
            .cells
            .iter()
            .find(|cell| cell.cell_id.as_str() == node_id.as_str())
            .unwrap()
    }

    pub async fn execute_executable_cell_for_target(
        &self,
        branch: &ExecutableTreeBranch,
        cell: &ExecutableTreeCell,
        target: &ExecutionTargetRef,
        working_dir: Option<&Path>,
    ) -> TineResult<(ExecutionId, NodeLogs)> {
        self.execute_single_cell_for_target(branch, cell, target, working_dir)
            .await
    }

    pub async fn execute_executable_branch_for_target(
        &self,
        execution_id: &ExecutionId,
        branch: &ExecutableTreeBranch,
        target: &ExecutionTargetRef,
        cache: &HashMap<NodeCacheKey, HashMap<SlotName, ArtifactKey>>,
        pool: Option<&SqlitePool>,
        working_dir: Option<&Path>,
    ) -> TineResult<ExecutionOutcome> {
        self.execute_branch_for_target(execution_id, branch, target, cache, pool, working_dir)
            .await
    }

    pub async fn execute_single_cell_for_target(
        &self,
        branch: &ExecutableTreeBranch,
        cell: &ExecutableTreeCell,
        target: &ExecutionTargetRef,
        working_dir: Option<&Path>,
    ) -> TineResult<(ExecutionId, NodeLogs)> {
        let execution_id = ExecutionId::generate();
        let effective_working_dir = working_dir.unwrap_or(self.workspace_root.as_path());

        eprintln!(
            "[scheduler] single-cell execution={} tree={} branch={} cell={} target={:?} cwd={}",
            execution_id.as_str(),
            branch.tree_id.as_str(),
            branch.branch_id.as_str(),
            cell.cell_id.as_str(),
            target,
            effective_working_dir.display()
        );

        // Ensure environment is ready
        eprintln!(
            "[scheduler] single-cell execution={} ensuring environment for tree={}",
            execution_id.as_str(),
            branch.tree_id.as_str()
        );
        let venv_dir = self
            .env_mgr
            .ensure_tree_environment(&Self::tree_environment(branch))
            .await?;
        eprintln!(
            "[scheduler] single-cell execution={} environment ready venv={}",
            execution_id.as_str(),
            venv_dir.display()
        );

        // Start kernel if needed
        if !self.kernel_mgr.has_tree_kernel(&branch.tree_id) {
            eprintln!(
                "[scheduler] single-cell execution={} starting kernel for tree={}",
                execution_id.as_str(),
                branch.tree_id.as_str()
            );
            self.kernel_mgr
                .start_tree_kernel(&branch.tree_id, &venv_dir, effective_working_dir)
                .await?;
            eprintln!(
                "[scheduler] single-cell execution={} kernel ready for tree={}",
                execution_id.as_str(),
                branch.tree_id.as_str()
            );
        } else {
            eprintln!(
                "[scheduler] single-cell execution={} reusing kernel for tree={}",
                execution_id.as_str(),
                branch.tree_id.as_str()
            );
        }

        eprintln!(
            "[scheduler] single-cell execution={} executing cell {}",
            execution_id.as_str(),
            cell.cell_id.as_str()
        );
        let (result, _artifacts, _metrics) = execute_cell(
            &self.kernel_mgr,
            branch,
            &execution_id,
            cell,
            target,
            &self.event_tx,
            &self.catalog,
        )
        .await?;
        eprintln!(
            "[scheduler] single-cell execution={} finished cell={} duration_ms={} error_present={}",
            execution_id.as_str(),
            cell.cell_id.as_str(),
            result.duration_ms,
            result.error.is_some()
        );

        let logs = NodeLogs {
            stdout: result.stdout.clone(),
            stderr: result.stderr.clone(),
            error: result.error.clone().map(|e| NodeError {
                ename: e.ename,
                evalue: e.evalue,
                traceback: e.traceback,
                hints: Vec::new(),
            }),
            outputs: result.outputs.clone(),
            metrics: HashMap::new(),
            duration_ms: Some(result.duration_ms),
        };

        Ok((execution_id, logs))
    }

    pub async fn execute_branch_for_target(
        &self,
        execution_id: &ExecutionId,
        branch: &ExecutableTreeBranch,
        target: &ExecutionTargetRef,
        cache: &HashMap<NodeCacheKey, HashMap<SlotName, ArtifactKey>>,
        pool: Option<&SqlitePool>,
        working_dir: Option<&Path>,
    ) -> TineResult<ExecutionOutcome> {
        // Acquire execution lock based on mode:
        // - Parallel: read lock (multiple can run concurrently)
        // - Sequential: write lock (exclusive, waits for all others to finish)
        let _read_guard;
        let _write_guard;
        match branch.execution_mode {
            ExecutionMode::Parallel => {
                _read_guard = Some(self.exec_lock.read().await);
                _write_guard = None;
            }
            ExecutionMode::Sequential => {
                _read_guard = None;
                _write_guard = Some(self.exec_lock.write().await);
            }
        }

        let start = Instant::now();
        let runtime_target = Self::scheduler_target(target);
        let tree_id = runtime_target.tree_id.clone();

        info!(
            execution = %execution_id,
            tree = %tree_id,
            branch = %branch.branch_id,
            cells = branch.cells.len(),
            "starting branch execution"
        );

        self.emit(ExecutionEvent::ExecutionStarted {
            execution_id: execution_id.clone(),
            tree_id: Some(runtime_target.tree_id.clone()),
            branch_id: runtime_target.branch_id.clone(),
            target_kind: Some(runtime_target.target_kind.clone()),
            target: Some(runtime_target.target.clone()),
        });

        // Build and validate graph
        let graph = ExecutableTreeGraph::from_branch(branch)?;
        graph.validate(&Self::branch_runtime_id(branch))?;

        // Compute lockfile hash
        let lockfile_hash = self
            .env_mgr
            .lockfile_hash_for_tree(&Self::tree_environment(branch))
            .await?;

        // Plan execution (cache check)
        let (to_execute, to_skip) = graph.plan_execution(branch, cache, lockfile_hash);

        info!(
            execution = %execution_id,
            execute = to_execute.len(),
            skip = to_skip.len(),
            "execution plan ready"
        );

        // Ensure environment is ready
        eprintln!("[scheduler] ensuring environment...");
        let venv_dir = self
            .env_mgr
            .ensure_tree_environment(&Self::tree_environment(branch))
            .await?;
        eprintln!("[scheduler] environment ready at {}", venv_dir.display());

        // Start kernel if needed
        if !self.kernel_mgr.has_tree_kernel(&tree_id) {
            eprintln!(
                "[scheduler] starting kernel for tree={} branch={}",
                branch.tree_id, branch.branch_id
            );
            self.kernel_mgr
                .start_tree_kernel(
                    &tree_id,
                    &venv_dir,
                    working_dir.unwrap_or(self.workspace_root.as_path()),
                )
                .await?;
            eprintln!("[scheduler] kernel started");
        }

        // Track per-node artifact keys (populated by execution or cache)
        let mut node_artifacts: HashMap<NodeId, HashMap<SlotName, ArtifactKey>> = HashMap::new();

        // Inject cached artifacts for skipped (cache-hit) nodes
        for node_id in &to_skip {
            let cell = Self::cell_by_node_id(branch, node_id);

            // Find the matching cache entry for this node
            let code_hash = NodeCacheKey::hash_code(&cell.code.source);
            let matching_entry = cache.iter().find(|(k, _)| k.code_hash == code_hash);

            if let Some((_, artifacts)) = matching_entry {
                // Inject each output artifact into the kernel namespace via _pf_load_artifact
                for slot in &cell.outputs {
                    if let Some(artifact_key) = artifacts.get(slot) {
                        if let Some(path) = self.catalog.get_path(artifact_key) {
                            let inject_code = format!(
                                "{} = _pf_load_artifact('{}')",
                                slot.as_str(),
                                path.display()
                            );
                            debug!(
                                node = %node_id,
                                slot = %slot,
                                artifact = %artifact_key,
                                "injecting cached artifact"
                            );
                            let _ = self
                                .kernel_mgr
                                .execute_tree_code(&tree_id, &inject_code)
                                .await;
                        }
                    }
                }
                node_artifacts.insert(node_id.clone(), artifacts.clone());
            }

            metrics::counter!(M_NODES_CACHE_HIT).increment(1);
            self.emit(ExecutionEvent::NodeCacheHit {
                execution_id: execution_id.clone(),
                node_id: node_id.clone(),
                tree_id: Some(tree_id.clone()),
                branch_id: runtime_target.branch_id.clone(),
                target_kind: Some(runtime_target.target_kind.clone()),
                target: Some(runtime_target.target.clone()),
                artifacts: node_artifacts.get(node_id).cloned().unwrap_or_default(),
            });
        }

        // Execute nodes in topological order with parallelism
        let mut completed: HashSet<NodeId> = to_skip.into_iter().collect();
        let mut failed_nodes = Vec::new();
        let mut node_logs: HashMap<NodeId, NodeLogs> = HashMap::new();
        let mut node_statuses: HashMap<NodeId, NodeStatus> = HashMap::new();

        for nid in &completed {
            node_statuses.insert(nid.clone(), NodeStatus::CacheHit);
        }

        loop {
            let ready = graph.ready_nodes(&completed);
            let ready: Vec<NodeId> = ready
                .into_iter()
                .filter(|n| to_execute.contains(n))
                .filter(|n| !completed.contains(n))
                .collect();

            if ready.is_empty() {
                break;
            }

            // Execute ready nodes (in parallel via tokio::spawn)
            let mut handles = Vec::new();
            for node_id in &ready {
                let cell = Self::cell_by_node_id(branch, node_id).clone();

                // Handle map_over nodes
                if let Some(ref map_slot) = cell.map_over {
                    // Resolve the collection from the kernel namespace
                    let collection = self
                        .resolve_map_collection(&tree_id, map_slot)
                        .await
                        .unwrap_or_else(|_| vec![serde_json::Value::Null]);
                    let map_results = self
                        .execute_map_node(
                            branch,
                            &cell,
                            collection,
                            &execution_id,
                            working_dir.unwrap_or(self.workspace_root.as_path()),
                        )
                        .await;
                    match map_results {
                        Ok(results) => {
                            let mut combined_stdout = String::new();
                            let mut combined_outputs = Vec::new();
                            for r in &results {
                                combined_stdout.push_str(&r.stdout);
                                combined_outputs.extend(r.outputs.clone());
                            }
                            node_logs.insert(
                                node_id.clone(),
                                NodeLogs {
                                    stdout: combined_stdout,
                                    stderr: String::new(),
                                    outputs: combined_outputs,
                                    error: None,
                                    duration_ms: Some(0),
                                    metrics: HashMap::new(),
                                },
                            );
                            node_statuses.insert(node_id.clone(), NodeStatus::Completed);
                            completed.insert(node_id.clone());
                        }
                        Err(e) => {
                            error!(node = %node_id, error = %e, "map node failed");
                            node_statuses.insert(node_id.clone(), NodeStatus::Failed);
                            failed_nodes.push(node_id.clone());
                        }
                    }
                    continue;
                }

                let exec_id = execution_id.clone();
                let kernel_mgr = self.kernel_mgr.clone();
                let branch_def = branch.clone();
                let cell_def = cell.clone();
                let event_tx = self.event_tx.clone();
                let catalog = self.catalog.clone();
                let node_target = target.clone();
                handles.push(tokio::spawn(async move {
                    execute_cell(
                        &kernel_mgr,
                        &branch_def,
                        &exec_id,
                        &cell_def,
                        &node_target,
                        &event_tx,
                        &catalog,
                    )
                    .await
                }));
            }

            // Collect results
            for (i, handle) in handles.into_iter().enumerate() {
                let node_id = &ready[i];
                match handle.await {
                    Ok(Ok((result, artifacts, extracted_metrics))) => {
                        let has_error = result.error.is_some();

                        node_logs.insert(
                            node_id.clone(),
                            NodeLogs {
                                stdout: result.stdout.clone(),
                                stderr: result.stderr,
                                outputs: result.outputs,
                                error: result.error.map(|e| NodeError {
                                    ename: e.ename,
                                    evalue: e.evalue,
                                    traceback: e.traceback,
                                    hints: Vec::new(),
                                }),
                                duration_ms: Some(result.duration_ms),
                                metrics: extracted_metrics,
                            },
                        );

                        if has_error {
                            // On error: introspect kernel context for hints
                            let hints = introspect_error_context(
                                &self.kernel_mgr,
                                &tree_id,
                                node_logs.get(node_id).unwrap(),
                            )
                            .await;

                            if !hints.is_empty() {
                                if let Some(logs) = node_logs.get_mut(node_id) {
                                    if let Some(ref mut err) = logs.error {
                                        err.hints = hints;
                                    }
                                }
                            }

                            node_statuses.insert(node_id.clone(), NodeStatus::Failed);
                            failed_nodes.push(node_id.clone());
                        } else {
                            // Store artifacts in tracker
                            if !artifacts.is_empty() {
                                node_artifacts.insert(node_id.clone(), artifacts.clone());
                            }

                            // Write cache entry if pool is available
                            if let Some(pool) = pool {
                                let _ = write_cache_entry(
                                    pool,
                                    Self::cell_by_node_id(branch, node_id),
                                    &Self::branch_runtime_id(branch),
                                    lockfile_hash,
                                    &node_artifacts,
                                    &artifacts,
                                )
                                .await;
                            }

                            node_statuses.insert(node_id.clone(), NodeStatus::Completed);
                            completed.insert(node_id.clone());
                        }
                    }
                    Ok(Err(e)) => {
                        error!(node = %node_id, error = %e, "node execution failed");
                        metrics::counter!(M_NODES_FAILED).increment(1);
                        self.emit(ExecutionEvent::NodeFailed {
                            execution_id: execution_id.clone(),
                            node_id: node_id.clone(),
                            tree_id: Some(tree_id.clone()),
                            branch_id: runtime_target.branch_id.clone(),
                            target_kind: Some(runtime_target.target_kind.clone()),
                            target: Some(runtime_target.target.clone()),
                            error: NodeError {
                                ename: "ExecutionError".to_string(),
                                evalue: e.to_string(),
                                traceback: Vec::new(),
                                hints: Vec::new(),
                            },
                        });
                        node_statuses.insert(node_id.clone(), NodeStatus::Failed);
                        node_logs.insert(
                            node_id.clone(),
                            NodeLogs {
                                stdout: String::new(),
                                stderr: e.to_string(),
                                outputs: Vec::new(),
                                error: Some(NodeError {
                                    ename: "ExecutionError".to_string(),
                                    evalue: e.to_string(),
                                    traceback: Vec::new(),
                                    hints: Vec::new(),
                                }),
                                duration_ms: None,
                                metrics: HashMap::new(),
                            },
                        );
                        failed_nodes.push(node_id.clone());
                    }
                    Err(e) => {
                        error!(node = %node_id, error = %e, "task panicked");
                        node_statuses.insert(node_id.clone(), NodeStatus::Failed);
                        failed_nodes.push(node_id.clone());
                    }
                }
            }

            if !failed_nodes.is_empty() {
                break;
            }
        }

        if !failed_nodes.is_empty() {
            for cell_id in &branch.topo_order {
                node_statuses
                    .entry(NodeId::new(cell_id.as_str()))
                    .or_insert(NodeStatus::Skipped);
            }
        }

        let duration_ms = start.elapsed().as_millis() as u64;

        if failed_nodes.is_empty() {
            info!(
                execution = %execution_id,
                tree = %branch.tree_id,
                branch = %branch.branch_id,
                duration_ms = duration_ms,
                "branch completed successfully"
            );
            metrics::counter!(M_PIPELINES_EXECUTED).increment(1);
            metrics::histogram!(M_EXECUTION_DURATION).record(duration_ms as f64 / 1000.0);
            self.emit(ExecutionEvent::ExecutionCompleted {
                execution_id: execution_id.clone(),
                tree_id: Some(tree_id.clone()),
                branch_id: runtime_target.branch_id.clone(),
                target_kind: Some(runtime_target.target_kind.clone()),
                target: Some(runtime_target.target.clone()),
                duration_ms,
            });
        } else {
            warn!(
                execution = %execution_id,
                tree = %branch.tree_id,
                branch = %branch.branch_id,
                failed = ?failed_nodes,
                "branch failed"
            );
            self.emit(ExecutionEvent::ExecutionFailed {
                execution_id: execution_id.clone(),
                tree_id: Some(tree_id.clone()),
                branch_id: runtime_target.branch_id.clone(),
                target_kind: Some(runtime_target.target_kind.clone()),
                target: Some(runtime_target.target.clone()),
                failed_nodes: failed_nodes.clone(),
            });
        }

        Ok(ExecutionOutcome {
            execution_id: execution_id.clone(),
            tree_id: Some(tree_id),
            branch_id: runtime_target.branch_id,
            target_kind: Some(runtime_target.target_kind),
            target: Some(runtime_target.target),
            node_logs,
            node_statuses,
            failed_nodes,
            duration_ms,
        })
    }

    /// Resolve the map_over collection from the kernel namespace.
    ///
    /// Evaluates `json.dumps(list(<slot>))` in the tree kernel to get the
    /// iterable as a JSON array, then deserializes it into `Vec<Value>`.
    async fn resolve_map_collection(
        &self,
        tree_id: &ExperimentTreeId,
        map_slot: &SlotName,
    ) -> TineResult<Vec<serde_json::Value>> {
        let code = format!(
            "import json as _j; print('__TINE_MAP__' + _j.dumps(list({})))",
            map_slot.as_str()
        );
        let result = self.kernel_mgr.execute_tree_code(tree_id, &code).await?;
        for line in result.stdout.lines() {
            if let Some(json_str) = line.strip_prefix("__TINE_MAP__") {
                if let Ok(arr) = serde_json::from_str::<Vec<serde_json::Value>>(json_str) {
                    return Ok(arr);
                }
            }
        }
        Err(TineError::Internal(format!(
            "failed to resolve map_over collection from slot '{}'",
            map_slot
        )))
    }

    /// Execute a mapped cell across collection elements in parallel.
    pub async fn execute_map_node(
        &self,
        branch: &ExecutableTreeBranch,
        cell: &ExecutableTreeCell,
        collection: Vec<serde_json::Value>,
        execution_id: &ExecutionId,
        working_dir: &Path,
    ) -> TineResult<Vec<KernelExecutionResult>> {
        let concurrency = cell.map_concurrency.unwrap_or(4);
        let venv_dir = self
            .env_mgr
            .ensure_tree_environment(&Self::tree_environment(branch))
            .await?;

        info!(
            execution = %execution_id,
            node = %Self::node_id_for_cell(cell),
            elements = collection.len(),
            concurrency = concurrency,
            "executing map node"
        );

        let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency));
        let mut handles = Vec::new();

        for (i, element) in collection.into_iter().enumerate() {
            let sem = semaphore.clone();
            let kernel_mgr = self.kernel_mgr.clone();
            let worker_id = format!(
                "{}::map::{}::{}",
                branch.tree_id.as_str(),
                branch.branch_id.as_str(),
                i
            );
            let node_code = cell.code.source.clone();
            let venv = venv_dir.clone();
            let worker_dir = working_dir.to_path_buf();

            handles.push(tokio::spawn(async move {
                let _permit = sem.acquire().await.unwrap();

                // Start ephemeral kernel
                kernel_mgr
                    .start_worker_kernel(&worker_id, &venv, &worker_dir)
                    .await?;

                // Inject element and execute
                let setup_code = format!(
                    "_pf_element = {}\n{}",
                    serde_json::to_string(&element).unwrap_or_default(),
                    node_code
                );

                let result = kernel_mgr
                    .execute_worker_code(&worker_id, &setup_code)
                    .await;

                // Shutdown ephemeral kernel
                kernel_mgr.shutdown_worker_kernel(&worker_id).await?;

                result
            }));
        }

        let mut results = Vec::new();
        for handle in handles {
            match handle.await {
                Ok(Ok(result)) => results.push(result),
                Ok(Err(e)) => {
                    return Err(TineError::NodeExecutionFailed {
                        node_id: Self::node_id_for_cell(cell),
                        message: format!("map worker failed: {}", e),
                    });
                }
                Err(e) => {
                    return Err(TineError::Internal(format!("map worker panicked: {}", e)));
                }
            }
        }

        Ok(results)
    }

    /// Check for runtime dependencies after node execution.
    pub async fn check_runtime_deps(&self, work_dir: &Path) -> TineResult<Vec<String>> {
        let runtime_deps_path = work_dir.join("_runtime_deps.json");
        if runtime_deps_path.exists() {
            let data = tokio::fs::read_to_string(&runtime_deps_path).await?;
            let deps: Vec<String> = serde_json::from_str(&data)?;
            Ok(deps)
        } else {
            Ok(Vec::new())
        }
    }

    fn emit(&self, event: ExecutionEvent) {
        let _ = self.event_tx.send(event);
    }
}

// ---------------------------------------------------------------------------
// execute_cell — runs one cell, then serializes outputs to .pkl for cache
// ---------------------------------------------------------------------------

/// Execute a single node in a kernel, then serialize each output slot via
/// cloudpickle for caching.  Node→node data flows through the shared kernel
/// namespace — serialization is ONLY for the cache cold path.
/// Returns the kernel result, artifact map, AND auto-extracted metrics.
async fn execute_cell(
    kernel_mgr: &KernelManager,
    branch: &ExecutableTreeBranch,
    execution_id: &ExecutionId,
    cell: &ExecutableTreeCell,
    target: &ExecutionTargetRef,
    event_tx: &broadcast::Sender<ExecutionEvent>,
    catalog: &DataCatalog,
) -> TineResult<(
    KernelExecutionResult,
    HashMap<SlotName, ArtifactKey>,
    HashMap<String, f64>,
)> {
    let branch_id = match target {
        ExecutionTargetRef::ExperimentTreeBranch { branch_id, .. } => Some(branch_id.clone()),
    };
    let target_kind = ExecutionTargetKind::ExperimentTreeBranch;
    let _ = event_tx.send(ExecutionEvent::NodeStarted {
        execution_id: execution_id.clone(),
        node_id: Scheduler::node_id_for_cell(cell),
        tree_id: Some(branch.tree_id.clone()),
        branch_id: branch_id.clone(),
        target_kind: Some(target_kind.clone()),
        target: Some(target.clone()),
    });

    let node_start = Instant::now();

    // Execute the node's code (with per-node timeout if set)
    // On communication failure, attempt one kernel restart and retry.
    let timeout_secs = cell.timeout_secs.unwrap_or(DEFAULT_EXECUTION_TIMEOUT_SECS);
    let stream_tx = event_tx.clone();
    let stream_execution_id = execution_id.clone();
    let stream_node_id = Scheduler::node_id_for_cell(cell);
    let stream_tree_id = branch.tree_id.clone();
    let stream_branch_id = branch_id.clone();
    let stream_target_kind = target_kind.clone();
    let stream_target = target.clone();
    let mut emit_live_stream = move |stream: &str, text: &str| {
        if text.is_empty() {
            return;
        }
        let _ = stream_tx.send(ExecutionEvent::NodeStream {
            execution_id: stream_execution_id.clone(),
            node_id: stream_node_id.clone(),
            tree_id: Some(stream_tree_id.clone()),
            branch_id: stream_branch_id.clone(),
            target_kind: Some(stream_target_kind.clone()),
            target: Some(stream_target.clone()),
            stream: stream.to_string(),
            text: text.to_string(),
        });
    };
    let mut result = match kernel_mgr
        .execute_tree_code_with_timeout_and_stream(
            &branch.tree_id,
            &cell.code.source,
            timeout_secs,
            &mut emit_live_stream,
        )
        .await
    {
        Ok(r) => r,
        Err(tine_core::TineError::KernelComm(_)) => {
            warn!(
                node = %Scheduler::node_id_for_cell(cell),
                "kernel communication failed, attempting restart and retry"
            );
            kernel_mgr.restart_tree_kernel(&branch.tree_id).await?;
            kernel_mgr
                .execute_tree_code_with_timeout_and_stream(
                    &branch.tree_id,
                    &cell.code.source,
                    timeout_secs,
                    &mut emit_live_stream,
                )
                .await?
        }
        Err(e) => return Err(e),
    };

    // Measure per-node duration
    let duration_ms = node_start.elapsed().as_millis() as u64;
    result.duration_ms = duration_ms;

    if let Some(ref err) = result.error {
        let _ = event_tx.send(ExecutionEvent::NodeFailed {
            execution_id: execution_id.clone(),
            node_id: Scheduler::node_id_for_cell(cell),
            tree_id: Some(branch.tree_id.clone()),
            branch_id: branch_id.clone(),
            target_kind: Some(target_kind.clone()),
            target: Some(target.clone()),
            error: NodeError {
                ename: err.ename.clone(),
                evalue: err.evalue.clone(),
                traceback: err.traceback.clone(),
                hints: Vec::new(),
            },
        });
        return Ok((result, HashMap::new(), HashMap::new()));
    }

    // Serialize each output slot to .pkl via cloudpickle (for cache only).
    // Node→node data flows through the shared kernel namespace — this is
    // ONLY for persisting to the content-addressed cache.
    let mut artifacts = HashMap::new();
    let mut node_metrics: HashMap<String, f64> = HashMap::new();
    let artifact_dir = catalog.artifact_dir();
    let runtime_id = Scheduler::branch_runtime_id(branch);

    for slot in &cell.outputs {
        let artifact_path =
            artifact_dir.join(format!("{}-{}-{}.pkl", runtime_id, cell.cell_id, slot));

        let save_code = format!(
            r#"
try:
    _pf_out = _pf_save_artifact({slot}, '{path}')
    print("__TINE_ARTIFACT__" + _pf_json.dumps({{"slot": "{slot}", "meta": _pf_out}}))
except Exception as _pf_err:
    print("__TINE_ARTIFACT_ERR__" + str(_pf_err))
"#,
            slot = slot.as_str(),
            path = artifact_path.display(),
        );

        let save_result = kernel_mgr
            .execute_tree_code(&branch.tree_id, &save_code)
            .await?;

        // Parse the artifact metadata from stdout
        for line in save_result.stdout.lines() {
            if let Some(json_str) = line.strip_prefix("__TINE_ARTIFACT__") {
                if let Ok(meta) = serde_json::from_str::<serde_json::Value>(json_str) {
                    if meta.get("slot").and_then(|s| s.as_str()) == Some(slot.as_str()) {
                        // Content-hash the artifact with blake3
                        if let Ok(data) = tokio::fs::read(&artifact_path).await {
                            let hash = blake3::hash(&data);
                            let key = ArtifactKey::new(hex::encode(hash.as_bytes()));

                            let _ = catalog.store(&key, &data).await;
                            let _ = catalog.register(key.clone(), artifact_path.clone()).await;

                            artifacts.insert(slot.clone(), key.clone());

                            debug!(
                                node = %Scheduler::node_id_for_cell(cell),
                                slot = %slot,
                                artifact = %key,
                                size = data.len(),
                                "artifact serialized and stored"
                            );
                        }

                        // Auto-extract metrics from save metadata
                        if let Some(inner) = meta.get("meta") {
                            if let Some(v) = inner.get("metric_value").and_then(|v| v.as_f64()) {
                                node_metrics.insert(slot.as_str().to_string(), v);
                            }
                            if let Some(dm) = inner.get("dict_metrics").and_then(|v| v.as_object())
                            {
                                for (k, v) in dm {
                                    if let Some(val) = v.as_f64() {
                                        node_metrics.insert(k.clone(), val);
                                    }
                                }
                            }
                        }
                    }
                }
            } else if let Some(err_str) = line.strip_prefix("__TINE_ARTIFACT_ERR__") {
                debug!(
                    node = %Scheduler::node_id_for_cell(cell),
                    slot = %slot,
                    error = err_str,
                    "artifact serialization failed for slot"
                );
            }
        }
    }

    metrics::counter!(M_NODES_EXECUTED).increment(1);
    let _ = event_tx.send(ExecutionEvent::NodeCompleted {
        execution_id: execution_id.clone(),
        node_id: Scheduler::node_id_for_cell(cell),
        tree_id: Some(branch.tree_id.clone()),
        branch_id: branch_id.clone(),
        target_kind: Some(target_kind.clone()),
        target: Some(target.clone()),
        artifacts: artifacts.clone(),
        duration_ms,
    });

    Ok((result, artifacts, node_metrics))
}

// ---------------------------------------------------------------------------
// Error context introspection
// ---------------------------------------------------------------------------

/// On node failure, query the live kernel namespace via `_pf_context()` to generate hints.
async fn introspect_error_context(
    kernel_mgr: &KernelManager,
    tree_id: &ExperimentTreeId,
    node_logs: &NodeLogs,
) -> Vec<String> {
    let mut hints = Vec::new();

    let ctx_result = kernel_mgr
        .execute_tree_code(tree_id, "print(_pf_json.dumps(_pf_context()))")
        .await;

    if let Ok(ctx_result) = ctx_result {
        if let Ok(context) = serde_json::from_str::<serde_json::Value>(&ctx_result.stdout.trim()) {
            // Generate hints based on error type + context
            if let Some(ref err) = node_logs.error {
                match err.ename.as_str() {
                    "KeyError" => {
                        // Look for DataFrames and list their columns
                        for (name, info) in context.as_object().unwrap_or(&serde_json::Map::new()) {
                            if let Some(cols) = info.get("columns") {
                                hints.push(format!("Available columns in '{}': {}", name, cols));
                            }
                        }
                        // Suggest similar column name
                        let missing = err.evalue.trim_matches('\'');
                        for (_name, info) in context.as_object().unwrap_or(&serde_json::Map::new())
                        {
                            if let Some(cols) = info.get("columns").and_then(|c| c.as_array()) {
                                for col in cols {
                                    if let Some(col_str) = col.as_str() {
                                        if levenshtein_distance(missing, col_str) <= 2
                                            && missing != col_str
                                        {
                                            hints.push(format!("Did you mean '{}'?", col_str));
                                        }
                                    }
                                }
                            }
                        }
                    }
                    "NameError" => {
                        let empty_map = serde_json::Map::new();
                        let obj = context.as_object().unwrap_or(&empty_map);
                        let available_vars: Vec<_> = obj.keys().collect();
                        hints.push(format!(
                            "Available variables in kernel namespace: {:?}",
                            available_vars
                        ));
                    }
                    _ => {}
                }
            }
        }
    }

    hints
}

#[cfg(test)]
mod tests {
    use tine_core::{
        BranchId, ExecutableTreeBranch, ExecutionTargetKind, ExecutionTargetRef, ExperimentTreeId,
        SlotName,
    };

    use super::Scheduler;

    #[test]
    fn branch_target_preserves_tree_and_branch_identity() {
        let target_ref = ExecutionTargetRef::ExperimentTreeBranch {
            tree_id: ExperimentTreeId::new("tree-1"),
            branch_id: BranchId::new("branch-1"),
        };
        let target = Scheduler::scheduler_target(&target_ref);

        assert_eq!(target.tree_id, ExperimentTreeId::new("tree-1"));
        assert_eq!(target.branch_id, Some(BranchId::new("branch-1")));
        assert_eq!(
            target.target_kind,
            ExecutionTargetKind::ExperimentTreeBranch
        );
        assert_eq!(target.target, target_ref);
    }

    #[test]
    fn executable_branch_runtime_helpers_preserve_identity() {
        let branch = sample_branch();

        assert_eq!(Scheduler::branch_runtime_id(&branch), "tree-1::branch-1");
        let second = Scheduler::cell_by_node_id(&branch, &tine_core::NodeId::new("cell-2"));
        let input = second.inputs.get(&SlotName::new("input")).unwrap();
        assert_eq!(input.source_cell_id.as_str(), "cell-1");
        assert_eq!(input.source_output.as_str(), "result");
    }

    fn sample_branch() -> ExecutableTreeBranch {
        serde_json::from_value(serde_json::json!({
            "tree_id": "tree-1",
            "branch_id": "branch-1",
            "name": "tree-1 [branch-1]",
            "lineage": ["main", "branch-1"],
            "path_cell_order": ["cell-1", "cell-2"],
            "topo_order": ["cell-1", "cell-2"],
            "cells": [
                {
                    "tree_id": "tree-1",
                    "branch_id": "branch-1",
                    "cell_id": "cell-1",
                    "name": "cell-1",
                    "code": { "source": "x = 1", "language": "python" },
                    "inputs": {},
                    "outputs": ["result"],
                    "cache": true,
                    "map_over": null,
                    "map_concurrency": null,
                    "timeout_secs": null,
                    "tags": {},
                    "revision_id": null
                },
                {
                    "tree_id": "tree-1",
                    "branch_id": "branch-1",
                    "cell_id": "cell-2",
                    "name": "cell-2",
                    "code": { "source": "print(result)", "language": "python" },
                    "inputs": {
                        "input": {
                            "source_cell_id": "cell-1",
                            "source_output": "result"
                        }
                    },
                    "outputs": ["output"],
                    "cache": false,
                    "map_over": null,
                    "map_concurrency": null,
                    "timeout_secs": 30,
                    "tags": { "stage": "eval" },
                    "revision_id": null
                }
            ],
            "environment": { "dependencies": ["pandas"] },
            "execution_mode": "parallel",
            "budget": null,
            "project_id": "project-1",
            "created_at": "2026-01-01T00:00:00Z"
        }))
        .unwrap()
    }
}

/// Simple Levenshtein distance for typo detection.
fn levenshtein_distance(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let mut dp = vec![vec![0usize; b.len() + 1]; a.len() + 1];
    for i in 0..=a.len() {
        dp[i][0] = i;
    }
    for j in 0..=b.len() {
        dp[0][j] = j;
    }
    for i in 1..=a.len() {
        for j in 1..=b.len() {
            let cost = if a[i - 1] == b[j - 1] { 0 } else { 1 };
            dp[i][j] = (dp[i - 1][j] + 1)
                .min(dp[i][j - 1] + 1)
                .min(dp[i - 1][j - 1] + cost);
        }
    }
    dp[a.len()][b.len()]
}

// ---------------------------------------------------------------------------
// Cache write helper
// ---------------------------------------------------------------------------

/// Write a cache entry to SQLite after successful node execution.
async fn write_cache_entry(
    pool: &SqlitePool,
    cell: &ExecutableTreeCell,
    runtime_id: &str,
    lockfile_hash: [u8; 32],
    all_node_artifacts: &HashMap<NodeId, HashMap<SlotName, ArtifactKey>>,
    produced_artifacts: &HashMap<SlotName, ArtifactKey>,
) -> TineResult<()> {
    // Build input hashes from upstream artifact keys
    let mut input_hashes: HashMap<SlotName, [u8; 32]> = HashMap::new();
    for (slot, input) in &cell.inputs {
        let source_node_id = NodeId::new(input.source_cell_id.as_str());
        if let Some(src_artifacts) = all_node_artifacts.get(&source_node_id) {
            for (_, artifact_key) in src_artifacts {
                input_hashes.insert(slot.clone(), NodeCacheKey::hash_code(artifact_key.as_str()));
            }
        }
    }

    let code_hash = NodeCacheKey::hash_code(&cell.code.source);
    let code_hash_hex = hex::encode(code_hash);
    let input_hashes_json = serde_json::to_string(&input_hashes).unwrap_or_default();
    let lockfile_hash_hex = hex::encode(lockfile_hash);
    let artifacts_json = serde_json::to_string(produced_artifacts).unwrap_or_default();

    sqlx::query(
        "INSERT OR REPLACE INTO cache (code_hash, input_hashes, lockfile_hash, artifacts, source_runtime_id, node_id, created_at, last_accessed) \
         VALUES (?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))"
    )
    .bind(&code_hash_hex)
    .bind(&input_hashes_json)
    .bind(&lockfile_hash_hex)
    .bind(&artifacts_json)
    .bind(runtime_id)
    .bind(cell.cell_id.as_str())
    .execute(pool)
    .await
    .map_err(|e| TineError::Database(format!("cache write failed: {}", e)))?;

    // Update artifact reference counts
    for (_, key) in produced_artifacts {
        sqlx::query(
            "INSERT INTO artifact_refs (artifact_key, ref_count, size_bytes, created_at, last_accessed) \
             VALUES (?, 1, 0, datetime('now'), datetime('now')) \
             ON CONFLICT(artifact_key) DO UPDATE SET ref_count = ref_count + 1, last_accessed = datetime('now')"
        )
        .bind(key.as_str())
        .execute(pool)
        .await
        .map_err(|e| TineError::Database(format!("artifact_refs write failed: {}", e)))?;
    }

    debug!(
        node = %cell.cell_id,
        artifacts = produced_artifacts.len(),
        "cache entry written"
    );

    Ok(())
}
