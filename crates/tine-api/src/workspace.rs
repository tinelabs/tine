use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePool};
use tokio::sync::{Mutex, Mutex as TokioMutex, Notify, RwLock};
use tracing::{debug, error, info, warn};

use crate::branch_projection::{branch_lineage, plan_branch_transition, BranchProjection};
use tine_catalog::DataCatalog;
use tine_core::{
    ArtifactKey, ArtifactStore, BranchDef, BranchId, BranchIsolationMode, BranchTargetInspection,
    CellDef, CellId, CellRuntimeState, ExecutableTreeBranch, ExecutableTreeCell, ExecutionAccepted,
    ExecutionEvent, ExecutionId, ExecutionLifecycleStatus, ExecutionPhase, ExecutionQueueTelemetry,
    ExecutionStatus, ExecutionTargetKind, ExecutionTargetRef, ExperimentTreeDef, ExperimentTreeId,
    IsolationResult, NodeCacheKey, NodeCode, NodeError, NodeId, NodeLogs, NodeStatus,
    PreparedContext, ProjectDef, ProjectId, RuntimeHealthSnapshot, SlotName, TineError, TineResult,
    TreeKernelState, TreeRuntimeState, WorkspaceApi,
};
use tine_env::{EnvironmentManager, TreeEnvironmentDescriptor};
use tine_kernel::{KernelIsolationOutcome, KernelLifecycleEvent, KernelManager};
use tine_observe::{
    OutcomeTimer, METRIC_PREPARE_CONTEXT_REPLAY, METRIC_PREPARE_CONTEXT_REPLAY_CELLS,
    METRIC_PREPARE_CONTEXT_TOTAL,
};
use tine_scheduler::Scheduler;

use dashmap::DashMap;

/// In-memory buffer for streamed cell output (`NodeStream` events). Streams
/// are appended here on the hot path; the buffer is drained into the
/// persisted `executions.node_logs` blob whenever a non-stream event for
/// the same execution arrives, or when the execution finishes. This avoids
/// the O(N²) read-modify-write that would otherwise happen for every
/// stream chunk against the cumulative log blob — see the regression test
/// `persist_execution_event_snapshot_streaming_does_not_scale_quadratically`.
type StreamingLogBuffer = DashMap<ExecutionId, HashMap<NodeId, NodeLogs>>;

/// Per-execution mutex registry. Both `persist_execution_event_snapshot`
/// (event-driven) and `flush_streaming_buffer_for_execution` (periodic)
/// do read-modify-write of `executions.node_logs`. Without coordination,
/// the slower writer can overwrite the faster writer's update — losing
/// either streamed chunks or terminal-event metadata. This registry hands
/// out an `Arc<tokio::sync::Mutex<()>>` per execution so both paths
/// serialize their critical section per row. Entries are not cleaned up
/// on execution finish; per-execution memory is one Arc<Mutex<()>>
/// (≈ 64 bytes) which is bounded by total executions ever observed.
type ExecutionLockRegistry = DashMap<ExecutionId, Arc<TokioMutex<()>>>;

fn execution_persist_lock(
    locks: &ExecutionLockRegistry,
    execution_id: &ExecutionId,
) -> Arc<TokioMutex<()>> {
    locks
        .entry(execution_id.clone())
        .or_insert_with(|| Arc::new(TokioMutex::new(())))
        .clone()
}

/// Parse SQLite datetime strings (both `YYYY-MM-DD HH:MM:SS` and RFC3339).
fn parse_sqlite_datetime(s: &str) -> chrono::DateTime<chrono::Utc> {
    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
        .map(|dt| dt.and_utc())
        .or_else(|_| {
            chrono::DateTime::parse_from_rfc3339(s).map(|dt| dt.with_timezone(&chrono::Utc))
        })
        .unwrap_or_default()
}

/// A file or directory entry in the workspace.
#[derive(Debug, Clone, serde::Serialize)]
pub struct FileEntry {
    pub name: String,
    pub is_dir: bool,
    pub size: u64,
}

/// One cell's entry in a branch execution dry-run (see
/// [`Workspace::preview_branch_execution_plan`]).
#[derive(Debug, Clone, serde::Serialize)]
pub struct BranchPlanPreviewCell {
    pub cell_id: String,
    /// "run" | "cache_hit"
    pub action: &'static str,
    /// "cached" | "cache_disabled" | "upstream_will_run" | "no_prior_run"
    /// | "code_changed" | "inputs_or_environment_changed"
    pub reason: &'static str,
}

#[derive(Debug, Clone)]
/// Client-supplied idempotency reservation for an execute submission,
/// scoped to its target and fingerprinted against the execution-relevant
/// request state.
struct ExecutionIdempotency<'a> {
    key: &'a str,
    scope: &'a str,
    fingerprint: &'a str,
}

struct TreeBranchExecutionPlan {
    executable_branch: ExecutableTreeBranch,
    target: ExecutionTargetRef,
}

#[derive(Debug, Clone)]
struct TreeCellExecutionPlan {
    executable_branch: ExecutableTreeBranch,
    executable_cell: ExecutableTreeCell,
    target: ExecutionTargetRef,
}

#[derive(Debug, Default)]
struct ExecutionQueueState {
    pending: VecDeque<ExecutionId>,
    active: HashSet<ExecutionId>,
}

// ---------------------------------------------------------------------------
// Workspace — the main WorkspaceApi implementation
// ---------------------------------------------------------------------------

/// Concrete implementation of `WorkspaceApi` backed by SQLite, the scheduler, etc.
///
/// Tree execution in this module is tree-native. Remaining `pipeline_id`
/// references only appear in database-migration guards for old persisted
/// columns, not in the active runtime path.
///
/// Naming guidance:
/// - "experiment" in user-facing/UI language increasingly means experiment tree
/// - "branch" means an in-tree path, not a forked pipeline
/// - "pipeline" appears only when migrating old on-disk schema
pub struct Workspace {
    pool: SqlitePool,
    scheduler: Arc<Scheduler>,
    kernel_mgr: Arc<KernelManager>,
    #[allow(dead_code)]
    env_mgr: Arc<EnvironmentManager>,
    #[allow(dead_code)]
    catalog: Arc<DataCatalog>,
    #[allow(dead_code)]
    workspace_root: PathBuf,
    tree_runtime_states: Arc<RwLock<HashMap<ExperimentTreeId, TreeRuntimeState>>>,
    execution_queue_state: Arc<Mutex<ExecutionQueueState>>,
    execution_queue_notify: Arc<Notify>,
    /// In-memory buffer for streaming `NodeStream` chunks. Flushed into the
    /// persisted blob whenever a non-stream event for the same execution
    /// arrives. See `persist_execution_event_snapshot` and the regression
    /// test it documents.
    streaming_log_buffer: Arc<StreamingLogBuffer>,
    /// Per-execution mutex registry serializing read-modify-write of
    /// `executions.node_logs` across the event-driven persist path and
    /// the periodic flush path. Without this, the two paths race and one
    /// can overwrite the other's update — see the regression test
    /// `flush_and_persist_must_not_lose_data_under_concurrent_access`.
    execution_persist_locks: Arc<ExecutionLockRegistry>,
    /// Shutdown signal for the execution-event bridge. On `shutdown()` we
    /// notify the bridge so it can drain any events still queued in its
    /// `broadcast::Receiver` before we tear down the rest of the
    /// workspace. Without this, queued events would be silently lost —
    /// see the regression test
    /// `shutdown_drains_pending_bridge_events_before_aborting`.
    bridge_shutdown_signal: Arc<Notify>,
    max_concurrent_executions: usize,
    max_queue_depth: usize,
    kernel_monitor_handle: tokio::task::JoinHandle<()>,
    kernel_lifecycle_handle: tokio::task::JoinHandle<()>,
    /// Wrapped in `Mutex<Option<_>>` so `shutdown()` (`&self`) can take
    /// ownership of the handle and `await` it after signaling the bridge
    /// to drain. Drop-time abort still works via a sync mutex try_lock.
    execution_event_bridge_handle: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    streaming_log_flush_handle: tokio::task::JoinHandle<()>,
}

impl Drop for Workspace {
    fn drop(&mut self) {
        self.kernel_monitor_handle.abort();
        self.kernel_lifecycle_handle.abort();
        if let Ok(mut guard) = self.execution_event_bridge_handle.try_lock() {
            if let Some(handle) = guard.take() {
                handle.abort();
            }
        }
        self.streaming_log_flush_handle.abort();
    }
}

impl Workspace {
    fn default_max_queue_depth(max_concurrent_executions: usize) -> usize {
        std::cmp::max(8, max_concurrent_executions.saturating_mul(4))
    }

    fn default_tree_runtime_state(
        tree_id: &ExperimentTreeId,
        branch_id: &BranchId,
    ) -> TreeRuntimeState {
        TreeRuntimeState {
            tree_id: tree_id.clone(),
            active_branch_id: branch_id.clone(),
            materialized_path_cell_ids: Vec::new(),
            runtime_epoch: 0,
            kernel_state: TreeKernelState::NeedsReplay,
            last_prepared_cell_id: None,
            isolation_mode: BranchIsolationMode::Disabled,
            last_isolation_result: None,
        }
    }

    fn execution_target_for_tree_branch(
        tree_id: &ExperimentTreeId,
        branch_id: &BranchId,
    ) -> ExecutionTargetRef {
        ExecutionTargetRef::ExperimentTreeBranch {
            tree_id: tree_id.clone(),
            branch_id: branch_id.clone(),
        }
    }

    fn branch_row_id(tree_id: &ExperimentTreeId, branch_id: &BranchId) -> String {
        format!("{}::{}", tree_id.as_str(), branch_id.as_str())
    }

    async fn sync_pending_queue_positions(
        pool: &SqlitePool,
        pending: &[ExecutionId],
    ) -> TineResult<()> {
        for (index, execution_id) in pending.iter().enumerate() {
            Self::update_execution_status_record(pool, execution_id, |status| {
                status.queue_position = Some((index + 1) as u64);
                if matches!(status.status, ExecutionLifecycleStatus::Queued) {
                    Self::apply_execution_phase(status, ExecutionPhase::Queued);
                }
            })
            .await?;
        }
        Ok(())
    }

    async fn enqueue_execution_with(
        pool: &SqlitePool,
        execution_queue_state: &Arc<Mutex<ExecutionQueueState>>,
        max_queue_depth: usize,
        execution_id: &ExecutionId,
    ) -> TineResult<u64> {
        let pending_snapshot = {
            let mut queue = execution_queue_state.lock().await;
            if queue.pending.len() >= max_queue_depth {
                return Err(TineError::BudgetExceeded(format!(
                    "execution queue full ({}/{})",
                    queue.pending.len(),
                    max_queue_depth
                )));
            }
            queue.pending.push_back(execution_id.clone());
            queue.pending.iter().cloned().collect::<Vec<_>>()
        };
        Self::sync_pending_queue_positions(pool, &pending_snapshot).await?;
        Ok(pending_snapshot.len() as u64)
    }

    async fn enqueue_execution(&self, execution_id: &ExecutionId) -> TineResult<u64> {
        Self::enqueue_execution_with(
            &self.pool,
            &self.execution_queue_state,
            self.max_queue_depth,
            execution_id,
        )
        .await
    }

    async fn reject_execution(&self, execution_id: &ExecutionId) -> TineResult<()> {
        // A rejected submission was never accepted, so release its
        // idempotency reservation first: a retry with the same key must
        // start a fresh execution, not reattach to this rejected one.
        sqlx::query(
            "UPDATE executions SET idempotency_key = NULL, idempotency_scope = NULL, idempotency_fingerprint = NULL WHERE id = ?",
        )
        .bind(execution_id.as_str())
        .execute(&self.pool)
        .await
        .map_err(|e| TineError::Database(e.to_string()))?;
        Self::update_execution_status_record(&self.pool, execution_id, |status| {
            status.queue_position = None;
            status.finished_at = Some(Utc::now());
            Self::apply_execution_phase(status, ExecutionPhase::Rejected);
        })
        .await?;
        Ok(())
    }

    async fn wait_for_execution_slot_with(
        pool: &SqlitePool,
        execution_queue_state: &Arc<Mutex<ExecutionQueueState>>,
        execution_queue_notify: &Arc<Notify>,
        max_concurrent_executions: usize,
        execution_id: &ExecutionId,
    ) -> TineResult<bool> {
        loop {
            let should_wait = {
                let mut queue = execution_queue_state.lock().await;
                if queue.active.contains(execution_id) {
                    false
                } else if queue.pending.front() == Some(execution_id)
                    && queue.active.len() < max_concurrent_executions
                {
                    queue.pending.pop_front();
                    queue.active.insert(execution_id.clone());
                    let pending_snapshot = queue.pending.iter().cloned().collect::<Vec<_>>();
                    drop(queue);
                    Self::sync_pending_queue_positions(pool, &pending_snapshot).await?;
                    Self::update_execution_status_record(pool, execution_id, |status| {
                        status.queue_position = None;
                    })
                    .await?;
                    return Ok(true);
                } else if queue
                    .pending
                    .iter()
                    .any(|queued_id| queued_id == execution_id)
                {
                    true
                } else {
                    return Ok(false);
                }
            };

            if should_wait {
                execution_queue_notify.notified().await;
            }
        }
    }

    async fn release_execution_slot_with(
        execution_queue_state: &Arc<Mutex<ExecutionQueueState>>,
        execution_queue_notify: &Arc<Notify>,
        execution_id: &ExecutionId,
    ) {
        let changed = {
            let mut queue = execution_queue_state.lock().await;
            let changed = queue.active.remove(execution_id);
            if !changed {
                queue.pending.retain(|queued_id| queued_id != execution_id);
            }
            changed
        };
        if changed {
            execution_queue_notify.notify_waiters();
        }
    }

    async fn release_execution_slot(&self, execution_id: &ExecutionId) {
        Self::release_execution_slot_with(
            &self.execution_queue_state,
            &self.execution_queue_notify,
            execution_id,
        )
        .await;
    }

    async fn dequeue_execution_with(
        pool: &SqlitePool,
        execution_queue_state: &Arc<Mutex<ExecutionQueueState>>,
        execution_queue_notify: &Arc<Notify>,
        execution_id: &ExecutionId,
    ) -> TineResult<bool> {
        let pending_snapshot = {
            let mut queue = execution_queue_state.lock().await;
            let Some(position) = queue
                .pending
                .iter()
                .position(|queued_id| queued_id == execution_id)
            else {
                return Ok(false);
            };
            queue.pending.remove(position);
            queue.pending.iter().cloned().collect::<Vec<_>>()
        };
        Self::sync_pending_queue_positions(pool, &pending_snapshot).await?;
        execution_queue_notify.notify_waiters();
        Ok(true)
    }

    async fn dequeue_execution(&self, execution_id: &ExecutionId) -> TineResult<bool> {
        Self::dequeue_execution_with(
            &self.pool,
            &self.execution_queue_state,
            &self.execution_queue_notify,
            execution_id,
        )
        .await
    }

    fn cell_row_id(tree_id: &ExperimentTreeId, cell_id: &CellId) -> String {
        format!("{}::{}", tree_id.as_str(), cell_id.as_str())
    }

    fn branch_path_cell_ids(
        tree: &ExperimentTreeDef,
        branch_id: &BranchId,
    ) -> TineResult<Vec<CellId>> {
        let mut branch_by_id = HashMap::new();
        for branch in &tree.branches {
            branch_by_id.insert(branch.id.clone(), branch);
        }

        let mut lineage = Vec::new();
        let mut current = branch_by_id.get(branch_id).copied().ok_or_else(|| {
            TineError::NotFound(format!(
                "branch '{}' not found in tree '{}'",
                branch_id, tree.id
            ))
        })?;
        loop {
            lineage.push(current);
            match &current.parent_branch_id {
                Some(parent_id) => {
                    current = branch_by_id.get(parent_id).copied().ok_or_else(|| {
                        TineError::NotFound(format!(
                            "parent branch '{}' not found in tree '{}'",
                            parent_id, tree.id
                        ))
                    })?;
                }
                None => break,
            }
        }
        lineage.reverse();

        let mut path = Vec::new();
        for (index, branch) in lineage.iter().enumerate() {
            if let Some(next_branch) = lineage.get(index + 1).copied() {
                if let Some(branch_point) = &next_branch.branch_point_cell_id {
                    let stop_idx = branch
                        .cell_order
                        .iter()
                        .position(|cell_id| cell_id == branch_point)
                        .ok_or_else(|| {
                            TineError::NotFound(format!(
                                "branch point '{}' not found in branch '{}' for tree '{}'",
                                branch_point, branch.id, tree.id
                            ))
                        })?;
                    path.extend(branch.cell_order.iter().take(stop_idx + 1).cloned());
                    continue;
                }
            }
            path.extend(branch.cell_order.iter().cloned());
        }
        Ok(path)
    }

    fn validate_branch_membership<'a>(
        tree: &'a ExperimentTreeDef,
        branch_id: &BranchId,
        cell_id: &CellId,
    ) -> TineResult<&'a BranchDef> {
        let branch = tree
            .branches
            .iter()
            .find(|branch| &branch.id == branch_id)
            .ok_or_else(|| {
                TineError::NotFound(format!(
                    "branch '{}' not found in tree '{}'",
                    branch_id, tree.id
                ))
            })?;
        if !branch.cell_order.iter().any(|existing| existing == cell_id) {
            return Err(TineError::NotFound(format!(
                "cell '{}' not found in branch '{}' for tree '{}'",
                cell_id, branch_id, tree.id
            )));
        }
        Ok(branch)
    }

    fn ordered_branch_ids(tree: &ExperimentTreeDef) -> TineResult<Vec<BranchId>> {
        let mut branches = tree
            .branches
            .iter()
            .enumerate()
            .map(|(index, branch)| {
                let depth = branch_lineage(tree, &branch.id)?.len();
                Ok((depth, index, branch.id.clone()))
            })
            .collect::<TineResult<Vec<_>>>()?;
        branches.sort_by_key(|(depth, index, _)| (*depth, *index));
        Ok(branches
            .into_iter()
            .map(|(_, _, branch_id)| branch_id)
            .collect())
    }

    fn build_tree_branch_execution_plan(
        tree: &ExperimentTreeDef,
        branch_id: &BranchId,
    ) -> TineResult<TreeBranchExecutionPlan> {
        let executable_branch =
            BranchProjection::from_tree(tree, branch_id)?.to_executable_tree_branch(tree)?;
        let target = Self::execution_target_for_tree_branch(&tree.id, branch_id);
        Ok(TreeBranchExecutionPlan {
            executable_branch,
            target,
        })
    }

    fn build_tree_cell_execution_plan(
        tree: &ExperimentTreeDef,
        target_branch_id: &BranchId,
        cell_id: &CellId,
    ) -> TineResult<TreeCellExecutionPlan> {
        let cell = tree
            .cells
            .iter()
            .find(|cell| &cell.id == cell_id)
            .ok_or_else(|| {
                TineError::NotFound(format!(
                    "cell '{}' not found in branch '{}' for tree '{}'",
                    cell_id, target_branch_id, tree.id
                ))
            })?
            .clone();
        let executable_cell = ExecutableTreeCell {
            tree_id: tree.id.clone(),
            branch_id: target_branch_id.clone(),
            cell_id: cell.id.clone(),
            name: cell.name.clone(),
            code: cell.code.clone(),
            inputs: HashMap::new(),
            outputs: cell.declared_outputs.clone(),
            // A single-cell plan strips the cell's inputs, so a cache key
            // derived from it would ignore upstream data entirely — it could
            // skip the cell with stale results and write entries that collide
            // with branch-run keys. Explicit cell submission therefore always
            // executes and never touches the cache.
            cache: false,
            map_over: cell.map_over.clone(),
            map_concurrency: cell.map_concurrency,
            tags: cell.tags.clone(),
            revision_id: cell.revision_id.clone(),
        };
        let executable_branch = ExecutableTreeBranch {
            tree_id: tree.id.clone(),
            branch_id: target_branch_id.clone(),
            name: format!("{} [{}]", tree.name, target_branch_id),
            lineage: vec![target_branch_id.clone()],
            path_cell_order: vec![cell.id.clone()],
            topo_order: vec![cell.id.clone()],
            cells: vec![executable_cell.clone()],
            environment: tree.environment.clone(),
            execution_mode: tree.execution_mode.clone(),
            budget: tree.budget.clone(),
            project_id: tree.project_id.clone(),
            created_at: tree.created_at,
        };
        let target = Self::execution_target_for_tree_branch(&tree.id, target_branch_id);
        Ok(TreeCellExecutionPlan {
            executable_branch,
            executable_cell,
            target,
        })
    }

    async fn insert_branch_execution_record(
        pool: &SqlitePool,
        execution_id: &ExecutionId,
        tree_id: &ExperimentTreeId,
        branch_id: &BranchId,
        target: &ExecutionTargetRef,
        topo_order: &[CellId],
    ) -> TineResult<()> {
        Self::insert_branch_execution_record_with_key(
            pool,
            execution_id,
            tree_id,
            branch_id,
            target,
            topo_order,
            None,
        )
        .await
    }

    async fn insert_branch_execution_record_with_key(
        pool: &SqlitePool,
        execution_id: &ExecutionId,
        tree_id: &ExperimentTreeId,
        branch_id: &BranchId,
        target: &ExecutionTargetRef,
        topo_order: &[CellId],
        idempotency: Option<&ExecutionIdempotency<'_>>,
    ) -> TineResult<()> {
        let initial_status = ExecutionStatus {
            execution_id: execution_id.clone(),
            tree_id: Some(tree_id.clone()),
            branch_id: Some(branch_id.clone()),
            target_kind: Some(ExecutionTargetKind::ExperimentTreeBranch),
            target: Some(target.clone()),
            status: ExecutionLifecycleStatus::Queued,
            phase: ExecutionPhase::Queued,
            queue_position: None,
            queue: None,
            runtime: None,
            cancellation_requested_at: None,
            node_statuses: topo_order
                .iter()
                .map(|cell_id| (NodeId::new(cell_id.as_str()), NodeStatus::Queued))
                .collect(),
            started_at: Utc::now(),
            finished_at: None,
        };
        let status_json = serde_json::to_string(&initial_status).unwrap_or_default();
        sqlx::query(
            "INSERT INTO executions (id, tree_id, branch_id, target_kind, status, started_at, idempotency_key, idempotency_scope, idempotency_fingerprint) VALUES (?, ?, ?, ?, ?, datetime('now'), ?, ?, ?)",
        )
        .bind(execution_id.as_str())
        .bind(tree_id.as_str())
        .bind(branch_id.as_str())
        .bind("experiment_tree_branch")
        .bind(&status_json)
        .bind(idempotency.map(|record| record.key))
        .bind(idempotency.map(|record| record.scope))
        .bind(idempotency.map(|record| record.fingerprint))
        .execute(pool)
        .await
        .map_err(|e| TineError::Database(e.to_string()))?;
        Ok(())
    }

    async fn finalize_branch_execution_success(
        pool: &SqlitePool,
        execution_id: &ExecutionId,
        tree_id: &ExperimentTreeId,
        branch_id: &BranchId,
        target: &ExecutionTargetRef,
        outcome: tine_core::ExecutionOutcome,
    ) {
        let existing: Option<(String, Option<String>)> =
            sqlx::query_as("SELECT status, node_logs FROM executions WHERE id = ?")
                .bind(execution_id.as_str())
                .fetch_optional(pool)
                .await
                .ok()
                .flatten();
        let mut existing_started_at =
            Utc::now() - chrono::Duration::milliseconds(outcome.duration_ms as i64);
        let mut merged_node_statuses = outcome.node_statuses.clone();
        let mut merged_node_logs = outcome.node_logs.clone();
        if let Some((status_json, node_logs_json)) = existing {
            if let Ok(status) = serde_json::from_str::<ExecutionStatus>(&status_json) {
                let status = Self::normalize_execution_status(status);
                if status.finished_at.is_some() || status.cancellation_requested_at.is_some() {
                    return;
                }
                existing_started_at = status.started_at;
                for (node_id, node_status) in status.node_statuses {
                    merged_node_statuses.entry(node_id).or_insert(node_status);
                }
            }
            let existing_logs: HashMap<NodeId, NodeLogs> = node_logs_json
                .as_deref()
                .and_then(|json| serde_json::from_str(json).ok())
                .unwrap_or_default();
            for (node_id, existing_log) in existing_logs {
                let target = merged_node_logs.entry(node_id).or_default();
                if target.stdout.is_empty() {
                    target.stdout = existing_log.stdout;
                } else if !existing_log.stdout.is_empty() {
                    target.stdout = format!("{}{}", existing_log.stdout, target.stdout);
                }
                if target.stderr.is_empty() {
                    target.stderr = existing_log.stderr;
                } else if !existing_log.stderr.is_empty() {
                    target.stderr = format!("{}{}", existing_log.stderr, target.stderr);
                }
                if target.outputs.is_empty() {
                    target.outputs = existing_log.outputs;
                } else if !existing_log.outputs.is_empty() {
                    let mut outputs = existing_log.outputs;
                    outputs.extend(target.outputs.clone());
                    target.outputs = outputs;
                }
                if target.error.is_none() {
                    target.error = existing_log.error;
                }
                if target.duration_ms.is_none() {
                    target.duration_ms = existing_log.duration_ms;
                }
                if target.metrics.is_empty() {
                    target.metrics = existing_log.metrics;
                } else {
                    for (name, value) in existing_log.metrics {
                        target.metrics.entry(name).or_insert(value);
                    }
                }
            }
        }

        let terminal_status =
            Self::terminal_status_from_outcome(&merged_node_statuses, &merged_node_logs);
        let status = ExecutionStatus {
            execution_id: execution_id.clone(),
            tree_id: Some(tree_id.clone()),
            branch_id: Some(branch_id.clone()),
            target_kind: Some(ExecutionTargetKind::ExperimentTreeBranch),
            target: Some(target.clone()),
            status: terminal_status.clone(),
            phase: Self::terminal_phase_from_status(&terminal_status),
            queue_position: None,
            queue: None,
            runtime: None,
            cancellation_requested_at: None,
            node_statuses: merged_node_statuses,
            started_at: existing_started_at,
            finished_at: Some(Utc::now()),
        };
        let status_json = serde_json::to_string(&status).unwrap_or_default();
        let logs_json = serde_json::to_string(&merged_node_logs).unwrap_or_default();

        // The pre-read above is best-effort merging; this conditional UPDATE is
        // the authoritative guard. Concurrent finalizers (e.g. cancel racing
        // completion) race the read-check, so only the writer that flips
        // finished_at from NULL wins — the loser must not write metrics either.
        let finalized = sqlx::query(
            "UPDATE executions SET status = ?, node_logs = ?, finished_at = datetime('now') WHERE id = ? AND finished_at IS NULL",
        )
        .bind(&status_json)
        .bind(&logs_json)
        .bind(execution_id.as_str())
        .execute(pool)
        .await
        .map(|result| result.rows_affected() > 0)
        .unwrap_or(false);
        if !finalized {
            return;
        }

        for (node_id, logs) in &merged_node_logs {
            for (name, value) in &logs.metrics {
                let _ = sqlx::query(
                    "INSERT INTO metrics (execution_id, node_id, metric_name, metric_value, step) \
                     VALUES (?, ?, ?, ?, 0)",
                )
                .bind(execution_id.as_str())
                .bind(node_id.as_str())
                .bind(name)
                .bind(value)
                .execute(pool)
                .await;
            }
        }
    }

    async fn finalize_branch_execution_failure(
        pool: &SqlitePool,
        execution_id: &ExecutionId,
        tree_id: &ExperimentTreeId,
        branch_id: &BranchId,
        target: &ExecutionTargetRef,
    ) {
        let existing: Option<(String, Option<String>)> =
            sqlx::query_as("SELECT status, node_logs FROM executions WHERE id = ?")
                .bind(execution_id.as_str())
                .fetch_optional(pool)
                .await
                .ok()
                .flatten();
        let mut existing_started_at = Utc::now();
        let mut existing_node_statuses = HashMap::new();
        let mut existing_logs_json: Option<String> = None;
        if let Some((status_json, node_logs_json)) = existing {
            if let Ok(status) = serde_json::from_str::<ExecutionStatus>(&status_json) {
                let status = Self::normalize_execution_status(status);
                if status.finished_at.is_some() || status.cancellation_requested_at.is_some() {
                    return;
                }
                existing_started_at = status.started_at;
                existing_node_statuses = status.node_statuses;
            }
            existing_logs_json = node_logs_json;
        }

        let status = ExecutionStatus {
            execution_id: execution_id.clone(),
            tree_id: Some(tree_id.clone()),
            branch_id: Some(branch_id.clone()),
            target_kind: Some(ExecutionTargetKind::ExperimentTreeBranch),
            target: Some(target.clone()),
            status: ExecutionLifecycleStatus::Failed,
            phase: ExecutionPhase::Failed,
            queue_position: None,
            queue: None,
            runtime: None,
            cancellation_requested_at: None,
            node_statuses: existing_node_statuses,
            started_at: existing_started_at,
            finished_at: Some(Utc::now()),
        };
        let status_json = serde_json::to_string(&status).unwrap_or_default();
        // Conditional on finished_at IS NULL: a concurrent finalizer that
        // already terminalized this record must not be overwritten.
        let _ = sqlx::query(
            "UPDATE executions SET status = ?, node_logs = COALESCE(?, node_logs), finished_at = datetime('now') WHERE id = ? AND finished_at IS NULL",
        )
        .bind(&status_json)
        .bind(existing_logs_json)
        .bind(execution_id.as_str())
        .execute(pool)
        .await;
    }

    fn terminal_status_from_outcome(
        node_statuses: &HashMap<NodeId, NodeStatus>,
        _node_logs: &HashMap<NodeId, NodeLogs>,
    ) -> ExecutionLifecycleStatus {
        if node_statuses
            .values()
            .any(|node_status| matches!(node_status, NodeStatus::Failed))
        {
            ExecutionLifecycleStatus::Failed
        } else if !node_statuses.is_empty()
            && node_statuses
                .values()
                .all(|node_status| matches!(node_status, NodeStatus::Interrupted))
        {
            ExecutionLifecycleStatus::Cancelled
        } else {
            ExecutionLifecycleStatus::Completed
        }
    }

    fn terminal_status_from_nodes(
        node_statuses: &HashMap<NodeId, NodeStatus>,
    ) -> ExecutionLifecycleStatus {
        if node_statuses
            .values()
            .any(|node_status| matches!(node_status, NodeStatus::Failed))
        {
            ExecutionLifecycleStatus::Failed
        } else if !node_statuses.is_empty()
            && node_statuses
                .values()
                .all(|node_status| matches!(node_status, NodeStatus::Interrupted))
        {
            ExecutionLifecycleStatus::Cancelled
        } else {
            ExecutionLifecycleStatus::Completed
        }
    }

    fn terminal_phase_from_status(status: &ExecutionLifecycleStatus) -> ExecutionPhase {
        match status {
            ExecutionLifecycleStatus::Completed => ExecutionPhase::Completed,
            ExecutionLifecycleStatus::Failed => ExecutionPhase::Failed,
            ExecutionLifecycleStatus::Cancelled => ExecutionPhase::Cancelled,
            ExecutionLifecycleStatus::TimedOut => ExecutionPhase::TimedOut,
            ExecutionLifecycleStatus::Rejected => ExecutionPhase::Rejected,
            ExecutionLifecycleStatus::Queued => ExecutionPhase::Queued,
            ExecutionLifecycleStatus::Running => ExecutionPhase::Running,
        }
    }

    fn normalize_execution_status(mut status: ExecutionStatus) -> ExecutionStatus {
        if status.finished_at.is_some() {
            let terminal_status = match status.status {
                ExecutionLifecycleStatus::Completed
                | ExecutionLifecycleStatus::Failed
                | ExecutionLifecycleStatus::Cancelled
                | ExecutionLifecycleStatus::TimedOut
                | ExecutionLifecycleStatus::Rejected => status.status.clone(),
                ExecutionLifecycleStatus::Queued | ExecutionLifecycleStatus::Running => {
                    Self::terminal_status_from_nodes(&status.node_statuses)
                }
            };
            status.status = terminal_status.clone();
            status.phase = Self::terminal_phase_from_status(&terminal_status);
            status.queue_position = None;
            status.queue = None;
            return status;
        }

        if matches!(
            status.phase,
            ExecutionPhase::Completed
                | ExecutionPhase::Failed
                | ExecutionPhase::Cancelled
                | ExecutionPhase::TimedOut
                | ExecutionPhase::Rejected
        ) {
            status.phase = ExecutionPhase::Running;
        }

        if status
            .node_statuses
            .values()
            .any(|node_status| matches!(node_status, NodeStatus::Running))
        {
            status.status = ExecutionLifecycleStatus::Running;
            if matches!(status.phase, ExecutionPhase::Queued) {
                status.phase = ExecutionPhase::Running;
            }
        } else if !status.node_statuses.is_empty()
            && status
                .node_statuses
                .values()
                .all(|node_status| matches!(node_status, NodeStatus::Pending | NodeStatus::Queued))
        {
            status.status = ExecutionLifecycleStatus::Queued;
            status.phase = ExecutionPhase::Queued;
        }

        status
    }

    async fn runtime_health_snapshot(&self, tree_id: &ExperimentTreeId) -> RuntimeHealthSnapshot {
        let current_runtime_state = self.get_tree_runtime_state(tree_id).await;
        let has_live_kernel = self.kernel_mgr.has_tree_kernel(tree_id);
        let tree_kernel_state = current_runtime_state
            .as_ref()
            .map(|state| state.kernel_state.clone());

        RuntimeHealthSnapshot {
            tree_id: tree_id.clone(),
            has_live_kernel,
            tree_kernel_state: tree_kernel_state.clone(),
            replay_required: matches!(
                tree_kernel_state,
                Some(
                    TreeKernelState::NeedsReplay
                        | TreeKernelState::KernelLost
                        | TreeKernelState::Switching
                )
            ),
            active_branch_id: current_runtime_state
                .as_ref()
                .map(|state| state.active_branch_id.clone()),
            runtime_epoch: current_runtime_state
                .as_ref()
                .map(|state| state.runtime_epoch),
        }
    }

    async fn enrich_execution_status(&self, mut status: ExecutionStatus) -> ExecutionStatus {
        status.runtime = match status.tree_id.clone() {
            Some(tree_id) => Some(self.runtime_health_snapshot(&tree_id).await),
            None => None,
        };

        if status.finished_at.is_some() {
            status.queue = None;
            return status;
        }

        let queue = self.execution_queue_state.lock().await;
        let active_executions = queue.active.len() as u64;
        let pending_total = queue.pending.len() as u64;
        let pending_index = queue
            .pending
            .iter()
            .position(|queued_id| queued_id == &status.execution_id);
        let is_active = queue.active.contains(&status.execution_id);
        drop(queue);

        if matches!(status.status, ExecutionLifecycleStatus::Queued)
            || matches!(status.phase, ExecutionPhase::Queued)
        {
            if let Some(index) = pending_index {
                let queue_head = index == 0;
                let queued_reason = if queue_head {
                    if active_executions >= self.max_concurrent_executions as u64 {
                        "waiting_for_execution_slot"
                    } else {
                        "awaiting_scheduler_dispatch"
                    }
                } else {
                    "waiting_for_earlier_executions"
                };
                status.queue_position = Some((index + 1) as u64);
                status.queue = Some(ExecutionQueueTelemetry {
                    pending_ahead: index as u64,
                    pending_total,
                    active_executions,
                    max_concurrent_executions: self.max_concurrent_executions as u64,
                    max_queue_depth: self.max_queue_depth as u64,
                    queue_head,
                    queued_reason: queued_reason.to_string(),
                });
            } else if is_active {
                Self::apply_execution_phase(&mut status, ExecutionPhase::Running);
                status.queue_position = None;
                status.queue = Some(ExecutionQueueTelemetry {
                    pending_ahead: 0,
                    pending_total,
                    active_executions,
                    max_concurrent_executions: self.max_concurrent_executions as u64,
                    max_queue_depth: self.max_queue_depth as u64,
                    queue_head: false,
                    queued_reason: "transitioning_to_start".to_string(),
                });
            } else {
                status.queue = Some(ExecutionQueueTelemetry {
                    pending_ahead: 0,
                    pending_total,
                    active_executions,
                    max_concurrent_executions: self.max_concurrent_executions as u64,
                    max_queue_depth: self.max_queue_depth as u64,
                    queue_head: false,
                    queued_reason: "scheduler_state_unknown".to_string(),
                });
            }
        } else {
            status.queue = None;
        }

        status
    }

    fn apply_execution_phase(status: &mut ExecutionStatus, phase: ExecutionPhase) {
        status.phase = phase.clone();
        status.status = match phase {
            ExecutionPhase::Queued => ExecutionLifecycleStatus::Queued,
            ExecutionPhase::PreparingEnvironment
            | ExecutionPhase::AcquiringRuntime
            | ExecutionPhase::ReplayingContext
            | ExecutionPhase::Running
            | ExecutionPhase::CancellationRequested
            | ExecutionPhase::SerializingArtifacts
            | ExecutionPhase::Retrying => ExecutionLifecycleStatus::Running,
            ExecutionPhase::Completed => ExecutionLifecycleStatus::Completed,
            ExecutionPhase::Failed => ExecutionLifecycleStatus::Failed,
            ExecutionPhase::Cancelled => ExecutionLifecycleStatus::Cancelled,
            ExecutionPhase::TimedOut => ExecutionLifecycleStatus::TimedOut,
            ExecutionPhase::Rejected => ExecutionLifecycleStatus::Rejected,
        };
    }

    async fn finalize_cancelled_execution(
        pool: &SqlitePool,
        execution_id: &ExecutionId,
        cancellation_requested_at: chrono::DateTime<Utc>,
    ) -> TineResult<()> {
        let row: Option<(String,)> = sqlx::query_as("SELECT status FROM executions WHERE id = ?")
            .bind(execution_id.as_str())
            .fetch_optional(pool)
            .await
            .map_err(|e| TineError::Database(e.to_string()))?;
        let Some((status_json,)) = row else {
            return Ok(());
        };
        let status: ExecutionStatus =
            Self::normalize_execution_status(serde_json::from_str(&status_json)?);
        if status.finished_at.is_some() && status.status == ExecutionLifecycleStatus::Cancelled {
            return Ok(());
        }

        let cancelled_status =
            Self::reconcile_cancelled_execution_status(status, cancellation_requested_at);
        let status_json = serde_json::to_string(&cancelled_status).unwrap_or_default();

        // Conditional on finished_at IS NULL: if the execution completed (or
        // failed) just before this cancellation finalize ran, keep that
        // terminal status rather than overwriting it with Cancelled.
        sqlx::query("UPDATE executions SET status = ?, finished_at = datetime('now') WHERE id = ? AND finished_at IS NULL")
            .bind(&status_json)
            .bind(execution_id.as_str())
            .execute(pool)
            .await
            .map_err(|e| TineError::Database(e.to_string()))?;
        Ok(())
    }

    async fn await_cancellation_settle(
        pool: &SqlitePool,
        execution_id: &ExecutionId,
    ) -> TineResult<()> {
        const MAX_POLLS: usize = 40;
        const POLL_DELAY_MS: u64 = 100;

        for _ in 0..MAX_POLLS {
            let row: Option<(String,)> =
                sqlx::query_as("SELECT status FROM executions WHERE id = ?")
                    .bind(execution_id.as_str())
                    .fetch_optional(pool)
                    .await
                    .map_err(|e| TineError::Database(e.to_string()))?;
            let Some((status_json,)) = row else {
                return Ok(());
            };

            let status: ExecutionStatus =
                Self::normalize_execution_status(serde_json::from_str(&status_json)?);
            if status.finished_at.is_some() {
                return Ok(());
            }

            let has_active_nodes = status.node_statuses.values().any(|node_status| {
                matches!(
                    node_status,
                    NodeStatus::Pending | NodeStatus::Queued | NodeStatus::Running
                )
            });
            if !has_active_nodes {
                return Ok(());
            }

            tokio::time::sleep(std::time::Duration::from_millis(POLL_DELAY_MS)).await;
        }

        warn!(
            execution = %execution_id,
            polls = MAX_POLLS,
            poll_delay_ms = POLL_DELAY_MS,
            "await_cancellation_settle reached poll budget while nodes were still active; \
             proceeding to finalize anyway"
        );
        Ok(())
    }

    /// Returns whether the update was committed to a live (non-terminal)
    /// row. `false` means the execution was missing or already finished —
    /// callers acting on the updated state (e.g. interrupting a kernel
    /// after marking cancellation) must treat that as "do nothing", since
    /// the tree may already be running a different execution.
    async fn update_execution_status_record<F>(
        pool: &SqlitePool,
        execution_id: &ExecutionId,
        update: F,
    ) -> TineResult<bool>
    where
        F: FnOnce(&mut ExecutionStatus),
    {
        let row: Option<(String,)> = sqlx::query_as("SELECT status FROM executions WHERE id = ?")
            .bind(execution_id.as_str())
            .fetch_optional(pool)
            .await
            .map_err(|e| TineError::Database(e.to_string()))?;
        let Some((status_json,)) = row else {
            return Ok(false);
        };

        let mut status = Self::normalize_execution_status(serde_json::from_str(&status_json)?);
        if status.finished_at.is_some() {
            return Ok(false);
        }
        update(&mut status);
        let updated_status_json =
            serde_json::to_string(&status).map_err(TineError::Serialization)?;
        // Conditional on finished_at IS NULL so a phase update racing a
        // finalizer cannot resurrect a record that just became terminal.
        let result = sqlx::query(
            "UPDATE executions SET status = ?, finished_at = COALESCE(finished_at, ?) WHERE id = ? AND finished_at IS NULL",
        )
        .bind(&updated_status_json)
        .bind(status.finished_at.map(|timestamp| timestamp.to_rfc3339()))
        .bind(execution_id.as_str())
        .execute(pool)
        .await
        .map_err(|e| TineError::Database(e.to_string()))?;
        Ok(result.rows_affected() > 0)
    }

    fn reconcile_abandoned_execution_status(mut status: ExecutionStatus) -> ExecutionStatus {
        status = Self::normalize_execution_status(status);
        status.finished_at = Some(Utc::now());
        for node_status in status.node_statuses.values_mut() {
            if matches!(
                node_status,
                NodeStatus::Pending | NodeStatus::Queued | NodeStatus::Running
            ) {
                *node_status = NodeStatus::Interrupted;
            }
        }
        status.status = ExecutionLifecycleStatus::Failed;
        status.phase = ExecutionPhase::Failed;
        status
    }

    fn reconcile_cancelled_execution_status(
        mut status: ExecutionStatus,
        cancellation_requested_at: chrono::DateTime<Utc>,
    ) -> ExecutionStatus {
        status = Self::normalize_execution_status(status);
        status.finished_at = Some(Utc::now());
        for node_status in status.node_statuses.values_mut() {
            if !matches!(node_status, NodeStatus::Completed | NodeStatus::CacheHit) {
                *node_status = NodeStatus::Interrupted;
            }
        }
        status.status = ExecutionLifecycleStatus::Cancelled;
        status.phase = ExecutionPhase::Cancelled;
        status.queue_position = None;
        status.cancellation_requested_at = Some(cancellation_requested_at);
        status
    }

    fn reconcile_kernel_lost_execution_status(
        mut status: ExecutionStatus,
        mut node_logs: HashMap<NodeId, NodeLogs>,
        reason: &str,
    ) -> (ExecutionStatus, HashMap<NodeId, NodeLogs>) {
        status = Self::normalize_execution_status(status);
        status.finished_at = Some(Utc::now());
        status.status = ExecutionLifecycleStatus::Failed;
        status.phase = ExecutionPhase::Failed;
        status.queue_position = None;

        for (node_id, node_status) in status.node_statuses.iter_mut() {
            if matches!(
                node_status,
                NodeStatus::Pending | NodeStatus::Queued | NodeStatus::Running
            ) {
                *node_status = NodeStatus::Interrupted;
                let logs = node_logs
                    .entry(node_id.clone())
                    .or_insert_with(|| NodeLogs {
                        stdout: String::new(),
                        stderr: String::new(),
                        outputs: Vec::new(),
                        error: None,
                        duration_ms: None,
                        metrics: HashMap::new(),
                    });
                if logs.stderr.is_empty() {
                    logs.stderr = reason.to_string();
                } else if !logs.stderr.contains(reason) {
                    logs.stderr.push('\n');
                    logs.stderr.push_str(reason);
                }
                logs.error = Some(NodeError {
                    ename: "KernelLost".to_string(),
                    evalue: reason.to_string(),
                    traceback: Vec::new(),
                    hints: Vec::new(),
                });
            }
        }

        (status, node_logs)
    }

    fn reconcile_shutdown_execution_status(
        mut status: ExecutionStatus,
        mut node_logs: HashMap<NodeId, NodeLogs>,
        reason: &str,
        cancellation_requested_at: chrono::DateTime<Utc>,
    ) -> (ExecutionStatus, HashMap<NodeId, NodeLogs>) {
        status = Self::normalize_execution_status(status);
        status.finished_at = Some(Utc::now());
        status.status = ExecutionLifecycleStatus::Cancelled;
        status.phase = ExecutionPhase::Cancelled;
        status.queue_position = None;
        status.queue = None;
        status.cancellation_requested_at = Some(cancellation_requested_at);

        for (node_id, node_status) in status.node_statuses.iter_mut() {
            if matches!(
                node_status,
                NodeStatus::Completed | NodeStatus::CacheHit | NodeStatus::Skipped
            ) {
                continue;
            }

            *node_status = NodeStatus::Interrupted;
            let logs = node_logs
                .entry(node_id.clone())
                .or_insert_with(|| NodeLogs {
                    stdout: String::new(),
                    stderr: String::new(),
                    outputs: Vec::new(),
                    error: None,
                    duration_ms: None,
                    metrics: HashMap::new(),
                });
            if logs.stderr.is_empty() {
                logs.stderr = reason.to_string();
            } else if !logs.stderr.contains(reason) {
                logs.stderr.push('\n');
                logs.stderr.push_str(reason);
            }
        }

        (status, node_logs)
    }

    async fn reconcile_unfinished_executions(pool: &SqlitePool) -> TineResult<()> {
        let rows: Vec<(String, String)> =
            sqlx::query_as("SELECT id, status FROM executions WHERE finished_at IS NULL")
                .fetch_all(pool)
                .await
                .map_err(|e| TineError::Database(e.to_string()))?;
        let mut reconciled = 0usize;
        for (execution_id, status_json) in rows {
            let status = match serde_json::from_str::<ExecutionStatus>(&status_json) {
                Ok(status) => Self::normalize_execution_status(status),
                Err(_) => continue,
            };
            let reconciled_status = Self::reconcile_abandoned_execution_status(status);
            let reconciled_json =
                serde_json::to_string(&reconciled_status).map_err(TineError::Serialization)?;
            sqlx::query(
                "UPDATE executions SET status = ?, finished_at = datetime('now') WHERE id = ? AND finished_at IS NULL",
            )
            .bind(&reconciled_json)
            .bind(&execution_id)
            .execute(pool)
            .await
            .map_err(|e| TineError::Database(e.to_string()))?;
            reconciled += 1;
        }
        if reconciled > 0 {
            warn!(
                reconciled,
                "reconciled unfinished execution records from previous session"
            );
        }
        Ok(())
    }

    async fn reconcile_tree_kernel_lost_executions(
        pool: &SqlitePool,
        tree_id: &ExperimentTreeId,
        reason: &str,
    ) -> TineResult<usize> {
        let rows: Vec<(String, String, Option<String>)> = sqlx::query_as(
            "SELECT id, status, node_logs FROM executions WHERE tree_id = ? AND finished_at IS NULL",
        )
        .bind(tree_id.as_str())
        .fetch_all(pool)
        .await
        .map_err(|e| TineError::Database(e.to_string()))?;

        let mut reconciled = 0usize;
        for (execution_id, status_json, node_logs_json) in rows {
            let status = match serde_json::from_str::<ExecutionStatus>(&status_json) {
                Ok(status) => Self::normalize_execution_status(status),
                Err(_) => continue,
            };
            if status.finished_at.is_some() || status.status != ExecutionLifecycleStatus::Running {
                continue;
            }

            let node_logs: HashMap<NodeId, NodeLogs> = node_logs_json
                .as_deref()
                .and_then(|json| serde_json::from_str(json).ok())
                .unwrap_or_default();
            let (reconciled_status, reconciled_logs) =
                Self::reconcile_kernel_lost_execution_status(status, node_logs, reason);
            let reconciled_status_json =
                serde_json::to_string(&reconciled_status).map_err(TineError::Serialization)?;
            let reconciled_logs_json =
                serde_json::to_string(&reconciled_logs).map_err(TineError::Serialization)?;
            sqlx::query(
                "UPDATE executions SET status = ?, node_logs = ?, finished_at = datetime('now') WHERE id = ? AND finished_at IS NULL",
            )
            .bind(&reconciled_status_json)
            .bind(&reconciled_logs_json)
            .bind(&execution_id)
            .execute(pool)
            .await
            .map_err(|e| TineError::Database(e.to_string()))?;
            reconciled += 1;
        }

        Ok(reconciled)
    }

    async fn reconcile_tree_kernel_shutdown_executions(
        pool: &SqlitePool,
        tree_id: &ExperimentTreeId,
        reason: &str,
        cancellation_requested_at: chrono::DateTime<Utc>,
    ) -> TineResult<usize> {
        let rows: Vec<(String, String, Option<String>)> = sqlx::query_as(
            "SELECT id, status, node_logs FROM executions WHERE tree_id = ? AND finished_at IS NULL",
        )
        .bind(tree_id.as_str())
        .fetch_all(pool)
        .await
        .map_err(|e| TineError::Database(e.to_string()))?;

        let mut reconciled = 0usize;
        for (execution_id, status_json, node_logs_json) in rows {
            let status = match serde_json::from_str::<ExecutionStatus>(&status_json) {
                Ok(status) => Self::normalize_execution_status(status),
                Err(err) => {
                    warn!(
                        tree = %tree_id,
                        execution = %execution_id,
                        error = %err,
                        "skipping execution during shutdown reconciliation: status JSON \
                         could not be parsed (likely DB corruption or schema drift)"
                    );
                    continue;
                }
            };
            if status.finished_at.is_some() {
                continue;
            }

            let node_logs: HashMap<NodeId, NodeLogs> = match node_logs_json.as_deref() {
                Some(json) => match serde_json::from_str(json) {
                    Ok(logs) => logs,
                    Err(err) => {
                        warn!(
                            tree = %tree_id,
                            execution = %execution_id,
                            error = %err,
                            "node_logs JSON could not be parsed during shutdown \
                             reconciliation; falling back to empty logs"
                        );
                        HashMap::new()
                    }
                },
                None => HashMap::new(),
            };
            let (reconciled_status, reconciled_logs) = Self::reconcile_shutdown_execution_status(
                status,
                node_logs,
                reason,
                cancellation_requested_at,
            );
            let reconciled_status_json =
                serde_json::to_string(&reconciled_status).map_err(TineError::Serialization)?;
            let reconciled_logs_json =
                serde_json::to_string(&reconciled_logs).map_err(TineError::Serialization)?;
            sqlx::query(
                "UPDATE executions SET status = ?, node_logs = ?, finished_at = datetime('now') WHERE id = ? AND finished_at IS NULL",
            )
            .bind(&reconciled_status_json)
            .bind(&reconciled_logs_json)
            .bind(&execution_id)
            .execute(pool)
            .await
            .map_err(|e| TineError::Database(e.to_string()))?;
            reconciled += 1;
        }

        Ok(reconciled)
    }

    async fn mark_tree_shutdown_requested(
        pool: &SqlitePool,
        tree_id: &ExperimentTreeId,
        cancellation_requested_at: chrono::DateTime<Utc>,
    ) -> TineResult<usize> {
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT id, status FROM executions WHERE tree_id = ? AND finished_at IS NULL",
        )
        .bind(tree_id.as_str())
        .fetch_all(pool)
        .await
        .map_err(|e| TineError::Database(e.to_string()))?;

        let mut updated = 0usize;
        for (execution_id, status_json) in rows {
            let mut status = match serde_json::from_str::<ExecutionStatus>(&status_json) {
                Ok(status) => Self::normalize_execution_status(status),
                Err(_) => continue,
            };
            if status.finished_at.is_some() {
                continue;
            }

            status.cancellation_requested_at = Some(cancellation_requested_at);
            status.queue_position = None;
            status.queue = None;
            Self::apply_execution_phase(&mut status, ExecutionPhase::CancellationRequested);

            let status_json = serde_json::to_string(&status).map_err(TineError::Serialization)?;
            sqlx::query("UPDATE executions SET status = ? WHERE id = ? AND finished_at IS NULL")
                .bind(&status_json)
                .bind(&execution_id)
                .execute(pool)
                .await
                .map_err(|e| TineError::Database(e.to_string()))?;
            updated += 1;
        }

        Ok(updated)
    }

    async fn collect_workspace_shutdown_tree_ids(&self) -> TineResult<Vec<ExperimentTreeId>> {
        let mut tree_ids: HashSet<ExperimentTreeId> = self
            .tree_runtime_states
            .read()
            .await
            .keys()
            .filter(|tree_id| self.kernel_mgr.has_tree_kernel(tree_id))
            .cloned()
            .collect();

        let execution_tree_ids: Vec<Option<String>> =
            sqlx::query_scalar("SELECT DISTINCT tree_id FROM executions WHERE finished_at IS NULL")
                .fetch_all(&self.pool)
                .await
                .map_err(|e| TineError::Database(e.to_string()))?;

        for tree_id in execution_tree_ids.into_iter().flatten() {
            let tree_id = ExperimentTreeId::new(tree_id);
            if self.kernel_mgr.has_tree_kernel(&tree_id) {
                tree_ids.insert(tree_id);
            }
        }

        let mut tree_ids: Vec<_> = tree_ids.into_iter().collect();
        tree_ids.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        Ok(tree_ids)
    }

    async fn run_branch_execution(
        scheduler: Arc<Scheduler>,
        pool: SqlitePool,
        tree_id: ExperimentTreeId,
        branch_id: BranchId,
        execution_id: ExecutionId,
        executable_branch: ExecutableTreeBranch,
        target: ExecutionTargetRef,
        cache: HashMap<NodeCacheKey, HashMap<SlotName, ArtifactKey>>,
        working_dir: PathBuf,
        // True when the caller already holds the scheduler's per-tree
        // execution lock (run-all holds it across the isolation session).
        tree_lock_held: bool,
    ) -> bool {
        eprintln!(
            "[workspace] branch execution start execution={} tree={} branch={} cwd={}",
            execution_id.as_str(),
            tree_id.as_str(),
            branch_id.as_str(),
            working_dir.display()
        );
        let execution_result = if tree_lock_held {
            scheduler
                .execute_executable_branch_for_target_prelocked(
                    &execution_id,
                    &executable_branch,
                    &target,
                    &cache,
                    Some(&pool),
                    Some(&working_dir),
                )
                .await
        } else {
            scheduler
                .execute_executable_branch_for_target(
                    &execution_id,
                    &executable_branch,
                    &target,
                    &cache,
                    Some(&pool),
                    Some(&working_dir),
                )
                .await
        };
        match execution_result {
            Ok(outcome) => {
                let succeeded = outcome.failed_nodes.is_empty();
                if succeeded {
                    eprintln!(
                        "[workspace] branch execution success execution={} tree={} branch={}",
                        execution_id.as_str(),
                        tree_id.as_str(),
                        branch_id.as_str()
                    );
                } else {
                    eprintln!(
                        "[workspace] branch execution failure execution={} tree={} branch={} failed={:?}",
                        execution_id.as_str(),
                        tree_id.as_str(),
                        branch_id.as_str(),
                        outcome.failed_nodes
                    );
                    warn!(
                        tree = %tree_id,
                        branch = %branch_id,
                        execution = %execution_id,
                        failed_nodes = ?outcome.failed_nodes,
                        "branch execution completed with failed nodes"
                    );
                }
                Self::finalize_branch_execution_success(
                    &pool,
                    &execution_id,
                    &tree_id,
                    &branch_id,
                    &target,
                    outcome,
                )
                .await;
                succeeded
            }
            Err(e) => {
                eprintln!(
                    "[workspace] branch execution failure execution={} tree={} branch={} error={}",
                    execution_id.as_str(),
                    tree_id.as_str(),
                    branch_id.as_str(),
                    e
                );
                error!(tree = %tree_id, branch = %branch_id, execution = %execution_id, error = %e, "branch execution failed");
                Self::finalize_branch_execution_failure(
                    &pool,
                    &execution_id,
                    &tree_id,
                    &branch_id,
                    &target,
                )
                .await;
                false
            }
        }
    }

    pub async fn get_experiment_tree(
        &self,
        tree_id: &ExperimentTreeId,
    ) -> TineResult<ExperimentTreeDef> {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT definition FROM experiment_trees WHERE id = ?")
                .bind(tree_id.as_str())
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| TineError::Database(e.to_string()))?;

        if let Some((def_json,)) = row {
            let def: ExperimentTreeDef = serde_json::from_str(&def_json)?;
            return Ok(def);
        }

        let runtime_row: Option<(String,)> = sqlx::query_as(
            "SELECT definition FROM experiment_trees \
             WHERE ? LIKE id || '__%' \
             ORDER BY length(id) DESC \
             LIMIT 1",
        )
        .bind(tree_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| TineError::Database(e.to_string()))?;
        if let Some((def_json,)) = runtime_row {
            let def: ExperimentTreeDef = serde_json::from_str(&def_json)?;
            return Ok(def);
        }

        Err(TineError::NotFound(format!(
            "experiment tree '{}' not found",
            tree_id
        )))
    }

    async fn hydrate_tree_runtime_states(
        pool: &SqlitePool,
    ) -> TineResult<HashMap<ExperimentTreeId, TreeRuntimeState>> {
        let rows: Vec<(String, Option<String>)> =
            sqlx::query_as("SELECT id, runtime_state FROM experiment_trees")
                .fetch_all(pool)
                .await
                .map_err(|e| TineError::Database(e.to_string()))?;

        let mut states = HashMap::new();
        for (tree_id, runtime_state_json) in rows {
            let Some(runtime_state_json) = runtime_state_json else {
                continue;
            };
            let state: TreeRuntimeState = serde_json::from_str(&runtime_state_json)?;
            states.insert(ExperimentTreeId::new(tree_id), state);
        }
        Ok(states)
    }

    async fn persist_tree_runtime_state_row(
        pool: &SqlitePool,
        state: &TreeRuntimeState,
    ) -> TineResult<()> {
        let runtime_state_json = serde_json::to_string(state)?;
        sqlx::query(
            "UPDATE experiment_trees SET runtime_state = ?, updated_at = datetime('now') WHERE id = ?",
        )
        .bind(&runtime_state_json)
        .bind(state.tree_id.as_str())
        .execute(pool)
        .await
        .map_err(|e| TineError::Database(e.to_string()))?;
        Ok(())
    }

    async fn apply_runtime_state_transition<F>(
        pool: &SqlitePool,
        tree_runtime_states: &Arc<RwLock<HashMap<ExperimentTreeId, TreeRuntimeState>>>,
        tree_id: &ExperimentTreeId,
        update: F,
    ) -> TineResult<Option<TreeRuntimeState>>
    where
        F: FnOnce(&mut TreeRuntimeState),
    {
        let mut guard = tree_runtime_states.write().await;
        let Some(state) = guard.get_mut(tree_id) else {
            return Ok(None);
        };
        update(state);
        let updated = state.clone();
        drop(guard);
        Self::persist_tree_runtime_state_row(pool, &updated).await?;
        Ok(Some(updated))
    }

    fn emit_tree_runtime_state_event(
        event_tx: &tokio::sync::broadcast::Sender<ExecutionEvent>,
        state: &TreeRuntimeState,
    ) {
        let _ = event_tx.send(ExecutionEvent::TreeRuntimeStateChanged {
            tree_id: state.tree_id.clone(),
            branch_id: state.active_branch_id.clone(),
            kernel_state: state.kernel_state.clone(),
            runtime_epoch: state.runtime_epoch,
            last_prepared_cell_id: state.last_prepared_cell_id.clone(),
            materialized_path_cell_ids: state.materialized_path_cell_ids.clone(),
        });
    }

    fn spawn_kernel_lifecycle_bridge(
        pool: SqlitePool,
        tree_runtime_states: Arc<RwLock<HashMap<ExperimentTreeId, TreeRuntimeState>>>,
        mut lifecycle_rx: tokio::sync::broadcast::Receiver<KernelLifecycleEvent>,
        event_tx: tokio::sync::broadcast::Sender<ExecutionEvent>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                match lifecycle_rx.recv().await {
                    Ok(KernelLifecycleEvent::Restarted { tree_id })
                    | Ok(KernelLifecycleEvent::Evicted { tree_id }) => {
                        match Self::apply_runtime_state_transition(
                            &pool,
                            &tree_runtime_states,
                            &tree_id,
                            |state| {
                                state.kernel_state = TreeKernelState::NeedsReplay;
                                state.materialized_path_cell_ids.clear();
                                state.last_prepared_cell_id = None;
                                state.runtime_epoch += 1;
                            },
                        )
                        .await
                        {
                            Ok(Some(state)) => {
                                Self::emit_tree_runtime_state_event(&event_tx, &state)
                            }
                            Ok(None) => {}
                            Err(err) => {
                                warn!(tree = %tree_id, error = %err, "failed to persist replay-needed runtime state");
                            }
                        }
                    }
                    Ok(KernelLifecycleEvent::HeartbeatFailed { tree_id }) => {
                        match Self::apply_runtime_state_transition(
                            &pool,
                            &tree_runtime_states,
                            &tree_id,
                            |state| {
                                state.kernel_state = TreeKernelState::KernelLost;
                                state.materialized_path_cell_ids.clear();
                                state.last_prepared_cell_id = None;
                                state.runtime_epoch += 1;
                            },
                        )
                        .await
                        {
                            Ok(Some(state)) => {
                                if let Err(err) = Self::reconcile_tree_kernel_lost_executions(
                                    &pool,
                                    &tree_id,
                                    "Kernel heartbeat lost while execution was running",
                                )
                                .await
                                {
                                    warn!(tree = %tree_id, error = %err, "failed to reconcile running executions after heartbeat loss");
                                }
                                Self::emit_tree_runtime_state_event(&event_tx, &state)
                            }
                            Ok(None) => {}
                            Err(err) => {
                                warn!(tree = %tree_id, error = %err, "failed to persist kernel-lost runtime state");
                            }
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        warn!(skipped = skipped, "kernel lifecycle bridge lagged behind");
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        })
    }

    /// Hot-path append for `NodeStream` chunks. Stream events arrive at the
    /// rate the kernel is producing stdout/stderr (potentially thousands per
    /// second on a chatty cell). Re-serializing the cumulative `node_logs`
    /// blob to disk on every chunk is O(N²) and historically caused
    /// 100+ ms/chunk persistence work on long-running cells, blocking the
    /// shared SQLite pool and starving status reads.
    ///
    /// We keep stream state in memory and fold it into the next read-modify-
    /// write that happens for any non-stream event (NodeStarted, NodeFailed,
    /// terminal events, etc.). Logs read paths must merge this buffer.
    fn buffer_streaming_chunk(
        streaming: &StreamingLogBuffer,
        execution_id: &ExecutionId,
        node_id: &NodeId,
        stream: &str,
        text: &str,
    ) {
        if text.is_empty() {
            return;
        }
        let mut entry = streaming.entry(execution_id.clone()).or_default();
        let logs = entry
            .entry(node_id.clone())
            .or_insert_with(NodeLogs::default);
        match stream {
            "stderr" => logs.stderr.push_str(text),
            _ => logs.stdout.push_str(text),
        }
    }

    /// Drain any buffered stream chunks for `execution_id` into `node_logs`.
    /// Called as the first step of any non-stream event's read-modify-write
    /// so the persisted blob picks up whatever streaming has accumulated.
    fn drain_streaming_buffer_into(
        streaming: &StreamingLogBuffer,
        execution_id: &ExecutionId,
        node_logs: &mut HashMap<NodeId, NodeLogs>,
    ) -> HashMap<NodeId, NodeLogs> {
        let Some((_, buffered)) = streaming.remove(execution_id) else {
            return HashMap::new();
        };
        for (node_id, buffered_logs) in &buffered {
            let entry = node_logs
                .entry(node_id.clone())
                .or_insert_with(NodeLogs::default);
            entry.stdout.push_str(&buffered_logs.stdout);
            entry.stderr.push_str(&buffered_logs.stderr);
        }
        buffered
    }

    /// Read-only overlay: append any unpersisted stream chunks for
    /// `(execution_id, node_id)` into `target` without removing them from
    /// the buffer. Used by log read paths so a poll during an in-flight
    /// execution sees the latest state. Returns `true` if any data was
    /// appended.
    fn overlay_streaming_buffer(
        streaming: &StreamingLogBuffer,
        execution_id: &ExecutionId,
        node_id: &NodeId,
        target: &mut NodeLogs,
    ) -> bool {
        let Some(entry) = streaming.get(execution_id) else {
            return false;
        };
        let Some(buffered) = entry.get(node_id) else {
            return false;
        };
        if buffered.stdout.is_empty() && buffered.stderr.is_empty() {
            return false;
        }
        target.stdout.push_str(&buffered.stdout);
        target.stderr.push_str(&buffered.stderr);
        true
    }

    #[cfg(test)]
    async fn persist_execution_event_snapshot(
        pool: &SqlitePool,
        streaming: &StreamingLogBuffer,
        event: &ExecutionEvent,
    ) -> TineResult<()> {
        Self::persist_execution_event_snapshot_locked(pool, streaming, None, event).await
    }

    /// Variant of `persist_execution_event_snapshot` that takes the
    /// per-execution lock registry so the read-modify-write critical
    /// section is serialized with the periodic flush path. The two-arg
    /// form above is kept so existing call sites and tests don't need to
    /// thread the registry; production callers should use this variant.
    async fn persist_execution_event_snapshot_locked(
        pool: &SqlitePool,
        streaming: &StreamingLogBuffer,
        locks: Option<&ExecutionLockRegistry>,
        event: &ExecutionEvent,
    ) -> TineResult<()> {
        // Hot-path: streamed stdout/stderr chunks get appended to the in-
        // memory buffer instead of round-tripping through the full
        // executions.node_logs blob. This is the single biggest server-side
        // bottleneck on long-running cells.
        if let ExecutionEvent::NodeStream {
            execution_id,
            node_id,
            stream,
            text,
            ..
        } = event
        {
            Self::buffer_streaming_chunk(streaming, execution_id, node_id, stream, text);
            return Ok(());
        }

        let execution_id = match event {
            ExecutionEvent::ExecutionStarted { execution_id, .. }
            | ExecutionEvent::NodeStarted { execution_id, .. }
            | ExecutionEvent::NodeDisplayData { execution_id, .. }
            | ExecutionEvent::NodeDisplayUpdate { execution_id, .. }
            | ExecutionEvent::ExecutionCompleted { execution_id, .. }
            | ExecutionEvent::ExecutionFailed { execution_id, .. }
            | ExecutionEvent::NodeCompleted { execution_id, .. }
            | ExecutionEvent::NodeCacheHit { execution_id, .. }
            | ExecutionEvent::NodeFailed { execution_id, .. } => execution_id,
            _ => return Ok(()),
        };

        // Serialize this read-modify-write with the periodic flush path
        // for the same execution. Without the lock the two paths can race
        // and one can overwrite the other's update — see the regression
        // test `flush_and_persist_must_not_lose_data_under_concurrent_access`.
        let lock = locks.map(|registry| execution_persist_lock(registry, execution_id));
        let _guard = match lock.as_ref() {
            Some(arc) => Some(arc.lock().await),
            None => None,
        };

        // Optimistic-concurrency loop: the inline finalizers (which write the
        // authoritative node_logs, including rich outputs) do NOT hold this
        // lock. If one flips `finished_at` between our read and our write,
        // writing our stale copy would silently erase the finalized outputs —
        // the conditional UPDATE below detects that and we re-apply the event
        // against the finalized row instead.
        let mut attempts = 0;
        loop {
            attempts += 1;
            let row: Option<(String, Option<String>, i64)> = sqlx::query_as(
                "SELECT status, node_logs, (finished_at IS NULL) FROM executions WHERE id = ?",
            )
            .bind(execution_id.as_str())
            .fetch_optional(pool)
            .await
            .map_err(|e| TineError::Database(e.to_string()))?;
            let Some((status_json, node_logs_json, row_unfinished)) = row else {
                return Ok(());
            };

            let mut status: ExecutionStatus =
                Self::normalize_execution_status(serde_json::from_str(&status_json)?);
            let row_already_finished = row_unfinished == 0 || status.finished_at.is_some();
            let mut node_logs: HashMap<NodeId, NodeLogs> = node_logs_json
                .as_deref()
                .and_then(|json| serde_json::from_str(json).ok())
                .unwrap_or_default();
            // Fold any buffered stream chunks into the canonical blob before
            // applying this event so a subsequent NodeCompleted/Failed/etc. sees
            // the full log.
            let drained_streaming_chunks =
                Self::drain_streaming_buffer_into(streaming, execution_id, &mut node_logs);

            let cancellation_pending = status.cancellation_requested_at.is_some();

            match event {
                ExecutionEvent::ExecutionStarted { .. } => {
                    if !row_already_finished {
                        Self::apply_execution_phase(&mut status, ExecutionPhase::Running);
                    }
                }
                ExecutionEvent::NodeStarted { node_id, .. } => {
                    if !row_already_finished {
                        Self::apply_execution_phase(&mut status, ExecutionPhase::Running);
                        status
                            .node_statuses
                            .insert(node_id.clone(), NodeStatus::Running);
                        node_logs
                            .entry(node_id.clone())
                            .or_insert_with(|| NodeLogs {
                                stdout: String::new(),
                                stderr: String::new(),
                                outputs: Vec::new(),
                                error: None,
                                duration_ms: None,
                                metrics: HashMap::new(),
                            });
                    }
                }
                ExecutionEvent::NodeStream {
                    node_id,
                    stream,
                    text,
                    ..
                } => {
                    let logs = node_logs
                        .entry(node_id.clone())
                        .or_insert_with(|| NodeLogs {
                            stdout: String::new(),
                            stderr: String::new(),
                            outputs: Vec::new(),
                            error: None,
                            duration_ms: None,
                            metrics: HashMap::new(),
                        });
                    match stream.as_str() {
                        "stderr" => logs.stderr.push_str(text),
                        _ => logs.stdout.push_str(text),
                    }
                }
                ExecutionEvent::NodeDisplayData {
                    node_id, output, ..
                }
                | ExecutionEvent::NodeDisplayUpdate {
                    node_id, output, ..
                } => {
                    let logs = node_logs
                        .entry(node_id.clone())
                        .or_insert_with(|| NodeLogs {
                            stdout: String::new(),
                            stderr: String::new(),
                            outputs: Vec::new(),
                            error: None,
                            duration_ms: None,
                            metrics: HashMap::new(),
                        });
                    logs.outputs.push(output.clone());
                }
                ExecutionEvent::NodeCompleted {
                    node_id,
                    duration_ms,
                    ..
                } => {
                    status
                        .node_statuses
                        .insert(node_id.clone(), NodeStatus::Completed);
                    if !row_already_finished {
                        Self::apply_execution_phase(&mut status, ExecutionPhase::Running);
                    }
                    let logs = node_logs
                        .entry(node_id.clone())
                        .or_insert_with(|| NodeLogs {
                            stdout: String::new(),
                            stderr: String::new(),
                            outputs: Vec::new(),
                            error: None,
                            duration_ms: None,
                            metrics: HashMap::new(),
                        });
                    logs.duration_ms = Some(*duration_ms);
                }
                ExecutionEvent::NodeCacheHit { node_id, .. } => {
                    if !row_already_finished {
                        Self::apply_execution_phase(&mut status, ExecutionPhase::Running);
                        status
                            .node_statuses
                            .insert(node_id.clone(), NodeStatus::CacheHit);
                    }
                }
                ExecutionEvent::NodeFailed { node_id, error, .. } => {
                    if !row_already_finished && !cancellation_pending {
                        Self::apply_execution_phase(&mut status, ExecutionPhase::Running);
                    }
                    status.node_statuses.insert(
                        node_id.clone(),
                        if cancellation_pending {
                            NodeStatus::Interrupted
                        } else {
                            NodeStatus::Failed
                        },
                    );
                    let logs = node_logs
                        .entry(node_id.clone())
                        .or_insert_with(|| NodeLogs {
                            stdout: String::new(),
                            stderr: String::new(),
                            outputs: Vec::new(),
                            error: None,
                            duration_ms: None,
                            metrics: HashMap::new(),
                        });
                    logs.error = Some(error.clone());
                }
                // ExecutionCompleted / ExecutionFailed deliberately do NOT
                // terminalize the row: they only act as triggers to fold the
                // remaining buffered stream chunks into the canonical blob.
                // The authoritative finalizer that emitted these events
                // writes the terminal status together with the full node
                // logs (including rich outputs the events don't carry).
                ExecutionEvent::ExecutionCompleted { .. }
                | ExecutionEvent::ExecutionFailed { .. } => {}
                _ => {}
            }

            if !row_already_finished && cancellation_pending && status.finished_at.is_none() {
                Self::apply_execution_phase(&mut status, ExecutionPhase::CancellationRequested);
            }
            // Deliberately NOT terminalizing here even when all nodes look
            // finished: the authoritative finalizers own the terminal
            // transition because only they hold the full outcome (rich
            // outputs, merged logs). If this event-driven path flips
            // `finished_at` first, the real finalizer backs off as the
            // "loser" of the atomic-finalize guard and the outputs are lost —
            // that was a 70%-reproducible bug, not a theoretical race.

            let updated_status_json = serde_json::to_string(&status).unwrap_or_default();
            let updated_logs_json = serde_json::to_string(&node_logs).unwrap_or_default();
            // Conditional on the finished-state we READ: if a finalizer flipped
            // `finished_at` in between, this no-ops instead of clobbering the
            // finalized row, and we retry against the fresh row.
            let update_result = sqlx::query(
                "UPDATE executions \
             SET status = ?, node_logs = ?, finished_at = COALESCE(finished_at, ?) \
             WHERE id = ? AND (finished_at IS NULL) = ?",
            )
            .bind(&updated_status_json)
            .bind(&updated_logs_json)
            .bind(status.finished_at.map(|timestamp| timestamp.to_rfc3339()))
            .bind(execution_id.as_str())
            .bind(row_unfinished)
            .execute(pool)
            .await
            .map_err(|e| TineError::Database(e.to_string()));

            match update_result {
                Ok(result) if result.rows_affected() == 0 && attempts < 3 => {
                    // Lost the race with a finalizer. Re-buffer the drained
                    // chunks so the retry folds them into the finalized row.
                    Self::reinsert_drained_chunks_on_flush_failure(
                        streaming,
                        execution_id,
                        drained_streaming_chunks,
                    );
                    continue;
                }
                Ok(_) => return Ok(()),
                Err(err) => {
                    Self::reinsert_drained_chunks_on_flush_failure(
                        streaming,
                        execution_id,
                        drained_streaming_chunks,
                    );
                    return Err(err);
                }
            }
        }
    }

    fn spawn_execution_event_bridge(
        pool: SqlitePool,
        streaming: Arc<StreamingLogBuffer>,
        locks: Arc<ExecutionLockRegistry>,
        mut event_rx: tokio::sync::broadcast::Receiver<ExecutionEvent>,
        shutdown: Arc<Notify>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            // Hold a future for the shutdown signal so we can race it
            // against `event_rx.recv()` without losing notifications.
            let shutdown_signal = shutdown.notified();
            tokio::pin!(shutdown_signal);

            loop {
                tokio::select! {
                    biased;
                    _ = &mut shutdown_signal => {
                        // Graceful shutdown: drain everything still queued
                        // in the broadcast receiver before exiting so
                        // terminal events and their associated logs land
                        // in the DB. Bounded by the broadcast channel's
                        // capacity, so this loop cannot run forever.
                        Self::drain_bridge_queue(&pool, &streaming, &locks, &mut event_rx).await;
                        break;
                    }
                    res = event_rx.recv() => {
                        match res {
                            Ok(event) => {
                                if let Err(err) = Self::persist_execution_event_snapshot_locked(
                                    &pool,
                                    &streaming,
                                    Some(&locks),
                                    &event,
                                )
                                .await
                                {
                                    warn!(error = %err, "failed to persist incremental execution event");
                                }
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                                warn!(skipped = skipped, "execution event bridge lagged behind");
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                        }
                    }
                }
            }
        })
    }

    /// Drain whatever is currently buffered in the bridge's broadcast
    /// receiver and persist each event. Used during graceful shutdown so
    /// queued events (especially terminal `NodeCompleted` /
    /// `ExecutionCompleted` events that mutate `executions.status` and
    /// `node_logs`) are not silently dropped when we abort the long-
    /// running bridge task.
    async fn drain_bridge_queue(
        pool: &SqlitePool,
        streaming: &StreamingLogBuffer,
        locks: &ExecutionLockRegistry,
        event_rx: &mut tokio::sync::broadcast::Receiver<ExecutionEvent>,
    ) {
        loop {
            match event_rx.try_recv() {
                Ok(event) => {
                    if let Err(err) = Self::persist_execution_event_snapshot_locked(
                        pool,
                        streaming,
                        Some(locks),
                        &event,
                    )
                    .await
                    {
                        warn!(error = %err, "failed to persist event during bridge drain");
                    }
                }
                Err(tokio::sync::broadcast::error::TryRecvError::Empty) => break,
                Err(tokio::sync::broadcast::error::TryRecvError::Closed) => break,
                Err(tokio::sync::broadcast::error::TryRecvError::Lagged(skipped)) => {
                    warn!(skipped = skipped, "execution event bridge drain saw lag");
                }
            }
        }
    }

    /// Periodic flush task: drains the streaming buffer to disk every
    /// `STREAMING_FLUSH_INTERVAL` so a server crash can lose at most one
    /// flush window of buffered stdout/stderr instead of the entire run's
    /// streamed output.
    fn spawn_streaming_log_flush_task(
        pool: SqlitePool,
        streaming: Arc<StreamingLogBuffer>,
        locks: Arc<ExecutionLockRegistry>,
    ) -> tokio::task::JoinHandle<()> {
        const STREAMING_FLUSH_INTERVAL: Duration = Duration::from_secs(1);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(STREAMING_FLUSH_INTERVAL);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                ticker.tick().await;
                if let Err(err) =
                    Self::flush_streaming_log_buffer_locked(&pool, &streaming, Some(&locks)).await
                {
                    warn!(error = %err, "periodic streaming log flush failed");
                }
            }
        })
    }

    async fn flush_streaming_log_buffer_locked(
        pool: &SqlitePool,
        streaming: &StreamingLogBuffer,
        locks: Option<&ExecutionLockRegistry>,
    ) -> TineResult<()> {
        // Snapshot the keys first so we can release the dashmap shard locks
        // before doing per-row DB work, and so concurrent buffer appends
        // for an execution_id we already started flushing aren't dropped.
        let pending_ids: Vec<ExecutionId> =
            streaming.iter().map(|entry| entry.key().clone()).collect();
        for execution_id in pending_ids {
            if let Err(err) = Self::flush_streaming_buffer_for_execution_locked(
                pool,
                streaming,
                locks,
                &execution_id,
            )
            .await
            {
                warn!(
                    execution = %execution_id,
                    error = %err,
                    "failed to flush streaming buffer for execution"
                );
            }
        }
        Ok(())
    }

    async fn flush_streaming_buffer_for_execution_locked(
        pool: &SqlitePool,
        streaming: &StreamingLogBuffer,
        locks: Option<&ExecutionLockRegistry>,
        execution_id: &ExecutionId,
    ) -> TineResult<()> {
        // Acquire the per-execution lock so this read-modify-write is
        // serialized with `persist_execution_event_snapshot_locked` for
        // the same row. See the regression test
        // `flush_and_persist_must_not_lose_data_under_concurrent_access`.
        let lock = locks.map(|registry| execution_persist_lock(registry, execution_id));
        let _guard = match lock.as_ref() {
            Some(arc) => Some(arc.lock().await),
            None => None,
        };

        // Cheap pre-check: nothing buffered for this execution → no work.
        let has_data = streaming
            .get(execution_id)
            .map(|entry| {
                entry
                    .values()
                    .any(|logs| !logs.stdout.is_empty() || !logs.stderr.is_empty())
            })
            .unwrap_or(false);
        if !has_data {
            // Drop any empty placeholder entry so we don't keep iterating it.
            streaming.remove(execution_id);
            return Ok(());
        }

        // Take ownership of the buffered chunks for this execution. If the
        // persist below fails, we re-insert the drained chunks so the next
        // flush retries them. See the regression test
        // `flush_re_buffers_chunks_when_update_fails`.
        let drained = streaming
            .remove(execution_id)
            .map(|(_, v)| v)
            .unwrap_or_default();

        let result =
            Self::write_drained_chunks_to_executions_row(pool, execution_id, &drained).await;

        if result.is_err() {
            Self::reinsert_drained_chunks_on_flush_failure(streaming, execution_id, drained);
        }
        result
    }

    /// Writes the drained streaming chunks for `execution_id` into the
    /// existing `executions.node_logs` blob in a read-modify-write.
    /// Caller is responsible for re-buffering the drained chunks on
    /// failure so transient DB errors don't permanently lose stdout.
    async fn write_drained_chunks_to_executions_row(
        pool: &SqlitePool,
        execution_id: &ExecutionId,
        drained: &HashMap<NodeId, NodeLogs>,
    ) -> TineResult<()> {
        let row: Option<(Option<String>, i64)> =
            sqlx::query_as("SELECT node_logs, (finished_at IS NULL) FROM executions WHERE id = ?")
                .bind(execution_id.as_str())
                .fetch_optional(pool)
                .await
                .map_err(|e| TineError::Database(e.to_string()))?;
        let Some((node_logs_json, row_unfinished)) = row else {
            // Row vanished (e.g. delete_experiment_tree race). Drained
            // chunks are unrecoverable but the row is gone too, so this
            // is not a data loss vs the user's intent.
            return Ok(());
        };

        let mut node_logs: HashMap<NodeId, NodeLogs> = node_logs_json
            .as_deref()
            .and_then(|json| serde_json::from_str(json).ok())
            .unwrap_or_default();

        // Append drained chunks to whatever was already persisted.
        for (node_id, drained_logs) in drained {
            let target = node_logs
                .entry(node_id.clone())
                .or_insert_with(NodeLogs::default);
            target.stdout.push_str(&drained_logs.stdout);
            target.stderr.push_str(&drained_logs.stderr);
        }

        let updated_logs_json =
            serde_json::to_string(&node_logs).map_err(TineError::Serialization)?;
        // Late stream chunks can legitimately arrive AFTER the row turned
        // terminal, so writes are allowed in both states — but only when the
        // finished-state still matches what we read. If a finalizer flipped
        // it in between, writing our stale copy would erase the finalized
        // node_logs (rich outputs); the conditional turns that into a no-op
        // and the error below makes the caller re-buffer the chunks so the
        // next flush retries against the finalized row.
        let result = sqlx::query(
            "UPDATE executions SET node_logs = ? WHERE id = ? AND (finished_at IS NULL) = ?",
        )
        .bind(&updated_logs_json)
        .bind(execution_id.as_str())
        .bind(row_unfinished)
        .execute(pool)
        .await
        .map_err(|e| TineError::Database(e.to_string()))?;
        if result.rows_affected() == 0 {
            return Err(TineError::Database(format!(
                "flush for execution {execution_id} conflicted with a concurrent finalizer; \
                 chunks re-buffered for the next flush"
            )));
        }
        Ok(())
    }

    /// Re-insert previously drained chunks back into the streaming buffer
    /// after a write failure. Drained chunks are placed BEFORE any chunks
    /// that have been concurrently appended to the buffer entry since we
    /// took ownership, preserving emission order (older chunks come first).
    fn reinsert_drained_chunks_on_flush_failure(
        streaming: &StreamingLogBuffer,
        execution_id: &ExecutionId,
        drained: HashMap<NodeId, NodeLogs>,
    ) {
        if drained.is_empty() {
            return;
        }
        let mut entry = streaming.entry(execution_id.clone()).or_default();
        for (node_id, drained_logs) in drained {
            let target = entry.entry(node_id).or_insert_with(NodeLogs::default);
            // Prepend drained text so that, in the buffer, drained chunks
            // appear before any chunks appended concurrently with the
            // failed flush. We rebuild the strings rather than using
            // `insert_str(0, ...)` to keep the implementation simple.
            let mut new_stdout = drained_logs.stdout;
            new_stdout.push_str(&target.stdout);
            target.stdout = new_stdout;
            let mut new_stderr = drained_logs.stderr;
            new_stderr.push_str(&target.stderr);
            target.stderr = new_stderr;
        }
        warn!(
            execution = %execution_id,
            "flush failed; chunks re-buffered for next attempt"
        );
    }

    pub async fn list_experiment_trees(&self) -> TineResult<Vec<ExperimentTreeDef>> {
        let rows: Vec<(String,)> =
            sqlx::query_as("SELECT definition FROM experiment_trees ORDER BY created_at DESC")
                .fetch_all(&self.pool)
                .await
                .map_err(|e| TineError::Database(e.to_string()))?;

        let mut trees = Vec::new();
        for (def_json,) in rows {
            let def: ExperimentTreeDef = serde_json::from_str(&def_json)?;
            trees.push(def);
        }
        Ok(trees)
    }

    pub async fn create_experiment_tree(
        &self,
        name: &str,
        project_id: Option<&ProjectId>,
    ) -> TineResult<ExperimentTreeDef> {
        let ts = chrono::Utc::now().timestamp();
        let safe = name
            .to_lowercase()
            .replace(' ', "_")
            .chars()
            .filter(|c| c.is_alphanumeric() || *c == '_')
            .collect::<String>();
        let id = ExperimentTreeId::new(format!("{safe}_{ts}"));
        let branch_id = BranchId::new("main");
        let cell_id = CellId::new("cell_1");
        let tree = ExperimentTreeDef {
            id: id.clone(),
            name: name.to_string(),
            project_id: project_id.cloned(),
            root_branch_id: branch_id.clone(),
            branches: vec![BranchDef {
                id: branch_id.clone(),
                name: "main".to_string(),
                parent_branch_id: None,
                branch_point_cell_id: None,
                cell_order: vec![cell_id.clone()],
                display: HashMap::new(),
            }],
            cells: vec![CellDef {
                id: cell_id.clone(),
                tree_id: id.clone(),
                branch_id: branch_id.clone(),
                name: "Cell 1".to_string(),
                code: NodeCode {
                    source: String::new(),
                    language: "python".to_string(),
                },
                upstream_cell_ids: vec![],
                declared_outputs: vec![],
                cache: true,
                map_over: None,
                map_concurrency: None,
                tags: HashMap::new(),
                revision_id: None,
                state: CellRuntimeState::Clean,
            }],
            environment: tine_core::EnvironmentSpec::default(),
            execution_mode: tine_core::ExecutionMode::default(),
            budget: None,
            created_at: chrono::Utc::now(),
        };
        self.save_experiment_tree(&tree).await?;
        Ok(tree)
    }

    /// Pre-warm a freshly created experiment's runtime in the background:
    /// verify the environment and boot the tree kernel while the user is
    /// still typing their first cell, so the first run pays only for the
    /// cell itself. Best-effort — failures are logged and the normal
    /// execution path redoes the work.
    pub fn spawn_runtime_prewarm(workspace: Arc<Self>, tree: &ExperimentTreeDef) {
        let tree_id = tree.id.clone();
        let root_branch_id = tree.root_branch_id.clone();
        let descriptor = TreeEnvironmentDescriptor::from_tree(tree);
        let project_id = tree.project_id.clone();
        tokio::spawn(async move {
            let working_dir = match workspace.file_base_for_project(project_id.as_ref()).await {
                Ok(dir) => dir,
                Err(err) => {
                    debug!(tree = %tree_id, error = %err, "runtime prewarm skipped: no working dir");
                    return;
                }
            };
            if let Err(err) = workspace
                .prewarm_tree_runtime(&tree_id, &root_branch_id, &descriptor, &working_dir)
                .await
            {
                debug!(tree = %tree_id, error = %err, "runtime prewarm skipped");
            }
        });
    }

    async fn prewarm_tree_runtime(
        &self,
        tree_id: &ExperimentTreeId,
        root_branch_id: &BranchId,
        descriptor: &TreeEnvironmentDescriptor,
        working_dir: &Path,
    ) -> TineResult<()> {
        let venv_dir = self.env_mgr.ensure_tree_environment(descriptor).await?;
        if self.kernel_mgr.has_tree_kernel(tree_id) {
            return Ok(());
        }
        self.kernel_mgr
            .start_tree_kernel(tree_id, &venv_dir, working_dir)
            .await?;
        // Mark the runtime Ready with an empty materialized path so the
        // first submission reuses this kernel instead of restarting it.
        // Only when no state exists yet: if a real execution raced ahead,
        // its bookkeeping wins (worst case for the rare late overwrite is
        // one defensive kernel restart, never wrong results).
        if self.get_tree_runtime_state(tree_id).await.is_none() {
            let mut state = Self::default_tree_runtime_state(tree_id, root_branch_id);
            state.kernel_state = TreeKernelState::Ready;
            self.set_tree_runtime_state(state).await?;
        }
        info!(tree = %tree_id, "runtime prewarmed");
        Ok(())
    }

    pub async fn delete_experiment_tree(&self, tree_id: &ExperimentTreeId) -> TineResult<()> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| TineError::Database(e.to_string()))?;

        sqlx::query("DELETE FROM cells WHERE tree_id = ?")
            .bind(tree_id.as_str())
            .execute(&mut *tx)
            .await
            .map_err(|e| TineError::Database(e.to_string()))?;

        sqlx::query("DELETE FROM branches WHERE tree_id = ?")
            .bind(tree_id.as_str())
            .execute(&mut *tx)
            .await
            .map_err(|e| TineError::Database(e.to_string()))?;

        let rows = sqlx::query("DELETE FROM experiment_trees WHERE id = ?")
            .bind(tree_id.as_str())
            .execute(&mut *tx)
            .await
            .map_err(|e| TineError::Database(e.to_string()))?
            .rows_affected();
        if rows == 0 {
            return Err(TineError::NotFound(format!(
                "experiment tree '{}' not found",
                tree_id
            )));
        }

        tx.commit()
            .await
            .map_err(|e| TineError::Database(e.to_string()))?;

        let _ = self.kernel_mgr.shutdown_tree(tree_id).await;
        self.tree_runtime_states.write().await.remove(tree_id);
        Ok(())
    }

    pub async fn rename_experiment_tree(
        &self,
        tree_id: &ExperimentTreeId,
        name: &str,
    ) -> TineResult<()> {
        let mut tree = self.get_experiment_tree(tree_id).await?;
        tree.name = name.to_string();
        self.save_experiment_tree(&tree).await?;
        Ok(())
    }

    fn is_auto_named_cell(name: &str) -> bool {
        if let Some(suffix) = name.strip_prefix("Cell ") {
            return suffix.parse::<usize>().is_ok();
        }
        if let Some(suffix) = name.strip_prefix("cell_") {
            return suffix.parse::<usize>().is_ok();
        }
        if name == "branch_cell" {
            return true;
        }
        if let Some(suffix) = name.strip_prefix("branch_cell_") {
            return suffix.parse::<usize>().is_ok();
        }
        false
    }

    fn renumber_auto_named_branch_cells(tree: &mut ExperimentTreeDef, branch_id: &BranchId) {
        let Some(cell_order) = tree
            .branches
            .iter()
            .find(|branch| &branch.id == branch_id)
            .map(|branch| branch.cell_order.clone())
        else {
            return;
        };

        for (idx, cell_id) in cell_order.iter().enumerate() {
            if let Some(cell) = tree
                .cells
                .iter_mut()
                .find(|cell| &cell.id == cell_id && &cell.branch_id == branch_id)
            {
                if Self::is_auto_named_cell(&cell.name) {
                    cell.name = format!("Cell {}", idx + 1);
                }
            }
        }
    }

    pub async fn save_experiment_tree(
        &self,
        def: &ExperimentTreeDef,
    ) -> TineResult<ExperimentTreeDef> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| TineError::Database(e.to_string()))?;

        let def_json = serde_json::to_string(def)?;
        sqlx::query(
            "INSERT OR REPLACE INTO experiment_trees (id, name, project_id, definition, runtime_state, created_at, updated_at) \
             VALUES (?, ?, ?, ?, COALESCE((SELECT runtime_state FROM experiment_trees WHERE id = ?), NULL), COALESCE((SELECT created_at FROM experiment_trees WHERE id = ?), datetime('now')), datetime('now'))",
        )
        .bind(def.id.as_str())
        .bind(&def.name)
        .bind(def.project_id.as_ref().map(|id| id.as_str()))
        .bind(&def_json)
        .bind(def.id.as_str())
        .bind(def.id.as_str())
        .execute(&mut *tx)
        .await
        .map_err(|e| TineError::Database(e.to_string()))?;

        sqlx::query("DELETE FROM cells WHERE tree_id = ?")
            .bind(def.id.as_str())
            .execute(&mut *tx)
            .await
            .map_err(|e| TineError::Database(e.to_string()))?;
        sqlx::query("DELETE FROM branches WHERE tree_id = ?")
            .bind(def.id.as_str())
            .execute(&mut *tx)
            .await
            .map_err(|e| TineError::Database(e.to_string()))?;

        for branch in &def.branches {
            let branch_json = serde_json::to_string(branch)?;
            sqlx::query(
                "INSERT INTO branches (id, tree_id, name, parent_branch_id, branch_point_cell_id, definition, created_at, updated_at) \
                 VALUES (?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))",
            )
            .bind(Self::branch_row_id(&def.id, &branch.id))
            .bind(def.id.as_str())
            .bind(&branch.name)
            .bind(
                branch
                    .parent_branch_id
                    .as_ref()
                    .map(|id| Self::branch_row_id(&def.id, id)),
            )
            .bind(branch.branch_point_cell_id.as_ref().map(|id| id.as_str()))
            .bind(&branch_json)
            .execute(&mut *tx)
            .await
            .map_err(|e| TineError::Database(e.to_string()))?;
        }

        let mut positions = HashMap::<(BranchId, CellId), i64>::new();
        for branch in &def.branches {
            for (idx, cell_id) in branch.cell_order.iter().enumerate() {
                positions.insert((branch.id.clone(), cell_id.clone()), idx as i64);
            }
        }

        for cell in &def.cells {
            let cell_json = serde_json::to_string(cell)?;
            let position = positions
                .get(&(cell.branch_id.clone(), cell.id.clone()))
                .copied()
                .unwrap_or(0);
            sqlx::query(
                "INSERT INTO cells (id, tree_id, branch_id, name, definition, position, created_at, updated_at) \
                 VALUES (?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))",
            )
            .bind(Self::cell_row_id(&def.id, &cell.id))
            .bind(def.id.as_str())
            .bind(Self::branch_row_id(&def.id, &cell.branch_id))
            .bind(&cell.name)
            .bind(&cell_json)
            .bind(position)
            .execute(&mut *tx)
            .await
            .map_err(|e| TineError::Database(e.to_string()))?;
        }

        tx.commit()
            .await
            .map_err(|e| TineError::Database(e.to_string()))?;

        self.get_experiment_tree(&def.id).await
    }

    pub async fn create_branch_in_experiment_tree(
        &self,
        tree_id: &ExperimentTreeId,
        parent_branch_id: &BranchId,
        branch_name: String,
        branch_point_cell_id: &CellId,
        mut first_cell: CellDef,
    ) -> TineResult<BranchId> {
        let mut tree = self.get_experiment_tree(tree_id).await?;
        if !tree
            .branches
            .iter()
            .any(|branch| &branch.id == parent_branch_id)
        {
            return Err(TineError::NotFound(format!(
                "parent branch '{}' not found in tree '{}'",
                parent_branch_id, tree_id
            )));
        }
        if !tree
            .cells
            .iter()
            .any(|cell| &cell.id == branch_point_cell_id)
        {
            return Err(TineError::NotFound(format!(
                "branch point cell '{}' not found in tree '{}'",
                branch_point_cell_id, tree_id
            )));
        }

        let branch_id = BranchId::generate();
        first_cell.tree_id = tree_id.clone();
        first_cell.branch_id = branch_id.clone();

        tree.branches.push(BranchDef {
            id: branch_id.clone(),
            name: branch_name,
            parent_branch_id: Some(parent_branch_id.clone()),
            branch_point_cell_id: Some(branch_point_cell_id.clone()),
            cell_order: vec![first_cell.id.clone()],
            display: HashMap::new(),
        });
        tree.cells.push(first_cell);
        Self::renumber_auto_named_branch_cells(&mut tree, &branch_id);

        self.save_experiment_tree(&tree).await?;
        self.invalidate_tree_runtime_for_mutation(tree_id, None)
            .await?;
        Ok(branch_id)
    }

    pub async fn inspect_branch_target(
        &self,
        tree_id: &ExperimentTreeId,
        branch_id: &BranchId,
        cell_id: &CellId,
    ) -> TineResult<BranchTargetInspection> {
        let tree = self.get_experiment_tree(tree_id).await?;
        Self::validate_branch_membership(&tree, branch_id, cell_id)?;

        let projection = BranchProjection::from_tree(&tree, branch_id)?;
        let current_runtime_state = self.get_tree_runtime_state(tree_id).await;
        let has_live_kernel = self.kernel_mgr.has_tree_kernel(tree_id);
        let planning_state = if has_live_kernel {
            current_runtime_state.as_ref()
        } else {
            None
        };
        let transition = plan_branch_transition(
            planning_state,
            branch_id,
            cell_id,
            &projection.path_cell_order,
        )?;
        let replay_prefix_before_target = transition.replay_prefix_before_target()?;

        Ok(BranchTargetInspection {
            tree_id: tree_id.clone(),
            branch_id: branch_id.clone(),
            target_cell_id: cell_id.clone(),
            lineage: projection.lineage,
            path_cell_order: projection.path_cell_order,
            topo_order: projection.topo_order,
            has_live_kernel,
            current_runtime_state,
            shared_prefix_cell_ids: transition.shared_prefix_cell_ids,
            divergence_cell_id: transition.divergence_cell_id,
            replay_from_idx: transition.replay_from_idx,
            replay_cell_ids: transition.replay_cell_ids,
            replay_prefix_before_target,
        })
    }

    pub async fn inspect_tree_kernel(
        &self,
        tree_id: &ExperimentTreeId,
    ) -> TineResult<RuntimeHealthSnapshot> {
        Ok(self.runtime_health_snapshot(tree_id).await)
    }

    pub async fn shutdown_tree_kernel(
        &self,
        tree_id: &ExperimentTreeId,
    ) -> TineResult<TreeRuntimeState> {
        let cancellation_requested_at = Utc::now();
        let marked =
            Self::mark_tree_shutdown_requested(&self.pool, tree_id, cancellation_requested_at)
                .await?;
        match self.scheduler.interrupt_tree_kernel(tree_id).await {
            Ok(()) => {}
            Err(TineError::KernelNotFound { .. }) => {}
            Err(err) => {
                // Don't abort: shutdown below SIGKILLs as a fallback. Aborting
                // here would leave the DB marked shutdown-requested while the
                // kernel is still alive — an inconsistent state.
                warn!(
                    tree = %tree_id,
                    error = %err,
                    "interrupt_tree_kernel failed during shutdown; proceeding to force shutdown"
                );
            }
        }
        match self.scheduler.shutdown_tree_kernel(tree_id).await {
            Ok(()) => {}
            Err(TineError::KernelNotFound { .. }) => {}
            Err(err) => {
                warn!(
                    tree = %tree_id,
                    error = %err,
                    "shutdown_tree_kernel failed; aborting before marking needs_replay"
                );
                return Err(err);
            }
        }
        let state = self.mark_tree_needs_replay(tree_id).await?;
        let reconciled = Self::reconcile_tree_kernel_shutdown_executions(
            &self.pool,
            tree_id,
            "Tree kernel was shut down while execution was running",
            cancellation_requested_at,
        )
        .await?;
        if marked > 0 || reconciled > 0 {
            warn!(tree = %tree_id, marked, reconciled, "processed active executions during tree kernel shutdown");
        }
        Ok(state)
    }

    pub async fn restart_tree_kernel(&self, tree_id: &ExperimentTreeId) -> TineResult<()> {
        match self.kernel_mgr.restart_tree_kernel(tree_id).await {
            Ok(()) => {}
            Err(TineError::KernelNotFound { .. }) => return Ok(()),
            Err(err) => return Err(err),
        }
        // The kernel process was replaced with a fresh one: empty namespace,
        // no replayed context. The persisted runtime state must reflect that,
        // otherwise it can stay `NeedsReplay` from a prior shutdown while a
        // live kernel now exists — which the UI reads as
        // `needs_replay && has_live_kernel` and hard-disables "Run Branch",
        // forcing "Run All" only. Reset to a clean Ready state with an empty
        // materialized path so the runtime is runnable again. This mirrors
        // `prewarm_tree_runtime`: `Ready` + empty `materialized_path_cell_ids`
        // means "fresh live kernel, no prepared branch context". The next
        // branch run still prepares from scratch because
        // `plan_branch_transition` sees an empty materialized path
        // (`replay_from_idx == 0`), so the entire branch prefix is replayed —
        // the kernel-reuse fast path only triggers when there is nothing to
        // replay. Bump `runtime_epoch` and clear `last_prepared_cell_id` so any
        // stale prepared-branch marker from the previous kernel is discarded.
        self.reset_runtime_state_after_kernel_restart(tree_id).await
    }

    /// Reset the persisted tree runtime state to reflect a freshly restarted
    /// kernel: `Ready` with an empty materialized path and no prepared-branch
    /// marker. See `restart_tree_kernel` for the rationale (UI runnability +
    /// guaranteed context replay on the next run). No-op when no runtime state
    /// exists yet.
    async fn reset_runtime_state_after_kernel_restart(
        &self,
        tree_id: &ExperimentTreeId,
    ) -> TineResult<()> {
        if let Some(mut state) = self.get_tree_runtime_state(tree_id).await {
            state.kernel_state = TreeKernelState::Ready;
            state.materialized_path_cell_ids.clear();
            state.last_prepared_cell_id = None;
            state.runtime_epoch += 1;
            self.set_tree_runtime_state(state).await?;
        }
        Ok(())
    }

    pub async fn add_cell_to_experiment_tree_branch(
        &self,
        tree_id: &ExperimentTreeId,
        branch_id: &BranchId,
        mut cell: CellDef,
        after_cell_id: Option<&CellId>,
    ) -> TineResult<()> {
        let mut tree = self.get_experiment_tree(tree_id).await?;
        let branch = tree
            .branches
            .iter_mut()
            .find(|branch| &branch.id == branch_id)
            .ok_or_else(|| {
                TineError::NotFound(format!(
                    "branch '{}' not found in tree '{}'",
                    branch_id, tree_id
                ))
            })?;

        cell.tree_id = tree_id.clone();
        cell.branch_id = branch_id.clone();

        let insert_at = match after_cell_id {
            Some(cell_id) => branch
                .cell_order
                .iter()
                .position(|existing| existing == cell_id)
                .map(|idx| idx + 1)
                .unwrap_or(branch.cell_order.len()),
            None => branch.cell_order.len(),
        };
        branch.cell_order.insert(insert_at, cell.id.clone());
        tree.cells.push(cell);
        Self::renumber_auto_named_branch_cells(&mut tree, branch_id);

        self.save_experiment_tree(&tree).await?;
        self.invalidate_tree_runtime_for_mutation(tree_id, None)
            .await?;
        Ok(())
    }

    async fn execute_tree_cell_for_target(
        &self,
        tree: &ExperimentTreeDef,
        target_branch_id: &BranchId,
        cell_id: &CellId,
    ) -> TineResult<(ExecutionId, NodeLogs)> {
        let plan = Self::build_tree_cell_execution_plan(tree, target_branch_id, cell_id)?;
        let working_dir = self.file_base_for_project(tree.project_id.as_ref()).await?;
        self.scheduler
            .execute_executable_cell_for_target(
                &plan.executable_branch,
                &plan.executable_cell,
                &plan.target,
                Some(&working_dir),
            )
            .await
    }

    pub async fn submit_cell_execution_in_experiment_tree_branch(
        workspace: Arc<Self>,
        tree_id: &ExperimentTreeId,
        branch_id: &BranchId,
        cell_id: &CellId,
    ) -> TineResult<ExecutionAccepted> {
        Self::submit_cell_execution_in_experiment_tree_branch_with_options(
            workspace, tree_id, branch_id, cell_id, None,
        )
        .await
    }

    /// Like [`Self::submit_cell_execution_in_experiment_tree_branch`], with
    /// an optional client-supplied idempotency key: a retried submission
    /// carrying the same key returns the original execution instead of
    /// starting a duplicate run.
    pub async fn submit_cell_execution_in_experiment_tree_branch_with_options(
        workspace: Arc<Self>,
        tree_id: &ExperimentTreeId,
        branch_id: &BranchId,
        cell_id: &CellId,
        idempotency_key: Option<&str>,
    ) -> TineResult<ExecutionAccepted> {
        let tree = workspace.get_experiment_tree(tree_id).await?;
        Self::validate_branch_membership(&tree, branch_id, cell_id)?;
        let working_dir = workspace
            .file_base_for_project(tree.project_id.as_ref())
            .await?;
        let plan = Self::build_tree_cell_execution_plan(&tree, branch_id, cell_id)?;
        let idempotency_scope = Self::cell_execution_idempotency_scope(tree_id, branch_id, cell_id);
        let idempotency_fingerprint =
            Self::execution_request_fingerprint(&plan.executable_branch, &working_dir);
        let idempotency = idempotency_key.map(|key| ExecutionIdempotency {
            key,
            scope: &idempotency_scope,
            fingerprint: &idempotency_fingerprint,
        });
        if let Some(record) = &idempotency {
            if let Some(existing) =
                Self::find_execution_by_idempotency_key(&workspace.pool, record).await?
            {
                return Ok(ExecutionAccepted::for_cell(
                    existing,
                    tree_id.clone(),
                    branch_id.clone(),
                    cell_id.clone(),
                    Utc::now(),
                ));
            }
        }
        let execution_id = ExecutionId::generate();
        let created_at = Utc::now();
        let topo_order = vec![cell_id.clone()];

        if let Err(err) = Self::insert_branch_execution_record_with_key(
            &workspace.pool,
            &execution_id,
            tree_id,
            branch_id,
            &plan.target,
            &topo_order,
            idempotency.as_ref(),
        )
        .await
        {
            // Unique-key race: a concurrent retry with the same key won the
            // insert — return its execution instead of erroring.
            if let Some(record) = &idempotency {
                if let Some(existing) =
                    Self::find_execution_by_idempotency_key(&workspace.pool, record).await?
                {
                    return Ok(ExecutionAccepted::for_cell(
                        existing,
                        tree_id.clone(),
                        branch_id.clone(),
                        cell_id.clone(),
                        Utc::now(),
                    ));
                }
            }
            return Err(err);
        }
        let queue_position = match workspace.enqueue_execution(&execution_id).await {
            Ok(queue_position) => queue_position,
            Err(err) => {
                workspace.reject_execution(&execution_id).await?;
                return Err(err);
            }
        };

        let execution_id_for_task = execution_id.clone();
        let tree_id_for_task = tree_id.clone();
        let branch_id_for_task = branch_id.clone();
        let cell_id_for_task = cell_id.clone();
        let target_for_task = plan.target.clone();
        let working_dir_for_task = working_dir.clone();
        let workspace_for_task = workspace.clone();
        let pool_for_task = workspace.pool.clone();
        let queue_state_for_task = workspace.execution_queue_state.clone();
        let queue_notify_for_task = workspace.execution_queue_notify.clone();
        let max_concurrent_executions = workspace.max_concurrent_executions;

        tokio::spawn(async move {
            let can_run = match Self::wait_for_execution_slot_with(
                &pool_for_task,
                &queue_state_for_task,
                &queue_notify_for_task,
                max_concurrent_executions,
                &execution_id_for_task,
            )
            .await
            {
                Ok(can_run) => can_run,
                Err(err) => {
                    error!(
                        tree = %tree_id_for_task,
                        branch = %branch_id_for_task,
                        cell = %cell_id_for_task,
                        execution = %execution_id_for_task,
                        error = %err,
                        "failed while waiting for queued cell execution slot"
                    );
                    let _ = Self::finalize_branch_execution_failure(
                        &pool_for_task,
                        &execution_id_for_task,
                        &tree_id_for_task,
                        &branch_id_for_task,
                        &target_for_task,
                    )
                    .await;
                    return;
                }
            };
            if !can_run {
                // Dequeued before getting a slot. Every dequeuer finalizes the
                // record (cancel → finalize_cancelled_execution, run-all
                // cleanup → finalize_branch_execution_failure); this defensive
                // call only fires if a future dequeue path forgets — the
                // finished_at IS NULL guard makes it a no-op otherwise.
                let _ = Self::finalize_cancelled_execution(
                    &pool_for_task,
                    &execution_id_for_task,
                    Utc::now(),
                )
                .await;
                return;
            }

            let cache = match Self::load_cache_from_pool(&workspace_for_task.pool).await {
                Ok(cache) => cache,
                Err(err) => {
                    error!(
                        tree = %tree_id_for_task,
                        branch = %branch_id_for_task,
                        cell = %cell_id_for_task,
                        execution = %execution_id_for_task,
                        error = %err,
                        "failed to load cache for submitted cell execution"
                    );
                    Self::finalize_branch_execution_failure(
                        &pool_for_task,
                        &execution_id_for_task,
                        &tree_id_for_task,
                        &branch_id_for_task,
                        &target_for_task,
                    )
                    .await;
                    Self::release_execution_slot_with(
                        &queue_state_for_task,
                        &queue_notify_for_task,
                        &execution_id_for_task,
                    )
                    .await;
                    return;
                }
            };

            let prepared = match workspace_for_task
                .prepare_context_internal(
                    &tree_id_for_task,
                    &branch_id_for_task,
                    &cell_id_for_task,
                    Some(&execution_id_for_task),
                )
                .await
            {
                Ok(prepared) => prepared,
                Err(err) => {
                    error!(
                        tree = %tree_id_for_task,
                        branch = %branch_id_for_task,
                        cell = %cell_id_for_task,
                        execution = %execution_id_for_task,
                        error = %err,
                        "failed to prepare runtime context for submitted cell execution"
                    );
                    Self::finalize_branch_execution_failure(
                        &pool_for_task,
                        &execution_id_for_task,
                        &tree_id_for_task,
                        &branch_id_for_task,
                        &target_for_task,
                    )
                    .await;
                    Self::release_execution_slot_with(
                        &queue_state_for_task,
                        &queue_notify_for_task,
                        &execution_id_for_task,
                    )
                    .await;
                    return;
                }
            };

            match workspace_for_task
                .scheduler
                .execute_executable_branch_for_target(
                    &execution_id_for_task,
                    &plan.executable_branch,
                    &target_for_task,
                    &cache,
                    Some(&workspace_for_task.pool),
                    Some(&working_dir_for_task),
                )
                .await
            {
                Ok(outcome) => {
                    Self::finalize_branch_execution_success(
                        &pool_for_task,
                        &execution_id_for_task,
                        &tree_id_for_task,
                        &branch_id_for_task,
                        &target_for_task,
                        outcome,
                    )
                    .await;
                    let mut runtime_state = prepared.runtime_state;
                    runtime_state.kernel_state = TreeKernelState::Ready;
                    runtime_state.last_prepared_cell_id = Some(cell_id_for_task.clone());
                    if let Err(err) = workspace_for_task
                        .set_tree_runtime_state(runtime_state)
                        .await
                    {
                        warn!(
                            tree = %tree_id_for_task,
                            branch = %branch_id_for_task,
                            cell = %cell_id_for_task,
                            execution = %execution_id_for_task,
                            error = %err,
                            "failed to persist runtime state after submitted cell execution"
                        );
                    }
                }
                Err(err) => {
                    error!(
                        tree = %tree_id_for_task,
                        branch = %branch_id_for_task,
                        cell = %cell_id_for_task,
                        execution = %execution_id_for_task,
                        error = %err,
                        "submitted cell execution failed"
                    );
                    Self::finalize_branch_execution_failure(
                        &pool_for_task,
                        &execution_id_for_task,
                        &tree_id_for_task,
                        &branch_id_for_task,
                        &target_for_task,
                    )
                    .await;
                }
            }

            workspace_for_task
                .release_execution_slot(&execution_id_for_task)
                .await;
        });

        Ok(ExecutionAccepted::for_cell(
            execution_id,
            tree_id.clone(),
            branch_id.clone(),
            cell_id.clone(),
            created_at,
        )
        .with_queue_position(Some(queue_position)))
    }

    async fn invalidate_tree_runtime_for_mutation(
        &self,
        tree_id: &ExperimentTreeId,
        changed_cell_id: Option<&CellId>,
    ) -> TineResult<()> {
        if let Some(changed_cell_id) = changed_cell_id {
            self.mark_stale_descendants_compat(tree_id, changed_cell_id)
                .await?;
        }
        if let Some(state) = self.get_tree_runtime_state(tree_id).await {
            // A ready runtime with nothing materialized cannot be staled by
            // edits — the namespace is empty. Downgrading it would discard
            // the prewarmed kernel right as the user types their first cell.
            if state.kernel_state == TreeKernelState::Ready
                && state.materialized_path_cell_ids.is_empty()
            {
                return Ok(());
            }
            self.mark_tree_needs_replay(tree_id).await?;
        }
        Ok(())
    }

    async fn persist_single_node_execution(
        &self,
        node_id: &NodeId,
        execution_id: &ExecutionId,
        logs: &NodeLogs,
        tree_id: Option<&ExperimentTreeId>,
        branch_id: Option<&BranchId>,
        target_kind: ExecutionTargetKind,
        target: ExecutionTargetRef,
    ) -> TineResult<()> {
        let mut node_statuses = HashMap::new();
        node_statuses.insert(
            node_id.clone(),
            if logs.error.is_some() {
                tine_core::NodeStatus::Failed
            } else {
                tine_core::NodeStatus::Completed
            },
        );
        let status = ExecutionStatus {
            execution_id: execution_id.clone(),
            tree_id: tree_id.cloned(),
            branch_id: branch_id.cloned(),
            target_kind: Some(target_kind),
            target: Some(target.clone()),
            status: if logs.error.is_some() {
                ExecutionLifecycleStatus::Failed
            } else {
                ExecutionLifecycleStatus::Completed
            },
            phase: if logs.error.is_some() {
                ExecutionPhase::Failed
            } else {
                ExecutionPhase::Completed
            },
            queue_position: None,
            queue: None,
            runtime: None,
            cancellation_requested_at: None,
            node_statuses,
            started_at: Utc::now()
                - chrono::Duration::milliseconds(logs.duration_ms.unwrap_or_default() as i64),
            finished_at: Some(Utc::now()),
        };
        let status_json = serde_json::to_string(&status).unwrap_or_default();
        let mut node_logs = HashMap::new();
        node_logs.insert(node_id.clone(), logs.clone());
        let logs_json = serde_json::to_string(&node_logs).unwrap_or_default();

        sqlx::query(
            "INSERT INTO executions (id, tree_id, branch_id, target_kind, status, node_logs, started_at, finished_at) \
             VALUES (?, ?, ?, ?, ?, ?, datetime('now', ?), datetime('now'))"
        )
        .bind(execution_id.as_str())
        .bind(status.tree_id.as_ref().map(|id| id.as_str()))
        .bind(status.branch_id.as_ref().map(|id| id.as_str()))
        .bind(
            status
                .target_kind
                .as_ref()
                .map(|_| "experiment_tree_branch"),
        )
        .bind(&status_json)
        .bind(&logs_json)
        .bind(format!("-{} seconds", logs.duration_ms.unwrap_or_default() / 1000))
        .execute(&self.pool)
        .await
        .map_err(|e| TineError::Database(e.to_string()))?;

        self.persist_metrics(execution_id, &node_logs).await
    }

    pub async fn update_cell_code_in_experiment_tree(
        &self,
        tree_id: &ExperimentTreeId,
        cell_id: &CellId,
        code: &str,
    ) -> TineResult<()> {
        let mut tree = self.get_experiment_tree(tree_id).await?;
        let cell = tree
            .cells
            .iter_mut()
            .find(|cell| &cell.id == cell_id)
            .ok_or_else(|| {
                TineError::NotFound(format!(
                    "cell '{}' not found in tree '{}'",
                    cell_id, tree_id
                ))
            })?;
        cell.code.source = code.to_string();
        cell.state = CellRuntimeState::Clean;
        self.save_experiment_tree(&tree).await?;
        self.invalidate_tree_runtime_for_mutation(tree_id, Some(cell_id))
            .await?;
        Ok(())
    }

    pub async fn update_cell_code_in_experiment_tree_branch(
        &self,
        tree_id: &ExperimentTreeId,
        branch_id: &BranchId,
        cell_id: &CellId,
        code: &str,
    ) -> TineResult<()> {
        let tree = self.get_experiment_tree(tree_id).await?;
        Self::validate_branch_membership(&tree, branch_id, cell_id)?;
        self.update_cell_code_in_experiment_tree(tree_id, cell_id, code)
            .await
    }

    pub async fn move_cell_in_experiment_tree_branch(
        &self,
        tree_id: &ExperimentTreeId,
        branch_id: &BranchId,
        cell_id: &CellId,
        direction: &str,
    ) -> TineResult<()> {
        let mut tree = self.get_experiment_tree(tree_id).await?;
        let branch = tree
            .branches
            .iter_mut()
            .find(|branch| &branch.id == branch_id)
            .ok_or_else(|| {
                TineError::NotFound(format!(
                    "branch '{}' not found in tree '{}'",
                    branch_id, tree_id
                ))
            })?;
        let idx = branch
            .cell_order
            .iter()
            .position(|existing| existing == cell_id)
            .ok_or_else(|| {
                TineError::NotFound(format!(
                    "cell '{}' not found in branch '{}' for tree '{}'",
                    cell_id, branch_id, tree_id
                ))
            })?;
        let target = match direction {
            "up" if idx > 0 => idx - 1,
            "down" if idx + 1 < branch.cell_order.len() => idx + 1,
            _ => return Ok(()),
        };
        branch.cell_order.swap(idx, target);
        Self::renumber_auto_named_branch_cells(&mut tree, branch_id);
        self.save_experiment_tree(&tree).await?;
        self.invalidate_tree_runtime_for_mutation(tree_id, None)
            .await?;
        Ok(())
    }

    pub async fn delete_cell_from_experiment_tree_branch(
        &self,
        tree_id: &ExperimentTreeId,
        branch_id: &BranchId,
        cell_id: &CellId,
    ) -> TineResult<()> {
        let mut tree = self.get_experiment_tree(tree_id).await?;
        let branch = tree
            .branches
            .iter_mut()
            .find(|branch| &branch.id == branch_id)
            .ok_or_else(|| {
                TineError::NotFound(format!(
                    "branch '{}' not found in tree '{}'",
                    branch_id, tree_id
                ))
            })?;
        let before = branch.cell_order.len();
        branch.cell_order.retain(|existing| existing != cell_id);
        if branch.cell_order.len() == before {
            return Err(TineError::NotFound(format!(
                "cell '{}' not found in branch '{}' for tree '{}'",
                cell_id, branch_id, tree_id
            )));
        }
        tree.cells.retain(|cell| &cell.id != cell_id);
        Self::renumber_auto_named_branch_cells(&mut tree, branch_id);
        self.save_experiment_tree(&tree).await?;
        self.invalidate_tree_runtime_for_mutation(tree_id, None)
            .await?;
        Ok(())
    }

    pub async fn delete_experiment_tree_branch(
        &self,
        tree_id: &ExperimentTreeId,
        branch_id: &BranchId,
    ) -> TineResult<()> {
        let mut tree = self.get_experiment_tree(tree_id).await?;
        if &tree.root_branch_id == branch_id {
            return Err(TineError::Config(
                "cannot delete the main branch; delete cells inside it instead".to_string(),
            ));
        }
        if !tree.branches.iter().any(|branch| &branch.id == branch_id) {
            return Err(TineError::NotFound(format!(
                "branch '{}' not found in tree '{}'",
                branch_id, tree_id
            )));
        }

        let mut removed_branch_ids = HashSet::from([branch_id.clone()]);
        loop {
            let next = tree
                .branches
                .iter()
                .filter_map(|branch| {
                    branch.parent_branch_id.as_ref().and_then(|parent| {
                        if removed_branch_ids.contains(parent)
                            && !removed_branch_ids.contains(&branch.id)
                        {
                            Some(branch.id.clone())
                        } else {
                            None
                        }
                    })
                })
                .collect::<Vec<_>>();
            if next.is_empty() {
                break;
            }
            removed_branch_ids.extend(next);
        }

        tree.branches
            .retain(|branch| !removed_branch_ids.contains(&branch.id));
        tree.cells
            .retain(|cell| !removed_branch_ids.contains(&cell.branch_id));
        self.save_experiment_tree(&tree).await?;

        if let Some(mut state) = self.get_tree_runtime_state(tree_id).await {
            if removed_branch_ids.contains(&state.active_branch_id) {
                state.active_branch_id = tree.root_branch_id.clone();
                state.materialized_path_cell_ids.clear();
                state.last_prepared_cell_id = None;
                state.kernel_state = TreeKernelState::NeedsReplay;
                state.runtime_epoch += 1;
                self.set_tree_runtime_state(state).await?;
            } else {
                self.mark_tree_needs_replay(tree_id).await?;
            }
        }
        Ok(())
    }

    pub async fn execute_cell_in_experiment_tree_branch(
        &self,
        tree_id: &ExperimentTreeId,
        branch_id: &BranchId,
        cell_id: &CellId,
    ) -> TineResult<(ExecutionId, NodeLogs)> {
        let prepared = self.prepare_context(tree_id, branch_id, cell_id).await?;
        let tree = self.get_experiment_tree(tree_id).await?;
        let target = Self::execution_target_for_tree_branch(tree_id, branch_id);
        let (execution_id, logs) = self
            .execute_tree_cell_for_target(&tree, branch_id, cell_id)
            .await?;
        self.persist_single_node_execution(
            &NodeId::new(cell_id.as_str()),
            &execution_id,
            &logs,
            Some(tree_id),
            Some(branch_id),
            ExecutionTargetKind::ExperimentTreeBranch,
            target,
        )
        .await?;
        let mut runtime_state = prepared.runtime_state;
        runtime_state.kernel_state = TreeKernelState::Ready;
        runtime_state.last_prepared_cell_id = Some(cell_id.clone());
        self.set_tree_runtime_state(runtime_state).await?;
        Ok((execution_id, logs))
    }

    pub async fn execute_branch_in_experiment_tree(
        &self,
        tree_id: &ExperimentTreeId,
        branch_id: &BranchId,
    ) -> TineResult<ExecutionId> {
        self.execute_branch_in_experiment_tree_with_options(tree_id, branch_id, None)
            .await
    }

    /// Like [`Self::execute_branch_in_experiment_tree`], with an optional
    /// client-supplied idempotency key: a retried submission carrying the
    /// same key returns the original execution instead of starting a
    /// duplicate run.
    pub async fn execute_branch_in_experiment_tree_with_options(
        &self,
        tree_id: &ExperimentTreeId,
        branch_id: &BranchId,
        idempotency_key: Option<&str>,
    ) -> TineResult<ExecutionId> {
        let tree = self.get_experiment_tree(tree_id).await?;
        let working_dir = self.file_base_for_project(tree.project_id.as_ref()).await?;
        let plan = Self::build_tree_branch_execution_plan(&tree, branch_id)?;
        let idempotency_scope = Self::branch_execution_idempotency_scope(tree_id, branch_id);
        let idempotency_fingerprint =
            Self::execution_request_fingerprint(&plan.executable_branch, &working_dir);
        let idempotency = idempotency_key.map(|key| ExecutionIdempotency {
            key,
            scope: &idempotency_scope,
            fingerprint: &idempotency_fingerprint,
        });
        if let Some(record) = &idempotency {
            if let Some(existing) =
                Self::find_execution_by_idempotency_key(&self.pool, record).await?
            {
                return Ok(existing);
            }
        }
        let exec_id = ExecutionId::generate();
        if let Err(err) = Self::insert_branch_execution_record_with_key(
            &self.pool,
            &exec_id,
            tree_id,
            branch_id,
            &plan.target,
            &plan.executable_branch.topo_order,
            idempotency.as_ref(),
        )
        .await
        {
            // Unique-key race: a concurrent retry with the same key won the
            // insert — return its execution instead of erroring.
            if let Some(record) = &idempotency {
                if let Some(existing) =
                    Self::find_execution_by_idempotency_key(&self.pool, record).await?
                {
                    return Ok(existing);
                }
            }
            return Err(err);
        }
        if let Err(err) = self.enqueue_execution(&exec_id).await {
            self.reject_execution(&exec_id).await?;
            return Err(err);
        }

        let pool_for_task = self.pool.clone();
        let scheduler = self.scheduler.clone();
        let queue_state_for_task = self.execution_queue_state.clone();
        let queue_notify_for_task = self.execution_queue_notify.clone();
        let max_concurrent_executions = self.max_concurrent_executions;
        let eid = exec_id.clone();
        let tid = tree_id.clone();
        let bid = branch_id.clone();
        let target_for_task = plan.target.clone();
        let working_dir_for_task = working_dir.clone();
        tokio::spawn(async move {
            let can_run = match Self::wait_for_execution_slot_with(
                &pool_for_task,
                &queue_state_for_task,
                &queue_notify_for_task,
                max_concurrent_executions,
                &eid,
            )
            .await
            {
                Ok(can_run) => can_run,
                Err(err) => {
                    error!(tree = %tid, branch = %bid, execution = %eid, error = %err, "failed while waiting for queued branch execution slot");
                    let _ = Self::finalize_branch_execution_failure(
                        &pool_for_task,
                        &eid,
                        &tid,
                        &bid,
                        &target_for_task,
                    )
                    .await;
                    return;
                }
            };
            if !can_run {
                // Dequeued before getting a slot; the dequeuer finalized the
                // record. Defensive idempotent backstop (no-op when terminal).
                let _ = Self::finalize_cancelled_execution(&pool_for_task, &eid, Utc::now()).await;
                return;
            }

            let cache = match Self::load_cache_from_pool(&pool_for_task).await {
                Ok(cache) => cache,
                Err(e) => {
                    error!(tree = %tid, branch = %bid, execution = %eid, error = %e, "failed to load cache for branch execution");
                    Self::finalize_branch_execution_failure(
                        &pool_for_task,
                        &eid,
                        &tid,
                        &bid,
                        &target_for_task,
                    )
                    .await;
                    Self::release_execution_slot_with(
                        &queue_state_for_task,
                        &queue_notify_for_task,
                        &eid,
                    )
                    .await;
                    return;
                }
            };

            Self::run_branch_execution(
                scheduler,
                pool_for_task.clone(),
                tid,
                bid,
                eid.clone(),
                plan.executable_branch,
                target_for_task,
                cache,
                working_dir_for_task,
                false,
            )
            .await;
            Self::release_execution_slot_with(&queue_state_for_task, &queue_notify_for_task, &eid)
                .await;
        });

        Ok(exec_id)
    }

    /// Execute all branches in the given experiment tree in deterministic order.
    ///
    /// **Execution Contract**:
    /// A tree runtime may be reused only while its state remains truthful for the next requested branch path.
    /// `run all` adds a stronger rule: sibling branches must execute in isolation from one another.
    /// Namespace-guarded isolation is one strategy for preserving that invariant; contamination or any
    /// indeterminate guard failure immediately invalidates runtime reuse and requires replay or restart
    /// before further branch work.
    pub async fn execute_all_branches_in_experiment_tree(
        &self,
        tree_id: &ExperimentTreeId,
    ) -> TineResult<Vec<(BranchId, ExecutionId)>> {
        let tree = self.get_experiment_tree(tree_id).await?;
        if self.get_tree_runtime_state(tree_id).await.is_none() {
            self.set_tree_runtime_state(Self::default_tree_runtime_state(
                tree_id,
                &tree.root_branch_id,
            ))
            .await?;
        }
        let working_dir = self.file_base_for_project(tree.project_id.as_ref()).await?;
        let branch_ids = Self::ordered_branch_ids(&tree)?;

        let mut runs = Vec::with_capacity(branch_ids.len());
        let mut response = Vec::with_capacity(branch_ids.len());
        for branch_id in branch_ids {
            let plan = Self::build_tree_branch_execution_plan(&tree, &branch_id)?;
            let exec_id = ExecutionId::generate();
            Self::insert_branch_execution_record(
                &self.pool,
                &exec_id,
                tree_id,
                &branch_id,
                &plan.target,
                &plan.executable_branch.topo_order,
            )
            .await?;
            if let Err(err) = self.enqueue_execution(&exec_id).await {
                self.reject_execution(&exec_id).await?;
                return Err(err);
            }
            response.push((branch_id.clone(), exec_id.clone()));
            runs.push((branch_id, exec_id, plan.executable_branch, plan.target));
        }

        let scheduler = self.scheduler.clone();
        let kernel_mgr = self.kernel_mgr.clone();
        let env_mgr = self.env_mgr.clone();
        let pool = self.pool.clone();
        let execution_queue_state = self.execution_queue_state.clone();
        let execution_queue_notify = self.execution_queue_notify.clone();
        let max_concurrent_executions = self.max_concurrent_executions;
        let tree_runtime_states = self.tree_runtime_states.clone();
        let tid = tree_id.clone();
        let event_tx = self.scheduler.event_sender();
        let working_dir_for_task = working_dir.clone();
        tokio::spawn(async move {
            let mut runs_iter = runs.into_iter();
            while let Some((branch_id, exec_id, executable_branch, target)) = runs_iter.next() {
                let can_run = match Self::wait_for_execution_slot_with(
                    &pool,
                    &execution_queue_state,
                    &execution_queue_notify,
                    max_concurrent_executions,
                    &exec_id,
                )
                .await
                {
                    Ok(can_run) => can_run,
                    Err(err) => {
                        error!(tree = %tid, branch = %branch_id, execution = %exec_id, error = %err, "failed while waiting for queued guarded execute-all slot");
                        Self::finalize_branch_execution_failure(
                            &pool, &exec_id, &tid, &branch_id, &target,
                        )
                        .await;
                        continue;
                    }
                };
                if !can_run {
                    // Dequeued before getting a slot; the dequeuer finalized
                    // the record. Defensive idempotent backstop, then move on
                    // to the next branch in the run-all sequence.
                    let _ = Self::finalize_cancelled_execution(&pool, &exec_id, Utc::now()).await;
                    continue;
                }

                // Load the cache fresh per branch (mirrors the single-branch
                // path) so branch N+1 can hit entries written by branch N
                // within this same run-all.
                let cache = match Self::load_cache_from_pool(&pool).await {
                    Ok(cache) => cache,
                    Err(err) => {
                        error!(tree = %tid, branch = %branch_id, execution = %exec_id, error = %err, "failed to load cache for guarded execute-all branch");
                        Self::finalize_branch_execution_failure(
                            &pool, &exec_id, &tid, &branch_id, &target,
                        )
                        .await;
                        Self::release_execution_slot_with(
                            &execution_queue_state,
                            &execution_queue_notify,
                            &exec_id,
                        )
                        .await;
                        continue;
                    }
                };

                // Hold the per-tree execution lock across the whole guarded
                // block (session begin → branch execution → session end) so a
                // queued same-tree execution cannot slip inside the isolation
                // session window.
                let tree_exec_lock = scheduler.tree_execution_lock(&tid);
                let _tree_guard = tree_exec_lock.lock().await;

                let mut restart_after_teardown = false;
                let branch_id_for_isolation = branch_id.clone();
                let session_id = exec_id.as_str().to_string();
                let _ = event_tx.send(ExecutionEvent::IsolationAttempted {
                    execution_id: exec_id.clone(),
                    tree_id: tid.clone(),
                    branch_id: branch_id.clone(),
                });

                if !kernel_mgr.has_tree_kernel(&tid) {
                    let tree_env = TreeEnvironmentDescriptor::new(
                        tid.clone(),
                        executable_branch.project_id.clone(),
                        executable_branch.environment.clone(),
                    );
                    match env_mgr.ensure_tree_environment(&tree_env).await {
                        Ok(venv_dir) => {
                            if let Err(err) = kernel_mgr
                                .start_tree_kernel(&tid, &venv_dir, &working_dir_for_task)
                                .await
                            {
                                let _ = event_tx.send(ExecutionEvent::FallbackRestartTriggered {
                                    execution_id: exec_id.clone(),
                                    tree_id: tid.clone(),
                                    branch_id: branch_id.clone(),
                                    reason: format!("failed_to_start_guarded_kernel:{}", err),
                                });
                                restart_after_teardown = true;
                            }
                        }
                        Err(err) => {
                            let _ = event_tx.send(ExecutionEvent::FallbackRestartTriggered {
                                execution_id: exec_id.clone(),
                                tree_id: tid.clone(),
                                branch_id: branch_id.clone(),
                                reason: format!("failed_to_prepare_guarded_environment:{}", err),
                            });
                            restart_after_teardown = true;
                        }
                    }
                }

                let mut begin_failure_outcome: Option<KernelIsolationOutcome> = None;
                let mut used_namespace_guard = false;
                if !restart_after_teardown {
                    match kernel_mgr
                        .begin_tree_branch_session(&tid, &session_id)
                        .await
                    {
                        Ok(()) => {
                            used_namespace_guard = true;
                        }
                        Err(err) => {
                            let failed_outcome = KernelIsolationOutcome {
                                contaminated: true,
                                signals: vec!["session_begin_failed".to_string()],
                                delta: tine_core::NamespaceDelta::default(),
                            };
                            begin_failure_outcome = Some(failed_outcome.clone());
                            let _ = Self::record_isolation_result(
                                &pool,
                                &tree_runtime_states,
                                &tid,
                                &branch_id_for_isolation,
                                &failed_outcome,
                            )
                            .await;
                            let _ = event_tx.send(ExecutionEvent::FallbackRestartTriggered {
                                execution_id: exec_id.clone(),
                                tree_id: tid.clone(),
                                branch_id: branch_id.clone(),
                                reason: format!("failed_to_begin_branch_session:{}", err),
                            });
                            restart_after_teardown = true;
                            if kernel_mgr.has_tree_kernel(&tid) {
                                let _ = kernel_mgr.shutdown_tree(&tid).await;
                            }
                        }
                    }
                }

                let succeeded = Self::run_branch_execution(
                    scheduler.clone(),
                    pool.clone(),
                    tid.clone(),
                    branch_id.clone(),
                    exec_id.clone(),
                    executable_branch,
                    target.clone(),
                    cache,
                    working_dir_for_task.clone(),
                    true,
                )
                .await;

                if let Some(outcome) = begin_failure_outcome.as_ref() {
                    let _ = Self::record_isolation_result(
                        &pool,
                        &tree_runtime_states,
                        &tid,
                        &branch_id_for_isolation,
                        outcome,
                    )
                    .await;
                }

                if used_namespace_guard {
                    match kernel_mgr.end_tree_branch_session(&tid, &session_id).await {
                        Ok(outcome) => {
                            let _ = Self::record_isolation_result(
                                &pool,
                                &tree_runtime_states,
                                &tid,
                                &branch_id_for_isolation,
                                &outcome,
                            )
                            .await;
                            if outcome.contaminated {
                                let _ = event_tx.send(ExecutionEvent::ContaminationDetected {
                                    execution_id: exec_id.clone(),
                                    tree_id: tid.clone(),
                                    branch_id: branch_id_for_isolation.clone(),
                                    signals: outcome.signals.clone(),
                                });
                                let _ = event_tx.send(ExecutionEvent::FallbackRestartTriggered {
                                    execution_id: exec_id.clone(),
                                    tree_id: tid.clone(),
                                    branch_id: branch_id_for_isolation.clone(),
                                    reason: "contamination_detected".to_string(),
                                });
                                restart_after_teardown = true;
                            } else {
                                let _ = event_tx.send(ExecutionEvent::IsolationSucceeded {
                                    execution_id: exec_id.clone(),
                                    tree_id: tid.clone(),
                                    branch_id: branch_id_for_isolation.clone(),
                                    delta: outcome.delta.clone(),
                                });
                            }
                        }
                        Err(err) => {
                            let failed_outcome = KernelIsolationOutcome {
                                contaminated: true,
                                signals: vec!["session_end_failed".to_string()],
                                delta: tine_core::NamespaceDelta::default(),
                            };
                            let _ = Self::record_isolation_result(
                                &pool,
                                &tree_runtime_states,
                                &tid,
                                &branch_id_for_isolation,
                                &failed_outcome,
                            )
                            .await;
                            let _ = event_tx.send(ExecutionEvent::FallbackRestartTriggered {
                                execution_id: exec_id.clone(),
                                tree_id: tid.clone(),
                                branch_id: branch_id_for_isolation.clone(),
                                reason: format!("failed_to_end_branch_session:{}", err),
                            });
                            restart_after_teardown = true;
                        }
                    }
                }

                Self::release_execution_slot_with(
                    &execution_queue_state,
                    &execution_queue_notify,
                    &exec_id,
                )
                .await;

                if restart_after_teardown && kernel_mgr.has_tree_kernel(&tid) {
                    let _ = kernel_mgr.shutdown_tree(&tid).await;
                }

                if !succeeded {
                    warn!(tree = %tid, branch = %branch_id, execution = %exec_id, "stopping execute-all after first branch failure");
                    for (remaining_branch_id, remaining_exec_id, _, remaining_target) in runs_iter {
                        let _ = Self::dequeue_execution_with(
                            &pool,
                            &execution_queue_state,
                            &execution_queue_notify,
                            &remaining_exec_id,
                        )
                        .await;
                        warn!(tree = %tid, branch = %remaining_branch_id, execution = %remaining_exec_id, "marking remaining run-all branch as failed after earlier branch failure");
                        Self::finalize_branch_execution_failure(
                            &pool,
                            &remaining_exec_id,
                            &tid,
                            &remaining_branch_id,
                            &remaining_target,
                        )
                        .await;
                    }
                    break;
                }
            }
        });

        Ok(response)
    }

    pub fn descendant_cell_ids_compat(tree: &ExperimentTreeDef, cell_id: &CellId) -> Vec<CellId> {
        let mut adjacency: HashMap<CellId, Vec<CellId>> = HashMap::new();
        for cell in &tree.cells {
            for upstream in &cell.upstream_cell_ids {
                adjacency
                    .entry(upstream.clone())
                    .or_default()
                    .push(cell.id.clone());
            }
        }

        let mut seen = HashSet::new();
        let mut stack = adjacency.get(cell_id).cloned().unwrap_or_default();
        let mut descendants = Vec::new();

        while let Some(next) = stack.pop() {
            if !seen.insert(next.clone()) {
                continue;
            }
            descendants.push(next.clone());
            if let Some(children) = adjacency.get(&next) {
                stack.extend(children.iter().cloned());
            }
        }

        descendants
    }

    pub async fn mark_stale_descendants_compat(
        &self,
        tree_id: &ExperimentTreeId,
        changed_cell_id: &CellId,
    ) -> TineResult<Vec<CellId>> {
        let mut tree = self.get_experiment_tree(tree_id).await?;
        let descendants = Self::descendant_cell_ids_compat(&tree, changed_cell_id);
        let descendant_set: HashSet<CellId> = descendants.iter().cloned().collect();

        for cell in &mut tree.cells {
            if descendant_set.contains(&cell.id) {
                cell.state = CellRuntimeState::Stale;
            }
        }

        self.save_experiment_tree(&tree).await?;
        Ok(descendants)
    }

    pub async fn get_tree_runtime_state(
        &self,
        tree_id: &ExperimentTreeId,
    ) -> Option<TreeRuntimeState> {
        self.tree_runtime_states.read().await.get(tree_id).cloned()
    }

    pub async fn set_tree_runtime_state(&self, state: TreeRuntimeState) -> TineResult<()> {
        Self::persist_tree_runtime_state_row(&self.pool, &state).await?;
        self.tree_runtime_states
            .write()
            .await
            .insert(state.tree_id.clone(), state.clone());
        Self::emit_tree_runtime_state_event(&self.scheduler.event_sender(), &state);
        Ok(())
    }

    pub async fn mark_tree_needs_replay(
        &self,
        tree_id: &ExperimentTreeId,
    ) -> TineResult<TreeRuntimeState> {
        let tree = self.get_experiment_tree(tree_id).await?;
        let active_branch_id = self
            .get_tree_runtime_state(tree_id)
            .await
            .map(|state| state.active_branch_id)
            .unwrap_or_else(|| tree.root_branch_id.clone());
        let mut state = self
            .get_tree_runtime_state(tree_id)
            .await
            .unwrap_or_else(|| Self::default_tree_runtime_state(tree_id, &active_branch_id));
        state.active_branch_id = active_branch_id;
        state.kernel_state = TreeKernelState::NeedsReplay;
        state.materialized_path_cell_ids.clear();
        state.last_prepared_cell_id = None;
        state.runtime_epoch += 1;
        self.set_tree_runtime_state(state.clone()).await?;
        Ok(state)
    }

    pub async fn mark_tree_kernel_lost(
        &self,
        tree_id: &ExperimentTreeId,
    ) -> TineResult<Option<TreeRuntimeState>> {
        match self.scheduler.interrupt_tree_kernel(tree_id).await {
            Ok(()) => {}
            Err(TineError::KernelNotFound { .. }) => {}
            Err(err) => {
                warn!(
                    tree = %tree_id,
                    error = %err,
                    "interrupt_tree_kernel failed while marking kernel lost; \
                     proceeding to force shutdown"
                );
            }
        }
        match self.scheduler.shutdown_tree_kernel(tree_id).await {
            Ok(()) => {}
            Err(TineError::KernelNotFound { .. }) => {}
            Err(err) => {
                warn!(
                    tree = %tree_id,
                    error = %err,
                    "shutdown_tree_kernel failed while marking kernel lost; \
                     aborting before flipping state"
                );
                return Err(err);
            }
        }
        let updated = Self::apply_runtime_state_transition(
            &self.pool,
            &self.tree_runtime_states,
            tree_id,
            |state| {
                state.kernel_state = TreeKernelState::KernelLost;
                state.materialized_path_cell_ids.clear();
                state.last_prepared_cell_id = None;
                state.runtime_epoch += 1;
            },
        )
        .await?;
        let reconciled = Self::reconcile_tree_kernel_lost_executions(
            &self.pool,
            tree_id,
            "Tree kernel was marked lost while execution was running",
        )
        .await?;
        if reconciled > 0 {
            warn!(tree = %tree_id, reconciled, "reconciled running executions after tree kernel loss");
        }
        if let Some(state) = &updated {
            Self::emit_tree_runtime_state_event(&self.scheduler.event_sender(), state);
        }
        Ok(updated)
    }

    async fn record_isolation_result(
        pool: &SqlitePool,
        tree_runtime_states: &Arc<RwLock<HashMap<ExperimentTreeId, TreeRuntimeState>>>,
        tree_id: &ExperimentTreeId,
        branch_id: &BranchId,
        outcome: &KernelIsolationOutcome,
    ) -> TineResult<Option<TreeRuntimeState>> {
        Self::apply_runtime_state_transition(pool, tree_runtime_states, tree_id, |state| {
            state.last_isolation_result = Some(IsolationResult {
                branch_id: branch_id.clone(),
                succeeded: !outcome.contaminated,
                contamination_signals: outcome.signals.clone(),
                namespace_delta: Some(outcome.delta.clone()),
            });
            if outcome.contaminated {
                state.kernel_state = TreeKernelState::NeedsReplay;
                state.materialized_path_cell_ids.clear();
                state.last_prepared_cell_id = None;
            } else {
                state.kernel_state = TreeKernelState::Ready;
            }
        })
        .await
    }

    async fn prepare_context_internal(
        &self,
        tree_id: &ExperimentTreeId,
        branch_id: &BranchId,
        cell_id: &CellId,
        execution_id: Option<&ExecutionId>,
    ) -> TineResult<PreparedContext> {
        let mut total_timer = OutcomeTimer::start(METRIC_PREPARE_CONTEXT_TOTAL);
        let tree = self.get_experiment_tree(tree_id).await?;
        Self::validate_branch_membership(&tree, branch_id, cell_id)?;
        let path_cell_ids = Self::branch_path_cell_ids(&tree, branch_id)?;
        let current = self
            .get_tree_runtime_state(tree_id)
            .await
            .unwrap_or_else(|| Self::default_tree_runtime_state(tree_id, branch_id));
        let working_dir = self.file_base_for_project(tree.project_id.as_ref()).await?;
        let has_live_kernel = self.kernel_mgr.has_tree_kernel(tree_id);
        let planning_state = if has_live_kernel {
            Some(&current)
        } else {
            None
        };
        let transition =
            plan_branch_transition(planning_state, branch_id, cell_id, &path_cell_ids)?;
        let replay_prefix = transition.replay_prefix_before_target()?;
        if let Some(execution_id) = execution_id {
            Self::update_execution_status_record(&self.pool, execution_id, |status| {
                Self::apply_execution_phase(status, ExecutionPhase::PreparingEnvironment);
            })
            .await?;
        }
        let reusing_existing_kernel = has_live_kernel
            && current.kernel_state == TreeKernelState::Ready
            && transition.replay_from_idx == current.materialized_path_cell_ids.len();
        let next_runtime_epoch = if reusing_existing_kernel {
            current.runtime_epoch
        } else {
            current.runtime_epoch + 1
        };
        let switching_state = TreeRuntimeState {
            tree_id: tree_id.clone(),
            active_branch_id: branch_id.clone(),
            materialized_path_cell_ids: transition.target_path_cell_ids.clone(),
            runtime_epoch: next_runtime_epoch,
            kernel_state: TreeKernelState::Switching,
            last_prepared_cell_id: None,
            isolation_mode: current.isolation_mode.clone(),
            last_isolation_result: current.last_isolation_result.clone(),
        };
        self.set_tree_runtime_state(switching_state).await?;

        // Everything between the persisted Switching state and the final Ready
        // state is fallible. A bare `?` here would strand the tree in
        // Switching forever; on failure, downgrade to NeedsReplay so the next
        // transition rebuilds from scratch, then surface the original error.
        let transition_result: TineResult<()> = async {
            if !reusing_existing_kernel {
                if let Some(execution_id) = execution_id {
                    Self::update_execution_status_record(&self.pool, execution_id, |status| {
                        Self::apply_execution_phase(status, ExecutionPhase::AcquiringRuntime);
                    })
                    .await?;
                }
                if has_live_kernel {
                    self.kernel_mgr.shutdown_tree(tree_id).await?;
                }

                let tree_env = TreeEnvironmentDescriptor::from_tree(&tree);
                let venv_dir = self.env_mgr.ensure_tree_environment(&tree_env).await?;
                self.kernel_mgr
                    .start_tree_kernel(tree_id, &venv_dir, &working_dir)
                    .await?;
            }

            if !replay_prefix.is_empty() {
                if let Some(execution_id) = execution_id {
                    Self::update_execution_status_record(&self.pool, execution_id, |status| {
                        Self::apply_execution_phase(status, ExecutionPhase::ReplayingContext);
                    })
                    .await?;
                }
            }
            let mut replay_timer = OutcomeTimer::start(METRIC_PREPARE_CONTEXT_REPLAY);
            metrics::histogram!(METRIC_PREPARE_CONTEXT_REPLAY_CELLS)
                .record(replay_prefix.len() as f64);
            for replay_cell_id in &replay_prefix {
                self.execute_tree_cell_for_target(&tree, branch_id, replay_cell_id)
                    .await?;
            }
            replay_timer.set_outcome("success");
            drop(replay_timer);
            Ok(())
        }
        .await;

        if let Err(err) = transition_result {
            if let Err(recovery_err) = self.mark_tree_needs_replay(tree_id).await {
                warn!(
                    tree = %tree_id,
                    error = %recovery_err,
                    "failed to mark tree NeedsReplay after prepare-context failure"
                );
            }
            total_timer.set_outcome("failed");
            return Err(err);
        }

        let outcome = if reusing_existing_kernel {
            "reuse"
        } else if replay_prefix.is_empty() {
            "restart_noreplay"
        } else {
            "restart_replay"
        };

        let runtime_state = TreeRuntimeState {
            tree_id: tree_id.clone(),
            active_branch_id: branch_id.clone(),
            materialized_path_cell_ids: transition.target_path_cell_ids,
            runtime_epoch: next_runtime_epoch,
            kernel_state: TreeKernelState::Ready,
            last_prepared_cell_id: replay_prefix.last().cloned(),
            isolation_mode: current.isolation_mode,
            last_isolation_result: current.last_isolation_result,
        };
        self.set_tree_runtime_state(runtime_state.clone()).await?;
        total_timer.set_outcome(outcome);
        Ok(PreparedContext {
            tree_id: tree_id.clone(),
            branch_id: branch_id.clone(),
            target_cell_id: cell_id.clone(),
            runtime_state,
        })
    }

    pub async fn prepare_context(
        &self,
        tree_id: &ExperimentTreeId,
        branch_id: &BranchId,
        cell_id: &CellId,
    ) -> TineResult<PreparedContext> {
        self.prepare_context_internal(tree_id, branch_id, cell_id, None)
            .await
    }

    /// Execution-scoped results: the status record plus every node's logs in
    /// one call, with live streaming chunks folded in. Saves clients one
    /// logs round-trip per cell, and serves as the progress feed for
    /// still-running executions.
    pub async fn execution_results(
        &self,
        execution_id: &ExecutionId,
    ) -> TineResult<(ExecutionStatus, HashMap<NodeId, NodeLogs>)> {
        let row: Option<(String, Option<String>)> =
            sqlx::query_as("SELECT status, node_logs FROM executions WHERE id = ?")
                .bind(execution_id.as_str())
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| TineError::Database(e.to_string()))?;
        let (status_json, node_logs_json) =
            row.ok_or_else(|| TineError::ExecutionNotFound(execution_id.clone()))?;
        let status: ExecutionStatus = serde_json::from_str(&status_json)?;
        let status = self
            .enrich_execution_status(Self::normalize_execution_status(status))
            .await;

        let mut node_logs: HashMap<NodeId, NodeLogs> = node_logs_json
            .as_deref()
            .and_then(|json| serde_json::from_str(json).ok())
            .unwrap_or_default();
        let buffered_nodes: Vec<NodeId> = self
            .streaming_log_buffer
            .get(execution_id)
            .map(|entry| entry.keys().cloned().collect())
            .unwrap_or_default();
        let known_nodes: Vec<NodeId> = status
            .node_statuses
            .keys()
            .cloned()
            .chain(buffered_nodes)
            .collect();
        for node_id in known_nodes {
            let logs = node_logs.entry(node_id.clone()).or_default();
            Self::overlay_streaming_buffer(
                &self.streaming_log_buffer,
                execution_id,
                &node_id,
                logs,
            );
        }
        node_logs.retain(|_, logs| {
            !logs.stdout.is_empty()
                || !logs.stderr.is_empty()
                || logs.error.is_some()
                || !logs.outputs.is_empty()
                || logs.duration_ms.is_some()
                || !logs.metrics.is_empty()
        });
        Ok((status, node_logs))
    }

    /// Dry-run a branch execution against the current cache: which cells
    /// would run vs cache-hit, and why. Advisory — uses the cache and
    /// lockfile state as of now, without preparing the environment.
    pub async fn preview_branch_execution_plan(
        &self,
        tree_id: &ExperimentTreeId,
        branch_id: &BranchId,
    ) -> TineResult<Vec<BranchPlanPreviewCell>> {
        let tree = self.get_experiment_tree(tree_id).await?;
        let plan = Self::build_tree_branch_execution_plan(&tree, branch_id)?;
        let branch = plan.executable_branch;
        let cache = Self::load_cache_from_pool(&self.pool).await?;
        let lockfile_hash = self
            .env_mgr
            .lockfile_hash_for_tree(&TreeEnvironmentDescriptor::from_tree(&tree))
            .await?;
        let graph = tine_graph::ExecutableTreeGraph::from_branch(&branch)?;
        let (_to_execute, to_skip) = graph.plan_execution(&branch, &cache, lockfile_hash);
        let skipped: HashSet<String> = to_skip
            .iter()
            .map(|(node, _)| node.as_str().to_string())
            .collect();

        let mut will_run: HashSet<String> = HashSet::new();
        let mut preview = Vec::with_capacity(branch.topo_order.len());
        for cell_id in &branch.topo_order {
            let cell = branch
                .cells
                .iter()
                .find(|cell| &cell.cell_id == cell_id)
                .expect("projected branch contains all path cells");
            if skipped.contains(cell_id.as_str()) {
                preview.push(BranchPlanPreviewCell {
                    cell_id: cell_id.as_str().to_string(),
                    action: "cache_hit",
                    reason: "cached",
                });
                continue;
            }
            will_run.insert(cell_id.as_str().to_string());
            let reason = if !cell.cache {
                "cache_disabled"
            } else if cell
                .inputs
                .values()
                .any(|input| will_run.contains(input.source_cell_id.as_str()))
            {
                "upstream_will_run"
            } else {
                let scope = NodeCacheKey::scope_for(branch.tree_id.as_str(), cell.cell_id.as_str());
                let code_hash = NodeCacheKey::hash_code(&cell.code.source);
                let mut saw_scope = false;
                let mut saw_code = false;
                for key in cache.keys() {
                    if key.scope_hash != scope {
                        continue;
                    }
                    saw_scope = true;
                    if key.code_hash == code_hash {
                        saw_code = true;
                        break;
                    }
                }
                if !saw_scope {
                    "no_prior_run"
                } else if !saw_code {
                    "code_changed"
                } else {
                    "inputs_or_environment_changed"
                }
            };
            preview.push(BranchPlanPreviewCell {
                cell_id: cell_id.as_str().to_string(),
                action: "run",
                reason,
            });
        }
        Ok(preview)
    }

    /// Idempotency keys are scoped to their execute target so a reused key
    /// on another tree/branch/cell never attaches to an unrelated run.
    fn branch_execution_idempotency_scope(
        tree_id: &ExperimentTreeId,
        branch_id: &BranchId,
    ) -> String {
        format!("branch:{}:{}", tree_id.as_str(), branch_id.as_str())
    }

    fn cell_execution_idempotency_scope(
        tree_id: &ExperimentTreeId,
        branch_id: &BranchId,
        cell_id: &CellId,
    ) -> String {
        format!(
            "cell:{}:{}:{}",
            tree_id.as_str(),
            branch_id.as_str(),
            cell_id.as_str()
        )
    }

    /// Canonical fingerprint of the execution-relevant request state. A
    /// retried submission only reattaches to the original execution when
    /// this matches — a reused key whose underlying plan changed is a
    /// different request, not a retry.
    ///
    /// Covers everything that changes what a run produces: cell order
    /// (path and topo), each cell's code, resolved inputs, declared
    /// outputs, cache/map settings, plus the tree environment, execution
    /// mode, budget, and the project context (project id and resolved
    /// working directory) that relative file access runs against.
    /// Deliberately excludes metadata that doesn't (names, tags) and
    /// revision ids, which advance on no-op saves and would turn genuine
    /// retries into false conflicts.
    fn execution_request_fingerprint(
        executable_branch: &ExecutableTreeBranch,
        working_dir: &Path,
    ) -> String {
        fn field(hasher: &mut blake3::Hasher, value: &str) {
            hasher.update(value.as_bytes());
            hasher.update(&[0]);
        }
        // Separates variable-length sections so adjacent lists can't
        // produce the same byte stream.
        fn section(hasher: &mut blake3::Hasher) {
            hasher.update(&[1]);
        }

        let mut hasher = blake3::Hasher::new();
        field(
            &mut hasher,
            executable_branch
                .project_id
                .as_ref()
                .map_or("", |project_id| project_id.as_str()),
        );
        field(&mut hasher, &working_dir.to_string_lossy());
        field(
            &mut hasher,
            &serde_json::to_string(&executable_branch.execution_mode).unwrap_or_default(),
        );
        field(
            &mut hasher,
            &serde_json::to_string(&executable_branch.budget).unwrap_or_default(),
        );
        field(
            &mut hasher,
            &serde_json::to_string(&executable_branch.environment).unwrap_or_default(),
        );
        for cell_id in &executable_branch.path_cell_order {
            field(&mut hasher, cell_id.as_str());
        }
        section(&mut hasher);
        for cell_id in &executable_branch.topo_order {
            field(&mut hasher, cell_id.as_str());
        }
        section(&mut hasher);
        for cell in &executable_branch.cells {
            field(&mut hasher, cell.cell_id.as_str());
            field(&mut hasher, &cell.code.language);
            field(&mut hasher, &cell.code.source);
            let mut inputs: Vec<_> = cell.inputs.iter().collect();
            inputs.sort_by(|(a, _), (b, _)| a.as_str().cmp(b.as_str()));
            for (slot, input) in inputs {
                field(&mut hasher, slot.as_str());
                field(&mut hasher, input.source_cell_id.as_str());
                field(&mut hasher, input.source_output.as_str());
            }
            section(&mut hasher);
            for output in &cell.outputs {
                field(&mut hasher, output.as_str());
            }
            section(&mut hasher);
            field(&mut hasher, if cell.cache { "1" } else { "0" });
            field(
                &mut hasher,
                cell.map_over.as_ref().map_or("", |slot| slot.as_str()),
            );
            field(
                &mut hasher,
                &cell.map_concurrency.map(|n| n.to_string()).unwrap_or_default(),
            );
            section(&mut hasher);
        }
        hasher.finalize().to_hex().to_string()
    }

    /// Resolve an idempotency reservation to its execution: `Ok(Some)` when
    /// the key exists with a matching fingerprint, `Ok(None)` when the key
    /// is unused, and `Err(IdempotencyConflict)` when the key is bound to a
    /// request whose execution-relevant state differs.
    async fn find_execution_by_idempotency_key(
        pool: &SqlitePool,
        idempotency: &ExecutionIdempotency<'_>,
    ) -> TineResult<Option<ExecutionId>> {
        let row: Option<(String, Option<String>)> = sqlx::query_as(
            "SELECT id, idempotency_fingerprint FROM executions WHERE idempotency_key = ? AND idempotency_scope = ?",
        )
        .bind(idempotency.key)
        .bind(idempotency.scope)
        .fetch_optional(pool)
        .await
        .map_err(|e| TineError::Database(e.to_string()))?;
        match row {
            None => Ok(None),
            Some((id, fingerprint)) if fingerprint.as_deref() == Some(idempotency.fingerprint) => {
                Ok(Some(ExecutionId::new(id)))
            }
            Some(_) => Err(TineError::IdempotencyConflict(format!(
                "key '{}' was already used on this target for a request with different code or environment; use a new key to run the current state",
                idempotency.key
            ))),
        }
    }

    /// How long a terminal execution's idempotency reservation is retained.
    /// Keys exist to make near-term retries (timeouts, client crashes,
    /// reconnects) safe; past this window a resubmission with the same key
    /// is treated as a new request rather than a reattach, which lets the
    /// keyed rows be reclaimed so the table and its unique index stay bounded.
    const IDEMPOTENCY_RETENTION_DAYS: i64 = 7;

    /// Null out the idempotency columns of terminal executions whose retry
    /// window has elapsed, removing them from the partial unique index.
    /// Best-effort: a failure here only forgoes cleanup, never correctness.
    async fn prune_stale_idempotency_reservations(pool: &SqlitePool) {
        let cutoff = format!("-{} days", Self::IDEMPOTENCY_RETENTION_DAYS);
        let result = sqlx::query(
            "UPDATE executions \
             SET idempotency_key = NULL, idempotency_scope = NULL, idempotency_fingerprint = NULL \
             WHERE idempotency_key IS NOT NULL \
               AND finished_at IS NOT NULL \
               AND finished_at < datetime('now', ?)",
        )
        .bind(&cutoff)
        .execute(pool)
        .await;
        match result {
            Ok(outcome) if outcome.rows_affected() > 0 => {
                info!(
                    released = outcome.rows_affected(),
                    retention_days = Self::IDEMPOTENCY_RETENTION_DAYS,
                    "released stale idempotency reservations"
                );
            }
            Ok(_) => {}
            Err(e) => {
                warn!(error = %e, "failed to prune stale idempotency reservations");
            }
        }
    }

    async fn run_additive_migration(pool: &SqlitePool, sql: &str) {
        for statement in sql.split(';') {
            let stripped: String = statement
                .lines()
                .filter(|line| {
                    let trimmed = line.trim();
                    !trimmed.is_empty() && !trimmed.starts_with("--")
                })
                .collect::<Vec<_>>()
                .join("\n");
            let stmt = stripped.trim();
            if stmt.is_empty() {
                continue;
            }
            let _ = sqlx::query(stmt).execute(pool).await;
        }
    }

    /// Run migration 007 idempotently.
    /// Guard: skip if the snapshots table is already gone or executions are already tree-native.
    /// Uses an explicit transaction to ensure DROP + RENAME is atomic.
    async fn run_migration_007(pool: &SqlitePool) -> TineResult<()> {
        let has_snapshots: bool = sqlx::query_scalar(
            "SELECT COUNT(*) > 0 FROM sqlite_master WHERE type='table' AND name='snapshots'",
        )
        .fetch_one(pool)
        .await
        .unwrap_or(false);

        let columns: Vec<(String,)> =
            sqlx::query_as("SELECT name FROM pragma_table_info('executions')")
                .fetch_all(pool)
                .await
                .map_err(|e| TineError::Database(format!("migration 007: inspect failed: {e}")))?;
        let has_pipeline_id = columns.iter().any(|(name,)| name == "pipeline_id");

        if !has_snapshots || !has_pipeline_id {
            return Ok(());
        }

        let raw = include_str!("../../../migrations/007_executions_tree_native.sql");
        // Strip pure-comment lines before splitting on ';' to avoid semicolons
        // inside comments breaking the split.
        let sql: String = raw
            .lines()
            .filter(|line| !line.trim_start().starts_with("--"))
            .collect::<Vec<_>>()
            .join("\n");

        let mut tx = pool
            .begin()
            .await
            .map_err(|e| TineError::Database(format!("migration 007: begin failed: {e}")))?;

        for statement in sql.split(';') {
            let stmt = statement.trim();
            if stmt.is_empty() {
                continue;
            }
            sqlx::query(stmt)
                .execute(&mut *tx)
                .await
                .map_err(|e| TineError::Database(format!("migration 007 failed: {e}")))?;
        }

        tx.commit()
            .await
            .map_err(|e| TineError::Database(format!("migration 007: commit failed: {e}")))?;
        Ok(())
    }

    /// Run migration 008 idempotently.
    /// Guard: skip if the executions table no longer has a pipeline_id column.
    async fn run_migration_008(pool: &SqlitePool) -> TineResult<()> {
        let columns: Vec<(String,)> =
            sqlx::query_as("SELECT name FROM pragma_table_info('executions')")
                .fetch_all(pool)
                .await
                .map_err(|e| TineError::Database(format!("migration 008: inspect failed: {e}")))?;
        let has_pipeline_id = columns.iter().any(|(name,)| name == "pipeline_id");
        if !has_pipeline_id {
            return Ok(());
        }

        let raw = include_str!("../../../migrations/008_drop_execution_pipeline_ids.sql");
        let sql: String = raw
            .lines()
            .filter(|line| !line.trim_start().starts_with("--"))
            .collect::<Vec<_>>()
            .join("\n");

        let mut tx = pool
            .begin()
            .await
            .map_err(|e| TineError::Database(format!("migration 008: begin failed: {e}")))?;

        for statement in sql.split(';') {
            let stmt = statement.trim();
            if stmt.is_empty() {
                continue;
            }
            sqlx::query(stmt)
                .execute(&mut *tx)
                .await
                .map_err(|e| TineError::Database(format!("migration 008 failed: {e}")))?;
        }

        tx.commit()
            .await
            .map_err(|e| TineError::Database(format!("migration 008: commit failed: {e}")))?;
        Ok(())
    }

    /// Run migration 009 idempotently.
    /// Guard: rename legacy cache provenance column to source_runtime_id.
    async fn run_migration_009(pool: &SqlitePool) -> TineResult<()> {
        let columns: Vec<(String,)> = sqlx::query_as("SELECT name FROM pragma_table_info('cache')")
            .fetch_all(pool)
            .await
            .map_err(|e| TineError::Database(format!("migration 009: inspect failed: {e}")))?;
        let has_legacy_column = columns.iter().any(|(name,)| name == "source_pipeline_id");
        let has_runtime_column = columns.iter().any(|(name,)| name == "source_runtime_id");
        if !has_legacy_column || has_runtime_column {
            return Ok(());
        }

        let raw = include_str!("../../../migrations/009_cache_runtime_ids.sql");
        let sql: String = raw
            .lines()
            .filter(|line| !line.trim_start().starts_with("--"))
            .collect::<Vec<_>>()
            .join("\n");

        let mut tx = pool
            .begin()
            .await
            .map_err(|e| TineError::Database(format!("migration 009: begin failed: {e}")))?;

        for statement in sql.split(';') {
            let stmt = statement.trim();
            if stmt.is_empty() {
                continue;
            }
            sqlx::query(stmt)
                .execute(&mut *tx)
                .await
                .map_err(|e| TineError::Database(format!("migration 009 failed: {e}")))?;
        }

        tx.commit()
            .await
            .map_err(|e| TineError::Database(format!("migration 009: commit failed: {e}")))?;
        Ok(())
    }

    /// Run migration 010 idempotently.
    /// Guard: skip if node_id is already part of the cache primary key.
    async fn run_migration_010(pool: &SqlitePool) -> TineResult<()> {
        let scope_hash_in_pk: bool = sqlx::query_scalar(
            "SELECT COUNT(*) > 0 FROM pragma_table_info('cache') WHERE name = 'scope_hash' AND pk > 0",
        )
        .fetch_one(pool)
        .await
        .map_err(|e| TineError::Database(format!("migration 010: inspect failed: {e}")))?;
        if scope_hash_in_pk {
            return Ok(());
        }

        let raw = include_str!("../../../migrations/010_cache_cell_scope.sql");
        let sql: String = raw
            .lines()
            .filter(|line| !line.trim_start().starts_with("--"))
            .collect::<Vec<_>>()
            .join("\n");

        let mut tx = pool
            .begin()
            .await
            .map_err(|e| TineError::Database(format!("migration 010: begin failed: {e}")))?;

        for statement in sql.split(';') {
            let stmt = statement.trim();
            if stmt.is_empty() {
                continue;
            }
            sqlx::query(stmt)
                .execute(&mut *tx)
                .await
                .map_err(|e| TineError::Database(format!("migration 010 failed: {e}")))?;
        }

        tx.commit()
            .await
            .map_err(|e| TineError::Database(format!("migration 010: commit failed: {e}")))?;
        Ok(())
    }

    /// Create a new workspace, initializing the database.
    pub async fn open(
        workspace_root: PathBuf,
        store: Arc<dyn ArtifactStore>,
        max_kernels: usize,
    ) -> TineResult<Self> {
        // Ensure .tine directory exists
        let tine_dir = workspace_root.join(".tine");
        tokio::fs::create_dir_all(&tine_dir).await?;

        // Open/create SQLite database
        let db_path = tine_dir.join("tine.db");
        let db_url = format!("sqlite://{}?mode=rwc", db_path.display());
        let connect_options = SqliteConnectOptions::from_str(&db_url)
            .map_err(|e| TineError::Database(e.to_string()))?
            .journal_mode(SqliteJournalMode::Wal)
            .busy_timeout(Duration::from_secs(5));
        let pool = SqlitePool::connect_with(connect_options)
            .await
            .map_err(|e| TineError::Database(e.to_string()))?;

        // Run migrations — split multi-statement SQL into individual queries
        let migration_sql = include_str!("../../../migrations/001_init.sql");
        for statement in migration_sql.split(';') {
            // Strip leading comment lines and whitespace to get to the actual SQL
            let stripped: String = statement
                .lines()
                .filter(|line| {
                    let trimmed = line.trim();
                    !trimmed.is_empty() && !trimmed.starts_with("--")
                })
                .collect::<Vec<_>>()
                .join("\n");
            let stmt = stripped.trim();
            if stmt.is_empty() {
                continue;
            }
            sqlx::query(stmt)
                .execute(&pool)
                .await
                .map_err(|e| TineError::Database(format!("migration failed: {}", e)))?;
        }

        // Run additive migrations (ALTER TABLE — may already exist, ignore errors)
        let migration_002 = include_str!("../../../migrations/002_cache_provenance.sql");
        Self::run_additive_migration(&pool, migration_002).await;

        // Run migration 003 — project hierarchy (additive, ignore duplicate errors)
        let migration_003 = include_str!("../../../migrations/003_project_hierarchy.sql");
        Self::run_additive_migration(&pool, migration_003).await;

        // Run migration 004 — experiment tree scaffolding (additive, ignore duplicate errors)
        let migration_004 = include_str!("../../../migrations/004_experiment_tree_scaffolding.sql");
        Self::run_additive_migration(&pool, migration_004).await;

        // Run migration 005 — execution target columns (additive, ignore duplicate errors)
        let migration_005 = include_str!("../../../migrations/005_execution_targets.sql");
        Self::run_additive_migration(&pool, migration_005).await;

        // Run migration 006 — persisted tree runtime state (additive, ignore duplicate errors)
        let migration_006 = include_str!("../../../migrations/006_tree_runtime_state.sql");
        Self::run_additive_migration(&pool, migration_006).await;

        // Run migration 007 — make executions tree-native, drop snapshots.
        Self::run_migration_007(&pool).await?;

        // Run migration 008 — remove legacy execution pipeline IDs.
        Self::run_migration_008(&pool).await?;

        // Run migration 009 — rename legacy cache provenance to runtime naming.
        Self::run_migration_009(&pool).await?;

        // Run migration 010 — scope cache entries to their owning cell.
        Self::run_migration_010(&pool).await?;

        // Run migration 011 — idempotency keys for execute submissions
        // (additive: duplicate-column errors on rerun are ignored).
        let migration_011 = include_str!("../../../migrations/011_execution_idempotency.sql");
        Self::run_additive_migration(&pool, migration_011).await;

        // Release idempotency reservations whose retry window has elapsed, so
        // the keyed rows (and their unique index) don't accumulate without
        // bound across long-lived workspaces. A retry older than the window
        // is no longer a near-term retry and may safely start a fresh run.
        Self::prune_stale_idempotency_reservations(&pool).await;

        let kernel_mgr = Arc::new(KernelManager::new(&workspace_root, max_kernels));
        let lifecycle_rx = kernel_mgr.subscribe_lifecycle();
        let env_mgr = Arc::new(EnvironmentManager::new(workspace_root.clone()));

        let artifact_dir = tine_dir.join("artifacts");
        tokio::fs::create_dir_all(&artifact_dir).await?;
        let catalog = Arc::new(DataCatalog::new(store, artifact_dir));

        let scheduler = Arc::new(Scheduler::new(
            kernel_mgr.clone(),
            env_mgr.clone(),
            catalog.clone(),
            workspace_root.clone(),
        ));
        let max_concurrent_executions = std::cmp::max(1, max_kernels);
        let max_queue_depth = Self::default_max_queue_depth(max_concurrent_executions);

        Self::reconcile_unfinished_executions(&pool).await?;

        // Cleanup orphaned kernels from previous runs
        kernel_mgr.cleanup_orphans().await?;

        // Start background heartbeat monitor + idle eviction
        let kernel_monitor_handle = kernel_mgr.spawn_monitor();

        info!(root = %workspace_root.display(), "workspace opened");

        let tree_runtime_states = Self::hydrate_tree_runtime_states(&pool).await?;
        let tree_runtime_states = Arc::new(RwLock::new(tree_runtime_states));
        let event_tx = scheduler.event_sender();
        let streaming_log_buffer: Arc<StreamingLogBuffer> = Arc::new(DashMap::new());
        let execution_persist_locks: Arc<ExecutionLockRegistry> = Arc::new(DashMap::new());
        let bridge_shutdown_signal: Arc<Notify> = Arc::new(Notify::new());
        let execution_event_bridge_handle = Self::spawn_execution_event_bridge(
            pool.clone(),
            streaming_log_buffer.clone(),
            execution_persist_locks.clone(),
            scheduler.subscribe(),
            bridge_shutdown_signal.clone(),
        );
        let streaming_log_flush_handle = Self::spawn_streaming_log_flush_task(
            pool.clone(),
            streaming_log_buffer.clone(),
            execution_persist_locks.clone(),
        );
        let kernel_lifecycle_handle = Self::spawn_kernel_lifecycle_bridge(
            pool.clone(),
            tree_runtime_states.clone(),
            lifecycle_rx,
            event_tx,
        );

        Ok(Self {
            pool,
            scheduler,
            kernel_mgr,
            env_mgr,
            catalog,
            workspace_root,
            tree_runtime_states,
            execution_queue_state: Arc::new(Mutex::new(ExecutionQueueState::default())),
            execution_queue_notify: Arc::new(Notify::new()),
            streaming_log_buffer,
            execution_persist_locks,
            bridge_shutdown_signal,
            max_concurrent_executions,
            max_queue_depth,
            kernel_monitor_handle,
            kernel_lifecycle_handle,
            execution_event_bridge_handle: std::sync::Mutex::new(Some(
                execution_event_bridge_handle,
            )),
            streaming_log_flush_handle,
        })
    }

    /// Get a reference to the scheduler's event subscriber.
    pub fn subscribe_events(&self) -> tokio::sync::broadcast::Receiver<ExecutionEvent> {
        self.scheduler.subscribe()
    }

    /// Get the event sender for broadcasting external events (e.g., file watcher).
    pub fn event_sender(&self) -> tokio::sync::broadcast::Sender<ExecutionEvent> {
        self.scheduler.event_sender()
    }

    /// Workspace root path accessor.
    pub fn workspace_root(&self) -> &std::path::Path {
        &self.workspace_root
    }

    /// Graceful shutdown: stop accepting work, cancel queued and
    /// running executions through the tree shutdown path, persist
    /// terminal state/logs, then stop kernels.
    pub async fn shutdown(&self) -> TineResult<()> {
        info!("shutting down workspace");

        // Stop background producers first while keeping the execution
        // bridge alive so any late terminal events emitted during kernel
        // teardown still flow into the DB / streaming buffer.
        self.kernel_monitor_handle.abort();

        let tree_ids = self.collect_workspace_shutdown_tree_ids().await?;
        for tree_id in tree_ids {
            self.shutdown_tree_kernel(&tree_id).await?;
        }

        self.kernel_mgr.shutdown_all().await?;

        // Now that producers are quiesced, ask the bridge to drain the
        // receiver and exit. This preserves events that were emitted
        // during `shutdown_all()`.
        self.bridge_shutdown_signal.notify_waiters();
        let bridge_handle = self
            .execution_event_bridge_handle
            .lock()
            .ok()
            .and_then(|mut guard| guard.take());
        if let Some(handle) = bridge_handle {
            // Bound how long we wait so a wedged bridge can't hang
            // shutdown forever; 10s is generous given the broadcast
            // channel's bounded capacity (1024).
            match tokio::time::timeout(Duration::from_secs(10), handle).await {
                Ok(Ok(())) => {}
                Ok(Err(err)) => warn!(error = %err, "execution event bridge join error"),
                Err(_) => warn!("execution event bridge drain exceeded 10s during shutdown"),
            }
        }

        // Persist anything the bridge buffered while teardown was in
        // progress before the flush task is aborted.
        if let Err(err) = Self::flush_streaming_log_buffer_locked(
            &self.pool,
            &self.streaming_log_buffer,
            Some(&self.execution_persist_locks),
        )
        .await
        {
            warn!(error = %err, "failed to flush streaming log buffer during shutdown");
        }

        self.kernel_lifecycle_handle.abort();
        self.streaming_log_flush_handle.abort();
        self.pool.close().await;
        Ok(())
    }

    /// Extract metrics auto-captured from node output slot values and persist
    /// to the metrics table.  Scalars and dict-of-scalars are auto-extracted
    /// by the scheduler during cache-write serialization.
    async fn persist_metrics(
        &self,
        execution_id: &ExecutionId,
        node_logs: &HashMap<NodeId, NodeLogs>,
    ) -> TineResult<()> {
        for (node_id, logs) in node_logs {
            for (name, value) in &logs.metrics {
                sqlx::query(
                    "INSERT INTO metrics (execution_id, node_id, metric_name, metric_value, step) \
                     VALUES (?, ?, ?, ?, 0)",
                )
                .bind(execution_id.as_str())
                .bind(node_id.as_str())
                .bind(name)
                .bind(value)
                .execute(&self.pool)
                .await
                .map_err(|e| TineError::Database(e.to_string()))?;
            }
        }
        Ok(())
    }

    async fn load_cache_from_pool(
        pool: &SqlitePool,
    ) -> TineResult<HashMap<NodeCacheKey, HashMap<SlotName, ArtifactKey>>> {
        let rows: Vec<(
            String,
            String,
            String,
            String,
            Option<String>,
            Option<String>,
            String,
        )> = sqlx::query_as(
            "SELECT code_hash, input_hashes, lockfile_hash, artifacts, source_runtime_id, node_id, scope_hash FROM cache",
        )
        .fetch_all(pool)
        .await
        .map_err(|e| TineError::Database(e.to_string()))?;

        let mut cache = HashMap::new();
        for (
            code_hash_hex,
            input_hashes_json,
            lockfile_hash_hex,
            artifacts_json,
            source_runtime_id,
            node_id,
            scope_hash_hex,
        ) in rows
        {
            // Parse code_hash
            let code_hash = match hex::decode(&code_hash_hex) {
                Ok(bytes) if bytes.len() == 32 => {
                    let mut arr = [0u8; 32];
                    arr.copy_from_slice(&bytes);
                    arr
                }
                _ => continue,
            };

            // Parse lockfile_hash
            let lockfile_hash = match hex::decode(&lockfile_hash_hex) {
                Ok(bytes) if bytes.len() == 32 => {
                    let mut arr = [0u8; 32];
                    arr.copy_from_slice(&bytes);
                    arr
                }
                _ => continue,
            };

            // New rows persist their scope directly; migrated legacy rows
            // (empty scope_hash) derive it from the provenance columns
            // (source_runtime_id is "tree::branch"). Entries without either
            // cannot be attributed to a cell, so they are unreachable by
            // design — reuse must stay top-to-bottom (same cell of the same
            // tree), never sideways across branches or trees.
            let scope_hash = if !scope_hash_hex.is_empty() {
                match hex::decode(&scope_hash_hex) {
                    Ok(bytes) if bytes.len() == 32 => {
                        let mut arr = [0u8; 32];
                        arr.copy_from_slice(&bytes);
                        arr
                    }
                    _ => continue,
                }
            } else {
                let Some(node_id) = node_id.filter(|id| !id.is_empty()) else {
                    continue;
                };
                let Some(tree_part) = source_runtime_id
                    .as_deref()
                    .and_then(|runtime_id| runtime_id.split_once("::"))
                    .map(|(tree_part, _)| tree_part)
                else {
                    continue;
                };
                NodeCacheKey::scope_for(tree_part, &node_id)
            };

            // Parse input_hashes
            let input_hashes: HashMap<SlotName, [u8; 32]> =
                serde_json::from_str(&input_hashes_json).unwrap_or_default();

            // Parse artifacts
            let artifacts: HashMap<SlotName, ArtifactKey> =
                serde_json::from_str(&artifacts_json).unwrap_or_default();

            let key = NodeCacheKey {
                code_hash,
                input_hashes,
                lockfile_hash,
                scope_hash,
            };
            cache.insert(key, artifacts);
        }

        info!(entries = cache.len(), "loaded cache from database");
        Ok(cache)
    }

    fn resolve_project_workspace_root(&self, project: &ProjectDef) -> PathBuf {
        let configured = PathBuf::from(&project.workspace_dir);
        if configured.is_absolute() {
            configured
        } else {
            self.workspace_root.join(configured)
        }
    }

    async fn file_base_for_project(&self, project_id: Option<&ProjectId>) -> TineResult<PathBuf> {
        match project_id {
            Some(project_id) => {
                let project = self.get_project(project_id).await?;
                Ok(self.resolve_project_workspace_root(&project))
            }
            None => Ok(self.workspace_root.clone()),
        }
    }

    /// List files and directories at a given relative path within the workspace.
    pub async fn list_files(&self, rel_path: &str) -> TineResult<Vec<FileEntry>> {
        self.list_project_files(None, rel_path).await
    }

    /// List files and directories at a given relative path within a project workspace.
    pub async fn list_project_files(
        &self,
        project_id: Option<&ProjectId>,
        rel_path: &str,
    ) -> TineResult<Vec<FileEntry>> {
        let base = self.file_base_for_project(project_id).await?;
        let trimmed = rel_path.trim_start_matches('/');
        let target = if trimmed.is_empty() || trimmed == "." {
            base.clone()
        } else {
            let joined = base.join(trimmed);
            // Return empty list if path does not exist or is not a directory
            if !joined.exists() || !joined.is_dir() {
                return Ok(Vec::new());
            }
            // Prevent path traversal
            let canonical = joined
                .canonicalize()
                .map_err(|e| TineError::Internal(e.to_string()))?;
            let base_canonical = base
                .canonicalize()
                .map_err(|e| TineError::Internal(e.to_string()))?;
            if !canonical.starts_with(&base_canonical) {
                return Err(TineError::Internal(
                    "path traversal not allowed".to_string(),
                ));
            }
            canonical
        };

        let mut entries = Vec::new();
        let mut read_dir = tokio::fs::read_dir(&target)
            .await
            .map_err(|e| TineError::Internal(format!("cannot read dir: {}", e)))?;

        while let Some(entry) = read_dir
            .next_entry()
            .await
            .map_err(|e| TineError::Internal(e.to_string()))?
        {
            let name = entry.file_name().to_string_lossy().to_string();
            // Skip hidden dirs like .tine, .git, __pycache__, .venv
            if name.starts_with('.') || name == "__pycache__" || name == "node_modules" {
                continue;
            }
            let ft = entry
                .file_type()
                .await
                .map_err(|e| TineError::Internal(e.to_string()))?;
            let meta = entry.metadata().await.ok();
            entries.push(FileEntry {
                name,
                is_dir: ft.is_dir(),
                size: meta.as_ref().map(|m| m.len()).unwrap_or(0),
            });
        }
        // Sort: directories first, then alphabetically
        entries.sort_by(|a, b| b.is_dir.cmp(&a.is_dir).then(a.name.cmp(&b.name)));
        Ok(entries)
    }

    /// Read the contents of a file within the workspace.
    pub async fn read_file(&self, rel_path: &str) -> TineResult<String> {
        self.read_project_file(None, rel_path).await
    }

    /// Read the contents of a file within a project workspace.
    pub async fn read_project_file(
        &self,
        project_id: Option<&ProjectId>,
        rel_path: &str,
    ) -> TineResult<String> {
        let bytes = self.read_project_file_bytes(project_id, rel_path).await?;
        String::from_utf8(bytes)
            .map_err(|e| TineError::Internal(format!("cannot decode file as utf-8: {}", e)))
    }

    /// Read raw bytes from a file within a project workspace.
    pub async fn read_project_file_bytes(
        &self,
        project_id: Option<&ProjectId>,
        rel_path: &str,
    ) -> TineResult<Vec<u8>> {
        let canonical = self.resolve_project_file_path(project_id, rel_path).await?;
        tokio::fs::read(&canonical)
            .await
            .map_err(|e| TineError::Internal(format!("cannot read file: {}", e)))
    }

    /// Write content to a file within the workspace.
    pub async fn write_file(&self, rel_path: &str, content: &str) -> TineResult<()> {
        self.write_project_file(None, rel_path, content).await
    }

    /// Write content to a file within a project workspace.
    pub async fn write_project_file(
        &self,
        project_id: Option<&ProjectId>,
        rel_path: &str,
        content: &str,
    ) -> TineResult<()> {
        let base = self.file_base_for_project(project_id).await?;
        let trimmed = rel_path.trim_start_matches('/');
        if trimmed.is_empty() {
            return Err(TineError::Internal("empty file path".to_string()));
        }
        let joined = base.join(trimmed);
        // Prevent path traversal: resolve parent dir, check it's inside workspace
        tokio::fs::create_dir_all(&base)
            .await
            .map_err(|e| TineError::Internal(format!("cannot create workspace root: {}", e)))?;
        if let Some(parent) = joined.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| TineError::Internal(format!("cannot create dirs: {}", e)))?;
        }
        let base_canonical = base
            .canonicalize()
            .map_err(|e| TineError::Internal(e.to_string()))?;
        // Write then verify the canonical path (the file may not exist yet)
        tokio::fs::write(&joined, content)
            .await
            .map_err(|e| TineError::Internal(format!("cannot write file: {}", e)))?;
        let canonical = joined
            .canonicalize()
            .map_err(|e| TineError::Internal(e.to_string()))?;
        if !canonical.starts_with(&base_canonical) {
            // Path traversal attempt — remove the written file
            let _ = tokio::fs::remove_file(&canonical).await;
            return Err(TineError::Internal(
                "path traversal not allowed".to_string(),
            ));
        }
        Ok(())
    }

    async fn resolve_project_file_path(
        &self,
        project_id: Option<&ProjectId>,
        rel_path: &str,
    ) -> TineResult<PathBuf> {
        let base = self.file_base_for_project(project_id).await?;
        let trimmed = rel_path.trim_start_matches('/');
        let joined = base.join(trimmed);
        let canonical = joined
            .canonicalize()
            .map_err(|e| TineError::Internal(e.to_string()))?;
        let base_canonical = base
            .canonicalize()
            .map_err(|e| TineError::Internal(e.to_string()))?;
        if !canonical.starts_with(&base_canonical) {
            return Err(TineError::Internal(
                "path traversal not allowed".to_string(),
            ));
        }
        Ok(canonical)
    }
}

#[async_trait]
impl WorkspaceApi for Workspace {
    async fn get_experiment_tree(&self, id: &ExperimentTreeId) -> TineResult<ExperimentTreeDef> {
        Workspace::get_experiment_tree(self, id).await
    }

    async fn list_experiment_trees(&self) -> TineResult<Vec<ExperimentTreeDef>> {
        Workspace::list_experiment_trees(self).await
    }

    async fn create_experiment_tree(
        &self,
        name: &str,
        project_id: Option<&ProjectId>,
    ) -> TineResult<ExperimentTreeDef> {
        Workspace::create_experiment_tree(self, name, project_id).await
    }

    async fn delete_experiment_tree(&self, tree_id: &ExperimentTreeId) -> TineResult<()> {
        Workspace::delete_experiment_tree(self, tree_id).await
    }

    async fn rename_experiment_tree(
        &self,
        tree_id: &ExperimentTreeId,
        name: &str,
    ) -> TineResult<()> {
        Workspace::rename_experiment_tree(self, tree_id, name).await
    }

    async fn save_experiment_tree(&self, def: &ExperimentTreeDef) -> TineResult<ExperimentTreeDef> {
        Workspace::save_experiment_tree(self, def).await
    }

    async fn inspect_branch_target(
        &self,
        tree_id: &ExperimentTreeId,
        branch_id: &BranchId,
        cell_id: &CellId,
    ) -> TineResult<BranchTargetInspection> {
        Workspace::inspect_branch_target(self, tree_id, branch_id, cell_id).await
    }

    async fn create_branch_in_experiment_tree(
        &self,
        tree_id: &ExperimentTreeId,
        parent_branch_id: &BranchId,
        branch_name: String,
        branch_point_cell_id: &CellId,
        first_cell: CellDef,
    ) -> TineResult<BranchId> {
        Workspace::create_branch_in_experiment_tree(
            self,
            tree_id,
            parent_branch_id,
            branch_name,
            branch_point_cell_id,
            first_cell,
        )
        .await
    }

    async fn add_cell_to_experiment_tree_branch(
        &self,
        tree_id: &ExperimentTreeId,
        branch_id: &BranchId,
        cell: CellDef,
        after_cell_id: Option<&CellId>,
    ) -> TineResult<()> {
        Workspace::add_cell_to_experiment_tree_branch(self, tree_id, branch_id, cell, after_cell_id)
            .await
    }

    async fn update_cell_code_in_experiment_tree_branch(
        &self,
        tree_id: &ExperimentTreeId,
        branch_id: &BranchId,
        cell_id: &CellId,
        code: &str,
    ) -> TineResult<()> {
        Workspace::update_cell_code_in_experiment_tree_branch(
            self, tree_id, branch_id, cell_id, code,
        )
        .await
    }

    async fn move_cell_in_experiment_tree_branch(
        &self,
        tree_id: &ExperimentTreeId,
        branch_id: &BranchId,
        cell_id: &CellId,
        direction: &str,
    ) -> TineResult<()> {
        Workspace::move_cell_in_experiment_tree_branch(self, tree_id, branch_id, cell_id, direction)
            .await
    }

    async fn delete_cell_from_experiment_tree_branch(
        &self,
        tree_id: &ExperimentTreeId,
        branch_id: &BranchId,
        cell_id: &CellId,
    ) -> TineResult<()> {
        Workspace::delete_cell_from_experiment_tree_branch(self, tree_id, branch_id, cell_id).await
    }

    async fn delete_experiment_tree_branch(
        &self,
        tree_id: &ExperimentTreeId,
        branch_id: &BranchId,
    ) -> TineResult<()> {
        Workspace::delete_experiment_tree_branch(self, tree_id, branch_id).await
    }

    async fn get_tree_runtime_state(&self, tree_id: &ExperimentTreeId) -> Option<TreeRuntimeState> {
        Workspace::get_tree_runtime_state(self, tree_id).await
    }

    async fn inspect_tree_kernel(
        &self,
        tree_id: &ExperimentTreeId,
    ) -> TineResult<RuntimeHealthSnapshot> {
        Workspace::inspect_tree_kernel(self, tree_id).await
    }

    async fn shutdown_tree_kernel(
        &self,
        tree_id: &ExperimentTreeId,
    ) -> TineResult<TreeRuntimeState> {
        Workspace::shutdown_tree_kernel(self, tree_id).await
    }

    async fn restart_tree_kernel(&self, tree_id: &ExperimentTreeId) -> TineResult<()> {
        Workspace::restart_tree_kernel(self, tree_id).await
    }

    async fn execute_cell_in_experiment_tree_branch(
        &self,
        tree_id: &ExperimentTreeId,
        branch_id: &BranchId,
        cell_id: &CellId,
    ) -> TineResult<(ExecutionId, NodeLogs)> {
        Workspace::execute_cell_in_experiment_tree_branch(self, tree_id, branch_id, cell_id).await
    }

    async fn execute_branch_in_experiment_tree(
        &self,
        tree_id: &ExperimentTreeId,
        branch_id: &BranchId,
    ) -> TineResult<ExecutionId> {
        Workspace::execute_branch_in_experiment_tree(self, tree_id, branch_id).await
    }

    async fn execute_all_branches_in_experiment_tree(
        &self,
        tree_id: &ExperimentTreeId,
    ) -> TineResult<Vec<(BranchId, ExecutionId)>> {
        Workspace::execute_all_branches_in_experiment_tree(self, tree_id).await
    }

    async fn cancel(&self, execution_id: &ExecutionId) -> TineResult<()> {
        let row: Option<(String,)> = sqlx::query_as("SELECT status FROM executions WHERE id = ?")
            .bind(execution_id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| TineError::Database(e.to_string()))?;

        let (status_json,) =
            row.ok_or_else(|| TineError::ExecutionNotFound(execution_id.clone()))?;
        let status: ExecutionStatus =
            Self::normalize_execution_status(serde_json::from_str(&status_json)?);
        if status.finished_at.is_some() {
            return Ok(());
        }

        if status.cancellation_requested_at.is_some()
            && matches!(status.phase, ExecutionPhase::CancellationRequested)
        {
            return Ok(());
        }

        let cancellation_requested_at = status.cancellation_requested_at.unwrap_or_else(Utc::now);
        let was_queued = self.dequeue_execution(execution_id).await?;
        if was_queued {
            Self::finalize_cancelled_execution(&self.pool, execution_id, cancellation_requested_at)
                .await?;
            return Ok(());
        }

        let marked = Self::update_execution_status_record(&self.pool, execution_id, |status| {
            status.cancellation_requested_at = Some(cancellation_requested_at);
            status.queue_position = None;
            Self::apply_execution_phase(status, ExecutionPhase::CancellationRequested);
        })
        .await?;
        if !marked {
            // The execution terminalized between our status read and the
            // cancellation marker: there is nothing left to cancel, and the
            // tree's kernel may already be running a *different* execution
            // — interrupting it now would kill the wrong run.
            return Ok(());
        }

        let tree_id = status.tree_id.clone().ok_or_else(|| {
            TineError::Internal(format!("execution '{}' is missing tree_id", execution_id))
        })?;
        // Interrupt only if this execution still occupies the tree kernel: it
        // may finish in the window after the marker commits, and the kernel is
        // shared, so an unconditional interrupt could hit a different
        // same-tree execution that started in the meantime.
        self.kernel_mgr
            .interrupt_tree_if_current(&tree_id, execution_id.as_str())
            .await?;

        let pool = self.pool.clone();
        let execution_id = execution_id.clone();
        tokio::spawn(async move {
            if let Err(err) = Self::await_cancellation_settle(&pool, &execution_id).await {
                warn!(execution = %execution_id, error = %err, "failed while waiting for cancelled execution to settle");
            }
            if let Err(err) =
                Self::finalize_cancelled_execution(&pool, &execution_id, cancellation_requested_at)
                    .await
            {
                warn!(execution = %execution_id, error = %err, "failed to finalize cancelled execution after interrupt");
            }
        });

        Ok(())
    }

    async fn status(&self, execution_id: &ExecutionId) -> TineResult<ExecutionStatus> {
        let row: Option<(String,)> = sqlx::query_as("SELECT status FROM executions WHERE id = ?")
            .bind(execution_id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| TineError::Database(e.to_string()))?;

        let (status_json,) =
            row.ok_or_else(|| TineError::ExecutionNotFound(execution_id.clone()))?;
        let status: ExecutionStatus = serde_json::from_str(&status_json)?;
        Ok(self
            .enrich_execution_status(Self::normalize_execution_status(status))
            .await)
    }

    async fn logs_for_tree_cell(
        &self,
        tree_id: &ExperimentTreeId,
        branch_id: &BranchId,
        cell_id: &CellId,
    ) -> TineResult<NodeLogs> {
        // Look up the most recent execution row for (tree, branch) and pull
        // its persisted logs. We then fold any unpersisted streaming chunks
        // for that execution out of the in-memory buffer so the response
        // reflects the latest live state.
        let rows: Vec<(String, Option<String>)> = sqlx::query_as(
            "SELECT id, node_logs FROM executions \
             WHERE tree_id = ? AND branch_id = ? AND target_kind = 'experiment_tree_branch' \
             ORDER BY rowid DESC LIMIT 50",
        )
        .bind(tree_id.as_str())
        .bind(branch_id.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| TineError::Database(e.to_string()))?;

        for (execution_id, maybe_logs_json) in &rows {
            if let Some(logs_json) = maybe_logs_json {
                let all_logs: HashMap<String, NodeLogs> =
                    serde_json::from_str(logs_json).unwrap_or_default();
                if let Some(logs) = all_logs.get(cell_id.as_str()) {
                    let mut merged = logs.clone();
                    Self::overlay_streaming_buffer(
                        &self.streaming_log_buffer,
                        &ExecutionId::new(execution_id),
                        &NodeId::new(cell_id.as_str()),
                        &mut merged,
                    );
                    return Ok(merged);
                }
            }
            // Even if persisted node_logs lacks this cell yet, any in-flight
            // streaming chunks may still exist for it.
            let mut empty = NodeLogs::default();
            if Self::overlay_streaming_buffer(
                &self.streaming_log_buffer,
                &ExecutionId::new(execution_id),
                &NodeId::new(cell_id.as_str()),
                &mut empty,
            ) {
                return Ok(empty);
            }
        }

        // Fallback: search any execution for this tree by cell id
        let fallback_rows: Vec<(String, Option<String>)> = sqlx::query_as(
            "SELECT id, node_logs FROM executions WHERE tree_id = ? ORDER BY rowid DESC LIMIT 50",
        )
        .bind(tree_id.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| TineError::Database(e.to_string()))?;

        for (execution_id, maybe_logs_json) in fallback_rows {
            if let Some(logs_json) = maybe_logs_json {
                let all_logs: HashMap<String, NodeLogs> =
                    serde_json::from_str(&logs_json).unwrap_or_default();
                if let Some(logs) = all_logs.get(cell_id.as_str()) {
                    let mut merged = logs.clone();
                    Self::overlay_streaming_buffer(
                        &self.streaming_log_buffer,
                        &ExecutionId::new(&execution_id),
                        &NodeId::new(cell_id.as_str()),
                        &mut merged,
                    );
                    return Ok(merged);
                }
            }
            let mut empty = NodeLogs::default();
            if Self::overlay_streaming_buffer(
                &self.streaming_log_buffer,
                &ExecutionId::new(&execution_id),
                &NodeId::new(cell_id.as_str()),
                &mut empty,
            ) {
                return Ok(empty);
            }
        }

        Ok(NodeLogs::default())
    }

    // -- Projects --

    async fn create_project(&self, project: ProjectDef) -> TineResult<ProjectId> {
        let id = if project.id.as_str().is_empty() {
            ProjectId::generate()
        } else {
            project.id.clone()
        };
        let workspace_dir = self.resolve_project_workspace_root(&project);
        tokio::fs::create_dir_all(&workspace_dir)
            .await
            .map_err(|e| {
                TineError::Internal(format!("cannot create project workspace dir: {}", e))
            })?;
        sqlx::query(
            "INSERT INTO projects (id, name, description, workspace_dir, created_at, updated_at) VALUES (?, ?, ?, ?, datetime('now'), datetime('now'))"
        )
            .bind(id.as_str())
            .bind(&project.name)
            .bind(&project.description)
            .bind(&project.workspace_dir)
            .execute(&self.pool)
            .await
            .map_err(|e| TineError::Database(e.to_string()))?;

        info!(project = %id, name = %project.name, "created project");
        Ok(id)
    }

    async fn list_projects(&self) -> TineResult<Vec<ProjectDef>> {
        let rows: Vec<(String, String, Option<String>, String, String, String)> =
            sqlx::query_as("SELECT id, name, description, workspace_dir, created_at, updated_at FROM projects ORDER BY created_at DESC")
                .fetch_all(&self.pool)
                .await
                .map_err(|e| TineError::Database(e.to_string()))?;

        let projects = rows
            .into_iter()
            .map(|(id, name, desc, dir, created, updated)| ProjectDef {
                id: ProjectId::new(id),
                name,
                description: desc,
                workspace_dir: dir,
                created_at: parse_sqlite_datetime(&created),
                updated_at: parse_sqlite_datetime(&updated),
            })
            .collect();
        Ok(projects)
    }

    async fn get_project(&self, id: &ProjectId) -> TineResult<ProjectDef> {
        let row: Option<(String, String, Option<String>, String, String, String)> =
            sqlx::query_as("SELECT id, name, description, workspace_dir, created_at, updated_at FROM projects WHERE id = ?")
                .bind(id.as_str())
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| TineError::Database(e.to_string()))?;

        let (pid, name, desc, dir, created, updated) =
            row.ok_or_else(|| TineError::ProjectNotFound(id.clone()))?;

        Ok(ProjectDef {
            id: ProjectId::new(pid),
            name,
            description: desc,
            workspace_dir: dir,
            created_at: parse_sqlite_datetime(&created),
            updated_at: parse_sqlite_datetime(&updated),
        })
    }

    async fn list_experiments(&self, project_id: &ProjectId) -> TineResult<Vec<ExperimentTreeDef>> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT definition FROM experiment_trees WHERE project_id = ? ORDER BY created_at DESC",
        )
        .bind(project_id.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| TineError::Database(e.to_string()))?;

        let results = rows
            .into_iter()
            .filter_map(|(def_json,)| serde_json::from_str(&def_json).ok())
            .collect();
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use async_trait::async_trait;
    use tempfile::TempDir;
    use tine_core::{ArtifactMetadata, CellRuntimeState, ExecutionMode, NodeCode};

    struct NoopArtifactStore;

    #[async_trait]
    impl ArtifactStore for NoopArtifactStore {
        async fn put(&self, _key: &ArtifactKey, data: &[u8]) -> TineResult<[u8; 32]> {
            Ok(*blake3::hash(data).as_bytes())
        }

        async fn get(&self, key: &ArtifactKey) -> TineResult<Vec<u8>> {
            Err(TineError::ArtifactNotFound(key.clone()))
        }

        async fn delete(&self, _key: &ArtifactKey) -> TineResult<()> {
            Ok(())
        }

        async fn exists(&self, _key: &ArtifactKey) -> TineResult<bool> {
            Ok(false)
        }

        async fn metadata(&self, key: &ArtifactKey) -> TineResult<ArtifactMetadata> {
            Err(TineError::ArtifactNotFound(key.clone()))
        }

        async fn list(&self) -> TineResult<Vec<ArtifactKey>> {
            Ok(Vec::new())
        }
    }

    fn code(source: &str) -> NodeCode {
        NodeCode {
            source: source.to_string(),
            language: "python".to_string(),
        }
    }

    fn test_tree() -> ExperimentTreeDef {
        let tree_id = ExperimentTreeId::new("tree");
        let main_branch = BranchId::new("main");
        let alt_branch = BranchId::new("alt");
        let cell_a = CellId::new("a");
        let cell_b = CellId::new("b");
        let cell_c = CellId::new("c");

        ExperimentTreeDef {
            id: tree_id.clone(),
            name: "Test Tree".to_string(),
            project_id: None,
            root_branch_id: main_branch.clone(),
            branches: vec![
                BranchDef {
                    id: main_branch.clone(),
                    name: "main".to_string(),
                    parent_branch_id: None,
                    branch_point_cell_id: None,
                    cell_order: vec![cell_a.clone(), cell_b.clone()],
                    display: HashMap::new(),
                },
                BranchDef {
                    id: alt_branch.clone(),
                    name: "alt".to_string(),
                    parent_branch_id: Some(main_branch),
                    branch_point_cell_id: Some(cell_a.clone()),
                    cell_order: vec![cell_c.clone()],
                    display: HashMap::new(),
                },
            ],
            cells: vec![
                CellDef {
                    id: cell_a.clone(),
                    tree_id: tree_id.clone(),
                    branch_id: BranchId::new("main"),
                    name: "A".to_string(),
                    code: code("a = 1"),
                    upstream_cell_ids: Vec::new(),
                    declared_outputs: Vec::new(),
                    cache: false,
                    map_over: None,
                    map_concurrency: None,
                    tags: HashMap::new(),
                    revision_id: None,
                    state: CellRuntimeState::Clean,
                },
                CellDef {
                    id: cell_b,
                    tree_id: tree_id.clone(),
                    branch_id: BranchId::new("main"),
                    name: "B".to_string(),
                    code: code("b = a + 1"),
                    upstream_cell_ids: vec![cell_a.clone()],
                    declared_outputs: Vec::new(),
                    cache: false,
                    map_over: None,
                    map_concurrency: None,
                    tags: HashMap::new(),
                    revision_id: None,
                    state: CellRuntimeState::Clean,
                },
                CellDef {
                    id: cell_c,
                    tree_id,
                    branch_id: alt_branch,
                    name: "C".to_string(),
                    code: code("c = a + 2"),
                    upstream_cell_ids: vec![cell_a.clone()],
                    declared_outputs: Vec::new(),
                    cache: false,
                    map_over: None,
                    map_concurrency: None,
                    tags: HashMap::new(),
                    revision_id: None,
                    state: CellRuntimeState::Clean,
                },
            ],
            environment: Default::default(),
            execution_mode: ExecutionMode::Parallel,
            budget: None,
            created_at: Utc::now(),
        }
    }

    #[test]
    fn tree_branch_execution_plan_projects_branch_runtime() {
        let tree = test_tree();
        let branch_id = BranchId::new("alt");

        let plan = Workspace::build_tree_branch_execution_plan(&tree, &branch_id).unwrap();

        assert_eq!(
            plan.executable_branch.tree_id,
            ExperimentTreeId::new("tree")
        );
        assert_eq!(plan.executable_branch.branch_id, BranchId::new("alt"));
        assert_eq!(
            plan.executable_branch.path_cell_order,
            vec![CellId::new("a"), CellId::new("c")]
        );
        assert_eq!(
            plan.executable_branch.topo_order,
            vec![CellId::new("a"), CellId::new("c")]
        );
        assert_eq!(
            plan.target,
            ExecutionTargetRef::ExperimentTreeBranch {
                tree_id: ExperimentTreeId::new("tree"),
                branch_id,
            }
        );
    }

    #[test]
    fn reconcile_abandoned_execution_status_marks_unfinished_nodes_interrupted() {
        let execution_id = ExecutionId::new("exec-1");
        let mut node_statuses = HashMap::new();
        node_statuses.insert(NodeId::new("queued"), NodeStatus::Queued);
        node_statuses.insert(NodeId::new("running"), NodeStatus::Running);
        node_statuses.insert(NodeId::new("done"), NodeStatus::Completed);

        let status = ExecutionStatus {
            execution_id,
            tree_id: Some(ExperimentTreeId::new("tree")),
            branch_id: Some(BranchId::new("main")),
            target_kind: Some(ExecutionTargetKind::ExperimentTreeBranch),
            target: Some(ExecutionTargetRef::ExperimentTreeBranch {
                tree_id: ExperimentTreeId::new("tree"),
                branch_id: BranchId::new("main"),
            }),
            status: ExecutionLifecycleStatus::Running,
            phase: ExecutionPhase::Running,
            queue_position: None,
            queue: None,
            runtime: None,
            cancellation_requested_at: None,
            node_statuses,
            started_at: Utc::now(),
            finished_at: None,
        };

        let reconciled = Workspace::reconcile_abandoned_execution_status(status);

        assert!(reconciled.finished_at.is_some());
        assert_eq!(reconciled.status, ExecutionLifecycleStatus::Failed);
        assert_eq!(reconciled.phase, ExecutionPhase::Failed);
        assert_eq!(
            reconciled.node_statuses.get(&NodeId::new("queued")),
            Some(&NodeStatus::Interrupted)
        );
        assert_eq!(
            reconciled.node_statuses.get(&NodeId::new("running")),
            Some(&NodeStatus::Interrupted)
        );
        assert_eq!(
            reconciled.node_statuses.get(&NodeId::new("done")),
            Some(&NodeStatus::Completed)
        );
    }

    #[test]
    fn reconcile_kernel_lost_execution_status_marks_running_nodes_interrupted() {
        let execution_id = ExecutionId::new("exec-kernel-lost");
        let mut node_statuses = HashMap::new();
        node_statuses.insert(NodeId::new("queued"), NodeStatus::Queued);
        node_statuses.insert(NodeId::new("running"), NodeStatus::Running);
        node_statuses.insert(NodeId::new("done"), NodeStatus::Completed);

        let status = ExecutionStatus {
            execution_id,
            tree_id: Some(ExperimentTreeId::new("tree")),
            branch_id: Some(BranchId::new("main")),
            target_kind: Some(ExecutionTargetKind::ExperimentTreeBranch),
            target: Some(ExecutionTargetRef::ExperimentTreeBranch {
                tree_id: ExperimentTreeId::new("tree"),
                branch_id: BranchId::new("main"),
            }),
            status: ExecutionLifecycleStatus::Running,
            phase: ExecutionPhase::Running,
            queue_position: None,
            queue: None,
            runtime: None,
            cancellation_requested_at: None,
            node_statuses,
            started_at: Utc::now(),
            finished_at: None,
        };

        let (reconciled, node_logs) = Workspace::reconcile_kernel_lost_execution_status(
            status,
            HashMap::new(),
            "Kernel heartbeat lost while execution was running",
        );

        assert!(reconciled.finished_at.is_some());
        assert_eq!(reconciled.status, ExecutionLifecycleStatus::Failed);
        assert_eq!(reconciled.phase, ExecutionPhase::Failed);
        assert_eq!(
            reconciled.node_statuses.get(&NodeId::new("queued")),
            Some(&NodeStatus::Interrupted)
        );
        assert_eq!(
            reconciled.node_statuses.get(&NodeId::new("running")),
            Some(&NodeStatus::Interrupted)
        );
        assert_eq!(
            reconciled.node_statuses.get(&NodeId::new("done")),
            Some(&NodeStatus::Completed)
        );
        assert_eq!(
            node_logs
                .get(&NodeId::new("running"))
                .and_then(|logs| logs.error.as_ref())
                .map(|error| error.ename.as_str()),
            Some("KernelLost")
        );
    }

    #[test]
    fn renumber_auto_named_branch_cells_preserves_custom_names() {
        let branch_id = BranchId::new("main");
        let mut tree = ExperimentTreeDef {
            id: ExperimentTreeId::new("tree"),
            name: "tree".to_string(),
            project_id: None,
            root_branch_id: branch_id.clone(),
            branches: vec![BranchDef {
                id: branch_id.clone(),
                name: "main".to_string(),
                parent_branch_id: None,
                branch_point_cell_id: None,
                cell_order: vec![
                    CellId::new("cell_1"),
                    CellId::new("cell_2"),
                    CellId::new("cell_3"),
                ],
                display: HashMap::new(),
            }],
            cells: vec![
                CellDef {
                    id: CellId::new("cell_1"),
                    tree_id: ExperimentTreeId::new("tree"),
                    branch_id: branch_id.clone(),
                    name: "cell_1".to_string(),
                    code: code("a = 1"),
                    upstream_cell_ids: Vec::new(),
                    declared_outputs: Vec::new(),
                    cache: false,
                    map_over: None,
                    map_concurrency: None,
                    tags: HashMap::new(),
                    revision_id: None,
                    state: CellRuntimeState::Clean,
                },
                CellDef {
                    id: CellId::new("cell_2"),
                    tree_id: ExperimentTreeId::new("tree"),
                    branch_id: branch_id.clone(),
                    name: "Important".to_string(),
                    code: code("b = 2"),
                    upstream_cell_ids: Vec::new(),
                    declared_outputs: Vec::new(),
                    cache: false,
                    map_over: None,
                    map_concurrency: None,
                    tags: HashMap::new(),
                    revision_id: None,
                    state: CellRuntimeState::Clean,
                },
                CellDef {
                    id: CellId::new("cell_3"),
                    tree_id: ExperimentTreeId::new("tree"),
                    branch_id: branch_id.clone(),
                    name: "branch_cell".to_string(),
                    code: code("c = 3"),
                    upstream_cell_ids: Vec::new(),
                    declared_outputs: Vec::new(),
                    cache: false,
                    map_over: None,
                    map_concurrency: None,
                    tags: HashMap::new(),
                    revision_id: None,
                    state: CellRuntimeState::Clean,
                },
            ],
            environment: Default::default(),
            execution_mode: ExecutionMode::Parallel,
            budget: None,
            created_at: Utc::now(),
        };

        Workspace::renumber_auto_named_branch_cells(&mut tree, &branch_id);

        assert_eq!(tree.cells[0].name, "Cell 1");
        assert_eq!(tree.cells[1].name, "Important");
        assert_eq!(tree.cells[2].name, "Cell 3");
    }

    #[tokio::test]
    async fn create_experiment_tree_starts_with_empty_first_cell() {
        let tmp = TempDir::new().expect("failed to create temp dir");
        let store: Arc<dyn ArtifactStore> = Arc::new(NoopArtifactStore);
        let workspace = Workspace::open(tmp.path().to_path_buf(), store, 1)
            .await
            .expect("failed to open workspace");

        let tree = workspace
            .create_experiment_tree("Simple Tree", None)
            .await
            .expect("failed to create experiment tree");

        assert_eq!(tree.branches.len(), 1);
        assert_eq!(tree.cells.len(), 1);
        assert_eq!(tree.cells[0].name, "Cell 1");
        assert_eq!(tree.cells[0].code.language, "python");
        assert!(tree.cells[0].code.source.is_empty());

        workspace
            .shutdown()
            .await
            .expect("failed to shut down workspace");
    }

    #[tokio::test]
    async fn enqueue_execution_tracks_queue_positions_and_enforces_backpressure() {
        let tmp = TempDir::new().expect("failed to create temp dir");
        let store: Arc<dyn ArtifactStore> = Arc::new(NoopArtifactStore);
        let workspace = Workspace::open(tmp.path().to_path_buf(), store, 1)
            .await
            .expect("failed to open workspace");

        let tree_id = ExperimentTreeId::new("tree");
        let branch_id = BranchId::new("main");
        let target = Workspace::execution_target_for_tree_branch(&tree_id, &branch_id);
        let topo_order = vec![CellId::new("cell_1")];

        let mut queued_ids = Vec::new();
        for expected_position in 1..=workspace.max_queue_depth {
            let execution_id = ExecutionId::generate();
            Workspace::insert_branch_execution_record(
                &workspace.pool,
                &execution_id,
                &tree_id,
                &branch_id,
                &target,
                &topo_order,
            )
            .await
            .expect("failed to insert execution record");
            let queue_position = workspace
                .enqueue_execution(&execution_id)
                .await
                .expect("failed to enqueue execution");
            assert_eq!(queue_position, expected_position as u64);
            queued_ids.push(execution_id);
        }

        let second_status = WorkspaceApi::status(&workspace, &queued_ids[1])
            .await
            .expect("failed to load queued status");
        assert_eq!(second_status.queue_position, Some(2));
        assert_eq!(second_status.status, ExecutionLifecycleStatus::Queued);

        assert!(workspace
            .dequeue_execution(&queued_ids[0])
            .await
            .expect("failed to dequeue queued execution"));
        let shifted_status = WorkspaceApi::status(&workspace, &queued_ids[1])
            .await
            .expect("failed to load shifted queued status");
        assert_eq!(shifted_status.queue_position, Some(1));

        let refill_execution = ExecutionId::generate();
        Workspace::insert_branch_execution_record(
            &workspace.pool,
            &refill_execution,
            &tree_id,
            &branch_id,
            &target,
            &topo_order,
        )
        .await
        .expect("failed to insert refill execution record");
        let refill_position = workspace
            .enqueue_execution(&refill_execution)
            .await
            .expect("expected queue slot freed by dequeue to be reusable");
        assert_eq!(refill_position, workspace.max_queue_depth as u64);

        let rejected_execution = ExecutionId::generate();
        Workspace::insert_branch_execution_record(
            &workspace.pool,
            &rejected_execution,
            &tree_id,
            &branch_id,
            &target,
            &topo_order,
        )
        .await
        .expect("failed to insert rejected execution record");
        let err = workspace
            .enqueue_execution(&rejected_execution)
            .await
            .expect_err("expected queue backpressure");
        assert!(matches!(err, TineError::BudgetExceeded(_)));

        workspace
            .shutdown()
            .await
            .expect("failed to shut down workspace");
    }

    /// Cancel marks `CancellationRequested` via this helper and then
    /// interrupts the tree's kernel — but only when the marker actually
    /// committed. If the execution terminalizes between cancel's status
    /// read and the marker write, the helper must report "not applied" so
    /// cancel does not interrupt a kernel that may already be running a
    /// different same-tree execution.
    #[tokio::test]
    async fn update_execution_status_record_reports_whether_a_live_row_was_updated() {
        let tmp = TempDir::new().expect("failed to create temp dir");
        let store: Arc<dyn ArtifactStore> = Arc::new(NoopArtifactStore);
        let workspace = Workspace::open(tmp.path().to_path_buf(), store, 1)
            .await
            .expect("failed to open workspace");

        let tree_id = ExperimentTreeId::new("tree");
        let branch_id = BranchId::new("main");
        let target = Workspace::execution_target_for_tree_branch(&tree_id, &branch_id);
        let execution_id = ExecutionId::generate();
        Workspace::insert_branch_execution_record(
            &workspace.pool,
            &execution_id,
            &tree_id,
            &branch_id,
            &target,
            &[CellId::new("cell_1")],
        )
        .await
        .expect("failed to insert execution record");

        let marked = Workspace::update_execution_status_record(
            &workspace.pool,
            &execution_id,
            |status| {
                status.cancellation_requested_at = Some(Utc::now());
                Workspace::apply_execution_phase(status, ExecutionPhase::CancellationRequested);
            },
        )
        .await
        .expect("status update failed");
        assert!(marked, "a live execution must accept the update");

        // A concurrent finalizer wins the race: the execution is terminal.
        Workspace::update_execution_status_record(&workspace.pool, &execution_id, |status| {
            status.finished_at = Some(Utc::now());
            Workspace::apply_execution_phase(status, ExecutionPhase::Completed);
        })
        .await
        .expect("finalize failed");

        let marked = Workspace::update_execution_status_record(
            &workspace.pool,
            &execution_id,
            |status| {
                status.cancellation_requested_at = Some(Utc::now());
                Workspace::apply_execution_phase(status, ExecutionPhase::CancellationRequested);
            },
        )
        .await
        .expect("status update failed");
        assert!(
            !marked,
            "a terminal execution must reject the update so cancel skips the kernel interrupt"
        );

        let marked = Workspace::update_execution_status_record(
            &workspace.pool,
            &ExecutionId::generate(),
            |_| {},
        )
        .await
        .expect("status update failed");
        assert!(!marked, "a missing execution must report not-applied");

        workspace
            .shutdown()
            .await
            .expect("failed to shut down workspace");
    }

    /// Idempotency reservations are reclaimed once a terminal execution
    /// ages past the retention window — bounding table/index growth — while
    /// recent terminal reservations and still-running ones are preserved so
    /// near-term retries remain safe.
    #[tokio::test]
    async fn prune_stale_idempotency_reservations_releases_only_aged_terminal_rows() {
        let tmp = TempDir::new().expect("failed to create temp dir");
        let store: Arc<dyn ArtifactStore> = Arc::new(NoopArtifactStore);
        let workspace = Workspace::open(tmp.path().to_path_buf(), store, 1)
            .await
            .expect("failed to open workspace");

        let tree_id = ExperimentTreeId::new("tree");
        let branch_id = BranchId::new("main");
        let target = Workspace::execution_target_for_tree_branch(&tree_id, &branch_id);
        let topo = [CellId::new("cell_1")];
        let scope = Workspace::branch_execution_idempotency_scope(&tree_id, &branch_id);

        let insert_keyed = |key: &'static str| {
            let exec_id = ExecutionId::generate();
            let scope = scope.clone();
            let target = target.clone();
            let topo = topo.clone();
            let pool = workspace.pool.clone();
            let tree_id = tree_id.clone();
            let branch_id = branch_id.clone();
            async move {
                let idem = ExecutionIdempotency {
                    key,
                    scope: &scope,
                    fingerprint: "fp",
                };
                Workspace::insert_branch_execution_record_with_key(
                    &pool, &exec_id, &tree_id, &branch_id, &target, &topo, Some(&idem),
                )
                .await
                .expect("insert keyed execution");
                exec_id
            }
        };

        let aged = insert_keyed("aged-key").await;
        let recent = insert_keyed("recent-key").await;
        let running = insert_keyed("running-key").await;

        // Terminalize aged (well past retention) and recent (just now).
        sqlx::query("UPDATE executions SET finished_at = datetime('now', '-30 days') WHERE id = ?")
            .bind(aged.as_str())
            .execute(&workspace.pool)
            .await
            .expect("age the terminal execution");
        sqlx::query("UPDATE executions SET finished_at = datetime('now') WHERE id = ?")
            .bind(recent.as_str())
            .execute(&workspace.pool)
            .await
            .expect("finalize the recent execution");
        // `running` keeps finished_at NULL.

        Workspace::prune_stale_idempotency_reservations(&workspace.pool).await;

        let key_present = |exec: ExecutionId| {
            let pool = workspace.pool.clone();
            async move {
                let (key,): (Option<String>,) =
                    sqlx::query_as("SELECT idempotency_key FROM executions WHERE id = ?")
                        .bind(exec.as_str())
                        .fetch_one(&pool)
                        .await
                        .expect("read row");
                key.is_some()
            }
        };

        assert!(
            !key_present(aged).await,
            "an aged terminal reservation must be released"
        );
        assert!(
            key_present(recent).await,
            "a recently terminal reservation must be retained for near-term retries"
        );
        assert!(
            key_present(running).await,
            "a running execution's reservation must never be pruned"
        );

        workspace
            .shutdown()
            .await
            .expect("failed to shut down workspace");
    }

    /// A pre-acceptance rejection (e.g. enqueue failure under queue
    /// backpressure) must release the submission's idempotency reservation:
    /// a retry with the same key has to start a fresh execution instead of
    /// reattaching to the rejected one forever.
    #[tokio::test]
    async fn reject_execution_releases_idempotency_reservation_for_retry() {
        let tmp = TempDir::new().expect("failed to create temp dir");
        let store: Arc<dyn ArtifactStore> = Arc::new(NoopArtifactStore);
        let workspace = Workspace::open(tmp.path().to_path_buf(), store, 1)
            .await
            .expect("failed to open workspace");

        let tree_id = ExperimentTreeId::new("tree");
        let branch_id = BranchId::new("main");
        let target = Workspace::execution_target_for_tree_branch(&tree_id, &branch_id);
        let topo_order = vec![CellId::new("cell_1")];
        let scope = Workspace::branch_execution_idempotency_scope(&tree_id, &branch_id);
        let idempotency = ExecutionIdempotency {
            key: "retry-key-1",
            scope: &scope,
            fingerprint: "fingerprint-1",
        };

        let rejected_execution = ExecutionId::generate();
        Workspace::insert_branch_execution_record_with_key(
            &workspace.pool,
            &rejected_execution,
            &tree_id,
            &branch_id,
            &target,
            &topo_order,
            Some(&idempotency),
        )
        .await
        .expect("failed to insert keyed execution record");
        assert_eq!(
            Workspace::find_execution_by_idempotency_key(&workspace.pool, &idempotency)
                .await
                .expect("failed to look up idempotency key"),
            Some(rejected_execution.clone())
        );

        // The same key with a different fingerprint is a conflict, not a
        // silent reattach.
        let changed_request = ExecutionIdempotency {
            key: "retry-key-1",
            scope: &scope,
            fingerprint: "fingerprint-2",
        };
        let err = Workspace::find_execution_by_idempotency_key(&workspace.pool, &changed_request)
            .await
            .expect_err("key reuse with changed state must conflict");
        assert!(matches!(err, TineError::IdempotencyConflict(_)));

        workspace
            .reject_execution(&rejected_execution)
            .await
            .expect("failed to reject execution");

        assert_eq!(
            Workspace::find_execution_by_idempotency_key(&workspace.pool, &idempotency)
                .await
                .expect("failed to look up idempotency key after rejection"),
            None,
            "a rejected submission must not hold its idempotency key"
        );

        // The retry reuses the key: the unique (key, scope) index must not
        // block it, and the lookup must resolve to the fresh execution.
        let retried_execution = ExecutionId::generate();
        Workspace::insert_branch_execution_record_with_key(
            &workspace.pool,
            &retried_execution,
            &tree_id,
            &branch_id,
            &target,
            &topo_order,
            Some(&idempotency),
        )
        .await
        .expect("retry with the same key must insert a fresh execution");
        assert_eq!(
            Workspace::find_execution_by_idempotency_key(&workspace.pool, &idempotency)
                .await
                .expect("failed to look up idempotency key after retry"),
            Some(retried_execution)
        );

        workspace
            .shutdown()
            .await
            .expect("failed to shut down workspace");
    }

    fn fingerprint_sample_branch() -> ExecutableTreeBranch {
        serde_json::from_value(serde_json::json!({
            "tree_id": "tree-1",
            "branch_id": "main",
            "name": "tree-1 [main]",
            "lineage": ["main"],
            "path_cell_order": ["cell-1", "cell-2"],
            "topo_order": ["cell-1", "cell-2"],
            "cells": [
                {
                    "tree_id": "tree-1",
                    "branch_id": "main",
                    "cell_id": "cell-1",
                    "name": "cell-1",
                    "code": { "source": "x = 1", "language": "python" },
                    "inputs": {},
                    "outputs": ["result"],
                    "cache": true,
                    "map_over": null,
                    "map_concurrency": null,
                    "tags": {},
                    "revision_id": null
                },
                {
                    "tree_id": "tree-1",
                    "branch_id": "main",
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
                    "tags": {},
                    "revision_id": null
                }
            ],
            "environment": { "dependencies": ["pandas"] },
            "execution_mode": "parallel",
            "budget": null,
            "created_at": "2026-01-01T00:00:00Z"
        }))
        .expect("sample branch must deserialize")
    }

    /// The idempotency fingerprint must cover every execution-relevant part
    /// of the request — a same-key retry after any of these mutations is a
    /// different request and must not match — while staying stable across
    /// pure-metadata changes, which would otherwise turn genuine retries
    /// into false conflicts.
    #[test]
    fn execution_request_fingerprint_tracks_plan_state_not_metadata() {
        let fingerprint = |branch: &ExecutableTreeBranch| {
            Workspace::execution_request_fingerprint(branch, Path::new("/ws"))
        };
        let base = fingerprint_sample_branch();
        let baseline = fingerprint(&base);
        assert_eq!(
            baseline,
            fingerprint(&fingerprint_sample_branch()),
            "identical plans must fingerprint identically"
        );

        let mut reordered = fingerprint_sample_branch();
        reordered.path_cell_order.reverse();
        reordered.topo_order.reverse();
        assert_ne!(
            baseline,
            fingerprint(&reordered),
            "cell reordering changes the execution graph"
        );

        let mut rewired = fingerprint_sample_branch();
        rewired.cells[1]
            .inputs
            .get_mut(&SlotName::new("input"))
            .expect("sample input")
            .source_output = SlotName::new("aux");
        assert_ne!(
            baseline,
            fingerprint(&rewired),
            "input rewiring changes what the cell consumes"
        );

        let mut outputs_changed = fingerprint_sample_branch();
        outputs_changed.cells[0].outputs.push(SlotName::new("aux"));
        assert_ne!(
            baseline,
            fingerprint(&outputs_changed)
        );

        let mut cache_changed = fingerprint_sample_branch();
        cache_changed.cells[0].cache = false;
        assert_ne!(
            baseline,
            fingerprint(&cache_changed)
        );

        let mut map_changed = fingerprint_sample_branch();
        map_changed.cells[1].map_over = Some(SlotName::new("input"));
        map_changed.cells[1].map_concurrency = Some(4);
        assert_ne!(
            baseline,
            fingerprint(&map_changed)
        );

        let mut mode_changed = fingerprint_sample_branch();
        mode_changed.execution_mode = ExecutionMode::Sequential;
        assert_ne!(
            baseline,
            fingerprint(&mode_changed)
        );

        let mut budget_changed = fingerprint_sample_branch();
        budget_changed.budget = Some(tine_core::WorkspaceBudget {
            max_kernels: Some(1),
            max_kernel_rss_bytes: None,
            max_artifact_storage_bytes: None,
        });
        assert_ne!(
            baseline,
            fingerprint(&budget_changed)
        );

        let mut project_changed = fingerprint_sample_branch();
        project_changed.project_id = Some(ProjectId::new("project-2"));
        assert_ne!(
            baseline,
            fingerprint(&project_changed),
            "a different project is a different execution context"
        );

        assert_ne!(
            baseline,
            Workspace::execution_request_fingerprint(&base, Path::new("/other-ws")),
            "a different working directory is a different execution context"
        );

        // Metadata-only mutations are not execution-relevant: a retry after
        // a rename, retag, or no-op save revision must still reattach.
        let mut metadata_changed = fingerprint_sample_branch();
        metadata_changed.cells[0].name = "renamed".to_string();
        metadata_changed.cells[0]
            .tags
            .insert("stage".to_string(), "eval".to_string());
        metadata_changed.cells[0].revision_id = Some(tine_core::RevisionId::new("rev-2"));
        assert_eq!(
            baseline,
            fingerprint(&metadata_changed),
            "metadata changes must not poison retries"
        );
    }

    /// Full upgrade path: a populated database written by the PREVIOUS
    /// release (cache keyed on node_id without scope_hash; executions without
    /// idempotency columns) must open cleanly through `Workspace::open`, with
    /// the cache rows migrated (data-moving rebuild) and the executions rows
    /// preserved (additive columns) — the one path the fresh-open and the
    /// isolated migration unit tests don't jointly cover.
    #[tokio::test]
    async fn open_upgrades_a_populated_pre_release_database() {
        let tmp = TempDir::new().expect("failed to create temp dir");
        let tine_dir = tmp.path().join(".tine");
        std::fs::create_dir_all(&tine_dir).expect("create .tine");
        let db_path = tine_dir.join("tine.db");
        let db_url = format!("sqlite://{}?mode=rwc", db_path.display());

        let code_hash_hex = hex::encode(NodeCacheKey::hash_code("x = 1"));
        let lockfile_hex = hex::encode([7u8; 32]);

        // Build the pre-release on-disk schema and populate it.
        {
            let pool = SqlitePool::connect(&db_url)
                .await
                .expect("open fixture db");
            // Pre-release cache: node_id in the PK, NO scope_hash column.
            sqlx::query(
                "CREATE TABLE cache (
                    code_hash       BLOB NOT NULL,
                    input_hashes    TEXT NOT NULL,
                    lockfile_hash   BLOB NOT NULL,
                    artifacts       TEXT NOT NULL,
                    source_runtime_id TEXT,
                    node_id         TEXT NOT NULL DEFAULT '',
                    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
                    last_accessed   TEXT NOT NULL DEFAULT (datetime('now')),
                    PRIMARY KEY (code_hash, input_hashes, lockfile_hash, node_id)
                )",
            )
            .execute(&pool)
            .await
            .expect("create old cache");
            sqlx::query(
                "INSERT INTO cache (code_hash, input_hashes, lockfile_hash, artifacts, source_runtime_id, node_id) \
                 VALUES (?, '{}', ?, '{\"out\": \"artifact-old\"}', 'tree-old::main', 'cell_1')",
            )
            .bind(&code_hash_hex)
            .bind(&lockfile_hex)
            .execute(&pool)
            .await
            .expect("seed old cache row");

            // Pre-release executions: the 001 shape, NO idempotency columns.
            // finished_at is set so startup reconciliation skips the row
            // (and never parses its status).
            sqlx::query(
                "CREATE TABLE executions (
                    id              TEXT PRIMARY KEY,
                    tree_id         TEXT NOT NULL,
                    branch_id       TEXT,
                    target_kind     TEXT,
                    status          TEXT NOT NULL,
                    started_at      TEXT NOT NULL DEFAULT (datetime('now')),
                    finished_at     TEXT,
                    node_logs       TEXT
                )",
            )
            .execute(&pool)
            .await
            .expect("create old executions");
            sqlx::query(
                "INSERT INTO executions (id, tree_id, branch_id, target_kind, status, finished_at) \
                 VALUES ('exec-old', 'tree-old', 'main', 'experiment_tree_branch', '{}', datetime('now'))",
            )
            .execute(&pool)
            .await
            .expect("seed old execution row");
            pool.close().await;
        }

        // Open the workspace: runs the full migration chain against the
        // populated old database.
        let store: Arc<dyn ArtifactStore> = Arc::new(NoopArtifactStore);
        let workspace = Workspace::open(tmp.path().to_path_buf(), store, 1)
            .await
            .expect("opening a populated pre-release database must succeed");

        // Cache: rebuilt with scope_hash in the key, old row preserved and
        // still reconstructable via its provenance.
        let scope_hash_in_pk: bool = sqlx::query_scalar(
            "SELECT COUNT(*) > 0 FROM pragma_table_info('cache') WHERE name = 'scope_hash' AND pk > 0",
        )
        .fetch_one(&workspace.pool)
        .await
        .expect("inspect cache pk");
        assert!(scope_hash_in_pk, "cache must be rebuilt with scope_hash in the key");

        let cache = Workspace::load_cache_from_pool(&workspace.pool)
            .await
            .expect("load migrated cache");
        let key = NodeCacheKey {
            code_hash: NodeCacheKey::hash_code("x = 1"),
            input_hashes: HashMap::new(),
            lockfile_hash: [7u8; 32],
            scope_hash: NodeCacheKey::scope_for("tree-old", "cell_1"),
        };
        assert_eq!(
            cache.get(&key).and_then(|a| a.get(&SlotName::new("out"))),
            Some(&ArtifactKey::new("artifact-old")),
            "the migrated cache row must survive and stay reachable by its provenance scope"
        );

        // Executions: idempotency columns added, old row preserved.
        let has_idempotency: bool = sqlx::query_scalar(
            "SELECT COUNT(*) = 3 FROM pragma_table_info('executions') \
             WHERE name IN ('idempotency_key', 'idempotency_scope', 'idempotency_fingerprint')",
        )
        .fetch_one(&workspace.pool)
        .await
        .expect("inspect executions columns");
        assert!(has_idempotency, "executions must gain the idempotency columns");
        let old_execution_survived: bool =
            sqlx::query_scalar("SELECT COUNT(*) > 0 FROM executions WHERE id = 'exec-old'")
                .fetch_one(&workspace.pool)
                .await
                .expect("count old execution");
        assert!(old_execution_survived, "the pre-release execution row must survive the upgrade");

        workspace.shutdown().await.expect("shutdown");
    }

    /// Migration 010 must upgrade a cache table from the previous schema
    /// (node_id-scoped primary key, no scope_hash) in place: rows survive
    /// with an empty scope_hash, and the rebuilt key accepts new scoped
    /// rows alongside them.
    #[tokio::test]
    async fn migration_010_rebuilds_old_cache_schema_with_scope_hash() {
        let pool = SqlitePool::connect("sqlite::memory:")
            .await
            .expect("failed to open in-memory sqlite");
        sqlx::query(
            "CREATE TABLE cache (
                code_hash       BLOB NOT NULL,
                input_hashes    TEXT NOT NULL,
                lockfile_hash   BLOB NOT NULL,
                artifacts       TEXT NOT NULL,
                source_runtime_id TEXT,
                node_id         TEXT NOT NULL DEFAULT '',
                created_at      TEXT NOT NULL DEFAULT (datetime('now')),
                last_accessed   TEXT NOT NULL DEFAULT (datetime('now')),
                PRIMARY KEY (code_hash, input_hashes, lockfile_hash, node_id)
            )",
        )
        .execute(&pool)
        .await
        .expect("failed to create old-schema cache table");
        sqlx::query(
            "INSERT INTO cache (code_hash, input_hashes, lockfile_hash, artifacts, source_runtime_id, node_id) \
             VALUES ('aa', '{}', 'bb', '{}', 'tree-1::main', 'cell_1')",
        )
        .execute(&pool)
        .await
        .expect("failed to insert old-schema row");

        Workspace::run_migration_010(&pool)
            .await
            .expect("migration 010 must rebuild the old schema");

        let (node_id, scope_hash): (String, String) =
            sqlx::query_as("SELECT node_id, scope_hash FROM cache")
                .fetch_one(&pool)
                .await
                .expect("migrated row must survive");
        assert_eq!(node_id, "cell_1");
        assert_eq!(scope_hash, "");

        // Same content from a different scope now coexists instead of
        // replacing the migrated row.
        sqlx::query(
            "INSERT OR REPLACE INTO cache (code_hash, input_hashes, lockfile_hash, artifacts, source_runtime_id, node_id, scope_hash) \
             VALUES ('aa', '{}', 'bb', '{}', 'tree-2::main', 'cell_1', 'scope-2')",
        )
        .execute(&pool)
        .await
        .expect("failed to insert scoped row");
        let row_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM cache")
            .fetch_one(&pool)
            .await
            .expect("failed to count cache rows");
        assert_eq!(row_count, 2);

        // Re-running the migration is a no-op once scope_hash is in the key.
        Workspace::run_migration_010(&pool)
            .await
            .expect("migration 010 must be idempotent");
    }

    /// The persisted cache must keep one row per (tree, cell) scope and
    /// reconstruct distinct in-memory keys for them: two trees sharing a
    /// cell id, code, inputs, and lockfile are different scopes, and legacy
    /// rows without a stored scope still derive theirs from provenance.
    #[tokio::test]
    async fn load_cache_keeps_same_content_entries_from_different_trees_distinct() {
        let tmp = TempDir::new().expect("failed to create temp dir");
        let store: Arc<dyn ArtifactStore> = Arc::new(NoopArtifactStore);
        let workspace = Workspace::open(tmp.path().to_path_buf(), store, 1)
            .await
            .expect("failed to open workspace");

        let code_hash_hex = hex::encode(NodeCacheKey::hash_code("x = 1"));
        let lockfile_hash_hex = hex::encode([7u8; 32]);
        for tree_id in ["tree-a", "tree-b"] {
            let scope_hash_hex = hex::encode(NodeCacheKey::scope_for(tree_id, "cell_1"));
            sqlx::query(
                "INSERT INTO cache (code_hash, input_hashes, lockfile_hash, artifacts, source_runtime_id, node_id, scope_hash) \
                 VALUES (?, '{}', ?, ?, ?, 'cell_1', ?)",
            )
            .bind(&code_hash_hex)
            .bind(&lockfile_hash_hex)
            .bind(format!("{{\"out\": \"artifact-{tree_id}\"}}"))
            .bind(format!("{tree_id}::main"))
            .bind(&scope_hash_hex)
            .execute(&workspace.pool)
            .await
            .expect("failed to insert scoped cache row");
        }
        // Legacy row migrated without a stored scope: same content again,
        // attributed via source_runtime_id at load time.
        sqlx::query(
            "INSERT INTO cache (code_hash, input_hashes, lockfile_hash, artifacts, source_runtime_id, node_id, scope_hash) \
             VALUES (?, '{}', ?, '{\"out\": \"artifact-tree-legacy\"}', 'tree-legacy::main', 'cell_1', '')",
        )
        .bind(&code_hash_hex)
        .bind(&lockfile_hash_hex)
        .execute(&workspace.pool)
        .await
        .expect("failed to insert legacy cache row");

        let cache = Workspace::load_cache_from_pool(&workspace.pool)
            .await
            .expect("failed to load cache");
        assert_eq!(
            cache.len(),
            3,
            "same-content entries from different trees must stay distinct"
        );
        for tree_id in ["tree-a", "tree-b", "tree-legacy"] {
            let key = NodeCacheKey {
                code_hash: NodeCacheKey::hash_code("x = 1"),
                input_hashes: HashMap::new(),
                lockfile_hash: [7u8; 32],
                scope_hash: NodeCacheKey::scope_for(tree_id, "cell_1"),
            };
            let artifacts = cache
                .get(&key)
                .unwrap_or_else(|| panic!("missing cache entry for {tree_id}"));
            assert_eq!(
                artifacts.get(&SlotName::new("out")),
                Some(&ArtifactKey::new(format!("artifact-{tree_id}")))
            );
        }

        workspace
            .shutdown()
            .await
            .expect("failed to shut down workspace");
    }

    /// Replicates the server-side root cause of MCP "failed to tool call"
    /// cascades during long-running cells: every NodeStream event reads,
    /// deserializes, mutates, serializes, and writes the entire node_logs
    /// blob. As the log grows, per-event work scales with total log size,
    /// producing O(N²) total work for N stream chunks. While the executor
    /// is hammering this update path, status reads compete for the same
    /// SQLite write lock and the same connection pool, slowing every poll.
    ///
    /// This test drives 200 NodeStream events of ~1 KiB each through the
    /// real `persist_execution_event_snapshot` path and asserts the *total*
    /// time stays bounded. Without the fix the test takes >10 s; with a
    /// linear-time persistence path it should complete in well under 5 s.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn persist_execution_event_snapshot_streaming_does_not_scale_quadratically() {
        let tmp = TempDir::new().expect("failed to create temp dir");
        let store: Arc<dyn ArtifactStore> = Arc::new(NoopArtifactStore);
        let workspace = Workspace::open(tmp.path().to_path_buf(), store, 1)
            .await
            .expect("failed to open workspace");

        let tree_id = ExperimentTreeId::new("tree-stream");
        let branch_id = BranchId::new("main");
        let cell_id = CellId::new("cell_stream");
        let execution_id = ExecutionId::new("exec-stream");
        let target = Workspace::execution_target_for_tree_branch(&tree_id, &branch_id);

        Workspace::insert_branch_execution_record(
            &workspace.pool,
            &execution_id,
            &tree_id,
            &branch_id,
            &target,
            &[cell_id.clone()],
        )
        .await
        .expect("failed to insert execution record");

        // Mark the execution as running so persist_execution_event_snapshot
        // doesn't short-circuit on finished_at.
        let started_event = ExecutionEvent::ExecutionStarted {
            execution_id: execution_id.clone(),
            tree_id: Some(tree_id.clone()),
            branch_id: Some(branch_id.clone()),
            target_kind: Some(ExecutionTargetKind::ExperimentTreeBranch),
            target: Some(target.clone()),
        };
        Workspace::persist_execution_event_snapshot(
            &workspace.pool,
            &workspace.streaming_log_buffer,
            &started_event,
        )
        .await
        .expect("failed to persist started event");
        let node_started_event = ExecutionEvent::NodeStarted {
            execution_id: execution_id.clone(),
            node_id: NodeId::new(cell_id.as_str()),
            tree_id: Some(tree_id.clone()),
            branch_id: Some(branch_id.clone()),
            target_kind: Some(ExecutionTargetKind::ExperimentTreeBranch),
            target: Some(target.clone()),
        };
        Workspace::persist_execution_event_snapshot(
            &workspace.pool,
            &workspace.streaming_log_buffer,
            &node_started_event,
        )
        .await
        .expect("failed to persist node started event");

        // 2000 chunks × 4 KiB ≈ 8 MiB cumulative log. Calibrated against
        // observed real workloads: a multi-fold LightGBM training run prints
        // ~4 KiB per boosting round and a 2-3 minute fit easily produces
        // thousands of rounds of output. Each NodeStream event under the
        // current implementation reads the full accumulated logs blob,
        // mutates it in memory, serializes it, and writes the whole thing
        // back, which is O(N²) total work for N chunks.
        const STREAM_CHUNKS: usize = 2000;
        const CHUNK_BYTES: usize = 4096;
        let chunk_payload = "x".repeat(CHUNK_BYTES);

        let start = std::time::Instant::now();
        for _ in 0..STREAM_CHUNKS {
            let event = ExecutionEvent::NodeStream {
                execution_id: execution_id.clone(),
                node_id: NodeId::new(cell_id.as_str()),
                tree_id: Some(tree_id.clone()),
                branch_id: Some(branch_id.clone()),
                target_kind: Some(ExecutionTargetKind::ExperimentTreeBranch),
                target: Some(target.clone()),
                stream: "stdout".to_string(),
                text: chunk_payload.clone(),
            };
            Workspace::persist_execution_event_snapshot(
                &workspace.pool,
                &workspace.streaming_log_buffer,
                &event,
            )
            .await
            .expect("failed to persist stream event");
        }
        let total = start.elapsed();
        eprintln!(
            "persist_execution_event_snapshot streamed {STREAM_CHUNKS} chunks in {:.2}s ({:.2}ms/chunk)",
            total.as_secs_f64(),
            total.as_secs_f64() * 1000.0 / STREAM_CHUNKS as f64,
        );

        // Hard upper bound. On the current quadratic implementation this
        // typically blows past 10s on commodity hardware. The fix should
        // bring it well under 5s.
        assert!(
            total < std::time::Duration::from_secs(5),
            "stream-event persistence is too slow: {STREAM_CHUNKS} chunks took {:.2}s; \
             expected linear-time persistence to finish well under 5s",
            total.as_secs_f64(),
        );

        // Bonus assertion: while the bridge is hammering writes, a status()
        // read should still complete promptly. Run one final status query
        // and assert it lands within 250ms — this is the symptom most
        // visible to MCP clients.
        let status_start = std::time::Instant::now();
        let _status = workspace
            .status(&execution_id)
            .await
            .expect("status failed");
        let status_elapsed = status_start.elapsed();
        eprintln!(
            "status() after streaming: {:.2}ms",
            status_elapsed.as_secs_f64() * 1000.0,
        );
        assert!(
            status_elapsed < std::time::Duration::from_millis(250),
            "status() after streaming took {:.2}ms — should be fast even under streaming load",
            status_elapsed.as_secs_f64() * 1000.0,
        );

        workspace
            .shutdown()
            .await
            .expect("failed to shut down workspace");
    }

    /// Regression for the durability concern raised against the streaming
    /// buffer: chunks held only in memory must not be lost on a graceful
    /// `Workspace::shutdown()`. We simulate streaming output without ever
    /// emitting a non-stream event (worst case for the previous design),
    /// shut down, reopen the workspace pointing at the same on-disk SQLite
    /// file, and assert the persisted node_logs blob contains the chunk.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn streaming_buffer_chunks_persist_across_workspace_shutdown_and_reopen() {
        let tmp = TempDir::new().expect("failed to create temp dir");
        let store: Arc<dyn ArtifactStore> = Arc::new(NoopArtifactStore);
        let workspace = Workspace::open(tmp.path().to_path_buf(), store.clone(), 1)
            .await
            .expect("failed to open workspace");

        let tree_id = ExperimentTreeId::new("tree-shutdown-flush");
        let branch_id = BranchId::new("main");
        let cell_id = CellId::new("cell_only");
        let execution_id = ExecutionId::new("exec-shutdown-flush");
        let target = Workspace::execution_target_for_tree_branch(&tree_id, &branch_id);

        Workspace::insert_branch_execution_record(
            &workspace.pool,
            &execution_id,
            &tree_id,
            &branch_id,
            &target,
            &[cell_id.clone()],
        )
        .await
        .expect("failed to insert execution record");

        // Move the execution into Running so the persist path doesn't
        // short-circuit on finished_at.
        let started_event = ExecutionEvent::ExecutionStarted {
            execution_id: execution_id.clone(),
            tree_id: Some(tree_id.clone()),
            branch_id: Some(branch_id.clone()),
            target_kind: Some(ExecutionTargetKind::ExperimentTreeBranch),
            target: Some(target.clone()),
        };
        Workspace::persist_execution_event_snapshot(
            &workspace.pool,
            &workspace.streaming_log_buffer,
            &started_event,
        )
        .await
        .expect("failed to persist started event");
        let node_started_event = ExecutionEvent::NodeStarted {
            execution_id: execution_id.clone(),
            node_id: NodeId::new(cell_id.as_str()),
            tree_id: Some(tree_id.clone()),
            branch_id: Some(branch_id.clone()),
            target_kind: Some(ExecutionTargetKind::ExperimentTreeBranch),
            target: Some(target.clone()),
        };
        Workspace::persist_execution_event_snapshot(
            &workspace.pool,
            &workspace.streaming_log_buffer,
            &node_started_event,
        )
        .await
        .expect("failed to persist node started event");

        // Drive a stream chunk into the buffer ONLY. No subsequent non-stream
        // event arrives before shutdown — this is the worst case for the
        // in-memory buffer.
        const SENTINEL: &str = "STREAMED_CHUNK_THAT_MUST_SURVIVE_SHUTDOWN";
        let stream_event = ExecutionEvent::NodeStream {
            execution_id: execution_id.clone(),
            node_id: NodeId::new(cell_id.as_str()),
            tree_id: Some(tree_id.clone()),
            branch_id: Some(branch_id.clone()),
            target_kind: Some(ExecutionTargetKind::ExperimentTreeBranch),
            target: Some(target.clone()),
            stream: "stdout".to_string(),
            text: SENTINEL.to_string(),
        };
        Workspace::persist_execution_event_snapshot(
            &workspace.pool,
            &workspace.streaming_log_buffer,
            &stream_event,
        )
        .await
        .expect("failed to buffer stream event");

        // Graceful shutdown — must drain the streaming buffer before the
        // bridge task is aborted, otherwise the chunk is lost.
        workspace
            .shutdown()
            .await
            .expect("failed to shut down workspace");

        // Re-open the same on-disk workspace. The persisted node_logs blob
        // for this execution must include the streamed sentinel.
        let reopened = Workspace::open(tmp.path().to_path_buf(), store, 1)
            .await
            .expect("failed to reopen workspace");

        let row: Option<(Option<String>,)> =
            sqlx::query_as("SELECT node_logs FROM executions WHERE id = ?")
                .bind(execution_id.as_str())
                .fetch_optional(&reopened.pool)
                .await
                .expect("failed to read execution row");

        let logs_json = row
            .expect("execution row missing after reopen")
            .0
            .expect("node_logs column should be populated after shutdown drain");

        assert!(
            logs_json.contains(SENTINEL),
            "streamed chunk was lost across shutdown — node_logs after reopen: {logs_json}"
        );

        reopened
            .shutdown()
            .await
            .expect("failed to shut down reopened workspace");
    }

    /// Regression test for the persist/flush race: the periodic flush task
    /// and a non-stream event (e.g. NodeCompleted) both do
    /// read-modify-write on `executions.node_logs`. Without coordination,
    /// the slower writer can clobber the faster writer's update — losing
    /// either the streamed chunks or the terminal-event metadata.
    /// We force the race deterministically by running both ops concurrently
    /// over many iterations and asserting both contributions survive.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn flush_and_persist_must_not_lose_data_under_concurrent_access() {
        let tmp = TempDir::new().expect("failed to create temp dir");
        let store: Arc<dyn ArtifactStore> = Arc::new(NoopArtifactStore);
        let workspace = Workspace::open(tmp.path().to_path_buf(), store, 1)
            .await
            .expect("failed to open workspace");

        let tree_id = ExperimentTreeId::new("tree-race");
        let branch_id = BranchId::new("main");
        let cell_id = CellId::new("cell_race");
        let target = Workspace::execution_target_for_tree_branch(&tree_id, &branch_id);

        // Run the race many iterations to catch interleavings reliably.
        const ITERATIONS: usize = 30;
        for iteration in 0..ITERATIONS {
            let execution_id = ExecutionId::new(format!("exec-race-{iteration}"));
            Workspace::insert_branch_execution_record(
                &workspace.pool,
                &execution_id,
                &tree_id,
                &branch_id,
                &target,
                &[cell_id.clone()],
            )
            .await
            .expect("failed to insert execution record");

            // Bring the execution into Running so persist doesn't short-circuit.
            Workspace::persist_execution_event_snapshot(
                &workspace.pool,
                &workspace.streaming_log_buffer,
                &ExecutionEvent::ExecutionStarted {
                    execution_id: execution_id.clone(),
                    tree_id: Some(tree_id.clone()),
                    branch_id: Some(branch_id.clone()),
                    target_kind: Some(ExecutionTargetKind::ExperimentTreeBranch),
                    target: Some(target.clone()),
                },
            )
            .await
            .unwrap();
            Workspace::persist_execution_event_snapshot(
                &workspace.pool,
                &workspace.streaming_log_buffer,
                &ExecutionEvent::NodeStarted {
                    execution_id: execution_id.clone(),
                    node_id: NodeId::new(cell_id.as_str()),
                    tree_id: Some(tree_id.clone()),
                    branch_id: Some(branch_id.clone()),
                    target_kind: Some(ExecutionTargetKind::ExperimentTreeBranch),
                    target: Some(target.clone()),
                },
            )
            .await
            .unwrap();

            // Pre-buffer some streamed chunks the flush should drain.
            let sentinel = format!("STREAMED_CHUNK_{iteration}");
            Workspace::persist_execution_event_snapshot(
                &workspace.pool,
                &workspace.streaming_log_buffer,
                &ExecutionEvent::NodeStream {
                    execution_id: execution_id.clone(),
                    node_id: NodeId::new(cell_id.as_str()),
                    tree_id: Some(tree_id.clone()),
                    branch_id: Some(branch_id.clone()),
                    target_kind: Some(ExecutionTargetKind::ExperimentTreeBranch),
                    target: Some(target.clone()),
                    stream: "stdout".to_string(),
                    text: sentinel.clone(),
                },
            )
            .await
            .unwrap();

            // Race: NodeCompleted (sets duration_ms = 42) AND a periodic flush
            // happen concurrently against the same execution row.
            let pool = workspace.pool.clone();
            let buf = workspace.streaming_log_buffer.clone();
            let exec_a = execution_id.clone();
            let target_a = target.clone();
            let tree_a = tree_id.clone();
            let branch_a = branch_id.clone();
            let cell_a = cell_id.clone();
            let pool_b = workspace.pool.clone();
            let buf_b = workspace.streaming_log_buffer.clone();
            let exec_b = execution_id.clone();
            let locks_a = workspace.execution_persist_locks.clone();
            let locks_b = workspace.execution_persist_locks.clone();
            let (a, b) = tokio::join!(
                async move {
                    Workspace::persist_execution_event_snapshot_locked(
                        &pool,
                        &buf,
                        Some(&locks_a),
                        &ExecutionEvent::NodeCompleted {
                            execution_id: exec_a,
                            node_id: NodeId::new(cell_a.as_str()),
                            tree_id: Some(tree_a),
                            branch_id: Some(branch_a),
                            target_kind: Some(ExecutionTargetKind::ExperimentTreeBranch),
                            target: Some(target_a),
                            artifacts: HashMap::new(),
                            duration_ms: 42,
                        },
                    )
                    .await
                },
                async move {
                    Workspace::flush_streaming_buffer_for_execution_locked(
                        &pool_b,
                        &buf_b,
                        Some(&locks_b),
                        &exec_b,
                    )
                    .await
                },
            );
            a.expect("persist failed");
            b.expect("flush failed");

            // Final invariant: the persisted node_logs row must contain BOTH
            // the streamed sentinel AND the duration_ms set by NodeCompleted.
            let row: Option<(Option<String>,)> =
                sqlx::query_as("SELECT node_logs FROM executions WHERE id = ?")
                    .bind(execution_id.as_str())
                    .fetch_optional(&workspace.pool)
                    .await
                    .expect("failed to read execution row");
            let logs_json = row
                .expect("execution row missing")
                .0
                .expect("node_logs should be populated");
            let parsed: HashMap<NodeId, NodeLogs> =
                serde_json::from_str(&logs_json).expect("logs json parse");
            let node_logs = parsed
                .get(&NodeId::new(cell_id.as_str()))
                .expect("node logs entry");
            assert!(
                node_logs.stdout.contains(&sentinel),
                "iteration {iteration}: streamed chunk lost. node_logs.stdout = {:?}",
                node_logs.stdout,
            );
            assert_eq!(
                node_logs.duration_ms,
                Some(42),
                "iteration {iteration}: duration_ms from NodeCompleted lost. node_logs = {:?}",
                node_logs,
            );
        }

        workspace
            .shutdown()
            .await
            .expect("failed to shut down workspace");
    }

    /// Regression for the broadcast-queue drain concern: events sit in the
    /// bridge's `broadcast::Receiver` queue waiting to be processed. If
    /// `shutdown()` aborts the bridge task before draining that queue,
    /// any non-yet-processed events are silently lost — including
    /// terminal events like `NodeCompleted` that mutate persisted state.
    ///
    /// We send a burst of events directly into the broadcast channel (so
    /// the bridge has them queued but unprocessed), call `shutdown()`,
    /// reopen the workspace at the same path, and assert the final row
    /// reflects all the events.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_drains_pending_bridge_events_before_aborting() {
        let tmp = TempDir::new().expect("failed to create temp dir");
        let store: Arc<dyn ArtifactStore> = Arc::new(NoopArtifactStore);
        let workspace = Workspace::open(tmp.path().to_path_buf(), store.clone(), 1)
            .await
            .expect("failed to open workspace");

        let tree_id = ExperimentTreeId::new("tree-shutdown-drain");
        let branch_id = BranchId::new("main");
        let cell_id = CellId::new("cell_drain");
        let execution_id = ExecutionId::new("exec-shutdown-drain");
        let target = Workspace::execution_target_for_tree_branch(&tree_id, &branch_id);

        Workspace::insert_branch_execution_record(
            &workspace.pool,
            &execution_id,
            &tree_id,
            &branch_id,
            &target,
            &[cell_id.clone()],
        )
        .await
        .expect("failed to insert execution record");

        let event_tx = workspace.scheduler.event_sender();
        // Fire a burst of stream events through the broadcast channel so
        // the bridge has work queued. Then immediately follow with a
        // terminal NodeCompleted that should set duration_ms = 99. If
        // shutdown drains the queue, the terminal event lands in the DB.
        // If it aborts the bridge instead, the NodeCompleted may be lost.
        const SENTINEL: &str = "DRAIN_TEST_CHUNK";
        let _ = event_tx.send(ExecutionEvent::ExecutionStarted {
            execution_id: execution_id.clone(),
            tree_id: Some(tree_id.clone()),
            branch_id: Some(branch_id.clone()),
            target_kind: Some(ExecutionTargetKind::ExperimentTreeBranch),
            target: Some(target.clone()),
        });
        let _ = event_tx.send(ExecutionEvent::NodeStarted {
            execution_id: execution_id.clone(),
            node_id: NodeId::new(cell_id.as_str()),
            tree_id: Some(tree_id.clone()),
            branch_id: Some(branch_id.clone()),
            target_kind: Some(ExecutionTargetKind::ExperimentTreeBranch),
            target: Some(target.clone()),
        });
        for _ in 0..40 {
            let _ = event_tx.send(ExecutionEvent::NodeStream {
                execution_id: execution_id.clone(),
                node_id: NodeId::new(cell_id.as_str()),
                tree_id: Some(tree_id.clone()),
                branch_id: Some(branch_id.clone()),
                target_kind: Some(ExecutionTargetKind::ExperimentTreeBranch),
                target: Some(target.clone()),
                stream: "stdout".to_string(),
                text: SENTINEL.to_string(),
            });
        }
        let _ = event_tx.send(ExecutionEvent::NodeCompleted {
            execution_id: execution_id.clone(),
            node_id: NodeId::new(cell_id.as_str()),
            tree_id: Some(tree_id.clone()),
            branch_id: Some(branch_id.clone()),
            target_kind: Some(ExecutionTargetKind::ExperimentTreeBranch),
            target: Some(target.clone()),
            artifacts: HashMap::new(),
            duration_ms: 99,
        });

        // Immediate shutdown — much of the burst is still queued in the
        // bridge's broadcast::Receiver and has not been persisted yet.
        workspace
            .shutdown()
            .await
            .expect("failed to shut down workspace");

        // Reopen and assert the terminal event landed (duration_ms was
        // captured) and all the buffered stream chunks were persisted.
        let reopened = Workspace::open(tmp.path().to_path_buf(), store, 1)
            .await
            .expect("failed to reopen workspace");
        let row: Option<(Option<String>,)> =
            sqlx::query_as("SELECT node_logs FROM executions WHERE id = ?")
                .bind(execution_id.as_str())
                .fetch_optional(&reopened.pool)
                .await
                .expect("failed to read execution row");
        let logs_json = row
            .expect("execution row missing after reopen")
            .0
            .expect("node_logs column should be populated after shutdown drain");
        let parsed: HashMap<NodeId, NodeLogs> =
            serde_json::from_str(&logs_json).expect("logs json parse");
        let node_logs = parsed
            .get(&NodeId::new(cell_id.as_str()))
            .expect("node logs entry");

        assert!(
            node_logs.stdout.contains(SENTINEL),
            "shutdown dropped buffered stream chunks before they could be persisted; \
             node_logs.stdout = {:?}",
            node_logs.stdout,
        );
        assert_eq!(
            node_logs.duration_ms,
            Some(99),
            "shutdown dropped a queued NodeCompleted event before the bridge \
             could persist it; node_logs = {:?}",
            node_logs,
        );

        reopened
            .shutdown()
            .await
            .expect("failed to shut down reopened workspace");
    }

    /// Regression for the late-stream-chunk loss path: streamed stdout
    /// can arrive AFTER a terminal `NodeCompleted` event (e.g. the kernel
    /// emits one final stdout chunk between the executor sending
    /// `NodeCompleted` and the iopub stream actually draining). With a
    /// `WHERE finished_at IS NULL` guard on the flush UPDATE, those
    /// chunks would be silently dropped — the buffer entry is removed
    /// after the (no-op) UPDATE and there is no remaining persistence
    /// path. This is exactly the failure tail of stdout/stderr, which is
    /// usually the most valuable failure context.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn flush_persists_late_stream_chunks_after_terminal_event() {
        let tmp = TempDir::new().expect("failed to create temp dir");
        let store: Arc<dyn ArtifactStore> = Arc::new(NoopArtifactStore);
        let workspace = Workspace::open(tmp.path().to_path_buf(), store, 1)
            .await
            .expect("failed to open workspace");

        let tree_id = ExperimentTreeId::new("tree-late-tail");
        let branch_id = BranchId::new("main");
        let cell_id = CellId::new("cell_late");
        let execution_id = ExecutionId::new("exec-late-tail");
        let target = Workspace::execution_target_for_tree_branch(&tree_id, &branch_id);

        Workspace::insert_branch_execution_record(
            &workspace.pool,
            &execution_id,
            &tree_id,
            &branch_id,
            &target,
            &[cell_id.clone()],
        )
        .await
        .expect("failed to insert execution record");

        let started = ExecutionEvent::ExecutionStarted {
            execution_id: execution_id.clone(),
            tree_id: Some(tree_id.clone()),
            branch_id: Some(branch_id.clone()),
            target_kind: Some(ExecutionTargetKind::ExperimentTreeBranch),
            target: Some(target.clone()),
        };
        Workspace::persist_execution_event_snapshot_locked(
            &workspace.pool,
            &workspace.streaming_log_buffer,
            Some(&workspace.execution_persist_locks),
            &started,
        )
        .await
        .unwrap();

        let node_started = ExecutionEvent::NodeStarted {
            execution_id: execution_id.clone(),
            node_id: NodeId::new(cell_id.as_str()),
            tree_id: Some(tree_id.clone()),
            branch_id: Some(branch_id.clone()),
            target_kind: Some(ExecutionTargetKind::ExperimentTreeBranch),
            target: Some(target.clone()),
        };
        Workspace::persist_execution_event_snapshot_locked(
            &workspace.pool,
            &workspace.streaming_log_buffer,
            Some(&workspace.execution_persist_locks),
            &node_started,
        )
        .await
        .unwrap();

        // NodeCompleted finalizes the execution row: with one cell on the
        // branch, `execution_nodes_finished` returns true and the same
        // persist call sets `finished_at` and writes the row.
        let node_completed = ExecutionEvent::NodeCompleted {
            execution_id: execution_id.clone(),
            node_id: NodeId::new(cell_id.as_str()),
            tree_id: Some(tree_id.clone()),
            branch_id: Some(branch_id.clone()),
            target_kind: Some(ExecutionTargetKind::ExperimentTreeBranch),
            target: Some(target.clone()),
            artifacts: HashMap::new(),
            duration_ms: 50,
        };
        Workspace::persist_execution_event_snapshot_locked(
            &workspace.pool,
            &workspace.streaming_log_buffer,
            Some(&workspace.execution_persist_locks),
            &node_completed,
        )
        .await
        .unwrap();

        // Terminalize via the authoritative finalizer — event persistence
        // deliberately never sets `finished_at` (only the finalizer holds
        // the complete outcome, including rich outputs).
        let mut node_statuses = HashMap::new();
        node_statuses.insert(NodeId::new(cell_id.as_str()), NodeStatus::Completed);
        Workspace::finalize_branch_execution_success(
            &workspace.pool,
            &execution_id,
            &tree_id,
            &branch_id,
            &target,
            tine_core::ExecutionOutcome {
                execution_id: execution_id.clone(),
                tree_id: Some(tree_id.clone()),
                branch_id: Some(branch_id.clone()),
                target_kind: Some(ExecutionTargetKind::ExperimentTreeBranch),
                target: Some(target.clone()),
                node_logs: HashMap::new(),
                node_statuses,
                failed_nodes: Vec::new(),
                duration_ms: 50,
            },
        )
        .await;

        // Sanity: the row is now finished.
        let finished_row: (String, Option<String>) =
            sqlx::query_as("SELECT status, node_logs FROM executions WHERE id = ?")
                .bind(execution_id.as_str())
                .fetch_one(&workspace.pool)
                .await
                .unwrap();
        let finished_status: ExecutionStatus = serde_json::from_str(&finished_row.0).unwrap();
        assert!(
            finished_status.finished_at.is_some(),
            "test prerequisite: row should be marked finished by the finalizer",
        );

        // Now buffer a late stream chunk that arrived after the terminal
        // persistence and call the flush. Without the fix the UPDATE's
        // `WHERE finished_at IS NULL` clause matches 0 rows and the chunk
        // is silently lost.
        const SENTINEL: &str = "TAIL_AFTER_TERMINAL";
        Workspace::buffer_streaming_chunk(
            &workspace.streaming_log_buffer,
            &execution_id,
            &NodeId::new(cell_id.as_str()),
            "stdout",
            SENTINEL,
        );
        Workspace::flush_streaming_buffer_for_execution_locked(
            &workspace.pool,
            &workspace.streaming_log_buffer,
            Some(&workspace.execution_persist_locks),
            &execution_id,
        )
        .await
        .expect("late flush failed");

        // Re-read the row and assert the late chunk landed.
        let after_row: (Option<String>,) =
            sqlx::query_as("SELECT node_logs FROM executions WHERE id = ?")
                .bind(execution_id.as_str())
                .fetch_one(&workspace.pool)
                .await
                .unwrap();
        let logs_json = after_row.0.expect("node_logs missing");
        let parsed: HashMap<NodeId, NodeLogs> = serde_json::from_str(&logs_json).unwrap();
        let node_logs = parsed
            .get(&NodeId::new(cell_id.as_str()))
            .expect("node logs entry");
        assert!(
            node_logs.stdout.contains(SENTINEL),
            "late stream chunk after terminal event was lost; \
             node_logs.stdout = {:?}",
            node_logs.stdout,
        );

        workspace
            .shutdown()
            .await
            .expect("failed to shut down workspace");
    }

    /// Regression for the failure-mode log loss: if the periodic flush
    /// drains the in-memory buffer BEFORE the SQL UPDATE commits, a
    /// transient DB error (pool closed, lock contention, serialization
    /// failure) silently discards a full flush window of stdout/stderr.
    /// We force a write failure by closing the pool, run the flush, and
    /// assert the buffered chunks remain in memory so the next flush can
    /// retry persistence.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn flush_re_buffers_chunks_when_update_fails() {
        let tmp = TempDir::new().expect("failed to create temp dir");
        let store: Arc<dyn ArtifactStore> = Arc::new(NoopArtifactStore);
        let workspace = Workspace::open(tmp.path().to_path_buf(), store, 1)
            .await
            .expect("failed to open workspace");

        let execution_id = ExecutionId::new("exec-rebuffer");
        let node_id = NodeId::new("cell_rebuffer");
        const SENTINEL: &str = "RE_BUFFER_TAIL_MUST_SURVIVE";
        Workspace::buffer_streaming_chunk(
            &workspace.streaming_log_buffer,
            &execution_id,
            &node_id,
            "stdout",
            SENTINEL,
        );

        // Force the flush UPDATE to fail by closing the pool first.
        workspace.pool.close().await;

        let result = Workspace::flush_streaming_buffer_for_execution_locked(
            &workspace.pool,
            &workspace.streaming_log_buffer,
            Some(&workspace.execution_persist_locks),
            &execution_id,
        )
        .await;
        assert!(
            result.is_err(),
            "expected flush to fail against closed pool, got {:?}",
            result
        );

        // The buffered chunks must still be in memory so a subsequent
        // flush can retry. Without re-buffering, this entry would be
        // gone (the drain happens before the UPDATE commits).
        let entry = workspace.streaming_log_buffer.get(&execution_id);
        assert!(
            entry.is_some(),
            "buffer entry was removed after flush failure — chunks are now permanently lost"
        );
        let entry = entry.unwrap();
        let logs = entry
            .get(&node_id)
            .expect("node entry missing after re-buffer");
        assert!(
            logs.stdout.contains(SENTINEL),
            "buffered chunks were not re-inserted after flush failure; stdout = {:?}",
            logs.stdout
        );
    }

    /// Regression for the event-driven persistence failure mode: a
    /// non-stream event drains buffered stdout/stderr before it writes
    /// the updated execution row. If that final UPDATE fails, the chunks
    /// must be restored to memory so a later event or periodic flush can
    /// retry instead of permanently losing the current flush window.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn persist_re_buffers_stream_chunks_when_update_fails() {
        let tmp = TempDir::new().expect("failed to create temp dir");
        let store: Arc<dyn ArtifactStore> = Arc::new(NoopArtifactStore);
        let workspace = Workspace::open(tmp.path().to_path_buf(), store, 1)
            .await
            .expect("failed to open workspace");

        let tree_id = ExperimentTreeId::new("tree-persist-rebuffer");
        let branch_id = BranchId::new("main");
        let cell_id = CellId::new("cell_persist_rebuffer");
        let execution_id = ExecutionId::new("exec-persist-rebuffer");
        let node_id = NodeId::new(cell_id.as_str());
        let target = Workspace::execution_target_for_tree_branch(&tree_id, &branch_id);

        Workspace::insert_branch_execution_record(
            &workspace.pool,
            &execution_id,
            &tree_id,
            &branch_id,
            &target,
            &[cell_id.clone()],
        )
        .await
        .expect("failed to insert execution record");

        Workspace::persist_execution_event_snapshot_locked(
            &workspace.pool,
            &workspace.streaming_log_buffer,
            Some(&workspace.execution_persist_locks),
            &ExecutionEvent::ExecutionStarted {
                execution_id: execution_id.clone(),
                tree_id: Some(tree_id.clone()),
                branch_id: Some(branch_id.clone()),
                target_kind: Some(ExecutionTargetKind::ExperimentTreeBranch),
                target: Some(target.clone()),
            },
        )
        .await
        .expect("failed to persist start event");

        const SENTINEL: &str = "PERSIST_REBUFFER_MUST_SURVIVE";
        Workspace::buffer_streaming_chunk(
            &workspace.streaming_log_buffer,
            &execution_id,
            &node_id,
            "stdout",
            SENTINEL,
        );

        // A schema-level (non-TEMP) trigger: TEMP triggers are per-connection
        // and the pool may serve the next UPDATE from a different connection.
        sqlx::query(
            "CREATE TRIGGER fail_execution_update \
             BEFORE UPDATE ON executions \
             BEGIN SELECT RAISE(ABORT, 'synthetic execution update failure'); END",
        )
        .execute(&workspace.pool)
        .await
        .expect("failed to install failing trigger");

        let result = Workspace::persist_execution_event_snapshot_locked(
            &workspace.pool,
            &workspace.streaming_log_buffer,
            Some(&workspace.execution_persist_locks),
            &ExecutionEvent::NodeCompleted {
                execution_id: execution_id.clone(),
                node_id: node_id.clone(),
                tree_id: Some(tree_id),
                branch_id: Some(branch_id),
                target_kind: Some(ExecutionTargetKind::ExperimentTreeBranch),
                target: Some(target),
                artifacts: HashMap::new(),
                duration_ms: 7,
            },
        )
        .await;
        assert!(
            result.is_err(),
            "expected synthetic update failure, got {:?}",
            result
        );

        let entry = workspace.streaming_log_buffer.get(&execution_id);
        assert!(
            entry.is_some(),
            "persist failure removed buffered chunks; they cannot be retried"
        );
        let entry = entry.unwrap();
        let logs = entry
            .get(&node_id)
            .expect("node entry missing after persist re-buffer");
        assert!(
            logs.stdout.contains(SENTINEL),
            "persist failure did not re-buffer streamed stdout; stdout = {:?}",
            logs.stdout
        );
    }

    // -----------------------------------------------------------------------
    // Finalization atomicity
    // -----------------------------------------------------------------------

    async fn open_bare_workspace() -> (TempDir, Workspace) {
        let tmp = TempDir::new().expect("failed to create temp dir");
        let store: Arc<dyn ArtifactStore> = Arc::new(NoopArtifactStore);
        let workspace = Workspace::open(tmp.path().to_path_buf(), store, 1)
            .await
            .expect("failed to open workspace");
        (tmp, workspace)
    }

    async fn insert_queued_execution(
        workspace: &Workspace,
    ) -> (ExecutionId, ExperimentTreeId, BranchId, ExecutionTargetRef) {
        let execution_id = ExecutionId::generate();
        let tree_id = ExperimentTreeId::new("tree");
        let branch_id = BranchId::new("main");
        let target = Workspace::execution_target_for_tree_branch(&tree_id, &branch_id);
        Workspace::insert_branch_execution_record(
            &workspace.pool,
            &execution_id,
            &tree_id,
            &branch_id,
            &target,
            &[CellId::new("cell_1")],
        )
        .await
        .expect("failed to insert execution record");
        (execution_id, tree_id, branch_id, target)
    }

    fn completed_outcome(
        execution_id: &ExecutionId,
        tree_id: &ExperimentTreeId,
        branch_id: &BranchId,
        target: &ExecutionTargetRef,
    ) -> tine_core::ExecutionOutcome {
        let node_id = NodeId::new("cell_1");
        let mut node_statuses = HashMap::new();
        node_statuses.insert(node_id.clone(), NodeStatus::Completed);
        let mut node_logs = HashMap::new();
        node_logs.insert(
            node_id,
            NodeLogs {
                stdout: String::new(),
                stderr: String::new(),
                outputs: Vec::new(),
                error: None,
                duration_ms: Some(5),
                metrics: HashMap::from([("score".to_string(), 1.0)]),
            },
        );
        tine_core::ExecutionOutcome {
            execution_id: execution_id.clone(),
            tree_id: Some(tree_id.clone()),
            branch_id: Some(branch_id.clone()),
            target_kind: Some(ExecutionTargetKind::ExperimentTreeBranch),
            target: Some(target.clone()),
            node_logs,
            node_statuses,
            failed_nodes: Vec::new(),
            duration_ms: 5,
        }
    }

    async fn metrics_row_count(workspace: &Workspace, execution_id: &ExecutionId) -> i64 {
        sqlx::query_scalar("SELECT COUNT(*) FROM metrics WHERE execution_id = ?")
            .bind(execution_id.as_str())
            .fetch_one(&workspace.pool)
            .await
            .expect("failed to count metrics")
    }

    #[tokio::test]
    async fn finalize_failure_after_success_keeps_terminal_status() {
        let (_tmp, workspace) = open_bare_workspace().await;
        let (execution_id, tree_id, branch_id, target) = insert_queued_execution(&workspace).await;

        Workspace::finalize_branch_execution_success(
            &workspace.pool,
            &execution_id,
            &tree_id,
            &branch_id,
            &target,
            completed_outcome(&execution_id, &tree_id, &branch_id, &target),
        )
        .await;
        Workspace::finalize_branch_execution_failure(
            &workspace.pool,
            &execution_id,
            &tree_id,
            &branch_id,
            &target,
        )
        .await;

        let status = WorkspaceApi::status(&workspace, &execution_id)
            .await
            .expect("failed to load status");
        assert_eq!(status.status, ExecutionLifecycleStatus::Completed);
        assert!(status.finished_at.is_some());
        assert_eq!(metrics_row_count(&workspace, &execution_id).await, 1);
    }

    #[tokio::test]
    async fn double_success_finalize_writes_metrics_once() {
        let (_tmp, workspace) = open_bare_workspace().await;
        let (execution_id, tree_id, branch_id, target) = insert_queued_execution(&workspace).await;
        let outcome = completed_outcome(&execution_id, &tree_id, &branch_id, &target);

        Workspace::finalize_branch_execution_success(
            &workspace.pool,
            &execution_id,
            &tree_id,
            &branch_id,
            &target,
            outcome.clone(),
        )
        .await;
        Workspace::finalize_branch_execution_success(
            &workspace.pool,
            &execution_id,
            &tree_id,
            &branch_id,
            &target,
            outcome,
        )
        .await;

        assert_eq!(metrics_row_count(&workspace, &execution_id).await, 1);
    }

    #[tokio::test]
    async fn cancel_finalize_does_not_overwrite_completed_record() {
        let (_tmp, workspace) = open_bare_workspace().await;
        let (execution_id, tree_id, branch_id, target) = insert_queued_execution(&workspace).await;

        Workspace::finalize_branch_execution_success(
            &workspace.pool,
            &execution_id,
            &tree_id,
            &branch_id,
            &target,
            completed_outcome(&execution_id, &tree_id, &branch_id, &target),
        )
        .await;
        Workspace::finalize_cancelled_execution(&workspace.pool, &execution_id, Utc::now())
            .await
            .expect("cancel finalize should be a no-op, not an error");

        let status = WorkspaceApi::status(&workspace, &execution_id)
            .await
            .expect("failed to load status");
        assert_eq!(status.status, ExecutionLifecycleStatus::Completed);
    }

    #[tokio::test]
    async fn concurrent_success_and_cancel_finalize_have_single_winner() {
        let (_tmp, workspace) = open_bare_workspace().await;
        let (execution_id, tree_id, branch_id, target) = insert_queued_execution(&workspace).await;

        let success = Workspace::finalize_branch_execution_success(
            &workspace.pool,
            &execution_id,
            &tree_id,
            &branch_id,
            &target,
            completed_outcome(&execution_id, &tree_id, &branch_id, &target),
        );
        let cancel =
            Workspace::finalize_cancelled_execution(&workspace.pool, &execution_id, Utc::now());
        let (_, cancel_result) = tokio::join!(success, cancel);
        let _ = cancel_result;

        let status = WorkspaceApi::status(&workspace, &execution_id)
            .await
            .expect("failed to load status");
        assert!(status.finished_at.is_some());
        match status.status {
            ExecutionLifecycleStatus::Completed => {
                assert_eq!(metrics_row_count(&workspace, &execution_id).await, 1);
            }
            ExecutionLifecycleStatus::Cancelled => {
                assert_eq!(
                    metrics_row_count(&workspace, &execution_id).await,
                    0,
                    "losing success finalizer must not write metrics"
                );
            }
            other => panic!("expected Completed or Cancelled, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn phase_update_cannot_resurrect_terminal_record() {
        let (_tmp, workspace) = open_bare_workspace().await;
        let (execution_id, tree_id, branch_id, target) = insert_queued_execution(&workspace).await;

        Workspace::finalize_branch_execution_failure(
            &workspace.pool,
            &execution_id,
            &tree_id,
            &branch_id,
            &target,
        )
        .await;
        Workspace::update_execution_status_record(&workspace.pool, &execution_id, |status| {
            Workspace::apply_execution_phase(status, ExecutionPhase::Running);
        })
        .await
        .expect("phase update should silently no-op on terminal records");

        let status = WorkspaceApi::status(&workspace, &execution_id)
            .await
            .expect("failed to load status");
        assert_eq!(status.status, ExecutionLifecycleStatus::Failed);
        assert!(status.finished_at.is_some());
    }

    /// Regression for the rich-output loss race: the event bridge used to
    /// terminalize the row itself on ExecutionCompleted/NodeCompleted, beating
    /// the authoritative finalizer — whose subsequent write (the only one
    /// carrying `outputs`) then backed off as the "loser" of the atomic
    /// finalize guard. Event persistence must never terminalize, and event
    /// writes landing after finalize must preserve the finalized outputs.
    #[tokio::test]
    async fn event_persistence_never_terminalizes_and_preserves_finalized_outputs() {
        let (_tmp, workspace) = open_bare_workspace().await;
        let (execution_id, tree_id, branch_id, target) = insert_queued_execution(&workspace).await;
        let node_id = NodeId::new("cell_1");

        let node_completed = ExecutionEvent::NodeCompleted {
            execution_id: execution_id.clone(),
            node_id: node_id.clone(),
            tree_id: Some(tree_id.clone()),
            branch_id: Some(branch_id.clone()),
            target_kind: Some(ExecutionTargetKind::ExperimentTreeBranch),
            target: Some(target.clone()),
            artifacts: HashMap::new(),
            duration_ms: 5,
        };
        let execution_completed = ExecutionEvent::ExecutionCompleted {
            execution_id: execution_id.clone(),
            tree_id: Some(tree_id.clone()),
            branch_id: Some(branch_id.clone()),
            target_kind: Some(ExecutionTargetKind::ExperimentTreeBranch),
            target: Some(target.clone()),
            duration_ms: 5,
        };

        // Events arriving BEFORE the finalizer must not terminalize the row.
        for event in [&node_completed, &execution_completed] {
            Workspace::persist_execution_event_snapshot_locked(
                &workspace.pool,
                &workspace.streaming_log_buffer,
                Some(&workspace.execution_persist_locks),
                event,
            )
            .await
            .unwrap();
        }
        let status = WorkspaceApi::status(&workspace, &execution_id)
            .await
            .unwrap();
        assert!(
            status.finished_at.is_none(),
            "event persistence must leave terminal transitions to the finalizer"
        );

        // The finalizer lands the outcome with rich outputs.
        let mut outcome = completed_outcome(&execution_id, &tree_id, &branch_id, &target);
        outcome
            .node_logs
            .get_mut(&node_id)
            .unwrap()
            .outputs
            .push(tine_core::NodeOutput {
                data: HashMap::from([("text/plain".to_string(), "2".to_string())]),
                metadata: HashMap::new(),
            });
        Workspace::finalize_branch_execution_success(
            &workspace.pool,
            &execution_id,
            &tree_id,
            &branch_id,
            &target,
            outcome,
        )
        .await;

        // Late event writes (the bridge is always slightly behind) must not
        // clobber the finalized outputs.
        for event in [&node_completed, &execution_completed] {
            Workspace::persist_execution_event_snapshot_locked(
                &workspace.pool,
                &workspace.streaming_log_buffer,
                Some(&workspace.execution_persist_locks),
                event,
            )
            .await
            .unwrap();
        }

        let (results_status, node_logs) = workspace
            .execution_results(&execution_id)
            .await
            .expect("results should load");
        assert_eq!(results_status.status, ExecutionLifecycleStatus::Completed);
        let logs = node_logs.get(&node_id).expect("node logs present");
        assert!(
            logs.outputs
                .iter()
                .any(|output| output.data.get("text/plain") == Some(&"2".to_string())),
            "finalized rich outputs were clobbered by late event persistence: {:?}",
            logs.outputs
        );
    }

    #[tokio::test]
    async fn prepare_context_failure_marks_tree_needs_replay() {
        let (tmp, workspace) = open_bare_workspace().await;
        let tree = test_tree();
        let tree_id = workspace
            .save_experiment_tree(&tree)
            .await
            .expect("failed to save tree")
            .id;

        // Force ensure_tree_environment to fail deterministically (no kernel
        // or network needed): the venv path exists as a regular file, so venv
        // recreation cannot proceed.
        let venv_path = tmp.path().join(".tine").join("venv");
        std::fs::create_dir_all(venv_path.parent().unwrap()).unwrap();
        std::fs::write(&venv_path, b"not a directory").unwrap();

        let result = workspace
            .prepare_context(&tree_id, &BranchId::new("main"), &CellId::new("a"))
            .await;
        assert!(result.is_err(), "prepare_context should fail");

        let state = workspace
            .get_tree_runtime_state(&tree_id)
            .await
            .expect("runtime state should exist after failed prepare");
        assert_eq!(
            state.kernel_state,
            TreeKernelState::NeedsReplay,
            "failed prepare must downgrade Switching to NeedsReplay"
        );
        assert!(state.materialized_path_cell_ids.is_empty());
    }

    #[tokio::test]
    async fn restart_resets_needs_replay_to_runnable_ready() {
        // Reproduces the reported bug: after a shutdown leaves the tree in
        // `NeedsReplay`, restarting the kernel must reset the persisted runtime
        // state so the UI no longer treats it as `needs_replay`-with-live-kernel
        // (which hard-disables "Run Branch"). We test the state-reset helper
        // directly so no real kernel is spun.
        let (_tmp, workspace) = open_bare_workspace().await;
        let tree = test_tree();
        let tree_id = workspace
            .save_experiment_tree(&tree)
            .await
            .expect("failed to save tree")
            .id;
        let branch_id = BranchId::new("main");

        // Seed a post-shutdown state: NeedsReplay, with a stale prepared marker
        // and materialized path, exactly as `mark_tree_needs_replay`/prior runs
        // would leave behind.
        let mut seeded = Workspace::default_tree_runtime_state(&tree_id, &branch_id);
        seeded.kernel_state = TreeKernelState::NeedsReplay;
        seeded.materialized_path_cell_ids = vec![CellId::new("a")];
        seeded.last_prepared_cell_id = Some(CellId::new("a"));
        seeded.runtime_epoch = 5;
        workspace
            .set_tree_runtime_state(seeded)
            .await
            .expect("seed runtime state");

        workspace
            .reset_runtime_state_after_kernel_restart(&tree_id)
            .await
            .expect("reset should succeed");

        let state = workspace
            .get_tree_runtime_state(&tree_id)
            .await
            .expect("runtime state should exist after restart");

        // Ready + empty materialized path: the UI's `branchRequiresReplay`
        // treats this as runnable (not needs_replay-with-live-kernel), so
        // "Run Branch" is enabled again.
        assert_eq!(state.kernel_state, TreeKernelState::Ready);
        assert!(
            state.materialized_path_cell_ids.is_empty(),
            "restart must clear the materialized path so the next run replays from scratch"
        );
        assert_eq!(
            state.last_prepared_cell_id, None,
            "restart must clear the stale prepared-branch marker"
        );
        assert!(
            state.runtime_epoch > 5,
            "restart must bump the runtime epoch to invalidate the prior kernel's bookkeeping"
        );
    }

    #[tokio::test]
    async fn restart_with_no_runtime_state_is_noop() {
        // KernelNotFound / no-prior-state must not fabricate state.
        let (_tmp, workspace) = open_bare_workspace().await;
        let tree = test_tree();
        let tree_id = workspace
            .save_experiment_tree(&tree)
            .await
            .expect("failed to save tree")
            .id;

        workspace
            .reset_runtime_state_after_kernel_restart(&tree_id)
            .await
            .expect("reset should succeed");

        assert!(
            workspace.get_tree_runtime_state(&tree_id).await.is_none(),
            "reset must not create runtime state when none exists"
        );
    }
}
