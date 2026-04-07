use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use sqlx::sqlite::SqlitePool;
use tokio::sync::{Mutex, Notify, RwLock};
use tracing::{error, info, warn};

use crate::branch_projection::{branch_lineage, plan_branch_transition, BranchProjection};
use tine_catalog::DataCatalog;
use tine_core::{
    ArtifactKey, ArtifactStore, BranchDef, BranchId, BranchIsolationMode,
    BranchTargetInspection, CellDef, CellId, CellRuntimeState, ExecutableTreeBranch,
    ExecutableTreeCell, ExecutionAccepted, ExecutionEvent, ExecutionId,
    ExecutionLifecycleStatus, ExecutionPhase, ExecutionStatus, ExecutionTargetKind,
    ExecutionTargetRef, ExperimentTreeDef, ExperimentTreeId, IsolationResult, NodeCacheKey,
    NodeCode, NodeError, NodeId, NodeLogs, NodeStatus, PreparedContext, ProjectDef, ProjectId,
    SlotName, TineError, TineResult, TreeKernelState, TreeRuntimeState, WorkspaceApi,
};
use tine_env::{EnvironmentManager, TreeEnvironmentDescriptor};
use tine_kernel::{KernelIsolationOutcome, KernelLifecycleEvent, KernelManager};
use tine_scheduler::Scheduler;

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

#[derive(Debug, Clone)]
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
    max_concurrent_executions: usize,
    max_queue_depth: usize,
    kernel_monitor_handle: tokio::task::JoinHandle<()>,
    kernel_lifecycle_handle: tokio::task::JoinHandle<()>,
    execution_event_bridge_handle: tokio::task::JoinHandle<()>,
}

impl Drop for Workspace {
    fn drop(&mut self) {
        self.kernel_monitor_handle.abort();
        self.kernel_lifecycle_handle.abort();
        self.execution_event_bridge_handle.abort();
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
        Self::update_execution_status_record(&self.pool, execution_id, |status| {
            status.queue_position = None;
            status.finished_at = Some(Utc::now());
            Self::apply_execution_phase(status, ExecutionPhase::Rejected);
        })
        .await
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
                } else if queue.pending.iter().any(|queued_id| queued_id == execution_id) {
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
            let Some(position) = queue.pending.iter().position(|queued_id| queued_id == execution_id)
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
            TineError::Internal(format!(
                "branch '{}' not found in tree '{}'",
                branch_id, tree.id
            ))
        })?;
        loop {
            lineage.push(current);
            match &current.parent_branch_id {
                Some(parent_id) => {
                    current = branch_by_id.get(parent_id).copied().ok_or_else(|| {
                        TineError::Internal(format!(
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
                            TineError::Internal(format!(
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
                TineError::Internal(format!(
                    "branch '{}' not found in tree '{}'",
                    branch_id, tree.id
                ))
            })?;
        if !branch.cell_order.iter().any(|existing| existing == cell_id) {
            return Err(TineError::Internal(format!(
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
                TineError::Internal(format!(
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
            cache: cell.cache,
            map_over: cell.map_over.clone(),
            map_concurrency: cell.map_concurrency,
            timeout_secs: cell.timeout_secs,
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
        let initial_status = ExecutionStatus {
            execution_id: execution_id.clone(),
            tree_id: Some(tree_id.clone()),
            branch_id: Some(branch_id.clone()),
            target_kind: Some(ExecutionTargetKind::ExperimentTreeBranch),
            target: Some(target.clone()),
            status: ExecutionLifecycleStatus::Queued,
            phase: ExecutionPhase::Queued,
            queue_position: None,
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
            "INSERT INTO executions (id, tree_id, branch_id, target_kind, status, started_at) VALUES (?, ?, ?, ?, ?, datetime('now'))",
        )
        .bind(execution_id.as_str())
        .bind(tree_id.as_str())
        .bind(branch_id.as_str())
        .bind("experiment_tree_branch")
        .bind(&status_json)
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
        let existing: Option<(String,)> = sqlx::query_as("SELECT status FROM executions WHERE id = ?")
            .bind(execution_id.as_str())
            .fetch_optional(pool)
            .await
            .ok()
            .flatten();
        if let Some((status_json,)) = existing {
            if let Ok(status) = serde_json::from_str::<ExecutionStatus>(&status_json) {
                if Self::normalize_execution_status(status).finished_at.is_some() {
                    return;
                }
            }
        }

        let terminal_status =
            Self::terminal_status_from_outcome(&outcome.node_statuses, &outcome.node_logs);
        let status = ExecutionStatus {
            execution_id: execution_id.clone(),
            tree_id: Some(tree_id.clone()),
            branch_id: Some(branch_id.clone()),
            target_kind: Some(ExecutionTargetKind::ExperimentTreeBranch),
            target: Some(target.clone()),
            status: terminal_status.clone(),
            phase: Self::terminal_phase_from_status(&terminal_status),
            queue_position: None,
            cancellation_requested_at: None,
            node_statuses: outcome.node_statuses,
            started_at: Utc::now() - chrono::Duration::milliseconds(outcome.duration_ms as i64),
            finished_at: Some(Utc::now()),
        };
        let status_json = serde_json::to_string(&status).unwrap_or_default();
        let logs_json = serde_json::to_string(&outcome.node_logs).unwrap_or_default();

        let _ = sqlx::query(
            "UPDATE executions SET status = ?, node_logs = ?, finished_at = datetime('now') WHERE id = ?",
        )
        .bind(&status_json)
        .bind(&logs_json)
        .bind(execution_id.as_str())
        .execute(pool)
        .await;

        for (node_id, logs) in &outcome.node_logs {
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
        let existing: Option<(String,)> = sqlx::query_as("SELECT status FROM executions WHERE id = ?")
            .bind(execution_id.as_str())
            .fetch_optional(pool)
            .await
            .ok()
            .flatten();
        if let Some((status_json,)) = existing {
            if let Ok(status) = serde_json::from_str::<ExecutionStatus>(&status_json) {
                if Self::normalize_execution_status(status).finished_at.is_some() {
                    return;
                }
            }
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
            cancellation_requested_at: None,
            node_statuses: HashMap::new(),
            started_at: Utc::now(),
            finished_at: Some(Utc::now()),
        };
        let status_json = serde_json::to_string(&status).unwrap_or_default();
        let _ = sqlx::query(
            "UPDATE executions SET status = ?, finished_at = datetime('now') WHERE id = ?",
        )
        .bind(&status_json)
        .bind(execution_id.as_str())
        .execute(pool)
        .await;
    }

    fn node_logs_indicate_timeout(node_logs: &HashMap<NodeId, NodeLogs>) -> bool {
        node_logs.values().any(|logs| {
            logs.error
                .as_ref()
                .map(|error| error.ename == "ExecutionTimedOut")
                .unwrap_or(false)
        })
    }

    fn terminal_status_from_outcome(
        node_statuses: &HashMap<NodeId, NodeStatus>,
        node_logs: &HashMap<NodeId, NodeLogs>,
    ) -> ExecutionLifecycleStatus {
        if Self::node_logs_indicate_timeout(node_logs) {
            ExecutionLifecycleStatus::TimedOut
        } else if node_statuses
            .values()
            .any(|node_status| matches!(node_status, NodeStatus::Failed))
        {
            ExecutionLifecycleStatus::Failed
        } else if !node_statuses.is_empty()
            && node_statuses.values().all(|node_status| matches!(node_status, NodeStatus::Interrupted))
        {
            ExecutionLifecycleStatus::Cancelled
        } else {
            ExecutionLifecycleStatus::Completed
        }
    }

    fn terminal_status_from_nodes(node_statuses: &HashMap<NodeId, NodeStatus>) -> ExecutionLifecycleStatus {
        if node_statuses
            .values()
            .any(|node_status| matches!(node_status, NodeStatus::Failed))
        {
            ExecutionLifecycleStatus::Failed
        } else if !node_statuses.is_empty()
            && node_statuses.values().all(|node_status| matches!(node_status, NodeStatus::Interrupted))
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
            return status;
        }

        if matches!(status.phase, ExecutionPhase::Completed | ExecutionPhase::Failed | ExecutionPhase::Cancelled | ExecutionPhase::TimedOut | ExecutionPhase::Rejected) {
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
            && status.node_statuses.values().all(|node_status| {
                matches!(node_status, NodeStatus::Pending | NodeStatus::Queued)
            })
        {
            status.status = ExecutionLifecycleStatus::Queued;
            status.phase = ExecutionPhase::Queued;
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

        sqlx::query("UPDATE executions SET status = ?, finished_at = datetime('now') WHERE id = ?")
            .bind(&status_json)
            .bind(execution_id.as_str())
            .execute(pool)
            .await
            .map_err(|e| TineError::Database(e.to_string()))?;
        Ok(())
    }

    async fn update_execution_status_record<F>(
        pool: &SqlitePool,
        execution_id: &ExecutionId,
        update: F,
    ) -> TineResult<()>
    where
        F: FnOnce(&mut ExecutionStatus),
    {
        let row: Option<(String,)> = sqlx::query_as("SELECT status FROM executions WHERE id = ?")
            .bind(execution_id.as_str())
            .fetch_optional(pool)
            .await
            .map_err(|e| TineError::Database(e.to_string()))?;
        let Some((status_json,)) = row else {
            return Ok(());
        };

        let mut status = Self::normalize_execution_status(serde_json::from_str(&status_json)?);
        if status.finished_at.is_some() {
            return Ok(());
        }
        update(&mut status);
        let updated_status_json = serde_json::to_string(&status).map_err(TineError::Serialization)?;
        sqlx::query(
            "UPDATE executions SET status = ?, finished_at = COALESCE(finished_at, ?) WHERE id = ?",
        )
        .bind(&updated_status_json)
        .bind(status.finished_at.map(|timestamp| timestamp.to_rfc3339()))
        .bind(execution_id.as_str())
        .execute(pool)
        .await
        .map_err(|e| TineError::Database(e.to_string()))?;
        Ok(())
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
            if matches!(node_status, NodeStatus::Pending | NodeStatus::Queued | NodeStatus::Running)
            {
                *node_status = NodeStatus::Interrupted;
                let logs = node_logs.entry(node_id.clone()).or_insert_with(|| NodeLogs {
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

    fn execution_nodes_finished(status: &ExecutionStatus) -> bool {
        !status.node_statuses.is_empty()
            && status.node_statuses.values().all(|node_status| {
                matches!(
                    node_status,
                    NodeStatus::Completed
                        | NodeStatus::Failed
                        | NodeStatus::CacheHit
                        | NodeStatus::Skipped
                        | NodeStatus::Interrupted
                )
            })
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
                "UPDATE executions SET status = ?, finished_at = datetime('now') WHERE id = ?",
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
                "UPDATE executions SET status = ?, node_logs = ?, finished_at = datetime('now') WHERE id = ?",
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
    ) -> bool {
        eprintln!(
            "[workspace] branch execution start execution={} tree={} branch={} cwd={}",
            execution_id.as_str(),
            tree_id.as_str(),
            branch_id.as_str(),
            working_dir.display()
        );
        let execution_result = scheduler
            .execute_executable_branch_for_target(
                &execution_id,
                &executable_branch,
                &target,
                &cache,
                Some(&pool),
                Some(&working_dir),
            )
            .await;
        match execution_result {
            Ok(outcome) => {
                eprintln!(
                    "[workspace] branch execution success execution={} tree={} branch={}",
                    execution_id.as_str(),
                    tree_id.as_str(),
                    branch_id.as_str()
                );
                Self::finalize_branch_execution_success(
                    &pool,
                    &execution_id,
                    &tree_id,
                    &branch_id,
                    &target,
                    outcome,
                )
                .await;
                true
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

        Err(TineError::Internal(format!(
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

    async fn persist_execution_event_snapshot(
        pool: &SqlitePool,
        event: &ExecutionEvent,
    ) -> TineResult<()> {
        let execution_id = match event {
            ExecutionEvent::ExecutionStarted { execution_id, .. }
            | ExecutionEvent::NodeStarted { execution_id, .. }
            | ExecutionEvent::NodeStream { execution_id, .. }
            | ExecutionEvent::NodeDisplayData { execution_id, .. }
            | ExecutionEvent::NodeDisplayUpdate { execution_id, .. }
            | ExecutionEvent::ExecutionCompleted { execution_id, .. }
            | ExecutionEvent::ExecutionFailed { execution_id, .. }
            | ExecutionEvent::NodeCompleted { execution_id, .. }
            | ExecutionEvent::NodeCacheHit { execution_id, .. }
            | ExecutionEvent::NodeFailed { execution_id, .. } => execution_id,
            _ => return Ok(()),
        };

        let row: Option<(String, Option<String>)> =
            sqlx::query_as("SELECT status, node_logs FROM executions WHERE id = ?")
                .bind(execution_id.as_str())
                .fetch_optional(pool)
                .await
                .map_err(|e| TineError::Database(e.to_string()))?;
        let Some((status_json, node_logs_json)) = row else {
            return Ok(());
        };

        let mut status: ExecutionStatus =
            Self::normalize_execution_status(serde_json::from_str(&status_json)?);
        if status.finished_at.is_some() {
            return Ok(());
        }
        let mut node_logs: HashMap<NodeId, NodeLogs> = node_logs_json
            .as_deref()
            .and_then(|json| serde_json::from_str(json).ok())
            .unwrap_or_default();

        match event {
            ExecutionEvent::ExecutionStarted { .. } => {
                Self::apply_execution_phase(&mut status, ExecutionPhase::Running);
            }
            ExecutionEvent::NodeStarted { node_id, .. } => {
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
                Self::apply_execution_phase(&mut status, ExecutionPhase::Running);
                status
                    .node_statuses
                    .insert(node_id.clone(), NodeStatus::Completed);
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
                Self::apply_execution_phase(&mut status, ExecutionPhase::Running);
                status
                    .node_statuses
                    .insert(node_id.clone(), NodeStatus::CacheHit);
            }
            ExecutionEvent::NodeFailed { node_id, error, .. } => {
                Self::apply_execution_phase(&mut status, ExecutionPhase::Running);
                status
                    .node_statuses
                    .insert(node_id.clone(), NodeStatus::Failed);
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
            ExecutionEvent::ExecutionCompleted { .. } => {
                status.finished_at = Some(Utc::now());
                Self::apply_execution_phase(&mut status, ExecutionPhase::Completed);
            }
            ExecutionEvent::ExecutionFailed { .. } => {
                status.finished_at = Some(Utc::now());
                let terminal_status =
                    Self::terminal_status_from_outcome(&status.node_statuses, &node_logs);
                status.status = terminal_status.clone();
                status.phase = Self::terminal_phase_from_status(&terminal_status);
            }
            _ => {}
        }

        if status.finished_at.is_none() && Self::execution_nodes_finished(&status) {
            status.finished_at = Some(Utc::now());
            let terminal_status =
                Self::terminal_status_from_outcome(&status.node_statuses, &node_logs);
            status.status = terminal_status.clone();
            status.phase = Self::terminal_phase_from_status(&terminal_status);
        }

        let updated_status_json = serde_json::to_string(&status).unwrap_or_default();
        let updated_logs_json = serde_json::to_string(&node_logs).unwrap_or_default();
        sqlx::query(
            "UPDATE executions \
             SET status = ?, node_logs = ?, finished_at = COALESCE(finished_at, ?) \
             WHERE id = ? AND finished_at IS NULL",
        )
        .bind(&updated_status_json)
        .bind(&updated_logs_json)
        .bind(status.finished_at.map(|timestamp| timestamp.to_rfc3339()))
        .bind(execution_id.as_str())
        .execute(pool)
        .await
        .map_err(|e| TineError::Database(e.to_string()))?;

        Ok(())
    }

    fn spawn_execution_event_bridge(
        pool: SqlitePool,
        mut event_rx: tokio::sync::broadcast::Receiver<ExecutionEvent>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                match event_rx.recv().await {
                    Ok(event) => {
                        if let Err(err) =
                            Self::persist_execution_event_snapshot(&pool, &event).await
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
        })
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
                timeout_secs: None,
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
            return Err(TineError::Internal(format!(
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
            return Err(TineError::Internal(format!(
                "parent branch '{}' not found in tree '{}'",
                parent_branch_id, tree_id
            )));
        }
        if !tree
            .cells
            .iter()
            .any(|cell| &cell.id == branch_point_cell_id)
        {
            return Err(TineError::Internal(format!(
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
                TineError::Internal(format!(
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
        let tree = workspace.get_experiment_tree(tree_id).await?;
        Self::validate_branch_membership(&tree, branch_id, cell_id)?;
        let working_dir = workspace.file_base_for_project(tree.project_id.as_ref()).await?;
        let plan = Self::build_tree_cell_execution_plan(&tree, branch_id, cell_id)?;
        let execution_id = ExecutionId::generate();
        let created_at = Utc::now();
        let topo_order = vec![cell_id.clone()];

        Self::insert_branch_execution_record(
            &workspace.pool,
            &execution_id,
            tree_id,
            branch_id,
            &plan.target,
            &topo_order,
        )
        .await?;
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
            .await {
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
                    if let Err(err) = workspace_for_task.set_tree_runtime_state(runtime_state).await
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
        if self.get_tree_runtime_state(tree_id).await.is_some() {
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
                TineError::Internal(format!(
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
                TineError::Internal(format!(
                    "branch '{}' not found in tree '{}'",
                    branch_id, tree_id
                ))
            })?;
        let idx = branch
            .cell_order
            .iter()
            .position(|existing| existing == cell_id)
            .ok_or_else(|| {
                TineError::Internal(format!(
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
                TineError::Internal(format!(
                    "branch '{}' not found in tree '{}'",
                    branch_id, tree_id
                ))
            })?;
        let before = branch.cell_order.len();
        branch.cell_order.retain(|existing| existing != cell_id);
        if branch.cell_order.len() == before {
            return Err(TineError::Internal(format!(
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
            return Err(TineError::Internal(format!(
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

    pub async fn execute_cell_in_experiment_tree(
        &self,
        tree_id: &ExperimentTreeId,
        cell_id: &CellId,
    ) -> TineResult<(ExecutionId, NodeLogs)> {
        let tree = self.get_experiment_tree(tree_id).await?;
        let branch = tree
            .branches
            .iter()
            .find(|branch| branch.cell_order.iter().any(|existing| existing == cell_id))
            .ok_or_else(|| {
                TineError::Internal(format!(
                    "cell '{}' is not assigned to any branch in tree '{}'",
                    cell_id, tree_id
                ))
            })?;
        let target = Self::execution_target_for_tree_branch(tree_id, &branch.id);
        let (execution_id, logs) = self
            .execute_tree_cell_for_target(&tree, &branch.id, cell_id)
            .await?;
        self.persist_single_node_execution(
            &NodeId::new(cell_id.as_str()),
            &execution_id,
            &logs,
            Some(tree_id),
            Some(&branch.id),
            ExecutionTargetKind::ExperimentTreeBranch,
            target,
        )
        .await?;
        Ok((execution_id, logs))
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
        let tree = self.get_experiment_tree(tree_id).await?;
        let working_dir = self.file_base_for_project(tree.project_id.as_ref()).await?;
        let plan = Self::build_tree_branch_execution_plan(&tree, branch_id)?;
        let exec_id = ExecutionId::generate();
        Self::insert_branch_execution_record(
            &self.pool,
            &exec_id,
            tree_id,
            branch_id,
            &plan.target,
            &plan.executable_branch.topo_order,
        )
        .await?;
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
            .await {
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
            )
            .await;
            Self::release_execution_slot_with(
                &queue_state_for_task,
                &queue_notify_for_task,
                &eid,
            )
            .await;
        });

        Ok(exec_id)
    }

    pub async fn execute_all_branches_in_experiment_tree(
        &self,
        tree_id: &ExperimentTreeId,
    ) -> TineResult<Vec<(BranchId, ExecutionId)>> {
        let tree = self.get_experiment_tree(tree_id).await?;
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
        let isolation_mode = self
            .get_tree_runtime_state(tree_id)
            .await
            .map(|state| state.isolation_mode)
            .unwrap_or_default();
        tokio::spawn(async move {
            match isolation_mode {
                BranchIsolationMode::Disabled => {
                    let mut runs_iter = runs.into_iter();
                    while let Some((branch_id, exec_id, executable_branch, target)) =
                        runs_iter.next()
                    {
                        let can_run = match Self::wait_for_execution_slot_with(
                            &pool,
                            &execution_queue_state,
                            &execution_queue_notify,
                            max_concurrent_executions,
                            &exec_id,
                        )
                        .await {
                            Ok(can_run) => can_run,
                            Err(err) => {
                                error!(tree = %tid, branch = %branch_id, execution = %exec_id, error = %err, "failed while waiting for queued execute-all slot");
                                Self::finalize_branch_execution_failure(
                                    &pool,
                                    &exec_id,
                                    &tid,
                                    &branch_id,
                                    &target,
                                )
                                .await;
                                continue;
                            }
                        };
                        if !can_run {
                            continue;
                        }
                        if kernel_mgr.has_tree_kernel(&tid) {
                            if let Err(e) = kernel_mgr.shutdown_tree(&tid).await {
                                error!(tree = %tid, branch = %branch_id, execution = %exec_id, error = %e, "failed to reset tree kernel before branch execution");
                            }
                        }
                        let succeeded = Self::run_branch_execution(
                            scheduler.clone(),
                            pool.clone(),
                            tid.clone(),
                            branch_id.clone(),
                            exec_id.clone(),
                            executable_branch,
                            target,
                            HashMap::new(),
                            working_dir_for_task.clone(),
                        )
                        .await;
                        Self::release_execution_slot_with(
                            &execution_queue_state,
                            &execution_queue_notify,
                            &exec_id,
                        )
                        .await;
                        if !succeeded {
                            warn!(tree = %tid, branch = %branch_id, execution = %exec_id, "stopping execute-all after first branch failure");
                            for (remaining_branch_id, remaining_exec_id, _, remaining_target) in
                                runs_iter
                            {
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
                }
                BranchIsolationMode::NamespaceGuarded => {
                    let mut handles = Vec::with_capacity(runs.len());
                    for (branch_id, exec_id, executable_branch, target) in runs {
                        let scheduler = scheduler.clone();
                        let kernel_mgr = kernel_mgr.clone();
                        let env_mgr = env_mgr.clone();
                        let pool = pool.clone();
                        let execution_queue_state = execution_queue_state.clone();
                        let execution_queue_notify = execution_queue_notify.clone();
                        let tree_runtime_states = tree_runtime_states.clone();
                        let tid = tid.clone();
                        let event_tx = event_tx.clone();
                        let working_dir_for_task = working_dir_for_task.clone();
                        handles.push(tokio::spawn(async move {
                            let can_run = match Self::wait_for_execution_slot_with(
                                &pool,
                                &execution_queue_state,
                                &execution_queue_notify,
                                max_concurrent_executions,
                                &exec_id,
                            )
                            .await {
                                Ok(can_run) => can_run,
                                Err(err) => {
                                    error!(tree = %tid, branch = %branch_id, execution = %exec_id, error = %err, "failed while waiting for queued guarded execute-all slot");
                                    Self::finalize_branch_execution_failure(
                                        &pool,
                                        &exec_id,
                                        &tid,
                                        &branch_id,
                                        &target,
                                    )
                                    .await;
                                    return false;
                                }
                            };
                            if !can_run {
                                return false;
                            }
                            let restart_after_teardown = async {
                                let mut restart_after_teardown = false;
                                let branch_id_for_isolation = branch_id.clone();
                                let session_id = exec_id.as_str().to_string();
                                let _ = event_tx.send(ExecutionEvent::IsolationAttempted {
                                    tree_id: tid.clone(),
                                    branch_id: branch_id.clone(),
                                });
                                let should_restart = if !kernel_mgr.has_tree_kernel(&tid) {
                                    let tree_env = TreeEnvironmentDescriptor::new(
                                        tid.clone(),
                                        executable_branch.project_id.clone(),
                                        executable_branch.environment.clone(),
                                    );
                                    match env_mgr.ensure_tree_environment(&tree_env).await {
                                        Ok(venv_dir) => {
                                            if let Err(err) = kernel_mgr
                                                .start_tree_kernel(
                                                    &tid,
                                                    &venv_dir,
                                                    &working_dir_for_task,
                                                )
                                                .await
                                            {
                                                let _ = event_tx.send(
                                                    ExecutionEvent::FallbackRestartTriggered {
                                                        tree_id: tid.clone(),
                                                        branch_id: branch_id.clone(),
                                                        reason: format!(
                                                            "failed_to_start_guarded_kernel:{}",
                                                            err
                                                        ),
                                                    },
                                                );
                                                true
                                            } else {
                                                false
                                            }
                                        }
                                        Err(err) => {
                                            let _ = event_tx.send(
                                                ExecutionEvent::FallbackRestartTriggered {
                                                    tree_id: tid.clone(),
                                                    branch_id: branch_id.clone(),
                                                    reason: format!(
                                                        "failed_to_prepare_guarded_environment:{}",
                                                        err
                                                    ),
                                                },
                                            );
                                            true
                                        }
                                    }
                                } else {
                                    false
                                };

                                let mut used_namespace_guard = false;
                                if !should_restart {
                                    match kernel_mgr
                                        .begin_tree_branch_session(&tid, &session_id)
                                        .await
                                    {
                                        Ok(()) => {
                                            used_namespace_guard = true;
                                            tokio::task::yield_now().await;
                                        }
                                        Err(err) => {
                                            let _ = event_tx.send(
                                                ExecutionEvent::FallbackRestartTriggered {
                                                    tree_id: tid.clone(),
                                                    branch_id: branch_id.clone(),
                                                    reason: format!(
                                                        "failed_to_begin_branch_session:{}",
                                                        err
                                                    ),
                                                },
                                            );
                                            if kernel_mgr.has_tree_kernel(&tid) {
                                                let _ = kernel_mgr.shutdown_tree(&tid).await;
                                            }
                                        }
                                    }
                                }

                                Self::run_branch_execution(
                                    scheduler,
                                    pool.clone(),
                                    tid.clone(),
                                    branch_id.clone(),
                                    exec_id.clone(),
                                    executable_branch,
                                    target,
                                    HashMap::new(),
                                    working_dir_for_task,
                                )
                                .await;

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
                                                let _ = event_tx.send(
                                                    ExecutionEvent::ContaminationDetected {
                                                        tree_id: tid.clone(),
                                                        branch_id: branch_id_for_isolation.clone(),
                                                        signals: outcome.signals.clone(),
                                                    },
                                                );
                                                let _ = event_tx.send(
                                                    ExecutionEvent::FallbackRestartTriggered {
                                                        tree_id: tid.clone(),
                                                        branch_id: branch_id_for_isolation.clone(),
                                                        reason: "contamination_detected".to_string(),
                                                    },
                                                );
                                                restart_after_teardown = true;
                                            } else {
                                                let _ = event_tx.send(ExecutionEvent::IsolationSucceeded {
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
                                            let _ = event_tx.send(
                                                ExecutionEvent::FallbackRestartTriggered {
                                                    tree_id: tid.clone(),
                                                    branch_id: branch_id_for_isolation.clone(),
                                                    reason: format!(
                                                        "failed_to_end_branch_session:{}",
                                                        err
                                                    ),
                                                },
                                            );
                                            restart_after_teardown = true;
                                        }
                                    }
                                }

                                restart_after_teardown
                            }
                            .await;
                            Self::release_execution_slot_with(
                                &execution_queue_state,
                                &execution_queue_notify,
                                &exec_id,
                            )
                            .await;
                            restart_after_teardown
                        }));
                    }
                    let mut restart_tree_after_guarded_run = false;
                    for handle in handles {
                        if let Ok(restart_requested) = handle.await {
                            restart_tree_after_guarded_run |= restart_requested;
                        }
                    }
                    if restart_tree_after_guarded_run && kernel_mgr.has_tree_kernel(&tid) {
                        let _ = kernel_mgr.shutdown_tree(&tid).await;
                    }
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
            Err(err) => return Err(err),
        }
        match self.scheduler.shutdown_tree_kernel(tree_id).await {
            Ok(()) => {}
            Err(TineError::KernelNotFound { .. }) => {}
            Err(err) => return Err(err),
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
        for replay_cell_id in &replay_prefix {
            self.execute_tree_cell_for_target(&tree, branch_id, replay_cell_id)
                .await?;
        }

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
        let pool = SqlitePool::connect(&db_url)
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
        let execution_event_bridge_handle =
            Self::spawn_execution_event_bridge(pool.clone(), scheduler.subscribe());
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
            max_concurrent_executions,
            max_queue_depth,
            kernel_monitor_handle,
            kernel_lifecycle_handle,
            execution_event_bridge_handle,
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

    /// Graceful shutdown: persist state, stop kernels.
    pub async fn shutdown(&self) -> TineResult<()> {
        info!("shutting down workspace");
        self.kernel_monitor_handle.abort();
        self.kernel_lifecycle_handle.abort();
        self.execution_event_bridge_handle.abort();
        self.kernel_mgr.shutdown_all().await?;
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
        let rows: Vec<(String, String, String, String)> =
            sqlx::query_as("SELECT code_hash, input_hashes, lockfile_hash, artifacts FROM cache")
                .fetch_all(pool)
                .await
                .map_err(|e| TineError::Database(e.to_string()))?;

        let mut cache = HashMap::new();
        for (code_hash_hex, input_hashes_json, lockfile_hash_hex, artifacts_json) in rows {
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
        Workspace::delete_cell_from_experiment_tree_branch(self, tree_id, branch_id, cell_id)
            .await
    }

    async fn delete_experiment_tree_branch(
        &self,
        tree_id: &ExperimentTreeId,
        branch_id: &BranchId,
    ) -> TineResult<()> {
        Workspace::delete_experiment_tree_branch(self, tree_id, branch_id).await
    }

    async fn get_tree_runtime_state(
        &self,
        tree_id: &ExperimentTreeId,
    ) -> Option<TreeRuntimeState> {
        Workspace::get_tree_runtime_state(self, tree_id).await
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

        Self::update_execution_status_record(&self.pool, execution_id, |status| {
            status.cancellation_requested_at = Some(cancellation_requested_at);
            status.queue_position = None;
            Self::apply_execution_phase(status, ExecutionPhase::CancellationRequested);
        })
        .await?;

        let tree_id = status.tree_id.clone().ok_or_else(|| {
            TineError::Internal(format!("execution '{}' is missing tree_id", execution_id))
        })?;
        self.kernel_mgr.interrupt_tree(&tree_id).await?;

        let pool = self.pool.clone();
        let execution_id = execution_id.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            if let Err(err) = Self::finalize_cancelled_execution(
                &pool,
                &execution_id,
                cancellation_requested_at,
            )
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
        Ok(Self::normalize_execution_status(status))
    }

    async fn logs_for_tree_cell(
        &self,
        tree_id: &ExperimentTreeId,
        branch_id: &BranchId,
        cell_id: &CellId,
    ) -> TineResult<NodeLogs> {
        let rows: Vec<(Option<String>,)> = sqlx::query_as(
            "SELECT node_logs FROM executions \
             WHERE tree_id = ? AND branch_id = ? AND target_kind = 'experiment_tree_branch' \
             ORDER BY rowid DESC LIMIT 50",
        )
        .bind(tree_id.as_str())
        .bind(branch_id.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| TineError::Database(e.to_string()))?;

        for (maybe_logs_json,) in &rows {
            if let Some(logs_json) = maybe_logs_json {
                let all_logs: HashMap<String, NodeLogs> =
                    serde_json::from_str(logs_json).unwrap_or_default();
                if let Some(logs) = all_logs.get(cell_id.as_str()) {
                    return Ok(logs.clone());
                }
            }
        }

        // Fallback: search any execution for this tree by cell id
        let fallback_rows: Vec<(Option<String>,)> = sqlx::query_as(
            "SELECT node_logs FROM executions WHERE tree_id = ? ORDER BY rowid DESC LIMIT 50",
        )
        .bind(tree_id.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| TineError::Database(e.to_string()))?;

        for (maybe_logs_json,) in fallback_rows {
            if let Some(logs_json) = maybe_logs_json {
                let all_logs: HashMap<String, NodeLogs> =
                    serde_json::from_str(&logs_json).unwrap_or_default();
                if let Some(logs) = all_logs.get(cell_id.as_str()) {
                    return Ok(logs.clone());
                }
            }
        }

        Ok(NodeLogs {
            stdout: String::new(),
            stderr: String::new(),
            outputs: Vec::new(),
            error: None,
            duration_ms: None,
            metrics: HashMap::new(),
        })
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
                    timeout_secs: None,
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
                    timeout_secs: None,
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
                    timeout_secs: None,
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
                    timeout_secs: None,
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
                    timeout_secs: None,
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
                    timeout_secs: None,
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

        workspace.shutdown().await.expect("failed to shut down workspace");
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

        workspace.shutdown().await.expect("failed to shut down workspace");
    }
}
