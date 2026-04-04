use async_trait::async_trait;
use std::collections::HashMap;

use crate::error::TineResult;
use crate::types::*;

// ---------------------------------------------------------------------------
// WorkspaceApi — the central trait all surfaces call
// ---------------------------------------------------------------------------

#[async_trait]
pub trait WorkspaceApi: Send + Sync {
    // -- Experiment tree management --

    /// Get an experiment tree definition.
    async fn get_experiment_tree(&self, id: &ExperimentTreeId) -> TineResult<ExperimentTreeDef>;

    /// List all experiment trees.
    async fn list_experiment_trees(&self) -> TineResult<Vec<ExperimentTreeDef>>;

    /// Create a new experiment tree with a default first cell.
    async fn create_experiment_tree(
        &self,
        name: &str,
        project_id: Option<&ProjectId>,
    ) -> TineResult<ExperimentTreeDef>;

    /// Delete an experiment tree by ID.
    async fn delete_experiment_tree(&self, tree_id: &ExperimentTreeId) -> TineResult<()>;

    /// Rename an experiment tree.
    async fn rename_experiment_tree(
        &self,
        tree_id: &ExperimentTreeId,
        name: &str,
    ) -> TineResult<()>;

    /// Persist an experiment tree definition and return the canonical saved tree.
    async fn save_experiment_tree(&self, def: &ExperimentTreeDef) -> TineResult<ExperimentTreeDef>;

    /// Inspect a branch target without mutating runtime state.
    async fn inspect_branch_target(
        &self,
        tree_id: &ExperimentTreeId,
        branch_id: &BranchId,
        cell_id: &CellId,
    ) -> TineResult<BranchTargetInspection>;

    /// Create a new branch in an experiment tree.
    async fn create_branch_in_experiment_tree(
        &self,
        tree_id: &ExperimentTreeId,
        parent_branch_id: &BranchId,
        branch_name: String,
        branch_point_cell_id: &CellId,
        first_cell: CellDef,
    ) -> TineResult<BranchId>;

    /// Add a cell to a branch in an experiment tree.
    async fn add_cell_to_experiment_tree_branch(
        &self,
        tree_id: &ExperimentTreeId,
        branch_id: &BranchId,
        cell: CellDef,
        after_cell_id: Option<&CellId>,
    ) -> TineResult<()>;

    /// Update source code for a specific cell in a branch.
    async fn update_cell_code_in_experiment_tree_branch(
        &self,
        tree_id: &ExperimentTreeId,
        branch_id: &BranchId,
        cell_id: &CellId,
        code: &str,
    ) -> TineResult<()>;

    /// Move a cell up or down within a branch.
    async fn move_cell_in_experiment_tree_branch(
        &self,
        tree_id: &ExperimentTreeId,
        branch_id: &BranchId,
        cell_id: &CellId,
        direction: &str,
    ) -> TineResult<()>;

    /// Delete a cell from a branch.
    async fn delete_cell_from_experiment_tree_branch(
        &self,
        tree_id: &ExperimentTreeId,
        branch_id: &BranchId,
        cell_id: &CellId,
    ) -> TineResult<()>;

    /// Delete a non-root branch and its subtree.
    async fn delete_experiment_tree_branch(
        &self,
        tree_id: &ExperimentTreeId,
        branch_id: &BranchId,
    ) -> TineResult<()>;

    /// Get current runtime state for a tree, if any exists.
    async fn get_tree_runtime_state(
        &self,
        tree_id: &ExperimentTreeId,
    ) -> Option<TreeRuntimeState>;

    /// Execute a single cell within a branch context.
    async fn execute_cell_in_experiment_tree_branch(
        &self,
        tree_id: &ExperimentTreeId,
        branch_id: &BranchId,
        cell_id: &CellId,
    ) -> TineResult<(ExecutionId, NodeLogs)>;

    /// Execute a branch.
    async fn execute_branch_in_experiment_tree(
        &self,
        tree_id: &ExperimentTreeId,
        branch_id: &BranchId,
    ) -> TineResult<ExecutionId>;

    /// Execute all branches in a tree.
    async fn execute_all_branches_in_experiment_tree(
        &self,
        tree_id: &ExperimentTreeId,
    ) -> TineResult<Vec<(BranchId, ExecutionId)>>;

    // -- Execution --

    /// Cancel a running execution.
    async fn cancel(&self, execution_id: &ExecutionId) -> TineResult<()>;

    /// Get execution status.
    async fn status(&self, execution_id: &ExecutionId) -> TineResult<ExecutionStatus>;

    /// Get logs for a specific cell in a tree branch runtime context.
    async fn logs_for_tree_cell(
        &self,
        tree_id: &ExperimentTreeId,
        branch_id: &BranchId,
        cell_id: &CellId,
    ) -> TineResult<NodeLogs>;

    // -- Projects --

    /// Create a new project.
    async fn create_project(&self, project: ProjectDef) -> TineResult<ProjectId>;

    /// List all projects.
    async fn list_projects(&self) -> TineResult<Vec<ProjectDef>>;

    /// Get a project by ID.
    async fn get_project(&self, id: &ProjectId) -> TineResult<ProjectDef>;

    /// List experiment trees within a project.
    async fn list_experiments(&self, project_id: &ProjectId) -> TineResult<Vec<ExperimentTreeDef>>;
}

// ---------------------------------------------------------------------------
// ArtifactStore — pluggable storage backend
// ---------------------------------------------------------------------------

#[async_trait]
pub trait ArtifactStore: Send + Sync {
    /// Store artifact bytes, returning the content hash.
    async fn put(&self, key: &ArtifactKey, data: &[u8]) -> TineResult<[u8; 32]>;

    /// Retrieve artifact bytes.
    async fn get(&self, key: &ArtifactKey) -> TineResult<Vec<u8>>;

    /// Delete an artifact.
    async fn delete(&self, key: &ArtifactKey) -> TineResult<()>;

    /// Check if an artifact exists.
    async fn exists(&self, key: &ArtifactKey) -> TineResult<bool>;

    /// Get artifact metadata (size, etc.).
    async fn metadata(&self, key: &ArtifactKey) -> TineResult<ArtifactMetadata>;

    /// List all artifact keys.
    async fn list(&self) -> TineResult<Vec<ArtifactKey>>;
}

// ---------------------------------------------------------------------------
// ExperimentRegistry — pluggable experiment tracking
// ---------------------------------------------------------------------------

#[async_trait]
pub trait ExperimentRegistry: Send + Sync {
    /// Log metrics for a node execution.
    async fn log_metrics(
        &self,
        execution_id: &ExecutionId,
        node_id: &NodeId,
        metrics: HashMap<String, f64>,
    ) -> TineResult<()>;

    /// Log parameters for a node execution.
    async fn log_params(
        &self,
        execution_id: &ExecutionId,
        node_id: &NodeId,
        params: HashMap<String, String>,
    ) -> TineResult<()>;

    /// Log an artifact reference.
    async fn log_artifact(
        &self,
        execution_id: &ExecutionId,
        node_id: &NodeId,
        key: &ArtifactKey,
        metadata: &ArtifactMetadata,
    ) -> TineResult<()>;

    /// Query metrics across executions for comparison.
    async fn query_metrics(
        &self,
        execution_ids: &[ExecutionId],
        metric_names: &[String],
    ) -> TineResult<HashMap<ExecutionId, HashMap<String, f64>>>;
}

// ---------------------------------------------------------------------------
// ExecutionEventSink — receives streaming execution events
// ---------------------------------------------------------------------------

#[async_trait]
pub trait ExecutionEventSink: Send + Sync {
    /// Handle an execution event (for streaming to clients).
    async fn send_event(&self, event: ExecutionEvent) -> TineResult<()>;
}
