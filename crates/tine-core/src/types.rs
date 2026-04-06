use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Identity types — all newtype wrappers for type safety
// ---------------------------------------------------------------------------

macro_rules! id_type {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        pub struct $name(pub String);

        impl $name {
            pub fn new(s: impl Into<String>) -> Self {
                Self(s.into())
            }

            pub fn generate() -> Self {
                Self(Uuid::new_v4().to_string())
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        impl From<String> for $name {
            fn from(s: String) -> Self {
                Self(s)
            }
        }

        impl From<&str> for $name {
            fn from(s: &str) -> Self {
                Self(s.to_string())
            }
        }
    };
}

id_type!(WorkspaceId);
id_type!(ProjectId);
id_type!(ExperimentTreeId);
id_type!(BranchId);
id_type!(CellId);
id_type!(NodeId);
id_type!(ExecutionId);
id_type!(ArtifactKey);
id_type!(RevisionId);
id_type!(SlotName);

// ---------------------------------------------------------------------------
// Migration glossary
// ---------------------------------------------------------------------------
//
// Preferred product/runtime vocabulary during the experiment-tree migration:
// - Experiment tree: kernel-owning notebook object (`ExperimentTreeDef`)
// - Branch: logical path inside one experiment tree (`BranchDef`)
// - Cell: authored unit inside one experiment tree (`CellDef`)
//
// Compatibility vocabulary that still exists in non-runtime graph/storage paths:
// - Node: legacy graph unit (`NodeDef`)
//
// New architecture work should prefer tree/branch/cell terminology unless it
// is explicitly touching compatibility flows. The canonical execution-ready
// tree-native shapes are `ExecutableTreeBranch` and `ExecutableTreeCell`.
//
// ---------------------------------------------------------------------------
// Experiment-tree definitions
// ---------------------------------------------------------------------------

/// A single authored cell in an experiment tree.
///
/// This is the target long-term model for notebook-native branching:
/// one experiment tree owns one kernel, and branches are logical paths
/// through cells inside that tree.
///
/// Execution-ready tree-native planning is represented separately by
/// `ExecutableTreeBranch` and `ExecutableTreeCell`. `CellDef` remains the
/// authored and persisted cell model inside a tree.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CellDef {
    /// Stable cell identifier within the experiment tree.
    pub id: CellId,
    /// Owning experiment tree.
    pub tree_id: ExperimentTreeId,
    /// Logical branch this cell belongs to.
    pub branch_id: BranchId,
    /// Human-readable name.
    pub name: String,
    /// The code to execute.
    pub code: NodeCode,
    /// Explicit upstream cell dependencies for DAG execution.
    #[serde(default)]
    pub upstream_cell_ids: Vec<CellId>,
    /// Named output slots.
    #[serde(default)]
    pub declared_outputs: Vec<SlotName>,
    /// Whether to cache this cell's results.
    #[serde(default)]
    pub cache: bool,
    /// If set, this cell runs in parallel across the named input collection.
    #[serde(default)]
    pub map_over: Option<SlotName>,
    /// Max concurrent workers for map execution.
    #[serde(default)]
    pub map_concurrency: Option<usize>,
    /// Per-cell execution timeout in seconds.
    #[serde(default)]
    pub timeout_secs: Option<u64>,
    /// Optional tags for filtering / labeling.
    #[serde(default)]
    pub tags: HashMap<String, String>,
    /// Optional revision/version metadata for invalidation tracking.
    #[serde(default)]
    pub revision_id: Option<RevisionId>,
    /// Tree-native runtime state for invalidation/recovery.
    #[serde(default)]
    pub state: CellRuntimeState,
}

/// Runtime state for a cell in an experiment tree.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum CellRuntimeState {
    #[default]
    Clean,
    Stale,
    Running,
    Failed,
    KernelLost,
    NeedsReplay,
}

/// A logical branch path inside an experiment tree.
///
/// Branches are not separate kernels. They provide ordered notebook display
/// and branch-point metadata inside one tree-owned runtime.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BranchDef {
    /// Stable branch identifier.
    pub id: BranchId,
    /// Human-readable branch name.
    pub name: String,
    /// Parent branch, if any.
    #[serde(default)]
    pub parent_branch_id: Option<BranchId>,
    /// Cell where this branch diverged from its parent.
    #[serde(default)]
    pub branch_point_cell_id: Option<CellId>,
    /// Explicit notebook display order for cells on this branch.
    #[serde(default)]
    pub cell_order: Vec<CellId>,
    /// Optional UI/display metadata.
    #[serde(default)]
    pub display: HashMap<String, String>,
}

/// Tree-owned notebook/runtime definition.
///
/// Long-term target model:
/// - one experiment tree = one kernel
/// - one experiment tree = one environment
/// - branches are in-tree paths, not forked pipelines
/// - cells are the authored/runtime-facing unit
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExperimentTreeDef {
    /// Stable tree identifier.
    #[serde(default = "ExperimentTreeId::generate")]
    pub id: ExperimentTreeId,
    /// Human-readable name.
    pub name: String,
    /// Optional project this tree belongs to.
    #[serde(default)]
    pub project_id: Option<ProjectId>,
    /// Root branch for the tree.
    pub root_branch_id: BranchId,
    /// All branch definitions in the tree.
    #[serde(default)]
    pub branches: Vec<BranchDef>,
    /// All cell definitions in the tree.
    #[serde(default)]
    pub cells: Vec<CellDef>,
    /// Environment specification for this tree.
    #[serde(default)]
    pub environment: EnvironmentSpec,
    /// Execution mode for the tree.
    #[serde(default)]
    pub execution_mode: ExecutionMode,
    /// Budget constraints.
    #[serde(default)]
    pub budget: Option<WorkspaceBudget>,
    /// Creation timestamp.
    #[serde(default = "chrono::Utc::now")]
    pub created_at: DateTime<Utc>,
}

// ---------------------------------------------------------------------------
// Node definitions
// ---------------------------------------------------------------------------

/// The code content of a node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeCode {
    /// The source code to execute.
    pub source: String,
    /// Language (currently only "python").
    pub language: String,
}

/// Tree-native runtime input reference for an executable cell.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutableTreeInput {
    /// Upstream executable cell that provides this input.
    pub source_cell_id: CellId,
    /// Output slot consumed from the upstream cell.
    pub source_output: SlotName,
}

/// A single execution-ready cell on a branch path inside an experiment tree.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutableTreeCell {
    /// Owning experiment tree.
    pub tree_id: ExperimentTreeId,
    /// Branch path this executable cell belongs to.
    pub branch_id: BranchId,
    /// Source/authored cell identifier.
    pub cell_id: CellId,
    /// Human-readable name.
    pub name: String,
    /// Code to execute.
    pub code: NodeCode,
    /// Resolved inputs keyed by local slot name.
    #[serde(default)]
    pub inputs: HashMap<SlotName, ExecutableTreeInput>,
    /// Declared output slots.
    #[serde(default)]
    pub outputs: Vec<SlotName>,
    /// Whether execution may reuse cached results.
    #[serde(default)]
    pub cache: bool,
    /// Map-style execution source slot, if any.
    #[serde(default)]
    pub map_over: Option<SlotName>,
    /// Max concurrent workers for map execution.
    #[serde(default)]
    pub map_concurrency: Option<usize>,
    /// Per-cell timeout in seconds.
    #[serde(default)]
    pub timeout_secs: Option<u64>,
    /// Optional tags for filtering / labeling.
    #[serde(default)]
    pub tags: HashMap<String, String>,
    /// Optional revision/version metadata for invalidation tracking.
    #[serde(default)]
    pub revision_id: Option<RevisionId>,
}

/// Canonical execution-ready branch path inside a single experiment tree.
///
/// This is the canonical execution-ready branch path used during tree-native
/// execution planning.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutableTreeBranch {
    /// Owning experiment tree.
    pub tree_id: ExperimentTreeId,
    /// Branch this executable path materializes.
    pub branch_id: BranchId,
    /// Human-readable display name.
    pub name: String,
    /// Ordered lineage from root branch to target branch.
    #[serde(default)]
    pub lineage: Vec<BranchId>,
    /// Ordered cell path for materialization/replay.
    #[serde(default)]
    pub path_cell_order: Vec<CellId>,
    /// Topologically sorted cells for execution.
    #[serde(default)]
    pub topo_order: Vec<CellId>,
    /// Execution-ready cells on this branch path.
    #[serde(default)]
    pub cells: Vec<ExecutableTreeCell>,
    /// Environment specification for the owning tree.
    #[serde(default)]
    pub environment: EnvironmentSpec,
    /// Execution mode for this branch path.
    #[serde(default)]
    pub execution_mode: ExecutionMode,
    /// Budget constraints inherited from the tree.
    #[serde(default)]
    pub budget: Option<WorkspaceBudget>,
    /// Optional project this tree belongs to.
    #[serde(default)]
    pub project_id: Option<ProjectId>,
    /// Creation timestamp inherited from the tree.
    pub created_at: DateTime<Utc>,
}

/// A single computation node in a legacy graph model.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeDef {
    /// Unique node identifier within the pipeline.
    pub id: NodeId,
    /// Human-readable name.
    pub name: String,
    /// The code to execute.
    pub code: NodeCode,
    /// Named input slots: slot_name -> (source_node_id, source_slot_name).
    #[serde(default)]
    pub inputs: HashMap<SlotName, (NodeId, SlotName)>,
    /// Named output slots.
    #[serde(default)]
    pub outputs: Vec<SlotName>,
    /// Whether to cache this node's results.
    #[serde(default)]
    pub cache: bool,
    /// If set, this node runs in parallel across the named input collection.
    #[serde(default)]
    pub map_over: Option<SlotName>,
    /// Max concurrent workers for map execution.
    #[serde(default)]
    pub map_concurrency: Option<usize>,
    /// Per-node execution timeout in seconds.  Overrides the global 7200s default.
    #[serde(default)]
    pub timeout_secs: Option<u64>,
    /// Optional tags for filtering / labeling.
    #[serde(default)]
    pub tags: HashMap<String, String>,
}

// ---------------------------------------------------------------------------
// Environment specification
// ---------------------------------------------------------------------------

/// Optional Python package requirements for a pipeline.
///
/// Runtime defaults are provided by `uv` during kernel initialization. The only
/// user-configurable part we persist is the extra dependency list.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnvironmentSpec {
    /// PyPI dependencies.
    #[serde(default)]
    pub dependencies: Vec<String>,
}

impl Default for EnvironmentSpec {
    fn default() -> Self {
        Self {
            dependencies: Vec::new(),
        }
    }
}

/// Execution mode controls how a tree runtime shares resources with other
/// concurrent executions.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionMode {
    /// Can run alongside other tree executions (default).
    #[default]
    Parallel,
    /// Acquires an exclusive lock — no other tree execution runs concurrently.
    Sequential,
}

// ---------------------------------------------------------------------------
// Execution status and events
// ---------------------------------------------------------------------------

/// Status of a single node execution.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NodeStatus {
    Pending,
    Queued,
    Running,
    Completed,
    Failed,
    Skipped,
    Interrupted,
    CacheHit,
}

/// Error information from a failed node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeError {
    /// The error name / type.
    pub ename: String,
    /// The error value / message.
    pub evalue: String,
    /// Traceback lines.
    pub traceback: Vec<String>,
    /// Optional hints for resolution.
    pub hints: Vec<String>,
}

/// Output data from a node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeOutput {
    /// MIME type -> data (e.g., "text/plain" -> "42", "image/png" -> base64).
    pub data: HashMap<String, String>,
    /// Metadata associated with the output.
    pub metadata: HashMap<String, serde_json::Value>,
}

/// Events emitted during execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ExecutionEvent {
    /// Execution started.
    ExecutionStarted {
        execution_id: ExecutionId,
        #[serde(default)]
        tree_id: Option<ExperimentTreeId>,
        #[serde(default)]
        branch_id: Option<BranchId>,
        #[serde(default)]
        target_kind: Option<ExecutionTargetKind>,
        #[serde(default)]
        target: Option<ExecutionTargetRef>,
    },
    /// A node began executing.
    NodeStarted {
        execution_id: ExecutionId,
        node_id: NodeId,
        #[serde(default)]
        tree_id: Option<ExperimentTreeId>,
        #[serde(default)]
        branch_id: Option<BranchId>,
        #[serde(default)]
        target_kind: Option<ExecutionTargetKind>,
        #[serde(default)]
        target: Option<ExecutionTargetRef>,
    },
    /// A node produced stream output (stdout/stderr).
    NodeStream {
        execution_id: ExecutionId,
        node_id: NodeId,
        #[serde(default)]
        tree_id: Option<ExperimentTreeId>,
        #[serde(default)]
        branch_id: Option<BranchId>,
        #[serde(default)]
        target_kind: Option<ExecutionTargetKind>,
        #[serde(default)]
        target: Option<ExecutionTargetRef>,
        stream: String,
        text: String,
    },
    /// A node produced display data (plots, rich output).
    NodeDisplayData {
        execution_id: ExecutionId,
        node_id: NodeId,
        #[serde(default)]
        tree_id: Option<ExperimentTreeId>,
        #[serde(default)]
        branch_id: Option<BranchId>,
        #[serde(default)]
        target_kind: Option<ExecutionTargetKind>,
        #[serde(default)]
        target: Option<ExecutionTargetRef>,
        output: NodeOutput,
    },
    /// A display output was updated (e.g., tqdm progress bar).
    NodeDisplayUpdate {
        execution_id: ExecutionId,
        node_id: NodeId,
        #[serde(default)]
        tree_id: Option<ExperimentTreeId>,
        #[serde(default)]
        branch_id: Option<BranchId>,
        #[serde(default)]
        target_kind: Option<ExecutionTargetKind>,
        #[serde(default)]
        target: Option<ExecutionTargetRef>,
        display_id: String,
        output: NodeOutput,
    },
    /// A node completed successfully.
    NodeCompleted {
        execution_id: ExecutionId,
        node_id: NodeId,
        #[serde(default)]
        tree_id: Option<ExperimentTreeId>,
        #[serde(default)]
        branch_id: Option<BranchId>,
        #[serde(default)]
        target_kind: Option<ExecutionTargetKind>,
        #[serde(default)]
        target: Option<ExecutionTargetRef>,
        artifacts: HashMap<SlotName, ArtifactKey>,
        duration_ms: u64,
    },
    /// A node hit the cache.
    NodeCacheHit {
        execution_id: ExecutionId,
        node_id: NodeId,
        #[serde(default)]
        tree_id: Option<ExperimentTreeId>,
        #[serde(default)]
        branch_id: Option<BranchId>,
        #[serde(default)]
        target_kind: Option<ExecutionTargetKind>,
        #[serde(default)]
        target: Option<ExecutionTargetRef>,
        artifacts: HashMap<SlotName, ArtifactKey>,
    },
    /// A node failed.
    NodeFailed {
        execution_id: ExecutionId,
        node_id: NodeId,
        #[serde(default)]
        tree_id: Option<ExperimentTreeId>,
        #[serde(default)]
        branch_id: Option<BranchId>,
        #[serde(default)]
        target_kind: Option<ExecutionTargetKind>,
        #[serde(default)]
        target: Option<ExecutionTargetRef>,
        error: NodeError,
    },
    /// Runtime dependencies detected after execution.
    RuntimeDepsDetected {
        execution_id: ExecutionId,
        node_id: NodeId,
        #[serde(default)]
        tree_id: Option<ExperimentTreeId>,
        #[serde(default)]
        branch_id: Option<BranchId>,
        #[serde(default)]
        target_kind: Option<ExecutionTargetKind>,
        #[serde(default)]
        target: Option<ExecutionTargetRef>,
        deps: Vec<String>,
    },
    /// Execution completed.
    ExecutionCompleted {
        execution_id: ExecutionId,
        #[serde(default)]
        tree_id: Option<ExperimentTreeId>,
        #[serde(default)]
        branch_id: Option<BranchId>,
        #[serde(default)]
        target_kind: Option<ExecutionTargetKind>,
        #[serde(default)]
        target: Option<ExecutionTargetRef>,
        duration_ms: u64,
    },
    /// Execution failed.
    ExecutionFailed {
        execution_id: ExecutionId,
        #[serde(default)]
        tree_id: Option<ExperimentTreeId>,
        #[serde(default)]
        branch_id: Option<BranchId>,
        #[serde(default)]
        target_kind: Option<ExecutionTargetKind>,
        #[serde(default)]
        target: Option<ExecutionTargetRef>,
        failed_nodes: Vec<NodeId>,
    },
    /// A branch isolation attempt started for a tree/branch runtime.
    IsolationAttempted {
        tree_id: ExperimentTreeId,
        branch_id: BranchId,
    },
    /// A branch isolation attempt completed successfully.
    IsolationSucceeded {
        tree_id: ExperimentTreeId,
        branch_id: BranchId,
        delta: NamespaceDelta,
    },
    /// A branch isolation attempt detected contamination.
    ContaminationDetected {
        tree_id: ExperimentTreeId,
        branch_id: BranchId,
        #[serde(default)]
        signals: Vec<String>,
    },
    /// Runtime fell back to restart/replay after an isolation decision.
    FallbackRestartTriggered {
        tree_id: ExperimentTreeId,
        branch_id: BranchId,
        reason: String,
    },
    /// Tree-owned runtime state changed.
    TreeRuntimeStateChanged {
        tree_id: ExperimentTreeId,
        branch_id: BranchId,
        kernel_state: TreeKernelState,
        runtime_epoch: u64,
        #[serde(default)]
        last_prepared_cell_id: Option<CellId>,
        #[serde(default)]
        materialized_path_cell_ids: Vec<CellId>,
    },
    /// A file in the workspace was created, modified, or deleted.
    FileChanged { path: String, kind: String },
}

/// High-level kind of execution target.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionTargetKind {
    ExperimentTreeBranch,
}

/// Canonical reference for the runtime target an execution was launched against.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ExecutionTargetRef {
    ExperimentTreeBranch {
        tree_id: ExperimentTreeId,
        branch_id: BranchId,
    },
}

/// Current trust/materialization state of a tree-owned kernel runtime.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TreeKernelState {
    Ready,
    NeedsReplay,
    Switching,
    KernelLost,
}

/// Runtime branch-isolation mode for a tree-owned kernel.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum BranchIsolationMode {
    #[default]
    Disabled,
    NamespaceGuarded,
}

/// High-level namespace diff captured by a branch-isolation attempt.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct NamespaceDelta {
    #[serde(default)]
    pub added: Vec<String>,
    #[serde(default)]
    pub removed: Vec<String>,
    #[serde(default)]
    pub changed: Vec<String>,
    #[serde(default)]
    pub module_drift: Vec<String>,
}

/// Result of the most recent branch-isolation attempt for a tree runtime.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IsolationResult {
    pub branch_id: BranchId,
    pub succeeded: bool,
    #[serde(default)]
    pub contamination_signals: Vec<String>,
    #[serde(default)]
    pub namespace_delta: Option<NamespaceDelta>,
}

/// Minimal runtime/materialization state for a tree-owned kernel.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TreeRuntimeState {
    pub tree_id: ExperimentTreeId,
    pub active_branch_id: BranchId,
    #[serde(default)]
    pub materialized_path_cell_ids: Vec<CellId>,
    pub runtime_epoch: u64,
    pub kernel_state: TreeKernelState,
    #[serde(default)]
    pub last_prepared_cell_id: Option<CellId>,
    #[serde(default)]
    pub isolation_mode: BranchIsolationMode,
    #[serde(default)]
    pub last_isolation_result: Option<IsolationResult>,
}

/// Result of preparing a tree runtime context for a target cell.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PreparedContext {
    pub tree_id: ExperimentTreeId,
    pub branch_id: BranchId,
    pub target_cell_id: CellId,
    pub runtime_state: TreeRuntimeState,
}

/// Read-only inspection result for a branch target without mutating runtime state.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BranchTargetInspection {
    pub tree_id: ExperimentTreeId,
    pub branch_id: BranchId,
    pub target_cell_id: CellId,
    #[serde(default)]
    pub lineage: Vec<BranchId>,
    #[serde(default)]
    pub path_cell_order: Vec<CellId>,
    #[serde(default)]
    pub topo_order: Vec<CellId>,
    pub has_live_kernel: bool,
    #[serde(default)]
    pub current_runtime_state: Option<TreeRuntimeState>,
    #[serde(default)]
    pub shared_prefix_cell_ids: Vec<CellId>,
    #[serde(default)]
    pub divergence_cell_id: Option<CellId>,
    pub replay_from_idx: usize,
    #[serde(default)]
    pub replay_cell_ids: Vec<CellId>,
    #[serde(default)]
    pub replay_prefix_before_target: Vec<CellId>,
}

/// Overall execution status.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionStatus {
    pub execution_id: ExecutionId,
    #[serde(default)]
    pub tree_id: Option<ExperimentTreeId>,
    #[serde(default)]
    pub branch_id: Option<BranchId>,
    #[serde(default)]
    pub target_kind: Option<ExecutionTargetKind>,
    #[serde(default)]
    pub target: Option<ExecutionTargetRef>,
    pub node_statuses: HashMap<NodeId, NodeStatus>,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
}

/// Result returned by the scheduler after execution completes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionOutcome {
    pub execution_id: ExecutionId,
    #[serde(default)]
    pub tree_id: Option<ExperimentTreeId>,
    #[serde(default)]
    pub branch_id: Option<BranchId>,
    #[serde(default)]
    pub target_kind: Option<ExecutionTargetKind>,
    #[serde(default)]
    pub target: Option<ExecutionTargetRef>,
    pub node_logs: HashMap<NodeId, NodeLogs>,
    pub node_statuses: HashMap<NodeId, NodeStatus>,
    pub failed_nodes: Vec<NodeId>,
    pub duration_ms: u64,
}

/// Logs from a single node execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeLogs {
    pub stdout: String,
    pub stderr: String,
    pub outputs: Vec<NodeOutput>,
    pub error: Option<NodeError>,
    pub duration_ms: Option<u64>,
    /// Metrics auto-extracted from output slot values.
    /// Scalars become `slot_name -> value`; dicts of scalars are flattened.
    #[serde(default)]
    pub metrics: HashMap<String, f64>,
}

// ---------------------------------------------------------------------------
// Cache types
// ---------------------------------------------------------------------------

/// Content-addressable cache key for a node execution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeCacheKey {
    /// blake3 hash of the node's source code.
    pub code_hash: [u8; 32],
    /// blake3 hashes of all input artifacts, keyed by slot.
    pub input_hashes: HashMap<SlotName, [u8; 32]>,
    /// blake3 hash of the lockfile (environment).
    pub lockfile_hash: [u8; 32],
}

impl std::hash::Hash for NodeCacheKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.code_hash.hash(state);
        // Sort keys for deterministic hashing
        let mut entries: Vec<_> = self.input_hashes.iter().collect();
        entries.sort_by_key(|(k, _)| (*k).clone());
        for (k, v) in entries {
            k.hash(state);
            v.hash(state);
        }
        self.lockfile_hash.hash(state);
    }
}

impl NodeCacheKey {
    /// Compute the code hash for a given source string.
    pub fn hash_code(source: &str) -> [u8; 32] {
        *blake3::hash(source.as_bytes()).as_bytes()
    }

    /// Compute the hash for raw bytes (artifact content).
    pub fn hash_bytes(data: &[u8]) -> [u8; 32] {
        *blake3::hash(data).as_bytes()
    }
}

// ---------------------------------------------------------------------------
// Artifact metadata
// ---------------------------------------------------------------------------

/// Metadata about a stored artifact.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactMetadata {
    pub key: ArtifactKey,
    pub size_bytes: u64,
    pub schema: Option<ArrowSchemaInfo>,
    pub created_at: DateTime<Utc>,
    pub content_hash: [u8; 32],
}

/// Serializable summary of an Arrow schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArrowSchemaInfo {
    pub columns: Vec<ColumnInfo>,
    pub num_rows: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

// ---------------------------------------------------------------------------
// Comparison
// ---------------------------------------------------------------------------

/// A side-by-side comparison of execution results across columns.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComparisonTable {
    /// Column headers (pipeline names or labels).
    pub columns: Vec<String>,
    /// Rows of metric values.
    pub rows: Vec<ComparisonRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComparisonRow {
    pub metric: String,
    pub values: Vec<Option<serde_json::Value>>,
}

// ---------------------------------------------------------------------------
// Workspace budget
// ---------------------------------------------------------------------------

/// Resource limits for a workspace/pipeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkspaceBudget {
    /// Maximum number of concurrent kernels.
    pub max_kernels: Option<usize>,
    /// Maximum RSS per kernel in bytes.
    pub max_kernel_rss_bytes: Option<u64>,
    /// Maximum total artifact storage in bytes.
    pub max_artifact_storage_bytes: Option<u64>,
    /// Idle kernel timeout in seconds.
    pub idle_kernel_timeout_secs: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KernelConnectionInfo {
    pub transport: String,
    pub ip: String,
    pub shell_port: u16,
    pub iopub_port: u16,
    pub stdin_port: u16,
    pub control_port: u16,
    pub hb_port: u16,
    pub key: String,
}

// ---------------------------------------------------------------------------
// Project
// ---------------------------------------------------------------------------

/// A top-level project container. Pipelines belong to a project.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectDef {
    pub id: ProjectId,
    pub name: String,
    pub description: Option<String>,
    pub workspace_dir: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tree_runtime_state_deserializes_with_isolation_defaults() {
        let json = serde_json::json!({
            "tree_id": "tree-1",
            "active_branch_id": "main",
            "materialized_path_cell_ids": ["cell-1"],
            "runtime_epoch": 3,
            "kernel_state": "ready",
            "last_prepared_cell_id": "cell-1"
        });

        let state: TreeRuntimeState = serde_json::from_value(json).unwrap();

        assert_eq!(state.isolation_mode, BranchIsolationMode::Disabled);
        assert_eq!(state.last_isolation_result, None);
    }

    #[test]
    fn isolation_event_payloads_round_trip() {
        let event = ExecutionEvent::IsolationSucceeded {
            tree_id: ExperimentTreeId::new("tree-1"),
            branch_id: BranchId::new("branch-a"),
            delta: NamespaceDelta {
                added: vec!["tmp_df".to_string()],
                removed: Vec::new(),
                changed: vec!["model".to_string()],
                module_drift: vec!["sys.path".to_string()],
            },
        };
        let json = serde_json::to_value(&event).unwrap();
        assert_eq!(json["type"], "isolation_succeeded");

        let runtime_state = TreeRuntimeState {
            tree_id: ExperimentTreeId::new("tree-1"),
            active_branch_id: BranchId::new("branch-a"),
            materialized_path_cell_ids: vec![CellId::new("cell-1"), CellId::new("cell-2")],
            runtime_epoch: 4,
            kernel_state: TreeKernelState::Ready,
            last_prepared_cell_id: Some(CellId::new("cell-2")),
            isolation_mode: BranchIsolationMode::NamespaceGuarded,
            last_isolation_result: Some(IsolationResult {
                branch_id: BranchId::new("branch-a"),
                succeeded: false,
                contamination_signals: vec!["sys.path".to_string()],
                namespace_delta: Some(NamespaceDelta {
                    added: vec!["tmp_df".to_string()],
                    removed: Vec::new(),
                    changed: vec!["model".to_string()],
                    module_drift: vec!["sys.path".to_string()],
                }),
            }),
        };

        let json = serde_json::to_value(&runtime_state).unwrap();
        let decoded: TreeRuntimeState = serde_json::from_value(json).unwrap();

        assert_eq!(
            decoded.isolation_mode,
            BranchIsolationMode::NamespaceGuarded
        );
        assert_eq!(
            decoded
                .last_isolation_result
                .as_ref()
                .unwrap()
                .contamination_signals,
            vec!["sys.path".to_string()]
        );
    }

    #[test]
    fn executable_tree_branch_round_trips_with_runtime_inputs() {
        let branch = ExecutableTreeBranch {
            tree_id: ExperimentTreeId::new("tree-1"),
            branch_id: BranchId::new("branch-a"),
            name: "tree-1 [branch-a]".to_string(),
            lineage: vec![BranchId::new("main"), BranchId::new("branch-a")],
            path_cell_order: vec![CellId::new("cell-1"), CellId::new("cell-2")],
            topo_order: vec![CellId::new("cell-1"), CellId::new("cell-2")],
            cells: vec![ExecutableTreeCell {
                tree_id: ExperimentTreeId::new("tree-1"),
                branch_id: BranchId::new("branch-a"),
                cell_id: CellId::new("cell-2"),
                name: "cell-2".to_string(),
                code: NodeCode {
                    source: "print(x)".to_string(),
                    language: "python".to_string(),
                },
                inputs: HashMap::from([(
                    SlotName::new("x"),
                    ExecutableTreeInput {
                        source_cell_id: CellId::new("cell-1"),
                        source_output: SlotName::new("value"),
                    },
                )]),
                outputs: vec![SlotName::new("result")],
                cache: true,
                map_over: None,
                map_concurrency: None,
                timeout_secs: Some(30),
                tags: HashMap::from([("stage".to_string(), "train".to_string())]),
                revision_id: Some(RevisionId::new("rev-1")),
            }],
            environment: EnvironmentSpec {
                dependencies: vec!["pandas".to_string()],
            },
            execution_mode: ExecutionMode::Sequential,
            budget: None,
            project_id: Some(ProjectId::new("project-1")),
            created_at: Utc::now(),
        };

        let json = serde_json::to_value(&branch).unwrap();
        let decoded: ExecutableTreeBranch = serde_json::from_value(json).unwrap();

        assert_eq!(decoded.tree_id.as_str(), "tree-1");
        assert_eq!(decoded.branch_id.as_str(), "branch-a");
        assert_eq!(decoded.path_cell_order.len(), 2);
        assert_eq!(decoded.cells.len(), 1);
        let input = decoded.cells[0].inputs.get(&SlotName::new("x")).unwrap();
        assert_eq!(input.source_cell_id.as_str(), "cell-1");
        assert_eq!(input.source_output.as_str(), "value");
    }
}
