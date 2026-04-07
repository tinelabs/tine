use crate::types::{ArtifactKey, ExecutionId, NodeId, ProjectId, SlotName};
use thiserror::Error;

/// Top-level error type for the tine engine.
#[derive(Debug, Error)]
pub enum TineError {
    // -- Graph errors --
    #[error("cycle detected in runtime {runtime_id}")]
    CycleDetected { runtime_id: String },

    #[error("node {node_id} not found in runtime {runtime_id}")]
    NodeNotFound { runtime_id: String, node_id: NodeId },

    #[error("runtime {0} not found")]
    RuntimeNotFound(String),

    #[error("duplicate node id {node_id} in runtime {runtime_id}")]
    DuplicateNode { runtime_id: String, node_id: NodeId },

    #[error("invalid edge: node {from} slot {slot} -> node {to} (slot not found)")]
    InvalidEdge {
        from: NodeId,
        to: NodeId,
        slot: SlotName,
    },

    // -- Execution errors --
    #[error("execution {0} not found")]
    ExecutionNotFound(ExecutionId),

    #[error("execution {execution_id} already running for runtime {runtime_id}")]
    ExecutionAlreadyRunning {
        execution_id: ExecutionId,
        runtime_id: String,
    },

    #[error("node {node_id} execution failed: {message}")]
    NodeExecutionFailed { node_id: NodeId, message: String },

    #[error("execution {0} was interrupted")]
    ExecutionInterrupted(ExecutionId),

    #[error("execution timed out after {timeout_secs}s")]
    ExecutionTimedOut { timeout_secs: u64 },

    // -- Kernel errors --
    #[error("kernel startup failed for runtime {runtime_id}: {message}")]
    KernelStartupFailed { runtime_id: String, message: String },

    #[error("kernel {kernel_id} not found")]
    KernelNotFound { kernel_id: String },

    #[error("kernel communication error: {0}")]
    KernelComm(String),

    #[error("kernel heartbeat timeout for runtime {runtime_id}")]
    KernelHeartbeatTimeout { runtime_id: String },

    // -- Data / catalog errors --
    #[error("artifact {0} not found")]
    ArtifactNotFound(ArtifactKey),

    #[error("type mismatch in node {node}: column {column} expected {expected}, got {actual}")]
    TypeMismatch {
        node: NodeId,
        column: String,
        expected: String,
        actual: String,
    },

    #[error(
        "missing column in node {node} input {input}: column {missing} not found (available: {available:?})"
    )]
    MissingColumn {
        node: NodeId,
        input: String,
        missing: String,
        available: Vec<String>,
    },

    #[error("schema validation failed: {0}")]
    SchemaValidation(String),

    // -- Environment errors --
    #[error("uv not found at {path}: {message}")]
    UvNotFound { path: String, message: String },

    #[error("environment creation failed for runtime {runtime_id}: {message}")]
    EnvironmentFailed { runtime_id: String, message: String },

    #[error("dependency resolution failed: {0}")]
    DependencyResolution(String),

    // -- Project errors --
    #[error("project {0} not found")]
    ProjectNotFound(ProjectId),

    // -- Persistence errors --
    #[error("database error: {0}")]
    Database(String),

    // -- Budget errors --
    #[error("budget exceeded: {0}")]
    BudgetExceeded(String),

    // -- Configuration errors --
    #[error("configuration error: {0}")]
    Config(String),

    // -- I/O errors --
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    // -- Serialization errors --
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    // -- Generic internal error --
    #[error("internal error: {0}")]
    Internal(String),
}

/// Result alias for TineError.
pub type TineResult<T> = Result<T, TineError>;
