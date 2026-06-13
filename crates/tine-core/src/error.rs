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

    #[error("idempotency key conflict: {0}")]
    IdempotencyConflict(String),

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

    // -- Caller-addressable lookup failures (bad ids in requests) --
    #[error("{0}")]
    NotFound(String),

    // -- Generic internal error --
    #[error("internal error: {0}")]
    Internal(String),
}

impl TineError {
    /// Stable machine-readable error code, exposed through API error bodies
    /// so clients (agents in particular) can branch on error class instead
    /// of parsing prose.
    pub fn code(&self) -> &'static str {
        match self {
            TineError::CycleDetected { .. } => "cycle_detected",
            TineError::NodeNotFound { .. } => "not_found",
            TineError::RuntimeNotFound(_) => "not_found",
            TineError::DuplicateNode { .. } => "duplicate_node",
            TineError::InvalidEdge { .. } => "invalid_edge",
            TineError::ExecutionNotFound(_) => "not_found",
            TineError::ExecutionAlreadyRunning { .. } => "execution_already_running",
            TineError::NodeExecutionFailed { .. } => "node_execution_failed",
            TineError::ExecutionInterrupted(_) => "execution_interrupted",
            TineError::IdempotencyConflict(_) => "idempotency_conflict",
            TineError::KernelStartupFailed { .. } => "kernel_startup_failed",
            TineError::KernelNotFound { .. } => "kernel_unavailable",
            TineError::KernelComm(_) => "kernel_unavailable",
            TineError::KernelHeartbeatTimeout { .. } => "kernel_unavailable",
            TineError::ArtifactNotFound(_) => "not_found",
            TineError::TypeMismatch { .. } => "type_mismatch",
            TineError::MissingColumn { .. } => "missing_column",
            TineError::SchemaValidation(_) => "schema_validation",
            TineError::UvNotFound { .. } => "environment_failed",
            TineError::EnvironmentFailed { .. } => "environment_failed",
            TineError::DependencyResolution(_) => "environment_failed",
            TineError::ProjectNotFound(_) => "not_found",
            TineError::Database(_) => "database",
            TineError::BudgetExceeded(_) => "queue_full",
            TineError::Config(_) => "validation",
            TineError::Io(_) => "io",
            TineError::Serialization(_) => "serialization",
            TineError::NotFound(_) => "not_found",
            TineError::Internal(_) => "internal",
        }
    }
}

/// Result alias for TineError.
pub type TineResult<T> = Result<T, TineError>;
