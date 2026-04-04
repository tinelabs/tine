-- tine SQLite schema v1
-- Cache index: NodeCacheKey -> artifact keys

CREATE TABLE IF NOT EXISTS cache (
    code_hash       BLOB NOT NULL,
    input_hashes    TEXT NOT NULL,     -- JSON: {slot_name: hex_hash}
    lockfile_hash   BLOB NOT NULL,
    artifacts       TEXT NOT NULL,     -- JSON: {slot_name: artifact_key}
    source_runtime_id TEXT,
    node_id         TEXT,
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    last_accessed   TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (code_hash, input_hashes, lockfile_hash)
);

-- Artifact reference counting for safe GC
CREATE TABLE IF NOT EXISTS artifact_refs (
    artifact_key    TEXT PRIMARY KEY,
    ref_count       INTEGER NOT NULL DEFAULT 1,
    size_bytes      INTEGER NOT NULL,
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    last_accessed   TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Execution history
CREATE TABLE IF NOT EXISTS executions (
    id              TEXT PRIMARY KEY,
    tree_id         TEXT NOT NULL,
    branch_id       TEXT,
    target_kind     TEXT,
    status          TEXT NOT NULL,     -- JSON: ExecutionStatus
    started_at      TEXT NOT NULL DEFAULT (datetime('now')),
    finished_at     TEXT,
    node_logs       TEXT               -- JSON: {node_id: NodeLogs}
);

CREATE INDEX IF NOT EXISTS idx_executions_tree ON executions(tree_id);
CREATE INDEX IF NOT EXISTS idx_executions_branch ON executions(branch_id);

-- Experiment metrics (builtin registry)
CREATE TABLE IF NOT EXISTS metrics (
    execution_id    TEXT NOT NULL REFERENCES executions(id),
    node_id         TEXT NOT NULL,
    metric_name     TEXT NOT NULL,
    metric_value    REAL NOT NULL,
    step            INTEGER NOT NULL DEFAULT 0,
    logged_at       TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (execution_id, node_id, metric_name, step)
);

-- Experiment parameters (builtin registry)
CREATE TABLE IF NOT EXISTS params (
    execution_id    TEXT NOT NULL REFERENCES executions(id),
    node_id         TEXT NOT NULL,
    param_name      TEXT NOT NULL,
    param_value     TEXT NOT NULL,
    logged_at       TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (execution_id, node_id, param_name)
);
