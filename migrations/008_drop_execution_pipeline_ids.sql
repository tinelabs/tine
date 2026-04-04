-- Migration 008: Drop legacy execution pipeline IDs
--
-- Goals:
--   1. Recreate the executions table without the legacy pipeline_id column.
--   2. Preserve child tables that reference executions while the parent table is recreated.
--   3. Keep execution history keyed by execution/tree/branch identity only.

CREATE TABLE metrics_backup AS
SELECT execution_id, node_id, metric_name, metric_value, step, logged_at
FROM metrics;

DROP TABLE metrics;

CREATE TABLE params_backup AS
SELECT execution_id, node_id, param_name, param_value, logged_at
FROM params;

DROP TABLE params;

CREATE TABLE executions_new (
    id          TEXT PRIMARY KEY,
    tree_id     TEXT NOT NULL,
    branch_id   TEXT,
    target_kind TEXT,
    status      TEXT NOT NULL,
    started_at  TEXT NOT NULL DEFAULT (datetime('now')),
    finished_at TEXT,
    node_logs   TEXT
);

INSERT INTO executions_new (id, tree_id, branch_id, target_kind, status, started_at, finished_at, node_logs)
SELECT
    id,
    tree_id,
    branch_id,
    target_kind,
    status,
    started_at,
    finished_at,
    node_logs
FROM executions;

DROP TABLE executions;
ALTER TABLE executions_new RENAME TO executions;

CREATE INDEX IF NOT EXISTS idx_executions_tree   ON executions(tree_id);
CREATE INDEX IF NOT EXISTS idx_executions_branch ON executions(branch_id);

CREATE TABLE metrics (
    execution_id    TEXT NOT NULL REFERENCES executions(id),
    node_id         TEXT NOT NULL,
    metric_name     TEXT NOT NULL,
    metric_value    REAL NOT NULL,
    step            INTEGER NOT NULL DEFAULT 0,
    logged_at       TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (execution_id, node_id, metric_name, step)
);

INSERT INTO metrics (execution_id, node_id, metric_name, metric_value, step, logged_at)
SELECT execution_id, node_id, metric_name, metric_value, step, logged_at
FROM metrics_backup;

DROP TABLE metrics_backup;

CREATE TABLE params (
    execution_id    TEXT NOT NULL REFERENCES executions(id),
    node_id         TEXT NOT NULL,
    param_name      TEXT NOT NULL,
    param_value     TEXT NOT NULL,
    logged_at       TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (execution_id, node_id, param_name)
);

INSERT INTO params (execution_id, node_id, param_name, param_value, logged_at)
SELECT execution_id, node_id, param_name, param_value, logged_at
FROM params_backup;

DROP TABLE params_backup;
