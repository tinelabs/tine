-- Migration 007: Make executions tree-native
--
-- Goals:
--   1. Backfill tree_id from pipeline_id for any pre-005 rows that have NULL tree_id.
--   2. Recreate the executions table with tree_id NOT NULL and pipeline_id nullable (no FK).
--      This removes the hard dependency on the pipelines table so that tree-only execution
--      records can be inserted once the legacy pipeline routes are removed in Phase 3.
--   3. Drop the snapshots table — snapshot/rollback is unlinked from the UI and will be
--      removed from the code in Phase 3.
--
-- Note: pipelines table is NOT dropped here; it is still referenced by workspace code that
-- will be deleted in Phase 3. A follow-up migration will drop it after the code is gone.

-- Step 1: backfill tree_id from pipeline_id for legacy rows
UPDATE executions SET tree_id = pipeline_id WHERE tree_id IS NULL AND pipeline_id IS NOT NULL;

-- Step 2: preserve child tables that reference executions so the parent table can
-- be recreated safely while foreign-key enforcement remains enabled.
CREATE TABLE metrics_backup AS
SELECT execution_id, node_id, metric_name, metric_value, step, logged_at
FROM metrics;

DROP TABLE metrics;

CREATE TABLE params_backup AS
SELECT execution_id, node_id, param_name, param_value, logged_at
FROM params;

DROP TABLE params;

-- Step 3: recreate executions without the FK/NOT NULL constraint on pipeline_id
CREATE TABLE executions_new (
    id          TEXT PRIMARY KEY,
    tree_id     TEXT NOT NULL,
    branch_id   TEXT,
    target_kind TEXT,
    pipeline_id TEXT,   -- legacy: nullable, no foreign key
    status      TEXT NOT NULL,
    started_at  TEXT NOT NULL DEFAULT (datetime('now')),
    finished_at TEXT,
    node_logs   TEXT
);

INSERT INTO executions_new (id, tree_id, branch_id, target_kind, pipeline_id, status, started_at, finished_at, node_logs)
SELECT
    id,
    COALESCE(tree_id, pipeline_id) AS tree_id,
    branch_id,
    target_kind,
    pipeline_id,
    status,
    started_at,
    finished_at,
    node_logs
FROM executions;

DROP TABLE executions;
ALTER TABLE executions_new RENAME TO executions;

CREATE INDEX IF NOT EXISTS idx_executions_tree     ON executions(tree_id);
CREATE INDEX IF NOT EXISTS idx_executions_branch   ON executions(branch_id);
CREATE INDEX IF NOT EXISTS idx_executions_pipeline ON executions(pipeline_id);

-- Step 4: restore child tables against the recreated executions table.
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

-- Step 5: drop snapshots (unused from UI; code will be removed in Phase 3)
DROP TABLE IF EXISTS snapshots;
