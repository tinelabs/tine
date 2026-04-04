-- Experiment tree scaffolding
-- Additive schema for the experiment-tree migration. These tables coexist
-- with legacy pipeline tables during the compatibility window.

CREATE TABLE IF NOT EXISTS experiment_trees (
    id              TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    project_id      TEXT REFERENCES projects(id),
    definition      TEXT NOT NULL,     -- JSON: ExperimentTreeDef
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_experiment_trees_project ON experiment_trees(project_id);

CREATE TABLE IF NOT EXISTS branches (
    id                  TEXT PRIMARY KEY,
    tree_id             TEXT NOT NULL REFERENCES experiment_trees(id),
    name                TEXT NOT NULL,
    parent_branch_id    TEXT,
    branch_point_cell_id TEXT,
    definition          TEXT NOT NULL, -- JSON: BranchDef
    created_at          TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at          TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_branches_tree ON branches(tree_id);
CREATE INDEX IF NOT EXISTS idx_branches_parent ON branches(parent_branch_id);

CREATE TABLE IF NOT EXISTS cells (
    id              TEXT PRIMARY KEY,
    tree_id         TEXT NOT NULL REFERENCES experiment_trees(id),
    branch_id       TEXT NOT NULL REFERENCES branches(id),
    name            TEXT NOT NULL,
    definition      TEXT NOT NULL,     -- JSON: CellDef
    position        INTEGER NOT NULL DEFAULT 0,
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_cells_tree ON cells(tree_id);
CREATE INDEX IF NOT EXISTS idx_cells_branch ON cells(branch_id);
