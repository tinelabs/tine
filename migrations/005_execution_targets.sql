ALTER TABLE executions ADD COLUMN tree_id TEXT;
ALTER TABLE executions ADD COLUMN branch_id TEXT;
ALTER TABLE executions ADD COLUMN target_kind TEXT;

CREATE INDEX IF NOT EXISTS idx_executions_tree ON executions(tree_id);
CREATE INDEX IF NOT EXISTS idx_executions_branch ON executions(branch_id);
