-- Optional client-supplied idempotency keys for execute submissions.
-- A retried submission carrying the same key for the same execution
-- target returns the original execution instead of starting a duplicate
-- run. Keys are scoped to the execute target (tree/branch, and cell for
-- cell submissions) so a reused key never attaches to an unrelated run.
-- The fingerprint captures the execution-relevant request state (cell
-- code and environment): a retry only reattaches when it matches, and a
-- reused key with different state is rejected as a conflict.
ALTER TABLE executions ADD COLUMN idempotency_key TEXT;
ALTER TABLE executions ADD COLUMN idempotency_scope TEXT;
ALTER TABLE executions ADD COLUMN idempotency_fingerprint TEXT;
DROP INDEX IF EXISTS idx_executions_idempotency_key;
CREATE UNIQUE INDEX IF NOT EXISTS idx_executions_idempotency_key_scope
    ON executions(idempotency_key, idempotency_scope)
    WHERE idempotency_key IS NOT NULL;
