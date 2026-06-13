-- Scope cache entries to their owning cell of their owning tree: reuse is
-- top-to-bottom only. Without node_id in the primary key, two cells with
-- identical code, inputs, and lockfile (e.g. sibling branch cells) evict
-- each other's rows; without scope_hash (tree id + cell id), the same
-- eviction happens across trees that share a cell id like the default
-- `cell_1`.
PRAGMA foreign_keys=OFF;

CREATE TABLE cache_scoped (
    code_hash         BLOB NOT NULL,
    input_hashes      TEXT NOT NULL,
    lockfile_hash     BLOB NOT NULL,
    artifacts         TEXT NOT NULL,
    source_runtime_id TEXT,
    node_id           TEXT NOT NULL DEFAULT '',
    -- hex blake3 of (tree id, cell id); '' for migrated legacy rows, whose
    -- scope is re-derived from source_runtime_id at load time.
    scope_hash        TEXT NOT NULL DEFAULT '',
    created_at        TEXT NOT NULL DEFAULT (datetime('now')),
    last_accessed     TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (code_hash, input_hashes, lockfile_hash, scope_hash, node_id)
);

INSERT OR IGNORE INTO cache_scoped (
    code_hash,
    input_hashes,
    lockfile_hash,
    artifacts,
    source_runtime_id,
    node_id,
    created_at,
    last_accessed
)
SELECT
    code_hash,
    input_hashes,
    lockfile_hash,
    artifacts,
    source_runtime_id,
    COALESCE(node_id, ''),
    created_at,
    last_accessed
FROM cache;

DROP TABLE cache;
ALTER TABLE cache_scoped RENAME TO cache;

PRAGMA foreign_keys=ON;
