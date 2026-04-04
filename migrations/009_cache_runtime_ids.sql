-- Rename legacy cache provenance to tree-native runtime naming.
PRAGMA foreign_keys=OFF;

CREATE TABLE cache_new (
    code_hash         BLOB NOT NULL,
    input_hashes      TEXT NOT NULL,
    lockfile_hash     BLOB NOT NULL,
    artifacts         TEXT NOT NULL,
    source_runtime_id TEXT,
    node_id           TEXT,
    created_at        TEXT NOT NULL DEFAULT (datetime('now')),
    last_accessed     TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (code_hash, input_hashes, lockfile_hash)
);

INSERT INTO cache_new (
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
    source_pipeline_id,
    node_id,
    created_at,
    last_accessed
FROM cache;

DROP TABLE cache;
ALTER TABLE cache_new RENAME TO cache;

PRAGMA foreign_keys=ON;
