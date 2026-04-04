-- Add provenance columns to cache table for staleness detection
ALTER TABLE cache ADD COLUMN source_runtime_id TEXT;
ALTER TABLE cache ADD COLUMN node_id TEXT;
