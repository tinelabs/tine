-- Persist tree runtime/materialization state alongside experiment trees.
ALTER TABLE experiment_trees ADD COLUMN runtime_state TEXT;
