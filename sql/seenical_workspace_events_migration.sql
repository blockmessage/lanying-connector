-- Existing workspace installations only; run once before deploying event workers.
-- Fresh installations use seenical_workspace_sync_mysql.sql instead.
-- Marks existing workspaces dirty once; does not modify documents or references.
ALTER TABLE seenical_workspace
  ADD COLUMN requested_generation bigint NOT NULL DEFAULT 1,
  ADD COLUMN completed_generation bigint NOT NULL DEFAULT 0,
  ADD UNIQUE KEY uq_workspace_id (workspace_id);
