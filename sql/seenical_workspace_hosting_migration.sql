-- Existing workspace installations only. Back up first; execute once before upgrading services.
-- Fresh installations use seenical_workspace_sync_mysql.sql instead.
-- No repositories, files, credentials or bindings are deleted or reassigned.
ALTER TABLE seenical_workspace
  ADD COLUMN repository_type varchar(16) NOT NULL DEFAULT 'own',
  ADD COLUMN creation_key varchar(32) DEFAULT NULL,
  ADD UNIQUE KEY uq_workspace_creation (app_id, creation_key);
