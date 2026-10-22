-- Existing development installations only. Back up before running; stop old workers first.
-- Keep legacy files/mappings and disable legacy layouts. No remote files are moved/deleted.
ALTER TABLE seenical_workspace ADD COLUMN layout_version int NOT NULL DEFAULT 1;
UPDATE seenical_workspace SET paused=1,status='paused',error_code='workspace_layout_upgrade_required';
ALTER TABLE seenical_workspace DROP PRIMARY KEY, ADD PRIMARY KEY (workspace_id),
  ADD KEY idx_workspace_app (app_id);
ALTER TABLE seenical_workspace MODIFY layout_version int NOT NULL DEFAULT 2;
ALTER TABLE seenical_workspace_file ADD COLUMN workspace_id varchar(64) NOT NULL DEFAULT '',
  ADD COLUMN skill_id varchar(64) NOT NULL DEFAULT '';
UPDATE seenical_workspace_file f JOIN seenical_workspace w ON w.app_id=f.app_id
  SET f.workspace_id=w.workspace_id;
ALTER TABLE seenical_workspace_file DROP PRIMARY KEY, DROP INDEX uq_workspace_document,
  ADD PRIMARY KEY (workspace_id,skill_id,path_hash),
  ADD UNIQUE KEY uq_workspace_document (workspace_id,skill_id,source_type,doc_id);
-- Then execute seenical_workspace_sync_mysql.sql to create the new partition table.
-- Execute seenical_workspace_candidate_mysql.sql for partition-scoped candidates.
-- Retain seenical_material_replacement for interrupted legacy replacements; never delete it.
